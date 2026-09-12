package microsoft

import (
	"context"
	"time"

	"github.com/domainry/domainry-connector-sdk/calendar"
)

const supportedIanaZonesPath = "/me/outlook/supportedTimeZones(TimeZoneStandard=microsoft.graph.timeZoneStandard'Iana')"

func (s *calendarSession) writeMoments(ctx context.Context, start, end calendar.Moment, series bool) (graphCalendarMoment, graphCalendarMoment, error) {
	var zones struct {
		Values *[]struct {
			Alias string `json:"alias"`
		} `json:"value"`
		Next string `json:"@odata.nextLink"`
	}
	if err := s.get(ctx, graphBase(s.connection)+supportedIanaZonesPath, nil, "UTC", &zones); err != nil {
		return graphCalendarMoment{}, graphCalendarMoment{}, err
	}
	if zones.Values == nil || len(*zones.Values) > 1000 || zones.Next != "" {
		return graphCalendarMoment{}, graphCalendarMoment{}, calendarInvalidResponse("supported time zones are incomplete")
	}
	allowed := map[string]bool{"UTC": true}
	for _, zone := range *zones.Values {
		if !calendarSourceZoneValid(zone.Alias) {
			return graphCalendarMoment{}, graphCalendarMoment{}, calendarInvalidResponse("invalid supported time zone")
		}
		allowed[zone.Alias] = true
	}
	if !allowed[start.TimeZone] || !allowed[end.TimeZone] {
		return graphCalendarMoment{}, graphCalendarMoment{}, permanent("calendar.unsupported_time_zone", "requested time zone is not supported by this mailbox")
	}
	if start.Date != "" {
		return graphCalendarMoment{DateTime: start.Date + "T00:00:00", TimeZone: start.TimeZone}, graphCalendarMoment{DateTime: end.Date + "T00:00:00", TimeZone: end.TimeZone}, nil
	}
	a, _ := time.Parse(time.RFC3339Nano, start.DateTime)
	b, _ := time.Parse(time.RFC3339Nano, end.DateTime)
	const localFormat = "2006-01-02T15:04:05.999999999"
	left, right := graphCalendarMoment{DateTime: a.Format(localFormat), TimeZone: start.TimeZone}, graphCalendarMoment{DateTime: b.Format(localFormat), TimeZone: end.TimeZone}
	_, errA := calendar.LocalDateTime(left.DateTime, left.TimeZone)
	_, errB := calendar.LocalDateTime(right.DateTime, right.TimeZone)
	if errA != nil || errB != nil {
		// A local Graph dateTime has no fold selector. An individual event can
		// retain the exact instant in UTC; converting an entire recurrence to
		// UTC would change future wall times, so that case is rejected explicitly.
		if series {
			return graphCalendarMoment{}, graphCalendarMoment{}, permanent("calendar.ambiguous_series_time", "Graph cannot preserve an explicit DST fold for the entire series")
		}
		left, right = graphCalendarMoment{DateTime: a.UTC().Format(localFormat), TimeZone: "UTC"}, graphCalendarMoment{DateTime: b.UTC().Format(localFormat), TimeZone: "UTC"}
	}
	return left, right, nil
}
