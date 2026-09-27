package feishucalendar

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

const feishuCalendarChangedEventV4 = "calendar.calendar.event.changed_v4"

type feishuCalendarChangedPayload struct {
	Header struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
	} `json:"header"`
	Event struct {
		CalendarID      string `json:"calendar_id"`
		CalendarEventID string `json:"calendar_event_id"`
		ChangeType      string `json:"change_type"`
		UserIDList      []struct {
			OpenID  string `json:"open_id"`
			UnionID string `json:"union_id"`
			UserID  string `json:"user_id"`
		} `json:"user_id_list"`
		RSVPInfos json.RawMessage `json:"rsvp_infos"`
	} `json:"event"`
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.VerifiedWebhook{}, permanent("webhook_provider_mismatch", "Feishu Calendar webhook provider mismatch")
	}
	verified, err := internalfeishu.VerifyWebhook(ctx, request, "feishu_calendar")
	if err != nil || verified.Challenge != "" {
		return verified, err
	}
	var payload feishuCalendarChangedPayload
	if json.Unmarshal(verified.Payload, &payload) != nil || payload.Header.EventID != verified.ExternalID || payload.Header.EventType != feishuCalendarChangedEventV4 || verified.EventType != feishuCalendarChangedEventV4 {
		return connector.VerifiedWebhook{}, permanent("webhook_event_invalid", "Feishu Calendar webhook event is invalid")
	}
	payload.Event.CalendarID = strings.TrimSpace(payload.Event.CalendarID)
	payload.Event.CalendarEventID = strings.TrimSpace(payload.Event.CalendarEventID)
	payload.Event.ChangeType = strings.ToLower(strings.TrimSpace(payload.Event.ChangeType))
	if payload.Event.CalendarID == "" || payload.Event.CalendarEventID == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_event_invalid", "Feishu Calendar webhook lacks calendar or event identity")
	}
	eventType := ""
	switch payload.Event.ChangeType {
	case "create":
		eventType = "feishu.calendar.event.created"
	case "update":
		eventType = "feishu.calendar.event.updated"
	case "delete":
		eventType = "feishu.calendar.event.cancelled"
	case "rsvp":
		eventType = "feishu.calendar.event.rsvp_changed"
	default:
		return connector.VerifiedWebhook{}, permanent("webhook_change_type_invalid", "Feishu Calendar webhook change type is unsupported")
	}
	type route struct {
		Kind  string `json:"kind"`
		Value string `json:"value"`
	}
	routeSeen := map[string]bool{}
	routes := []route{}
	for _, user := range payload.Event.UserIDList {
		for _, candidate := range []route{{Kind: "open_id", Value: user.OpenID}, {Kind: "union_id", Value: user.UnionID}, {Kind: "user_id", Value: user.UserID}} {
			candidate.Value = strings.TrimSpace(candidate.Value)
			identity := candidate.Kind + "\x00" + candidate.Value
			if candidate.Value == "" || routeSeen[identity] {
				continue
			}
			routeSeen[identity] = true
			routes = append(routes, candidate)
		}
	}
	if len(routes) == 0 || len(routes) > 100 {
		return connector.VerifiedWebhook{}, permanent("webhook_route_invalid", "Feishu Calendar webhook has no bounded account route")
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Kind != routes[j].Kind {
			return routes[i].Kind < routes[j].Kind
		}
		return routes[i].Value < routes[j].Value
	})
	data := map[string]any{
		"calendar_id": payload.Event.CalendarID, "event_id": payload.Event.CalendarEventID, "change_type": payload.Event.ChangeType,
	}
	if len(payload.Event.RSVPInfos) != 0 && string(payload.Event.RSVPInfos) != "null" {
		var rsvp any
		if json.Unmarshal(payload.Event.RSVPInfos, &rsvp) != nil {
			return connector.VerifiedWebhook{}, permanent("webhook_event_invalid", "Feishu Calendar RSVP payload is invalid")
		}
		data["rsvp_infos"] = rsvp
	}
	eventPayload, _ := json.Marshal(map[string]any{
		"provider": ProviderKey, "calendar_id": payload.Event.CalendarID, "event_id": payload.Event.CalendarEventID,
		"change_type": payload.Event.ChangeType, "source": "webhook",
	})
	envelope, err := json.Marshal(map[string]any{
		"routes": routes, "wake_tasks": []string{feishuCalendarSyncTaskKey}, "data": data,
		"events": []connector.BackgroundEvent{{ExternalID: "feishu-calendar:webhook:" + verified.ExternalID, EventType: eventType, Payload: eventPayload}},
	})
	if err != nil {
		return connector.VerifiedWebhook{}, connector.PermanentError("feishu_calendar.webhook_event_invalid", errors.New("encode Feishu Calendar webhook event"))
	}
	verified.Payload = envelope
	verified.EventType = eventType
	return verified, nil
}

var _ connector.WebhookVerifier = (*provider)(nil)
