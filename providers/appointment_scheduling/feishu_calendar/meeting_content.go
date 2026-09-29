package feishucalendar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

// Integration's personal-account read boundary retains at most a 4 MiB JSON
// response in memory and never persists it. Keep the raw SRT at 2 MiB so JSON
// escaping plus metadata cannot cross that owner-enforced ceiling.
const transcriptResponseLimit int64 = 2 << 20

var meetingNumberPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{6,64}$`)
var minuteTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{6,128}$`)

type FetchMeetingContentInput struct {
	MeetingNo       string `json:"meeting_no"`
	CalendarEventID string `json:"calendar_event_id,omitempty"`
	Start           string `json:"start"`
	End             string `json:"end"`
}

type FetchMeetingContentOutput struct {
	Status             string `json:"status"`
	MeetingID          string `json:"meeting_id,omitempty"`
	RecordingStatus    string `json:"recording_status"`
	RecordingID        string `json:"recording_id,omitempty"`
	MinuteToken        string `json:"minute_token,omitempty"`
	MinuteURL          string `json:"minute_url,omitempty"`
	MinuteTitle        string `json:"minute_title,omitempty"`
	NoteID             string `json:"note_id,omitempty"`
	DurationMS         string `json:"duration_ms,omitempty"`
	Transcript         string `json:"transcript,omitempty"`
	TranscriptFormat   string `json:"transcript_format,omitempty"`
	TranscriptSHA256   string `json:"transcript_sha256,omitempty"`
	SpeakerPreserved   bool   `json:"speaker_preserved"`
	TimestampPreserved bool   `json:"timestamp_preserved"`
}

var FetchMeetingContent = connector.CallOperation[FetchMeetingContentInput, FetchMeetingContentOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "fetch_meeting_content",
	ContractSHA256: "574d2b2142e2bb2438d348f111ae843ed07e7de7f6a6b0cfc7852fb0b39bf33d",
	Reliability:    readReliability(),
}

type feishuMeetingBrief struct {
	ID              string `json:"id"`
	MeetingNo       string `json:"meeting_no"`
	StartTime       string `json:"start_time"`
	EndTime         string `json:"end_time"`
	CalendarEventID string `json:"calendar_event_id"`
}

type feishuMeetingListData struct {
	HasMore       bool                 `json:"has_more"`
	PageToken     string               `json:"page_token"`
	MeetingBriefs []feishuMeetingBrief `json:"meeting_briefs"`
}

type feishuRecording struct {
	ID        string `json:"id"`
	MeetingID string `json:"meeting_id"`
	URL       string `json:"url"`
	Duration  string `json:"duration"`
}

type feishuMinute struct {
	Token    string `json:"token"`
	Title    string `json:"title"`
	Duration string `json:"duration"`
	URL      string `json:"url"`
	NoteID   string `json:"note_id"`
}

func (p *provider) fetchMeetingContent(ctx context.Context, request connector.TypedRequest[FetchMeetingContentInput]) (connector.TypedResult[FetchMeetingContentOutput], error) {
	var output FetchMeetingContentOutput
	meetingNo := strings.TrimSpace(request.Input.MeetingNo)
	calendarEventID := strings.TrimSpace(request.Input.CalendarEventID)
	startAt, endAt, err := validateRange(request.Input.Start, request.Input.End)
	if err != nil || !meetingNumberPattern.MatchString(meetingNo) || endAt.Sub(startAt) > 7*24*time.Hour {
		return connector.TypedResult[FetchMeetingContentOutput]{}, permanent("meeting_content.invalid_request", "meeting_no and a bounded RFC3339 meeting window are required")
	}
	output.Status, output.RecordingStatus = meetingContentWaitingStatus(endAt)
	session := p.newAPISession(request.Connection, request.Secrets)
	meetings, responseRef, err := p.meetingsByNumber(ctx, session, meetingNo, startAt, endAt)
	if err != nil {
		return meetingContentResult(session, output, responseRef), err
	}
	meeting, found := selectMeetingBrief(meetings, calendarEventID, startAt)
	if !found {
		return meetingContentResult(session, output, responseRef), nil
	}
	output.MeetingID = meeting.ID
	meetingEnd, _ := parseUnixSeconds(meeting.EndTime)
	if !meetingEnd.IsZero() && time.Now().UTC().Before(meetingEnd.UTC()) {
		output.Status, output.RecordingStatus = "pending", "pending"
		return meetingContentResult(session, output, responseRef), nil
	}
	recordingResult, err := session.call(ctx, http.MethodGet, "/open-apis/vc/v1/meetings/"+url.PathEscape(meeting.ID)+"/recording", nil, nil, false)
	if err != nil {
		return meetingContentResult(session, output, recordingResult.ResponseRef), err
	}
	var recordingData struct {
		Recording *feishuRecording `json:"recording"`
	}
	if err = decodeResponseData(recordingResult.Output, &recordingData); err != nil {
		return meetingContentResult(session, output, recordingResult.ResponseRef), permanent("meeting_content.invalid_response", "Feishu recording response is invalid")
	}
	if recordingData.Recording == nil || strings.TrimSpace(recordingData.Recording.URL) == "" {
		return meetingContentResult(session, output, recordingResult.ResponseRef), nil
	}
	recording := *recordingData.Recording
	output.RecordingID = strings.TrimSpace(recording.ID)
	output.DurationMS = strings.TrimSpace(recording.Duration)
	minuteToken, err := minuteTokenFromURL(recording.URL)
	if err != nil {
		return meetingContentResult(session, output, recordingResult.ResponseRef), err
	}
	output.MinuteToken = minuteToken
	minuteResult, err := session.call(ctx, http.MethodGet, "/open-apis/minutes/v1/minutes/"+url.PathEscape(minuteToken), nil, nil, false)
	if err != nil {
		return meetingContentResult(session, output, minuteResult.ResponseRef), err
	}
	var minuteData struct {
		Minute *feishuMinute `json:"minute"`
	}
	if err = decodeResponseData(minuteResult.Output, &minuteData); err != nil || minuteData.Minute == nil {
		return meetingContentResult(session, output, minuteResult.ResponseRef), permanent("meeting_content.invalid_response", "Feishu Minutes response is invalid")
	}
	minute := *minuteData.Minute
	if token := strings.TrimSpace(minute.Token); token != "" && token != minuteToken {
		return meetingContentResult(session, output, minuteResult.ResponseRef), permanent("meeting_content.identity_mismatch", "Feishu Minutes token does not match the recording")
	}
	output.MinuteURL = strings.TrimSpace(minute.URL)
	output.MinuteTitle = strings.TrimSpace(minute.Title)
	output.NoteID = strings.TrimSpace(minute.NoteID)
	if output.DurationMS == "" {
		output.DurationMS = strings.TrimSpace(minute.Duration)
	}
	transcript, transcriptRef, err := session.download(ctx, "/open-apis/minutes/v1/minutes/"+url.PathEscape(minuteToken)+"/transcript", url.Values{
		"need_speaker":   {"true"},
		"need_timestamp": {"true"},
		"file_format":    {"srt"},
	})
	if err != nil {
		return meetingContentResult(session, output, transcriptRef), err
	}
	transcript = bytes.TrimPrefix(transcript, []byte{0xef, 0xbb, 0xbf})
	if len(bytes.TrimSpace(transcript)) == 0 || !utf8.Valid(transcript) {
		return meetingContentResult(session, output, transcriptRef), permanent("meeting_content.transcript_invalid", "Feishu transcript is empty or is not UTF-8")
	}
	digest := sha256.Sum256(transcript)
	output.Status, output.RecordingStatus = "ready", "ready"
	output.Transcript = string(transcript)
	output.TranscriptFormat = "srt"
	output.TranscriptSHA256 = hex.EncodeToString(digest[:])
	output.SpeakerPreserved = true
	output.TimestampPreserved = true
	return meetingContentResult(session, output, transcriptRef), nil
}

func (p *provider) meetingsByNumber(ctx context.Context, session *apiSession, meetingNo string, startAt, endAt time.Time) ([]feishuMeetingBrief, string, error) {
	queryStart := startAt.Add(-6 * time.Hour)
	queryEnd := endAt.Add(6 * time.Hour)
	meetings := make([]feishuMeetingBrief, 0, 4)
	pageToken, responseRef := "", ""
	seenTokens := map[string]bool{}
	for page := 0; page < 4; page++ {
		query := url.Values{
			"meeting_no": {meetingNo}, "start_time": {strconv.FormatInt(queryStart.Unix(), 10)},
			"end_time": {strconv.FormatInt(queryEnd.Unix(), 10)}, "page_size": {"50"},
		}
		if pageToken != "" {
			query.Set("page_token", pageToken)
		}
		result, err := session.call(ctx, http.MethodGet, "/open-apis/vc/v1/meetings/list_by_no", query, nil, false)
		responseRef = result.ResponseRef
		if err != nil {
			return nil, responseRef, err
		}
		var data feishuMeetingListData
		if err = decodeResponseData(result.Output, &data); err != nil || len(data.MeetingBriefs) > 50 {
			return nil, responseRef, permanent("meeting_content.invalid_response", "Feishu meeting list response is invalid")
		}
		meetings = append(meetings, data.MeetingBriefs...)
		if !data.HasMore {
			return meetings, responseRef, nil
		}
		pageToken = strings.TrimSpace(data.PageToken)
		if pageToken == "" || seenTokens[pageToken] {
			return nil, responseRef, permanent("meeting_content.invalid_response", "Feishu meeting pagination is invalid")
		}
		seenTokens[pageToken] = true
	}
	return nil, responseRef, permanent("meeting_content.page_limit", "Feishu meeting lookup exceeded 200 results")
}

func selectMeetingBrief(meetings []feishuMeetingBrief, calendarEventID string, startAt time.Time) (feishuMeetingBrief, bool) {
	type candidate struct {
		meeting feishuMeetingBrief
		exact   bool
		delta   time.Duration
	}
	candidates := make([]candidate, 0, len(meetings))
	for _, meeting := range meetings {
		meeting.ID = strings.TrimSpace(meeting.ID)
		if meeting.ID == "" {
			continue
		}
		candidateStart, err := parseUnixSeconds(meeting.StartTime)
		if err != nil {
			continue
		}
		delta := candidateStart.Sub(startAt)
		if delta < 0 {
			delta = -delta
		}
		candidates = append(candidates, candidate{meeting: meeting, exact: calendarEventID != "" && strings.TrimSpace(meeting.CalendarEventID) == calendarEventID, delta: delta})
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		if candidates[left].exact != candidates[right].exact {
			return candidates[left].exact
		}
		return candidates[left].delta < candidates[right].delta
	})
	if len(candidates) == 0 || calendarEventID != "" && !candidates[0].exact && candidates[0].delta > 6*time.Hour {
		return feishuMeetingBrief{}, false
	}
	return candidates[0].meeting, true
}

func (s *apiSession) download(ctx context.Context, path string, query url.Values) ([]byte, string, error) {
	token, err := s.accessToken(ctx)
	if err != nil {
		return nil, "", err
	}
	body, ref, err := s.provider.executeDownload(ctx, s.connection, token, path, query)
	if err == nil || !feishuAccessTokenRejected(err) || strings.TrimSpace(s.secrets["refresh_token"]) == "" {
		return body, ref, err
	}
	updates, refreshErr := s.provider.refreshUserToken(ctx, s.connection, s.secrets)
	if refreshErr != nil {
		return nil, "oauth:refresh_failed", refreshErr
	}
	for key, value := range updates {
		s.secrets[key] = value
		s.updates[key] = value
	}
	s.token = updates["access_token"]
	return s.provider.executeDownload(ctx, s.connection, s.token, path, query)
}

func (p *provider) executeDownload(ctx context.Context, connection connector.Connection, token, path string, query url.Values) ([]byte, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", permanent("request_invalid", "request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"text/plain, application/x-subrip"}},
		SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: transcriptResponseLimit,
	})
	if transportErr != nil {
		return nil, "", connector.RetryableError("feishu_calendar.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	providerOutput := Response{}
	hasProviderError := len(bytes.TrimSpace(response.Body)) > 0 && bytes.HasPrefix(bytes.TrimSpace(response.Body), []byte("{")) && json.Unmarshal(response.Body, &providerOutput) == nil && intValue(providerOutput, "code") != 0
	if internalfeishu.IsRateLimitedResponse(response.StatusCode, response.Body) {
		return nil, ref, internalfeishu.RateLimitError("feishu_calendar")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || hasProviderError {
		code := "feishu_calendar.http_" + strconv.Itoa(response.StatusCode)
		if hasProviderError {
			code = "feishu_calendar.provider_code_" + strconv.Itoa(intValue(providerOutput, "code"))
		}
		cause := fmt.Errorf("provider rejected transcript download with HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			return nil, ref, connector.RetryableError(code, cause)
		}
		return nil, ref, connector.PermanentError(code, cause)
	}
	return response.Body, ref, nil
}

func decodeResponseData(output Response, target any) error {
	raw, err := json.Marshal(output["data"])
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func parseUnixSeconds(value string) (time.Time, error) {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, errors.New("invalid unix timestamp")
	}
	return time.Unix(seconds, 0), nil
}

func minuteTokenFromURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return "", permanent("meeting_content.minute_url_invalid", "Feishu recording URL is invalid")
	}
	segments := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	for index := range segments {
		if segments[index] != "minutes" || index+1 >= len(segments) {
			continue
		}
		token, unescapeErr := url.PathUnescape(segments[index+1])
		if unescapeErr == nil && minuteTokenPattern.MatchString(token) {
			return token, nil
		}
	}
	return "", permanent("meeting_content.minute_token_missing", "Feishu recording URL does not contain a Minutes token")
}

func meetingContentWaitingStatus(endAt time.Time) (string, string) {
	if time.Now().UTC().Before(endAt.UTC().Add(24 * time.Hour)) {
		return "pending", "pending"
	}
	return "unavailable", "unavailable"
}

func meetingContentResult(session *apiSession, output FetchMeetingContentOutput, responseRef string) connector.TypedResult[FetchMeetingContentOutput] {
	return connector.TypedResult[FetchMeetingContentOutput]{Output: output, ResponseRef: responseRef, SecretUpdates: cloneStringMap(session.updates)}
}
