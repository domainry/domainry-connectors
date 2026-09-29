package feishucalendar

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestFetchMeetingContentResolvesRecordingMinutesAndSpeakerTranscript(t *testing.T) {
	transcript := "1\n00:00:01,000 --> 00:00:03,000\nAlice: Confirm the budget.\n"
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":false,"meeting_briefs":[{"id":"meeting-42","meeting_no":"337736498","start_time":"1600000000","end_time":"1600003600","calendar_event_id":"event-42"}]}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"recording":{"id":"recording-42","meeting_id":"meeting-42","url":"https://tenant.feishu.cn/minutes/obcnminutes42","duration":"3600000"}}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"minute":{"token":"obcnminutes42","title":"Customer discovery","duration":"3600000","url":"https://tenant.feishu.cn/minutes/obcnminutes42","note_id":"note-42"}}}`)},
		{StatusCode: http.StatusOK, Headers: map[string][]string{"Content-Type": {"application/x-subrip"}}, Body: []byte(transcript)},
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(FetchMeetingContentInput{
		MeetingNo: "337736498", CalendarEventID: "event-42", Start: "2020-09-13T12:26:40Z", End: "2020-09-13T13:26:40Z",
	})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: FetchMeetingContent.Key,
		ContractSHA256: FetchMeetingContent.ContractSHA256, Mode: connector.ModeCall, Connection: oauthConnection(),
		Secrets: map[string]string{"access_token": "token"}, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output FetchMeetingContentOutput
	if err = json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(transcript))
	wantHash := hex.EncodeToString(digest[:])
	if output.Status != "ready" || output.RecordingStatus != "ready" || output.MeetingID != "meeting-42" || output.RecordingID != "recording-42" || output.MinuteToken != "obcnminutes42" || output.MinuteURL == "" || output.NoteID != "note-42" || output.Transcript != transcript || output.TranscriptFormat != "srt" || output.TranscriptSHA256 != wantHash || !output.SpeakerPreserved || !output.TimestampPreserved {
		t.Fatalf("output=%+v", output)
	}
	if len(transport.requests) != 4 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	if !strings.Contains(transport.requests[0].URL, "/open-apis/vc/v1/meetings/list_by_no?") || !strings.Contains(transport.requests[0].URL, "meeting_no=337736498") {
		t.Fatalf("meeting lookup=%s", transport.requests[0].URL)
	}
	if transport.requests[1].URL != "http://localhost:8080/open-apis/vc/v1/meetings/meeting-42/recording" {
		t.Fatalf("recording URL=%s", transport.requests[1].URL)
	}
	if transport.requests[2].URL != "http://localhost:8080/open-apis/minutes/v1/minutes/obcnminutes42" {
		t.Fatalf("minute URL=%s", transport.requests[2].URL)
	}
	transcriptURL := transport.requests[3].URL
	for _, query := range []string{"file_format=srt", "need_speaker=true", "need_timestamp=true"} {
		if !strings.Contains(transcriptURL, query) {
			t.Fatalf("transcript URL=%s", transcriptURL)
		}
	}
}

func TestFetchMeetingContentReturnsPendingBeforeMeetingExists(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":false,"meeting_briefs":[]}}`)}}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(FetchMeetingContentInput{MeetingNo: "337736498", CalendarEventID: "event-future", Start: "2099-09-26T01:00:00Z", End: "2099-09-26T02:00:00Z"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: FetchMeetingContent.Key,
		ContractSHA256: FetchMeetingContent.ContractSHA256, Mode: connector.ModeCall, Connection: oauthConnection(),
		Secrets: map[string]string{"access_token": "token"}, Payload: payload,
	})
	var output FetchMeetingContentOutput
	if err != nil || json.Unmarshal(result.Payload, &output) != nil || output.Status != "pending" || output.RecordingStatus != "pending" || output.Transcript != "" {
		t.Fatalf("output=%+v payload=%s err=%v", output, result.Payload, err)
	}
}

func TestMeetingContentOAuthScopesCoverCalendarMeetingRecordingAndMinutes(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	scopes, declared := connector.ResolveOAuthOperationScopes(adapter, FetchMeetingContent.Key)
	if !declared || len(scopes) != 2 {
		t.Fatalf("scopes=%v declared=%v", scopes, declared)
	}
	for _, alternative := range scopes {
		joined := " " + strings.Join(alternative, " ") + " "
		for _, required := range []string{" vc:meeting:readonly ", " vc:record:readonly "} {
			if !strings.Contains(joined, required) {
				t.Fatalf("alternative %v lacks %s", alternative, required)
			}
		}
	}
}
