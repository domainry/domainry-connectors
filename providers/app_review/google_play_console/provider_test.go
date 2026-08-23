package googleplayconsole

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestListAndReplyUseTypedContractsAndSecretHeader(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"reviews":[{"reviewId":"one"}]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), callRequest(ListReviews, mustJSON(ListReviewsInput{PageSize: 25, After: "next", TranslationLanguage: "zh"})))
	if err != nil || !strings.Contains(string(result.Payload), "one") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer token" || !strings.Contains(request.URL, "maxResults=25") || !strings.Contains(request.URL, "translationLanguage=zh") {
		t.Fatalf("request=%+v", request)
	}
	transport.response.Body = []byte(`{"reviewId":"one"}`)
	reply, err := adapter.Call(t.Context(), callRequest(ReplyReview, mustJSON(ReplyReviewInput{ReviewID: "one/two", ReplyText: "Thank you"})))
	if err != nil || reply.ResponseRef != "google_play:review:one" {
		t.Fatalf("reply=%+v error=%v", reply, err)
	}
	request = transport.requests[1]
	if request.Method != http.MethodPost || !strings.Contains(request.URL, "/reviews/one%2Ftwo:reply") || string(request.Body) != `{"replyText":"Thank you"}` {
		t.Fatalf("request=%+v", request)
	}
}

func TestWriteAmbiguityAndReadRetriesAreDistinct(t *testing.T) {
	transport := &recordingTransport{err: errors.New("connection reset")}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Call(t.Context(), callRequest(ListReviews, mustJSON(ListReviewsInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("read error=%v class=%q", err, class)
	}
	_, err = adapter.Call(t.Context(), callRequest(ReplyReview, mustJSON(ReplyReviewInput{ReviewID: "one", ReplyText: "reply"})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("write error=%v class=%q", err, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusInternalServerError, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), callRequest(ReplyReview, mustJSON(ReplyReviewInput{ReviewID: "one", ReplyText: "reply"})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("write 500=%v class=%q", err, class)
	}
}

func TestValidationProbeAndUnknownInputFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"reviews":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	for _, payload := range [][]byte{[]byte(`{"unknown":true}`), mustJSON(ListReviewsInput{PageSize: 101})} {
		if _, err := adapter.Call(t.Context(), callRequest(ListReviews, payload)); err == nil {
			t.Fatal("invalid list input accepted")
		}
	}
	if _, err := adapter.Call(t.Context(), callRequest(ReplyReview, mustJSON(ReplyReviewInput{ReviewID: "one"}))); err == nil {
		t.Fatal("incomplete reply accepted")
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"package_name": "com.example", "base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
	request := callRequest(ListReviews, mustJSON(ListReviewsInput{}))
	request.Secrets = nil
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("missing token accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "package_name": "com.example"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
