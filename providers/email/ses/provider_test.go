package ses

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type transportProbe struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
	deadline time.Time
}

func (f *transportProbe) RoundTripHTTP(ctx context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	f.requests = append(f.requests, r)
	f.deadline, _ = ctx.Deadline()
	return f.response, f.err
}
func (*transportProbe) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func request(t *testing.T) connector.CallRequest {
	t.Helper()
	raw, err := json.Marshal(SendEmailInput{To: []string{"recipient@example.test"}, Subject: "设置密码", Text: "邀请链接 https://crm.example/set-password#token=private-fixture <not-html>"})
	if err != nil {
		t.Fatal(err)
	}
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendSecurityEmail.Key, ContractSHA256: SendSecurityEmail.ContractSHA256, Mode: connector.ModeCall, Payload: raw, Connection: connector.Connection{Config: map[string]any{"region": "us-east-1", "from_email": "no-reply@example.test", "from_name": "Aurora", "timeout_seconds": 10}}, Secrets: map[string]string{"access_key_id": "AKIATESTFIXTURE", "secret_access_key": "private-secret-fixture", "session_token": "private-session-fixture"}}
}
func successfulProbe() *transportProbe {
	return &transportProbe{response: connector.HTTPResponse{StatusCode: 200, Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte(`{"MessageId":"ses-fixture-123"}`)}}
}
func TestSESUsesSDKSigningThroughHostTransportAndRequiresReceipt(t *testing.T) {
	f := successfulProbe()
	adapter, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 0 {
		t.Fatal("constructor performed I/O")
	}
	result, err := adapter.Call(t.Context(), request(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.ResponseRef != "ses:ses-fixture-123" || !strings.Contains(string(result.Payload), `"accepted":true`) || len(f.requests) != 1 {
		t.Fatal("missing confirmed SES acceptance")
	}
	r := f.requests[0]
	if r.Method != "POST" || r.URL != "https://email.us-east-1.amazonaws.com/v2/email/outbound-emails" {
		t.Fatalf("not SES v2: %s %s", r.Method, r.URL)
	}
	if len(r.Headers["Authorization"]) != 0 || !strings.Contains(strings.Join(r.SecretHeaders["Authorization"], ""), "/us-east-1/ses/aws4_request") || strings.Join(r.SecretHeaders["X-Amz-Security-Token"], "") != "private-session-fixture" {
		t.Fatal("AWS signature was not kept in secret headers")
	}
	var body struct {
		FromEmailAddress string
		Destination      struct{ ToAddresses []string }
		Content          struct {
			Simple struct {
				Subject struct{ Charset, Data string }
				Body    struct {
					Text, Html struct{ Charset, Data string }
				}
			}
		}
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.FromEmailAddress != "\"Aurora\" <no-reply@example.test>" || len(body.Destination.ToAddresses) != 1 || body.Content.Simple.Subject.Charset != "UTF-8" || body.Content.Simple.Subject.Data != "设置密码" || !strings.Contains(body.Content.Simple.Body.Text.Data, "private-fixture") || !strings.Contains(body.Content.Simple.Body.Html.Data, "&lt;not-html&gt;") {
		t.Fatal("incorrect SES sender, recipient, UTF-8 or text/HTML body")
	}
	if time.Until(f.deadline) > 10*time.Second || time.Until(f.deadline) < 9*time.Second {
		t.Fatal("missing bounded send timeout")
	}
	serialized, _ := json.Marshal(r)
	if strings.Contains(string(serialized), "private-session-fixture") || strings.Contains(string(serialized), "AKIATESTFIXTURE") {
		t.Fatal("AWS auth leaked into serializable transport metadata")
	}
	f.response.Body = []byte(`{}`)
	if _, err := adapter.Call(t.Context(), request(t)); err == nil {
		t.Fatal("missing SES message ID accepted as sent")
	}
}
func TestSESDefaultCredentialChainAndEndpointAreNotReplacedByGlobalURL(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAENVFIXTURE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret-fixture")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "https://attacker.example.test")
	f := successfulProbe()
	adapter, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	r := request(t)
	r.Secrets = nil
	if _, err := adapter.Call(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(f.requests[0].URL, "https://email.us-east-1.amazonaws.com/") || !strings.Contains(strings.Join(f.requests[0].SecretHeaders["Authorization"], ""), "AKIAENVFIXTURE/") {
		t.Fatal("default credential chain or fixed AWS endpoint was bypassed")
	}
}
func TestSESRejectsInvalidInputsBeforeNetworkAndDoesNotRetryFailures(t *testing.T) {
	for _, change := range []func(*connector.CallRequest){
		func(r *connector.CallRequest) { r.Connection.Config["region"] = "bad/region" },
		func(r *connector.CallRequest) { r.Connection.Config["base_url"] = "https://attacker.example.test" },
		func(r *connector.CallRequest) { r.Connection.Config["from_email"] = "invalid" },
		func(r *connector.CallRequest) { r.Connection.Config["timeout_seconds"] = 1.5 },
		func(r *connector.CallRequest) { delete(r.Secrets, "secret_access_key") },
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"to":["one@example.test","two@example.test"],"subject":"x","text":"private"}`)
		},
	} {
		f := successfulProbe()
		adapter, err := New(f)
		if err != nil {
			t.Fatal(err)
		}
		r := request(t)
		change(&r)
		if _, err := adapter.Call(t.Context(), r); err == nil || len(f.requests) != 0 {
			t.Fatal("invalid input reached network")
		}
	}
	for _, failure := range []struct {
		response connector.HTTPResponse
		err      error
		class    connector.ErrorClassification
	}{
		{connector.HTTPResponse{StatusCode: 400, Headers: map[string][]string{"Content-Type": {"application/json"}, "X-Amzn-Errortype": {"MessageRejected"}}, Body: []byte(`{"message":"private-upstream-fixture","__type":"MessageRejected"}`)}, nil, connector.ErrorPermanent},
		{connector.HTTPResponse{StatusCode: 503, Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte(`{"message":"private-upstream-fixture"}`)}, nil, connector.ErrorUncertain},
		{connector.HTTPResponse{}, errors.New("private-upstream-fixture"), connector.ErrorUncertain},
	} {
		f := &transportProbe{response: failure.response, err: failure.err}
		adapter, err := New(f)
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Call(t.Context(), request(t))
		class, _ := connector.ErrorClassificationOf(err)
		if err == nil || class != failure.class || len(f.requests) != 1 {
			t.Fatalf("failure retry/classification=%s attempts=%d err=%v", class, len(f.requests), err)
		}
		for cause := err; cause != nil; cause = errors.Unwrap(cause) {
			if strings.Contains(cause.Error(), "private-") {
				t.Fatal("private failure retained")
			}
		}
	}
}
func TestSESCredentialChainContainerRoleAlsoUsesHostTransport(t *testing.T) {
	// Exercise an IAM-role style endpoint without any static keys or real AWS I/O.
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/missing")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:1234/credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	f := &roleTransport{}
	adapter, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	r := request(t)
	r.Secrets = nil
	if _, err := adapter.Call(t.Context(), r); err != nil {
		t.Fatalf("IAM fixture error=%v role=%d send=%d", err, f.roleCalls, f.sendCalls)
	}
	if f.roleCalls != 1 || f.sendCalls != 1 {
		t.Fatalf("IAM chain not routed through host: role=%d send=%d", f.roleCalls, f.sendCalls)
	}
}

type roleTransport struct{ roleCalls, sendCalls int }

func (f *roleTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	if strings.Contains(r.URL, "/credentials") {
		f.roleCalls++
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"AccessKeyId":"AKIAROLEFIXTURE","SecretAccessKey":"role-secret","Token":"role-session","Expiration":"2099-01-01T00:00:00Z"}`)}, nil
	}
	f.sendCalls++
	if !strings.Contains(strings.Join(r.SecretHeaders["Authorization"], ""), "AKIAROLEFIXTURE/") {
		return connector.HTTPResponse{}, errors.New("role credential not used")
	}
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"MessageId":"role-message"}`)}, nil
}
func (*roleTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
