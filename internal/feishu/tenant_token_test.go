package feishu

import (
	"context"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = request
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestFetchTenantTokenKeepsCredentialsRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"tenant-token","expire":7200}`)}}
	token, err := FetchTenantToken(t.Context(), transport, TenantTokenRequest{Endpoint: "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", AppID: "cli-secret-id", AppSecret: "app-secret", ErrorPrefix: "calendar"})
	if err != nil || token.AccessToken != "tenant-token" || token.ExpiresInSeconds != 7200 {
		t.Fatalf("token=%+v err=%v", token, err)
	}
	public := transport.request.URL + string(transport.request.Body)
	if strings.Contains(public, "cli-secret-id") || strings.Contains(public, "app-secret") || transport.request.SecretJSON["app_id"] != "cli-secret-id" || transport.request.SecretJSON["app_secret"] != "app-secret" {
		t.Fatalf("request=%+v", transport.request)
	}
}

func TestFetchTenantTokenClassifiesFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		transport *recordingTransport
		want      connector.ErrorClassification
	}{{"network", &recordingTransport{err: errors.New("reset")}, connector.ErrorRetryable}, {"rate limit", &recordingTransport{response: connector.HTTPResponse{StatusCode: 429}}, connector.ErrorRetryable}, {"server", &recordingTransport{response: connector.HTTPResponse{StatusCode: 502}}, connector.ErrorRetryable}, {"rejected", &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":10003,"msg":"invalid"}`)}}, connector.ErrorPermanent}, {"bad response", &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}}, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			_, err := FetchTenantToken(t.Context(), test.transport, TenantTokenRequest{Endpoint: "http://localhost/token", AppID: "app", AppSecret: "secret"})
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != test.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, test.want)
			}
		})
	}
}
