package shopify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"testing"
)

type transport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *transport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = r
	return t.response, t.err
}
func (*transport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestEndpointTransportAndWebhook(t *testing.T) {
	for _, config := range []Config{{ShopDomain: "store.myshopify.com", APIVersion: "2026-04"}, {ShopDomain: "localhost:8080"}} {
		if err := Validate(config); err != nil {
			t.Fatalf("valid=%+v err=%v", config, err)
		}
	}
	for _, config := range []Config{{ShopDomain: "example.com"}, {ShopDomain: "store.myshopify.com.evil.test"}, {ShopDomain: "store.myshopify.com/path"}, {ShopDomain: "store.myshopify.com", APIVersion: "2026-13"}} {
		if Validate(config) == nil {
			t.Fatalf("invalid accepted=%+v", config)
		}
	}
	tr := &transport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{}}`)}}
	if _, _, err := Execute(t.Context(), tr, Config{ShopDomain: "localhost:8080"}, "token", "query", nil, false); err != nil {
		t.Fatal(err)
	}
	if tr.request.SecretHeaders["X-Shopify-Access-Token"][0] != "token" || tr.request.Headers["X-Shopify-Access-Token"] != nil {
		t.Fatalf("request=%+v", tr.request)
	}
	body := []byte(`{"id":42}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	event, err := VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Headers: map[string][]string{"X-Shopify-Hmac-Sha256": {base64.StdEncoding.EncodeToString(mac.Sum(nil))}, "X-Shopify-Topic": {"orders/create"}, "X-Shopify-Webhook-Id": {"delivery"}}, Body: body})
	if err != nil || event.Topic != "orders/create" || event.DeliveryID != "delivery" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
