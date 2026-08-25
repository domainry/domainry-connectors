package notificationmessage

import (
	"encoding/json"
	"testing"
)

func TestCompileOwnsEveryNotificationDeliveryProvider(t *testing.T) {
	content := Content{
		SchemaVersion: SchemaVersion, Subject: "Subject", Title: "Title", Text: "Text", Markdown: "**Text**", Message: "Title\nText", ProductName: "Domainry",
		Facts: []Fact{{Key: "Status", Value: "Ready"}}, Actions: []Action{{Label: "Open", URL: "https://example.test/open", Style: "primary"}},
		Template: TemplateSnapshot{Key: "workflow.ready", Version: 2, Locale: "en-US", ContentHash: "content", VariablesHash: "variables"},
	}
	for _, provider := range []string{"slack", "feishu", "dingtalk", "enterprise_wechat", "discord", "google_workspace", "teams", "microsoft_365", "line_works", "line", "meta_cloud_api"} {
		t.Run(provider, func(t *testing.T) {
			payload, err := Compile(provider, content)
			if err != nil || len(payload) == 0 {
				t.Fatalf("payload=%+v err=%v", payload, err)
			}
			if _, err := json.Marshal(payload); err != nil {
				t.Fatalf("payload is not JSON-safe: %v", err)
			}
		})
	}
}

func TestCompileWhatsAppApprovedTemplate(t *testing.T) {
	payload, err := Compile("meta_cloud_api", Content{Message: "fallback", ProductName: "Domainry", ProviderTemplate: &ProviderTemplate{
		Name: "approval", Language: "zh_CN", Components: []ProviderTemplateComponent{{Type: "button", SubType: "quick_reply", Index: "0", Parameters: []string{"approve"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(payload)
	const want = `{"template":{"components":[{"index":"0","parameters":[{"payload":"approve","type":"payload"}],"sub_type":"quick_reply","type":"button"}],"language":{"code":"zh_CN"},"name":"approval"},"type":"template"}`
	if string(encoded) != want {
		t.Fatalf("payload=%s", encoded)
	}
}

func TestCompileRejectsUnknownProvider(t *testing.T) {
	if _, err := Compile("unknown", Content{}); err == nil {
		t.Fatal("expected unsupported provider error")
	}
}

func TestResolveProviderPayloadPrefersPortableContentAndClonesLegacy(t *testing.T) {
	content := map[string]any{"schema_version": 1, "message": "ready", "product_name": "Domainry", "template": map[string]any{"key": "notice", "version": 1, "locale": "en-US", "content_hash": "content", "variables_hash": "variables"}}
	payload, err := ResolveProviderPayload("slack", content, map[string]any{"legacy": true})
	if err != nil || payload["legacy"] != nil || payload["text"] != "ready" {
		t.Fatalf("payload=%+v err=%v", payload, err)
	}
	legacy := map[string]any{"text": "old"}
	cloned, err := ResolveProviderPayload("slack", nil, legacy)
	if err != nil {
		t.Fatal(err)
	}
	cloned["text"] = "changed"
	if legacy["text"] != "old" {
		t.Fatal("legacy payload was mutated")
	}
}
