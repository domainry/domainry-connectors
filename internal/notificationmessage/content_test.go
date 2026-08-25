package notificationmessage

import "testing"

func TestDecodeRequiresVersionedImmutableTemplateSnapshot(t *testing.T) {
	value := map[string]any{
		"schema_version": 1, "message": "ready", "product_name": "Domainry",
		"template": map[string]any{"key": "workflow.ready", "version": 2, "locale": "en-US", "content_hash": "content", "variables_hash": "variables"},
		"actions":  []any{map[string]any{"label": "Open", "url": "https://example.test/open", "style": "primary"}},
	}
	content, err := Decode(value)
	if err != nil || content.Template.Key != "workflow.ready" || len(content.Actions) != 1 {
		t.Fatalf("content=%+v err=%v", content, err)
	}
}

func TestDecodeRejectsProviderNativeAndIncompleteContent(t *testing.T) {
	for _, value := range []map[string]any{
		{"schema_version": 1, "message": "ready", "product_name": "Domainry", "provider_payload": map[string]any{}},
		{"schema_version": 2, "message": "ready", "template": map[string]any{}},
		{"schema_version": 1, "message": "ready", "product_name": "Domainry", "template": map[string]any{"key": "notice", "version": 1, "locale": "en-US", "content_hash": "content", "variables_hash": "variables"}, "actions": []any{map[string]any{"label": "Open"}}},
	} {
		if _, err := Decode(value); err == nil {
			t.Fatalf("expected invalid content: %+v", value)
		}
	}
}
