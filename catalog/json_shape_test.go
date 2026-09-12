package catalog

import "testing"

func TestProviderJSONShapeIsDeclaredAndEnforced(t *testing.T) {
	for _, tc := range []struct {
		shape   string
		value   any
		allowed bool
	}{
		{"array", []string{"example.com"}, true}, {"array", []any{"example.com"}, true},
		{"object", map[string]any{"key": "value"}, true},
		{"array", map[string]any{}, false}, {"object", []any{}, false},
		{"array", `["example.com"]`, false}, {"array", nil, false}, {"object", nil, false}, {"unknown", []any{}, false},
	} {
		field := FieldSchema{Type: "json", Config: map[string]any{"json_shape": tc.shape}}
		if got := providerConfigFieldValueMatchesType(tc.value, field); got != tc.allowed {
			t.Fatalf("shape %s value %T: %v", tc.shape, tc.value, got)
		}
	}
	documents, err := DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range documents {
		if doc.Key != "web" {
			continue
		}
		if len(doc.Operations) != 2 || len(doc.Providers) != 1 {
			t.Fatal("web definition incomplete")
		}
		config := ApplyProviderConfigDefaults(doc, "llm_proxy", map[string]any{"base_url": "https://proxy.example.com", "allowed_source_hosts": []string{"example.com"}})
		if err := ValidateProviderConfig(doc, "llm_proxy", config); err != nil {
			t.Fatal(err)
		}
		config["allowed_source_hosts"] = map[string]any{"host": "example.com"}
		if err := ValidateProviderConfig(doc, "llm_proxy", config); err == nil {
			t.Fatal("source object accepted as array")
		}
		return
	}
	t.Fatal("web definition missing")
}
