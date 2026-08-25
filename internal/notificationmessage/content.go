// Package notificationmessage defines the provider-neutral rich-message value
// accepted by outbound messaging Connectors. It deliberately contains no
// Runtime, template-engine, recipient-resolution, or delivery-policy types.
package notificationmessage

import (
	"encoding/json"
	"fmt"
	"strings"
)

const SchemaVersion = 1

// Content is an immutable rendering snapshot. Connectors may translate it to
// their provider's native wire shape without loading mutable notification data.
type Content struct {
	SchemaVersion    int               `json:"schema_version"`
	Subject          string            `json:"subject,omitempty"`
	Title            string            `json:"title,omitempty"`
	Text             string            `json:"text,omitempty"`
	HTML             string            `json:"html,omitempty"`
	Markdown         string            `json:"markdown,omitempty"`
	Message          string            `json:"message,omitempty"`
	ProductName      string            `json:"product_name,omitempty"`
	Facts            []Fact            `json:"facts,omitempty"`
	Actions          []Action          `json:"actions,omitempty"`
	ProviderTemplate *ProviderTemplate `json:"provider_template,omitempty"`
	Template         TemplateSnapshot  `json:"template"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
}

type Fact struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Action struct {
	Label string `json:"label"`
	URL   string `json:"url"`
	Style string `json:"style,omitempty"`
}

type ProviderTemplate struct {
	Name       string                      `json:"name"`
	Language   string                      `json:"language"`
	Components []ProviderTemplateComponent `json:"components,omitempty"`
}

type ProviderTemplateComponent struct {
	Type       string   `json:"type"`
	SubType    string   `json:"sub_type,omitempty"`
	Index      string   `json:"index,omitempty"`
	Parameters []string `json:"parameters,omitempty"`
}

type TemplateSnapshot struct {
	Key           string `json:"key"`
	Version       int    `json:"version"`
	Locale        string `json:"locale"`
	ContentHash   string `json:"content_hash"`
	VariablesHash string `json:"variables_hash"`
}

// Decode validates the closed wire shape used between Notification and a
// Connector. Provider-native payloads are intentionally not accepted here.
func Decode(value map[string]any) (Content, error) {
	if len(value) == 0 {
		return Content{}, fmt.Errorf("notification_content is required")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return Content{}, fmt.Errorf("encode notification_content: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var content Content
	if err := decoder.Decode(&content); err != nil {
		return Content{}, fmt.Errorf("decode notification_content: %w", err)
	}
	if content.SchemaVersion != SchemaVersion {
		return Content{}, fmt.Errorf("notification_content schema_version must be %d", SchemaVersion)
	}
	if strings.TrimSpace(content.Message) == "" {
		return Content{}, fmt.Errorf("notification_content message is required")
	}
	if strings.TrimSpace(content.ProductName) == "" {
		return Content{}, fmt.Errorf("notification_content product_name is required")
	}
	if strings.TrimSpace(content.Template.Key) == "" || content.Template.Version <= 0 || strings.TrimSpace(content.Template.Locale) == "" || strings.TrimSpace(content.Template.ContentHash) == "" || strings.TrimSpace(content.Template.VariablesHash) == "" {
		return Content{}, fmt.Errorf("notification_content template snapshot is incomplete")
	}
	for _, action := range content.Actions {
		if strings.TrimSpace(action.Label) == "" || strings.TrimSpace(action.URL) == "" {
			return Content{}, fmt.Errorf("notification_content action label and url are required")
		}
	}
	return content, nil
}
