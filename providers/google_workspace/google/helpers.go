package google

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"mime"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
)

func (p *provider) gmailMessage(input GmailSendMessageInput) (map[string]any, error) {
	recipients, err := gmailRecipients(input.To)
	if err != nil || len(recipients) == 0 {
		return nil, permanent("gmail.recipients_required", "valid recipients are required")
	}
	subject, text, messageID := cleanHeader(input.Subject), input.Text, cleanHeader(input.RFCMessageID)
	if subject == "" || strings.TrimSpace(text) == "" || !validMessageID(messageID) {
		return nil, permanent("gmail.message_fields_required", "subject, text and valid rfc_message_id are required")
	}
	date := cleanHeader(input.Date)
	if date == "" {
		date = p.now().UTC().Format("Mon, 02 Jan 2006 15:04:05 -0700")
	}
	lines := []string{"To: " + strings.Join(recipients, ", "), "Subject: " + mime.QEncoding.Encode("UTF-8", subject), "Date: " + date, "Message-ID: " + messageID, "MIME-Version: 1.0", "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: 8bit"}
	if reply := cleanHeader(input.InReplyTo); reply != "" {
		if !validMessageID(reply) {
			return nil, permanent("gmail.reply_identity_invalid", "in_reply_to is invalid")
		}
		lines = append(lines, "In-Reply-To: "+reply)
	}
	if references := cleanHeader(input.References); references != "" {
		for _, reference := range strings.Fields(references) {
			if !validMessageID(reference) {
				return nil, permanent("gmail.reply_identity_invalid", "references are invalid")
			}
		}
		lines = append(lines, "References: "+references)
	}
	raw := strings.Join(lines, "\r\n") + "\r\n\r\n" + strings.ReplaceAll(text, "\r\n", "\n")
	result := map[string]any{"raw": base64.RawURLEncoding.EncodeToString([]byte(raw))}
	if thread := strings.TrimSpace(input.ThreadID); thread != "" {
		result["threadId"] = thread
	}
	return result, nil
}
func gmailRecipients(raw any) ([]string, error) {
	values := []string{}
	switch typed := raw.(type) {
	case string:
		values = append(values, typed)
	case []string:
		values = append(values, typed...)
	case []any:
		for _, item := range typed {
			value, ok := item.(string)
			if !ok {
				return nil, errors.New("recipient must be text")
			}
			values = append(values, value)
		}
	default:
		return nil, errors.New("recipient list is required")
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		address, err := mail.ParseAddress(strings.TrimSpace(value))
		if err != nil || strings.ContainsAny(address.Address, "\r\n") {
			return nil, errors.New("recipient is invalid")
		}
		result = append(result, address.String())
	}
	return result, nil
}
func cleanHeader(value string) string {
	if strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return strings.TrimSpace(value)
}
func validMessageID(value string) bool {
	return strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") && strings.Contains(value, "@") && !strings.ContainsAny(value, "\r\n \t")
}
func pageLimit(value int) (int, error) {
	if value == 0 {
		return 100, nil
	}
	if value < 1 || value > 1000 {
		return 0, permanent("limit_invalid", "limit must be between 1 and 1000")
	}
	return value, nil
}
func set(values url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values.Set(key, value)
	}
}
func resource(project, kind, id string) (string, error) {
	project, id = strings.TrimSpace(project), strings.TrimSpace(id)
	if project == "" || id == "" {
		return "", permanent("pubsub.resource_identity_required", "project and resource IDs are required")
	}
	return "projects/" + url.PathEscape(project) + "/" + kind + "s/" + url.PathEscape(id), nil
}
func topicResource(project, id string) (string, error) { return resource(project, "topic", id) }
func subscriptionResource(project, id string) (string, error) {
	return resource(project, "subscription", id)
}
func cleanIDs(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}
func apiBase(c connector.Connection) string {
	return strings.TrimRight(configDefault(c, "api_base_url", defaultAPIBaseURL), "/")
}
func gmailBase(c connector.Connection) string {
	return strings.TrimRight(configDefault(c, "gmail_base_url", defaultGmailBaseURL), "/")
}
func pubsubBase(c connector.Connection) string {
	return strings.TrimRight(configDefault(c, "pubsub_base_url", defaultPubSubBaseURL), "/")
}
func tokenURL(c connector.Connection) string { return configDefault(c, "token_url", defaultTokenURL) }
func config(c connector.Connection, key string) string {
	value, _ := c.Config[key].(string)
	return strings.TrimSpace(value)
}
func configDefault(c connector.Connection, key, fallback string) string {
	if value := config(c, key); value != "" {
		return value
	}
	return fallback
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := strconv.Atoi(typed.String()); err == nil {
			return parsed
		}
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func loopback(host string) bool              { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(code, message string) error {
	return connector.PermanentError("google."+code, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("google."+suffix, cause)
	}
	return connector.RetryableError("google."+suffix, cause)
}
