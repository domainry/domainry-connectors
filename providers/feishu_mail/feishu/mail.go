package feishu

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	stdmail "net/mail"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mail"
	"golang.org/x/net/html"
)

type feishuAddress struct {
	Address string `json:"mail_address"`
	Name    string `json:"name"`
}

type feishuMessage struct {
	Raw           *string         `json:"raw"`
	Subject       *string         `json:"subject"`
	To            []feishuAddress `json:"to"`
	CC            []feishuAddress `json:"cc"`
	BCC           []feishuAddress `json:"bcc"`
	From          *feishuAddress  `json:"head_from"`
	BodyHTML      *string         `json:"body_html"`
	BodyPlainText *string         `json:"body_plain_text"`
	InternalDate  *string         `json:"internal_date"`
	MessageState  *int            `json:"message_state"`
	SMTPMessageID *string         `json:"smtp_message_id"`
	MessageID     *string         `json:"message_id"`
	ThreadID      *string         `json:"thread_id"`
	Attachments   []struct {
		Filename       string `json:"filename"`
		ID             string `json:"id"`
		AttachmentType int    `json:"attachment_type"`
	} `json:"attachments"`
}

type feishuMessagePage struct {
	Items     []string `json:"items"`
	PageToken string   `json:"page_token"`
	HasMore   bool     `json:"has_more"`
}

func (p *provider) mailList(ctx context.Context, request connector.TypedRequest[mail.PageRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mail.MessagesPage](nil, "", nil, permanent("mail.invalid_request", err.Error()))
	}
	return p.mailPage(ctx, request.Connection, request.Secrets, request.Input, mailQuery{})
}

func (p *provider) mailSearch(ctx context.Context, request connector.TypedRequest[mail.SearchRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mail.MessagesPage](nil, "", nil, permanent("mail.invalid_request", err.Error()))
	}
	if request.Input.QuerySyntax != mail.FeishuSyntax {
		return mailResult[mail.MessagesPage](nil, "", nil, permanent("mail.query_syntax_mismatch", "Feishu Mail query syntax is required"))
	}
	query, err := parseMailQuery(request.Input.Query)
	if err != nil {
		return mailResult[mail.MessagesPage](nil, "", nil, permanent("mail.query_invalid", err.Error()))
	}
	return p.mailPage(ctx, request.Connection, request.Secrets, mail.PageRequest{Limit: request.Input.Limit, Cursor: request.Input.Cursor}, query)
}

func (p *provider) mailPage(ctx context.Context, connection connector.Connection, secrets map[string]string, page mail.PageRequest, search mailQuery) (connector.TypedResult[mail.MessagesPage], error) {
	session := p.newAPISession(connection, secrets)
	pageSize := min(page.PageSize(), mailSyncPageSize)
	query := url.Values{"page_size": {strconv.Itoa(pageSize)}}
	if page.Cursor != "" {
		query.Set("page_token", page.Cursor)
	}
	if search.Folder != "" {
		query.Set("folder_id", search.Folder)
	}
	if search.OnlyUnread {
		query.Set("only_unread", "true")
	}
	raw, err := session.call(ctx, "GET", messagePath(connection), query, nil, false)
	if err != nil {
		return mailResult[mail.MessagesPage](nil, raw.ResponseRef, session.updates, err)
	}
	var providerPage feishuMessagePage
	if err = decodeData(raw.Output, &providerPage); err != nil || len(providerPage.Items) > pageSize || providerPage.HasMore && providerPage.PageToken == "" || !providerPage.HasMore && providerPage.PageToken != "" {
		return mailResult[mail.MessagesPage](nil, raw.ResponseRef, session.updates, mailInvalidResponse())
	}
	output := mail.MessagesPage{
		Items:        []mail.Summary{},
		NextCursor:   providerPage.PageToken,
		Complete:     !providerPage.HasMore,
		QuerySyntax:  mail.FeishuSyntax,
		MailboxScope: search.scope(),
	}
	for _, messageID := range providerPage.Items {
		if !mail.ValidID(messageID) {
			return mailResult[mail.MessagesPage](nil, raw.ResponseRef, session.updates, mailInvalidResponse())
		}
		messageRaw, message, fetchErr := session.getMessage(ctx, messageID)
		if fetchErr != nil {
			return mailResult[mail.MessagesPage](nil, messageRaw.ResponseRef, session.updates, fetchErr)
		}
		summary, summaryErr := message.summary(messageID)
		if summaryErr != nil {
			return mailResult[mail.MessagesPage](nil, messageRaw.ResponseRef, session.updates, summaryErr)
		}
		if search.matches(summary) {
			output.Items = append(output.Items, summary)
		}
	}
	if err = output.Validate(page.PageSize()); err != nil {
		return mailResult[mail.MessagesPage](nil, raw.ResponseRef, session.updates, mailInvalidResponse())
	}
	return mailResult(&output, raw.ResponseRef, session.updates, nil)
}

func (p *provider) mailRead(ctx context.Context, request connector.TypedRequest[mail.ReadRequest]) (connector.TypedResult[mail.Message], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mail.Message](nil, "", nil, permanent("mail.invalid_request", err.Error()))
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	raw, message, err := session.getMessage(ctx, request.Input.MessageID)
	if err != nil {
		return mailResult[mail.Message](nil, raw.ResponseRef, session.updates, err)
	}
	summary, err := message.summary(request.Input.MessageID)
	if err != nil {
		return mailResult[mail.Message](nil, raw.ResponseRef, session.updates, err)
	}
	body, err := message.body(request.Input.BodyLimit())
	if err != nil {
		return mailResult[mail.Message](nil, raw.ResponseRef, session.updates, err)
	}
	output := mail.Message{Summary: summary, Body: body}
	if output.Validate(request.Input) != nil {
		return mailResult[mail.Message](nil, raw.ResponseRef, session.updates, mailInvalidResponse())
	}
	return mailResult(&output, raw.ResponseRef, session.updates, nil)
}

func (s *apiSession) getMessage(ctx context.Context, messageID string) (connector.TypedResult[Response], feishuMessage, error) {
	var message feishuMessage
	raw, err := s.call(ctx, "GET", messagePath(s.connection)+"/"+url.PathEscape(messageID), nil, nil, false)
	if err != nil {
		return raw, message, err
	}
	if err = decodeDataField(raw.Output, "message", &message); err != nil {
		return raw, feishuMessage{}, mailInvalidResponse()
	}
	return raw, message, nil
}

func (message feishuMessage) summary(expectedID string) (mail.Summary, error) {
	if message.MessageID == nil || *message.MessageID != expectedID {
		return mail.Summary{}, mailInvalidResponse()
	}
	from := []mail.Address{}
	if message.From != nil {
		from = append(from, message.From.normalized())
	}
	replyTo, rawHeadersKnown := message.replyTo()
	summary := mail.Summary{
		ID:                expectedID,
		ThreadID:          stringValue(message.ThreadID),
		InternetMessageID: stringValue(message.SMTPMessageID),
		Subject:           stringValue(message.Subject),
		From:              from,
		To:                normalizeAddresses(message.To),
		CC:                normalizeAddresses(message.CC),
		ReplyTo:           replyTo,
		MetadataComplete:  message.Subject != nil && message.From != nil && message.To != nil && message.CC != nil && message.MessageState != nil && rawHeadersKnown,
	}
	if message.MessageState != nil {
		draft := *message.MessageState == 3
		summary.IsDraft = &draft
	}
	if message.InternalDate != nil && *message.InternalDate != "" {
		milliseconds, err := strconv.ParseInt(*message.InternalDate, 10, 64)
		if err != nil || milliseconds <= 0 {
			return mail.Summary{}, mailInvalidResponse()
		}
		instant := time.UnixMilli(milliseconds).UTC().Format(time.RFC3339Nano)
		if message.MessageState != nil && *message.MessageState == 2 {
			summary.SentAt = instant
		} else {
			summary.ReceivedAt = instant
		}
	}
	if err := summary.Validate(); err != nil {
		return mail.Summary{}, mailInvalidResponse()
	}
	return summary, nil
}

func (message feishuMessage) replyTo() ([]mail.Address, bool) {
	if message.Raw == nil || *message.Raw == "" {
		return []mail.Address{}, false
	}
	raw, err := decodeBase64URL(*message.Raw)
	if err != nil {
		return nil, false
	}
	parsed, err := stdmail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		return nil, false
	}
	values, err := parsed.Header.AddressList("Reply-To")
	if err != nil {
		if errors.Is(err, stdmail.ErrHeaderNotPresent) {
			return []mail.Address{}, true
		}
		return nil, false
	}
	result := make([]mail.Address, 0, len(values))
	for _, address := range values {
		result = append(result, mail.Address{Name: address.Name, Address: address.Address})
	}
	return result, true
}

func (message feishuMessage) body(limit int) (mail.Body, error) {
	omitted := []string{}
	var encoded string
	if message.BodyPlainText != nil {
		encoded = *message.BodyPlainText
	} else if message.BodyHTML != nil {
		encoded = *message.BodyHTML
		omitted = append(omitted, "html_flattened")
	} else {
		return mail.Body{Text: "", Complete: false, OmittedReasons: []string{"body_unavailable"}}, nil
	}
	raw, err := decodeBase64URL(encoded)
	if err != nil || !utf8.Valid(raw) {
		return mail.Body{}, mailInvalidResponse()
	}
	text := string(raw)
	if message.BodyPlainText == nil {
		text, err = flattenHTML(text)
		if err != nil {
			return mail.Body{}, mailInvalidResponse()
		}
	}
	if len(text) > limit {
		text = truncateUTF8(text, limit)
		omitted = append(omitted, "body_truncated")
	}
	return mail.Body{Text: text, Complete: len(omitted) == 0, OmittedReasons: omitted}, nil
}

func (address feishuAddress) normalized() mail.Address {
	return mail.Address{Name: strings.TrimSpace(address.Name), Address: strings.TrimSpace(address.Address)}
}

func normalizeAddresses(values []feishuAddress) []mail.Address {
	result := make([]mail.Address, 0, len(values))
	for _, value := range values {
		result = append(result, value.normalized())
	}
	return result
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func decodeBase64URL(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
}

func flattenHTML(value string) (string, error) {
	root, err := html.Parse(strings.NewReader(value))
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	var visit func(*html.Node, bool)
	visit = func(node *html.Node, hidden bool) {
		if node.Type == html.ElementNode && (node.Data == "script" || node.Data == "style" || node.Data == "head") {
			hidden = true
		}
		if !hidden && node.Type == html.TextNode {
			text := strings.TrimSpace(node.Data)
			if text != "" {
				if builder.Len() > 0 {
					builder.WriteByte('\n')
				}
				builder.WriteString(text)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child, hidden)
		}
	}
	visit(root, false)
	return builder.String(), nil
}

func truncateUTF8(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

type mailQuery struct {
	Folder     string
	OnlyUnread bool
	Terms      []mailQueryTerm
}

type mailQueryTerm struct{ Field, Value string }

func parseMailQuery(value string) (mailQuery, error) {
	result := mailQuery{}
	for _, token := range strings.Fields(value) {
		field, term, found := strings.Cut(token, ":")
		if !found {
			result.Terms = append(result.Terms, mailQueryTerm{Value: strings.ToLower(token)})
			continue
		}
		field, term = strings.ToLower(field), strings.TrimSpace(term)
		if term == "" {
			return mailQuery{}, errors.New("Feishu Mail query contains an empty value")
		}
		switch field {
		case "folder":
			if result.Folder != "" || !mail.ValidID(term) {
				return mailQuery{}, errors.New("Feishu Mail query contains an invalid folder")
			}
			result.Folder = term
		case "unread":
			if !strings.EqualFold(term, "true") || result.OnlyUnread {
				return mailQuery{}, errors.New("Feishu Mail only supports unread:true")
			}
			result.OnlyUnread = true
		case "subject", "from", "to":
			result.Terms = append(result.Terms, mailQueryTerm{Field: field, Value: strings.ToLower(term)})
		default:
			return mailQuery{}, fmt.Errorf("Feishu Mail query field %q is not supported", field)
		}
	}
	return result, nil
}

func (query mailQuery) scope() string {
	if query.Folder != "" {
		return "folder:" + query.Folder
	}
	return "mailbox"
}

func (query mailQuery) matches(summary mail.Summary) bool {
	for _, term := range query.Terms {
		var values []string
		switch term.Field {
		case "subject":
			values = []string{summary.Subject}
		case "from":
			values = addressText(summary.From)
		case "to":
			values = append(addressText(summary.To), addressText(summary.CC)...)
		default:
			values = []string{summary.Subject}
			values = append(values, addressText(summary.From)...)
			values = append(values, addressText(summary.To)...)
			values = append(values, addressText(summary.CC)...)
		}
		matched := false
		for _, value := range values {
			matched = matched || strings.Contains(strings.ToLower(value), term.Value)
		}
		if !matched {
			return false
		}
	}
	return true
}

func addressText(addresses []mail.Address) []string {
	values := make([]string, 0, len(addresses)*2)
	for _, address := range addresses {
		values = append(values, address.Name, address.Address)
	}
	return values
}

func decodeData(input Response, output any) error {
	data, ok := input["data"]
	if !ok || data == nil {
		return errors.New("missing response data")
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, output)
}

func decodeDataField(input Response, key string, output any) error {
	var data map[string]json.RawMessage
	if err := decodeData(input, &data); err != nil || len(data[key]) == 0 || string(data[key]) == "null" {
		return errors.New("missing response field")
	}
	return json.Unmarshal(data[key], output)
}

func mailInvalidResponse() error {
	return permanent("mail.invalid_response", "Feishu Mail response is invalid")
}

func mailResult[O any](output *O, responseRef string, updates map[string]string, err error) (connector.TypedResult[O], error) {
	result := connector.TypedResult[O]{ResponseRef: responseRef, SecretUpdates: cloneStrings(updates)}
	if output != nil {
		result.Output = *output
	}
	return result, err
}
