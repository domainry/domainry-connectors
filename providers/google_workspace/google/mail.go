package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connectors/internal/mailpaging"
)

var (
	MailList   = mailRead[mail.PageRequest, mail.MessagesPage](mail.ListOperationKey)
	MailSearch = mailRead[mail.SearchRequest, mail.MessagesPage](mail.SearchOperationKey)
	MailRead   = mailRead[mail.ReadRequest, mail.Message](mail.ReadOperationKey)
)

func mailRead[I, O any](key string) connector.CallOperation[I, O] {
	return connector.CallOperation[I, O]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: mail.OperationSHA256(key), Reliability: readReliability()}
}

// Call-local credentials ensure metadata fan-out and body part reads use the
// most recent rotation without mutating the host's input map.
type mailSession struct {
	p          *provider
	connection connector.Connection
	secrets    map[string]string
	state      connector.TypedResult[Response]
}

func newMailSession(p *provider, c connector.Connection, secrets map[string]string) *mailSession {
	return &mailSession{p: p, connection: c, secrets: cloneStrings(secrets)}
}
func (s *mailSession) get(ctx context.Context, path string, q url.Values, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := s.p.executeWithRefresh(ctx, s.connection, s.secrets, http.MethodGet, gmailBase(s.connection)+"/gmail/v1/users/me/"+path, q, nil, false)
	s.state.ResponseRef, s.state.ResourceHealth = r.ResponseRef, r.ResourceHealth
	for k, v := range r.SecretUpdates {
		if s.state.SecretUpdates == nil {
			s.state.SecretUpdates = map[string]string{}
		}
		s.state.SecretUpdates[k], s.secrets[k] = v, v
	}
	if err != nil {
		return err
	}
	b, err := json.Marshal(r.Output)
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		return mailInvalidResponse()
	}
	return nil
}
func mailResult[T any](s *mailSession, out T, err error) (connector.TypedResult[T], error) {
	return connector.TypedResult[T]{Output: out, ResponseRef: s.state.ResponseRef, SecretUpdates: s.state.SecretUpdates, ResourceHealth: s.state.ResourceHealth}, err
}
func mailInvalidResponse() error { return permanent("mail.invalid_response", "invalid mail response") }

func (p *provider) mailList(ctx context.Context, r connector.TypedRequest[mail.PageRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	s := newMailSession(p, r.Connection, r.Secrets)
	if err := r.Input.Validate(); err != nil {
		return mailResult(s, mail.MessagesPage{}, permanent("mail.invalid_request", err.Error()))
	}
	out, err := s.list(ctx, r.Input, "")
	return mailResult(s, out, err)
}
func (p *provider) mailSearch(ctx context.Context, r connector.TypedRequest[mail.SearchRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	s := newMailSession(p, r.Connection, r.Secrets)
	if err := r.Input.Validate(); err != nil {
		return mailResult(s, mail.MessagesPage{}, permanent("mail.invalid_request", err.Error()))
	}
	if r.Input.QuerySyntax != mail.GmailSyntax {
		return mailResult(s, mail.MessagesPage{}, permanent("mail.query_syntax_mismatch", "Gmail search syntax is required"))
	}
	out, err := s.list(ctx, mail.PageRequest{Limit: r.Input.Limit, Cursor: r.Input.Cursor}, r.Input.Query)
	return mailResult(s, out, err)
}
func (s *mailSession) list(ctx context.Context, r mail.PageRequest, query string) (mail.MessagesPage, error) {
	var out mail.MessagesPage
	q := url.Values{"maxResults": {strconv.Itoa(r.PageSize())}, "includeSpamTrash": {"false"}, "fields": {"messages(id,threadId),nextPageToken,resultSizeEstimate"}}
	set(q, "q", query)
	scope := mailpaging.Scope(s.connection.WorkspaceID, s.connection.Key, gmailBase(s.connection), q.Encode())
	cursor, err := mailpaging.Decode(scope, r.Cursor)
	if err != nil {
		return out, permanent("mail.invalid_cursor", "invalid mail continuation")
	}
	set(q, "pageToken", cursor.Token)
	var page struct {
		Messages []struct {
			ID       string `json:"id"`
			ThreadID string `json:"threadId"`
		} `json:"messages"`
		Next     string `json:"nextPageToken"`
		Estimate *int64 `json:"resultSizeEstimate"`
	}
	if err = s.get(ctx, "messages", q, &page); err != nil {
		return out, err
	}
	// Google omits an empty messages array, but an absent array and absent
	// estimate together are not evidence of a successful empty query.
	if len(page.Messages) > r.PageSize() || page.Messages == nil && (page.Estimate == nil || *page.Estimate != 0) || page.Estimate != nil && *page.Estimate < 0 || page.Next != "" && page.Next == cursor.Token {
		return out, mailInvalidResponse()
	}
	out = mail.MessagesPage{Items: []mail.Summary{}, Complete: page.Next == "", QuerySyntax: mail.GmailSyntax, MailboxScope: "mailbox_excluding_spam_trash"}
	if page.Next != "" {
		out.NextCursor, err = mailpaging.Encode(scope, page.Next, 0)
		if err != nil {
			return mail.MessagesPage{}, mailInvalidResponse()
		}
	}
	seen := map[string]bool{}
	for _, item := range page.Messages {
		if !mail.ValidID(item.ID) || !mail.ValidID(item.ThreadID) || seen[item.ID] {
			return mail.MessagesPage{}, mailInvalidResponse()
		}
		seen[item.ID] = true
		var message googleMailMessage
		q := url.Values{"format": {"metadata"}, "metadataHeaders": {"From", "To", "Cc", "Reply-To", "Subject", "Date", "Message-ID"}, "fields": {"id,threadId,labelIds,internalDate,payload(headers)"}}
		if err = s.get(ctx, "messages/"+url.PathEscape(item.ID), q, &message); err != nil {
			return mail.MessagesPage{}, err
		}
		if message.ID != item.ID || message.ThreadID != item.ThreadID {
			return mail.MessagesPage{}, mailInvalidResponse()
		}
		summary, err := message.summary()
		if err != nil {
			return mail.MessagesPage{}, err
		}
		out.Items = append(out.Items, summary)
	}
	if out.Validate(r.PageSize()) != nil {
		return mail.MessagesPage{}, mailInvalidResponse()
	}
	return out, nil
}
func (p *provider) mailRead(ctx context.Context, r connector.TypedRequest[mail.ReadRequest]) (connector.TypedResult[mail.Message], error) {
	s := newMailSession(p, r.Connection, r.Secrets)
	var out mail.Message
	if err := r.Input.Validate(); err != nil {
		return mailResult(s, out, permanent("mail.invalid_request", err.Error()))
	}
	var message googleMailMessage
	if err := s.get(ctx, "messages/"+url.PathEscape(r.Input.MessageID), url.Values{"format": {"full"}, "fields": {"id,threadId,labelIds,internalDate,payload"}}, &message); err != nil {
		return mailResult(s, out, err)
	}
	if message.ID != r.Input.MessageID {
		return mailResult(s, out, mailInvalidResponse())
	}
	summary, err := message.summary()
	if err != nil {
		return mailResult(s, out, err)
	}
	body, err := s.body(ctx, message.ID, message.Payload, r.Input.BodyLimit())
	if err != nil {
		return mailResult(s, out, err)
	}
	out = mail.Message{Summary: summary, Body: body}
	if out.Validate(r.Input) != nil {
		return mailResult(s, mail.Message{}, mailInvalidResponse())
	}
	return mailResult(s, out, nil)
}
