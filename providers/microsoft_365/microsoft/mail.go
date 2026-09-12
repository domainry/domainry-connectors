package microsoft

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
)

var (
	MailList   = mailRead[mail.PageRequest, mail.MessagesPage](mail.ListOperationKey)
	MailSearch = mailRead[mail.SearchRequest, mail.MessagesPage](mail.SearchOperationKey)
	MailRead   = mailRead[mail.ReadRequest, mail.Message](mail.ReadOperationKey)
)

func mailRead[I, O any](key string) connector.CallOperation[I, O] {
	return connector.CallOperation[I, O]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: mail.OperationSHA256(key), Reliability: readReliability()}
}

func mailInvalidResponse() error { return permanent("mail.invalid_response", "invalid mail response") }
func decodeMailResponse(raw Response, out any) error {
	b, err := json.Marshal(raw)
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		return mailInvalidResponse()
	}
	return nil
}

const mailMetadataFields = "id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,replyTo,receivedDateTime,sentDateTime,isRead,isDraft"
const mailSearchMaximum = 1000

func (p *provider) mailList(ctx context.Context, r connector.TypedRequest[mail.PageRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, mail.MessagesPage{}, permanent("mail.invalid_request", err.Error()))
	}
	return p.mailPage(ctx, r.Connection, r.Secrets, r.Input, "")
}
func (p *provider) mailSearch(ctx context.Context, r connector.TypedRequest[mail.SearchRequest]) (connector.TypedResult[mail.MessagesPage], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, mail.MessagesPage{}, permanent("mail.invalid_request", err.Error()))
	}
	if r.Input.QuerySyntax != mail.GraphSyntax {
		return providerResult(connector.TypedResult[Response]{}, mail.MessagesPage{}, permanent("mail.query_syntax_mismatch", "Microsoft Graph KQL search syntax is required"))
	}
	return p.mailPage(ctx, r.Connection, r.Secrets, mail.PageRequest{Limit: r.Input.Limit, Cursor: r.Input.Cursor}, r.Input.Query)
}

func (p *provider) mailPage(ctx context.Context, c connector.Connection, secrets map[string]string, r mail.PageRequest, search string) (connector.TypedResult[mail.MessagesPage], error) {
	var out mail.MessagesPage
	base := url.Values{"$top": {strconv.Itoa(r.PageSize())}, "$select": {mailMetadataFields}}
	if search != "" {
		base.Set("$search", strconv.Quote(search))
	} else {
		base.Set("$orderby", "receivedDateTime desc")
	}
	endpoint := graphBase(c) + "/me/messages"
	q, seen, err := mailPageQuery(c, endpoint, base, r.Cursor)
	if err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, err)
	}
	if search != "" && seen >= mailSearchMaximum || search == "" && seen != 0 {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("mail.invalid_cursor", "invalid mail continuation"))
	}
	if err = ctx.Err(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, err)
	}
	raw, err := p.executeWithRefresh(ctx, c, secrets, endpoint, q, `IdType="ImmutableId"`)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var page struct {
		Items *[]graphMailMessage `json:"value"`
		Next  string              `json:"@odata.nextLink"`
	}
	if err = decodeMailResponse(raw.Output, &page); err != nil {
		return providerResult(raw, out, err)
	}
	if page.Items == nil || len(*page.Items) > r.PageSize() {
		return providerResult(raw, out, mailInvalidResponse())
	}
	out = mail.MessagesPage{Items: []mail.Summary{}, QuerySyntax: mail.GraphSyntax, MailboxScope: "mailbox_including_deleted_items", Complete: page.Next == ""}
	for _, item := range *page.Items {
		summary, err := item.summary()
		if err != nil {
			return providerResult(raw, mail.MessagesPage{}, err)
		}
		out.Items = append(out.Items, summary)
	}
	count := 0
	if search != "" {
		count = seen + len(out.Items)
	}
	// The Graph search cap is not proof that all matches were retrieved.
	// Reaching it never returns a completed search or another continuation.
	if count > mailSearchMaximum {
		return providerResult(raw, mail.MessagesPage{}, mailInvalidResponse())
	}
	if count == mailSearchMaximum {
		out.Complete = false
		out.LimitReason = "provider_search_limit"
	} else if page.Next != "" {
		out.NextCursor, err = mailNextCursor(c, endpoint, base, q, page.Next, count)
		if err != nil {
			return providerResult(raw, mail.MessagesPage{}, err)
		}
	}
	if out.Validate(r.PageSize()) != nil {
		return providerResult(raw, mail.MessagesPage{}, mailInvalidResponse())
	}
	return providerResult(raw, out, nil)
}

func (p *provider) mailRead(ctx context.Context, r connector.TypedRequest[mail.ReadRequest]) (connector.TypedResult[mail.Message], error) {
	var out mail.Message
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("mail.invalid_request", err.Error()))
	}
	if err := ctx.Err(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, err)
	}
	q := url.Values{"$select": {mailMetadataFields + ",body"}}
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, graphBase(r.Connection)+"/me/messages/"+url.PathEscape(r.Input.MessageID), q, `IdType="ImmutableId"`, `outlook.body-content-type="text"`)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var message graphMailMessage
	if err = decodeMailResponse(raw.Output, &message); err != nil {
		return providerResult(raw, out, err)
	}
	if message.ID != r.Input.MessageID {
		return providerResult(raw, out, mailInvalidResponse())
	}
	summary, err := message.summary()
	if err != nil {
		return providerResult(raw, out, err)
	}
	out = mail.Message{Summary: summary, Body: message.body(r.Input.BodyLimit())}
	if out.Validate(r.Input) != nil {
		return providerResult(raw, mail.Message{}, mailInvalidResponse())
	}
	return providerResult(raw, out, nil)
}
