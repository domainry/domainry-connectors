package microsoft

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

var (
	MailSend  = connector.CallOperation[mailwrite.SendRequest, mailwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.SendOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey), Reliability: writeReliability()}
	MailReply = connector.CallOperation[mailwrite.ReplyRequest, mailwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.ReplyOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.ReplyOperationKey), Reliability: writeReliability()}
)

func graphOutgoingMessage(m mailwrite.Message) map[string]any {
	recipients := func(values []maildto.Address) []map[string]any {
		out := make([]map[string]any, 0, len(values))
		for _, a := range values {
			out = append(out, map[string]any{"emailAddress": map[string]string{"name": a.Name, "address": a.Address}})
		}
		return out
	}
	return map[string]any{"subject": m.Subject, "body": map[string]string{"contentType": "text", "content": m.Text}, "toRecipients": recipients(m.To), "ccRecipients": recipients(m.CC), "bccRecipients": recipients(m.BCC)}
}

func (p *provider) mailSend(ctx context.Context, r connector.TypedRequest[mailwrite.SendRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), mailwrite.Result{}, permanent("mail.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return providerResult(empty(), mailwrite.Result{}, err)
	}
	// /me chooses the authenticated mailbox; From/send-as is not caller input.
	body := map[string]any{"message": graphOutgoingMessage(r.Input.Message), "saveToSentItems": true}
	raw, err := p.executeGraphWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, graphBase(r.Connection)+"/me/sendMail", nil, body, "", `IdType="ImmutableId"`)
	return p.mailMutationResult(raw, err, mailwrite.SendOperationKey, r.RequestRef, "")
}

func (p *provider) mailReply(ctx context.Context, r connector.TypedRequest[mailwrite.ReplyRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), mailwrite.Result{}, permanent("mail.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return providerResult(empty(), mailwrite.Result{}, err)
	}
	endpoint := graphBase(r.Connection) + "/me/messages/" + url.PathEscape(r.Input.MessageID)
	read, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, endpoint, url.Values{"$select": {mailMetadataFields}}, `IdType="ImmutableId"`)
	if err != nil {
		return providerResult(read, mailwrite.Result{}, err)
	}
	var source graphMailMessage
	if err = decodeMailResponse(read.Output, &source); err != nil {
		return providerResult(read, mailwrite.Result{}, err)
	}
	summary, err := source.summary()
	if err != nil {
		return providerResult(read, mailwrite.Result{}, err)
	}
	if err = r.Input.ValidateAgainst(summary); err != nil {
		return providerResult(read, mailwrite.Result{}, permanent("mail.reply_source_changed", err.Error()))
	}
	if err = ctx.Err(); err != nil {
		return providerResult(read, mailwrite.Result{}, err)
	}
	// Native reply owns the thread. Explicit recipients/body are supplied in
	// message; no comment is added, and no hidden reply-all expansion is done.
	raw, err := p.executeGraphWithRefresh(ctx, r.Connection, writeSecrets(r.Secrets, read), http.MethodPost, endpoint+"/reply", nil, map[string]any{"message": graphOutgoingMessage(r.Input.Message)}, "", `IdType="ImmutableId"`)
	return p.mailMutationResult(mergeWriteState(read, raw), err, mailwrite.ReplyOperationKey, r.RequestRef, r.Input.MessageID)
}

func (p *provider) mailMutationResult(raw connector.TypedResult[Response], err error, operation, ref, original string) (connector.TypedResult[mailwrite.Result], error) {
	if err != nil {
		return providerResult(raw, mailwrite.Result{}, err)
	}
	if raw.ResponseRef != "http:202" || len(raw.Output) != 0 {
		return providerResult(raw, mailwrite.Result{}, connector.UncertainError("microsoft.mail.write_response_invalid", errors.New("Graph may have accepted mail but returned an unexpected response")))
	}
	out := mailwrite.Result{RequestRef: ref, Status: "accepted", InReplyToMessageID: original, AcceptedAt: p.now().UTC().Format(time.RFC3339Nano), Delivery: "unknown"}
	if out.Validate(operation, ref, original) != nil {
		return providerResult(raw, mailwrite.Result{}, connector.UncertainError("microsoft.mail.write_response_invalid", errors.New("mail acceptance receipt could not be represented")))
	}
	return providerResult(raw, out, nil)
}
