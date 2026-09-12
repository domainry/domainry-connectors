package google

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

var (
	MailSend  = connector.CallOperation[mailwrite.SendRequest, mailwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.SendOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey), Reliability: writeReliability()}
	MailReply = connector.CallOperation[mailwrite.ReplyRequest, mailwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.ReplyOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.ReplyOperationKey), Reliability: writeReliability()}
)

func (p *provider) mailSend(ctx context.Context, r connector.TypedRequest[mailwrite.SendRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), mailwrite.Result{}, permanent("mail.invalid_request", err.Error()))
	}
	return p.sendAccountMail(ctx, r.Connection, r.Secrets, r.RequestRef, r.Input.Message, "")
}

func (p *provider) mailReply(ctx context.Context, r connector.TypedRequest[mailwrite.ReplyRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), mailwrite.Result{}, permanent("mail.invalid_request", err.Error()))
	}
	return p.sendAccountMail(ctx, r.Connection, r.Secrets, r.RequestRef, r.Input.Message, r.Input.MessageID)
}

func (p *provider) sendAccountMail(ctx context.Context, c connector.Connection, secrets map[string]string, ref string, message mailwrite.Message, original string) (connector.TypedResult[mailwrite.Result], error) {
	s := newMailSession(p, c, secrets)
	if err := validateWriteEnvelope(c, ref); err != nil {
		return mailResult(s, mailwrite.Result{}, err)
	}
	// The authenticated profile owns From. Do not trust model text or a stale
	// caller-supplied mailbox address, and do not select an alternate send-as.
	var profile struct {
		Email string `json:"emailAddress"`
	}
	if err := s.get(ctx, "profile", url.Values{"fields": {"emailAddress"}}, &profile); err != nil {
		return mailResult(s, mailwrite.Result{}, err)
	}
	if !writeMailbox(profile.Email) {
		return mailResult(s, mailwrite.Result{}, mailInvalidResponse())
	}
	thread, reply, references := "", "", []string{}
	operation := mailwrite.SendOperationKey
	if original != "" {
		operation = mailwrite.ReplyOperationKey
		var source googleMailMessage
		q := url.Values{"format": {"metadata"}, "metadataHeaders": {"From", "To", "Cc", "Reply-To", "Subject", "Date", "Message-ID", "References"}, "fields": {"id,threadId,labelIds,internalDate,payload(headers)"}}
		if err := s.get(ctx, "messages/"+url.PathEscape(original), q, &source); err != nil {
			return mailResult(s, mailwrite.Result{}, err)
		}
		summary, err := source.summary()
		if err != nil {
			return mailResult(s, mailwrite.Result{}, err)
		}
		if err = (mailwrite.ReplyRequest{MessageID: original, Message: message}).ValidateAgainst(summary); err != nil {
			return mailResult(s, mailwrite.Result{}, permanent("mail.reply_source_changed", err.Error()))
		}
		references, err = writeReferences(source.Payload.headers("References"), summary.InternetMessageID)
		if err != nil {
			return mailResult(s, mailwrite.Result{}, err)
		}
		thread, reply = summary.ThreadID, summary.InternetMessageID
	}
	correlation := "<" + writeCorrelation(c, ref, operation, original) + "@domainry.invalid>"
	body := accountMailMIME(message, profile.Email, correlation, p.now(), thread, reply, references)
	if err := ctx.Err(); err != nil {
		return mailResult(s, mailwrite.Result{}, err)
	}
	raw, err := p.executeWithRefresh(ctx, c, s.secrets, http.MethodPost, gmailBase(c)+"/gmail/v1/users/me/messages/send", nil, body, true)
	raw = mergeWriteState(s.state, raw)
	if err != nil {
		return providerResult(raw, mailwrite.Result{}, err)
	}
	var sent struct {
		ID       string `json:"id"`
		ThreadID string `json:"threadId"`
	}
	encoded, decodeErr := json.Marshal(raw.Output)
	if decodeErr == nil && json.Unmarshal(encoded, &sent) == nil {
		out := mailwrite.Result{RequestRef: ref, Status: "accepted", MessageID: sent.ID, ThreadID: sent.ThreadID, InReplyToMessageID: original, AcceptedAt: p.now().UTC().Format(time.RFC3339Nano), Delivery: "unknown"}
		if sent.ID != "" && sent.ThreadID != "" && (thread == "" || sent.ThreadID == thread) && out.Validate(operation, ref, original) == nil {
			return providerResult(raw, out, nil)
		}
	}
	return providerResult(raw, mailwrite.Result{}, connector.UncertainError("google.mail.write_response_invalid", errors.New("Google may have accepted the mail but returned no valid receipt")))
}
