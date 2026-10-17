package feishu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net/http"
	stdmail "net/mail"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

func (p *provider) mailSend(ctx context.Context, request connector.TypedRequest[mailwrite.SendRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mailwrite.Result](nil, "", nil, permanent("mail.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(request.Connection, request.RequestRef); err != nil {
		return mailResult[mailwrite.Result](nil, "", nil, err)
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	sender, err := session.authenticatedMailbox(ctx)
	if err != nil {
		return mailResult[mailwrite.Result](nil, "", session.updates, err)
	}
	correlation := writeCorrelation(request.Connection, request.RequestRef, mailwrite.SendOperationKey, "")
	mimeMessage, err := outgoingMIME(request.Input.Message, sender, correlation, "", p.now())
	if err != nil {
		return mailResult[mailwrite.Result](nil, "", session.updates, permanent("mail.send_invalid", err.Error()))
	}
	body := map[string]any{"raw": base64.RawURLEncoding.EncodeToString(mimeMessage), "dedupe_key": correlation}
	raw, err := session.call(ctx, http.MethodPost, messagePath(request.Connection)+"/send", nil, body, true)
	return p.mailMutationResult(raw, session.updates, err, mailwrite.SendOperationKey, request.RequestRef, "")
}

func (p *provider) mailReply(ctx context.Context, request connector.TypedRequest[mailwrite.ReplyRequest]) (connector.TypedResult[mailwrite.Result], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mailwrite.Result](nil, "", nil, permanent("mail.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(request.Connection, request.RequestRef); err != nil {
		return mailResult[mailwrite.Result](nil, "", nil, err)
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	readRaw, original, err := session.getMessage(ctx, request.Input.MessageID)
	if err != nil {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, err)
	}
	summary, err := original.summary(request.Input.MessageID)
	if err != nil {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, err)
	}
	if err = request.Input.ValidateAgainst(summary); err != nil {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, permanent("mail.reply_source_changed", err.Error()))
	}
	if summary.InternetMessageID == "" {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, permanent("mail.reply_source_incomplete", "Feishu reply source has no RFC Message-ID"))
	}
	sender, err := session.authenticatedMailbox(ctx)
	if err != nil {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, err)
	}
	correlation := writeCorrelation(request.Connection, request.RequestRef, mailwrite.ReplyOperationKey, request.Input.MessageID)
	mimeMessage, err := outgoingMIME(request.Input.Message, sender, correlation, summary.InternetMessageID, p.now())
	if err != nil {
		return mailResult[mailwrite.Result](nil, readRaw.ResponseRef, session.updates, permanent("mail.reply_invalid", err.Error()))
	}
	body := map[string]any{"raw": base64.RawURLEncoding.EncodeToString(mimeMessage), "dedupe_key": correlation}
	raw, err := session.call(ctx, http.MethodPost, messagePath(request.Connection)+"/send", nil, body, true)
	return p.mailMutationResult(raw, session.updates, err, mailwrite.ReplyOperationKey, request.RequestRef, request.Input.MessageID)
}

func (p *provider) mailMutationResult(raw connector.TypedResult[Response], updates map[string]string, err error, operation, requestRef, originalMessageID string) (connector.TypedResult[mailwrite.Result], error) {
	if err != nil {
		if code, ok := connector.ProviderErrorCodeOf(err); !ok || code != "feishu_mail.provider_code_1236005" {
			return mailResult[mailwrite.Result](nil, raw.ResponseRef, updates, err)
		}
		// Feishu documents 1236005 as a repeated RFC Message-ID. The stable
		// Message-ID therefore proves an earlier call was accepted even when its
		// original response was lost.
	}
	var receipt struct {
		MessageID string `json:"message_id"`
		ThreadID  string `json:"thread_id"`
	}
	if err == nil {
		if decodeErr := decodeData(raw.Output, &receipt); decodeErr != nil {
			return mailResult[mailwrite.Result](nil, raw.ResponseRef, updates, connector.UncertainError("feishu_mail.mail.write_response_invalid", errors.New("Feishu may have accepted the mail but returned no valid receipt")))
		}
	}
	output := mailwrite.Result{
		RequestRef:         requestRef,
		Status:             "accepted",
		MessageID:          receipt.MessageID,
		ThreadID:           receipt.ThreadID,
		InReplyToMessageID: originalMessageID,
		AcceptedAt:         p.now().UTC().Format(time.RFC3339Nano),
		Delivery:           "unknown",
	}
	if output.Validate(operation, requestRef, originalMessageID) != nil {
		return mailResult[mailwrite.Result](nil, raw.ResponseRef, updates, connector.UncertainError("feishu_mail.mail.write_response_invalid", errors.New("Feishu mail acceptance receipt could not be represented")))
	}
	return mailResult(&output, raw.ResponseRef, updates, nil)
}

func validateWriteEnvelope(connection connector.Connection, requestRef string) error {
	if strings.TrimSpace(connection.WorkspaceID) == "" || strings.TrimSpace(connection.Key) == "" || requestRef == "" || strings.TrimSpace(requestRef) != requestRef || len(requestRef) > 2048 || !utf8.ValidString(requestRef) || strings.ContainsFunc(requestRef, unicode.IsControl) {
		return permanent("mail.write_identity_required", "mail write requires a trusted connection and request reference")
	}
	return nil
}

func writeCorrelation(connection connector.Connection, requestRef, operation, target string) string {
	raw, _ := json.Marshal([]string{ConnectorKey, ProviderKey, connection.WorkspaceID, connection.Key, requestRef, operation, target})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (p *provider) Reconcile(ctx context.Context, request connector.ReconcileRequest) (connector.ReconcileResult, error) {
	if err := request.Validate(); err != nil {
		return connector.ReconcileResult{}, err
	}
	if request.ConnectorKey != ConnectorKey || request.ProviderKey != ProviderKey || request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.ReconcileResult{}, permanent("mail.reconcile_provider_mismatch", "mail reconciliation provider does not match")
	}
	if err := validateWriteEnvelope(request.Connection, request.RequestRef); err != nil {
		return connector.ReconcileResult{}, err
	}
	operation, target := request.OperationKey, ""
	switch operation {
	case mailwrite.SendOperationKey:
		if request.ContractSHA256 != mailwrite.OperationSHA256(operation) {
			return connector.ReconcileResult{}, permanent("mail.reconcile_contract_mismatch", "mail send reconciliation contract changed")
		}
		var input mailwrite.SendRequest
		if err := decodeStrictWriteRequest(request.Payload, &input); err != nil || input.Validate() != nil {
			return connector.ReconcileResult{}, permanent("mail.reconcile_request_invalid", "mail send reconciliation request is invalid")
		}
	case mailwrite.ReplyOperationKey:
		if request.ContractSHA256 != mailwrite.OperationSHA256(operation) {
			return connector.ReconcileResult{}, permanent("mail.reconcile_contract_mismatch", "mail reply reconciliation contract changed")
		}
		var input mailwrite.ReplyRequest
		if err := decodeStrictWriteRequest(request.Payload, &input); err != nil || input.Validate() != nil {
			return connector.ReconcileResult{}, permanent("mail.reconcile_request_invalid", "mail reply reconciliation request is invalid")
		}
		target = input.MessageID
	default:
		return connector.ReconcileResult{}, permanent("mail.reconcile_operation_invalid", "mail reconciliation operation is unsupported")
	}
	correlation := writeCorrelation(request.Connection, request.RequestRef, operation, target) + "@domainry.invalid"
	return p.lookupSentMessage(ctx, request, correlation, target)
}

func decodeStrictWriteRequest(raw json.RawMessage, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("mail reconciliation request must contain exactly one JSON value")
	}
	return nil
}

func (p *provider) lookupSentMessage(ctx context.Context, request connector.ReconcileRequest, internetMessageID, originalMessageID string) (connector.ReconcileResult, error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	pageToken := ""
	responseRef := request.ResponseRef
	pageLimit := configInt(request.Connection.Config, "mail_write_reconcile_pages", 5)
	for page := 0; page < pageLimit; page++ {
		query := url.Values{"folder_id": {"SENT"}, "page_size": {strconv.Itoa(mailSyncPageSize)}}
		if pageToken != "" {
			query.Set("page_token", pageToken)
		}
		raw, err := session.call(ctx, http.MethodGet, messagePath(request.Connection), query, nil, false)
		if raw.ResponseRef != "" {
			responseRef = raw.ResponseRef
		}
		if err != nil {
			return connector.ReconcileResult{}, err
		}
		var messages feishuMessagePage
		if decodeData(raw.Output, &messages) != nil || len(messages.Items) > mailSyncPageSize || messages.HasMore && messages.PageToken == "" || !messages.HasMore && messages.PageToken != "" {
			return connector.ReconcileResult{}, mailInvalidResponse()
		}
		for _, messageID := range messages.Items {
			if !maildto.ValidID(messageID) {
				return connector.ReconcileResult{}, mailInvalidResponse()
			}
			detailRaw, message, err := session.getMessage(ctx, messageID)
			if detailRaw.ResponseRef != "" {
				responseRef = detailRaw.ResponseRef
			}
			if err != nil {
				return connector.ReconcileResult{}, err
			}
			summary, err := message.summary(messageID)
			if err != nil {
				return connector.ReconcileResult{}, err
			}
			if normalizeInternetMessageID(summary.InternetMessageID) != internetMessageID {
				continue
			}
			acceptedAt := summary.SentAt
			if acceptedAt == "" {
				acceptedAt = p.now().UTC().Format(time.RFC3339Nano)
			}
			receipt := mailwrite.Result{RequestRef: request.RequestRef, Status: "accepted", MessageID: summary.ID, ThreadID: summary.ThreadID, InReplyToMessageID: originalMessageID, AcceptedAt: acceptedAt, Delivery: "unknown"}
			if err := receipt.Validate(request.OperationKey, request.RequestRef, originalMessageID); err != nil {
				return connector.ReconcileResult{}, mailInvalidResponse()
			}
			payload, err := json.Marshal(receipt)
			if err != nil {
				return connector.ReconcileResult{}, err
			}
			return connector.ReconcileResult{Outcome: connector.ReconciliationSucceeded, Result: &connector.CallResult{Payload: payload, ResponseRef: responseRef, SecretUpdates: cloneStrings(session.updates)}}, nil
		}
		if !messages.HasMore {
			return connector.ReconcileResult{Outcome: connector.ReconciliationNotFound}, nil
		}
		pageToken = messages.PageToken
	}
	// The lookup is intentionally bounded. A later explicit reconciliation may
	// observe eventual consistency, while Integration must never interpret this
	// partial scan as permission to resend.
	return connector.ReconcileResult{Outcome: connector.ReconciliationPending, RetryAfter: 30 * time.Second, Result: &connector.CallResult{ResponseRef: responseRef, SecretUpdates: cloneStrings(session.updates)}}, nil
}

func normalizeInternetMessageID(value string) string {
	return strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(value), "<"), ">")
}

func (s *apiSession) authenticatedMailbox(ctx context.Context) (string, error) {
	path := "/open-apis/mail/v1/user_mailboxes/" + url.PathEscape(mailboxID(s.connection)) + "/profile"
	raw, err := s.call(ctx, http.MethodGet, path, nil, nil, false)
	if err != nil {
		return "", err
	}
	var profile struct {
		PrimaryEmailAddress string `json:"primary_email_address"`
	}
	if err = decodeData(raw.Output, &profile); err != nil {
		return "", mailInvalidResponse()
	}
	mailboxAddress := strings.TrimSpace(profile.PrimaryEmailAddress)
	parsed, err := stdmail.ParseAddress(mailboxAddress)
	if err != nil || parsed.Address != mailboxAddress || parsed.Name != "" {
		return "", permanent("mailbox_identity_invalid", "Feishu mailbox profile lacks a primary email address")
	}
	configured := mailboxID(s.connection)
	if configured != "me" && !strings.EqualFold(configured, parsed.Address) {
		return "", permanent("mailbox_identity_mismatch", "configured mailbox does not match the authenticated Feishu user")
	}
	return parsed.Address, nil
}

func outgoingMIME(message mailwrite.Message, sender, correlation, originalMessageID string, now time.Time) ([]byte, error) {
	if strings.ContainsAny(originalMessageID, "\r\n") {
		return nil, errors.New("reply source Message-ID is invalid")
	}
	originalMessageID = strings.TrimSpace(originalMessageID)
	if originalMessageID != "" {
		if !strings.HasPrefix(originalMessageID, "<") {
			originalMessageID = "<" + originalMessageID
		}
		if !strings.HasSuffix(originalMessageID, ">") {
			originalMessageID += ">"
		}
	}
	formatAddresses := func(values []maildto.Address) string {
		formatted := make([]string, 0, len(values))
		for _, value := range values {
			formatted = append(formatted, (&stdmail.Address{Name: value.Name, Address: value.Address}).String())
		}
		return strings.Join(formatted, ", ")
	}
	var buffer bytes.Buffer
	headers := [][2]string{
		{"From", (&stdmail.Address{Address: sender}).String()},
		{"To", formatAddresses(message.To)},
		{"Cc", formatAddresses(message.CC)},
		{"Bcc", formatAddresses(message.BCC)},
		{"Subject", mime.BEncoding.Encode("UTF-8", message.Subject)},
		{"Date", now.UTC().Format(time.RFC1123Z)},
		{"Message-ID", "<" + correlation + "@domainry.invalid>"},
		{"In-Reply-To", originalMessageID},
		{"References", originalMessageID},
		{"MIME-Version", "1.0"},
		{"Content-Type", `text/plain; charset="UTF-8"`},
		{"Content-Transfer-Encoding", "quoted-printable"},
	}
	for _, header := range headers {
		if header[1] != "" {
			fmt.Fprintf(&buffer, "%s: %s\r\n", header[0], header[1])
		}
	}
	buffer.WriteString("\r\n")
	encoded := quotedprintable.NewWriter(&buffer)
	if _, err := encoded.Write([]byte(message.Text)); err != nil {
		return nil, err
	}
	if err := encoded.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
