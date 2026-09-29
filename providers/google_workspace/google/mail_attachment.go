package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/mailattachment"
)

type gmailAttachmentResponse struct {
	AttachmentID string `json:"attachmentId"`
	Size         int64  `json:"size"`
	Data         string `json:"data"`
}

func (p *provider) mailAttachmentDownload(ctx context.Context, request connector.TypedRequest[mailattachment.DownloadRequest]) (connector.TypedResult[mailattachment.DownloadResult], error) {
	if err := request.Input.Validate(); err != nil {
		return connector.TypedResult[mailattachment.DownloadResult]{}, permanent("mail.attachment_invalid_request", err.Error())
	}
	endpoint := gmailBase(request.Connection) + "/gmail/v1/users/me/messages/" + url.PathEscape(request.Input.MessageID) + "/attachments/" + url.PathEscape(request.Input.AttachmentID)
	raw, err := p.executeReadWithLimit(ctx, request.Connection, request.Secrets, endpoint, nil, mailattachment.MaxProviderResponseBytes)
	if err != nil {
		return attachmentResult(raw, mailattachment.DownloadResult{}, err)
	}
	encoded, marshalErr := json.Marshal(raw.Output)
	var provider gmailAttachmentResponse
	if marshalErr != nil || json.Unmarshal(encoded, &provider) != nil || provider.AttachmentID != "" && provider.AttachmentID != request.Input.AttachmentID || provider.Size < 0 || provider.Size > mailattachment.MaxBytes {
		return attachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", "Gmail attachment response is invalid"))
	}
	decoded, decodeErr := base64.RawURLEncoding.DecodeString(strings.TrimRight(provider.Data, "="))
	if decodeErr != nil || int64(len(decoded)) != provider.Size {
		return attachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", "Gmail attachment response is invalid"))
	}
	result := mailattachment.DownloadResult{
		MessageID: request.Input.MessageID, AttachmentID: request.Input.AttachmentID,
		Size: provider.Size, DataBase64: base64.StdEncoding.EncodeToString(decoded),
	}
	if _, validateErr := result.Decode(request.Input); validateErr != nil {
		return attachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", validateErr.Error()))
	}
	return attachmentResult(raw, result, nil)
}

func attachmentResult(raw connector.TypedResult[Response], output mailattachment.DownloadResult, err error) (connector.TypedResult[mailattachment.DownloadResult], error) {
	return connector.TypedResult[mailattachment.DownloadResult]{Output: output, ResponseRef: raw.ResponseRef, SecretUpdates: raw.SecretUpdates, ResourceHealth: raw.ResourceHealth}, err
}
