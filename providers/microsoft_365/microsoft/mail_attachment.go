package microsoft

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/mailattachment"
)

type graphFileAttachment struct {
	Type         string `json:"@odata.type"`
	ID           string `json:"id"`
	Size         int64  `json:"size"`
	ContentBytes string `json:"contentBytes"`
}

func (p *provider) mailAttachmentDownload(ctx context.Context, request connector.TypedRequest[mailattachment.DownloadRequest]) (connector.TypedResult[mailattachment.DownloadResult], error) {
	if err := request.Input.Validate(); err != nil {
		return connector.TypedResult[mailattachment.DownloadResult]{}, permanent("mail.attachment_invalid_request", err.Error())
	}
	endpoint := graphBase(request.Connection) + "/me/messages/" + url.PathEscape(request.Input.MessageID) + "/attachments/" + url.PathEscape(request.Input.AttachmentID)
	raw, err := p.executeGraphWithRefreshLimit(ctx, request.Connection, request.Secrets, "GET", endpoint, nil, nil, "", mailattachment.MaxProviderResponseBytes, `IdType="ImmutableId"`)
	if err != nil {
		return graphAttachmentResult(raw, mailattachment.DownloadResult{}, err)
	}
	encoded, marshalErr := json.Marshal(raw.Output)
	var provider graphFileAttachment
	if marshalErr != nil || json.Unmarshal(encoded, &provider) != nil || provider.Type != "#microsoft.graph.fileAttachment" || provider.ID != request.Input.AttachmentID || provider.Size < 0 || provider.Size > mailattachment.MaxBytes {
		return graphAttachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", "Microsoft attachment response is invalid"))
	}
	decoded, decodeErr := base64.StdEncoding.DecodeString(provider.ContentBytes)
	if decodeErr != nil || int64(len(decoded)) != provider.Size {
		return graphAttachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", "Microsoft attachment response is invalid"))
	}
	result := mailattachment.DownloadResult{
		MessageID: request.Input.MessageID, AttachmentID: request.Input.AttachmentID,
		Size: provider.Size, DataBase64: base64.StdEncoding.EncodeToString(decoded),
	}
	if _, validateErr := result.Decode(request.Input); validateErr != nil {
		return graphAttachmentResult(raw, mailattachment.DownloadResult{}, permanent("mail.attachment_invalid_response", validateErr.Error()))
	}
	return graphAttachmentResult(raw, result, nil)
}

func graphAttachmentResult(raw connector.TypedResult[Response], output mailattachment.DownloadResult, err error) (connector.TypedResult[mailattachment.DownloadResult], error) {
	return connector.TypedResult[mailattachment.DownloadResult]{Output: output, ResponseRef: raw.ResponseRef, SecretUpdates: raw.SecretUpdates, ResourceHealth: raw.ResourceHealth}, err
}
