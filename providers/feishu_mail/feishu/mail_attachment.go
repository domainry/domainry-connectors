package feishu

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/domain/mailattachment"
)

func (p *provider) mailAttachmentDownload(ctx context.Context, request connector.TypedRequest[mailattachment.DownloadRequest]) (connector.TypedResult[mailattachment.DownloadResult], error) {
	if err := request.Input.Validate(); err != nil {
		return mailResult[mailattachment.DownloadResult](nil, "", nil, permanent("mail.attachment_invalid_request", err.Error()))
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	query := url.Values{"attachment_ids": {request.Input.AttachmentID}}
	raw, err := session.call(ctx, http.MethodGet, messagePath(request.Connection)+"/"+url.PathEscape(request.Input.MessageID)+"/attachments/download_url", query, nil, false)
	if err != nil {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, err)
	}
	var response struct {
		DownloadURLs []struct {
			AttachmentID string `json:"attachment_id"`
			DownloadURL  string `json:"download_url"`
		} `json:"download_urls"`
		FailedIDs []string `json:"failed_ids"`
	}
	if err = decodeData(raw.Output, &response); err != nil || len(response.DownloadURLs) != 1 || len(response.FailedIDs) != 0 || response.DownloadURLs[0].AttachmentID != request.Input.AttachmentID {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, permanent("mail.attachment_invalid_response", "Feishu attachment download URL response is invalid"))
	}
	endpoint, secretQuery, err := signedDownloadRequest(response.DownloadURLs[0].DownloadURL)
	if err != nil {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, permanent("mail.attachment_invalid_response", err.Error()))
	}
	download, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodGet, URL: endpoint, SecretQuery: secretQuery, MaxResponseBytes: mailattachment.MaxBytes,
	})
	if transportErr != nil {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, connector.RetryableError("feishu_mail.mail.attachment_download_failed", errors.New("Feishu attachment download failed")))
	}
	if download.StatusCode != http.StatusOK || len(download.Body) > mailattachment.MaxBytes {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, permanent("mail.attachment_invalid_response", "Feishu attachment download response is invalid"))
	}
	output := mailattachment.DownloadResult{
		MessageID: request.Input.MessageID, AttachmentID: request.Input.AttachmentID,
		Size: int64(len(download.Body)), DataBase64: base64.StdEncoding.EncodeToString(download.Body),
	}
	if _, err = output.Decode(request.Input); err != nil {
		return mailResult[mailattachment.DownloadResult](nil, raw.ResponseRef, session.updates, permanent("mail.attachment_invalid_response", err.Error()))
	}
	return mailResult(&output, raw.ResponseRef, session.updates, nil)
}

func signedDownloadRequest(raw string) (string, map[string]string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() != "api-drive-stream.feishu.cn" || parsed.User != nil || parsed.Fragment != "" || parsed.Port() != "" {
		return "", nil, errors.New("Feishu attachment download URL is not trusted")
	}
	secret := map[string]string{}
	for key, values := range parsed.Query() {
		if len(values) != 1 || key == "" || values[0] == "" {
			return "", nil, errors.New("Feishu attachment download URL is invalid")
		}
		secret[key] = values[0]
	}
	if len(secret) == 0 {
		return "", nil, errors.New("Feishu attachment download URL is unsigned")
	}
	parsed.RawQuery = ""
	return parsed.String(), secret, nil
}
