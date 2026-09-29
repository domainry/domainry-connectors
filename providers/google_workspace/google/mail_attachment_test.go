package google

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/mailattachment"
)

func TestMailAttachmentDownloadUsesExactProviderIdentifiersAndBoundedResponse(t *testing.T) {
	content := []byte("private gmail attachment")
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if !strings.HasSuffix(request.URL, "/messages/message%2Fa/attachments/attachment%2Fb") || request.MaxResponseBytes != mailattachment.MaxProviderResponseBytes {
			t.Fatalf("request=%+v", request)
		}
		body, _ := json.Marshal(gmailAttachmentResponse{AttachmentID: "attachment/b", Size: int64(len(content)), Data: base64.RawURLEncoding.EncodeToString(content)})
		return connector.HTTPResponse{StatusCode: 200, Body: body}, nil
	}}
	adapter, _ := New(transport)
	input := mailattachment.DownloadRequest{MessageID: "message/a", AttachmentID: "attachment/b"}
	payload, _ := json.Marshal(input)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailAttachmentDownload.Key, ContractSHA256: MailAttachmentDownload.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	var output mailattachment.DownloadResult
	if err != nil || json.Unmarshal(result.Payload, &output) != nil {
		t.Fatalf("result=%s err=%v", result.Payload, err)
	}
	decoded, err := output.Decode(input)
	if err != nil || string(decoded) != string(content) {
		t.Fatalf("decoded=%q err=%v", decoded, err)
	}
}

func TestMailAttachmentDownloadRejectsProviderIdentityOrSizeMismatch(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"attachmentId":"other","size":2,"data":"eA"}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(mailattachment.DownloadRequest{MessageID: "message", AttachmentID: "attachment"})
	if _, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailAttachmentDownload.Key, ContractSHA256: MailAttachmentDownload.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}); err == nil {
		t.Fatal("mismatched Gmail attachment accepted")
	}
}
