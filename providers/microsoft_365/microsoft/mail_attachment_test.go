package microsoft

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/mailattachment"
)

func TestMailAttachmentDownloadUsesImmutableMessageAndFileAttachment(t *testing.T) {
	content := []byte("private graph attachment")
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if !strings.HasSuffix(request.URL, "/messages/message%2Fa/attachments/attachment%2Fb") || request.MaxResponseBytes != mailattachment.MaxProviderResponseBytes || len(request.Headers["Prefer"]) != 1 || request.Headers["Prefer"][0] != `IdType="ImmutableId"` {
			t.Fatalf("request=%+v", request)
		}
		body, _ := json.Marshal(graphFileAttachment{Type: "#microsoft.graph.fileAttachment", ID: "attachment/b", Size: int64(len(content)), ContentBytes: base64.StdEncoding.EncodeToString(content)})
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

func TestMailAttachmentDownloadRejectsNonFileAttachment(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"@odata.type":"#microsoft.graph.itemAttachment","id":"attachment","size":0}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(mailattachment.DownloadRequest{MessageID: "message", AttachmentID: "attachment"})
	if _, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailAttachmentDownload.Key, ContractSHA256: MailAttachmentDownload.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}); err == nil {
		t.Fatal("non-file Microsoft attachment accepted")
	}
}
