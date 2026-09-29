package mailattachment_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/domainry/domainry-connectors/mailattachment"
)

func TestContractIdentity(t *testing.T) {
	if got := mailattachment.ComputedContractSHA256(); got != mailattachment.ContractSHA256 {
		t.Fatalf("contract hash: %s", got)
	}
	if hash := mailattachment.OperationSHA256(mailattachment.DownloadOperationKey); len(hash) != 64 {
		t.Fatalf("operation hash=%q", hash)
	}
	if mailattachment.OperationSHA256("mail_read") != "" {
		t.Fatal("unrelated operation acquired attachment identity")
	}
}

func TestDownloadResultRequiresExactIdentityAndBoundedBytes(t *testing.T) {
	request := mailattachment.DownloadRequest{MessageID: "message/a", AttachmentID: "attachment/b"}
	data := []byte("private attachment")
	result := mailattachment.DownloadResult{MessageID: request.MessageID, AttachmentID: request.AttachmentID, Size: int64(len(data)), DataBase64: base64.StdEncoding.EncodeToString(data)}
	decoded, err := result.Decode(request)
	if err != nil || string(decoded) != string(data) {
		t.Fatalf("decode=%q err=%v", decoded, err)
	}
	for _, mutate := range []func(*mailattachment.DownloadResult){
		func(value *mailattachment.DownloadResult) { value.MessageID = "other" },
		func(value *mailattachment.DownloadResult) { value.AttachmentID = "other" },
		func(value *mailattachment.DownloadResult) { value.Size++ },
		func(value *mailattachment.DownloadResult) { value.DataBase64 = "%%%" },
		func(value *mailattachment.DownloadResult) { value.Size = mailattachment.MaxBytes + 1 },
	} {
		invalid := result
		mutate(&invalid)
		if _, err := invalid.Decode(request); err == nil {
			t.Fatalf("invalid result accepted: %#v", invalid)
		}
	}
	if (mailattachment.DownloadRequest{MessageID: strings.Repeat("x", 4097), AttachmentID: "a"}).Validate() == nil {
		t.Fatal("oversized provider identity accepted")
	}
}
