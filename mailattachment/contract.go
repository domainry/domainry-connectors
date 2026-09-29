// Package mailattachment defines the provider-neutral contract used to fetch
// one source-owned email attachment. It owns no accounts, credentials or I/O.
package mailattachment

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"unicode"
)

const (
	ContractVersion      = "mail-attachment-download-v1"
	ContractSHA256       = "12ee30af8c970a9873378b8f8160ad805d09d51842bde35f7b15df971191bf09"
	DownloadOperationKey = "mail_attachment_download"
	MaxBytes             = 25 << 20
	// Provider responses contain base64 plus a small JSON envelope.
	MaxProviderResponseBytes int64 = 36 << 20
)

type DownloadRequest struct {
	MessageID    string `json:"message_id"`
	AttachmentID string `json:"attachment_id"`
}

type DownloadResult struct {
	MessageID    string `json:"message_id"`
	AttachmentID string `json:"attachment_id"`
	Size         int64  `json:"size"`
	DataBase64   string `json:"data_base64"`
}

func validID(value string) bool {
	return value != "" && len(value) <= 4096 && strings.TrimSpace(value) == value && value != "." && value != ".." && strings.IndexFunc(value, unicode.IsControl) < 0
}

func (r DownloadRequest) Validate() error {
	if !validID(r.MessageID) || !validID(r.AttachmentID) {
		return errors.New("mail attachment identity is invalid")
	}
	return nil
}

func (r DownloadResult) Decode(request DownloadRequest) ([]byte, error) {
	if request.Validate() != nil || r.MessageID != request.MessageID || r.AttachmentID != request.AttachmentID || r.Size < 0 || r.Size > MaxBytes {
		return nil, errors.New("mail attachment result is invalid")
	}
	decoded, err := base64.StdEncoding.DecodeString(r.DataBase64)
	if err != nil || int64(len(decoded)) != r.Size || len(decoded) > MaxBytes {
		return nil, errors.New("mail attachment result is invalid")
	}
	return decoded, nil
}

func ComputedContractSHA256() string {
	var b strings.Builder
	b.WriteString(ContractVersion + ";read-only;source-account-owned;message-and-attachment-bound;max-bytes=26214400;standard-base64;provider-response-max=37748736;")
	for _, value := range []any{DownloadRequest{}, DownloadResult{}} {
		typeOf := reflect.TypeOf(value)
		b.WriteString(typeOf.Name() + "{")
		for index := 0; index < typeOf.NumField(); index++ {
			field := typeOf.Field(index)
			b.WriteString(field.Name + ":" + field.Type.String() + ":" + field.Tag.Get("json") + ";")
		}
		b.WriteString("}")
	}
	digest := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(digest[:])
}

func OperationSHA256(key string) string {
	if key != DownloadOperationKey {
		return ""
	}
	digest := sha256.Sum256([]byte(ContractSHA256 + ":" + key))
	return hex.EncodeToString(digest[:])
}
