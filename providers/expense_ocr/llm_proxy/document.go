package llmproxy

import (
	"encoding/base64"
	"strings"
)

const MaxDocumentBytes = 20 << 20

func canonicalInput(input ParseExpenseInput) (ParseExpenseInput, error) {
	value := strings.TrimSpace(input.Document)
	mimeType := strings.ToLower(strings.TrimSpace(input.MIMEType))
	if value == "" {
		return input, permanent("document_required", "document is required")
	}
	if len(value) > maxRequestBytes {
		return input, permanent("document_too_large", "document exceeds the encoded request limit")
	}
	switch mimeType {
	case "application/pdf", "image/tiff", "image/jpeg", "image/png", "image/bmp", "image/gif", "image/webp":
	default:
		return input, permanent("mime_type_invalid", "unsupported mime_type")
	}
	if len(value) >= 5 && strings.EqualFold(value[:5], "data:") {
		metadata, content, found := strings.Cut(value[5:], ",")
		if !found || strings.ToLower(metadata) != mimeType+";base64" {
			return input, permanent("document_invalid", "base64 data URL must match mime_type")
		}
		value = content
	}
	// Go's standard base64 decoder allows CR/LF. Remove them before enforcing the
	// encoded length, so oversized data is rejected before allocating a buffer.
	value = strings.NewReplacer("\r", "", "\n", "").Replace(value)
	if len(value) > base64.StdEncoding.EncodedLen(MaxDocumentBytes) {
		return input, permanent("document_too_large", "decoded document exceeds 20 MiB")
	}
	document, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		document, err = base64.RawStdEncoding.DecodeString(value)
	}
	if err != nil || len(document) == 0 {
		return input, permanent("document_invalid", "document must be nonempty standard base64")
	}
	if len(document) > MaxDocumentBytes {
		return input, permanent("document_too_large", "decoded document exceeds 20 MiB")
	}
	if sniffMIME(document) != mimeType {
		return input, permanent("mime_type_mismatch", "document signature does not match mime_type")
	}
	for _, value := range []string{input.SessionID, input.ConvID, input.ReactID} {
		if len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
			return input, permanent("correlation_invalid", "correlation identifiers must be at most 256 bytes without line breaks or NUL")
		}
	}
	input.Document = base64.StdEncoding.EncodeToString(document)
	input.MIMEType = mimeType
	return input, nil
}

func sniffMIME(data []byte) string {
	switch {
	case len(data) >= 5 && string(data[:5]) == "%PDF-":
		return "application/pdf"
	case len(data) >= 4 && (string(data[:4]) == "II\x2a\x00" || string(data[:4]) == "MM\x00\x2a"):
		return "image/tiff"
	case len(data) >= 3 && string(data[:3]) == "\xff\xd8\xff":
		return "image/jpeg"
	case len(data) >= 8 && string(data[:8]) == "\x89PNG\r\n\x1a\n":
		return "image/png"
	case len(data) >= 2 && string(data[:2]) == "BM":
		return "image/bmp"
	case len(data) >= 6 && (string(data[:6]) == "GIF87a" || string(data[:6]) == "GIF89a"):
		return "image/gif"
	case len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP":
		return "image/webp"
	default:
		return ""
	}
}
