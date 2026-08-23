// Package pdf implements the reusable PDF document-generation Provider.
package pdf

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"strings"
)

const (
	ConnectorKey        = "document_generation"
	ProviderKey         = "pdf"
	defaultContentLimit = 256 << 10
	maximumContentLimit = 4 << 20
)

type RenderInput struct {
	Title    string `json:"title"`
	Content  string `json:"content"`
	Filename string `json:"filename,omitempty"`
}
type Response map[string]any

var (
	RenderPDF      = connector.CallOperation[RenderInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "render_pdf", ContractSHA256: "d11160b6a449ea92590653e020ca9c5f190dc827294d657e8f039ded32c9f5d9", Reliability: reliability(connector.EffectWrite, connector.IdempotencyNone)}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "43d62754d16693942e4c9a1db92adc684a59f3e194a46de790c3be96cc0b1039", Reliability: reliability(connector.EffectRead, connector.IdempotencyNatural)}
)

func reliability(effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct{ connector.Adapter }

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("PDF provider transport is required")
	}
	p := &provider{}
	render, err := connector.BindCall(RenderPDF, p.render)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), render, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(maximumContentLimit)
	field := func(key, en, zh string) connector.ConfigField {
		return connector.ConfigField{Key: key, Name: en, Type: connector.ConfigFieldText, I18n: i18n(en, zh)}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{field("header", "Header", "页眉"), field("footer", "Footer", "页脚"), field("watermark", "Watermark", "水印"), {Key: "default_locale", Name: "Default Locale", Type: connector.ConfigFieldText, Default: json.RawMessage(`"en-US"`), I18n: i18n("Default Locale", "默认语言")}, {Key: "max_content_bytes", Name: "Maximum Content Bytes", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`262144`), Validation: connector.ConfigValidation{Min: &min, Max: &max}, I18n: i18n("Maximum Content Bytes", "内容最大字节数")}}, SecretFields: []connector.SecretField{{Key: "signature_secret", Name: "Signature Secret", I18n: i18n("Signature Secret", "签名密钥"), CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func i18n(en, zh string) map[string]connector.FieldLocalization {
	return map[string]connector.FieldLocalization{"en-US": {Name: en, Description: en}, "zh-CN": {Name: zh, Description: zh}}
}
func (*provider) ValidateConfig(c connector.Connection) error {
	limit := integer(c.Config["max_content_bytes"], defaultContentLimit)
	if limit < 1 || limit > maximumContentLimit {
		return permanent("content_limit_invalid", "max_content_bytes is invalid")
	}
	return nil
}
func (*provider) test(ctx context.Context, _ connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	if err := ctx.Err(); err != nil {
		return empty(), err
	}
	document := renderDocument([]string{"Domainry PDF provider ready"}, "")
	return connector.TypedResult[Response]{Output: Response{"connected": len(document) > 0}, ResponseRef: "pdf:ready"}, nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) render(ctx context.Context, r connector.TypedRequest[RenderInput]) (connector.TypedResult[Response], error) {
	if err := ctx.Err(); err != nil {
		return empty(), err
	}
	if err := p.ValidateConfig(r.Connection); err != nil {
		return empty(), err
	}
	title, content := strings.TrimSpace(r.Input.Title), r.Input.Content
	if title == "" || strings.TrimSpace(content) == "" {
		return empty(), permanent("content_required", "title and content are required")
	}
	if len([]byte(content)) > integer(r.Connection.Config["max_content_bytes"], defaultContentLimit) {
		return empty(), permanent("content_too_large", "content exceeds max_content_bytes")
	}
	lines := []string{}
	if header := config(r.Connection, "header"); header != "" {
		lines = append(lines, header)
	}
	lines = append(lines, title)
	lines = append(lines, strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")...)
	if footer := config(r.Connection, "footer"); footer != "" {
		lines = append(lines, footer)
	}
	document := renderDocument(lines, config(r.Connection, "watermark"))
	sum := sha256.Sum256(document)
	digest := hex.EncodeToString(sum[:])
	filename := sanitizeFilename(r.Input.Filename)
	if filename == "" {
		filename = "document.pdf"
	}
	if !strings.HasSuffix(strings.ToLower(filename), ".pdf") {
		filename += ".pdf"
	}
	output := Response{"filename": filename, "mime_type": "application/pdf", "document_base64": base64.StdEncoding.EncodeToString(document), "sha256": digest, "bytes": len(document)}
	if secret := strings.TrimSpace(r.Secrets["signature_secret"]); secret != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(document)
		output["signature_hmac_sha256"] = hex.EncodeToString(mac.Sum(nil))
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: "pdf:sha256:" + digest}, nil
}
func sanitizeFilename(value string) string {
	value = strings.TrimSpace(value)
	var out strings.Builder
	for _, char := range value {
		if char == '/' || char == '\\' || char < 32 {
			out.WriteByte('_')
		} else {
			out.WriteRune(char)
		}
	}
	return out.String()
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		if parsed, err := fmt.Sscan(clean(value), &fallback); parsed > 0 && err == nil {
			return fallback
		}
		return fallback
	}
}
func config(c connector.Connection, key string) string { return clean(c.Config[key]) }
func clean(value any) string {
	result := strings.TrimSpace(fmt.Sprint(value))
	if result == "<nil>" {
		return ""
	}
	return result
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("pdf."+suffix, errors.New(message))
}
