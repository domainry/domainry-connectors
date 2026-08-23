// Package smtp implements the official SMTP email Provider.
package smtp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"path/filepath"
	"strings"
	"time"
)

const (
	ConnectorKey = "email"
	ProviderKey  = "smtp"
)

type SendEmailInput struct {
	To      []string `json:"to"`
	Subject string   `json:"subject,omitempty"`
	Text    string   `json:"text,omitempty"`
	HTML    string   `json:"html,omitempty"`
}
type SendFileEmailInput struct {
	To                    []string `json:"to"`
	Subject               string   `json:"subject,omitempty"`
	Text                  string   `json:"text,omitempty"`
	AttachmentBase64      string   `json:"_runtime_attachment_base64"`
	AttachmentFilename    string   `json:"_runtime_attachment_filename"`
	AttachmentContentType string   `json:"_runtime_attachment_content_type"`
	ScanEvidenceRef       string   `json:"_runtime_scan_evidence_ref,omitempty"`
}

var (
	SendEmail      = connector.EnqueueOperation[SendEmailInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_email", ContractSHA256: "ce81e72365b4f16b75a26909f2e30d8325a88ec305adb11f81d9247714c4968c", Reliability: writeReliability()}
	SendFileEmail  = connector.EnqueueOperation[SendFileEmailInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_file_email", ContractSHA256: "3c91521147ec9c087a6c04612ba0ecd63a30248f3afa6691850437f0ad154483", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "6f25b4a2fc713dbb566f56a7a1ae5121ea5a3740710f4cdf3618da0492c6eb34", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	smtp connector.SMTPTransport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("SMTP transport is required")
	}
	smtpTransport, ok := transport.(connector.SMTPTransport)
	if !ok {
		return nil, errors.New("Runtime transport does not provide SMTP capability")
	}
	p := &provider{smtp: smtpTransport}
	send, err := connector.BindEnqueueDelivery(SendEmail, p.send)
	if err != nil {
		return nil, err
	}
	sendFile, err := connector.BindEnqueueDelivery(SendFileEmail, p.sendFile)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), send, sendFile, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	minPort, maxPort, minTimeout, maxTimeout := float64(1), float64(65535), float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "host", Name: "SMTP Host", Type: connector.ConfigFieldText, Required: true}, {Key: "port", Name: "Port", Type: connector.ConfigFieldInteger, Required: true, Default: json.RawMessage(`25`), Validation: connector.ConfigValidation{Min: &minPort, Max: &maxPort}}, {Key: "from_email", Name: "From Address", Type: connector.ConfigFieldEmail}, {Key: "from_name", Name: "From Name", Type: connector.ConfigFieldText}, {Key: "username", Name: "Username", Type: connector.ConfigFieldText}, {Key: "tls", Name: "Implicit TLS", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}, {Key: "start_tls", Name: "STARTTLS", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}}, {Key: "tls_server_name", Name: "TLS Server Name", Type: connector.ConfigFieldText}, {Key: "tls_ca_pem", Name: "TLS CA Certificate", Type: connector.ConfigFieldText}}, SecretFields: []connector.SecretField{{Key: "password", Name: "Password", CredentialKind: connector.SecretCredentialBasicAuthPassword, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	host := config(connection, "host")
	if host == "" || strings.ContainsAny(host, "/\r\n\t ") {
		return permanent("host_required", "valid SMTP host is required")
	}
	port := intValue(connection.Config["port"], 25)
	if port < 1 || port > 65535 {
		return permanent("port_invalid", "SMTP port must be between 1 and 65535")
	}
	implicit, start := boolValue(connection.Config["tls"]), boolValue(connection.Config["start_tls"])
	if implicit && start {
		return permanent("tls_mode_conflict", "implicit TLS and STARTTLS are mutually exclusive")
	}
	username, from := config(connection, "username"), config(connection, "from_email")
	if from == "" {
		from = username
	}
	if from == "" || !validAddress(from) {
		return permanent("from_address_required", "valid from_email or username is required")
	}
	if ca := config(connection, "tls_ca_pem"); ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return permanent("tls_ca_invalid", "TLS CA PEM is invalid")
		}
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) send(ctx context.Context, r connector.TypedRequest[SendEmailInput]) (connector.DeliveryResult, error) {
	from, recipients, err := addresses(r.Connection, r.Input.To)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	messageID := messageID(r.RequestRef)
	message := textMessage(from, recipients, r.Input.Subject, r.Input.Text, r.Input.HTML, messageID)
	return p.deliver(ctx, r.Connection, r.Secrets, from, recipients, message, messageID)
}
func (p *provider) sendFile(ctx context.Context, r connector.TypedRequest[SendFileEmailInput]) (connector.DeliveryResult, error) {
	from, recipients, err := addresses(r.Connection, r.Input.To)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	content, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.Input.AttachmentBase64))
	if err != nil || len(content) == 0 {
		return connector.DeliveryResult{}, permanent("attachment_unavailable", "runtime attachment is unavailable")
	}
	id := messageID(r.RequestRef)
	message := fileMessage(from, recipients, r.Input.Subject, r.Input.Text, id, r.Input.AttachmentFilename, r.Input.AttachmentContentType, content)
	return p.deliver(ctx, r.Connection, r.Secrets, from, recipients, message, id)
}
func (p *provider) deliver(ctx context.Context, connection connector.Connection, secrets map[string]string, from string, recipients []string, message []byte, id string) (connector.DeliveryResult, error) {
	if config(connection, "username") != "" && secrets["password"] == "" {
		return connector.DeliveryResult{}, permanent("password_required", "password is required when username is configured")
	}
	request := smtpRequest(connection, secrets)
	request.EnvelopeFrom, request.Recipients, request.Message = from, recipients, message
	result, err := p.smtp.SendSMTP(ctx, request)
	if err != nil {
		return connector.DeliveryResult{}, connector.UncertainError("smtp.delivery_uncertain", err)
	}
	if !result.Accepted {
		return connector.DeliveryResult{}, connector.PermanentError("smtp.delivery_rejected", errors.New("SMTP server did not accept the message"))
	}
	return connector.DeliveryResult{ResponseRef: "smtp:" + id}, nil
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(r.Connection); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	request := smtpRequest(r.Connection, r.Secrets)
	request.ProbeOnly = true
	result, err := p.smtp.SendSMTP(ctx, request)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, connector.RetryableError("smtp.connection_failed", err)
	}
	if !result.Connected {
		return connector.TypedResult[map[string]any]{}, connector.RetryableError("smtp.connection_failed", errors.New("SMTP server did not confirm the connection"))
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": result.Connected}, ResponseRef: "smtp:connected"}, nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func smtpRequest(connection connector.Connection, secrets map[string]string) connector.SMTPRequest {
	return connector.SMTPRequest{Host: config(connection, "host"), Port: intValue(connection.Config["port"], 25), ImplicitTLS: boolValue(connection.Config["tls"]), StartTLS: boolValue(connection.Config["start_tls"]), TLSServerName: config(connection, "tls_server_name"), TLSCAPEM: config(connection, "tls_ca_pem"), Username: config(connection, "username"), SecretPassword: secrets["password"], Timeout: time.Duration(intValue(connection.Config["timeout_seconds"], 30)) * time.Second}
}
func addresses(connection connector.Connection, to []string) (string, []string, error) {
	from := config(connection, "from_email")
	if from == "" {
		from = config(connection, "username")
	}
	if !validAddress(from) || len(to) == 0 {
		return "", nil, permanent("address_required", "valid sender and recipients are required")
	}
	recipients := make([]string, 0, len(to))
	for _, address := range to {
		address = strings.TrimSpace(address)
		if !validAddress(address) {
			return "", nil, permanent("address_invalid", "email address is invalid")
		}
		recipients = append(recipients, address)
	}
	return from, recipients, nil
}
func validAddress(value string) bool {
	parsed, err := mail.ParseAddress(strings.TrimSpace(value))
	return err == nil && parsed.Address == strings.TrimSpace(value)
}
func messageID(local string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(local)))
	return "<" + hex.EncodeToString(digest[:16]) + "@domainry-runtime>"
}
func textMessage(from string, to []string, subject, text, html, id string) []byte {
	header := headers(from, to, subject, id)
	if strings.TrimSpace(html) == "" {
		return []byte(header + "Content-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n" + quoted(text))
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.SetBoundary("domainry-notification-boundary")
	writePart(writer, "text/plain; charset=UTF-8", text)
	writePart(writer, "text/html; charset=UTF-8", html)
	_ = writer.Close()
	return []byte(header + "Content-Type: multipart/alternative; boundary=\"" + writer.Boundary() + "\"\r\n\r\n" + body.String())
}
func fileMessage(from string, to []string, subject, text, id, filename, contentType string, content []byte) []byte {
	filename = strings.ReplaceAll(strings.ReplaceAll(filepath.Base(filename), "\r", ""), "\n", "")
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.SetBoundary("domainry-file-delivery-boundary")
	writePart(writer, "text/plain; charset=UTF-8", text)
	part, err := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {contentType}, "Content-Disposition": {`attachment; filename="` + filename + `"`}, "Content-Transfer-Encoding": {"base64"}})
	if err == nil {
		encoder := base64.NewEncoder(base64.StdEncoding, part)
		_, _ = encoder.Write(content)
		_ = encoder.Close()
	}
	_ = writer.Close()
	return []byte(headers(from, to, subject, id) + "Content-Type: multipart/mixed; boundary=\"" + writer.Boundary() + "\"\r\n\r\n" + body.String())
}
func headers(from string, to []string, subject, id string) string {
	subject = strings.ReplaceAll(strings.ReplaceAll(subject, "\r", ""), "\n", "")
	return "From: " + from + "\r\nTo: " + strings.Join(to, ", ") + "\r\nSubject: " + mime.QEncoding.Encode("UTF-8", subject) + "\r\nMessage-ID: " + id + "\r\nMIME-Version: 1.0\r\n"
}
func writePart(writer *multipart.Writer, contentType, content string) {
	part, err := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {contentType}, "Content-Transfer-Encoding": {"quoted-printable"}})
	if err == nil {
		_, _ = part.Write([]byte(quoted(content)))
	}
}
func quoted(content string) string {
	var output bytes.Buffer
	writer := quotedprintable.NewWriter(&output)
	_, _ = writer.Write([]byte(content))
	_ = writer.Close()
	return output.String()
}
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		var result int
		if _, err := fmt.Sscan(typed.String(), &result); err == nil {
			return result
		}
	}
	return fallback
}
func boolValue(value any) bool { result, _ := value.(bool); return result }
func permanent(code, message string) error {
	return connector.PermanentError("smtp."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
