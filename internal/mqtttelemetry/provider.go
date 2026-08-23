// Package mqtttelemetry contains the shared Provider policy for generic MQTT
// and AWS IoT Core telemetry. Network and MQTT protocol execution stay in the
// Runtime-owned connector.MQTTTransport.
package mqtttelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey        = "iot_telemetry"
	publishHash         = "b6fb13a6411ad45e592310c76fba08e7ca986a148b10a9ed5f270afdc7e30714"
	testHash            = "dcb85be24768ffdff7ccda9db4c93254baac73b864885b7ad1527d2c48e401ac"
	defaultTopic        = "devices/{device_id}/telemetry"
	defaultTimeout      = 15
	maximumTimeout      = 300
	maximumPayloadBytes = 1 << 20
)

type PublishInput struct {
	DeviceID  string         `json:"device_id"`
	EventID   string         `json:"event_id"`
	Metrics   map[string]any `json:"metrics"`
	Timestamp any            `json:"timestamp,omitempty"`
}
type PublishOutput struct {
	Accepted bool   `json:"accepted"`
	PacketID uint16 `json:"packet_id"`
}
type TestOutput struct {
	Connected bool `json:"connected"`
}

type Options struct {
	ProviderKey, ProviderName            string
	RequireTLS, RequireClientCertificate bool
}

func PublishOperation(providerKey string) connector.CallOperation[PublishInput, PublishOutput] {
	return connector.CallOperation[PublishInput, PublishOutput]{ConnectorKey: ConnectorKey, ProviderKey: providerKey, Key: "publish_telemetry", ContractSHA256: publishHash, Reliability: reliability(connector.EffectWrite, connector.IdempotencyNone)}
}
func TestOperation(providerKey string) connector.CallOperation[struct{}, TestOutput] {
	return connector.CallOperation[struct{}, TestOutput]{ConnectorKey: ConnectorKey, ProviderKey: providerKey, Key: "test_connection", ContractSHA256: testHash, Reliability: reliability(connector.EffectRead, connector.IdempotencyNatural)}
}
func reliability(effect connector.OperationEffect, strategy connector.IdempotencyStrategy) connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: strategy}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	options   Options
}

func New(transport connector.Transport, options Options, publish connector.CallOperation[PublishInput, PublishOutput], test connector.CallOperation[struct{}, TestOutput]) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("MQTT transport is required")
	}
	if strings.TrimSpace(options.ProviderKey) == "" || publish.ProviderKey != options.ProviderKey || test.ProviderKey != options.ProviderKey {
		return nil, errors.New("MQTT Provider identity is invalid")
	}
	p := &provider{transport: transport, options: options}
	publishBound, err := connector.BindCall(publish, p.publish)
	if err != nil {
		return nil, err
	}
	testBound, err := connector.BindCall(test, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(options), publishBound, testBound)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema(options Options) connector.ProviderSchema {
	minimum, maximum := float64(1), float64(maximumTimeout)
	localized := func(en, zh string) map[string]connector.FieldLocalization {
		return map[string]connector.FieldLocalization{"en-US": {Name: en, Description: en}, "zh-CN": {Name: zh, Description: zh}}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: options.ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "broker_url", Name: "MQTT Broker URL", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{Pattern: `^mqtts?://[^\s]+$`}, I18n: localized("MQTT Broker URL", "MQTT 服务地址")},
		{Key: "client_id", Name: "Client ID", Type: connector.ConfigFieldText, I18n: localized("Client ID", "客户端 ID")},
		{Key: "topic", Name: "Telemetry Topic", Type: connector.ConfigFieldText, Default: json.RawMessage(`"devices/{device_id}/telemetry"`), I18n: localized("Telemetry Topic", "遥测主题")},
		{Key: "tls_server_name", Name: "TLS Server Name", Type: connector.ConfigFieldText, I18n: localized("TLS Server Name", "TLS 服务器名称")},
		{Key: "tls_ca_pem", Name: "TLS CA Certificate", Type: connector.ConfigFieldText, I18n: localized("TLS CA Certificate", "TLS CA 证书")},
		{Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}, I18n: localized("Timeout Seconds", "超时秒数")},
	}, SecretFields: []connector.SecretField{
		secret("mqtt_username", "MQTT Username", "MQTT 用户名", false, connector.SecretCredentialIdentifier),
		secret("mqtt_password", "MQTT Password", "MQTT 密码", false, connector.SecretCredentialBasicAuthPassword),
		secret("certificate", "Client Certificate", "客户端证书", options.RequireClientCertificate, connector.SecretCredentialCertificate),
		secret("private_key", "Client Private Key", "客户端私钥", options.RequireClientCertificate, connector.SecretCredentialPrivateKey),
	}}
}

func secret(key, en, zh string, required bool, kind connector.SecretCredentialKind) connector.SecretField {
	format := connector.SecretMaterialOpaque
	if kind == connector.SecretCredentialCertificate || kind == connector.SecretCredentialPrivateKey {
		format = connector.SecretMaterialPEM
	}
	return connector.SecretField{Key: key, Name: en, I18n: map[string]connector.FieldLocalization{"en-US": {Name: en, Description: en}, "zh-CN": {Name: zh, Description: zh}}, Required: required, CredentialKind: kind, MaterialFormat: format, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(config(connection, "broker_url"))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "mqtt" && parsed.Scheme != "mqtts") {
		return permanent("broker_invalid", "valid MQTT broker URL is required")
	}
	if p.options.RequireTLS && parsed.Scheme != "mqtts" {
		return permanent("tls_required", "TLS is required")
	}
	if parsed.Scheme == "mqtt" && !loopback(parsed.Hostname()) {
		return permanent("plaintext_forbidden", "plaintext MQTT is restricted to loopback")
	}
	if topic := configDefault(connection, "topic", defaultTopic); topic == "" || len(topic) > 65535 || strings.ContainsAny(topic, "\x00#+") {
		return permanent("topic_invalid", "publish topic template is invalid")
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[TestOutput], error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, "", nil, true)
	if err != nil {
		return connector.TypedResult[TestOutput]{}, err
	}
	return connector.TypedResult[TestOutput]{Output: TestOutput{Connected: result.Connected}, ResponseRef: "mqtt:connected"}, nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: result.Output.Connected, Details: details}, nil
}
func (p *provider) publish(ctx context.Context, request connector.TypedRequest[PublishInput]) (connector.TypedResult[PublishOutput], error) {
	deviceID, eventID := strings.TrimSpace(request.Input.DeviceID), strings.TrimSpace(request.Input.EventID)
	if deviceID == "" || eventID == "" || len(request.Input.Metrics) == 0 {
		return connector.TypedResult[PublishOutput]{}, permanent("payload_invalid", "device_id, event_id and metrics are required")
	}
	payload := map[string]any{"device_id": deviceID, "event_id": eventID, "metrics": request.Input.Metrics}
	if request.Input.Timestamp != nil {
		payload["timestamp"] = request.Input.Timestamp
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > maximumPayloadBytes {
		return connector.TypedResult[PublishOutput]{}, permanent("payload_invalid", "telemetry payload is invalid or too large")
	}
	topic := strings.ReplaceAll(configDefault(request.Connection, "topic", defaultTopic), "{device_id}", deviceID)
	if strings.ContainsAny(topic, "\x00#+") || len(topic) > 65535 {
		return connector.TypedResult[PublishOutput]{}, permanent("topic_invalid", "resolved publish topic is invalid")
	}
	result, err := p.execute(ctx, request.Connection, request.Secrets, topic, raw, false)
	if err != nil {
		return connector.TypedResult[PublishOutput]{}, err
	}
	return connector.TypedResult[PublishOutput]{Output: PublishOutput{Accepted: result.Accepted, PacketID: result.PacketID}, ResponseRef: fmt.Sprintf("mqtt:%d", result.PacketID)}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, topic string, payload []byte, probe bool) (connector.MQTTResult, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.MQTTResult{}, err
	}
	if p.options.RequireClientCertificate && (strings.TrimSpace(secrets["certificate"]) == "" || strings.TrimSpace(secrets["private_key"]) == "") {
		return connector.MQTTResult{}, permanent("client_certificate_required", "client certificate and private key are required")
	}
	mqttTransport, ok := p.transport.(connector.MQTTTransport)
	if !ok {
		return connector.MQTTResult{}, permanent("transport_unavailable", "Runtime does not provide MQTT transport capability")
	}
	result, err := mqttTransport.ExecuteMQTT(ctx, connector.MQTTRequest{BrokerURL: config(connection, "broker_url"), ClientID: config(connection, "client_id"), Topic: topic, Payload: payload, QoS: 1, ProbeOnly: probe, Timeout: time.Duration(integer(connection.Config["timeout_seconds"], defaultTimeout)) * time.Second, TLSServerName: config(connection, "tls_server_name"), TLSCAPEM: config(connection, "tls_ca_pem"), SecretUsername: secrets["mqtt_username"], SecretPassword: secrets["mqtt_password"], SecretCertificate: secrets["certificate"], SecretPrivateKey: secrets["private_key"]})
	if err != nil {
		if probe {
			return connector.MQTTResult{}, connector.RetryableError("mqtt.connection_failed", err)
		}
		return connector.MQTTResult{}, connector.UncertainError("mqtt.publish_uncertain", err)
	}
	if !result.Connected || (!probe && !result.Accepted) {
		return connector.MQTTResult{}, permanent("result_invalid", "Runtime MQTT result is invalid")
	}
	return result, nil
}

func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func configDefault(connection connector.Connection, key, fallback string) string {
	if value := config(connection, key); value != "" {
		return value
	}
	return fallback
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := strconv.Atoi(typed.String()); err == nil {
			return parsed
		}
	}
	return fallback
}
func loopback(host string) bool { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func permanent(code, message string) error {
	return connector.PermanentError("mqtt."+code, errors.New(message))
}
