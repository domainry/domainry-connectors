package mqtttelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.MQTTRequest
	result   connector.MQTTResult
	err      error
}

func (*recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func (t *recordingTransport) ExecuteMQTT(_ context.Context, request connector.MQTTRequest) (connector.MQTTResult, error) {
	t.requests = append(t.requests, request)
	return t.result, t.err
}

func TestGenericAndAWSProvidersUseRuntimeMQTT(t *testing.T) {
	for _, options := range []Options{{ProviderKey: "mqtt", ProviderName: "MQTT"}, {ProviderKey: "aws_iot", ProviderName: "AWS IoT Core", RequireTLS: true, RequireClientCertificate: true}} {
		t.Run(options.ProviderKey, func(t *testing.T) {
			transport := &recordingTransport{result: connector.MQTTResult{Connected: true, Accepted: true, PacketID: 7}}
			adapter, err := New(transport, options, PublishOperation(options.ProviderKey), TestOperation(options.ProviderKey))
			if err != nil || contracttest.ValidateAdapter(adapter) != nil {
				t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
			}
			connection := validConnection(options.RequireTLS)
			secrets := map[string]string{"mqtt_username": "runtime-user", "mqtt_password": "runtime-password"}
			if options.RequireClientCertificate {
				secrets["certificate"], secrets["private_key"] = "certificate", "private-key"
			}
			payload, _ := json.Marshal(PublishInput{DeviceID: "device-1", EventID: "event-1", Metrics: map[string]any{"temperature": 21}, Timestamp: "2026-08-24T00:00:00Z"})
			result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: options.ProviderKey, OperationKey: "publish_telemetry", ContractSHA256: publishHash, Mode: connector.ModeCall, Connection: connection, Secrets: secrets, Payload: payload})
			if callErr != nil || result.ResponseRef != "mqtt:7" {
				t.Fatalf("result=%+v err=%v", result, callErr)
			}
			request := transport.requests[0]
			if request.Topic != "devices/device-1/telemetry" || request.QoS != 1 || request.SecretPassword != "runtime-password" || request.SecretPrivateKey != secrets["private_key"] {
				t.Fatalf("request=%+v", request)
			}
			public, _ := json.Marshal(request)
			for _, secret := range []string{"runtime-user", "runtime-password", "private-key"} {
				if string(public) != "" && contains(string(public), secret) {
					t.Fatalf("serialized request leaks secret: %s", public)
				}
			}
		})
	}
}

func TestValidationAndFailureSemantics(t *testing.T) {
	transport := &recordingTransport{result: connector.MQTTResult{Connected: true}}
	adapter, _ := New(transport, Options{ProviderKey: "mqtt"}, PublishOperation("mqtt"), TestOperation("mqtt"))
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{{"broker_url": "mqtt://remote.example"}, {"broker_url": "http://localhost"}, {"broker_url": "mqtts://user:pass@broker.example"}, {"broker_url": "mqtts://broker.example?x=1"}, {"broker_url": "mqtts://broker.example", "topic": "devices/#"}, {"broker_url": "mqtts://broker.example", "timeout_seconds": 301}} {
		if validator.ValidateConfig(connector.Connection{Config: config}) == nil {
			t.Fatalf("accepted=%v", config)
		}
	}
	aws, _ := New(transport, Options{ProviderKey: "aws_iot", RequireTLS: true, RequireClientCertificate: true}, PublishOperation("aws_iot"), TestOperation("aws_iot"))
	if aws.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"broker_url": "mqtt://localhost"}}) == nil {
		t.Fatal("AWS plaintext accepted")
	}
	payload, _ := json.Marshal(PublishInput{DeviceID: "d", EventID: "e", Metrics: map[string]any{"x": 1}})
	_, err := aws.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: "aws_iot", OperationKey: "publish_telemetry", ContractSHA256: publishHash, Mode: connector.ModeCall, Connection: validConnection(true), Payload: payload})
	if err == nil {
		t.Fatal("AWS missing client certificate accepted")
	}
	transport.err = errors.New("ack lost")
	generic, _ := New(transport, Options{ProviderKey: "mqtt"}, PublishOperation("mqtt"), TestOperation("mqtt"))
	_, err = generic.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: "mqtt", OperationKey: "publish_telemetry", ContractSHA256: publishHash, Mode: connector.ModeCall, Connection: validConnection(false), Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}

func validConnection(tls bool) connector.Connection {
	scheme := "mqtt"
	host := "localhost:1883"
	if tls {
		scheme, host = "mqtts", "broker.example:8883"
	}
	return connector.Connection{Config: map[string]any{"broker_url": scheme + "://" + host, "topic": defaultTopic, "timeout_seconds": 15}}
}
func contains(value, target string) bool {
	for index := 0; index+len(target) <= len(value); index++ {
		if value[index:index+len(target)] == target {
			return true
		}
	}
	return false
}
