package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// OperationContractSHA256 is the stable Connector operation wire identity.
// Provider identity and revision are deliberately outside this hash because
// multiple Providers may implement the same Connectors-owned operation.
func OperationContractSHA256(connectorKey, _ string, operation ConnectorOperationSchema) (string, error) {
	type fieldContract struct {
		Key          string          `json:"key"`
		Type         string          `json:"type"`
		Required     bool            `json:"required"`
		Default      any             `json:"default,omitempty"`
		DefaultValue any             `json:"default_value,omitempty"`
		Validation   FieldValidation `json:"validation,omitempty"`
		Options      any             `json:"options,omitempty"`
	}
	convertFields := func(fields []FieldSchema) []fieldContract {
		result := make([]fieldContract, 0, len(fields))
		for _, field := range fields {
			result = append(result, fieldContract{Key: field.Key, Type: field.Type, Required: field.Required, Default: field.Default, DefaultValue: field.DefaultValue, Validation: field.Validation, Options: field.Options})
		}
		return result
	}
	payload := struct {
		ConnectorKey         string          `json:"connector_key"`
		Key                  string          `json:"key"`
		Method               string          `json:"method"`
		ExecutionMode        string          `json:"execution_mode"`
		SideEffect           string          `json:"side_effect"`
		Input                []fieldContract `json:"input"`
		Output               []fieldContract `json:"output"`
		IdempotencySupported bool            `json:"idempotency_supported"`
		Compensation         string          `json:"compensation_operation"`
	}{connectorKey, operation.Key, operation.Method, operation.ExecutionMode, operation.SideEffect, convertFields(operation.Input), convertFields(operation.Output), operation.IdempotencySupported, operation.CompensationOperation}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode Connector %s operation %s contract: %w", connectorKey, operation.Key, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
