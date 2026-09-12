package microsoft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func validateWriteEnvelope(c connector.Connection, ref string) error {
	if strings.TrimSpace(c.WorkspaceID) == "" || strings.TrimSpace(c.Key) == "" || ref == "" || strings.TrimSpace(ref) != ref || len(ref) > 2048 || !utf8.ValidString(ref) || strings.ContainsFunc(ref, unicode.IsControl) {
		return permanent("write.identity_required", "account write requires a trusted connection and invocation reference")
	}
	return nil
}

// The native transactionId is a correlation hint. Without a documented key
// retention promise it does not authorize replay; Integration owns that claim.
func writeCorrelation(c connector.Connection, ref, operation, target string) string {
	b, _ := json.Marshal([]string{ConnectorKey, ProviderKey, c.WorkspaceID, c.Key, ref, operation, target})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func writeSecrets(initial map[string]string, state connector.TypedResult[Response]) map[string]string {
	out := map[string]string{}
	for k, v := range initial {
		out[k] = v
	}
	for k, v := range state.SecretUpdates {
		out[k] = v
	}
	return out
}

func mergeWriteState(previous, current connector.TypedResult[Response]) connector.TypedResult[Response] {
	current.SecretUpdates = writeSecrets(previous.SecretUpdates, current)
	return current
}
