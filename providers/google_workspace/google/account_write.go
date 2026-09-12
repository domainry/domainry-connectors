package google

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

func validateWriteEnvelope(c connector.Connection, ref string) error {
	if strings.TrimSpace(c.WorkspaceID) == "" || strings.TrimSpace(c.Key) == "" || ref == "" || strings.TrimSpace(ref) != ref || len(ref) > 2048 || !utf8.ValidString(ref) || strings.ContainsFunc(ref, unicode.IsControl) {
		return permanent("write.identity_required", "account write requires a trusted connection and request reference")
	}
	return nil
}

// Correlation is deterministic and scoped to the host invocation. Neither a
// Google event ID nor an RFC Message-ID promises deduplication or permits replay.
// Integration owns the durable claim and exact parameter fingerprint.
func writeCorrelation(c connector.Connection, ref, operation, target string) string {
	b, _ := json.Marshal([]string{ConnectorKey, ProviderKey, c.WorkspaceID, c.Key, ref, operation, target})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func mergeWriteState(previous, current connector.TypedResult[Response]) connector.TypedResult[Response] {
	updates := cloneStrings(previous.SecretUpdates)
	for k, v := range current.SecretUpdates {
		updates[k] = v
	}
	current.SecretUpdates = updates
	return current
}

func writeSecrets(initial map[string]string, state connector.TypedResult[Response]) map[string]string {
	secrets := cloneStrings(initial)
	for k, v := range state.SecretUpdates {
		secrets[k] = v
	}
	return secrets
}
