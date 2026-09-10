package catalog

import "testing"

func TestSecretTestPolicyMatchesSDKForProvidersWithoutReadinessProbe(t *testing.T) {
	documents, err := DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range documents {
		if document.Key != "expense_ocr" {
			continue
		}
		for _, policy := range []string{"optional", "when_bound"} {
			document.Providers[0].SecretFields[0].Config["test_requirement"] = policy
			if err := validateConnectorDefinition("expense_ocr", "expense_ocr", document); err != nil {
				t.Fatalf("valid SDK policy %q rejected: %v", policy, err)
			}
		}
		document.Providers[0].SecretFields[0].Config["test_requirement"] = "never_validate"
		if err := validateConnectorDefinition("expense_ocr", "expense_ocr", document); err == nil {
			t.Fatal("unknown secret test policy accepted")
		}
		return
	}
	t.Fatal("expense_ocr definition is missing")
}
