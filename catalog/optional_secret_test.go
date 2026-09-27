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
		for _, rotation := range []string{"manual", "oauth_refresh"} {
			document.Providers[0].SecretFields[0].Config["test_requirement"] = "when_bound"
			document.Providers[0].SecretFields[0].Config["rotation_policy"] = rotation
			if err := validateConnectorDefinition("expense_ocr", "expense_ocr", document); err != nil {
				t.Fatalf("valid SDK rotation policy %q rejected: %v", rotation, err)
			}
		}
		document.Providers[0].SecretFields[0].Config["rotation_policy"] = "background_guess"
		if err := validateConnectorDefinition("expense_ocr", "expense_ocr", document); err == nil {
			t.Fatal("unknown secret rotation policy accepted")
		}
		document.Providers[0].SecretFields[0].Config["rotation_policy"] = "manual"
		document.Providers[0].SecretFields[0].Config["test_requirement"] = "never_validate"
		if err := validateConnectorDefinition("expense_ocr", "expense_ocr", document); err == nil {
			t.Fatal("unknown secret test policy accepted")
		}
		return
	}
	t.Fatal("expense_ocr definition is missing")
}
