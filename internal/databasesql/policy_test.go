package databasesql

import "testing"

func TestReadOnlyPolicyRejectsWritesAndQuotedFalsePositives(t *testing.T) {
	policy := ReadOnlyPolicy{AllowedFirstKeywords: []string{"select", "with"}, DeniedKeywords: []string{"insert", "update", "delete", "drop"}, QuotePairs: map[rune]rune{'\'': '\'', '"': '"', '`': '`'}, KeywordRules: []KeywordRule{{Sequence: []string{"for", "update"}, Code: LockingReadDenied}}}
	accepted := []string{"SELECT id FROM users", "SELECT 'drop table users' AS text", "SELECT `update` FROM users;"}
	for _, query := range accepted {
		if code := ValidateReadOnly(query, policy); code != "" {
			t.Fatalf("query=%q code=%s", query, code)
		}
	}
	rejected := map[string]string{"": QueryUnsafe, "SELECT 1; SELECT 2": MultipleStatements, "UPDATE users SET active=0": ReadOnlyQueryRequired, "WITH removed AS (DELETE FROM users RETURNING id) SELECT * FROM removed": WriteKeywordDenied, "SELECT * FROM users FOR UPDATE": LockingReadDenied}
	for query, want := range rejected {
		if got := ValidateReadOnly(query, policy); got != want {
			t.Fatalf("query=%q got=%s want=%s", query, got, want)
		}
	}
}
