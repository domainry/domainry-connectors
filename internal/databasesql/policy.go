// Package databasesql contains private SQL safety policy shared by official
// external-database Providers. Runtime remains responsible for drivers and I/O.
package databasesql

import (
	"strings"
	"unicode"
)

const (
	QueryUnsafe           = "query_unsafe"
	MultipleStatements    = "multiple_statements_denied"
	ReadOnlyQueryRequired = "readonly_query_required"
	WriteKeywordDenied    = "write_keyword_denied"
	LockingReadDenied     = "locking_read_denied"
	SelectIntoDenied      = "select_into_denied"
)

type KeywordRule struct {
	Sequence []string
	Code     string
}
type ReadOnlyPolicy struct {
	AllowedFirstKeywords []string
	DeniedKeywords       []string
	UnsafeFragments      []string
	QuotePairs           map[rune]rune
	KeywordRules         []KeywordRule
}

func ValidateReadOnly(query string, policy ReadOnlyPolicy) string {
	query = strings.TrimSpace(query)
	if query == "" || strings.ContainsRune(query, '\x00') || containsAny(query, append([]string{"--", "/*", "*/"}, policy.UnsafeFragments...)) {
		return QueryUnsafe
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if strings.Contains(trimmed, ";") {
		return MultipleStatements
	}
	words := keywords(trimmed, policy.QuotePairs)
	if len(words) == 0 || !contains(policy.AllowedFirstKeywords, words[0]) {
		return ReadOnlyQueryRequired
	}
	for _, rule := range policy.KeywordRules {
		if containsSequence(words, rule.Sequence) {
			return rule.Code
		}
	}
	denied := map[string]struct{}{}
	for _, keyword := range policy.DeniedKeywords {
		denied[keyword] = struct{}{}
	}
	for _, word := range words {
		if _, exists := denied[word]; exists {
			return WriteKeywordDenied
		}
	}
	return ""
}
func keywords(query string, pairs map[rune]rune) []string {
	words := []string{}
	var current strings.Builder
	quoteEnd := rune(0)
	flush := func() {
		if current.Len() > 0 {
			words = append(words, strings.ToLower(current.String()))
			current.Reset()
		}
	}
	for _, char := range query {
		if quoteEnd != 0 {
			if char == quoteEnd {
				quoteEnd = 0
			}
			continue
		}
		if end, quoted := pairs[char]; quoted {
			flush()
			quoteEnd = end
			continue
		}
		if unicode.IsLetter(char) || char == '_' {
			current.WriteRune(char)
		} else {
			flush()
		}
	}
	flush()
	return words
}
func containsAny(value string, fragments []string) bool {
	for _, fragment := range fragments {
		if fragment != "" && strings.Contains(value, fragment) {
			return true
		}
	}
	return false
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func containsSequence(words, sequence []string) bool {
	if len(sequence) == 0 || len(sequence) > len(words) {
		return false
	}
	for start := 0; start <= len(words)-len(sequence); start++ {
		matched := true
		for offset, expected := range sequence {
			if words[start+offset] != expected {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
