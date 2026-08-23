package journal

import (
	"math"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func validEntry() Entry {
	return Entry{BatchKey: "batch", EntryKey: "entry", TransactionDate: "2026-08-12", Currency: "jpy", Lines: []Line{
		{Side: "debit", Amount: 100, AccountID: "1", TaxCode: "2"},
		{Side: "debit", Amount: 200, AccountID: "3", TaxCode: "4"},
		{Side: "credit", Amount: 300, AccountID: "5", TaxCode: "6"},
	}}
}

func TestValidateAndPairBalancedEntry(t *testing.T) {
	entry, err := Validate(validEntry())
	if err != nil || entry.Currency != "JPY" {
		t.Fatalf("entry=%+v error=%v", entry, err)
	}
	branches := PairBranches(entry)
	if len(branches) != 2 || branches[0].Debitor.Amount != 100 || branches[0].Creditor.Amount != 100 || branches[1].Debitor.Amount != 200 {
		t.Fatalf("branches=%+v", branches)
	}
}

func TestValidateRejectsInvalidAndOverflowingEntries(t *testing.T) {
	tests := []Entry{
		{EntryKey: "entry", TransactionDate: "2026-08-12", Currency: "JPY"},
		{BatchKey: "batch", EntryKey: "entry", TransactionDate: "12/08/2026", Currency: "JPY"},
		{BatchKey: "batch", EntryKey: "entry", TransactionDate: "2026-08-12", Currency: "USD"},
		{BatchKey: "batch", EntryKey: "entry", TransactionDate: "2026-08-12", Currency: "JPY", Lines: []Line{{Side: "debit", Amount: math.MaxInt64, AccountID: "1", TaxCode: "2"}, {Side: "debit", Amount: 1, AccountID: "1", TaxCode: "2"}, {Side: "credit", Amount: math.MaxInt64, AccountID: "3", TaxCode: "4"}, {Side: "credit", Amount: 1, AccountID: "3", TaxCode: "4"}}},
	}
	for _, input := range tests {
		_, err := Validate(input)
		if code, ok := connector.ProviderErrorCodeOf(err); !ok || code == "" {
			t.Fatalf("input accepted or error unclassified: %+v error=%v", input, err)
		}
	}
}
