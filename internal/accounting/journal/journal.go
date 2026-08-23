// Package journal contains the provider-neutral journal validation used by
// official accounting Providers. It is internal implementation, not SDK API.
package journal

import (
	"errors"
	"math"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

type Entry struct {
	BatchKey        string
	EntryKey        string
	TransactionDate string
	Currency        string
	Memo            string
	Lines           []Line
}

type Line struct {
	Side         string
	Amount       int64
	AccountID    string
	TaxCode      string
	DepartmentID string
	SubAccountID string
	PartnerCode  string
	Description  string
	InvoiceKind  string
}

type Branch struct {
	Debitor  Line
	Creditor Line
}

func Validate(input Entry) (Entry, error) {
	entry := input
	entry.BatchKey = strings.TrimSpace(entry.BatchKey)
	entry.EntryKey = strings.TrimSpace(entry.EntryKey)
	entry.TransactionDate = strings.TrimSpace(entry.TransactionDate)
	entry.Currency = strings.ToUpper(strings.TrimSpace(entry.Currency))
	entry.Memo = strings.TrimSpace(entry.Memo)
	if entry.BatchKey == "" {
		return Entry{}, invalid("batch_key_required")
	}
	if entry.EntryKey == "" {
		return Entry{}, invalid("entry_key_required")
	}
	if parsed, err := time.Parse("2006-01-02", entry.TransactionDate); err != nil || parsed.Format("2006-01-02") != entry.TransactionDate {
		return Entry{}, invalid("transaction_date_invalid")
	}
	if entry.Currency != "JPY" {
		return Entry{}, invalid("currency_unsupported")
	}
	if len(entry.Lines) < 2 || len(entry.Lines) > 100 {
		return Entry{}, invalid("lines_invalid")
	}
	var debit, credit int64
	for index := range entry.Lines {
		line := &entry.Lines[index]
		line.Side = strings.ToLower(strings.TrimSpace(line.Side))
		line.AccountID = strings.TrimSpace(line.AccountID)
		line.TaxCode = strings.TrimSpace(line.TaxCode)
		line.DepartmentID = strings.TrimSpace(line.DepartmentID)
		line.SubAccountID = strings.TrimSpace(line.SubAccountID)
		line.PartnerCode = strings.TrimSpace(line.PartnerCode)
		line.Description = strings.TrimSpace(line.Description)
		line.InvoiceKind = strings.TrimSpace(line.InvoiceKind)
		if line.Amount < 1 || line.AccountID == "" || line.TaxCode == "" || (line.Side != "debit" && line.Side != "credit") {
			return Entry{}, invalid("line_invalid")
		}
		if line.Side == "debit" {
			if debit > math.MaxInt64-line.Amount {
				return Entry{}, invalid("amount_overflow")
			}
			debit += line.Amount
		} else {
			if credit > math.MaxInt64-line.Amount {
				return Entry{}, invalid("amount_overflow")
			}
			credit += line.Amount
		}
	}
	if debit == 0 || credit == 0 || debit != credit {
		return Entry{}, invalid("unbalanced")
	}
	return entry, nil
}

func PairBranches(entry Entry) []Branch {
	debits, credits := splitSides(entry.Lines)
	branches := make([]Branch, 0, len(debits)+len(credits)-1)
	for debitIndex, creditIndex := 0, 0; debitIndex < len(debits) && creditIndex < len(credits); {
		amount := debits[debitIndex].Amount
		if credits[creditIndex].Amount < amount {
			amount = credits[creditIndex].Amount
		}
		debit, credit := debits[debitIndex], credits[creditIndex]
		debit.Amount, credit.Amount = amount, amount
		branches = append(branches, Branch{Debitor: debit, Creditor: credit})
		debits[debitIndex].Amount -= amount
		credits[creditIndex].Amount -= amount
		if debits[debitIndex].Amount == 0 {
			debitIndex++
		}
		if credits[creditIndex].Amount == 0 {
			creditIndex++
		}
	}
	return branches
}

func splitSides(lines []Line) ([]Line, []Line) {
	debits, credits := []Line{}, []Line{}
	for _, line := range lines {
		if line.Side == "debit" {
			debits = append(debits, line)
		} else {
			credits = append(credits, line)
		}
	}
	return debits, credits
}

func invalid(code string) error {
	return connector.PermanentError("accounting.journal."+code, errors.New("journal input is invalid"))
}
