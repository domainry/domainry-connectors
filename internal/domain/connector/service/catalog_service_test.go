package service

import (
	"strings"
	"testing"

	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
)

func TestCatalogServiceValidatesIdentityAndOrdering(t *testing.T) {
	verification := &model.ProviderVerification{Mode: "deterministic", TestCommand: "go test"}
	valid := model.Catalog{ContractVersion: model.CatalogContractVersion, Providers: []model.ProviderEntry{
		{ConnectorKey: "email", ProviderKey: "smtp", Verification: verification},
		{ConnectorKey: "payment", ProviderKey: "stripe", Verification: verification},
	}}
	if err := (CatalogService{}).Validate(valid); err != nil {
		t.Fatal(err)
	}
	duplicate := valid
	duplicate.Providers = append(duplicate.Providers, valid.Providers[1])
	if err := (CatalogService{}).Validate(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Validate duplicate error = %v", err)
	}
	unsorted := valid
	unsorted.Providers = []model.ProviderEntry{valid.Providers[1], valid.Providers[0]}
	if err := (CatalogService{}).Validate(unsorted); err == nil || !strings.Contains(err.Error(), "not sorted") {
		t.Fatalf("Validate unsorted error = %v", err)
	}
}
