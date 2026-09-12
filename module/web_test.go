package module_test

import (
	"context"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/web"
	"github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-connectors/module"
)

type noIO struct{ calls int }

func (n *noIO) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	n.calls++
	return connector.HTTPResponse{}, errors.New("no I/O")
}
func (*noIO) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("no SQL")
}

func TestPublicWebCompositionAndCatalog(t *testing.T) {
	if _, err := module.PublicWebProviders(nil); err == nil {
		t.Fatal("nil transport accepted")
	}
	transport := &noIO{}
	set, err := module.PublicWebProviders(transport)
	if err != nil || len(set.Providers) != 1 {
		t.Fatal(err)
	}
	registry, err := module.NewFactory(module.Options{Providers: set}).Registry()
	if err != nil || !registry.Frozen() {
		t.Fatal(err)
	}
	p, ok := registry.Provider("web", "llm_proxy")
	if !ok {
		t.Fatal("selected web Provider missing")
	}
	for _, key := range []string{web.SearchOperationKey, web.FetchOperationKey} {
		rules, declared := connector.ResolveOAuthOperationScopes(p, key)
		if !declared || len(rules) != 1 || len(rules[0]) != 0 {
			t.Fatal("frozen scope contract changed")
		}
	}
	work, err := module.WorkAccountProviders(transport)
	if err != nil || len(work.Providers) != 2 {
		t.Fatal("existing work-account selection changed")
	}
	empty, err := module.NewFactory(module.Options{}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := empty.Provider("web", "llm_proxy"); ok {
		t.Fatal("unselected web Provider installed")
	}
	doc, err := catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range doc.Providers {
		if entry.ConnectorKey != "web" || entry.ProviderKey != "llm_proxy" {
			continue
		}
		found = true
		if len(entry.Operations) != 2 || entry.Verification == nil {
			t.Fatal("web catalog incomplete")
		}
		for _, op := range entry.Operations {
			if op.ContractSHA256 != web.OperationSHA256(op.Key) || op.Mode != string(connector.ModeCall) {
				t.Fatal("catalog shared identity mismatch")
			}
		}
	}
	if !found || transport.calls != 0 {
		t.Fatal("catalog missing or composition performed I/O")
	}
}
