package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/catalog"
)

const outputPath = "catalog/catalog.json"

type providerSpec struct {
	importPath  string
	packageName string
	constructor func(connector.Transport) (connector.Adapter, error)
}

func main() {
	check := flag.Bool("check", false, "fail when catalog/catalog.json is stale")
	flag.Parse()
	specifications := providerSpecifications()
	document := catalog.Document{ContractVersion: catalog.ContractVersion}
	identity := connector.CurrentIdentity()
	document.SDK = catalog.SDKIdentity{Version: identity.SDKVersion, ContractVersion: identity.ContractVersion, ContractSHA256: identity.ContractSHA256}
	for _, specification := range specifications {
		adapter, err := specification.constructor(noopTransport{})
		if err != nil {
			fatal(err)
		}
		descriptor := adapter.Descriptor()
		descriptorJSON, err := json.Marshal(descriptor)
		if err != nil {
			fatal(err)
		}
		entry := catalog.ProviderEntry{
			ConnectorKey: descriptor.ConnectorKey, ProviderKey: descriptor.ProviderKey,
			ProviderRevision: descriptor.ProviderRevision, ImportPath: specification.importPath,
			PackageName: specification.packageName, Constructor: "New",
			DescriptorSHA256: fmt.Sprintf("%x", sha256.Sum256(descriptorJSON)),
			Operations:       make([]catalog.OperationEntry, 0, len(descriptor.Operations)),
		}
		for _, operation := range descriptor.Operations {
			entry.Operations = append(entry.Operations, catalog.OperationEntry{Key: operation.Key, Mode: string(operation.Mode), ContractSHA256: operation.ContractSHA256})
		}
		sort.Slice(entry.Operations, func(i, j int) bool { return entry.Operations[i].Key < entry.Operations[j].Key })
		document.Providers = append(document.Providers, entry)
	}
	sort.Slice(document.Providers, func(i, j int) bool {
		left := document.Providers[i].ConnectorKey + ":" + document.Providers[i].ProviderKey
		right := document.Providers[j].ConnectorKey + ":" + document.Providers[j].ProviderKey
		return left < right
	})
	raw, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fatal(err)
	}
	raw = append(raw, '\n')
	if *check {
		committed, err := os.ReadFile(outputPath)
		if err != nil {
			fatal(err)
		}
		if !bytes.Equal(committed, raw) {
			fatal(fmt.Errorf("official Connector Catalog is stale; run go run ./scripts/generate_catalog"))
		}
		return
	}
	if err := os.WriteFile(outputPath, raw, 0o644); err != nil {
		fatal(err)
	}
}

type noopTransport struct{}

func (noopTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, fmt.Errorf("Catalog generation transport cannot perform HTTP")
}

func (noopTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, fmt.Errorf("Catalog generation transport cannot perform SQL")
}

func (noopTransport) SendSMTP(context.Context, connector.SMTPRequest) (connector.SMTPResult, error) {
	return connector.SMTPResult{}, fmt.Errorf("Catalog generation transport cannot perform SMTP")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
