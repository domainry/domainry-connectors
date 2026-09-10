// Command verify_composio_connection checks one existing Composio account.
// It never authorizes a new account or executes a SaaS tool.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
	"github.com/domainry/domainry-connectors/providers/saas_tool/composio"
)

func main() {
	path := flag.String("connection", "", "path to a Domainry connection JSON document (no credentials)")
	flag.Parse()
	if err := verify(*path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Composio account is active and matches the configured owner and toolkit; no SaaS tool was executed.")
}

func verify(path string) error {
	if path == "" {
		return errors.New("--connection is required")
	}
	key := os.Getenv("COMPOSIO_API_KEY")
	if key == "" {
		return errors.New("COMPOSIO_API_KEY is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open connection configuration")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var connection connector.Connection
	if decoder.Decode(&connection) != nil {
		return errors.New("invalid connection JSON")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("connection file must contain one JSON document")
	}
	if connection.ConnectorKey != composio.ConnectorKey || connection.ProviderKey != composio.ProviderKey {
		return errors.New("connection must select saas_tool/composio")
	}
	transport := releaseverify.NewHTTPTransport()
	// Custom API-key headers must never follow a redirect to another origin.
	transport.Client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	adapter, err := composio.New(transport)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"api_key": key}})
	if err != nil {
		return err
	}
	if !result.Connected {
		return errors.New("Composio account is not connected")
	}
	return nil
}
