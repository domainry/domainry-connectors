package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
	"github.com/domainry/domainry-connectors/providers/accounting/freee"
	"github.com/domainry/domainry-connectors/providers/accounting/money_forward"
	"github.com/domainry/domainry-connectors/providers/accounting/quickbooks"
	"github.com/domainry/domainry-connectors/providers/collaboration/feishu"
	googleworkspace "github.com/domainry/domainry-connectors/providers/collaboration/google_workspace"
	microsoft365 "github.com/domainry/domainry-connectors/providers/collaboration/microsoft_365"
	"github.com/domainry/domainry-connectors/providers/collaboration/slack"
	"github.com/domainry/domainry-connectors/providers/ecommerce/shopify"
)

func main() {
	provider := flag.String("provider", "", "supported Provider identity")
	flag.Parse()
	if err := verify(*provider); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%s tenant read-probe verification passed\n", *provider)
}

func verify(identity string) error {
	transport := releaseverify.NewHTTPTransport()
	var adapter connector.Adapter
	var connection connector.Connection
	var secrets map[string]string
	var err error
	switch identity {
	case "collaboration/feishu":
		app, secret := required("FEISHU_TEST_APP_ID"), required("FEISHU_TEST_APP_SECRET")
		if app == "" || secret == "" {
			return errors.New("Feishu test app credentials are required")
		}
		adapter, err = feishu.New(transport)
		connection = connector.Connection{Config: map[string]any{"app_id": app, "api_base_url": "https://open.feishu.cn", "receive_id_type": "open_id"}}
		secrets = map[string]string{"app_secret": secret}
	case "collaboration/google_workspace":
		space, token := required("GOOGLE_CHAT_TEST_SPACE"), required("GOOGLE_CHAT_TEST_ACCESS_TOKEN")
		if space == "" || token == "" {
			return errors.New("Google Chat test space and token are required")
		}
		adapter, err = googleworkspace.New(transport)
		connection = connector.Connection{Config: map[string]any{"api_base_url": "https://chat.googleapis.com/v1", "space_name": space}}
		secrets = map[string]string{"access_token": token}
	case "collaboration/microsoft_365":
		team, token := required("MICROSOFT_365_TEST_TEAM_ID"), required("MICROSOFT_365_TEST_ACCESS_TOKEN")
		if team == "" || token == "" {
			return errors.New("Microsoft 365 test team and token are required")
		}
		adapter, err = microsoft365.New(transport)
		connection = connector.Connection{Config: map[string]any{"target_type": "channel", "team_id": team, "graph_base_url": "https://graph.microsoft.com/v1.0"}}
		secrets = map[string]string{"access_token": token}
	case "collaboration/slack":
		token := required("SLACK_TEST_BOT_TOKEN")
		if token == "" {
			return errors.New("SLACK_TEST_BOT_TOKEN is required")
		}
		adapter, err = slack.New(transport)
		connection = connector.Connection{Config: map[string]any{"api_base_url": "https://slack.com/api", "timeout_seconds": 30}}
		secrets = map[string]string{"bot_token": token}
	case "ecommerce/shopify":
		shop := required("SHOPIFY_TEST_SHOP_DOMAIN")
		token := required("SHOPIFY_TEST_ACCESS_TOKEN")
		if shop == "" || token == "" {
			return errors.New("SHOPIFY_TEST_SHOP_DOMAIN and SHOPIFY_TEST_ACCESS_TOKEN are required")
		}
		adapter, err = shopify.New(transport)
		connection = connector.Connection{Config: map[string]any{"shop_domain": shop, "api_version": "2026-04", "timeout_seconds": 30}}
		secrets = map[string]string{"access_token": token}
	case "accounting/quickbooks":
		company, token := required("QUICKBOOKS_SANDBOX_COMPANY_ID"), required("QUICKBOOKS_SANDBOX_ACCESS_TOKEN")
		if company == "" || token == "" {
			return errors.New("QuickBooks sandbox company and token are required")
		}
		adapter, err = quickbooks.New(transport)
		connection = connector.Connection{Config: map[string]any{"company_id": company, "base_url": "https://sandbox-quickbooks.api.intuit.com", "token_url": "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", "minor_version": 75}}
		secrets = map[string]string{"access_token": token}
	case "accounting/freee":
		company, token := required("FREEE_TEST_COMPANY_ID"), required("FREEE_TEST_ACCESS_TOKEN")
		if company == "" || token == "" {
			return errors.New("freee test company and token are required")
		}
		adapter, err = freee.New(transport)
		connection = connector.Connection{}
		secrets = map[string]string{"company_id": company, "access_token": token}
	case "accounting/money_forward":
		token := required("MONEY_FORWARD_TEST_ACCESS_TOKEN")
		if token == "" {
			return errors.New("MONEY_FORWARD_TEST_ACCESS_TOKEN is required")
		}
		adapter, err = moneyforward.New(transport)
		connection = connector.Connection{}
		secrets = map[string]string{"access_token": token}
	default:
		return fmt.Errorf("unsupported tenant read-probe Provider %q", identity)
	}
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: secrets})
	if err != nil {
		return err
	}
	if !result.Connected {
		return errors.New("Provider did not report a connected test tenant")
	}
	return nil
}

func required(key string) string { return strings.TrimSpace(os.Getenv(key)) }
