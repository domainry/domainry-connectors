package composio

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-connectors/module"
)

const activeAccount = `{"id":"ca_test","user_id":"workspace_1:owner_1","toolkit":{"slug":"slack"},"status":"ACTIVE","state":{"val":{"access_token":"upstream-secret"}},"params":{"password":"upstream-password"}}`

type fakeTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
	deadlines []time.Time
}

func (f *fakeTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	i := len(f.requests)
	f.requests = append(f.requests, request)
	deadline, _ := ctx.Deadline()
	f.deadlines = append(f.deadlines, deadline)
	var response connector.HTTPResponse
	var err error
	if i < len(f.responses) {
		response = f.responses[i]
	}
	if i < len(f.errors) {
		err = f.errors[i]
	}
	return response, err
}

func (*fakeTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func validConnection() connector.Connection {
	return connector.Connection{Key: "slack_tools", WorkspaceID: "workspace_1", ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{
		"connected_account_id": "ca_test", "user_id": "workspace_1:owner_1", "toolkit": "slack", "timeout_seconds": 30,
		"tools": map[string]toolMapping{
			"send_message":    {Slug: "SLACK_SEND_MESSAGE", Version: "20260901_00", Effect: "write"},
			"search_messages": {Slug: "SLACK_SEARCH_MESSAGES", Version: "20260901_00", Effect: "read"},
		},
	}}
}

func invocation(operation connector.OperationDescriptor, tool string) connector.CallRequest {
	payload, _ := json.Marshal(ToolInput{ToolKey: tool, Arguments: map[string]json.RawMessage{"text": json.RawMessage(`"hello"`), "sequence": json.RawMessage(`9007199254740993`)}})
	if operation.Key == "test_connection" {
		payload = []byte(`{}`)
	}
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: operation.Mode, Connection: validConnection(), Payload: payload, Secrets: map[string]string{"api_key": "project-secret"}, Delivery: operation.Mode != connector.ModeCall, RequestRef: "intent-1"}
}

func okResponse(body string) connector.HTTPResponse {
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(body)}
}

func adapterFor(t *testing.T, f *fakeTransport) connector.Adapter {
	t.Helper()
	a, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestCatalogContractAndSelectedComposition(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil transport accepted")
	}
	f := &fakeTransport{}
	a := adapterFor(t, f)
	if err := contracttest.ValidateAdapter(a); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 0 {
		t.Fatal("constructor performed I/O")
	}
	documents, err := catalog.DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, doc := range documents {
		if doc.Key != ConnectorKey {
			continue
		}
		found = true
		if len(doc.Operations) != 4 {
			t.Fatal("incomplete catalog operations")
		}
		for _, op := range doc.Operations {
			hash, err := catalog.OperationContractSHA256(doc.Key, ProviderKey, op)
			if err != nil {
				t.Fatal(err)
			}
			matched := false
			for _, descriptor := range a.Descriptor().Operations {
				if descriptor.Key != op.Key {
					continue
				}
				matched = true
				if hash != descriptor.ContractSHA256 || op.SideEffect != string(descriptor.Reliability.Effect) {
					t.Fatalf("operation contract drift: %s", op.Key)
				}
				if op.SideEffect == "write" && (op.IdempotencySupported || descriptor.Reliability.Idempotency.Strategy != connector.IdempotencyNone) {
					t.Fatal("write advertises unsupported idempotency")
				}
			}
			if !matched {
				t.Fatalf("missing operation %s", op.Key)
			}
		}
	}
	if !found {
		t.Fatal("SaaS tools definition not discoverable")
	}
	registry, err := module.NewFactory(module.Options{Providers: connector.ProviderSet{Providers: []connector.Adapter{a}}}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Provider(ConnectorKey, ProviderKey); !ok || !registry.Frozen() {
		t.Fatal("selected provider absent")
	}
	empty, err := module.NewFactory(module.Options{}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := empty.Provider(ConnectorKey, ProviderKey); ok {
		t.Fatal("unselected provider registered")
	}
}

func TestAccountCheckDoesNotExposeCredentialsOrExecuteTools(t *testing.T) {
	f := &fakeTransport{responses: []connector.HTTPResponse{okResponse(activeAccount)}}
	a := adapterFor(t, f)
	result, err := a.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"api_key": "project-secret"}})
	if err != nil || !result.Connected {
		t.Fatalf("connection test failed: %v", err)
	}
	if len(f.requests) != 1 || f.requests[0].Method != "GET" || f.requests[0].URL != defaultBaseURL+"/api/v3.1/connected_accounts/ca_test" {
		t.Fatal("connection test executed unexpected request")
	}
	if strings.Contains(string(result.Details), "secret") || strings.Contains(string(result.Details), "password") || strings.Contains(string(result.Details), "state") {
		t.Fatal("account credentials exposed")
	}
	serialized, err := json.Marshal(f.requests[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serialized), "project-secret") || f.requests[0].SecretHeaders["x-api-key"][0] != "project-secret" {
		t.Fatal("request secret boundary broken")
	}
}

func TestMappedExecutionAndDurableDelivery(t *testing.T) {
	for _, op := range []connector.OperationDescriptor{StartTool.Descriptor(), QueryTool.Descriptor(), EnqueueTool.Descriptor()} {
		t.Run(op.Key, func(t *testing.T) {
			f := &fakeTransport{responses: []connector.HTTPResponse{okResponse(activeAccount), okResponse(`{"successful":true,"data":{"sequence":9007199254740993},"log_id":"log_123","extra":"ignored"}`)}}
			a := adapterFor(t, f)
			tool, slug := "send_message", "SLACK_SEND_MESSAGE"
			if op.Key == "query_tool" {
				tool, slug = "search_messages", "SLACK_SEARCH_MESSAGES"
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			result, err := a.Call(ctx, invocation(op, tool))
			if err != nil {
				t.Fatal(err)
			}
			if len(f.requests) != 2 || result.ResponseRef != "composio:log:log_123" {
				t.Fatal("missing account check or execution reference")
			}
			request := f.requests[1]
			if request.URL != defaultBaseURL+"/api/v3.1/tools/execute/"+slug || request.Method != "POST" {
				t.Fatal("incorrect tool endpoint")
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(request.Body, &body); err != nil {
				t.Fatal(err)
			}
			if len(body) != 4 || string(body["connected_account_id"]) != `"ca_test"` || string(body["user_id"]) != `"workspace_1:owner_1"` || string(body["version"]) != `"20260901_00"` {
				t.Fatal("execution identity or version drift")
			}
			if !strings.Contains(string(body["arguments"]), "9007199254740993") {
				t.Fatal("argument precision lost")
			}
			if f.deadlines[0].IsZero() || !f.deadlines[0].Equal(f.deadlines[1]) || time.Until(f.deadlines[1]) > 3*time.Second {
				t.Fatal("total deadline not preserved")
			}
			if op.Mode != connector.ModeCall {
				if len(result.Payload) != 0 {
					t.Fatal("delivery should return a receipt reference only")
				}
			} else if !strings.Contains(string(result.Payload), "9007199254740993") {
				t.Fatal("output precision lost")
			}
		})
	}
}

func TestRejectsCallerRoutingAndEffectOverrides(t *testing.T) {
	for _, change := range []func(*connector.CallRequest){
		func(r *connector.CallRequest) { r.Payload = []byte(`{"tool_key":"missing"}`) },
		func(r *connector.CallRequest) { r.Payload = []byte(`{"tool_key":"search_messages"}`) },
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"tool_key":"send_message","connected_account_id":"ca_other"}`)
		},
		func(r *connector.CallRequest) { r.Payload = []byte(`{"tool_key":"send_message","version":"latest"}`) },
		func(r *connector.CallRequest) { r.ContractSHA256 = strings.Repeat("0", 64) },
		func(r *connector.CallRequest) { r.Secrets = nil },
	} {
		f := &fakeTransport{}
		r := invocation(StartTool.Descriptor(), "send_message")
		change(&r)
		if _, err := adapterFor(t, f).Call(t.Context(), r); err == nil {
			t.Fatal("invalid caller override accepted")
		}
		if len(f.requests) != 0 {
			t.Fatal("invalid call performed I/O")
		}
	}
	f := &fakeTransport{}
	r := invocation(EnqueueTool.Descriptor(), "send_message")
	r.Delivery = false
	if _, err := adapterFor(t, f).Call(t.Context(), r); err == nil || len(f.requests) != 0 {
		t.Fatal("enqueue bypassed Runtime delivery")
	}
	r = invocation(QueryTool.Descriptor(), "send_message")
	if _, err := adapterFor(t, f).Call(t.Context(), r); err == nil || len(f.requests) != 0 {
		t.Fatal("read operation executed a write mapping")
	}
}

func TestOwnershipAndInactiveAccountsBlockExecution(t *testing.T) {
	for _, body := range []string{
		strings.Replace(activeAccount, "ca_test", "ca_other", 1),
		strings.Replace(activeAccount, "workspace_1:owner_1", "workspace_2:owner_1", 1),
		strings.Replace(activeAccount, "slack", "gmail", 1),
		strings.Replace(activeAccount, "ACTIVE", "EXPIRED", 1),
		strings.Replace(activeAccount, `"status":"ACTIVE"`, `"status":"ACTIVE","is_disabled":true`, 1),
		strings.Replace(activeAccount, `"status":"ACTIVE"`, `"status":"ACTIVE","auth_config":{"is_disabled":true}`, 1),
	} {
		f := &fakeTransport{responses: []connector.HTTPResponse{okResponse(body)}}
		_, err := adapterFor(t, f).Call(t.Context(), invocation(StartTool.Descriptor(), "send_message"))
		class, ok := connector.ErrorClassificationOf(err)
		if !ok || class != connector.ErrorPermanent || len(f.requests) != 1 {
			t.Fatalf("account guard failed: %v", err)
		}
	}
}

func TestFailureClassificationAndSecretRedaction(t *testing.T) {
	tests := []struct {
		name        string
		response    connector.HTTPResponse
		err         error
		read, write connector.ErrorClassification
	}{
		{"network", connector.HTTPResponse{}, errors.New("upstream-password project-secret"), connector.ErrorRetryable, connector.ErrorUncertain},
		{"server", connector.HTTPResponse{StatusCode: 503, Body: []byte(`project-secret`)}, nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"timeout", connector.HTTPResponse{StatusCode: 408}, nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable, connector.ErrorRetryable},
		{"auth", connector.HTTPResponse{StatusCode: 401}, nil, connector.ErrorPermanent, connector.ErrorPermanent},
		{"invalid arguments", connector.HTTPResponse{StatusCode: 422}, nil, connector.ErrorPermanent, connector.ErrorPermanent},
		{"invalid JSON", okResponse(`{`), nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"missing success", okResponse(`{"data":{},"log_id":"log_1"}`), nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"tool failed", okResponse(`{"successful":false,"error":"project-secret","log_id":"log_1"}`), nil, connector.ErrorPermanent, connector.ErrorUncertain},
		{"missing log", okResponse(`{"successful":true,"data":{}}`), nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"missing data", okResponse(`{"successful":true,"log_id":"log_1"}`), nil, connector.ErrorRetryable, connector.ErrorUncertain},
		{"oversized", okResponse(strings.Repeat("x", responseLimit+1)), nil, connector.ErrorRetryable, connector.ErrorUncertain},
	}
	for _, test := range tests {
		for _, write := range []bool{false, true} {
			t.Run(test.name+map[bool]string{true: "/write", false: "/read"}[write], func(t *testing.T) {
				f := &fakeTransport{responses: []connector.HTTPResponse{okResponse(activeAccount), test.response}, errors: []error{nil, test.err}}
				op, tool, want := QueryTool.Descriptor(), "search_messages", test.read
				if write {
					op, tool, want = StartTool.Descriptor(), "send_message", test.write
				}
				result, err := adapterFor(t, f).Call(t.Context(), invocation(op, tool))
				class, ok := connector.ErrorClassificationOf(err)
				if !ok || class != want {
					t.Fatalf("class %q; want %q; err %v", class, want, err)
				}
				if strings.Contains(err.Error()+string(result.Payload), "project-secret") {
					t.Fatal("upstream secret exposed")
				}
			})
		}
	}
	// A failed account read occurs before tool dispatch and is safe to retry.
	f := &fakeTransport{errors: []error{errors.New("credential-bearing account failure")}}
	_, err := adapterFor(t, f).Call(t.Context(), invocation(StartTool.Descriptor(), "send_message"))
	if class, _ := connector.ErrorClassificationOf(err); class != connector.ErrorRetryable || len(f.requests) != 1 {
		t.Fatal("preflight failure treated as executed write")
	}
}

func TestConfigurationValidation(t *testing.T) {
	a := adapterFor(t, &fakeTransport{}).(connector.ConfigValidator)
	for _, endpoint := range []string{defaultBaseURL, "https://composio.internal.example", "http://localhost:9000", "http://[::1]:9000"} {
		c := validConnection()
		c.Config["base_url"] = endpoint
		if err := a.ValidateConfig(c); err != nil {
			t.Fatalf("valid endpoint rejected: %v", err)
		}
	}
	invalid := []struct {
		key   string
		value any
	}{
		{"base_url", "http://backend.composio.dev"}, {"base_url", "https://user:pass@example.com"}, {"base_url", "https://example.com/api/v3"}, {"base_url", "https://example.com?token=secret"}, {"base_url", "https://example.com#"},
		{"timeout_seconds", 0}, {"timeout_seconds", 301}, {"timeout_seconds", 1.5}, {"timeout_seconds", "30"},
		{"connected_account_id", "ca_other/../ca_test"}, {"user_id", ""}, {"toolkit", "SLACK"}, {"tools", map[string]any{}}, {"tools", "not an object"},
	}
	for _, entry := range invalid {
		c := validConnection()
		c.Config[entry.key] = entry.value
		if err := a.ValidateConfig(c); err == nil {
			t.Fatalf("invalid %s accepted", entry.key)
		}
	}
	for _, mapping := range []toolMapping{{Slug: "SLACK_SEND_MESSAGE", Version: "latest", Effect: "write"}, {Slug: "COMPOSIO_MULTI_EXECUTE_TOOL", Version: "20260901_00", Effect: "write"}, {Slug: "SLACK_SEND_MESSAGE", Version: "20260901_00", Effect: "unknown"}, {Slug: "../execute", Version: "20260901_00", Effect: "write"}} {
		c := validConnection()
		c.Config["tools"] = map[string]toolMapping{"send_message": mapping}
		if err := a.ValidateConfig(c); err == nil {
			t.Fatal("unsafe mapping accepted")
		}
	}
}
