// Package ses sends credential mail through the AWS SES v2 API. All SDK HTTP
// traffic, including the default credential chain, uses the host Transport.
package ses

import (
	"bytes"
	"context"
	"errors"
	"html"
	"io"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	connector "github.com/domainry/domainry-connector-sdk"
)

const ConnectorKey, ProviderKey = "email", "ses"

type SendEmailInput struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}
type SendEmailOutput struct {
	Accepted  bool   `json:"accepted"`
	MessageID string `json:"message_id"`
}

// SES SendEmail has no client idempotency token. Never automatically retry a
// potentially accepted security email; Integration records only its receipt.
var SendSecurityEmail = connector.CallOperation[SendEmailInput, SendEmailOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_security_email",
	ContractSHA256: "81aede511e615f567d19b7111ceba5a598a83204291c0a9c59aafe45677e7b66",
	Reliability:    connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("SES HTTP transport is required")
	}
	p := &provider{transport: transport}
	send, err := connector.BindCall(SendSecurityEmail, p.send)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "region", Name: "AWS region", Type: connector.ConfigFieldText, Required: true},
			{Key: "from_email", Name: "SES verified sender", Type: connector.ConfigFieldEmail, Required: true},
			{Key: "from_name", Name: "Sender name", Type: connector.ConfigFieldText},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger},
			{Key: "base_url", Name: "Local SES emulator endpoint", Type: connector.ConfigFieldText},
		},
		SecretFields: []connector.SecretField{
			{Key: "access_key_id", Name: "AWS access key ID", CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialText, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional},
			{Key: "secret_access_key", Name: "AWS secret access key", CredentialKind: connector.SecretCredentialGeneric, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional},
			{Key: "session_token", Name: "AWS session token", CredentialKind: connector.SecretCredentialGeneric, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional},
		},
	}, send)
	return p, err
}

func setting(c connector.Connection, key string) string {
	value, _ := c.Config[key].(string)
	return strings.TrimSpace(value)
}

var regionPattern = regexp.MustCompile(`^[a-z]{2}(?:-[a-z]+)+-[0-9]+$`)

func validAddress(value string) bool {
	address, err := mail.ParseAddress(value)
	return err == nil && address.Address == value && !strings.ContainsAny(value, "\r\n")
}
func timeout(c connector.Connection) (time.Duration, error) {
	seconds := 10
	if value, found := c.Config["timeout_seconds"]; found {
		parsed, err := strconv.Atoi(strings.TrimSpace(toString(value)))
		if err != nil || parsed < 1 || parsed > 60 {
			return 0, permanent("config_invalid")
		}
		seconds = parsed
	}
	return time.Duration(seconds) * time.Second, nil
}
func toString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	default:
		return ""
	}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	if !regionPattern.MatchString(setting(c, "region")) || !validAddress(setting(c, "from_email")) || strings.ContainsAny(setting(c, "from_name"), "\r\n") {
		return permanent("config_invalid")
	}
	if _, err := timeout(c); err != nil {
		return err
	}
	if endpoint := setting(c, "base_url"); endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.Scheme != "http" && u.Scheme != "https" {
			return permanent("endpoint_invalid")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return permanent("endpoint_invalid")
		}
	}
	return nil
}

func (p *provider) send(ctx context.Context, r connector.TypedRequest[SendEmailInput]) (connector.TypedResult[SendEmailOutput], error) {
	var empty connector.TypedResult[SendEmailOutput]
	if err := p.ValidateConfig(r.Connection); err != nil {
		return empty, err
	}
	if len(r.Input.To) != 1 || !validAddress(r.Input.To[0]) || strings.TrimSpace(r.Input.Subject) == "" || len(r.Input.Subject) > 256 || strings.ContainsAny(r.Input.Subject, "\r\n") || len(r.Input.Text) == 0 || len(r.Input.Text) > 16384 {
		return empty, permanent("message_invalid")
	}
	duration, _ := timeout(r.Connection)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	httpClient := sdkHTTPClient{p.transport}
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(setting(r.Connection, "region")), awsconfig.WithHTTPClient(httpClient),
		// The SDK's container credential resolver does not inherit cfg.HTTPClient;
		// explicitly bind it as well so ECS/EKS role retrieval cannot bypass host I/O.
		awsconfig.WithEndpointCredentialOptions(func(o *endpointcreds.Options) { o.HTTPClient = httpClient }),
		awsconfig.WithClientLogMode(0), awsconfig.WithLogger(logging.Nop{}),
	}
	id, secret, token := r.Secrets["access_key_id"], r.Secrets["secret_access_key"], r.Secrets["session_token"]
	if id != "" || secret != "" || token != "" {
		if id == "" || secret == "" {
			return empty, permanent("credentials_invalid")
		}
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(id, secret, token)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return empty, permanent("credentials_unavailable")
	}
	// An explicit SDK service endpoint prevents process-wide endpoint overrides
	// from redirecting signed requests. Only loopback emulators are configurable.
	endpoint := "https://email." + cfg.Region + ".amazonaws.com"
	if strings.HasPrefix(cfg.Region, "cn-") {
		endpoint += ".cn"
	}
	if local := setting(r.Connection, "base_url"); local != "" {
		endpoint = local
	}
	client := sesv2.NewFromConfig(cfg, func(o *sesv2.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Retryer = aws.NopRetryer{}
		o.ClientLogMode = 0
		o.Logger = logging.Nop{}
	})
	sender := (&mail.Address{Name: setting(r.Connection, "from_name"), Address: setting(r.Connection, "from_email")}).String()
	result, err := client.SendEmail(ctx, &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(sender), Destination: &types.Destination{ToAddresses: r.Input.To},
		Content: &types.EmailContent{Simple: &types.Message{Subject: &types.Content{Charset: aws.String("UTF-8"), Data: aws.String(r.Input.Subject)}, Body: &types.Body{
			Text: &types.Content{Charset: aws.String("UTF-8"), Data: aws.String(r.Input.Text)},
			Html: &types.Content{Charset: aws.String("UTF-8"), Data: aws.String("<pre>" + html.EscapeString(r.Input.Text) + "</pre>")},
		}}},
	})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && api.ErrorFault() == smithy.FaultClient {
			return empty, permanent("request_rejected")
		}
		return empty, connector.UncertainError("ses.delivery_uncertain", errors.New("SES did not confirm email acceptance"))
	}
	if result == nil || strings.TrimSpace(aws.ToString(result.MessageId)) == "" {
		return empty, connector.UncertainError("ses.receipt_invalid", errors.New("SES acceptance receipt is missing"))
	}
	return connector.TypedResult[SendEmailOutput]{Output: SendEmailOutput{Accepted: true, MessageID: aws.ToString(result.MessageId)}, ResponseRef: "ses:" + aws.ToString(result.MessageId)}, nil
}
func permanent(code string) error {
	return connector.PermanentError("ses."+code, errors.New("SES credential email configuration or request is invalid"))
}

// The AWS SDK signs requests; the host still owns sockets, TLS and bounds.
// Secret-bearing AWS headers are never included in serializable Headers.
type sdkHTTPClient struct{ transport connector.Transport }

func (c sdkHTTPClient) Do(request *http.Request) (*http.Response, error) {
	var body []byte
	var err error
	if request.Body != nil {
		defer request.Body.Close()
		body, err = io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			return nil, errors.New("AWS request body could not be bounded")
		}
	}
	headers, secrets := map[string][]string{}, map[string][]string{}
	for key, values := range request.Header {
		switch strings.ToLower(key) {
		case "authorization", "x-amz-security-token", "x-aws-ec2-metadata-token":
			secrets[key] = append([]string(nil), values...)
		default:
			headers[key] = append([]string(nil), values...)
		}
	}
	response, err := c.transport.RoundTripHTTP(request.Context(), connector.HTTPRequest{Method: request.Method, URL: request.URL.String(), Headers: headers, SecretHeaders: secrets, Body: body, MaxResponseBytes: 1 << 20})
	if err != nil {
		return nil, errors.New("host AWS HTTP exchange failed")
	}
	return &http.Response{StatusCode: response.StatusCode, Header: http.Header(response.Headers), Body: io.NopCloser(bytes.NewReader(response.Body)), ContentLength: int64(len(response.Body)), Request: request}, nil
}
