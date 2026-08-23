// Package s3 implements the official S3-compatible object-storage Provider.
package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey    = "file_storage"
	ProviderKey     = "s3"
	defaultEndpoint = "https://s3.amazonaws.com"
	defaultMaxBytes = 25 << 20
)

type PathInput struct {
	Path string `json:"path"`
}

type PutTextInput struct {
	Path        string `json:"path"`
	Content     string `json:"content"`
	ContentType string `json:"content_type,omitempty"`
}

var (
	Delete         = writeOp[PathInput]("delete", "6a170bdbf602291c72d9f37fd2dfbf3591f0c32cf8ce3b751062e3fcd20dbbb8")
	GetText        = readOp[PathInput]("get_text", "37a7acbfb6ba0e7382a9e39668290cc2d62e0d74567e5029a240712b799210dc")
	PutText        = writeOp[PutTextInput]("put_text", "98623acc286f328d5bb9208fd741e85c0b147fa24eb685e602982d83760750ec")
	TestConnection = readOp[struct{}]("test_connection", "e1d1443180eb710663214176b0c6987a8086f9260203b88b57bdcaf3fbcb6964")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectRead, connector.IdempotencyNatural)
}

func writeOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectWrite, connector.IdempotencyNone)
}

func operation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("S3 transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	deleteOperation, err := connector.BindCall(Delete, p.delete)
	if err != nil {
		return nil, err
	}
	getOperation, err := connector.BindCall(GetText, p.get)
	if err != nil {
		return nil, err
	}
	putOperation, err := connector.BindCall(PutText, p.put)
	if err != nil {
		return nil, err
	}
	testOperation, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), deleteOperation, getOperation, putOperation, testOperation)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum := float64(1)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "endpoint", Name: "Endpoint", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://s3.amazonaws.com"`), Validation: connector.ConfigValidation{Pattern: `^https?://[^\s]+$`}}, {Key: "region", Name: "Region", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"us-east-1"`)}, {Key: "bucket", Name: "Bucket", Type: connector.ConfigFieldText, Required: true}, {Key: "prefix", Name: "Key Prefix", Type: connector.ConfigFieldText}, {Key: "max_file_bytes", Name: "Maximum File Bytes", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`26214400`), Validation: connector.ConfigValidation{Min: &minimum}}, {Key: "allow_delete", Name: "Allow Delete", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}}, SecretFields: []connector.SecretField{secret("access_key_id", "Access Key ID", true, connector.SecretCredentialIdentifier), secret("secret_access_key", "Secret Access Key", true, connector.SecretCredentialAPIKey), secret("session_token", "Session Token", false, connector.SecretCredentialBearerToken)}}
}

func secret(key, name string, required bool, kind connector.SecretCredentialKind) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	bucket := config(connection, "bucket", "")
	if bucket == "" {
		return permanent("bucket_required", "bucket is required")
	}
	if strings.ContainsAny(bucket, "/\\?#") || strings.ContainsRune(bucket, '\x00') {
		return permanent("bucket_invalid", "bucket is invalid")
	}
	if config(connection, "region", "") == "" {
		return permanent("region_required", "region is required")
	}
	endpoint, err := url.Parse(config(connection, "endpoint", defaultEndpoint))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "endpoint must use HTTPS unless it targets loopback")
	}
	if prefix := strings.Trim(config(connection, "prefix", ""), "/"); prefix != "" {
		cleaned := path.Clean(prefix)
		if cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.ContainsRune(cleaned, '\x00') {
			return permanent("prefix_invalid", "prefix escapes the object namespace")
		}
	}
	if integer(connection.Config["max_file_bytes"], defaultMaxBytes) < 1 {
		return permanent("max_file_bytes_invalid", "max_file_bytes must be positive")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	response, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodHead, "", nil, "", false)
	if err != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, err
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true, "bucket": config(request.Connection, "bucket", "")}, ResponseRef: responseRef(response, ref)}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) put(ctx context.Context, request connector.TypedRequest[PutTextInput]) (connector.TypedResult[map[string]any], error) {
	key, err := objectKey(request.Connection, request.Input.Path)
	if err != nil {
		return empty(), err
	}
	body := []byte(request.Input.Content)
	if len(body) > integer(request.Connection.Config["max_file_bytes"], defaultMaxBytes) {
		return empty(), permanent("file_too_large", "content exceeds max_file_bytes")
	}
	contentType := strings.TrimSpace(request.Input.ContentType)
	if contentType == "" {
		contentType = "text/plain; charset=utf-8"
	}
	_, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, key, body, contentType, true)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": key, "bytes": len(body), "content_type": contentType}, ResponseRef: ref}, err
}

func (p *provider) get(ctx context.Context, request connector.TypedRequest[PathInput]) (connector.TypedResult[map[string]any], error) {
	key, err := objectKey(request.Connection, request.Input.Path)
	if err != nil {
		return empty(), err
	}
	response, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, key, nil, "", false)
	if err != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, err
	}
	contentType := firstHeader(response.headers, "Content-Type")
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": key, "content": string(response.body), "bytes": len(response.body), "content_type": contentType}, ResponseRef: responseRef(response, ref)}, nil
}

func (p *provider) delete(ctx context.Context, request connector.TypedRequest[PathInput]) (connector.TypedResult[map[string]any], error) {
	if !boolean(request.Connection.Config["allow_delete"], false) {
		return empty(), permanent("delete_forbidden", "delete is disabled")
	}
	key, err := objectKey(request.Connection, request.Input.Path)
	if err != nil {
		return empty(), err
	}
	_, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodDelete, key, nil, "", true)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": key, "deleted": err == nil}, ResponseRef: ref}, err
}

type response struct {
	body    []byte
	headers map[string][]string
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, key string, body []byte, contentType string, write bool) (response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return response{}, "", err
	}
	accessKey := strings.TrimSpace(secrets["access_key_id"])
	secretKey := strings.TrimSpace(secrets["secret_access_key"])
	if accessKey == "" || secretKey == "" {
		return response{}, "", permanent("credentials_required", "resolved access credentials are required")
	}
	endpoint, _ := url.Parse(strings.TrimRight(config(connection, "endpoint", defaultEndpoint), "/") + "/" + url.PathEscape(config(connection, "bucket", "")))
	if key != "" {
		endpoint.Path += "/" + key
	}
	headers := map[string][]string{}
	if contentType != "" {
		headers["Content-Type"] = []string{contentType}
	}
	secretHeaders := sign(method, endpoint, headers, body, accessKey, secretKey, secrets["session_token"], config(connection, "region", ""), p.now().UTC())
	limit := int64(1 << 20)
	if method == http.MethodGet {
		limit = int64(integer(connection.Config["max_file_bytes"], defaultMaxBytes))
	}
	raw, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: limit})
	if transportErr != nil {
		if write {
			return response{}, "", connector.UncertainError("s3.network_error", transportErr)
		}
		return response{}, "", connector.RetryableError("s3.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(raw.StatusCode)
	result := response{body: raw.Body, headers: raw.Headers}
	if raw.StatusCode >= 200 && raw.StatusCode < 300 {
		return result, responseRef(result, ref), nil
	}
	cause := fmt.Errorf("S3 returned HTTP %d", raw.StatusCode)
	code := "s3.http_" + strconv.Itoa(raw.StatusCode)
	if raw.StatusCode == http.StatusTooManyRequests {
		return result, ref, connector.RetryableError(code, cause)
	}
	if raw.StatusCode == http.StatusRequestTimeout || raw.StatusCode >= 500 {
		if write {
			return result, ref, connector.UncertainError(code, cause)
		}
		return result, ref, connector.RetryableError(code, cause)
	}
	return result, ref, connector.PermanentError(code, cause)
}

func objectKey(connection connector.Connection, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" || strings.HasPrefix(requested, "/") {
		return "", permanent("path_invalid", "path must be relative")
	}
	cleaned := path.Clean(requested)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", permanent("path_invalid", "path escapes its object prefix")
	}
	if prefix := strings.Trim(config(connection, "prefix", ""), "/"); prefix != "" {
		cleaned = prefix + "/" + cleaned
	}
	return cleaned, nil
}

func sign(method string, endpoint *url.URL, headers map[string][]string, body []byte, accessKey, secretKey, sessionToken, region string, now time.Time) map[string][]string {
	payloadHash := sha256Hex(body)
	dateHeader := now.Format("20060102T150405Z")
	secretHeaders := map[string][]string{"X-Amz-Content-Sha256": {payloadHash}, "X-Amz-Date": {dateHeader}}
	if sessionToken != "" {
		secretHeaders["X-Amz-Security-Token"] = []string{sessionToken}
	}
	combined := cloneHeaders(headers)
	for key, values := range secretHeaders {
		combined[key] = values
	}
	canonical, signed := canonicalHeaders(endpoint.Host, combined)
	canonicalRequest := strings.Join([]string{method, canonicalURI(endpoint), endpoint.Query().Encode(), canonical, signed, payloadHash}, "\n")
	date := now.Format("20060102")
	scope := date + "/" + region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + dateHeader + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), date)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, "s3")
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	secretHeaders["Authorization"] = []string{"AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + signature}
	return secretHeaders
}

func canonicalHeaders(host string, headers map[string][]string) (string, string) {
	values := map[string]string{"host": host}
	for key, items := range headers {
		lower := strings.ToLower(strings.TrimSpace(key))
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-type" {
			values[lower] = strings.Join(items, ",")
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var canonical strings.Builder
	for _, key := range keys {
		canonical.WriteString(key)
		canonical.WriteByte(':')
		canonical.WriteString(strings.Join(strings.Fields(values[key]), " "))
		canonical.WriteByte('\n')
	}
	return canonical.String(), strings.Join(keys, ";")
}

func canonicalURI(value *url.URL) string {
	segments := strings.Split(value.EscapedPath(), "/")
	for index, segment := range segments {
		decoded, _ := url.PathUnescape(segment)
		segments[index] = url.PathEscape(decoded)
	}
	result := strings.Join(segments, "/")
	if result == "" {
		return "/"
	}
	return result
}

func responseRef(value response, fallback string) string {
	if etag := strings.Trim(firstHeader(value.headers, "ETag"), `"`); etag != "" {
		return "s3:etag:" + etag
	}
	return fallback
}

func cloneHeaders(source map[string][]string) map[string][]string {
	target := make(map[string][]string, len(source))
	for key, values := range source {
		target[key] = append([]string(nil), values...)
	}
	return target
}

func firstHeader(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(candidate, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte(value))
	return hash.Sum(nil)
}

func loopback(host string) bool { return host == "localhost" || host == "127.0.0.1" || host == "::1" }

func config(connection connector.Connection, key, fallback string) string {
	value, _ := connection.Config[key].(string)
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := strconv.Atoi(typed.String()); err == nil {
			return parsed
		}
	}
	return fallback
}

func boolean(value any, fallback bool) bool {
	typed, ok := value.(bool)
	if ok {
		return typed
	}
	return fallback
}

func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }

func permanent(code, message string) error {
	return connector.PermanentError("s3."+code, errors.New(message))
}
