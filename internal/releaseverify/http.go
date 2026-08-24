// Package releaseverify provides the deliberately small network transport used
// only by opt-in Connector release gates. Provider production code continues to
// receive Runtime-owned transports.
package releaseverify

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

type HTTPTransport struct{ Client *http.Client }

func NewHTTPTransport() HTTPTransport {
	return HTTPTransport{Client: &http.Client{Timeout: 30 * time.Second}}
}

func (transport HTTPTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	requestURL, err := url.Parse(request.URL)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	query := requestURL.Query()
	for key, value := range request.SecretQuery {
		if query.Has(key) {
			return connector.HTTPResponse{}, errors.New("secret query collides with public query")
		}
		query.Set(key, value)
	}
	requestURL.RawQuery = query.Encode()
	body := append([]byte(nil), request.Body...)
	if len(request.SecretForm) > 0 {
		form, parseErr := url.ParseQuery(string(body))
		if parseErr != nil {
			return connector.HTTPResponse{}, parseErr
		}
		for key, value := range request.SecretForm {
			if form.Has(key) {
				return connector.HTTPResponse{}, errors.New("secret form collides with public form")
			}
			form.Set(key, value)
		}
		body = []byte(form.Encode())
	}
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for key, values := range request.Headers {
		for _, value := range values {
			httpRequest.Header.Add(key, value)
		}
	}
	for key, values := range request.SecretHeaders {
		for _, value := range values {
			httpRequest.Header.Add(key, value)
		}
	}
	if len(request.SecretJSON) > 0 || (strings.Contains(httpRequest.Header.Get("Content-Type"), "application/json") && len(request.SecretJSON) > 0) {
		return connector.HTTPResponse{}, errors.New("secret JSON injection is not supported by release verifier")
	}
	response, err := transport.Client.Do(httpRequest)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	defer response.Body.Close()
	limit := request.MaxResponseBytes
	if limit <= 0 {
		limit = 2 << 20
	}
	body, err = io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	if int64(len(body)) > limit {
		return connector.HTTPResponse{}, errors.New("release verification response exceeded limit")
	}
	return connector.HTTPResponse{StatusCode: response.StatusCode, Headers: response.Header, Body: body}, nil
}

func (HTTPTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL is unavailable")
}
