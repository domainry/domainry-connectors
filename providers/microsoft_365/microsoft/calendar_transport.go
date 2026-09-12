package microsoft

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

// A session is local to one call. Later pages/refetches use rotated credentials;
// every completed rotation survives a subsequent transport or decoding failure.
type calendarSession struct {
	p          *provider
	connection connector.Connection
	secrets    map[string]string
	state      connector.TypedResult[Response]
}

func newCalendarSession(p *provider, c connector.Connection, secrets map[string]string) *calendarSession {
	s := &calendarSession{p: p, connection: c, secrets: map[string]string{}}
	for k, v := range secrets {
		s.secrets[k] = v
	}
	return s
}

func (s *calendarSession) get(ctx context.Context, endpoint string, q url.Values, zone string, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := s.p.executeWithRefresh(ctx, s.connection, s.secrets, endpoint, q,
		`outlook.timezone="`+zone+`"`, `outlook.body-content-type="text"`, `IdType="ImmutableId"`)
	s.state.ResponseRef, s.state.ResourceHealth = r.ResponseRef, r.ResourceHealth
	for k, v := range r.SecretUpdates {
		if s.state.SecretUpdates == nil {
			s.state.SecretUpdates = map[string]string{}
		}
		s.state.SecretUpdates[k], s.secrets[k] = v, v
	}
	if err != nil {
		return err
	}
	b, err := json.Marshal(r.Output)
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		return calendarInvalidResponse("invalid calendar response")
	}
	return nil
}

func calendarResult[T any](s *calendarSession, out T, err error) (connector.TypedResult[T], error) {
	return connector.TypedResult[T]{Output: out, ResponseRef: s.state.ResponseRef,
		SecretUpdates: s.state.SecretUpdates, ResourceHealth: s.state.ResourceHealth}, err
}

func calendarInvalidResponse(message string) error {
	return permanent("calendar.invalid_response", message)
}

// Cursors contain only validated paging query parameters, never a destination.
// The scope prevents accidental continuation on another account/window. It is
// not an authorization grant: the host must authorize every call independently.
type calendarCursor struct {
	Version int    `json:"version"`
	Scope   string `json:"scope"`
	Query   string `json:"query"`
}

func (s *calendarSession) cursorScope(endpoint string, base url.Values, zone string) string {
	b, _ := json.Marshal([]string{s.connection.WorkspaceID, s.connection.Key, endpoint, base.Encode(), zone})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func calendarPagingQuery(base, q url.Values) bool {
	for key, values := range base {
		if len(values) != 1 || len(q[key]) != 1 || q.Get(key) != values[0] {
			return false
		}
	}
	count := 0
	for key, values := range q {
		if len(values) != 1 {
			return false
		}
		if _, exists := base[key]; exists {
			continue
		}
		if key != "$skiptoken" && key != "$skip" {
			return false
		}
		if values[0] == "" || len(values[0]) > 4096 || strings.ContainsAny(values[0], "\r\n\x00") {
			return false
		}
		if key == "$skip" {
			n, err := strconv.ParseUint(values[0], 10, 63)
			if err != nil || n == 0 {
				return false
			}
		}
		count++
	}
	return count == 1
}

func (s *calendarSession) pageQuery(endpoint string, base url.Values, zone, cursor string) (url.Values, error) {
	if cursor == "" {
		return base, nil
	}
	invalid := permanent("calendar.invalid_cursor", "invalid calendar continuation")
	if len(cursor) > 8192 {
		return nil, invalid
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, invalid
	}
	var c calendarCursor
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || c.Version != 1 || c.Scope != s.cursorScope(endpoint, base, zone) {
		return nil, invalid
	}
	q, err := url.ParseQuery(c.Query)
	if err != nil || !calendarPagingQuery(base, q) {
		return nil, invalid
	}
	return q, nil
}

func (s *calendarSession) nextCursor(endpoint string, base url.Values, zone, next string) (string, error) {
	if next == "" {
		return "", nil
	}
	u, err := url.Parse(next)
	fixed, fixedErr := url.Parse(endpoint)
	if err != nil || fixedErr != nil || len(next) > 8192 || u.User != nil || u.Fragment != "" || u.Scheme != fixed.Scheme || u.Host != fixed.Host || u.EscapedPath() != fixed.EscapedPath() {
		return "", calendarInvalidResponse("invalid calendar continuation destination")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || !calendarPagingQuery(base, q) {
		return "", calendarInvalidResponse("invalid calendar continuation parameters")
	}
	b, _ := json.Marshal(calendarCursor{Version: 1, Scope: s.cursorScope(endpoint, base, zone), Query: q.Encode()})
	cursor := base64.RawURLEncoding.EncodeToString(b)
	if len(cursor) > 8192 {
		return "", calendarInvalidResponse("calendar continuation exceeds limit")
	}
	return cursor, nil
}

type calendarPage[T any] struct {
	Items *[]T   `json:"value"`
	Next  string `json:"@odata.nextLink"`
}

func readCalendarPage[T any](ctx context.Context, s *calendarSession, endpoint string, base url.Values, zone, cursor string, limit int) ([]T, string, error) {
	q, err := s.pageQuery(endpoint, base, zone, cursor)
	if err != nil {
		return nil, "", err
	}
	var page calendarPage[T]
	if err = s.get(ctx, endpoint, q, "UTC", &page); err != nil {
		return nil, "", err
	}
	if page.Items == nil || len(*page.Items) > limit {
		return nil, "", calendarInvalidResponse("invalid calendar page")
	}
	next, err := s.nextCursor(endpoint, base, zone, page.Next)
	if err == nil && next != "" && next == cursor {
		err = calendarInvalidResponse("calendar continuation did not advance")
	}
	return *page.Items, next, err
}
