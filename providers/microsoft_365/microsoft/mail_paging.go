package microsoft

import (
	"net/url"
	"strconv"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/mailpaging"
)

func mailCursorScope(c connector.Connection, endpoint string, base url.Values) string {
	return mailpaging.Scope(c.WorkspaceID, c.Key, endpoint, base.Encode())
}
func mailPagingValues(base, q url.Values) bool {
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
		if key != "$skiptoken" && key != "$skip" || !mailpaging.ValidToken(values[0]) {
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
func mailPageQuery(c connector.Connection, endpoint string, base url.Values, raw string) (url.Values, int, error) {
	if raw == "" {
		return base, 0, nil
	}
	invalid := permanent("mail.invalid_cursor", "invalid mail continuation")
	cursor, err := mailpaging.Decode(mailCursorScope(c, endpoint, base), raw)
	if err != nil {
		return nil, 0, invalid
	}
	q, err := url.ParseQuery(cursor.Token)
	if err != nil || !mailPagingValues(base, q) {
		return nil, 0, invalid
	}
	return q, cursor.Seen, nil
}
func mailNextCursor(c connector.Connection, endpoint string, base, current url.Values, next string, seen int) (string, error) {
	u, err := url.Parse(next)
	fixed, fixedErr := url.Parse(endpoint)
	if err != nil || fixedErr != nil || len(next) > 8192 || u.Scheme != fixed.Scheme || u.Host != fixed.Host || u.EscapedPath() != fixed.EscapedPath() || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return "", mailInvalidResponse()
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || !mailPagingValues(base, q) || q.Encode() == current.Encode() {
		return "", mailInvalidResponse()
	}
	// Never manipulate Graph's skip count. Preserve the complete validated query.
	out, err := mailpaging.Encode(mailCursorScope(c, endpoint, base), q.Encode(), seen)
	if err != nil {
		return "", mailInvalidResponse()
	}
	return out, nil
}
