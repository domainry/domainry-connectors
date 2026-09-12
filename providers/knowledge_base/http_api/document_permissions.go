package httpapi

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

// Document ACLs are trusted connection policy, never operation arguments or
// caller-supplied HTTP headers. Omission preserves the upstream ACL; explicit
// [] is a distinct, team-visible ACL command.
func documentPermissionHeader(connection connector.Connection) (string, bool, error) {
	value, present := connection.Config["document_permission_ids"]
	if !present {
		return "", false, nil
	}
	var ids []string
	switch values := value.(type) {
	case []string:
		ids = slices.Clone(values)
	case []any:
		if values == nil || len(values) > 4096 {
			return "", true, permanent("request_invalid")
		}
		ids = make([]string, len(values))
		for i, value := range values {
			var ok bool
			ids[i], ok = value.(string)
			if !ok {
				return "", true, permanent("request_invalid")
			}
		}
	default:
		return "", true, permanent("request_invalid")
	}
	if ids == nil || len(ids) > 4096 {
		return "", true, permanent("request_invalid")
	}
	for _, id := range ids {
		if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || !utf8.ValidString(id) || strings.ContainsFunc(id, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
			return "", true, permanent("request_invalid")
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) > 256 {
		return "", true, permanent("request_invalid")
	}
	var out strings.Builder
	out.WriteByte('[')
	for i, id := range ids {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteByte('"')
		for _, r := range id {
			switch {
			case r == '"' || r == '\\':
				out.WriteByte('\\')
				out.WriteRune(r)
			case r < 0x80:
				out.WriteRune(r)
			case r <= 0xffff:
				fmt.Fprintf(&out, "\\u%04x", r)
			default:
				high, low := utf16.EncodeRune(r)
				fmt.Fprintf(&out, "\\u%04x\\u%04x", high, low)
			}
		}
		out.WriteByte('"')
	}
	out.WriteByte(']')
	if out.Len() > 8192 {
		return "", true, permanent("request_invalid")
	}
	return out.String(), true, nil
}

func documentPermissionHeaders(connection connector.Connection, requestRef string) (map[string][]string, error) {
	header, present, err := documentPermissionHeader(connection)
	if err != nil || !present {
		return nil, err
	}
	if requestRef == "" || len(requestRef) > 512 || strings.TrimSpace(requestRef) != requestRef || !utf8.ValidString(requestRef) || strings.ContainsFunc(requestRef, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return nil, permanent("request_invalid")
	}
	return map[string][]string{"X-KB-Permission-Ids": {header}, "X-KB-Request-ID": {requestRef}}, nil
}
