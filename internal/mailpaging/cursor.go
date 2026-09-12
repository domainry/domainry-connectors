// Package mailpaging binds an opaque provider continuation to its exact query.
// The digest prevents accidental cross-query reuse; it is not authorization.
package mailpaging

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Cursor struct {
	Version int    `json:"version"`
	Scope   string `json:"scope"`
	Token   string `json:"token"`
	Seen    int    `json:"seen"`
}

func Scope(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func ValidToken(token string) bool {
	return token != "" && len(token) <= 4096 && utf8.ValidString(token) && !strings.ContainsFunc(token, unicode.IsControl)
}
func Encode(scope, token string, seen int) (string, error) {
	if !ValidToken(token) || seen < 0 {
		return "", errors.New("invalid mail continuation")
	}
	b, _ := json.Marshal(Cursor{Version: 1, Scope: scope, Token: token, Seen: seen})
	out := base64.RawURLEncoding.EncodeToString(b)
	if len(out) > 8192 {
		return "", errors.New("mail continuation exceeds limit")
	}
	return out, nil
}
func Decode(scope, raw string) (Cursor, error) {
	var out Cursor
	if raw == "" {
		return out, nil
	}
	invalid := errors.New("invalid mail continuation")
	if len(raw) > 8192 {
		return out, invalid
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return out, invalid
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || d.Decode(new(any)) != io.EOF || out.Version != 1 || out.Scope != scope || !ValidToken(out.Token) || out.Seen < 0 {
		return Cursor{}, invalid
	}
	return out, nil
}
