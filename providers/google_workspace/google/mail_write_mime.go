package google

import (
	"encoding/base64"
	"net/mail"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

func writeMailbox(s string) bool {
	a, err := mail.ParseAddress(s)
	return err == nil && a.Address == s && a.Name == "" && len(s) <= 320 && !strings.ContainsFunc(s, unicode.IsControl)
}

// Deliberately support a bounded dot-atom RFC identity. Exotic quoted/comment
// forms require another adapter path; never clean and then reuse hostile input.
func writeMessageID(s string) bool {
	if len(s) < 5 || len(s) > 900 || s[0] != '<' || s[len(s)-1] != '>' {
		return false
	}
	s = s[1 : len(s)-1]
	if strings.Count(s, "@") != 1 || strings.HasPrefix(s, "@") || strings.HasSuffix(s, "@") {
		return false
	}
	for _, side := range strings.Split(s, "@") {
		for _, atom := range strings.Split(side, ".") {
			if atom == "" {
				return false
			}
		}
	}
	for _, c := range s {
		if c < 33 || c > 126 || strings.ContainsRune("<>()[,:;\\\"]", c) {
			return false
		}
	}
	return true
}

func writeReferences(headers []string, original string) ([]string, error) {
	if !writeMessageID(original) || len(headers) > 1 {
		return nil, permanent("mail.reply_identity_invalid", "original mail has no safe RFC reply identity")
	}
	refs := []string{}
	if len(headers) == 1 {
		if len(headers[0]) > 8192 {
			return nil, permanent("mail.reply_identity_invalid", "original References exceeds the supported limit")
		}
		refs = strings.Fields(headers[0])
		if len(refs) > 50 {
			return nil, permanent("mail.reply_identity_invalid", "original References exceeds the supported limit")
		}
		for _, ref := range refs {
			if !writeMessageID(ref) {
				return nil, permanent("mail.reply_identity_invalid", "original References contains an invalid identity")
			}
		}
	}
	if len(refs) == 0 || refs[len(refs)-1] != original {
		refs = append(refs, original)
	}
	return refs, nil
}

// Encode even ASCII subjects so long unbroken text stays within RFC line
// limits without inserting visible spaces into the user's subject.
func writeSubject(s string) string {
	words := []string{}
	for len(s) > 0 {
		n := min(42, len(s))
		for !utf8.ValidString(s[:n]) {
			n--
		}
		words = append(words, "=?UTF-8?B?"+base64.StdEncoding.EncodeToString([]byte(s[:n]))+"?=")
		s = s[n:]
	}
	return strings.Join(words, "\r\n ")
}

func accountMailMIME(m mailwrite.Message, from, id string, at time.Time, thread, reply string, references []string) map[string]any {
	lines := []string{"From: " + (&mail.Address{Address: from}).String()}
	for _, group := range []struct {
		name   string
		values []maildto.Address
	}{{"To", m.To}, {"Cc", m.CC}, {"Bcc", m.BCC}} {
		if len(group.values) == 0 {
			continue
		}
		addresses := make([]string, 0, len(group.values))
		for _, a := range group.values {
			address := (&mail.Address{Address: a.Address}).String()
			if a.Name != "" {
				address = writeSubject(a.Name) + " " + address
			}
			addresses = append(addresses, address)
		}
		lines = append(lines, group.name+": "+strings.Join(addresses, ",\r\n "))
	}
	lines = append(lines, "Subject: "+writeSubject(m.Subject), "Date: "+at.UTC().Format(time.RFC1123Z), "Message-ID: "+id, "MIME-Version: 1.0", "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: base64")
	if reply != "" {
		lines = append(lines, "In-Reply-To: "+reply, "References: "+strings.Join(references, "\r\n "))
	}
	text := base64.StdEncoding.EncodeToString([]byte(m.Text))
	var body strings.Builder
	for len(text) > 0 {
		n := min(76, len(text))
		body.WriteString(text[:n])
		body.WriteString("\r\n")
		text = text[n:]
	}
	raw := strings.Join(lines, "\r\n") + "\r\n\r\n" + body.String()
	out := map[string]any{"raw": base64.RawURLEncoding.EncodeToString([]byte(raw))}
	if thread != "" {
		out["threadId"] = thread
	}
	return out
}
