// Package mailcontent handles deterministic, bounded mail text conversion.
// It performs no network access and never renders HTML or follows references.
package mailcontent

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"net/mail"
	"strings"
	"unicode"
	"unicode/utf8"

	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connectors/internal/textcontent"
	"golang.org/x/net/html/charset"
)

// Text clips at a UTF-8 boundary after removing non-text control characters.
func Text(s string, max int, multiline bool) (string, bool) {
	changed := !utf8.ValidString(s)
	s = strings.ToValidUTF8(s, "�")
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			if multiline && (r == '\n' || r == '\t') {
				return r
			}
			if r == '\r' || r == '\n' || r == '\t' {
				return ' '
			}
			changed = true
			return -1
		}
		return r
	}, s)
	if len(s) > max {
		s = s[:max]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		changed = true
	}
	return s, changed
}

func CharsetReader(name string, r io.Reader) (io.Reader, error) {
	enc, _ := charset.Lookup(name)
	if enc == nil {
		return nil, errors.New("unsupported mail charset")
	}
	return enc.NewDecoder().Reader(r), nil
}

func DecodeText(data []byte, encoding string) (string, error) {
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	if encoding == "" || encoding == "utf-8" || encoding == "utf8" || encoding == "us-ascii" {
		if !utf8.Valid(data) {
			return "", errors.New("invalid mail text encoding")
		}
		if encoding == "us-ascii" {
			for _, b := range data {
				if b > 127 {
					return "", errors.New("invalid ASCII mail text")
				}
			}
		}
		return string(data), nil
	}
	r, err := CharsetReader(encoding, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	b, err := io.ReadAll(io.LimitReader(r, 4<<20+1))
	if err != nil || len(b) > 4<<20 || !utf8.Valid(b) || bytes.Contains(b, []byte("�")) {
		return "", errors.New("invalid mail text encoding")
	}
	return string(b), nil
}

func Header(value string, max int) (string, bool) {
	d := mime.WordDecoder{CharsetReader: CharsetReader}
	s, err := d.DecodeHeader(value)
	if err != nil {
		return "", true
	}
	return Text(s, max, false)
}

func Addresses(values []string) ([]maildto.Address, bool) {
	out := []maildto.Address{}
	omitted := false
	p := mail.AddressParser{WordDecoder: &mime.WordDecoder{CharsetReader: CharsetReader}}
	for _, value := range values {
		items, err := p.ParseList(value)
		if err != nil {
			omitted = true
			continue
		}
		for _, item := range items {
			if len(out) == 50 {
				omitted = true
				break
			}
			address, bad := Text(item.Address, 320, false)
			if bad || address == "" {
				omitted = true
				continue
			}
			name, cut := Text(item.Name, 512, false)
			omitted = omitted || cut
			out = append(out, maildto.Address{Name: name, Address: address})
		}
	}
	return out, omitted
}

// HTMLText retains the mail helper entry point while sharing only neutral text conversion.
func HTMLText(source string) (string, error) { return textcontent.HTMLText(source) }
