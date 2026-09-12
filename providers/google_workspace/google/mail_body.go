package google

import (
	"context"
	"encoding/base64"
	"mime"
	"net/url"
	"strings"

	mail "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

type googleBodyReader struct {
	s              *mailSession
	id             string
	parts, fetches int
}
type googleBodyText struct {
	text    string
	found   bool
	reasons []string
}

func omission(reason string) googleBodyText { return googleBodyText{reasons: []string{reason}} }
func addReason(reasons []string, reason string) []string {
	for _, old := range reasons {
		if old == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}
func (s *mailSession) body(ctx context.Context, id string, p *googleMailPart, limit int) (mail.Body, error) {
	out := mail.Body{OmittedReasons: []string{}}
	r := googleBodyReader{s: s, id: id}
	part, err := r.read(ctx, p, 0)
	if err != nil {
		return out, err
	}
	out.OmittedReasons = append(out.OmittedReasons, part.reasons...)
	if !part.found {
		out.OmittedReasons = addReason(out.OmittedReasons, "body_unavailable")
	}
	var clipped bool
	out.Text, clipped = mailcontent.Text(part.text, limit, true)
	if clipped {
		out.OmittedReasons = addReason(out.OmittedReasons, "body_truncated")
	}
	out.Complete = len(out.OmittedReasons) == 0
	return out, nil
}
func mailMIME(p *googleMailPart) (string, map[string]string, bool) {
	if p == nil {
		return "", nil, false
	}
	typeName := strings.ToLower(p.MIMEType)
	values := p.headers("Content-Type")
	if len(values) > 1 {
		return "", nil, false
	}
	if len(values) == 0 {
		return typeName, nil, typeName != ""
	}
	parsed, params, err := mime.ParseMediaType(values[0])
	return typeName, params, err == nil && parsed == typeName
}
func mailAttachment(p *googleMailPart) bool {
	if p.Filename != "" {
		return true
	}
	for _, value := range p.headers("Content-Disposition") {
		t, _, err := mime.ParseMediaType(value)
		if err != nil || t == "attachment" {
			return true
		}
	}
	return false
}

// Alternative bodies are different representations of the same message.
// Prefer plain text and never concatenate the HTML duplicate.
func bodyPreference(p *googleMailPart, depth int) int {
	if p == nil || depth > 16 || mailAttachment(p) {
		return 0
	}
	t, _, ok := mailMIME(p)
	if !ok {
		return 0
	}
	if t == "text/plain" {
		return 2
	}
	if t == "text/html" {
		return 1
	}
	if !strings.HasPrefix(t, "multipart/") || len(p.Parts) > 256 {
		return 0
	}
	best := 0
	for i := range p.Parts {
		score := bodyPreference(&p.Parts[i], depth+1)
		if score > best {
			best = score
		}
		if best == 2 {
			break
		}
	}
	return best
}
func (r *googleBodyReader) read(ctx context.Context, p *googleMailPart, depth int) (googleBodyText, error) {
	if err := ctx.Err(); err != nil {
		return googleBodyText{}, err
	}
	r.parts++
	if depth > 16 || r.parts > 256 {
		return omission("mime_limit"), nil
	}
	if p == nil {
		return omission("body_missing"), nil
	}
	if len(p.Headers) > 200 {
		return omission("mime_limit"), nil
	}
	if mailAttachment(p) {
		return googleBodyText{}, nil
	}
	t, params, ok := mailMIME(p)
	if !ok {
		return omission("mime_unsupported"), nil
	}
	if strings.HasPrefix(t, "multipart/") {
		if len(p.Parts) == 0 {
			return omission("body_missing"), nil
		}
		if len(p.Parts) > 256 {
			return omission("mime_limit"), nil
		}
		if t == "multipart/related" {
			selected := 0
			if start := params["start"]; start != "" {
				selected = -1
				for i := range p.Parts {
					ids := p.Parts[i].headers("Content-ID")
					if len(ids) == 1 && strings.TrimSpace(ids[0]) == start {
						if selected != -1 {
							return omission("mime_unsupported"), nil
						}
						selected = i
					}
				}
			}
			if selected < 0 {
				return omission("body_missing"), nil
			}
			if declared := params["type"]; declared != "" && !strings.EqualFold(declared, p.Parts[selected].MIMEType) {
				return omission("mime_unsupported"), nil
			}
			return r.read(ctx, &p.Parts[selected], depth+1)
		}
		if t == "multipart/alternative" {
			selected, best := -1, 0
			for i := range p.Parts {
				score := bodyPreference(&p.Parts[i], depth+1)
				if score > best {
					selected, best = i, score
				}
			}
			if selected < 0 {
				return omission("mime_unsupported"), nil
			}
			return r.read(ctx, &p.Parts[selected], depth+1)
		}
		out := googleBodyText{}
		texts := []string{}
		for i := range p.Parts {
			item, err := r.read(ctx, &p.Parts[i], depth+1)
			if err != nil {
				return out, err
			}
			if item.found {
				texts = append(texts, item.text)
				out.found = true
			}
			for _, reason := range item.reasons {
				out.reasons = addReason(out.reasons, reason)
			}
			if r.parts > 256 {
				break
			}
		}
		out.text = strings.Join(texts, "\n")
		return out, nil
	}
	// Attached email, image, calendar, signed/encrypted or other MIME payloads
	// are not textual message content. No attachment or remote URL is followed.
	if t != "text/plain" && t != "text/html" {
		return googleBodyText{}, nil
	}
	if len(p.Parts) > 0 || p.Body == nil || p.Body.Size == nil || *p.Body.Size < 0 {
		return omission("body_missing"), nil
	}
	body := *p.Body
	if body.AttachmentID != "" {
		if body.Data != "" || !mail.ValidID(body.AttachmentID) {
			return omission("body_invalid"), nil
		}
		if *body.Size > 256<<10 {
			return omission("body_part_too_large"), nil
		}
		if r.fetches >= 4 {
			return omission("body_part_limit"), nil
		}
		r.fetches++
		var fetched googleMailBody
		if err := r.s.get(ctx, "messages/"+url.PathEscape(r.id)+"/attachments/"+url.PathEscape(body.AttachmentID), nil, &fetched); err != nil {
			return googleBodyText{}, err
		}
		if fetched.AttachmentID != "" || fetched.Size == nil || *fetched.Size != *body.Size {
			return omission("body_invalid"), nil
		}
		body = fetched
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(body.Data, "="))
	if err != nil || len(data) != *body.Size {
		return omission("body_invalid"), nil
	}
	text, err := mailcontent.DecodeText(data, params["charset"])
	if err != nil {
		return omission("body_encoding_unsupported"), nil
	}
	if t == "text/html" {
		text, err = mailcontent.HTMLText(text)
		if err != nil {
			return omission("body_html_invalid"), nil
		}
	}
	return googleBodyText{text: text, found: true}, nil
}
