package google

import (
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"

	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

type googleMailHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Obsolete named zones can be resolved differently by time.Parse depending on
// the host's Local zone. Preserve only explicit offsets or universal GMT/UT;
// an unresolved sender date stays unknown. Gmail internalDate remains usable.
var mailDateZone = regexp.MustCompile(`\s(?:[+-][0-9]{4}|UT|GMT)\s*(?:\([^()]*\)\s*)*$`)

type googleMailBody struct {
	AttachmentID string `json:"attachmentId"`
	Size         *int   `json:"size"`
	Data         string `json:"data"`
}
type googleMailPart struct {
	MIMEType string             `json:"mimeType"`
	Filename string             `json:"filename"`
	Headers  []googleMailHeader `json:"headers"`
	Body     *googleMailBody    `json:"body"`
	Parts    []googleMailPart   `json:"parts"`
}
type googleMailMessage struct {
	ID           string          `json:"id"`
	ThreadID     string          `json:"threadId"`
	Labels       *[]string       `json:"labelIds"`
	InternalDate string          `json:"internalDate"`
	Payload      *googleMailPart `json:"payload"`
}

func (p *googleMailPart) headers(name string) []string {
	out := []string{}
	if p != nil {
		for _, h := range p.Headers {
			if strings.EqualFold(h.Name, name) {
				out = append(out, h.Value)
			}
		}
	}
	return out
}
func (m googleMailMessage) summary() (maildto.Summary, error) {
	out := maildto.Summary{ID: m.ID, ThreadID: m.ThreadID, MetadataComplete: true}
	if !maildto.ValidID(m.ID) || !maildto.ValidID(m.ThreadID) || m.Payload == nil || len(m.Payload.Headers) > 200 {
		return out, mailInvalidResponse()
	}
	for _, target := range []struct {
		name string
		to   *[]maildto.Address
	}{{"From", &out.From}, {"To", &out.To}, {"Cc", &out.CC}, {"Reply-To", &out.ReplyTo}} {
		list, omitted := mailcontent.Addresses(m.Payload.headers(target.name))
		*target.to = list
		out.MetadataComplete = out.MetadataComplete && !omitted
	}
	one := func(name string) string {
		values := m.Payload.headers(name)
		if len(values) == 0 {
			return ""
		}
		if len(values) != 1 {
			out.MetadataComplete = false
			return ""
		}
		return values[0]
	}
	var omitted bool
	out.Subject, omitted = mailcontent.Header(one("Subject"), 2048)
	out.MetadataComplete = out.MetadataComplete && !omitted
	out.InternetMessageID, omitted = mailcontent.Text(strings.TrimSpace(one("Message-ID")), 2048, false)
	if omitted {
		out.InternetMessageID = ""
		out.MetadataComplete = false
	}
	if raw := one("Date"); raw != "" {
		date, err := mail.ParseDate(raw)
		if err == nil && mailDateZone.MatchString(raw) {
			out.SentAt = date.Format(time.RFC3339Nano)
		} else {
			out.MetadataComplete = false
		}
	}
	if m.InternalDate != "" {
		millis, err := strconv.ParseInt(m.InternalDate, 10, 64)
		if err != nil || millis < 0 {
			return out, mailInvalidResponse()
		}
		out.ReceivedAt = time.UnixMilli(millis).UTC().Format(time.RFC3339Nano)
	} else {
		out.MetadataComplete = false
	}
	if m.Labels != nil {
		isRead, isDraft := true, false
		for _, label := range *m.Labels {
			if label == "UNREAD" {
				isRead = false
			}
			if label == "DRAFT" {
				isDraft = true
			}
		}
		out.IsRead, out.IsDraft = &isRead, &isDraft
	} else {
		out.MetadataComplete = false
	}
	if out.Validate() != nil {
		return out, mailInvalidResponse()
	}
	return out, nil
}
