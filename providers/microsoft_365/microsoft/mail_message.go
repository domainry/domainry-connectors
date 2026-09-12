package microsoft

import (
	"strings"
	"time"

	mail "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

type graphMailRecipient struct {
	EmailAddress *struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	} `json:"emailAddress"`
}
type graphMailMessage struct {
	ID                string                `json:"id"`
	ThreadID          string                `json:"conversationId"`
	InternetMessageID *string               `json:"internetMessageId"`
	Subject           *string               `json:"subject"`
	From              *graphMailRecipient   `json:"from"`
	To                *[]graphMailRecipient `json:"toRecipients"`
	CC                *[]graphMailRecipient `json:"ccRecipients"`
	ReplyTo           *[]graphMailRecipient `json:"replyTo"`
	ReceivedAt        string                `json:"receivedDateTime"`
	SentAt            string                `json:"sentDateTime"`
	IsRead            *bool                 `json:"isRead"`
	IsDraft           *bool                 `json:"isDraft"`
	Body              *struct {
		Type    string  `json:"contentType"`
		Content *string `json:"content"`
	} `json:"body"`
}

func (m graphMailMessage) summary() (mail.Summary, error) {
	out := mail.Summary{ID: m.ID, ThreadID: m.ThreadID, IsRead: m.IsRead, IsDraft: m.IsDraft, MetadataComplete: true}
	if !mail.ValidID(m.ID) || m.ThreadID != "" && !mail.ValidID(m.ThreadID) {
		return out, mailInvalidResponse()
	}
	if m.IsRead == nil || m.IsDraft == nil || m.Subject == nil || m.InternetMessageID == nil || m.From == nil || m.To == nil || m.CC == nil || m.ReplyTo == nil {
		out.MetadataComplete = false
	}
	var cut bool
	if m.Subject != nil {
		out.Subject, cut = mailcontent.Text(*m.Subject, 2048, false)
		out.MetadataComplete = out.MetadataComplete && !cut
	}
	if m.InternetMessageID != nil {
		out.InternetMessageID, cut = mailcontent.Text(*m.InternetMessageID, 2048, false)
		if cut {
			out.InternetMessageID = ""
			out.MetadataComplete = false
		}
	}
	from := []graphMailRecipient{}
	if m.From != nil {
		from = append(from, *m.From)
	}
	for _, target := range []struct {
		source *[]graphMailRecipient
		out    *[]mail.Address
	}{{&from, &out.From}, {m.To, &out.To}, {m.CC, &out.CC}, {m.ReplyTo, &out.ReplyTo}} {
		*target.out = []mail.Address{}
		if target.source == nil {
			continue
		}
		for _, item := range *target.source {
			if len(*target.out) == 50 {
				out.MetadataComplete = false
				break
			}
			if item.EmailAddress == nil {
				out.MetadataComplete = false
				continue
			}
			address, cut := mailcontent.Text(item.EmailAddress.Address, 320, false)
			if cut || address == "" {
				out.MetadataComplete = false
				continue
			}
			name, cut := mailcontent.Text(item.EmailAddress.Name, 512, false)
			out.MetadataComplete = out.MetadataComplete && !cut
			*target.out = append(*target.out, mail.Address{Name: name, Address: address})
		}
	}
	for _, target := range []struct {
		source string
		out    *string
	}{{m.ReceivedAt, &out.ReceivedAt}, {m.SentAt, &out.SentAt}} {
		if target.source == "" {
			out.MetadataComplete = false
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, target.source)
		if err != nil {
			return out, mailInvalidResponse()
		}
		*target.out = t.Format(time.RFC3339Nano)
	}
	if out.Validate() != nil {
		return out, mailInvalidResponse()
	}
	return out, nil
}
func (m graphMailMessage) body(limit int) mail.Body {
	out := mail.Body{OmittedReasons: []string{}}
	if m.Body == nil || m.Body.Content == nil {
		out.OmittedReasons = []string{"body_missing"}
		return out
	}
	text := *m.Body.Content
	switch strings.ToLower(m.Body.Type) {
	case "text":
	case "html":
		var err error
		text, err = mailcontent.HTMLText(text)
		if err != nil {
			out.OmittedReasons = []string{"body_html_invalid"}
			return out
		}
	default:
		out.OmittedReasons = []string{"body_format_unsupported"}
		return out
	}
	var cut bool
	out.Text, cut = mailcontent.Text(text, limit, true)
	if cut {
		out.OmittedReasons = []string{"body_truncated"}
	}
	out.Complete = len(out.OmittedReasons) == 0
	return out
}
