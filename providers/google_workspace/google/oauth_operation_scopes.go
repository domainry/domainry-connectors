package google

import (
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	mail "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

// Each read contract declares only grants sufficient for its selected content.
// Metadata-only mail and freebusy-only calendar grants remain limited to their
// respective operations. The upstream service still owns per-resource ACLs.
func (*provider) OAuthOperationScopes(key string) ([][]string, bool) {
	var scopes []string
	switch key {
	case mail.ListOperationKey:
		return [][]string{{"https://www.googleapis.com/auth/gmail.metadata"}, {"https://www.googleapis.com/auth/gmail.readonly"}, {"https://www.googleapis.com/auth/gmail.modify"}, {"https://mail.google.com/"}}, true
	case mail.SearchOperationKey, mail.ReadOperationKey:
		return [][]string{{"https://www.googleapis.com/auth/gmail.readonly"}, {"https://www.googleapis.com/auth/gmail.modify"}, {"https://mail.google.com/"}}, true
	case calendar.ListOperationKey:
		scopes = []string{"calendar.readonly", "calendar", "calendar.calendarlist", "calendar.calendarlist.readonly"}
	case calendar.EventsOperationKey, calendar.EventOperationKey, calendarwrite.InspectOperationKey:
		scopes = []string{"calendar.readonly", "calendar", "calendar.events.readonly", "calendar.events", "calendar.app.created", "calendar.events.owned", "calendar.events.owned.readonly", "calendar.events.public.readonly"}
	case calendar.AvailabilityOperationKey:
		scopes = []string{"calendar.readonly", "calendar", "calendar.events.freebusy", "calendar.freebusy"}
	case calendarwrite.CreateOperationKey, calendarwrite.UpdateOperationKey:
		scopes = []string{"calendar", "calendar.events", "calendar.app.created", "calendar.events.owned"}
	case mailwrite.SendOperationKey, mailwrite.ReplyOperationKey:
		// Profile lookup establishes From. Reply additionally reads current
		// original headers; compose alone does not authorize messages.get.
		prefix := "https://www.googleapis.com/auth/gmail."
		alternatives := [][]string{{prefix + "modify"}, {"https://mail.google.com/"}}
		for _, read := range []string{"metadata", "readonly"} {
			alternatives = append(alternatives, []string{prefix + "send", prefix + read})
			if key == mailwrite.ReplyOperationKey {
				alternatives = append(alternatives, []string{prefix + "compose", prefix + read})
			}
		}
		if key == mailwrite.SendOperationKey {
			alternatives = append(alternatives, []string{prefix + "compose"})
		}
		return alternatives, true
	default:
		return nil, false
	}
	alternatives := make([][]string, 0, len(scopes))
	for _, scope := range scopes {
		alternatives = append(alternatives, []string{"https://www.googleapis.com/auth/" + scope})
	}
	return alternatives, true
}
