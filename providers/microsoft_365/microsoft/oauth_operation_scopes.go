package microsoft

import (
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	mail "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

// Basic grants enable only operations that return metadata. Search and content
// reads require the matching content grant. Only delegated grants apply to /me.
func (*provider) OAuthOperationScopes(key string) ([][]string, bool) {
	scopes := []string{"Calendars.Read", "Calendars.ReadWrite", "Calendars.Read.Shared", "Calendars.ReadWrite.Shared"}
	switch key {
	case mail.ListOperationKey:
		scopes = []string{"Mail.ReadBasic", "Mail.ReadBasic.Shared", "Mail.Read", "Mail.ReadWrite", "Mail.Read.Shared", "Mail.ReadWrite.Shared"}
	case mail.SearchOperationKey, mail.ReadOperationKey:
		scopes = []string{"Mail.Read", "Mail.ReadWrite", "Mail.Read.Shared", "Mail.ReadWrite.Shared"}
	case calendar.ListOperationKey, calendar.EventsOperationKey, calendar.AvailabilityOperationKey:
		scopes = append(scopes, "Calendars.ReadBasic")
	case calendar.EventOperationKey, calendarwrite.InspectOperationKey:
	case calendarwrite.CreateOperationKey, calendarwrite.UpdateOperationKey:
		var out [][]string
		// supportedTimeZones is configured per mailbox and requires User.Read
		// (or the documented higher grants), independently of calendar writes.
		for _, write := range []string{"Calendars.ReadWrite", "Calendars.ReadWrite.Shared"} {
			for _, profile := range []string{"User.Read", "User.Read.All", "User.ReadBasic.All"} {
				out = append(out, graphScopeSpellings(write, profile)...)
			}
		}
		return out, true
	case mailwrite.SendOperationKey:
		return append(graphScopeSpellings("Mail.Send"), graphScopeSpellings("Mail.Send.Shared")...), true
	case mailwrite.ReplyOperationKey:
		var out [][]string
		for _, send := range []string{"Mail.Send", "Mail.Send.Shared"} {
			for _, read := range []string{"Mail.ReadBasic", "Mail.ReadBasic.Shared", "Mail.Read", "Mail.ReadWrite", "Mail.Read.Shared", "Mail.ReadWrite.Shared"} {
				out = append(out, graphScopeSpellings(send, read)...)
			}
		}
		return out, true
	default:
		return nil, false
	}
	alternatives := make([][]string, 0, len(scopes)*2)
	for _, scope := range scopes {
		alternatives = append(alternatives, []string{scope}, []string{"https://graph.microsoft.com/" + scope})
	}
	return alternatives, true
}

// Each required permission can independently use its qualified or short form.
func graphScopeSpellings(required ...string) [][]string {
	out := [][]string{{}}
	for _, scope := range required {
		next := [][]string{}
		for _, prefix := range out {
			for _, value := range []string{scope, "https://graph.microsoft.com/" + scope} {
				values := append([]string(nil), prefix...)
				next = append(next, append(values, value))
			}
		}
		out = next
	}
	return out
}
