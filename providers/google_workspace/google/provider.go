// Package google implements the official Google Workspace synchronization Provider.
package google

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey               = "google_workspace"
	ProviderKey                = "google"
	defaultAPIBaseURL          = "https://www.googleapis.com"
	defaultGmailBaseURL        = "https://gmail.googleapis.com"
	defaultPubSubBaseURL       = "https://pubsub.googleapis.com"
	defaultTokenURL            = "https://oauth2.googleapis.com/token"
	defaultTimeout             = 30
	maximumTimeout             = 300
	responseLimit        int64 = 4 << 20
)

type Response map[string]any
type SyncCalendarInput struct {
	CalendarID string `json:"calendar_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	PageToken  string `json:"page_token,omitempty"`
	SyncToken  string `json:"sync_token,omitempty"`
	UpdatedMin string `json:"updated_min,omitempty"`
}
type SyncDriveFileRefsInput struct {
	Limit     int    `json:"limit,omitempty"`
	PageToken string `json:"page_token,omitempty"`
	Query     string `json:"query,omitempty"`
}
type SyncEmailHistoryInput struct {
	StartHistoryID string `json:"start_history_id"`
	Limit          int    `json:"limit,omitempty"`
	PageToken      string `json:"page_token,omitempty"`
	LabelID        string `json:"label_id,omitempty"`
}
type GmailListMessagesInput struct {
	Limit     int    `json:"limit,omitempty"`
	PageToken string `json:"page_token,omitempty"`
	LabelID   string `json:"label_id,omitempty"`
	Query     string `json:"query,omitempty"`
}
type GmailGetMessageInput struct {
	MessageID string `json:"message_id"`
	Format    string `json:"format,omitempty"`
}
type GmailWatchInput struct {
	TopicName string `json:"topic_name"`
	LabelID   string `json:"label_id,omitempty"`
}
type GmailSendMessageInput struct {
	To           any    `json:"to"`
	Subject      string `json:"subject"`
	Text         string `json:"text"`
	RFCMessageID string `json:"rfc_message_id"`
	Date         string `json:"date,omitempty"`
	InReplyTo    string `json:"in_reply_to,omitempty"`
	References   string `json:"references,omitempty"`
	ThreadID     string `json:"thread_id,omitempty"`
}
type PubSubResourceInput struct {
	ProjectID      string `json:"project_id"`
	TopicID        string `json:"topic_id,omitempty"`
	SubscriptionID string `json:"subscription_id,omitempty"`
}
type PubSubEnsureSubscriptionInput struct {
	ProjectID          string `json:"project_id"`
	TopicID            string `json:"topic_id"`
	SubscriptionID     string `json:"subscription_id"`
	AckDeadlineSeconds int    `json:"ack_deadline_seconds,omitempty"`
}
type PubSubSetTopicPolicyInput struct {
	ProjectID string         `json:"project_id"`
	TopicID   string         `json:"topic_id"`
	Policy    map[string]any `json:"policy"`
}
type PubSubPullInput struct {
	ProjectID      string `json:"project_id"`
	SubscriptionID string `json:"subscription_id"`
	MaxMessages    int    `json:"max_messages,omitempty"`
}
type PubSubAcknowledgeInput struct {
	ProjectID      string   `json:"project_id"`
	SubscriptionID string   `json:"subscription_id"`
	AckIDs         []string `json:"ack_ids"`
}

var (
	GmailGetMessage          = readOp[GmailGetMessageInput]("gmail_get_message", "fd63131ba3621a277bad6d12cae583c3b7dbb8d6160efb9ff9ecc497bf199bbc")
	GmailGetProfile          = readOp[struct{}]("gmail_get_profile", "d7a362cc7bc11aaa5eb1d7e84e45ddff284a177eeb295238fb11070f6c9dec26")
	GmailListMessages        = readOp[GmailListMessagesInput]("gmail_list_messages", "f696580b2823fe0924ece61a97fb667bfe9127c6ee7bc39d159757ab81df2d92")
	GmailSendMessage         = connector.EnqueueOperation[GmailSendMessageInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "gmail_send_message", ContractSHA256: "9f76169c0993f9abc01a70f4a27b0e04e49a2ab318bd23a6ee3cbe8c9b2ff649", Reliability: writeReliability()}
	GmailStop                = writeOp[struct{}]("gmail_stop", "c4503fac2720b68a86dfe33a4e6f4e82fa2d00e67968af62e55d2f5a93f7df38")
	GmailWatch               = writeOp[GmailWatchInput]("gmail_watch", "45250915092fe46cc248ff760cb8456526cc4733f32813008d35a3fb6e0f746d")
	PubSubAcknowledge        = writeOp[PubSubAcknowledgeInput]("pubsub_acknowledge", "bc3fcb26ff6a343ed9b80209d039bb4b8c06288f8cc9fcfe11f3ee31fb0c9bfd")
	PubSubDeleteSubscription = writeOp[PubSubResourceInput]("pubsub_delete_subscription", "55bd9833c49c013c0859bf924214fae535c675f2f668f4fcd529ce0c2d4deaa1")
	PubSubEnsureSubscription = writeOp[PubSubEnsureSubscriptionInput]("pubsub_ensure_subscription", "a27eaa2ee1147c0a2f875e75490650754f62225e467db833bbe764469e43f69d")
	PubSubEnsureTopic        = writeOp[PubSubResourceInput]("pubsub_ensure_topic", "eb5762e09843dba2872b999b4a0fc8565e43c8bcf456c035e7729f8bf788d213")
	PubSubGetTopicPolicy     = readOp[PubSubResourceInput]("pubsub_get_topic_policy", "ba762cbb3149c394b412928911be1f36213296fa81b3507681a2bb69475a3651")
	PubSubPull               = readOp[PubSubPullInput]("pubsub_pull", "5f0be78aa9e49fc990b4200de0c7fe9331c6ae9dc0c170a93f658286bce56ccf")
	PubSubSetTopicPolicy     = writeOp[PubSubSetTopicPolicyInput]("pubsub_set_topic_policy", "5b857ff9a6e6d73a87abc3256d46329c33fd883f4a41ab9b7b0098a0e6d16c2f")
	SyncCalendar             = readOp[SyncCalendarInput]("sync_calendar", "99bf434b3985bb7b3257936be1be59af60caebcf287ecf06ef8daaacb047989a")
	SyncDriveFileRefs        = readOp[SyncDriveFileRefsInput]("sync_drive_file_refs", "2101dde3285ca7a4e53983bd2c9bb5ed52792c3fb556103585658f4cbb7f93bc")
	SyncEmailHistory         = readOp[SyncEmailHistoryInput]("sync_email_history", "44e4ad5abfa1a37448d571030b9cb5a0e550ce3012f0baccf69084ab345bbfa1")
	TestConnection           = readOp[struct{}]("test_connection", "b5024bf0fdfcd11b45b9f7a7bfe304582c76ba5a1119246a7e66184035d43371")
)

func readOp[I any](key, hash string) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: readReliability()}
}
func writeOp[I any](key, hash string) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: writeReliability()}
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Google Workspace transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	bindings := []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) {
		return connector.BindCall(GmailGetMessage, p.gmailGetMessage)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(GmailGetProfile, p.gmailGetProfile)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(GmailListMessages, p.gmailListMessages)
	}, func() (connector.BoundOperation, error) {
		return connector.BindEnqueueDelivery(GmailSendMessage, p.gmailSendMessage)
	}, func() (connector.BoundOperation, error) { return connector.BindCall(GmailStop, p.gmailStop) }, func() (connector.BoundOperation, error) { return connector.BindCall(GmailWatch, p.gmailWatch) }, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubAcknowledge, p.pubsubAcknowledge)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubDeleteSubscription, p.pubsubDeleteSubscription)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubEnsureSubscription, p.pubsubEnsureSubscription)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubEnsureTopic, p.pubsubEnsureTopic)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubGetTopicPolicy, p.pubsubGetTopicPolicy)
	}, func() (connector.BoundOperation, error) { return connector.BindCall(PubSubPull, p.pubsubPull) }, func() (connector.BoundOperation, error) {
		return connector.BindCall(PubSubSetTopicPolicy, p.pubsubSetTopicPolicy)
	}, func() (connector.BoundOperation, error) { return connector.BindCall(SyncCalendar, p.syncCalendar) }, func() (connector.BoundOperation, error) { return connector.BindCall(SyncDriveFileRefs, p.syncDrive) }, func() (connector.BoundOperation, error) { return connector.BindCall(SyncEmailHistory, p.syncEmail) }, func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) }}
	bindings = append(bindings,
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CalendarEventInspect, p.calendarEventInspect)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CalendarEventCreate, p.calendarEventCreate)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CalendarEventUpdate, p.calendarEventUpdate)
		},
		func() (connector.BoundOperation, error) { return connector.BindCall(MailSend, p.mailSend) },
		func() (connector.BoundOperation, error) { return connector.BindCall(MailReply, p.mailReply) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CalendarList, p.calendarList) },
		func() (connector.BoundOperation, error) { return connector.BindCall(MailList, p.mailList) },
		func() (connector.BoundOperation, error) { return connector.BindCall(MailSearch, p.mailSearch) },
		func() (connector.BoundOperation, error) { return connector.BindCall(MailRead, p.mailRead) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CalendarEvents, p.calendarEvents) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CalendarEvent, p.calendarEvent) },
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CalendarAvailability, p.calendarAvailability)
		},
	)
	operations := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		op, err := bind()
		if err != nil {
			return nil, err
		}
		operations = append(operations, op)
	}
	adapter, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(maximumTimeout)
	gmailPollMaximum := float64(3600)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.3.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Google API Base URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://www.googleapis.com"`)}, {Key: "gmail_base_url", Name: "Gmail API Base URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://gmail.googleapis.com"`)}, {Key: "pubsub_base_url", Name: "Pub/Sub API Base URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://pubsub.googleapis.com"`)}, {Key: "token_url", Name: "OAuth Token URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://oauth2.googleapis.com/token"`)}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}, {Key: "gmail_ingest_enabled", Name: "Gmail Ingest Enabled", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}, {Key: "gmail_ingest_label", Name: "Gmail Ingest Label", Type: connector.ConfigFieldText, Default: json.RawMessage(`"INBOX"`)}, {Key: "gmail_ingest_query", Name: "Gmail Ingest Query", Type: connector.ConfigFieldText}, {Key: "gmail_ingest_bootstrap", Name: "Gmail Ingest Bootstrap", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}, {Key: "gmail_poll_seconds", Name: "Gmail Poll Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`60`), Validation: connector.ConfigValidation{Min: &minimum, Max: &gmailPollMaximum}}, {Key: "gmail_watch_enabled", Name: "Managed Gmail Watch Enabled", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)}, {Key: "gmail_pubsub_project_id", Name: "Gmail Pub/Sub Project ID", Type: connector.ConfigFieldText}, {Key: "gmail_pubsub_topic", Name: "Gmail Pub/Sub Topic", Type: connector.ConfigFieldText, Default: json.RawMessage(`"domainry-gmail-events"`)}, {Key: "gmail_pubsub_subscription", Name: "Gmail Pub/Sub Subscription", Type: connector.ConfigFieldText}}, SecretFields: []connector.SecretField{secret("access_token", "Access Token", connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh), secret("refresh_token", "Refresh Token", connector.SecretCredentialRefreshToken, connector.SecretRotationOAuthRefresh), secret("client_id", "OAuth Client ID", connector.SecretCredentialIdentifier, connector.SecretRotationManual), secret("client_secret", "OAuth Client Secret", connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual)}}
}
func secret(key, name string, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, raw := range []string{apiBase(connection), gmailBase(connection), pubsubBase(connection), tokenURL(connection)} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && loopback(parsed.Hostname()))) {
			return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoints are required")
		}
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}
