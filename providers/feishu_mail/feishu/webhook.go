package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

const feishuMessageReceivedEventV1 = "mail.user_mailbox.event.message_received_v1"

type messageReceivedPayload struct {
	Header struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
	} `json:"header"`
	Event struct {
		MailAddress string `json:"mail_address"`
		MessageID   string `json:"message_id"`
		MailboxType int    `json:"mailbox_type"`
		Subscriber  struct {
			UserIDs []struct {
				UserID  string `json:"user_id"`
				OpenID  string `json:"open_id"`
				UnionID string `json:"union_id"`
			} `json:"user_ids"`
		} `json:"subscriber"`
	} `json:"event"`
}

func (*provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.VerifiedWebhook{}, permanent("webhook_provider_mismatch", "Feishu Mail webhook provider mismatch")
	}
	verified, err := internalfeishu.VerifyWebhook(ctx, request, "feishu_mail")
	if err != nil || verified.Challenge != "" {
		return verified, err
	}
	var payload messageReceivedPayload
	if json.Unmarshal(verified.Payload, &payload) != nil || payload.Header.EventID != verified.ExternalID || payload.Header.EventType != feishuMessageReceivedEventV1 || verified.EventType != feishuMessageReceivedEventV1 {
		return connector.VerifiedWebhook{}, permanent("webhook_event_invalid", "Feishu Mail webhook event is invalid")
	}
	payload.Event.MailAddress = strings.ToLower(strings.TrimSpace(payload.Event.MailAddress))
	payload.Event.MessageID = strings.TrimSpace(payload.Event.MessageID)
	if payload.Event.MailAddress == "" || payload.Event.MessageID == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_event_invalid", "Feishu Mail webhook lacks mailbox or message identity")
	}
	type route struct {
		Kind  string `json:"kind"`
		Value string `json:"value"`
	}
	routes := []route{{Kind: "email", Value: payload.Event.MailAddress}}
	seen := map[string]bool{"email\x00" + payload.Event.MailAddress: true}
	for _, user := range payload.Event.Subscriber.UserIDs {
		for _, candidate := range []route{{Kind: "open_id", Value: user.OpenID}, {Kind: "union_id", Value: user.UnionID}, {Kind: "user_id", Value: user.UserID}} {
			candidate.Value = strings.TrimSpace(candidate.Value)
			identity := candidate.Kind + "\x00" + candidate.Value
			if candidate.Value == "" || seen[identity] {
				continue
			}
			seen[identity] = true
			routes = append(routes, candidate)
		}
	}
	if len(routes) > 100 {
		return connector.VerifiedWebhook{}, permanent("webhook_route_invalid", "Feishu Mail webhook has too many account routes")
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Kind != routes[j].Kind {
			return routes[i].Kind < routes[j].Kind
		}
		return routes[i].Value < routes[j].Value
	})
	envelope, err := json.Marshal(map[string]any{
		"routes": routes, "wake_tasks": []string{mailSyncTaskKey},
		"data": map[string]any{"mail_address": payload.Event.MailAddress, "message_id": payload.Event.MessageID, "mailbox_type": payload.Event.MailboxType},
	})
	if err != nil {
		return connector.VerifiedWebhook{}, connector.PermanentError("feishu_mail.webhook_event_invalid", errors.New("encode Feishu Mail webhook event"))
	}
	verified.Payload = envelope
	verified.EventType = "feishu_mail.message.notification"
	return verified, nil
}

var _ connector.WebhookVerifier = (*provider)(nil)
