package notificationmessage

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
)

// ResolveProviderPayload supports already-persisted legacy Outbox messages
// while all newly authored messages use Content. The returned map is always a
// clone because Providers append recipient and idempotency fields.
func ResolveProviderPayload(provider string, content, legacy map[string]any) (map[string]any, error) {
	if len(content) > 0 {
		decoded, err := Decode(content)
		if err != nil {
			return nil, err
		}
		return Compile(provider, decoded)
	}
	if len(legacy) == 0 {
		return nil, nil
	}
	result := make(map[string]any, len(legacy))
	for key, value := range legacy {
		result[key] = value
	}
	return result, nil
}

// Compile translates portable notification content into one provider's native
// request fragment. Recipient identity, credentials, idempotency and transport
// fields remain owned by the calling Provider.
func Compile(provider string, content Content) (map[string]any, error) {
	switch strings.TrimSpace(provider) {
	case "slack":
		return compileSlack(content), nil
	case "feishu":
		return compileFeishu(content)
	case "dingtalk":
		return compileDingTalk(content), nil
	case "enterprise_wechat":
		return compileEnterpriseWeChat(content), nil
	case "discord":
		return compileDiscord(content), nil
	case "google_workspace":
		return compileGoogleChat(content), nil
	case "teams", "microsoft_365":
		return compileTeams(content)
	case "line_works":
		return compileLineWorks(content), nil
	case "line":
		return compileLine(content), nil
	case "meta_cloud_api":
		return compileWhatsApp(content), nil
	default:
		return nil, fmt.Errorf("notification provider %q is unsupported", provider)
	}
}

func title(value Content) string {
	if strings.TrimSpace(value.Title) != "" {
		return value.Title
	}
	return value.Subject
}

func body(value Content) string {
	if strings.TrimSpace(value.Markdown) != "" {
		return value.Markdown
	}
	return value.Text
}

func compileWhatsApp(value Content) map[string]any {
	if value.ProviderTemplate == nil {
		return map[string]any{"type": "text", "text": map[string]any{"preview_url": false, "body": value.Message}}
	}
	components := make([]any, 0, len(value.ProviderTemplate.Components))
	for _, component := range value.ProviderTemplate.Components {
		parameters := make([]any, 0, len(component.Parameters))
		for _, parameter := range component.Parameters {
			if component.Type == "button" && component.SubType == "quick_reply" {
				parameters = append(parameters, map[string]any{"type": "payload", "payload": parameter})
			} else {
				parameters = append(parameters, map[string]any{"type": "text", "text": parameter})
			}
		}
		compiled := map[string]any{"type": component.Type, "parameters": parameters}
		if component.SubType != "" {
			compiled["sub_type"] = component.SubType
		}
		if component.Index != "" {
			compiled["index"] = component.Index
		}
		components = append(components, compiled)
	}
	return map[string]any{"type": "template", "template": map[string]any{"name": value.ProviderTemplate.Name, "language": map[string]any{"code": value.ProviderTemplate.Language}, "components": components}}
}

func compileSlack(value Content) map[string]any {
	blocks := []any{}
	if heading := title(value); heading != "" {
		blocks = append(blocks, map[string]any{"type": "header", "text": map[string]any{"type": "plain_text", "text": heading}})
	}
	blocks = append(blocks, map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": body(value)}})
	if len(value.Facts) > 0 {
		fields := make([]any, 0, len(value.Facts))
		for _, fact := range value.Facts {
			fields = append(fields, map[string]any{"type": "mrkdwn", "text": "*" + fact.Key + "*\n" + fact.Value})
		}
		blocks = append(blocks, map[string]any{"type": "section", "fields": fields})
	}
	if len(value.Actions) > 0 {
		elements := make([]any, 0, len(value.Actions))
		for index, action := range value.Actions {
			button := map[string]any{"type": "button", "text": map[string]any{"type": "plain_text", "text": action.Label}, "url": action.URL, "action_id": fmt.Sprintf("notification_action_%d", index+1)}
			if action.Style == "primary" || action.Style == "danger" {
				button["style"] = action.Style
			}
			elements = append(elements, button)
		}
		blocks = append(blocks, map[string]any{"type": "actions", "elements": elements})
	}
	return map[string]any{"text": value.Message, "blocks": blocks}
}

func compileFeishu(value Content) (map[string]any, error) {
	elements := []any{map[string]any{"tag": "markdown", "content": body(value)}}
	if len(value.Facts) > 0 {
		fields := make([]any, 0, len(value.Facts))
		for _, fact := range value.Facts {
			fields = append(fields, map[string]any{"is_short": true, "text": map[string]any{"tag": "lark_md", "content": "**" + fact.Key + "**\n" + fact.Value}})
		}
		elements = append(elements, map[string]any{"tag": "div", "fields": fields})
	}
	if len(value.Actions) > 0 {
		actions := make([]any, 0, len(value.Actions))
		for _, action := range value.Actions {
			kind := "default"
			if action.Style == "primary" || action.Style == "danger" {
				kind = action.Style
			}
			actions = append(actions, map[string]any{"tag": "button", "text": map[string]any{"tag": "plain_text", "content": action.Label}, "url": action.URL, "type": kind})
		}
		elements = append(elements, map[string]any{"tag": "action", "actions": actions})
	}
	card := map[string]any{"config": map[string]any{"wide_screen_mode": true}, "header": map[string]any{"template": "blue", "title": map[string]any{"tag": "plain_text", "content": title(value)}}, "elements": elements}
	encoded, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	return map[string]any{"msg_type": "interactive", "content": string(encoded)}, nil
}

func compileDingTalk(value Content) map[string]any {
	heading, text := title(value), body(value)
	for _, fact := range value.Facts {
		text += "\n\n**" + fact.Key + "：** " + fact.Value
	}
	if len(value.Actions) == 0 {
		return map[string]any{"msgtype": "markdown", "markdown": map[string]any{"title": heading, "text": "### " + heading + "\n\n" + text}}
	}
	buttons := make([]any, 0, len(value.Actions))
	for _, action := range value.Actions {
		buttons = append(buttons, map[string]any{"title": action.Label, "actionURL": action.URL})
	}
	return map[string]any{"msgtype": "actionCard", "actionCard": map[string]any{"title": heading, "text": "### " + heading + "\n\n" + text, "btnOrientation": "0", "btns": buttons}}
}

func compileEnterpriseWeChat(value Content) map[string]any {
	if len(value.Actions) == 0 {
		return map[string]any{"msgtype": "markdown", "markdown": map[string]any{"content": "**" + title(value) + "**\n" + body(value)}}
	}
	facts := make([]any, 0, len(value.Facts))
	for _, fact := range value.Facts {
		facts = append(facts, map[string]any{"keyname": fact.Key, "value": fact.Value})
	}
	jumps := make([]any, 0, len(value.Actions))
	for _, action := range value.Actions {
		jumps = append(jumps, map[string]any{"type": 1, "title": action.Label, "url": action.URL})
	}
	return map[string]any{"msgtype": "template_card", "template_card": map[string]any{"card_type": "text_notice", "source": map[string]any{"desc": strings.TrimSpace(value.ProductName)}, "main_title": map[string]any{"title": title(value)}, "sub_title_text": body(value), "horizontal_content_list": facts, "jump_list": jumps, "card_action": map[string]any{"type": 1, "url": value.Actions[0].URL}}}
}

func compileDiscord(value Content) map[string]any {
	fields := make([]any, 0, len(value.Facts))
	for _, fact := range value.Facts {
		fields = append(fields, map[string]any{"name": fact.Key, "value": fact.Value, "inline": true})
	}
	payload := map[string]any{"content": value.Message, "embeds": []any{map[string]any{"title": title(value), "description": body(value), "color": 5793266, "fields": fields, "footer": map[string]any{"text": strings.TrimSpace(value.ProductName)}}}, "allowed_mentions": map[string]any{"parse": []any{}}}
	if len(value.Actions) > 0 {
		buttons := make([]any, 0, len(value.Actions))
		for _, action := range value.Actions {
			buttons = append(buttons, map[string]any{"type": 2, "style": 5, "label": action.Label, "url": action.URL})
		}
		payload["components"] = []any{map[string]any{"type": 1, "components": buttons}}
	}
	return payload
}

func compileGoogleChat(value Content) map[string]any {
	widgets := []any{map[string]any{"textParagraph": map[string]any{"text": html.EscapeString(body(value))}}}
	for _, fact := range value.Facts {
		widgets = append(widgets, map[string]any{"decoratedText": map[string]any{"topLabel": fact.Key, "text": fact.Value}})
	}
	if len(value.Actions) > 0 {
		buttons := make([]any, 0, len(value.Actions))
		for _, action := range value.Actions {
			buttons = append(buttons, map[string]any{"text": action.Label, "onClick": map[string]any{"openLink": map[string]any{"url": action.URL}}})
		}
		widgets = append(widgets, map[string]any{"buttonList": map[string]any{"buttons": buttons}})
	}
	return map[string]any{"text": value.Message, "cardsV2": []any{map[string]any{"cardId": "domainry-notification", "card": map[string]any{"header": map[string]any{"title": title(value)}, "sections": []any{map[string]any{"widgets": widgets}}}}}}
}

func compileTeams(value Content) (map[string]any, error) {
	cardBody := []any{map[string]any{"type": "TextBlock", "text": title(value), "size": "Medium", "weight": "Bolder", "wrap": true}, map[string]any{"type": "TextBlock", "text": body(value), "wrap": true}}
	if len(value.Facts) > 0 {
		facts := make([]any, 0, len(value.Facts))
		for _, fact := range value.Facts {
			facts = append(facts, map[string]any{"title": fact.Key, "value": fact.Value})
		}
		cardBody = append(cardBody, map[string]any{"type": "FactSet", "facts": facts})
	}
	actions := make([]any, 0, len(value.Actions))
	for _, action := range value.Actions {
		actions = append(actions, map[string]any{"type": "Action.OpenUrl", "title": action.Label, "url": action.URL})
	}
	card := map[string]any{"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4", "body": cardBody, "actions": actions}
	encoded, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	const attachmentID = "domainry-notification-card"
	return map[string]any{"body": map[string]any{"contentType": "html", "content": `<attachment id="` + attachmentID + `"></attachment>`}, "attachments": []any{map[string]any{"id": attachmentID, "contentType": "application/vnd.microsoft.card.adaptive", "content": string(encoded)}}}, nil
}

func compileLineWorks(value Content) map[string]any { return compileLineMessage(value, false) }
func compileLine(value Content) map[string]any      { return compileLineMessage(value, true) }

func compileLineMessage(value Content, wrapMessages bool) map[string]any {
	contents := []any{map[string]any{"type": "text", "text": title(value), "weight": "bold", "size": "xl", "wrap": true}, map[string]any{"type": "text", "text": body(value), "wrap": true, "margin": "md"}}
	for _, fact := range value.Facts {
		contents = append(contents, map[string]any{"type": "box", "layout": "baseline", "margin": "sm", "contents": []any{map[string]any{"type": "text", "text": fact.Key, "color": "#888888", "size": "sm", "flex": 2}, map[string]any{"type": "text", "text": fact.Value, "wrap": true, "size": "sm", "flex": 3}}})
	}
	bubble := map[string]any{"type": "bubble", "body": map[string]any{"type": "box", "layout": "vertical", "contents": contents}}
	if len(value.Actions) > 0 {
		buttons := make([]any, 0, len(value.Actions))
		for _, action := range value.Actions {
			style := "secondary"
			if action.Style == "primary" {
				style = "primary"
			}
			buttons = append(buttons, map[string]any{"type": "button", "style": style, "action": map[string]any{"type": "uri", "label": action.Label, "uri": action.URL}})
		}
		bubble["footer"] = map[string]any{"type": "box", "layout": "vertical", "spacing": "sm", "contents": buttons}
	}
	message := map[string]any{"type": "flex", "altText": value.Message, "contents": bubble}
	if wrapMessages {
		return map[string]any{"messages": []any{message}}
	}
	return map[string]any{"content": message}
}
