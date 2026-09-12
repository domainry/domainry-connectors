package llmproxy

import (
	"context"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/web"
)

func (p *provider) search(ctx context.Context, request connector.TypedRequest[web.SearchRequest]) (connector.TypedResult[web.SearchResult], error) {
	var result connector.TypedResult[web.SearchResult]
	if request.Input.Validate() != nil {
		return result, permanent("request_invalid", "invalid public web search request")
	}
	c, err := settings(request.Connection)
	if err != nil {
		return result, err
	}
	body, err := p.post(ctx, c, searchPath, request.Secrets, struct {
		Objective         string `json:"objective"`
		Processor         string `json:"processor"`
		MaxResults        int    `json:"max_results"`
		MaxCharsPerResult int    `json:"max_chars_per_result"`
		SourcePolicy      struct {
			Sources []string `json:"sources"`
		} `json:"source_policy"`
	}{request.Input.Query, c.processor, request.Input.ResultLimit(), request.Input.ExcerptLimit(), struct {
		Sources []string `json:"sources"`
	}{c.hosts}})
	if err != nil {
		return result, err
	}
	var response struct {
		SearchID string `json:"search_id"`
		Results  []struct {
			URL      string   `json:"url"`
			Title    string   `json:"title"`
			Excerpts []string `json:"excerpts"`
		} `json:"results"`
	}
	if json.Unmarshal(body, &response) != nil || response.Results == nil {
		return result, invalidResponse()
	}
	out := web.SearchResult{SearchID: response.SearchID, Items: []web.SearchItem{}, Scope: "ranked_results"}
	seen := map[string]bool{}
	for _, raw := range response.Results {
		source, allowed := c.allowedSource(raw.URL)
		if !allowed || seen[source] || len(out.Items) >= request.Input.ResultLimit() {
			out.Truncated = true
			continue
		}
		seen[source] = true
		item := web.SearchItem{URL: source, Excerpts: []string{}}
		item.Title, item.Truncated = boundedText(raw.Title, 1024, false)
		remaining := request.Input.ExcerptLimit()
		for _, excerpt := range raw.Excerpts {
			if remaining <= 0 || len(item.Excerpts) >= 16 {
				item.Truncated = true
				break
			}
			text, truncated := boundedText(excerpt, remaining, true)
			item.Excerpts = append(item.Excerpts, text)
			remaining -= len(text)
			item.Truncated = item.Truncated || truncated
		}
		out.Items = append(out.Items, item)
		out.Truncated = out.Truncated || item.Truncated
	}
	if out.Validate(request.Input) != nil {
		return result, invalidResponse()
	}
	result.Output, result.ResponseRef = out, responseRef()
	return result, nil
}

func (p *provider) fetch(ctx context.Context, request connector.TypedRequest[web.FetchRequest]) (connector.TypedResult[web.Page], error) {
	var result connector.TypedResult[web.Page]
	if request.Input.Validate() != nil {
		return result, permanent("request_invalid", "invalid public web fetch request")
	}
	c, err := settings(request.Connection)
	if err != nil {
		return result, err
	}
	requested, allowed := c.allowedSource(request.Input.URL)
	if !allowed {
		return result, permanent("source_denied", "requested page is outside the configured source policy")
	}
	body, err := p.post(ctx, c, fetchPath, request.Secrets, struct {
		URL string `json:"url"`
	}{requested})
	if err != nil {
		return result, err
	}
	var response struct {
		Code *int `json:"code"`
		Data *struct {
			URL         string  `json:"url"`
			Title       string  `json:"title"`
			Description string  `json:"description"`
			Content     *string `json:"content"`
			Warning     string  `json:"warning"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &response) != nil || response.Code == nil || *response.Code != 0 || response.Data == nil || response.Data.Content == nil {
		return result, invalidResponse()
	}
	source, allowed := c.allowedSource(response.Data.URL)
	if !allowed {
		return result, permanent("source_denied", "returned page is outside the configured source policy")
	}
	out := web.Page{RequestedURL: requested, URL: source, SourceCompleteness: "unknown", Warnings: []string{}}
	var shortened bool
	out.Content, out.Truncated = boundedText(*response.Data.Content, request.Input.ContentLimit(), true)
	out.Title, shortened = boundedText(response.Data.Title, 1024, false)
	out.Truncated = out.Truncated || shortened
	out.Description, shortened = boundedText(response.Data.Description, 4096, false)
	out.Truncated = out.Truncated || shortened
	if response.Data.Warning != "" {
		warning, changed := boundedText(response.Data.Warning, 512, false)
		if strings.TrimSpace(warning) != "" {
			out.Warnings = append(out.Warnings, warning)
		}
		out.Truncated = out.Truncated || changed
	}
	if out.Validate(request.Input) != nil {
		return result, invalidResponse()
	}
	result.Output, result.ResponseRef = out, responseRef()
	return result, nil
}

// Output remains untrusted text. Normalize line endings and remove controls;
// byte bounds never split UTF-8. Any removal/shortening is explicitly signaled.
func boundedText(value string, limit int, multiline bool) (string, bool) {
	original := value
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && !(multiline && (r == '\n' || r == '\t')) {
			return -1
		}
		return r
	}, value)
	if len(value) > limit {
		end := limit
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		value = value[:end]
	}
	return value, value != original
}
