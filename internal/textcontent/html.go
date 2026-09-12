// Package textcontent performs pure text conversion without account or provider contracts.
package textcontent

import (
	"errors"
	"golang.org/x/net/html"
	"io"
	"strings"
)

// HTMLText extracts textual nodes with a tokenizer. Attributes, active content,
// styling, embedded objects and remote images are not included or evaluated.
func HTMLText(source string) (string, error) {
	z := html.NewTokenizer(strings.NewReader(source))
	z.SetMaxBuf(4 << 20)
	var out strings.Builder
	var ignored string
	depth := 0
	for {
		kind := z.Next()
		switch kind {
		case html.ErrorToken:
			if z.Err() != io.EOF || ignored != "" {
				return "", errors.New("invalid HTML")
			}
			return out.String(), nil
		case html.StartTagToken, html.SelfClosingTagToken:
			name, _ := z.TagName()
			tag := string(name)
			if ignored != "" {
				if tag == ignored && kind != html.SelfClosingTagToken {
					depth++
				}
				continue
			}
			switch tag {
			case "head", "script", "style", "template", "svg", "object", "iframe":
				if kind != html.SelfClosingTagToken {
					ignored = tag
					depth = 1
				}
			case "br", "p", "div", "li", "tr", "table", "blockquote", "h1", "h2", "h3", "hr":
				out.WriteByte('\n')
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			tag := string(name)
			if ignored != "" {
				if tag == ignored {
					depth--
					if depth == 0 {
						ignored = ""
					}
				}
				continue
			}
			switch tag {
			case "p", "div", "li", "tr", "table", "blockquote", "h1", "h2", "h3":
				out.WriteByte('\n')
			case "td", "th":
				out.WriteByte('\t')
			}
		case html.TextToken:
			if ignored == "" {
				out.Write(z.Text())
			}
		}
	}
}
