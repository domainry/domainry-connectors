package mailcontent_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

func TestMailHTMLNeverIncludesActiveContentOrRemoteAttributes(t *testing.T) {
	if _, err := mailcontent.HTMLText(`<p>visible</p><head>unclosed`); err == nil {
		t.Fatal("malformed skipped region declared complete")
	}
	s, err := mailcontent.HTMLText(`<html><head><title>PRIVATE TITLE</title><style>CSS SECRET</style></head><body><p>Hello &amp; 你好</p><script>EVIL()</script><iframe>HIDDEN</iframe><svg/><p>Visible</p><img src="https://tracker.test/private"/><a href="javascript:bad()">Actual text</a><table><tr><td>A</td><td>B</td></tr></table></body></html>`)
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{"Hello & 你好", "Visible", "Actual text", "A\tB"} {
		if !strings.Contains(s, wanted) {
			t.Fatal(s, wanted)
		}
	}
	for _, hidden := range []string{"PRIVATE", "CSS", "EVIL", "HIDDEN", "tracker", "javascript", "src="} {
		if strings.Contains(s, hidden) {
			t.Fatal(s, hidden)
		}
	}
}
func TestMailTextEncodingAndRecipientBoundsAreExplicit(t *testing.T) {
	text, err := mailcontent.DecodeText([]byte{0x63, 0x61, 0x66, 0xe9}, "iso-8859-1")
	if err != nil || text != "café" {
		t.Fatal(text, err)
	}
	if _, err = mailcontent.DecodeText([]byte{0xff}, "utf-8"); err == nil {
		t.Fatal("invalid UTF8 accepted")
	}
	if _, err = mailcontent.DecodeText([]byte("x"), "unknown-charset"); err == nil {
		t.Fatal("unknown charset guessed")
	}
	if _, err = mailcontent.DecodeText([]byte("é"), "us-ascii"); err == nil {
		t.Fatal("non-ASCII accepted")
	}
	text, cut := mailcontent.Header("=?ISO-8859-1?Q?caf=E9?=", 2048)
	if cut || text != "café" {
		t.Fatal(text, cut)
	}
	text, cut = mailcontent.Text(strings.Repeat("你", 400), 1024, true)
	if !cut || len(text) != 1023 || !utf8.ValidString(text) {
		t.Fatal(len(text), cut)
	}
	addresses, omitted := mailcontent.Addresses([]string{`=?UTF-8?B?5byg5LiJ?= <person@example.test>`})
	if omitted || len(addresses) != 1 || addresses[0].Name != "张三" {
		t.Fatal(addresses, omitted)
	}
	addresses, omitted = mailcontent.Addresses([]string{strings.Repeat("a@example.test,", 50) + "b@example.test", "invalid <"})
	if !omitted || len(addresses) != 50 {
		t.Fatal(len(addresses), omitted)
	}
}
