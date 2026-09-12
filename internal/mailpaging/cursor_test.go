package mailpaging_test

import (
	"strings"
	"testing"

	"github.com/domainry/domainry-connectors/internal/mailpaging"
)

func TestMailContinuationBindsExactScopeAndHasNoDestination(t *testing.T) {
	scope := mailpaging.Scope("workspace", "account", "https://mail.test", "subject:report")
	raw, err := mailpaging.Encode(scope, "opaque/+==", 10)
	if err != nil {
		t.Fatal(err)
	}
	c, err := mailpaging.Decode(scope, raw)
	if err != nil || c.Token != "opaque/+==" || c.Seen != 10 {
		t.Fatal(c, err)
	}
	for _, other := range []string{mailpaging.Scope("workspace", "other", "https://mail.test", "subject:report"), mailpaging.Scope("workspace", "account", "https://other.test", "subject:report"), mailpaging.Scope("workspace", "account", "https://mail.test", "different")} {
		if _, err = mailpaging.Decode(other, raw); err == nil {
			t.Fatal("scope changed")
		}
	}
	for _, token := range []string{"", strings.Repeat("x", 4097), "a\n"} {
		if _, err = mailpaging.Encode(scope, token, 0); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
}
