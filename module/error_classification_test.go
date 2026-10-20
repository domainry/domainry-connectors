package module

import (
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/apperror"
)

func TestConnectorProviderErrorClassification(t *testing.T) {
	tests := []struct {
		classification connector.ErrorClassification
		kind           apperror.ErrorKind
		code           string
	}{
		{connector.ErrorRetryable, apperror.KindUnavailable, "connector.provider_unavailable"},
		{connector.ErrorUncertain, apperror.KindUnavailable, "connector.delivery_uncertain"},
		{connector.ErrorPermanent, apperror.KindInternal, "connector.request_rejected"},
	}
	for _, test := range tests {
		err := connector.NewProviderError(test.classification, test.code, errors.New("private provider detail"))
		if apperror.KindOf(err) != test.kind || apperror.CodeOf(err) != test.code {
			t.Fatalf("classification=%q kind=%q code=%q", test.classification, apperror.KindOf(err), apperror.CodeOf(err))
		}
	}
}
