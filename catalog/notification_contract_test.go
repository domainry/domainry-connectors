package catalog

import (
	"testing"

	apns "github.com/domainry/domainry-connectors/providers/notification/apns"
	fcm "github.com/domainry/domainry-connectors/providers/notification/fcm"
)

func TestNativePushProvidersUseNotificationContract(t *testing.T) {
	documents, err := DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	var operation *ConnectorOperationSchema
	for _, document := range documents {
		if document.Key != "notification" {
			continue
		}
		for index := range document.Operations {
			if document.Operations[index].Key == "send_native" {
				operation = &document.Operations[index]
				break
			}
		}
	}
	if operation == nil {
		t.Fatal("notification send_native operation is missing")
	}
	want, err := OperationContractSHA256("notification", "", *operation)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]string{"apns": apns.SendNative.ContractSHA256, "fcm": fcm.SendNative.ContractSHA256} {
		if got != want {
			t.Errorf("%s send_native contract SHA-256 = %s, want %s", name, got, want)
		}
	}
}
