package google

import connector "github.com/domainry/domainry-connector-sdk"

func providerResult[T any](raw connector.TypedResult[Response], value T, err error) (connector.TypedResult[T], error) {
	return connector.TypedResult[T]{Output: value, ResponseRef: raw.ResponseRef, SecretUpdates: raw.SecretUpdates, ResourceHealth: raw.ResourceHealth}, err
}
