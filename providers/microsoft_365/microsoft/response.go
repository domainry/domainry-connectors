package microsoft

import connector "github.com/domainry/domainry-connector-sdk"

func providerResult[T any](raw connector.TypedResult[Response], out T, err error) (connector.TypedResult[T], error) {
	return connector.TypedResult[T]{Output: out, ResponseRef: raw.ResponseRef, SecretUpdates: raw.SecretUpdates, ResourceHealth: raw.ResourceHealth}, err
}
