package pubsub

import (
	"context"

	"github.com/jeremybower/go-common/optional"
	"github.com/jeremybower/go-common/postgres"
)

type Configuration struct {
	MissedMessageSeconds int32
}

type ConfigurationPatch struct {
	MissedMessageSeconds optional.Value[int32]
}

func PatchConfiguration(
	ctx context.Context,
	querier postgres.Querier,
	patch ConfigurationPatch,
) (*Configuration, error) {
	return patchConfiguration(NewPostgresDataStore(), ctx, querier, patch)
}

func patchConfiguration(
	dataStore DataStore,
	ctx context.Context,
	querier postgres.Querier,
	patch ConfigurationPatch,
) (*Configuration, error) {
	return dataStore.PatchConfiguration(ctx, querier, patch)
}

func ReadConfiguration(
	ctx context.Context,
	querier postgres.Querier,
) (*Configuration, error) {
	return readConfiguration(NewPostgresDataStore(), ctx, querier)
}

func readConfiguration(
	dataStore DataStore,
	ctx context.Context,
	querier postgres.Querier,
) (*Configuration, error) {
	return dataStore.ReadConfiguration(ctx, querier)
}
