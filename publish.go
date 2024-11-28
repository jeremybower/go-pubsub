package pubsub

import (
	"context"
	"log/slog"

	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

type MessageId int64

func Publish[T any](
	ctx context.Context,
	querier postgres.Querier,
	topic string,
	message T,
) (MessageId, error) {
	return publish(ctx, querier, topic, message, &standardDependencies{})
}

func publish[T any](
	ctx context.Context,
	querier postgres.Querier,
	topic string,
	message T,
	deps dependencies,
) (MessageId, error) {
	// Get the logger from the context.
	logger, err := common.Logger(ctx)
	if err != nil {
		return 0, err
	}

	payload, err := deps.MarshalStringJSON(message)
	if err != nil {
		logger.Error("pubsub: failed to marshal message", slog.Any("error", err))
		return 0, err
	}

	var id MessageId
	sql := "INSERT INTO pubsub_messages (topic, payload) VALUES ($1, $2) RETURNING id;"
	row := deps.QueryRow(ctx, querier, sql, topic, payload)
	if err := row.Scan(&id); err != nil {
		logger.Error("pubsub: failed to publish message", slog.Any("error", err))
		return 0, err
	}

	logger.Debug("pubsub: published message",
		slog.String("pubsub:topic", topic),
		slog.Int64("pubsub:message_id", int64(id)),
	)
	return id, nil
}
