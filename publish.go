package pubsub

import (
	"context"
	"log/slog"

	"github.com/jeremybower/go-common/postgres"
)

func Publish[T any](
	ctx context.Context,
	logger *slog.Logger,
	querier postgres.Querier,
	topic string,
	message T,
) (int64, error) {
	return publish(ctx, logger, querier, topic, message, &standardDependencies{})
}

func publish[T any](
	ctx context.Context,
	logger *slog.Logger,
	querier postgres.Querier,
	topic string,
	message T,
	deps dependencies,
) (int64, error) {
	payload, err := deps.MarshalStringJSON(message)
	if err != nil {
		logger.Error("pubsub: failed to marshal message", slog.Any("error", err))
		return 0, err
	}

	var id int64
	sql := "INSERT INTO pubsub_messages (topic, payload) VALUES ($1, $2) RETURNING id;"
	row := deps.QueryRow(ctx, querier, sql, topic, payload)
	if err := row.Scan(&id); err != nil {
		logger.Error("pubsub: failed to publish message", slog.Any("error", err))
		return 0, err
	}

	logger.Info("pubsub: published message", slog.String("topic", topic), slog.Int64("message_id", id))
	return id, nil
}
