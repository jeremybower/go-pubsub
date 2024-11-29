package pubsub

import (
	"context"
	"log/slog"

	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

type MessageId int64

type publishOptions struct {
	typeFn     func() string
	marshaller Marshaller
}

type publishOption func(opts *publishOptions)

func WithMarshaller(typ string, marshaller Marshaller) publishOption {
	return func(opts *publishOptions) {
		opts.marshaller = marshaller
		opts.typeFn = func() string {
			return typ
		}
	}
}

func Publish[T any](
	ctx context.Context,
	querier postgres.Querier,
	topic string,
	message T,
	options ...publishOption,
) (MessageId, error) {
	return publish(ctx, querier, topic, message, &standardDependencies{}, options...)
}

func publish[T any](
	ctx context.Context,
	querier postgres.Querier,
	topic string,
	message T,
	deps dependencies,
	options ...publishOption,
) (MessageId, error) {
	// Get the logger from the context.
	logger, err := common.Logger(ctx)
	if err != nil {
		return 0, err
	}

	// Handle the options.
	opts := &publishOptions{
		typeFn:     fullyQualifiedNameFromType[T],
		marshaller: defaultMarshaller[T],
	}
	for _, option := range options {
		option(opts)
	}

	// Marshal the message.
	typ := opts.typeFn()
	payload, err := opts.marshaller(message)
	if err != nil {
		logger.Error("pubsub: failed to marshal message", slog.Any("error", err))
		return 0, err
	}

	// Insert the message into the database.
	var id MessageId
	sql := "INSERT INTO pubsub_messages (topic, type, payload) VALUES ($1, $2, $3) RETURNING id;"
	row := deps.QueryRow(ctx, querier, sql, topic, typ, payload)
	if err := row.Scan(&id); err != nil {
		logger.Error("pubsub: failed to publish message", slog.Any("error", err))
		return 0, err
	}

	// Log that the message was published.
	logger.Debug("pubsub: published message",
		slog.String("pubsub:topic", topic),
		slog.Int64("pubsub:message_id", int64(id)),
	)

	// Return the message id.
	return id, nil
}
