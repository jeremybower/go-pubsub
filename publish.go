package pubsub

import (
	"context"
	"log/slog"
	"time"

	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

func Publish(
	ctx context.Context,
	querier postgres.Querier,
	value any,
	encoder Encoder,
	topicNames []string,
) (*PublishReceipt, int64, error) {
	dataStore := NewPostgresDataStore()
	return publish(dataStore, ctx, querier, value, encoder, topicNames, nil)
}

func publish(
	dataStore DataStore,
	ctx context.Context,
	querier postgres.Querier,
	value any,
	encoder Encoder,
	topicNames []string,
	publishedAt *time.Time,
) (*PublishReceipt, int64, error) {
	// Check that the context is not nil.
	if ctx == nil {
		panic("pubsub: context is nil")
	}

	// Check that the querier is not nil.
	if querier == nil {
		panic("pubsub: querier is nil")
	}

	// Check that the topics are valid.
	err := validateTopics(topicNames)
	if err != nil {
		return nil, 0, err
	}

	// Check that value and encoder are either both nil or both not nil.
	if (value == nil && encoder != nil) || (value != nil && encoder == nil) {
		panic("pubsub: value and encoder must both be nil or both not nil")
	}

	// Get the logger from the context.
	logger, err := common.Logger(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Encode the value.
	var encodedValue *EncodedValue
	if value != nil && encoder != nil {
		encodedValue, err = encoder.Encode(ctx, value)
		if err != nil {
			return nil, 0, err
		}
	}

	// Insert the message into the data store.
	receipt, deletedMessageCount, err := dataStore.Publish(ctx, querier, topicNames, value, encodedValue, publishedAt)
	if err != nil {
		return nil, 0, err
	}

	// Log that the message was published.
	logger.Debug("pubsub: published message",
		slog.Int64("pubsub:message_id", int64(receipt.MessageID)),
		slog.Any("pubsub:topics", topicNames),
	)

	// Return the receipt.
	return receipt, deletedMessageCount, nil
}
