package pubsub

import (
	"context"
	"log/slog"

	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

func (client *Client) Publish(
	ctx context.Context,
	querier postgres.Querier,
	topicNames []string,
	value any,
	encoder Encoder,
) (*PublishReceipt, error) {
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
		return nil, err
	}

	// Check that value and encoder are either both nil or both not nil.
	if (value == nil && encoder != nil) || (value != nil && encoder == nil) {
		panic("pubsub: value and encoder must both be nil or both not nil")
	}

	// Get the logger from the context.
	logger, err := common.Logger(ctx)
	if err != nil {
		return nil, err
	}

	// Encode the value.
	var encodedValue *EncodedValue
	if value != nil && encoder != nil {
		encodedValue, err = encoder.Encode(ctx, value)
		if err != nil {
			return nil, err
		}
	}

	// Insert the message into the data store.
	receipt, err := client.dataStore.Publish(ctx, querier, topicNames, value, encodedValue)
	if err != nil {
		return nil, err
	}

	// Log that the message was published.
	logger.Debug("pubsub: published message",
		slog.Int64("pubsub:message_id", int64(receipt.MessageID)),
		slog.Any("pubsub:topics", topicNames),
	)

	// Return the receipt.
	return receipt, nil
}
