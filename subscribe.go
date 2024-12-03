package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/backoff"
)

func Subscribe(
	ctx context.Context,
	dbPool *pgxpool.Pool,
	topics []string,
	decoders ...Decoder,
) (*Subscription, error) {
	dataStore := NewPostgresDataStore()
	return subscribe(dataStore, ctx, dbPool, topics, decoders...)
}

func subscribe(
	dataStore DataStore,
	ctx context.Context,
	dbPool *pgxpool.Pool,
	topics []string,
	decoders ...Decoder,
) (*Subscription, error) {
	// Check that the context is not nil.
	if ctx == nil {
		panic("pubsub: context is nil")
	}

	// Check that the database pool is not nil.
	if dbPool == nil {
		panic("pubsub: database pool is nil")
	}

	// Check that the topics are valid.
	err := validateTopics(topics)
	if err != nil {
		return nil, err
	}

	// Create a context that is cancelled when the subscription is closed.
	ctx, cancel := context.WithCancel(ctx)

	// Create the subscription.
	sub := newSubscription(dbPool, cancel, dataStore, decoders...)

	// Get the context's logger.
	logger, err := common.Logger(ctx)
	if err != nil {
		return nil, err
	}

	// Customize the logger for this subscription.
	logger = logger.With(
		slog.String("pubsub:subscription_id", string(sub.id)),
	)

	// Continue the subscription in a goroutine.
	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()

		// Use an exponential backoff before attempting to resubscribe.
		exponentialBackoff := backoff.New(0, 60, 120, true)
		for {
			// Handle the subscription.
			logger.Debug("pubsub: subscribing to topics")
			sub.subscribe(
				ctx,
				logger,
				topics,
				sub.handleSubscribed,
				sub.handleMessage,
				sub.handleError,
			)

			// Check if closed.
			if sub.closed {
				logger.Debug("pubsub: subscription closed")
				return
			}

			// Check if cancelled.
			if ctx.Err() != nil {
				logger.Debug("pubsub: subscription cancelled")
				return
			}

			// Calculate the delay before the next subscription attempt.
			delay := exponentialBackoff.Attempt(time.Now())
			logger.Debug("pubsub: subscription will subscribe after delay",
				slog.Duration("delay", delay),
			)

			// Notify the client that the subscription is waiting to resubscribe.
			sub.handleDelay(ctx, logger, delay)

			// The subscription is waiting to resubscribe, but can be cancelled
			// or stopped while waiting.
			select {
			case <-ctx.Done():
				// The subscription was closed or cancelled.
				// Return immediately.
				if sub.closed {
					logger.Debug("pubsub: subscription stopped")
				} else {
					logger.Debug("pubsub: subscription cancelled")
				}
				return
			case <-time.After(delay):
				// The subscription failed.
				// Continue to the next iteration of the loop after the delay.
			}
		}
	}()

	return sub, nil
}

func validateTopics(topics []string) error {
	if len(topics) == 0 {
		return fmt.Errorf("%w (no topics)", ErrTopicValidation)
	}

	for _, topic := range topics {
		if topic == "" {
			return fmt.Errorf("%w (topic is empty): %q", ErrTopicValidation, topic)
		}

		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("%w (topic is only whitespace): %q", ErrTopicValidation, topic)
		}
	}

	return nil
}
