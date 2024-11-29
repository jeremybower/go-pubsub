package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/backoff"
)

type SubscriptionId string

type MessageEvent struct {
	Topic          string
	SubscriptionId SubscriptionId
	MessageId      MessageId
	MessageType    string
	Message        any
	PublishedAt    time.Time
}

type SubscriptionEvent struct {
	Topic          string
	SubscriptionId SubscriptionId
	Subscribed     bool
}

type Subscription struct {
	Id            SubscriptionId
	Events        chan any
	ctx           context.Context
	dbPool        *pgxpool.Pool
	logger        *slog.Logger
	maxMessageID  MessageId
	unmarshallers map[string]Unmarshaller
	errorHandler  func(error)
}

func newSubscription(
	ctx context.Context,
	dbPool *pgxpool.Pool,
	logger *slog.Logger,
	id SubscriptionId,
	bufferSize int,
	unmarshallers map[string]Unmarshaller,
) *Subscription {
	return &Subscription{
		Id:            id,
		Events:        make(chan any, bufferSize),
		ctx:           ctx,
		dbPool:        dbPool,
		logger:        logger,
		unmarshallers: unmarshallers,
		errorHandler:  func(error) {},
	}
}

type SubscribeOptions struct {
	bufferSize    int
	unmarshallers map[string]Unmarshaller
}

func defaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		unmarshallers: make(map[string]Unmarshaller),
	}
}

type SubscriptionOption func(opts *SubscribeOptions)

func WithBufferSize(bufferSize int) SubscriptionOption {
	return func(options *SubscribeOptions) {
		options.bufferSize = bufferSize
	}
}

func WithMessage[T any]() SubscriptionOption {
	// Check if the type T is a string.
	var t T
	switch any(t).(type) {
	case string:
		return WithUnmarshaller(fullyQualifiedNameFromType[string](), UnmarshalString)
	default:
		return WithUnmarshaller(fullyQualifiedNameFromType[T](), UnmarshalJSON[T])
	}
}

func WithUnmarshaller(typ string, unmarshaller Unmarshaller) SubscriptionOption {
	return func(options *SubscribeOptions) {
		options.unmarshallers[typ] = unmarshaller
	}
}

func Subscribe(
	ctx context.Context,
	dbPool *pgxpool.Pool,
	topics []string,
	opts ...SubscriptionOption,
) (*Subscription, error) {
	if ctx == nil {
		panic("pubsub: context is nil")
	}

	if dbPool == nil {
		panic("pubsub: database pool is nil")
	}

	options := defaultSubscribeOptions()
	for _, opt := range opts {
		opt(options)
	}

	topics, err := validateTopics(topics)
	if err != nil {
		return nil, err
	}

	// Generate a unique identifier for this subscription.
	subscriptionId := SubscriptionId(uuid.NewString())

	// Get the context's logger.
	logger, err := common.Logger(ctx)
	if err != nil {
		return nil, err
	}

	// Customize the logger for this subscription.
	logger = logger.With(
		slog.String("pubsub:subscription_id", string(subscriptionId)),
	)

	// Create the subscription.
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, options.bufferSize, options.unmarshallers)

	// Start the subscription in a goroutine that can recover from panics.
	go func() {
		// Since defers are executed in LIFO order, handle panics before closing the channel.
		defer close(sub.Events)
		defer handlePanic(sub)

		// Use an exponential backoff to wait an increasing amount of time before trying to
		// reconnect. This is useful for handling transient errors like network outages or
		// database restarts.
		exponentialBackoff := backoff.New(0, 60, 120, true)
		for {
			// Block while subscribed to the topic.
			logger.Debug("pubsub: subscribing to topics")
			subscribe(sub, topics, &standardDependencies{})

			// If the listener function returns, then either the context was cancelled or the
			// the connection was lost.
			select {
			case <-ctx.Done():
				return
			case <-exponentialBackoff.Wait():
			}
		}
	}()

	return sub, nil
}

func subscribe(
	sub *Subscription,
	topics []string,
	deps dependencies,
) {
	// Acquire a connection to the database from the pool.
	sub.logger.Debug("pubsub: acquiring database connection")
	conn, err := deps.Acquire(sub.ctx, sub.dbPool)
	if err != nil {
		sub.logger.Error("pubsub: failed to acquire database connection", slog.Any("error", err))
		sub.errorHandler(err)
		return
	}
	defer conn.Release()

	// Subscribe to the topic and read the current max message ID.
	// If there are no messages in the database (for any topic), then the max
	// message ID will be 0. Otherwise, it will be the highest message ID for any
	// topic.
	//
	// It is important to subscribe to the topic before checking for missed
	// messages because the subscription will block until a notification is
	// received. If the subscription is started after checking for missed
	// messages, then it is possible to miss messages that are published between
	// the time of checking and the time of subscribing.
	//
	// If the subscription is started for the first time, then the max message ID
	// will be 0 and there are no missed messages.
	//
	// If the subscription is restarted after a connection error, then the max
	// message ID will be the last message ID that was processed before the
	// connection was lost.
	var newMaxMessageID MessageId
	row := deps.QueryRow(sub.ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", topics)
	if err := row.Scan(&newMaxMessageID); err != nil {
		sub.logger.Error("pubsub: failed to subscribe to topic", slog.Any("error", err))
		sub.errorHandler(err)
		return
	}

	// Log that the subscription is subscribed to the topic.
	sub.logger.Debug("pubsub: subscribed to topic")

	// Notify the caller that the subscription is subscribed to the topic.
	// Schedule a deferred callback to notify the caller when the subscription
	// is cancelled
	sub.Events <- SubscriptionEvent{
		SubscriptionId: sub.Id,
		Subscribed:     true,
	}
	defer func() {
		sub.logger.Debug("pubsub: unsubscribing from topic")
		sub.Events <- SubscriptionEvent{
			SubscriptionId: sub.Id,
			Subscribed:     false,
		}
	}()

	// If the max message ID has changed, then there might be messages
	// for this topic. Check for missed messages and process them.
	// If this subscription is starting for the first time, then
	// the max message ID will be 0 and there are no missed messages.
	if sub.maxMessageID != 0 && newMaxMessageID > sub.maxMessageID {
		sub.logger.Debug("pubsub: checking for missed messages")
		sql := `SELECT id, topic, "type", payload, published_at FROM pubsub_messages WHERE topic = ANY($1) AND id > $2 ORDER BY id ASC;`
		rows, err := deps.Query(sub.ctx, conn, sql, topics, sub.maxMessageID)
		if err != nil {
			sub.logger.Error("pubsub: failed to check for missed messages", slog.Any("error", err))
			sub.errorHandler(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var messageId MessageId
			var topic string
			var messageType string
			var payload string
			var publishedAt time.Time
			if err := rows.Scan(&messageId, &topic, &messageType, &payload, &publishedAt); err != nil {
				sub.logger.Error("pubsub: failed to read missed message", slog.Any("error", err))
				sub.errorHandler(err)
				return
			}

			handlePayload(sub, topic, messageId, messageType, payload, publishedAt)
			sub.maxMessageID = messageId
		}
	}

	// Block and process messages until the context is cancelled or a
	// connection error occurs.
	for {
		// Block until a notification is received. This is cancellable
		// by the context passed to the subscribe function.
		sub.logger.Debug("pubsub: waiting for message")
		notification, err := deps.WaitForNotification(sub.ctx, conn)
		if err != nil {
			sub.logger.Error("pubsub: failed while waiting for message", slog.Any("error", err))
			sub.errorHandler(err)
			return
		}

		// Unmarshal the envelope.
		var envelope struct {
			MessageId   MessageId `json:"message_id"`
			Type        string    `json:"type"`
			Payload     string    `json:"payload"`
			PublishedAt time.Time `json:"published_at"`
		}
		if err := deps.UnmarshalStringJSON(notification.Payload, &envelope); err != nil {
			// Report errors, but continue handling messages.
			sub.logger.Error("pubsub: failed to unmarshal envelope", slog.Any("error", err))
			sub.errorHandler(err)
			continue
		}

		// Handle the message and track the max message ID.
		// While it shouldn't be possible to receive a message ID less than the max
		// message ID because of the Postgres function used to lock the table,
		// select the max message ID, and start listening within a transaction, it
		// seems more robust to check.
		if envelope.MessageId > sub.maxMessageID {
			// Handle the message payload.
			handlePayload(
				sub,
				notification.Channel,
				envelope.MessageId,
				envelope.Type,
				envelope.Payload,
				envelope.PublishedAt,
			)

			// Update the max message ID.
			sub.maxMessageID = envelope.MessageId
		} else {
			sub.logger.Warn("pubsub: ignoring out-of-order message",
				slog.Int64("pubsub:message_id", int64(envelope.MessageId)),
				slog.Int64("pubsub:max_message_id", int64(sub.maxMessageID)),
			)
		}
	}
}

func handlePanic(sub *Subscription) {
	if r := recover(); r != nil {
		var err error
		switch r := r.(type) {
		case error:
			err = r
		case string:
			err = errors.New(r)
		default:
			msg := fmt.Sprintf("panic with unexpected value: %v", r)
			err = errors.New(msg)
		}

		sub.logger.Error("pubsub: subscription panicked", slog.Any("error", err))
		sub.errorHandler(err)
	}
}

func handlePayload(
	sub *Subscription,
	topic string,
	messageId MessageId,
	messageType string,
	payload string,
	publishedAt time.Time,
) {
	logger := sub.logger.With(
		slog.Int64("pubsub:message_id", int64(messageId)),
		slog.String("pubsub:message_type", messageType),
	)

	logger.Debug("pubsub: received message",
		slog.Duration("pubsub:latency", time.Since(publishedAt)),
	)

	unmarshaller, ok := sub.unmarshallers[messageType]
	if !ok {
		logger.Error("pubsub: no unmarshaller for type", slog.String("type", messageType))
		sub.errorHandler(fmt.Errorf("%w: %q", ErrNoUnmarshaller, messageType))
		return
	}

	message, err := unmarshaller(payload)
	if err != nil {
		logger.Error("pubsub: failed to unmarshal message", slog.String("error", err.Error()))
		sub.errorHandler(err)
		return
	}

	sub.Events <- MessageEvent{
		Topic:          topic,
		SubscriptionId: sub.Id,
		MessageId:      messageId,
		MessageType:    messageType,
		Message:        message,
		PublishedAt:    publishedAt,
	}
}

func validateTopics(topics []string) ([]string, error) {
	validTopics := make([]string, 0, len(topics))
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			continue
		}

		if len(topic) >= 128 {
			return nil, fmt.Errorf("%w (length >= 128): %q", ErrInvalidTopic, topic)
		}

		validTopics = append(validTopics, topic)
	}

	return validTopics, nil
}
