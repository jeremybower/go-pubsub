package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/backoff"
)

type SubscriptionId string

type MessageEvent[T any] struct {
	Topic          string
	SubscriptionId SubscriptionId
	MessageId      MessageId
	Message        T
	PublishedAt    time.Time
}

type ErrorEvent struct {
	Topic          string
	SubscriptionId SubscriptionId
	Error          error
}

type SubscriptionEvent struct {
	Topic          string
	SubscriptionId SubscriptionId
	Subscribed     bool
}

type Subscription struct {
	Id           SubscriptionId
	Topic        string
	Events       chan any
	ctx          context.Context
	logger       *slog.Logger
	maxMessageID MessageId
}

func newSubscription(
	ctx context.Context,
	logger *slog.Logger,
	id SubscriptionId,
	topic string,
	bufferSize int,
) *Subscription {
	return &Subscription{
		Id:     id,
		Topic:  topic,
		Events: make(chan any, bufferSize),
		ctx:    ctx,
		logger: logger,
	}
}

func (client *Client[T]) Subscribe(
	ctx context.Context,
	topic string,
	bufferSize int,
) (*Subscription, error) {
	if ctx == nil {
		panic("pubsub: context is nil")
	}

	if strings.TrimSpace(topic) == "" {
		panic("pubsub: topic is empty")
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
		slog.String("pubsub:topic", topic),
		slog.String("pubsub:subscription_id", string(subscriptionId)),
	)

	// Create the subscription.
	sub := newSubscription(ctx, logger, subscriptionId, topic, bufferSize)

	// Start the subscription in a goroutine that can recover from panics.
	go func() {
		// Since defers are executed in LIFO order, handle panics before closing the channel.
		defer close(sub.Events)
		defer client.handlePanic(sub)

		// Use an exponential backoff to wait an increasing amount of time before trying to
		// reconnect. This is useful for handling transient errors like network outages or
		// database restarts.
		exponentialBackoff := backoff.New(0, 60, 120, true)
		for {
			// Block while subscribed to the topic.
			logger.Debug("pubsub: subscribing to topic")
			client.subscribe(sub, &standardDependencies{})

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

func (client *Client[T]) subscribe(
	sub *Subscription,
	deps dependencies,
) {
	// Acquire a connection to the database from the pool.
	sub.logger.Debug("pubsub: acquiring database connection")
	conn, err := deps.Acquire(sub.ctx, client.dbPool)
	if err != nil {
		err = fmt.Errorf("pubsub: failed to acquire database connection: %w", err)
		sub.Events <- ErrorEvent{
			Topic:          sub.Topic,
			SubscriptionId: sub.Id,
			Error:          err,
		}
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
	row := deps.QueryRow(sub.ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", sub.Topic)
	if err := row.Scan(&newMaxMessageID); err != nil {
		err = fmt.Errorf("pubsub: failed to subscribe to topic: %w", err)
		sub.Events <- ErrorEvent{
			Topic:          sub.Topic,
			SubscriptionId: sub.Id,
			Error:          err,
		}
		return
	}

	// Log that the subscription is listening to the topic.
	// Schedule a deferred callback to log when the subscription
	// has stopped listening.
	sub.logger.Debug("pubsub: subscribed to topic")

	// Notify the caller that the subscription is listening to the topic.
	// Schedule a deferred callback to notify the caller when the subscription
	// has stopped listening.
	sub.Events <- SubscriptionEvent{
		Topic:          sub.Topic,
		SubscriptionId: sub.Id,
		Subscribed:     true,
	}
	defer func() {
		sub.logger.Debug("pubsub: unsubscribing from topic")
		sub.Events <- SubscriptionEvent{
			Topic:          sub.Topic,
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
		sql := "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;"
		rows, err := deps.Query(sub.ctx, conn, sql, sub.Topic, sub.maxMessageID)
		if err != nil {
			err = fmt.Errorf("pubsub: failed to check for missed messages: %w", err)
			sub.Events <- ErrorEvent{
				Topic:          sub.Topic,
				SubscriptionId: sub.Id,
				Error:          err,
			}
			return
		}
		defer rows.Close()

		for rows.Next() {
			var messageId MessageId
			var payload string
			var publishedAt time.Time
			if err := rows.Scan(&messageId, &payload, &publishedAt); err != nil {
				err = fmt.Errorf("pubsub: failed to read missed message: %w", err)
				sub.Events <- ErrorEvent{
					Topic:          sub.Topic,
					SubscriptionId: sub.Id,
					Error:          err,
				}
				return
			}

			client.handlePayload(sub, messageId, payload, publishedAt)
			sub.maxMessageID = messageId
		}
	}

	// Wait for and process messages until the context is cancelled or a
	// connection error occurs.
	for {
		// Block until a notification is received. This is cancellable
		// by the context passed to the subscribe function.
		sub.logger.Debug("pubsub: waiting for message")
		notification, err := deps.WaitForNotification(sub.ctx, conn)
		if err != nil {
			err = fmt.Errorf("pubsub: failed while waiting for message: %w", err)
			sub.Events <- ErrorEvent{
				Topic:          sub.Topic,
				SubscriptionId: sub.Id,
				Error:          err,
			}
			return
		}

		// Unmarshal the envelope.
		// Report errors but do not stop handling messages.
		var envelope struct {
			MessageId   MessageId `json:"message_id"`
			Payload     string    `json:"payload"`
			PublishedAt time.Time `json:"published_at"`
		}
		if err := deps.UnmarshalStringJSON(notification.Payload, &envelope); err != nil {
			err = fmt.Errorf("pubsub: failed to unmarshal envelope: %w", err)
			sub.Events <- ErrorEvent{
				Topic:          sub.Topic,
				SubscriptionId: sub.Id,
				Error:          err,
			}
			continue
		}

		// Handle the message and track the max message ID.
		// While it shouldn't be possible to receive a message ID less than the max
		// message ID because of the Postgres function used to lock the table,
		// select the max message ID, and start listening within a transaction, it
		// seems more robust to check.
		if envelope.MessageId > sub.maxMessageID {
			client.handlePayload(sub, envelope.MessageId, envelope.Payload, envelope.PublishedAt)
			sub.maxMessageID = envelope.MessageId
		} else {
			sub.logger.Warn("pubsub: ignoring out-of-order message",
				slog.Int64("pubsub:message_id", int64(envelope.MessageId)),
				slog.Int64("pubsub:max_message_id", int64(sub.maxMessageID)),
			)
		}
	}
}

func (client *Client[T]) handlePanic(sub *Subscription) {
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

		sub.Events <- ErrorEvent{
			Topic:          sub.Topic,
			SubscriptionId: sub.Id,
			Error:          err,
		}
	}
}

func (client *Client[T]) handlePayload(
	sub *Subscription,
	messageId MessageId,
	payload string,
	publishedAt time.Time,
) {
	var message T
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		err = fmt.Errorf("pubsub: failed to unmarshal message: %w", err)
		sub.Events <- ErrorEvent{
			Topic:          sub.Topic,
			SubscriptionId: sub.Id,
			Error:          err,
		}
		return
	}

	sub.logger.Debug("pubsub: received message",
		slog.Int64("pubsub:message_id", int64(messageId)),
		slog.String("pubsub:latency", time.Since(publishedAt).String()),
	)
	sub.Events <- MessageEvent[T]{
		Topic:          sub.Topic,
		SubscriptionId: sub.Id,
		MessageId:      messageId,
		Message:        message,
		PublishedAt:    publishedAt,
	}
}
