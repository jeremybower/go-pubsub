package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"mime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/optional"
	"github.com/jeremybower/go-common/postgres"
)

type Subscription struct {
	cancel             context.CancelFunc
	client             *Client
	closed             bool
	dbPool             *pgxpool.Pool
	decoders           map[string]Decoder
	events             []any
	eventsCond         *sync.Cond
	id                 SubscriptionID
	mutex              *sync.Mutex
	restartAtMessageID MessageID
	statusCond         *sync.Cond
	subscribed         bool
	topicCache         *cache[TopicID, Topic]
	wg                 sync.WaitGroup
}

type errorHandler func(ctx context.Context, logger *slog.Logger, err error) error
type messageHandler func(ctx context.Context, logger *slog.Logger, topic Topic, message Message) (*Message, error)
type subscribedHandler func(ctx context.Context, logger *slog.Logger, subscribed bool)

func newSubscription(
	client *Client,
	dbPool *pgxpool.Pool,
	cancel context.CancelFunc,
	options *SubscribeOptions,
) *Subscription {
	// Replace a nil cancel function with a no-op function.
	if cancel == nil {
		cancel = func() {}
	}

	// Share the mutex with condition variables.
	mutex := &sync.Mutex{}

	// Create the subscription.
	sub := &Subscription{
		cancel:             cancel,
		client:             client,
		dbPool:             dbPool,
		decoders:           options.decoders,
		events:             make([]any, 0, 10),
		eventsCond:         sync.NewCond(mutex),
		id:                 SubscriptionID(uuid.NewString()),
		mutex:              mutex,
		restartAtMessageID: options.restartAtMessageID,
		statusCond:         sync.NewCond(mutex),
		topicCache:         newCache[TopicID, Topic](),
	}

	// Return the subscription.
	return sub
}

func (sub *Subscription) Close() {
	sub.mutex.Lock()
	if !sub.closed {
		// Set the closed flag.
		sub.closed = true

		// Notify the subscriber that the subscription was closed.
		sub.statusCond.Broadcast()

		// Add an event to the queue.
		sub.events = append(sub.events, StatusEvent{
			SubscriptionID: sub.id,
			Closed:         optional.NewValue(true),
		})

		// Notify the subscriber that an event was queued.
		sub.eventsCond.Signal()
	}
	sub.mutex.Unlock()

	// Cancel the context to force any blocked goroutines to return.
	sub.cancel()

	// Wait for the subscription goroutines to terminate.
	sub.wg.Wait()
}

func (sub *Subscription) Event() any {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	if len(sub.events) > 0 {
		event := sub.events[0]
		sub.events = sub.events[1:]
		return event
	}

	return nil
}

func (sub *Subscription) WaitForEvent(ctx context.Context) (any, error) {
	// If there is an event in the queue, then return it immediately.
	event := sub.Event()
	if event != nil {
		return event, nil
	}

	// Create a channel to notify when an event is queued.
	ch := make(chan bool, 1)
	go func() {
		defer close(ch)

		sub.eventsCond.L.Lock()
		defer sub.eventsCond.L.Unlock()

		for {
			if len(sub.events) > 0 {
				ch <- true
				return
			}

			sub.eventsCond.Wait()
		}
	}()

	// Wait for an event to be queued or the context to be cancelled.
	for {
		select {
		case <-ctx.Done():
			// The context was cancelled.
			return nil, ctx.Err()
		case <-ch:
			// An event was queued.
			event := sub.Event()
			if event != nil {
				return event, nil
			}
		}
	}
}

func (sub *Subscription) WaitUntilSubscribed(ctx context.Context) error {
	sub.statusCond.L.Lock()
	defer sub.statusCond.L.Unlock()

	// TODO: Replace this for loop with a select statement that waits on
	// channels for the context to cancel or the subscription to subscribe.
	for !sub.subscribed {
		// Check if the context is cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Block until the subscription is subscribed.
		sub.statusCond.Wait()
	}

	return nil
}

func (sub *Subscription) decodeValue(
	ctx context.Context,
	logger *slog.Logger,
	encodedValue *EncodedValue,
	errorHandler errorHandler,
) (any, error) {
	// Check for a nil encoded value.
	if encodedValue == nil {
		return nil, nil
	}

	// Parse the content type.
	contentType, _, err := mime.ParseMediaType(encodedValue.ContentType)
	if err != nil {
		logger.Error("pubsub: failed to parse content type", slog.Any("error", err))
		return nil, errorHandler(ctx, logger, err)
	}

	// Get the decoder for the content type.
	decoder, ok := sub.decoders[contentType]
	if !ok {
		logger.Error("pubsub: no decoder for content type",
			slog.String("pubsub:content_type", encodedValue.ContentType),
		)

		err := fmt.Errorf("%w: %s", ErrDecoderNotRegistered, contentType)
		return nil, errorHandler(ctx, logger, err)
	}

	// Decode the value.
	value, err := decoder.Decode(ctx, encodedValue)
	if err != nil {
		logger.Error("pubsub: failed to decode message", slog.Any("error", err))
		return nil, errorHandler(ctx, logger, err)
	}

	// Return the decoded value.
	return value, nil
}

func (sub *Subscription) handleDelay(
	_ context.Context,
	_ *slog.Logger,
	delay time.Duration,
) {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// Add an event to the queue.
	sub.events = append(sub.events, StatusEvent{
		SubscriptionID: sub.id,
		Delay:          optional.NewValue(delay),
	})

	// Notify the subscriber that an event was queued.
	sub.eventsCond.Signal()
}

func (sub *Subscription) handleEncodedMessage(
	ctx context.Context,
	logger *slog.Logger,
	encodedMessage *EncodedMessage,
	messageHandler messageHandler,
	errorHandler errorHandler,
) (*Message, error) {
	// Create a logger with the message ID and encoding.
	logger = logger.With(slog.Int64("pubsub:message_id", int64(encodedMessage.MessageID)))
	if encodedMessage.EncodedValue != nil {
		logger = logger.With(slog.String("pubsub:message_content_type", encodedMessage.EncodedValue.ContentType))
	}

	// Log that a message was received.
	logger.Debug("pubsub: received message",
		slog.Duration("pubsub:latency", time.Since(encodedMessage.PublishedAt)),
	)

	// Decode the value.
	value, err := sub.decodeValue(ctx, logger, encodedMessage.EncodedValue, errorHandler)
	if err != nil {
		return nil, err
	}

	// Handle the message.
	return messageHandler(ctx, logger, encodedMessage.Topic, Message{
		ID:          encodedMessage.MessageID,
		Value:       value,
		PublishedAt: encodedMessage.PublishedAt,
	})
}

func (sub *Subscription) handleError(
	_ context.Context,
	_ *slog.Logger,
	err error,
) error {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// Add an event to the queue.
	sub.events = append(sub.events, ErrorEvent{
		SubscriptionID: sub.id,
		Error:          err,
	})

	// Notify the subscriber that an event was queued.
	sub.eventsCond.Signal()

	// Return nil to indicate that the error was handled.
	return nil
}

func (sub *Subscription) handleMessage(
	_ context.Context,
	_ *slog.Logger,
	topic Topic,
	message Message,
) (*Message, error) {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// Add an event to the queue.
	sub.events = append(sub.events, MessageEvent{
		SubscriptionID: sub.id,
		Topic:          topic,
		Message:        message,
	})

	// Notify the subscriber that an event was queued.
	sub.eventsCond.Signal()

	// Return nil to indicate that the message was handled.
	return nil, nil
}

func (sub *Subscription) handleMessageNotification(
	ctx context.Context,
	logger *slog.Logger,
	channel string,
	notification *MessageNotification,
	messageHandler messageHandler,
	errorHandler errorHandler,
) (*Message, error) {

	// Get the topic from the channel.
	topic, err := topicForChannel(channel, sub.topicCache)
	if err != nil {
		logger.Error("pubsub: failed to find topic for channel", slog.Any("error", err))
		return nil, errorHandler(ctx, logger, err)
	}

	// Check for an encoded value.
	var encodedValue *EncodedValue
	if notification.HasValue {
		encodedValue, err = sub.client.dataStore.ReadEncodedValue(ctx, sub.dbPool, notification.MessageID)
		if err != nil {
			logger.Error("pubsub: failed to read encoded value", slog.Any("error", err))
			return nil, errorHandler(ctx, logger, err)
		}
	}

	// Create the encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    notification.MessageID,
		Topic:        *topic,
		EncodedValue: encodedValue,
		PublishedAt:  postgres.Time(notification.PublishedAt),
	}

	// Handle the payload.
	return sub.handleEncodedMessage(
		ctx,
		logger,
		encodedMessage,
		messageHandler,
		errorHandler,
	)
}

func (sub *Subscription) handleSubscribed(
	_ context.Context,
	_ *slog.Logger,
	subscribed bool,
) {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// Update the subscribed flag.
	sub.subscribed = subscribed

	// Notify the subscriber that the status changed.
	sub.statusCond.Broadcast()

	// Add an event to the queue.
	sub.events = append(sub.events, StatusEvent{
		SubscriptionID: sub.id,
		Subscribed:     optional.NewValue(subscribed),
	})

	// Notify the subscriber that an event was queued.
	sub.eventsCond.Signal()
}

func (sub *Subscription) subscribe(
	ctx context.Context,
	logger *slog.Logger,
	topics []string,
	subscribedHandler subscribedHandler,
	messageHandler messageHandler,
	errorHandler errorHandler,
) error {
	// Acquire a connection to the database from the pool.
	logger.Debug("pubsub: acquiring database connection")
	conn, err := sub.client.dataStore.AcquireConnection(ctx, sub.dbPool)
	if err != nil {
		logger.Error("pubsub: failed to acquire database connection", slog.Any("error", err))
		return errorHandler(ctx, logger, err)
	}
	defer conn.Release()

	// Subscribe to the topic and read the current max message ID.
	// If there are no messages in the database (for any topic), then the max
	// message ID will be 0. Otherwise, it will be the highest message ID for any
	// topic.
	//
	// Subscribe to the topic before checking for missed messages. If the
	// subscription occurs after checking for missed messages, then there is a
	// possible race condition if new messages are created before subscribing.
	//
	// If the subscription is started for the first time, then the max message ID
	// will be 0 and there are no missed messages.
	//
	// If the subscription is restarted after a connection error, then the max
	// message ID will be the last message ID that was processed before the
	// connection was lost.
	receipt, err := sub.client.dataStore.Subscribe(ctx, conn, topics)
	if err != nil {
		logger.Error("pubsub: failed to subscribe to topic", slog.Any("error", err))
		return errorHandler(ctx, logger, err)
	}

	// Log that the subscription is subscribed to the topic.
	logger.Debug("pubsub: subscribed to topic")

	// Notify the caller that the subscription is subscribed to the topic.
	// Schedule a deferred callback to notify the caller when the subscription
	// is cancelled
	subscribedHandler(ctx, logger, true)
	defer func() {
		logger.Debug("pubsub: unsubscribing from topic")
		subscribedHandler(ctx, logger, false)
	}()

	// Cache the topics.
	for _, topic := range receipt.Topics {
		sub.topicCache.set(topic.ID, topic)
	}

	// If the max message ID has changed, then there might be messages
	// for this topic. Check for missed messages and process them.
	// If this subscription is starting for the first time, then
	// the max message ID will be 0 and there are no missed messages.
	if sub.restartAtMessageID != 0 && receipt.MaxMessageID > sub.restartAtMessageID {
		// Log that the subscription is checking for missed messages.
		logger.Debug("pubsub: checking for missed messages")

		// Query for missed messages.
		ch, err := sub.client.dataStore.ReadEncodedMessagesAfterID(ctx, conn, sub.restartAtMessageID, topics)
		if err != nil {
			logger.Error("pubsub: failed to check for missed messages", slog.Any("error", err))
			return errorHandler(ctx, logger, err)
		}

		// Handle each missed message.
		for result := range ch {
			// Check for an error.
			if result.Error != nil {
				logger.Error("pubsub: failed to read missed message", slog.Any("error", result.Error))
				return errorHandler(ctx, logger, result.Error)
			}

			// Get the encoded message.
			encodedMessage := &result.Value

			// Update the max message ID to track the last processed message.
			sub.restartAtMessageID = encodedMessage.MessageID

			// Start a goroutine to allow this loop to continue processing
			// missed messages.
			sub.wg.Add(1)
			go func() {
				defer sub.wg.Done()

				sub.handleEncodedMessage(
					ctx,
					logger,
					encodedMessage,
					messageHandler,
					errorHandler,
				)
			}()
		}
	}

	// Block and process messages until the context is cancelled or a
	// connection error occurs.
	for {
		// Wait for a notification, or for the context to be cancelled.
		notification, err := sub.client.dataStore.WaitForNotification(ctx, conn)
		if err != nil {
			logger.Error("pubsub: failed to wait for notification", slog.Any("error", err))
			return errorHandler(ctx, logger, err)
		}

		// Handle the notification.
		sub.handleNotification(ctx, logger, notification, messageHandler, errorHandler)
	}
}

func (sub *Subscription) handleNotification(
	ctx context.Context,
	logger *slog.Logger,
	n *pgconn.Notification,
	messageHandler messageHandler,
	errorHandler errorHandler,
) error {
	// Unmarshal the message notification payload.
	notification, err := sub.client.dataStore.UnmarshalMessageNotification(n.Payload)
	if err != nil {
		// Report the payload error, but continue handling messages.
		logger.Error("pubsub: failed to unmarshal message notification", slog.Any("error", err))
		return errorHandler(ctx, logger, err)
	}

	// Update the max message ID to track the last processed message.
	sub.restartAtMessageID = notification.MessageID

	// Continue handing the message notification in a goroutine.
	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()

		// Handle the message notification.
		sub.handleMessageNotification(
			ctx,
			logger,
			n.Channel,
			notification,
			messageHandler,
			errorHandler,
		)
	}()

	return nil
}

const TopicChannelPrefix = "pubsub_topic:"

func topicForChannel(channel string, topicCache *cache[TopicID, Topic]) (*Topic, error) {
	// Check for the expected prefix.
	if !strings.HasPrefix(channel, TopicChannelPrefix) {
		return nil, ErrTopicInvalidChannelPrefix
	}

	// Parse the topic ID from the channel.
	int64ID, err := strconv.ParseInt(strings.TrimPrefix(channel, TopicChannelPrefix), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrTopicInvalidID, err)
	}

	// Check the cache for the topic.
	topicID := TopicID(int64ID)
	if topic, ok := topicCache.get(topicID); ok {
		return &topic, nil
	}

	// Topic not found.
	return nil, fmt.Errorf("%w: %d", ErrTopicUnknown, topicID)
}
