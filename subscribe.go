package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/backoff"
)

type Handler[T any] func(v T)

func HandlerWithCond[T any](handler Handler[T]) (Handler[T], *sync.Cond, func() T) {
	if handler == nil {
		handler = func(v T) {}
	}

	var lastest T
	cond := sync.NewCond(&sync.Mutex{})
	return func(v T) {
			handler(v)
			lastest = v
			cond.Broadcast()
		}, cond, func() T {
			return lastest
		}
}

func Subscribe[T any](
	ctx context.Context,
	logger *slog.Logger,
	dbPool *pgxpool.Pool,
	topic string,
	msgHandler Handler[T],
	subHandler Handler[bool],
	errHandler Handler[SubscriptionError],
) (string, func()) {
	if ctx == nil {
		panic("pubsub: context is nil")
	}

	if logger == nil {
		panic("pubsub: logger is nil")
	}

	if dbPool == nil {
		panic("pubsub: database pool is nil")
	}

	if strings.TrimSpace(topic) == "" {
		panic("pubsub: topic is empty")
	}

	if msgHandler == nil {
		panic("pubsub: message handler is nil")
	}

	if subHandler == nil {
		subHandler = func(v bool) {}
	}

	if errHandler == nil {
		errHandler = func(v SubscriptionError) {}
	}

	// Customize the logger with a unique identifier for this subscription.
	id := uuid.NewString()
	logger = logger.With(slog.String("topic", topic), slog.String("subscription_id", id))

	// Create a cancelable context and a wait group to
	// track when the subscribe function returns.
	cancelableCtx, cancelCtx := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	// Create a cancel function that cancels the context and
	// waits for the subscribe function to return.
	cancel := func() {
		logger.Info("pubsub: cancelling subscription")
		cancelCtx()
		wg.Wait()
	}

	// Start the subscription in a goroutine that can recover from errors.
	go func() {
		logger.Info("pubsub: subscribing to topic")
		defer logger.Info("pubsub: unsubscribed from topic")

		// Since defers are executed in LIFO order, the handlePanic function will be called
		// before the wg.Done function. This is important because the wg.Done function must
		// be called last to signal that the Subscribe function has completed and no other
		// calls will be made to the msgHandler or errHandler functions.
		defer wg.Done()
		defer handlePanic(logger, Fatal, errHandler)

		// Keep track of the max message ID that has been processed so that subscribers
		// can get caught up if the connection fails.
		var maxMessageID int64

		// Use an exponential backoff to wait an increasing amount of time before trying to
		// reconnect. This is useful for handling transient errors like network outages or
		// database restarts.
		exp := backoff.New(0, 60, 120, false)
		for {
			// Listen for messages.
			listen[T](cancelableCtx, logger, dbPool, topic, &maxMessageID, msgHandler, subHandler, errHandler, &standardDependencies{})

			// If the subscribe function returns, then either the context was cancelled or the
			// the connection was lost.
			select {
			case <-cancelableCtx.Done():
				errHandler(NewSubscriptionError(cancelableCtx.Err(), true))
				return
			case <-exp.Wait():
			}
		}
	}()

	return id, cancel
}

func listen[T any](
	ctx context.Context,
	logger *slog.Logger,
	dbPool *pgxpool.Pool,
	topic string,
	maxMessageID *int64,
	msgHandler Handler[T],
	subHandler Handler[bool],
	errHandler Handler[SubscriptionError],
	deps dependencies,
) {
	// Acquire a connection to the database from the pool.
	logger.Info("pubsub: acquiring database connection")
	conn, err := deps.Acquire(ctx, dbPool)
	if err != nil {
		logger.Error("pubsub: failed to acquire database connection", slog.Any("error", err))
		errHandler(NewSubscriptionError(err, false))
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
	var newMaxMessageID int64
	row := deps.QueryRow(ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", topic)
	if err := row.Scan(&newMaxMessageID); err != nil {
		logger.Error("pubsub: failed to subscribe to topic", slog.Any("error", err))
		errHandler(NewSubscriptionError(err, false))
		return
	}

	// If the max message ID has changed, then there might be messages
	// for this topic. Check for missed messages and process them.
	// If this subscription is starting for the first time, then
	// the max message ID will be 0 and there are no missed messages.
	if *maxMessageID != 0 && newMaxMessageID > *maxMessageID {
		logger.Info("pubsub: checking for missed messages")
		sql := "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;"
		rows, err := deps.Query(ctx, conn, sql, topic, *maxMessageID)
		if err != nil {
			logger.Error("pubsub: failed to check for missed messages", slog.Any("error", err))
			errHandler(NewSubscriptionError(err, false))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var id int64
			var payload string
			var publishedAt time.Time
			if err := rows.Scan(&id, &payload, &publishedAt); err != nil {
				logger.Error("pubsub: failed to read missed message", slog.Any("error", err))
				errHandler(NewSubscriptionError(err, false))
				return
			}

			handleMessage(logger, id, payload, publishedAt, msgHandler, errHandler)
			*maxMessageID = id
		}
	}

	// Log that the subscription is listening to the topic.
	// Schedule a deferred callback to log when the subscription
	// has stopped listening.
	logger.Info("pubsub: started listening to topic")
	defer logger.Info("pubsub: stopped listening to topic")

	// Notify the caller that the subscription is listening for messages.
	// Schedule a deferred callback to notify the caller when the subscription
	// has stopped listening.
	subHandler(true)
	defer subHandler(false)

	// Wait for and process messages until the context is cancelled or a
	// connection error occurs.
	for {
		// Block until a notification is received. This is cancellable
		// by the context passed to the subscribe function.
		logger.Info("pubsub: waiting for message")
		notification, err := deps.WaitForNotification(ctx, conn)
		if err != nil {
			logger.Error("pubsub: failed while waiting for message", slog.Any("error", err))
			errHandler(NewSubscriptionError(err, false))
			return
		}

		// Unmarshal the notification payload.
		// Log errors but do not stop handling messages.
		var np struct {
			ID          int64     `json:"id"`
			Payload     string    `json:"payload"`
			PublishedAt time.Time `json:"published_at"`
		}
		if err := deps.UnmarshalStringJSON(notification.Payload, &np); err != nil {
			logger.Error("pubsub: failed to unmarshal envelope", slog.Any("error", err))
			errHandler(NewSubscriptionError(err, false))
			continue
		}

		// Handle the message and track the max message ID.
		// While it shouldn't be possible to receive a message with an ID less than
		// the max message ID because of the Postgres function used to lock the
		// table select the max message ID and start listening within a transaction,
		// it seems more robust to check.
		if np.ID > *maxMessageID {
			handleMessage(logger, np.ID, np.Payload, np.PublishedAt, msgHandler, errHandler)
			*maxMessageID = max(*maxMessageID, np.ID)
		} else {
			logger.Warn("pubsub: ignoring out-of-order message", slog.Int64("message_id", np.ID), slog.Int64("max_message_id", *maxMessageID))
		}
	}
}

func handlePanic(logger *slog.Logger, fatal bool, errHandler Handler[SubscriptionError]) {
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

		logger.Error("pubsub: panic", slog.Any("error", err))
		errHandler(NewSubscriptionError(err, fatal))
	}
}

func handleMessage[T any](
	logger *slog.Logger,
	id int64,
	payload string,
	publishedAt time.Time,
	msgHandler Handler[T],
	errHandler Handler[SubscriptionError],
) {
	var message T
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		logger.Error("pubsub: failed to unmarshal message", slog.Any("error", err))
		errHandler(NewSubscriptionError(err, false))
		return
	}

	logger.Info("pubsub: received message", slog.Int64("message_id", id), slog.String("latency", time.Since(publishedAt).String()))
	msgHandler(message)
}
