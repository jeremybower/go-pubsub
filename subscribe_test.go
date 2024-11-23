package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type TestMessage struct {
	Value int `json:"value"`
}

func handlerForTesting[T any](accumulator *[]T) (Handler[T], *sync.Cond, func() T) {
	return HandlerWithCond[T](func(v T) {
		*accumulator = append(*accumulator, v)
	})
}

func TestHandlerWithCond(t *testing.T) {
	t.Parallel()

	handler, cond, latest := HandlerWithCond[int](nil)
	require.NotNil(t, handler)
	require.NotNil(t, cond)
	require.NotNil(t, latest)

	// Expect to be notified when the handler is called.
	var wasNotified bool
	wasNotifiedCond := sync.NewCond(&sync.Mutex{})
	go func() {
		cond.L.Lock()
		for latest() == 0 {
			cond.Wait()
		}
		cond.L.Unlock()

		wasNotified = true
		wasNotifiedCond.Broadcast()
	}()

	// Call the handler.
	handler(42)

	// Wait for the notification.
	wasNotifiedCond.L.Lock()
	for !wasNotified {
		wasNotifiedCond.Wait()
	}
	wasNotifiedCond.L.Unlock()

	// Check the values.
	assert.True(t, wasNotified)
	assert.Equal(t, 42, latest())
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	// Handle messages.
	var actualMessages []*TestMessage
	msgHandler, messagesCond, _ := handlerForTesting(&actualMessages)

	// Handle subscribed events.
	var actualListenings []bool
	subHandler, subscribedCond, isSubscribed := handlerForTesting(&actualListenings)

	// Handle errors.
	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	// Subscribe to a topic.
	topic := "test"
	subscriptionID, cancel := Subscribe[*TestMessage](context.Background(), logger, dbPool, topic, msgHandler, subHandler, errHandler)
	require.NotEmpty(t, subscriptionID)
	require.NotNil(t, cancel)
	defer cancel()

	// Wait for a subscription.
	subscribedCond.L.Lock()
	for !isSubscribed() {
		subscribedCond.Wait()
	}
	subscribedCond.L.Unlock()

	// Publish a message.
	messageID, err := Publish(context.Background(), logger, dbPool, topic, &TestMessage{Value: 42})
	require.NoError(t, err)
	assert.Greater(t, messageID, int64(0))

	// Wait for the message to be received.
	messagesCond.L.Lock()
	for len(actualMessages) == 0 {
		messagesCond.Wait()
	}
	messagesCond.L.Unlock()

	assert.Equal(t, []*TestMessage{{Value: 42}}, actualMessages)

	// Cancel the subscription.
	cancel()
	assert.Equal(t, []bool{true, false}, actualListenings)

	// Check for errors.
	assert.Equal(t, []string{
		"pubsub subscription: context canceled",
		"pubsub subscription: context canceled (fatal)",
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestSubscribeWhenMissedMessages(t *testing.T) {
	t.Parallel()

	const messageCount = 3

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	// Publish messages to the database.
	topic := "test"
	var messageIDs []int64
	var messages []*TestMessage
	for i := 0; i < messageCount; i++ {
		message := &TestMessage{Value: int(rand.Int63n(2 ^ 31))}
		messageID, err := Publish(context.Background(), logger, dbPool, topic, message)
		require.NoError(t, err)
		assert.Greater(t, messageID, int64(0))

		messageIDs = append(messageIDs, messageID)
		messages = append(messages, message)
	}

	// Handle messages.
	var actualMessages []*TestMessage
	msgHandler, messagesCond, _ := handlerForTesting(&actualMessages)

	// Handle subscribed events.
	var actualListenings []bool
	subHandler, _, _ := handlerForTesting(&actualListenings)

	// Handle errors.
	errHandler := func(err SubscriptionError) {}

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	require.NotNil(t, cancel)
	require.NotNil(t, ctx)
	defer cancel()

	// Setup expected values.
	expectedMessages := messages[1:]
	expectedListenings := []bool{true, false}

	// Stop listening after the expected number of messages.
	go func() {
		messagesCond.L.Lock()
		for len(actualMessages) < len(expectedMessages) {
			messagesCond.Wait()
		}
		messagesCond.L.Unlock()

		cancel()
	}()

	// Listen for messages.
	maxMessageID := messageIDs[0]
	listen(ctx, logger, dbPool, topic, &maxMessageID, msgHandler, subHandler, errHandler, &standardDependencies{})

	// Check the actual values.
	assert.Equal(t, expectedMessages, actualMessages)
	assert.Equal(t, expectedListenings, actualListenings)
}

func TestSubscribeParams(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	msgHandler := func(message *TestMessage) {}

	tests := []struct {
		name       string
		ctx        context.Context
		logger     *slog.Logger
		dbPool     *pgxpool.Pool
		topic      string
		msgHandler Handler[*TestMessage]
		subHandler Handler[bool]
		errHandler Handler[SubscriptionError]
		panicValue any
	}{
		{
			name:       "context is nil",
			logger:     logger,
			dbPool:     dbPool,
			topic:      "test",
			msgHandler: msgHandler,
			panicValue: "pubsub: context is nil",
		},
		{
			name:       "logger is nil",
			ctx:        context.Background(),
			dbPool:     dbPool,
			topic:      "test",
			msgHandler: msgHandler,
			subHandler: nil,
			errHandler: nil,
			panicValue: "pubsub: logger is nil",
		},
		{
			name:       "dbPool is nil",
			ctx:        context.Background(),
			logger:     logger,
			topic:      "test",
			msgHandler: msgHandler,
			subHandler: nil,
			errHandler: nil,
			panicValue: "pubsub: database pool is nil",
		},
		{
			name:       "topic is empty",
			ctx:        context.Background(),
			logger:     logger,
			dbPool:     dbPool,
			msgHandler: msgHandler,
			subHandler: nil,
			errHandler: nil,
			panicValue: "pubsub: topic is empty",
		},
		{
			name:       "message handler is nil",
			ctx:        context.Background(),
			logger:     logger,
			dbPool:     dbPool,
			topic:      "test",
			msgHandler: nil,
			subHandler: nil,
			errHandler: nil,
			panicValue: "pubsub: message handler is nil",
		},
		{
			name:       "subscribed handler is nil",
			ctx:        context.Background(),
			logger:     logger,
			dbPool:     dbPool,
			topic:      "test",
			msgHandler: msgHandler,
			subHandler: nil,
			errHandler: nil,
			panicValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panicValue == nil {
				assert.NotPanics(t, func() {
					_, cancel := Subscribe(tt.ctx, tt.logger, tt.dbPool, tt.topic, tt.msgHandler, tt.subHandler, tt.errHandler)
					cancel()
				})
			} else {
				assert.PanicsWithValue(t, tt.panicValue, func() {
					Subscribe(tt.ctx, tt.logger, tt.dbPool, tt.topic, tt.msgHandler, tt.subHandler, tt.errHandler)
				})
			}
		})
	}
}

func TestSubscribeWhenAquiringConnectionFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	ctx := context.Background()
	logger := slogt.New(t)

	errAcquire := errors.New("Acquire")

	var nilConn *pgxpool.Conn
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Return(nilConn, errAcquire)

	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	maxMessageID := int64(0)
	msgHandler := func(message *TestMessage) {}
	subHandler := func(subscribed bool) {}
	listen(ctx, logger, dbPool, "test", &maxMessageID, msgHandler, subHandler, errHandler, deps)

	deps.AssertExpectations(t)
	assert.Equal(t, []string{
		"pubsub subscription: " + errAcquire.Error(),
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestSubscribeWhenSubscriptionFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	ctx := context.Background()
	logger := slogt.New(t)

	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	errScan := errors.New("Scan")

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Return(errScan)

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", context.Background(), conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)

	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	maxMessageID := int64(0)
	msgHandler := func(message *TestMessage) {}
	subHandler := func(subscribed bool) {}
	listen(ctx, logger, dbPool, "test", &maxMessageID, msgHandler, subHandler, errHandler, deps)

	deps.AssertExpectations(t)
	assert.Equal(t, []string{
		"pubsub subscription: " + errScan.Error(),
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestSubscribeWhenQueryingForMissedRowsFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	ctx := context.Background()
	logger := slogt.New(t)

	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*int64)
		*maxMessageID = int64(123)
	}).Return(nil)

	var errQuery = errors.New("Query")

	var nilRows *mockRows
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;", []any{"test", int64(1)}).Once().Return(nilRows, errQuery)

	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	maxMessageID := int64(1)
	msgHandler := func(message *TestMessage) {}
	subHandler := func(subscribed bool) {}
	listen(ctx, logger, dbPool, "test", &maxMessageID, msgHandler, subHandler, errHandler, deps)

	deps.AssertExpectations(t)
	assert.Equal(t, []string{
		"pubsub subscription: " + errQuery.Error(),
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestSubscribeWhenScanningMissedRowFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	ctx := context.Background()
	logger := slogt.New(t)

	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*int64)
		*maxMessageID = int64(123)
	}).Return(nil)

	errScan := errors.New("Scan")

	missedMessageRows := &mockRows{}
	missedMessageRows.On("Close").Once().Return()
	missedMessageRows.On("Next").Once().Return(true)
	missedMessageRows.On("Scan", mock.Anything).Once().Return(errScan)

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;", []any{"test", int64(1)}).Once().Return(missedMessageRows, nil)

	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	maxMessageID := int64(1)
	msgHandler := func(message *TestMessage) {}
	subHandler := func(subscribed bool) {}
	listen(ctx, logger, dbPool, "test", &maxMessageID, msgHandler, subHandler, errHandler, deps)

	deps.AssertExpectations(t)
	assert.Equal(t, []string{
		"pubsub subscription: " + errScan.Error(),
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestSubscribeWhenUnmarshallingFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	ctx := context.Background()
	logger := slogt.New(t)

	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*int64)
		*maxMessageID = int64(0)
	}).Return(nil)

	errUnmarshalStringJSON := errors.New("UnmarshalStringJSON")
	errWaitForNotification := errors.New("WaitForNotification")

	var nilNotification *pgconn.Notification
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("WaitForNotification", ctx, conn).Once().Return(&pgconn.Notification{
		PID:     123,
		Channel: "test",
		Payload: `{"value": 42}`,
	}, nil)
	deps.On("UnmarshalStringJSON", `{"value": 42}`, mock.Anything).Once().Return(errUnmarshalStringJSON)
	deps.On("WaitForNotification", ctx, conn).Once().Return(nilNotification, errWaitForNotification)

	var actualErrs []SubscriptionError
	errHandler, _, _ := handlerForTesting(&actualErrs)

	maxMessageID := int64(1)
	msgHandler := func(message *TestMessage) {}
	subHandler := func(subscribed bool) {}
	listen(ctx, logger, dbPool, "test", &maxMessageID, msgHandler, subHandler, errHandler, deps)

	deps.AssertExpectations(t)
	assert.Equal(t, []string{
		"pubsub subscription: " + errUnmarshalStringJSON.Error(),
		"pubsub subscription: " + errWaitForNotification.Error(),
	}, SubscriptionErrors(actualErrs).Errors())
}

func TestHandleMessageWhenInvalidPayload(t *testing.T) {
	t.Parallel()

	logger := slogt.New(t)
	msgCallback := func(message *TestMessage) {}
	errs := []SubscriptionError{}
	errCallback := func(err SubscriptionError) { errs = append(errs, err) }
	handleMessage[*TestMessage](logger, 123, "invalid", time.Now(), msgCallback, errCallback)
	assert.Equal(t, "pubsub subscription: invalid character 'i' looking for beginning of value", errs[0].Error())
}

func TestHandlePanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		fatal  bool
		pvalue any
		err    SubscriptionError
	}{
		{
			name:   "string",
			fatal:  false,
			pvalue: "test",
			err:    NewSubscriptionError(errors.New("test"), false),
		},
		{
			name:   "error",
			fatal:  true,
			pvalue: errors.New("test"),
			err:    NewSubscriptionError(errors.New("test"), true),
		},
		{
			name:   "any",
			fatal:  false,
			pvalue: 123,
			err:    NewSubscriptionError(errors.New("panic with unexpected value: 123"), false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := slogt.New(t)

			var err SubscriptionError
			errCallback := func(e SubscriptionError) { err = e }
			func() {
				defer handlePanic(logger, tt.fatal, errCallback)
				panic(tt.pvalue)
			}()

			assert.Equal(t, tt.err, err)
		})
	}
}
