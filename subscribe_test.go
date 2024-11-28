package pubsub

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type TestMessage struct {
	Value int `json:"value"`
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	ctx = common.WithLogger(ctx, slogt.New(t))

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Subscribe to a topic.
	const topic = "test"
	sub, err := client.Subscribe(ctx, topic, 0)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Wait for the subscription to be established.
	waitSubscribed(t, sub, true)

	// Publish a message.
	message := TestMessage{Value: 42}
	messageId, err := client.Publish(ctx, topic, message)
	require.NoError(t, err)
	assert.Greater(t, messageId, int64(0))

	// Wait for the message to be received.
	waitMessage(t, sub, messageId, message)

	// Cancel the subscription.
	cancel()

	// Wait for the subscription to cancel.
	waitError(t, sub, "pubsub: failed while waiting for message: context canceled")
	waitSubscribed(t, sub, false)
	waitCancelled(t, sub)
}

func TestSubscribeWhenMissedMessages(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Publish messages to the database.
	topic := "test"
	const messageCount = 3
	var messageIDs []MessageId
	var messages []TestMessage
	for i := 0; i < messageCount; i++ {
		message := TestMessage{Value: int(rand.Int63n(2 ^ 31))}
		messageID, err := client.Publish(ctx, topic, message)
		require.NoError(t, err)
		assert.Greater(t, messageID, int64(0))

		messageIDs = append(messageIDs, messageID)
		messages = append(messages, message)
	}

	// Listen for messages.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	sub.maxMessageID = messageIDs[0]
	go client.subscribe(sub, &standardDependencies{})

	// Wait for the subscription to be established.
	waitSubscribed(t, sub, true)

	// Wait for the messages to be received.
	for i := 1; i < messageCount; i++ {
		waitMessage(t, sub, messageIDs[i], messages[i])
	}

	// Cancel the subscription.
	cancel()

	// Wait for the events.
	waitError(t, sub, "pubsub: failed while waiting for message: timeout: context already done: context canceled")
	waitSubscribed(t, sub, false)
}

func TestSubscribeParams(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := common.WithLogger(context.Background(), slogt.New(t))

	// Test cases.
	tests := []struct {
		name       string
		dbPool     *pgxpool.Pool
		topic      string
		panicValue any
	}{
		{
			name:       "dbPool is nil",
			topic:      "test",
			panicValue: "pubsub: database pool is nil",
		},
		{
			name:       "topic is empty",
			dbPool:     dbPool,
			panicValue: "pubsub: topic is empty",
		},
	}

	// Run the test cases.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a cancelable context to stop listening.
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			if tt.panicValue == nil {
				assert.NotPanics(t, func() {
					client := NewClient[TestMessage](tt.dbPool)
					client.Subscribe(ctx, tt.topic, 0)
				})
			} else {
				assert.PanicsWithValue(t, tt.panicValue, func() {
					client := NewClient[TestMessage](tt.dbPool)
					client.Subscribe(ctx, tt.topic, 0)
				})
			}
		})
	}
}

func TestSubscribeWhenAquiringConnectionFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Create the dependencies.
	expectedErr := errors.New("expected")
	var nilConn *pgxpool.Conn
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Return(nilConn, expectedErr)

	// Subscribe to the topic.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	go client.subscribe(sub, deps)

	// Wait for the error.
	waitError(t, sub, "pubsub: failed to acquire database connection: expected")

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenSubscriptionFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Create the dependencies.
	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	expectedErr := errors.New("expected")
	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Return(expectedErr)

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)

	// Subscribe to the topic.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	go client.subscribe(sub, deps)

	// Wait for the events.
	waitError(t, sub, "pubsub: failed to subscribe to topic: expected")

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenQueryingForMissedRowsFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Create the dependencies.
	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*MessageId)
		*maxMessageID = 123
	}).Return(nil)

	expectedErr := errors.New("expected")

	var nilRows *mockRows
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;", []any{"test", MessageId(1)}).Once().Return(nilRows, expectedErr)

	// Subscribe to the topic.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	sub.maxMessageID = 1
	go client.subscribe(sub, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
	waitError(t, sub, "pubsub: failed to check for missed messages: expected")
	waitSubscribed(t, sub, false)

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenScanningMissedRowFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Create the dependencies.
	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*MessageId)
		*maxMessageID = 123
	}).Return(nil)

	expectedErr := errors.New("expected")

	missedMessageRows := &mockRows{}
	missedMessageRows.On("Close").Once().Return()
	missedMessageRows.On("Next").Once().Return(true)
	missedMessageRows.On("Scan", mock.Anything).Once().Return(expectedErr)

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, "SELECT id, payload, published_at FROM pubsub_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC;", []any{"test", MessageId(1)}).Once().Return(missedMessageRows, nil)

	// Subscribe to the topic.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	sub.maxMessageID = 1
	go client.subscribe(sub, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
	waitError(t, sub, "pubsub: failed to read missed message: expected")
	waitSubscribed(t, sub, false)

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenUnmarshallingFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Create the dependencies.
	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Run(func(args mock.Arguments) {
		dest := args[0].([]interface{})
		maxMessageID := dest[0].(*MessageId)
		*maxMessageID = 0
	}).Return(nil)

	expectedErr := errors.New("expected")

	var nilNotification *pgconn.Notification
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{"test"}).Once().Return(maxMessageIDRow, nil)
	deps.On("WaitForNotification", ctx, conn).Once().Return(&pgconn.Notification{
		PID:     123,
		Channel: "test",
		Payload: `{"value": 42}`,
	}, nil)
	deps.On("UnmarshalStringJSON", `{"value": 42}`, mock.Anything).Once().Return(expectedErr)
	deps.On("WaitForNotification", ctx, conn).Once().Return(nilNotification, expectedErr)

	// Subscribe to the topic.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	sub.maxMessageID = 1
	go client.subscribe(sub, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
	waitError(t, sub, "pubsub: failed to unmarshal envelope: expected")
	waitError(t, sub, "pubsub: failed while waiting for message: expected")
	waitSubscribed(t, sub, false)

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestHandleMessageWhenInvalidPayload(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	// Create a client.
	client := NewClient[TestMessage](dbPool)

	// Handle the payload.
	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, logger, subscriptionId, topic, 0)
	go client.handlePayload(sub, 123, "invalid", time.Now())

	// Wait for the events.
	waitError(t, sub, "pubsub: failed to unmarshal message: invalid character 'i' looking for beginning of value")
}

func TestHandlePanic(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	logger := slogt.New(t)
	ctx = common.WithLogger(ctx, logger)

	tests := []struct {
		name   string
		pvalue any
		msg    string
	}{
		{
			name:   "string",
			pvalue: "test",
			msg:    "test",
		},
		{
			name:   "error",
			pvalue: errors.New("test"),
			msg:    "test",
		},
		{
			name:   "any",
			pvalue: 123,
			msg:    "panic with unexpected value: 123",
		},
	}

	const topic = "test"
	subscriptionId := SubscriptionId(uuid.NewString())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient[TestMessage](dbPool)
			sub := newSubscription(ctx, logger, subscriptionId, topic, 0)

			go func() {
				defer client.handlePanic(sub)
				panic(tt.pvalue)
			}()

			waitError(t, sub, tt.msg)
		})
	}
}

func waitSubscribed(t *testing.T, sub *Subscription, expected bool) {
	v := <-sub.Events
	switch event := v.(type) {
	case SubscriptionEvent:
		assert.Equal(t, expected, event.Subscribed)
	default:
		t.Fatalf("unexpected event type: %T", event)
	}
}

func waitMessage[T any](t *testing.T, sub *Subscription, messageId MessageId, expected T) {
	v := <-sub.Events
	switch event := v.(type) {
	case MessageEvent[T]:
		assert.Equal(t, sub.Id, event.SubscriptionId)
		assert.Equal(t, messageId, event.MessageId)
		assert.Equal(t, expected, event.Message)
	default:
		t.Fatalf("unexpected event type: %T", event)
	}
}

func waitError(t *testing.T, sub *Subscription, expectedError string) {
	v := <-sub.Events
	switch event := v.(type) {
	case ErrorEvent:
		assert.Equal(t, sub.Topic, event.Topic)
		assert.Equal(t, sub.Id, event.SubscriptionId)
		assert.Equal(t, expectedError, event.Error.Error())
	default:
		t.Fatalf("unexpected event type: %T", event)
	}
}

func waitCancelled(t *testing.T, sub *Subscription) {
	_, ok := <-sub.Events
	assert.False(t, ok)
}
