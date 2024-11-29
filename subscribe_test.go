package pubsub

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
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

var messageTypesForTesting = map[string]Unmarshaller{
	fullyQualifiedNameFromType[TestMessage](): UnmarshalJSON[TestMessage],
}

func TestSubscriptionOptionsWithBufferSize(t *testing.T) {
	t.Parallel()

	opts := defaultSubscribeOptions()
	WithBufferSize(10)(opts)
	assert.Equal(t, 10, opts.bufferSize)
}

func TestSubscriptionOptionWithJsonMessage(t *testing.T) {
	t.Parallel()

	opts := defaultSubscribeOptions()
	WithMessage[TestMessage]()(opts)
	fqn := fullyQualifiedNameFromType[TestMessage]()
	assert.NotNil(t, opts.unmarshallers[fqn])
}

func TestSubscriptionOptionWithStringMessage(t *testing.T) {
	t.Parallel()

	opts := defaultSubscribeOptions()
	WithMessage[string]()(opts)
	fqn := fullyQualifiedNameFromType[string]()
	assert.NotNil(t, opts.unmarshallers[fqn])
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

	// Subscribe to a topic.
	const topic = "test"
	topics := []string{topic}
	sub, err := Subscribe(ctx, dbPool, topics, WithMessage[TestMessage]())
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Wait for the subscription to be established.
	waitSubscribed(t, sub, true)

	// Publish a message.
	message := TestMessage{Value: 42}
	messageId, err := Publish(ctx, dbPool, topic, message)
	require.NoError(t, err)
	assert.Greater(t, messageId, int64(0))

	// Wait for the message to be received.
	waitMessage(t, sub, messageId, message)

	// Cancel the subscription.
	cancel()

	// Wait for the subscription to cancel.
	waitSubscribed(t, sub, false)
	waitCancelled(t, sub)
}

func TestSubscribeExcluesOtherTopics(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a cancelable context to stop listening.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a logger to the context.
	ctx = common.WithLogger(ctx, slogt.New(t))

	// Subscribe to a topic.
	const topic = "test"
	topics := []string{topic}
	sub, err := Subscribe(ctx, dbPool, topics, WithMessage[TestMessage]())
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Wait for the subscription to be established.
	waitSubscribed(t, sub, true)

	// Publish a message.
	message := TestMessage{Value: 42}
	messageId, err := Publish(ctx, dbPool, "other", message)
	require.NoError(t, err)
	assert.Greater(t, messageId, int64(0))

	// Cancel the subscription.
	cancel()

	// Wait for the subscription to cancel.
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

	// Publish messages to the database.
	const topic = "test"
	topics := []string{topic}
	const messageCount = 3
	var messageIDs []MessageId
	var messages []TestMessage
	for i := 0; i < messageCount; i++ {
		message := TestMessage{Value: int(rand.Int63n(2 ^ 31))}
		messageID, err := Publish(ctx, dbPool, topic, message)
		require.NoError(t, err)
		assert.Greater(t, messageID, int64(0))

		messageIDs = append(messageIDs, messageID)
		messages = append(messages, message)
	}

	// Listen for messages.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.maxMessageID = messageIDs[0]
	go subscribe(sub, topics, &standardDependencies{})

	// Wait for the subscription to be established.
	waitSubscribed(t, sub, true)

	// Wait for the messages to be received.
	for i := 1; i < messageCount; i++ {
		waitMessage(t, sub, messageIDs[i], messages[i])
	}

	// Cancel the subscription.
	cancel()

	// Wait for the events.
	waitSubscribed(t, sub, false)
}

func TestSubscribeWhenContextIsNil(t *testing.T) {
	t.Parallel()

	// Create a nil context.
	var nilCtx context.Context

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Setup the topics.
	topics := []string{"test"}

	// Test that Subscribe panics when the context is nil.
	assert.PanicsWithValue(t, "pubsub: context is nil", func() {
		Subscribe(nilCtx, dbPool, topics)
	})
}

func TestSubscribeWhenDatabasePoolIsNil(t *testing.T) {
	t.Parallel()

	// Set the context.
	ctx := context.Background()

	// Create a nil database pool.
	var nilDbPool *pgxpool.Pool

	// Setup the topics.
	topics := []string{"test"}

	// Test that Subscribe panics when the database pool is nil.
	assert.PanicsWithValue(t, "pubsub: database pool is nil", func() {
		Subscribe(ctx, nilDbPool, topics)
	})
}

func TestSubscribeWhenTopicIsTooLong(t *testing.T) {
	t.Parallel()

	// Create a context.
	ctx := context.Background()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Setup the topics.
	topics := []string{strings.Repeat("a", 129)}

	// Test that Subscribe panics when the topic is too long.
	sub, err := Subscribe(ctx, dbPool, topics)
	assert.Nil(t, sub)
	assert.ErrorContains(t, err, "pubsub: invalid topic (length >= 128)")
}

func TestSubscribeWhenLoggerNotSetOnContext(t *testing.T) {
	t.Parallel()

	// Create a context.
	ctx := context.Background()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Setup the topics.
	topics := []string{"test"}

	// Test that Subscribe panics when the logger is not set on the context.
	sub, err := Subscribe(ctx, dbPool, topics)
	assert.Nil(t, sub)
	assert.ErrorIs(t, err, common.ErrLoggerNotSet)
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

	// Create the dependencies.
	expectedErr := errors.New("expected")
	var nilConn *pgxpool.Conn
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Return(nilConn, expectedErr)

	// Create an error handler that will notify a condition.
	cond := sync.NewCond(&sync.Mutex{})
	receivedErr := false
	errorHandler := func(err error) {
		cond.L.Lock()
		defer cond.L.Unlock()
		assert.ErrorIs(t, err, expectedErr)
		receivedErr = true
		cond.Broadcast()
	}

	// Subscribe to the topic.
	const topic = "test"
	topics := []string{topic}
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.errorHandler = errorHandler
	go subscribe(sub, topics, deps)

	// Wait for the error handler to be called.
	cond.L.Lock()
	if !receivedErr {
		cond.Wait()
	}
	cond.L.Unlock()

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

	// Create the dependencies.
	stdDeps := &standardDependencies{}
	conn, err := stdDeps.Acquire(ctx, dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	expectedErr := errors.New("expected")
	maxMessageIDRow := &mockRow{}
	maxMessageIDRow.On("Scan", mock.Anything).Once().Return(expectedErr)

	const topic = "test"
	topics := []string{topic}

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{topics}).Once().Return(maxMessageIDRow, nil)

	// Create an error handler that will notify a condition.
	cond := sync.NewCond(&sync.Mutex{})
	receivedErr := false
	errorHandler := func(err error) {
		cond.L.Lock()
		defer cond.L.Unlock()
		assert.ErrorIs(t, err, expectedErr)
		receivedErr = true
		cond.Broadcast()
	}

	// Subscribe to the topic.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.errorHandler = errorHandler
	go subscribe(sub, topics, deps)

	// Wait for the error handler to be called.
	cond.L.Lock()
	if !receivedErr {
		cond.Wait()
	}
	cond.L.Unlock()

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

	const topic = "test"
	topics := []string{topic}

	var nilRows *mockRows
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, `SELECT max_message_id FROM pubsub_subscribe($1);`, []any{topics}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, `SELECT id, topic, "type", payload, published_at FROM pubsub_messages WHERE topic = ANY($1) AND id > $2 ORDER BY id ASC;`, []any{topics, MessageId(1)}).Once().Return(nilRows, expectedErr)

	// Subscribe to the topic.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.maxMessageID = 1
	go subscribe(sub, topics, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
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

	const topic = "test"
	topics := []string{topic}

	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, `SELECT max_message_id FROM pubsub_subscribe($1);`, []any{topics}).Once().Return(maxMessageIDRow, nil)
	deps.On("Query", ctx, conn, `SELECT id, topic, "type", payload, published_at FROM pubsub_messages WHERE topic = ANY($1) AND id > $2 ORDER BY id ASC;`, []any{topics, MessageId(1)}).Once().Return(missedMessageRows, nil)

	// Subscribe to the topic.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.maxMessageID = 1
	go subscribe(sub, topics, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
	waitSubscribed(t, sub, false)

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenUnmarshallingEnvelopeFails(t *testing.T) {
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

	const topic = "test"
	topics := []string{topic}

	var nilNotification *pgconn.Notification
	deps := &mockDependencies{}
	deps.On("Acquire", ctx, dbPool).Once().Return(conn, nil)
	deps.On("QueryRow", ctx, conn, "SELECT max_message_id FROM pubsub_subscribe($1);", []any{topics}).Once().Return(maxMessageIDRow, nil)
	deps.On("WaitForNotification", ctx, conn).Once().Return(&pgconn.Notification{
		PID:     123,
		Channel: topic,
		Payload: `{"value": 42}`,
	}, nil)
	deps.On("UnmarshalStringJSON", `{"value": 42}`, mock.Anything).Once().Return(expectedErr)
	deps.On("WaitForNotification", ctx, conn).Once().Return(nilNotification, expectedErr)

	// Subscribe to the topic.
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, messageTypesForTesting)
	sub.maxMessageID = 1
	go subscribe(sub, topics, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)
	waitSubscribed(t, sub, false)

	// Assert that the dependencies were called as expected.
	deps.AssertExpectations(t)
}

func TestSubscribeWhenUnmarshallingMessageFails(t *testing.T) {
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

	// Create the subscription.
	expectedErr := errors.New("expected")
	subscriptionId := SubscriptionId(uuid.NewString())
	fqn := fullyQualifiedNameFromType[TestMessage]()
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, map[string]Unmarshaller{
		fqn: func(payload string) (any, error) {
			return nil, expectedErr
		},
	})

	// Create an error handler that will notify a condition.
	cond := sync.NewCond(&sync.Mutex{})
	receivedErr := false
	errorHandler := func(err error) {
		if !receivedErr {
			cond.L.Lock()
			defer cond.L.Unlock()

			assert.ErrorIs(t, err, expectedErr)
			receivedErr = true
			cond.Broadcast()
		}
	}

	// Subscribe to the topic.
	const topic = "test"
	topics := []string{topic}
	deps := &standardDependencies{}
	sub.errorHandler = errorHandler
	go subscribe(sub, topics, deps)

	// Wait for the events.
	waitSubscribed(t, sub, true)

	// Publish a message.
	message := TestMessage{Value: 42}
	messageId, err := Publish(ctx, dbPool, topic, message)
	require.NoError(t, err)
	assert.Greater(t, messageId, int64(0))

	// Assert that the error handler was called.
	cond.L.Lock()
	if !receivedErr {
		cond.Wait()
	}
	cond.L.Unlock()

	// Cancel the subscription.
	cancel()

	// Wait for the subscription to be cancelled.
	waitSubscribed(t, sub, false)
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

	// Handle the payload.
	const topic = "test"
	const messageId = MessageId(123)
	const payload = "invalid"
	messageType := fullyQualifiedNameFromType[TestMessage]()
	publishedAt := time.Now()
	subscriptionId := SubscriptionId(uuid.NewString())
	sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, map[string]Unmarshaller{
		messageType: UnmarshalJSON[TestMessage],
	})
	go handlePayload(sub, topic, messageId, messageType, payload, publishedAt)
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

	subscriptionId := SubscriptionId(uuid.NewString())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := newSubscription(ctx, dbPool, logger, subscriptionId, 0, map[string]Unmarshaller{})

			go func() {
				defer handlePanic(sub)
				panic(tt.pvalue)
			}()
		})
	}
}

func TestValidateTopicsWhenEmpty(t *testing.T) {
	t.Parallel()

	topics, err := validateTopics([]string{})
	require.NoError(t, err)
	assert.Empty(t, topics)
}

func TestValidateTopicsWhenWhitespace(t *testing.T) {
	t.Parallel()

	topics, err := validateTopics([]string{" ", "test1   ", "   test2", " "})
	require.NoError(t, err)
	assert.Equal(t, []string{"test1   ", "   test2"}, topics)
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

func waitMessage(t *testing.T, sub *Subscription, messageId MessageId, expected any) {
	v := <-sub.Events
	switch event := v.(type) {
	case MessageEvent:
		assert.Equal(t, sub.Id, event.SubscriptionId)
		assert.Equal(t, messageId, event.MessageId)
		assert.Equal(t, expected, event.Message)
	default:
		t.Fatalf("unexpected event type: %T", event)
	}
}

func waitCancelled(t *testing.T, sub *Subscription) {
	_, ok := <-sub.Events
	assert.False(t, ok)
}
