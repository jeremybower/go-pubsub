package pubsub

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func TestSubscribe(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Subscribe to a topic.
	topicNames := h.GenerateTopicNames(1)
	sub := h.Subscribe(topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
	defer sub.Close()

	// Wait until subscribed before publishing.
	sub.WaitUntilSubscribed(h.Context())

	// Publish a message.
	value := &TestValue{Value: 42}
	receipt := h.Publish(value, NewJSONEncoder(), topicNames)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	h.WaitConsumeMessageEventID(sub, receipt.MessageID, func(event MessageEvent) {
		AssertMessageEventForReceipt(t, event, receipt)
	})
	sub.Close()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeStatusEventClosed(sub)
	AssertNoQueuedEvents(t, sub)
}

func TestSubscribeWhenTopicValidationFails(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Subscribe to an invalid topic.
	topicNames := []string{""}
	h.SubscribeExpectingError(ErrTopicValidation, topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
}

func TestSubscribeExcludesOtherTopics(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Subscribe to a topic.
	topicNames := h.GenerateTopicNames(1)
	sub := h.Subscribe(topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
	defer sub.Close()

	// Wait until subscribed before publishing.
	sub.WaitUntilSubscribed(h.Context())

	// Publish a message.
	otherTopicNames := h.GenerateTopicNames(1)
	value := &TestValue{Value: 42}
	h.Publish(value, NewJSONEncoder(), otherTopicNames)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	sub.Close()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeStatusEventClosed(sub)
	AssertNoQueuedEvents(t, sub)
}

func TestSubscribeWhenMissedMessages(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Publish messages.
	topicNames := h.GenerateTopicNames(1)
	values := h.GenerateValues(10)
	receipts := h.PublishMany(values, NewJSONEncoder(), topicNames)

	// Restart after the first message.
	restartAtMessageID := receipts[0].MessageID

	// Subscribe to a topic.
	sub := h.Subscribe(
		topicNames,
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	sub.restartAtMessageID = restartAtMessageID
	defer sub.Close()

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	for _, receipt := range receipts[1:] {
		h.WaitConsumeMessageEventID(sub, receipt.MessageID, func(event MessageEvent) {
			AssertMessageEventForReceipt(t, event, receipt)
		})
	}
	sub.Close()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeStatusEventClosed(sub)
	AssertNoQueuedEvents(t, sub)
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

func TestSubscribeWhenStoppedAfterSubscribed(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Subscribe to a topic.
	topicNames := h.GenerateTopicNames(1)
	sub := h.Subscribe(topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
	defer sub.Close()

	// Wait until subscribed before publishing.
	sub.WaitUntilSubscribed(h.Context())

	// Publish a message.
	value := &TestValue{Value: 42}
	receipt := h.Publish(value, NewJSONEncoder(), topicNames)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	h.WaitConsumeMessageEventID(sub, receipt.MessageID, func(event MessageEvent) {
		AssertMessageEventForReceipt(t, event, receipt)
	})
	sub.Close()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeStatusEventClosed(sub)
	AssertNoQueuedEvents(t, sub)
}

func TestSubscribeWhenSubscriptionDelayed(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Define test values.
	nilConn := (*pgxpool.Conn)(nil)

	// Declare expected mock calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Return(nilConn, assert.AnError)

	// Subscribe to a topic.
	topicNames := h.GenerateTopicNames(1)
	sub := h.Subscribe(topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
	defer sub.Close()

	// Assert the events.
	h.WaitConsumeErrorEventIs(sub, assert.AnError)
	h.WaitConsumeStatusEventDelayed(sub)
	h.WaitConsumeErrorEventIs(sub, assert.AnError)
	sub.Close()
	h.WaitConsumeStatusEventClosed(sub)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscribeWhenSubscriptionCancelled(t *testing.T) {
	t.Parallel()

	// Create a client test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Subscribe to a topic.
	ctx, cancel := context.WithCancel(h.Context())
	topicNames := h.GenerateTopicNames(1)
	sub, err := Subscribe(ctx, h.DBPool(), topicNames, NewJSONDecoder(reflect.TypeOf(TestValue{})))
	require.NoError(t, err)
	defer sub.Close()

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	cancel()
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	AssertNoQueuedEvents(t, sub)
}

func TestValidateTopicsWhenEmpty(t *testing.T) {
	t.Parallel()

	err := validateTopics([]string{})
	require.ErrorIs(t, err, ErrTopicValidation)
}

func TestValidateTopicsWhenBlank(t *testing.T) {
	t.Parallel()

	err := validateTopics([]string{""})
	require.ErrorIs(t, err, ErrTopicValidation)
}

func TestValidateTopicsWhenWhitespace(t *testing.T) {
	t.Parallel()

	err := validateTopics([]string{" "})
	require.ErrorIs(t, err, ErrTopicValidation)
}
