package pubsub

import (
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/optional"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func ErrorHandlerForTesting(ctx context.Context, logger *slog.Logger, err error) error {
	return err
}

func MessageHandlerForTesting(ctx context.Context, logger *slog.Logger, topic Topic, message Message) (*Message, error) {
	return &message, nil
}

func TestSubscriptionEvent(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore(), NewJSONDecoder())
	defer sub.Close()

	// Generate a subscribed event.
	sub.handleSubscribed(h.Context(), h.Logger(), true)

	// Get the event.
	event := sub.Event()
	assert.Equal(t, StatusEvent{
		SubscriptionID: sub.id,
		Subscribed:     optional.NewValue(true),
	}, event)
}

func TestSubscriptionWaitForEventWhenImmediate(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore(), NewJSONDecoder())
	defer sub.Close()

	// Generate a subscribed event.
	sub.handleSubscribed(h.Context(), h.Logger(), true)

	// Get the event.
	event, err := sub.WaitForEvent(h.Context())
	assert.NoError(t, err)
	assert.Equal(t, StatusEvent{
		SubscriptionID: sub.id,
		Subscribed:     optional.NewValue(true),
	}, event)
}

func TestSubscriptionWaitForEventWhenBlocks(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore(), NewJSONDecoder())
	defer sub.Close()

	// Get the event.
	waiting := make(chan bool)
	completed := make(chan bool)
	go func() {
		event, err := sub.WaitForEvent(h.Context(), waiting)
		assert.NoError(t, err)
		assert.Equal(t, StatusEvent{
			SubscriptionID: sub.id,
			Subscribed:     optional.NewValue(true),
		}, event)
		completed <- true
	}()

	// Generate a subscribed event.
	<-waiting
	sub.handleSubscribed(h.Context(), h.Logger(), true)
	<-completed
}

func TestSubscriptionWaitUntilSubscribed(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore(), NewJSONDecoder())
	defer sub.Close()

	// Generate a subscribed event.
	sub.handleSubscribed(h.Context(), h.Logger(), true)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
}

func TestSubscriptionWaitUntilSubscribedWhenContextIsCancelled(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore(), NewJSONDecoder())
	defer sub.Close()

	// Create a context that is cancelled.
	ctx, cancel := context.WithCancel(h.Context())
	cancel()

	// Wait until the subscription is subscribed.
	err := sub.WaitUntilSubscribed(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSubscriptionHandleEncodedMessage(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	defer sub.Close()
	// Create an encoded value.
	value := &TestValue{Value: 42}
	encodedValue, err := NewJSONEncoder().Encode(h.Context(), value)
	require.NoError(t, err)
	require.NotNil(t, encodedValue)

	// Create an encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    1,
		Topic:        Topic{ID: 1, Name: "name"},
		EncodedValue: encodedValue,
		PublishedAt:  time.Now(),
	}

	// Handle the encoded message.
	message, err := sub.handleEncodedMessage(
		h.Context(),
		h.Logger(),
		encodedMessage,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.NoError(t, err)
	assert.NotNil(t, message)
}

func TestSubscriptionHandleEncodedMessageWhenEncodedValueIsNil(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	defer sub.Close()

	// Create an encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    1,
		Topic:        Topic{ID: 1, Name: "name"},
		EncodedValue: nil,
		PublishedAt:  time.Now(),
	}

	// Handle the encoded message.
	message, err := sub.handleEncodedMessage(
		h.Context(),
		h.Logger(),
		encodedMessage,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.NoError(t, err)
	assert.NotNil(t, message)
}

func TestSubscriptionHandleEncodedMessageWhenContentTypeIsInvalid(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	defer sub.Close()

	// Create an encoded value.
	encodedValue := &EncodedValue{
		ContentType: "application/json; type=invalid=invalid",
		Bytes:       []byte(`{"Value":42}`),
	}

	// Create an encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    1,
		Topic:        Topic{ID: 1, Name: "name"},
		EncodedValue: encodedValue,
		PublishedAt:  time.Now(),
	}

	// Generate a message event.
	message, err := sub.handleEncodedMessage(
		h.Context(), h.Logger(), encodedMessage,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.Error(t, err)
	assert.Nil(t, message)
}

func TestSubscriptionHandleEncodedMessageWhenDecoderNotRegistered(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore())
	defer sub.Close()

	// Create an encoded value.
	value := &TestValue{Value: 42}
	encodedValue, err := NewJSONEncoder().Encode(h.Context(), value)
	require.NoError(t, err)
	require.NotNil(t, encodedValue)

	// Create an encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    1,
		Topic:        Topic{ID: 1, Name: "name"},
		EncodedValue: encodedValue,
		PublishedAt:  time.Now(),
	}

	// Generate a message event.
	message, err := sub.handleEncodedMessage(
		h.Context(), h.Logger(), encodedMessage,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, ErrDecoderNotRegistered)
	assert.Nil(t, message)
}

func TestSubscriptionHandleEncodedMessageWhenDecoderUnknownType(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(),
	)
	defer sub.Close()

	// Create an encoded value.
	value := &TestValue{Value: 42}
	encodedValue, err := NewJSONEncoder().Encode(h.Context(), value)
	require.NoError(t, err)
	require.NotNil(t, encodedValue)

	// Create an encoded message.
	encodedMessage := &EncodedMessage{
		MessageID:    1,
		Topic:        Topic{ID: 1, Name: "name"},
		EncodedValue: encodedValue,
		PublishedAt:  time.Now(),
	}

	// Generate a message event.
	message, err := sub.handleEncodedMessage(
		h.Context(), h.Logger(), encodedMessage,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, ErrTypeUnknown)
	assert.Nil(t, message)
}

func TestSubscriptionHandleError(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(),
	)
	defer sub.Close()

	// Handle the error.
	err := sub.handleError(h.Context(), h.Logger(), assert.AnError)
	assert.NoError(t, err)

	// Assert the events.
	h.WaitConsumeErrorEventIs(sub, assert.AnError)
}

func TestSubscriptionHandleMessageNotification(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	defer sub.Close()

	// Register a topic.
	topicName := "test"
	sub.topicCache.set(TopicID(1), Topic{ID: TopicID(1), Name: topicName})

	// Create a notification.
	notification := &MessageNotification{
		MessageID:   1,
		HasValue:    false,
		PublishedAt: time.Now(),
	}

	// Generate a message event.
	channel := TopicChannelPrefix + "1"
	message, err := sub.handleMessageNotification(
		h.Context(),
		h.Logger(),
		channel,
		notification,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.NoError(t, err)
	assert.NotNil(t, message)
}

func TestSubscriptionHandleMessageNotificationWhenTopicUnknown(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		NewPostgresDataStore(),
		NewJSONDecoder(reflect.TypeOf(TestValue{})),
	)
	defer sub.Close()

	// Create a notification.
	notification := &MessageNotification{
		MessageID:   1,
		HasValue:    false,
		PublishedAt: time.Now(),
	}

	// Generate a message event.
	channel := TopicChannelPrefix + "1"
	message, err := sub.handleMessageNotification(
		h.Context(),
		h.Logger(),
		channel,
		notification,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, ErrTopicUnknown)
	assert.Nil(t, message)
}

func TestSubscriptionHandleMessageNotificationWhenReadEncodedValueFails(t *testing.T) {
	t.Parallel()

	// Define expected values.
	nilEncodedValue := (*EncodedValue)(nil)

	// Create a mock data store.
	mockDataStore := NewMockDataStore()
	mockDataStore.On("ReadEncodedValue", mock.Anything, mock.Anything, mock.Anything).Once().Return(nilEncodedValue, assert.AnError)

	// Create a test harness.
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(
		h.dbPool,
		nil,
		mockDataStore,
	)
	defer sub.Close()

	// Register a topic.
	topicName := "test"
	sub.topicCache.set(TopicID(1), Topic{ID: TopicID(1), Name: topicName})

	// Create a notification.
	notification := &MessageNotification{
		MessageID:   1,
		HasValue:    true,
		PublishedAt: time.Now(),
	}

	// Generate a message event.
	channel := TopicChannelPrefix + "1"
	message, err := sub.handleMessageNotification(
		h.Context(),
		h.Logger(),
		channel,
		notification,
		MessageHandlerForTesting,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, message)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenDataStoreAcquireConnectionFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Define test values.
	nilConnection := (*pgxpool.Conn)(nil)

	// Register expected mock method calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Once().Return(nilConnection, assert.AnError)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	defer sub.Close()

	// Subscribe to the topic.
	topicName := "test"
	err := sub.subscribe(
		h.Context(),
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenDataStoreSubscribeFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Acquire a connection to the database.
	postgresDataStore := NewPostgresDataStore()
	conn, err := postgresDataStore.AcquireConnection(h.Context(), h.dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Define test values.
	nilReceipt := (*SubscribeReceipt)(nil)

	// Register expected mock method calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Once().Return(conn, nil)
	mockDataStore.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Once().Return(nilReceipt, assert.AnError)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	defer sub.Close()

	// Subscribe to the topic.
	topicName := "test"
	err = sub.subscribe(
		h.Context(),
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenDataStoreReadEncodedMessagesAfterIDFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Acquire a connection to the database.
	postgresDataStore := NewPostgresDataStore()
	conn, err := postgresDataStore.AcquireConnection(h.Context(), h.dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Define test values.
	topicName := "test"
	topicID := TopicID(1)
	topic := Topic{ID: topicID, Name: topicName}

	receipt := &SubscribeReceipt{
		MaxMessageID: 2,
		Topics:       []Topic{topic},
	}

	nilChannel := make(chan common.Result[EncodedMessage])

	// Register expected mock method calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Once().Return(conn, nil)
	mockDataStore.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Once().Return(receipt, nil)
	mockDataStore.On("ReadEncodedMessagesAfterID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(nilChannel, assert.AnError)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	sub.restartAtMessageID = 1
	defer sub.Close()

	// Subscribe to the topic.
	err = sub.subscribe(
		h.Context(),
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	h.WaitConsumeStatusEventSubscribed(sub, false)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenDataStoreReadEncodedMessagesChannelFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Acquire a connection to the database.
	postgresDataStore := NewPostgresDataStore()
	conn, err := postgresDataStore.AcquireConnection(h.Context(), h.dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Define test values.
	topicName := "test"
	topicID := TopicID(1)
	topic := Topic{ID: topicID, Name: topicName}

	receipt := &SubscribeReceipt{
		MaxMessageID: 2,
		Topics:       []Topic{topic},
	}

	ch := make(chan common.Result[EncodedMessage], 1)
	ch <- common.Result[EncodedMessage]{Error: assert.AnError}
	defer close(ch)

	// Register expected mock method calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Once().Return(conn, nil)
	mockDataStore.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Once().Return(receipt, nil)
	mockDataStore.On("ReadEncodedMessagesAfterID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(ch, nil)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	sub.restartAtMessageID = 1
	defer sub.Close()

	// Subscribe to the topic.
	err = sub.subscribe(
		h.Context(),
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	h.WaitConsumeStatusEventSubscribed(sub, false)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenDataStoreWaitForNotificationFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Acquire a connection to the database.
	postgresDataStore := NewPostgresDataStore()
	conn, err := postgresDataStore.AcquireConnection(h.Context(), h.dbPool)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Define test values.
	topicName := "test"
	topicID := TopicID(1)
	topic := Topic{ID: topicID, Name: topicName}

	receipt := &SubscribeReceipt{
		MaxMessageID: 0,
		Topics:       []Topic{topic},
	}

	nilNotification := (*pgconn.Notification)(nil)

	// Register expected mock method calls.
	mockDataStore.On("AcquireConnection", mock.Anything, mock.Anything).Once().Return(conn, nil)
	mockDataStore.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Once().Return(receipt, nil)
	mockDataStore.On("WaitForNotification", mock.Anything, mock.Anything).Once().Return(nilNotification, assert.AnError)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	defer sub.Close()

	// Subscribe to the topic.
	go sub.subscribe(
		h.Context(),
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		sub.handleError,
	)

	// Assert the events.
	h.WaitConsumeStatusEventSubscribed(sub, true)
	h.WaitConsumeErrorEventIs(sub, assert.AnError)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	AssertNoQueuedEvents(t, sub)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestSubscriptionSubscribeWhenCancelled(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, NewPostgresDataStore())
	defer sub.Close()

	// Create a context to cancel.
	ctx, cancel := context.WithCancel(h.Context())

	// Subscribe to the topic.
	topicName := "test"
	go sub.subscribe(
		ctx,
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		sub.handleError,
	)

	h.WaitConsumeStatusEventSubscribed(sub, true)
	cancel()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	AssertNoQueuedEvents(t, sub)
}

func TestSubscriptionSubscribeWhenClosed(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create a context to cancel.
	ctx, cancel := context.WithCancel(h.Context())

	// Create the subscription.
	sub := newSubscription(h.dbPool, cancel, NewPostgresDataStore())
	defer sub.Close()

	// Subscribe to the topic.
	topicName := "test"
	go sub.subscribe(
		ctx,
		h.Logger(),
		[]string{topicName},
		sub.handleSubscribed,
		sub.handleMessage,
		sub.handleError,
	)

	h.WaitConsumeStatusEventSubscribed(sub, true)
	sub.Close()
	h.WaitConsumeErrorEventIs(sub, context.Canceled)
	h.WaitConsumeStatusEventSubscribed(sub, false)
	h.WaitConsumeStatusEventClosed(sub)
	AssertNoQueuedEvents(t, sub)
}

func TestSubscriptionHandleNotificationWhenUnmarshalMessageNotificationFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Define expected values.
	nilNotification := (*MessageNotification)(nil)

	// Register expected mock method calls.
	mockDataStore.On("UnmarshalMessageNotification", mock.Anything).Once().Return(nilNotification, assert.AnError)

	// Create the subscription.
	sub := newSubscription(h.dbPool, nil, mockDataStore)
	defer sub.Close()
	// Create the notification.
	n := &pgconn.Notification{
		Channel: TopicChannelPrefix + "1",
		Payload: `{"message_id":1,"has_value":false,"published_at":"2024-01-01T00:00:00Z"}`,
	}

	// Handle the notification.
	err := sub.handleNotification(
		h.Context(),
		h.Logger(),
		n,
		sub.handleMessage,
		ErrorHandlerForTesting,
	)
	assert.ErrorIs(t, err, assert.AnError)

	// Assert the mock calls.
	mockDataStore.AssertExpectations(t)
}

func TestTopicForChannelWhenCacheIsNil(t *testing.T) {
	t.Parallel()

	cache := newCache[TopicID, Topic]()
	cache.set(TopicID(1), Topic{ID: TopicID(1), Name: "foo"})

	topic, err := topicForChannel(TopicChannelPrefix+"1", cache)
	assert.NoError(t, err)
	assert.NotNil(t, topic)
}

func TestTopicForChannelWhenTopicInvalidChannelPrefix(t *testing.T) {
	t.Parallel()

	cache := newCache[TopicID, Topic]()
	topic, err := topicForChannel("foo", cache)
	assert.ErrorIs(t, err, ErrTopicInvalidChannelPrefix)
	assert.Nil(t, topic)
}

func TestTopicForChannelWhenTopicInvalidID(t *testing.T) {
	t.Parallel()

	cache := newCache[TopicID, Topic]()
	topic, err := topicForChannel(TopicChannelPrefix+"foo", cache)
	assert.ErrorIs(t, err, ErrTopicInvalidID)
	assert.Nil(t, topic)
}

func TestTopicForChannelWhenTopicUnknown(t *testing.T) {
	t.Parallel()

	cache := newCache[TopicID, Topic]()
	topic, err := topicForChannel(TopicChannelPrefix+"1", cache)
	assert.ErrorIs(t, err, ErrTopicUnknown)
	assert.Nil(t, topic)
}
