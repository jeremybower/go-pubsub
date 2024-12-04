package pubsub

import (
	"context"
	"mime"
	"reflect"
	"testing"
	"time"

	"github.com/jeremybower/go-common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPublishWhenContextIsNil(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Publish the message.
	assert.Panics(t, func() {
		nilContext := (context.Context)(nil)
		topicNames := h.GenerateTopicNames(1)
		Publish(nilContext, h.DBPool(), topicNames, nil, nil)
	})
}

func TestPublishWhenQuerierIsNil(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Publish the message.
	assert.Panics(t, func() {
		topicNames := h.GenerateTopicNames(1)
		Publish(h.Context(), nil, topicNames, nil, nil)
	})
}

func TestPublishWhenTopicValidationFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Generate topicNames and messages.
	topicNames := []string{}

	// Publish the message.
	h.PublishExpectingError(ErrTopicValidation, topicNames, nil, nil)
}

func TestPublishWhenValueAndEncoderAreNotBothNil(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Publish the message.
	assert.Panics(t, func() {
		topicNames := h.GenerateTopicNames(1)
		Publish(h.Context(), h.DBPool(), nil, NewJSONEncoder(), topicNames)
	})

	// Publish the message.
	assert.Panics(t, func() {
		topicNames := h.GenerateTopicNames(1)
		value := &TestValue{Value: 42}
		Publish(h.Context(), h.DBPool(), value, nil, topicNames)
	})
}

func TestPublishWhenEncoderFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Create a mock encoder.
	mockEncoder := &MockEncoder{}
	mockEncoder.On("Encode", mock.Anything, mock.Anything).Once().Return((*EncodedValue)(nil), assert.AnError)

	// Publish the message.
	topicNames := h.GenerateTopicNames(1)
	value := &TestValue{Value: 42}
	h.PublishExpectingError(assert.AnError, value, mockEncoder, topicNames)
}

func TestPublishWhenValueIsNil(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Generate topicNames and messages.
	topicName := "test"
	topicNames := []string{topicName}

	// Publish the message.
	receipt, deletedMessageCount := h.Publish(nil, nil, topicNames)
	assert.Greater(t, receipt.MessageID, MessageID(0))
	assert.Greater(t, receipt.Topics[0].ID, TopicID(0))
	assert.Equal(t, topicName, receipt.Topics[0].Name)
	assert.Nil(t, receipt.EncodedValue)
	assert.WithinDuration(t, time.Now(), receipt.PublishedAt, 5*time.Second)
	assert.Zero(t, deletedMessageCount)
}

func TestPublishWhenValueIsJSONEncoded(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Generate topicNames and messages.
	topicName := "test"
	topicNames := []string{topicName}
	value := &TestValue{Value: 42}
	fqn := fullyQualifiedName(reflect.TypeFor[TestValue]())

	// Define expected values.
	contentType := mime.FormatMediaType(JSONContentType, map[string]string{"type": fqn})
	encodedValueBytes := []byte(`{"value":42}`)

	// Publish the message.
	receipt, deletedMessageCount := h.Publish(value, NewJSONEncoder(), topicNames)
	assert.Greater(t, receipt.MessageID, MessageID(0))
	assert.Greater(t, receipt.Topics[0].ID, TopicID(0))
	assert.Equal(t, topicName, receipt.Topics[0].Name)
	assert.Equal(t, contentType, receipt.EncodedValue.ContentType)
	assert.Equal(t, encodedValueBytes, receipt.EncodedValue.Bytes)
	assert.WithinDuration(t, time.Now(), receipt.PublishedAt, 5*time.Second)
	assert.Zero(t, deletedMessageCount)
}

func TestPublishMany(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Generate topicNames and messages.
	topicNames := h.GenerateTopicNames(2)
	values := h.GenerateValues(3)

	// Publish messages.
	receipts, deletedMessageCount := h.PublishMany(values, NewJSONEncoder(), topicNames)
	assert.Equal(t, int64(0), deletedMessageCount)

	// Create the set of expected encoded messages.
	expected := h.GenerateEncodedMessages(topicNames, values, receipts)

	// Read the encoded messages.
	actual := make([]EncodedMessage, 0)
	for _, receipt := range receipts {
		actual = append(actual, h.ReadEncodedMessagesWithID(receipt.MessageID)...)
	}

	// Sort the encoded messages.
	h.SortEncodedMessages(actual)
	h.SortEncodedMessages(expected)

	// Verify the encoded messages are equal.
	assert.Equal(t, expected, actual)
}

func TestPublishWhenCommonLoggerNotSetOnContext(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Create a context without a logger.
	ctx := context.Background()

	topicNames := []string{"test"}
	receipt, deletedMessageCount, err := Publish(ctx, dbPool, nil, nil, topicNames)
	require.ErrorIs(t, err, common.ErrLoggerNotSet)
	assert.Nil(t, receipt)
	assert.Zero(t, deletedMessageCount)
}

func TestPublishWhenPublishFails(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	mockDataStore := NewMockDataStore()
	h := NewTestHarness(t, mockDataStore)
	defer h.Close()

	// Define test values.
	nilPublishedReceipt := (*PublishReceipt)(nil)
	zeroDeletedMessageCount := int64(0)

	// Register the expectations.
	topicNames := h.GenerateTopicNames(1)
	mockDataStore.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Once().
		Return(nilPublishedReceipt, zeroDeletedMessageCount, assert.AnError)

	// Publish a message.
	h.PublishExpectingError(assert.AnError, nil, nil, topicNames)

	// Verify the dependencies were called.
	mockDataStore.AssertExpectations(t)
}

func TestPublishWhenDeletedMessages(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Generate an older message.
	topicNames := h.GenerateTopicNames(1)
	publishedAt := time.Now().Add(-24 * time.Hour)
	receipt, deletedMessageCount, err := publish(h.dataStore, h.context, h.dbPool, nil, nil, topicNames, &publishedAt)
	require.NoError(t, err)
	require.Zero(t, deletedMessageCount)
	require.NotNil(t, receipt)

	// Publish a new message.
	receipt, deletedMessageCount, err = publish(h.dataStore, h.context, h.dbPool, nil, nil, topicNames, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), deletedMessageCount)
	require.NotNil(t, receipt)
}
