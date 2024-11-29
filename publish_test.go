package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := common.WithLogger(context.Background(), slogt.New(t))

	// Publish a message.
	now := postgres.Time(time.Now())
	expectedTopic := "test"
	expectedMessageID, err := publish(ctx, dbPool, expectedTopic, TestMessage{Value: 42}, &standardDependencies{})
	require.NoError(t, err)
	require.Greater(t, expectedMessageID, MessageId(0))

	// Verify the message was published.
	var actualMessageID MessageId
	var actualTopic string
	var actualType string
	var actualPayload string
	var actualPublishedAt time.Time
	row := dbPool.QueryRow(ctx, `SELECT id, topic, "type", payload, published_at FROM pubsub_messages WHERE id = $1;`, expectedMessageID)
	err = row.Scan(&actualMessageID, &actualTopic, &actualType, &actualPayload, &actualPublishedAt)
	require.NoError(t, err)
	assert.Equal(t, expectedMessageID, actualMessageID)
	assert.Equal(t, expectedTopic, actualTopic)
	assert.Equal(t, fullyQualifiedNameFromType[TestMessage](), actualType)
	assert.JSONEq(t, `{"value": 42}`, actualPayload)
	assert.WithinDuration(t, now, actualPublishedAt, 5*time.Second)
}

func TestPublishWhenCommonNotSetOnContext(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := context.Background()

	// Publish a message.
	const topic = "test"
	message := TestMessage{Value: 42}
	deps := &standardDependencies{}
	expectedMessageID, err := publish(ctx, dbPool, topic, message, deps)
	require.ErrorIs(t, err, common.ErrLoggerNotSet)
	assert.Zero(t, expectedMessageID)
}

func TestPublishWhenSerializationFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := common.WithLogger(context.Background(), slogt.New(t))

	// Create a message.
	message := TestMessage{Value: 42}

	// Create a marshaller that always returns an error.
	marshaller := func(v interface{}) (string, error) {
		return "", assert.AnError
	}

	// Publish a message.
	const topic = "test"
	deps := &standardDependencies{}
	opts := []publishOption{
		WithMarshaller(fullyQualifiedNameFromType[TestMessage](), marshaller),
	}
	messageID, err := publish(ctx, dbPool, topic, message, deps, opts...)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)
}

func TestPublishWhenQueryRowScanFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := common.WithLogger(context.Background(), slogt.New(t))

	// Create a message.
	message := TestMessage{Value: 42}

	// Setup mock dependencies.
	mockRow := &mockRow{}
	mockRow.On("Scan", mock.Anything).Once().Return(assert.AnError)

	deps := &mockDependencies{}
	deps.On("QueryRow", ctx, dbPool, "INSERT INTO pubsub_messages (topic, type, payload) VALUES ($1, $2, $3) RETURNING id;", []any{"test", fullyQualifiedNameFromType[TestMessage](), `{"value":42}`}).Once().Return(mockRow, nil)

	// Publish a message.
	messageID, err := publish(ctx, dbPool, "test", message, deps)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)

	// Verify the dependencies were called.
	deps.AssertExpectations(t)
}
