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
	var actualPayload string
	var actualPublishedAt time.Time
	row := dbPool.QueryRow(ctx, "SELECT id, topic, payload, published_at FROM pubsub_messages WHERE id = $1;", expectedMessageID)
	err = row.Scan(&actualMessageID, &actualTopic, &actualPayload, &actualPublishedAt)
	require.NoError(t, err)
	assert.Equal(t, expectedMessageID, actualMessageID)
	assert.Equal(t, expectedTopic, actualTopic)
	assert.JSONEq(t, `{"value": 42}`, actualPayload)
	assert.WithinDuration(t, now, actualPublishedAt, 5*time.Second)
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

	// Setup the mock dependencies.
	deps := &mockDependencies{}
	deps.On("MarshalStringJSON", message).Once().Return("", assert.AnError)

	// Publish a message.
	messageID, err := publish(ctx, dbPool, "test", message, deps)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)

	// Verify the dependencies were called.
	deps.AssertExpectations(t)
}

func TestPublishWhenQueryRowScanFails(t *testing.T) {
	t.Parallel()

	// Create a database pool for testing.
	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	// Add a logger to the context.
	ctx := common.WithLogger(context.Background(), slogt.New(t))

	// Setup the mock dependencies.
	stdDeps := &standardDependencies{}

	// Create a message.
	message := TestMessage{Value: 42}

	// Setup mock dependencies.
	messageJSON, err := stdDeps.MarshalStringJSON(message)
	require.NoError(t, err)
	require.NotEmpty(t, messageJSON)

	mockRow := &mockRow{}
	mockRow.On("Scan", mock.Anything).Once().Return(assert.AnError)

	deps := &mockDependencies{}
	deps.On("MarshalStringJSON", message).Once().Return(messageJSON, nil)
	deps.On("QueryRow", ctx, dbPool, "INSERT INTO pubsub_messages (topic, payload) VALUES ($1, $2) RETURNING id;", []any{"test", `{"value":42}`}).Once().Return(mockRow, nil)

	// Publish a message.
	messageID, err := publish(ctx, dbPool, "test", message, deps)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)

	// Verify the dependencies were called.
	deps.AssertExpectations(t)
}
