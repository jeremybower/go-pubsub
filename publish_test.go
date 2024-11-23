package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/jeremybower/go-common/postgres"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	now := postgres.Time(time.Now())
	ctx := context.Background()
	expectedTopic := "test"
	expectedMessageID, err := publish(ctx, logger, dbPool, expectedTopic, &TestMessage{Value: 42}, &standardDependencies{})
	require.NoError(t, err)
	require.Greater(t, expectedMessageID, int64(0))

	var actualMessageID int64
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

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	message := &TestMessage{Value: 42}

	deps := &mockDependencies{}
	deps.On("MarshalStringJSON", message).Once().Return("", assert.AnError)

	ctx := context.Background()
	messageID, err := publish(ctx, logger, dbPool, "test", message, deps)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)

	deps.AssertExpectations(t)
}

func TestPublishWhenQueryRowScanFails(t *testing.T) {
	t.Parallel()

	dbPool := databasePoolForTesting(t)
	defer dbPool.Close()

	logger := slogt.New(t)

	stdDeps := &standardDependencies{}

	message := &TestMessage{Value: 42}
	messageJSON, err := stdDeps.MarshalStringJSON(message)
	require.NoError(t, err)
	require.NotEmpty(t, messageJSON)

	mockRow := &mockRow{}
	mockRow.On("Scan", mock.Anything).Once().Return(assert.AnError)

	deps := &mockDependencies{}
	deps.On("MarshalStringJSON", message).Once().Return(messageJSON, nil)
	deps.On("QueryRow", context.Background(), dbPool, "INSERT INTO pubsub_messages (topic, payload) VALUES ($1, $2) RETURNING id;", []any{"test", `{"value":42}`}).Once().Return(mockRow, nil)

	ctx := context.Background()
	messageID, err := publish(ctx, logger, dbPool, "test", message, deps)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, messageID)

	deps.AssertExpectations(t)
}
