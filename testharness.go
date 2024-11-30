package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/url"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/env"
	"github.com/jeremybower/go-pubsub/v2/db"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestValue struct {
	Value int64 `json:"value"`
}

type TestHarness struct {
	t         testing.TB
	context   context.Context
	cancel    context.CancelFunc
	client    *Client
	closed    bool
	dataStore DataStore
	dbPool    *pgxpool.Pool
	logger    *slog.Logger
	mutex     sync.Mutex
}

func NewTestHarness(
	t *testing.T,
	dataStore DataStore,
) *TestHarness {
	logger := slogt.New(t)
	ctx := common.WithLogger(context.Background(), logger)
	ctx, cancel := context.WithCancel(ctx)
	client := NewClient(WithDataStore(dataStore))
	dbPool := databasePoolForTesting(t)

	return &TestHarness{
		t:         t,
		context:   ctx,
		cancel:    cancel,
		client:    client,
		closed:    false,
		dataStore: dataStore,
		dbPool:    dbPool,
		logger:    logger,
	}
}

func (h *TestHarness) Client() *Client {
	return h.client
}

func (h *TestHarness) Close() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.closed {
		h.closed = true
		h.dbPool.Close()
		h.cancel()
	}
}

func (h *TestHarness) Context() context.Context {
	return h.context
}

func (h *TestHarness) DataStore() DataStore {
	return h.dataStore
}

func (h *TestHarness) DBPool() *pgxpool.Pool {
	return h.dbPool
}

func (h *TestHarness) GenerateEncodedMessages(
	topicNames []string,
	values []any,
	receipts []*PublishReceipt,
) []EncodedMessage {
	expected := make([]EncodedMessage, len(values)*len(topicNames))
	for receiptIDx, receipt := range receipts {
		for topicIDx, topicName := range topicNames {
			require.Equal(h.t, topicName, receipt.Topics[topicIDx].Name)
			expected[receiptIDx*len(topicNames)+topicIDx] = EncodedMessage{
				MessageID:    receipt.MessageID,
				Topic:        receipt.Topics[topicIDx],
				EncodedValue: receipt.EncodedValue,
				PublishedAt:  receipt.PublishedAt,
			}
		}
	}
	return expected
}

func (h *TestHarness) GenerateTopicNames(n int) []string {
	topicNames := make([]string, 0, n)
	for i := 0; len(topicNames) < n; i++ {
		// Extremely unlikely to collide, but check for duplicates anyway.
		id := rand.Int64N(math.MaxInt64) + 1
		topic := fmt.Sprintf("/topics/%d", id)
		if !slices.Contains(topicNames, topic) {
			topicNames = append(topicNames, topic)
		}
	}
	return topicNames
}

func (h *TestHarness) GenerateValues(n int) []any {
	messages := make([]any, n)
	for i := 0; i < n; i++ {
		id := rand.Int64N(math.MaxInt64) + 1
		messages[i] = &TestValue{Value: id}
	}
	return messages
}

func (h *TestHarness) Logger() *slog.Logger {
	return h.logger
}

func (h *TestHarness) Publish(
	topicNames []string,
	v any,
	encoder Encoder,
) *PublishReceipt {
	receipt, err := h.client.Publish(h.context, h.dbPool, topicNames, v, encoder)
	require.NotNil(h.t, receipt)
	require.NoError(h.t, err)
	return receipt
}

func (h *TestHarness) PublishMany(
	topicNames []string,
	values []any,
	encoder Encoder,
) []*PublishReceipt {
	receipts := make([]*PublishReceipt, len(values))
	for i, value := range values {
		receipts[i] = h.Publish(topicNames, value, encoder)
	}
	return receipts
}

func (h *TestHarness) PublishExpectingError(
	expectedErr error,
	topicNames []string,
	v any,
	encoder Encoder,
) {
	receipt, err := h.client.Publish(h.context, h.dbPool, topicNames, v, encoder)
	require.Nil(h.t, receipt)
	require.ErrorIs(h.t, err, expectedErr)
}

func (h *TestHarness) SortEncodedMessages(m []EncodedMessage) {
	slices.SortFunc(m, func(a, b EncodedMessage) int {
		// Sort by message id then topic id.
		if a.MessageID != b.MessageID {
			return int(a.MessageID - b.MessageID)
		}
		return int(a.Topic.ID - b.Topic.ID)
	})
}

func (h *TestHarness) Subscribe(
	topicNames []string,
	opts ...SubscribeOption,
) *Subscription {
	sub, err := h.client.Subscribe(h.context, h.dbPool, topicNames, opts...)
	require.NotNil(h.t, sub)
	require.NoError(h.t, err)
	return sub
}

func (h *TestHarness) SubscribeExpectingError(
	expectedErr error,
	topicNames []string,
	opts ...SubscribeOption,
) {
	sub, err := h.client.Subscribe(h.context, h.dbPool, topicNames, opts...)
	require.Nil(h.t, sub)
	require.ErrorIs(h.t, err, expectedErr)
}

func (h *TestHarness) ReadEncodedMessagesWithID(
	messageID MessageID,
) []EncodedMessage {
	ch, err := h.dataStore.ReadEncodedMessagesWithID(h.context, h.dbPool, messageID)
	require.NoError(h.t, err)
	require.NotNil(h.t, ch)

	encodedMessages := make([]EncodedMessage, 0)
	for result := range ch {
		require.NoError(h.t, result.Error)
		encodedMessages = append(encodedMessages, result.Value)
	}

	return encodedMessages
}

func (h *TestHarness) WaitConsumeErrorEvent(
	sub *Subscription,
	testFn func(event ErrorEvent) bool,
	fns ...func(event ErrorEvent),
) ErrorEvent {
	return waitEvent(h.context, sub, testFn, fns...)
}

func (h *TestHarness) WaitConsumeErrorEventIs(
	sub *Subscription,
	err error,
	fns ...func(event ErrorEvent),
) ErrorEvent {
	return h.WaitConsumeErrorEvent(sub, func(event ErrorEvent) bool {
		return errors.Is(event.Error, err)
	}, fns...)
}

func (h *TestHarness) WaitConsumeMessageEvent(
	sub *Subscription,
	testFn func(event MessageEvent) bool,
	fns ...func(event MessageEvent),
) MessageEvent {
	return waitEvent(h.context, sub, testFn, fns...)
}

func (h *TestHarness) WaitConsumeMessageEventID(
	sub *Subscription,
	messageID MessageID,
	fns ...func(event MessageEvent),
) MessageEvent {
	return h.WaitConsumeMessageEvent(sub, func(event MessageEvent) bool {
		return event.Message.ID == messageID
	}, fns...)
}

func (h *TestHarness) WaitConsumeStatusEvent(
	sub *Subscription,
	testFn func(event StatusEvent) bool,
	fns ...func(event StatusEvent),
) StatusEvent {
	return waitEvent(h.context, sub, testFn, fns...)
}

func (h *TestHarness) WaitConsumeStatusEventClosed(
	sub *Subscription,
	fns ...func(event StatusEvent),
) StatusEvent {
	return h.WaitConsumeStatusEvent(sub, func(event StatusEvent) bool {
		return event.Closed.Valid && event.Closed.Value
	}, fns...)
}

func (h *TestHarness) WaitConsumeStatusEventDelayed(
	sub *Subscription,
	fns ...func(event StatusEvent),
) StatusEvent {
	return h.WaitConsumeStatusEvent(sub, func(event StatusEvent) bool {
		return event.Delay.Valid
	}, fns...)
}

func (h *TestHarness) WaitConsumeStatusEventSubscribed(
	sub *Subscription,
	subscribed bool,
	fns ...func(event StatusEvent),
) StatusEvent {
	return h.WaitConsumeStatusEvent(sub, func(event StatusEvent) bool {
		return event.Subscribed.Valid && event.Subscribed.Value == subscribed
	}, fns...)
}

func AssertMessageEventForReceipt(t *testing.T, event MessageEvent, receipt *PublishReceipt) {
	assert.Equal(t, receipt.MessageID, event.Message.ID)
	assert.Contains(t, receipt.Topics, event.Topic)
	assert.Equal(t, receipt.Value, event.Message.Value)
	assert.Equal(t, receipt.PublishedAt, event.Message.PublishedAt)
}

func AssertNoQueuedEvents(t *testing.T, sub *Subscription) {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	assert.Empty(t, sub.events)
}

func databasePoolForTesting(t *testing.T) *pgxpool.Pool {
	// Parse the database url.
	url, err := url.Parse(env.Required("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	// Replace the path with a unique database name.
	url.Path = fmt.Sprintf("/test-%s", uuid.NewString())

	// Create a logger.
	logger := slogt.New(t)

	// Create the database.
	err = db.Create(context.Background(), logger, url)
	require.NoError(t, err)

	// Migrate the database.
	err = db.Migrate(context.Background(), logger, url)
	require.NoError(t, err)

	// Create a new pool.
	pool, err := pgxpool.New(context.Background(), url.String())
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}

	// Success.
	return pool
}

func waitEvent[T any](
	_ context.Context,
	sub *Subscription,
	testFn func(event T) bool,
	fns ...func(event T),
) T {
	sub.eventsCond.L.Lock()
	defer sub.eventsCond.L.Unlock()

	for {
		for idx := 0; idx < len(sub.events); idx++ {
			if event, ok := sub.events[idx].(T); ok {
				if testFn(event) {
					for _, fn := range fns {
						fn(event)
					}
					sub.events = append(sub.events[:idx], sub.events[idx+1:]...)
					return event
				}
			}
		}

		sub.eventsCond.Wait()
	}
}
