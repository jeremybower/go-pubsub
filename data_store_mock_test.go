package pubsub

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
	"github.com/stretchr/testify/mock"
)

type MockDataStore struct {
	mock.Mock
}

var _ DataStore = NewMockDataStore()

func NewMockDataStore() *MockDataStore {
	return &MockDataStore{}
}

func (md *MockDataStore) AcquireConnection(ctx context.Context, dbPool *pgxpool.Pool) (*pgxpool.Conn, error) {
	mockArgs := md.Called(ctx, dbPool)
	return mockArgs.Get(0).(*pgxpool.Conn), mockArgs.Error(1)
}

func (md *MockDataStore) PatchConfiguration(ctx context.Context, querier postgres.Querier, patch ConfigurationPatch) (*Configuration, error) {
	mockArgs := md.Called(ctx, querier, patch)
	return mockArgs.Get(0).(*Configuration), mockArgs.Error(1)
}

func (md *MockDataStore) Publish(ctx context.Context, querier postgres.Querier, topicNames []string, value any, encodedValue *EncodedValue, publishedAt *time.Time) (*PublishReceipt, int64, error) {
	mockArgs := md.Called(ctx, querier, topicNames, encodedValue)
	return mockArgs.Get(0).(*PublishReceipt), mockArgs.Get(1).(int64), mockArgs.Error(2)
}

func (md *MockDataStore) ReadConfiguration(ctx context.Context, querier postgres.Querier) (*Configuration, error) {
	mockArgs := md.Called(ctx, querier)
	return mockArgs.Get(0).(*Configuration), mockArgs.Error(1)
}

func (md *MockDataStore) ReadEncodedMessagesAfterID(ctx context.Context, querier postgres.Querier, messageID MessageID, topics []string) (chan common.Result[EncodedMessage], error) {
	mockArgs := md.Called(ctx, querier, messageID, topics)
	return mockArgs.Get(0).(chan common.Result[EncodedMessage]), mockArgs.Error(1)
}

func (md *MockDataStore) ReadEncodedMessagesWithID(ctx context.Context, querier postgres.Querier, messageID MessageID) (chan common.Result[EncodedMessage], error) {
	mockArgs := md.Called(ctx, querier, messageID)
	return mockArgs.Get(0).(chan common.Result[EncodedMessage]), mockArgs.Error(1)
}

func (md *MockDataStore) ReadEncodedValue(ctx context.Context, querier postgres.Querier, messageID MessageID) (*EncodedValue, error) {
	mockArgs := md.Called(ctx, querier, messageID)
	return mockArgs.Get(0).(*EncodedValue), mockArgs.Error(1)
}

func (md *MockDataStore) StopSubscriptions(ctx context.Context, querier postgres.Querier) error {
	mockArgs := md.Called(ctx, querier)
	return mockArgs.Error(0)
}

func (md *MockDataStore) Subscribe(ctx context.Context, querier postgres.Querier, topicNames []string) (*SubscribeReceipt, error) {
	mockArgs := md.Called(ctx, querier, topicNames)
	return mockArgs.Get(0).(*SubscribeReceipt), mockArgs.Error(1)
}

func (md *MockDataStore) UnmarshalControlNotification(payload string) (*ControlNotification, error) {
	mockArgs := md.Called(payload)
	return mockArgs.Get(0).(*ControlNotification), mockArgs.Error(1)
}

func (md *MockDataStore) UnmarshalMessageNotification(payload string) (*MessageNotification, error) {
	mockArgs := md.Called(payload)
	return mockArgs.Get(0).(*MessageNotification), mockArgs.Error(1)
}

func (md *MockDataStore) WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error) {
	mockArgs := md.Called(ctx, conn)
	return mockArgs.Get(0).(*pgconn.Notification), mockArgs.Error(1)
}
