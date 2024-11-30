package pubsub

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

type DataStore interface {
	AcquireConnection(ctx context.Context, dbPool *pgxpool.Pool) (*pgxpool.Conn, error)
	Publish(ctx context.Context, querier postgres.Querier, topicNames []string, value any, encodedValue *EncodedValue) (*PublishReceipt, error)
	ReadEncodedMessagesAfterID(ctx context.Context, querier postgres.Querier, messageID MessageID, topicNames []string) (chan common.Result[EncodedMessage], error)
	ReadEncodedMessagesWithID(ctx context.Context, querier postgres.Querier, messageID MessageID) (chan common.Result[EncodedMessage], error)
	ReadEncodedValue(ctx context.Context, querier postgres.Querier, messageID MessageID) (*EncodedValue, error)
	Subscribe(ctx context.Context, querier postgres.Querier, topicNames []string) (*SubscribeReceipt, error)
	UnmarshalMessageNotification(payload string) (*MessageNotification, error)
	WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error)
}
