package pubsub

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/postgres"
)

type dependencies interface {
	Acquire(ctx context.Context, dbPool *pgxpool.Pool) (*pgxpool.Conn, error)
	Query(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) pgx.Row
	WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error)
	MarshalStringJSON(v interface{}) (string, error)
	UnmarshalStringJSON(data string, v interface{}) error
}

var _ dependencies = &standardDependencies{}

type standardDependencies struct{}

func (sd *standardDependencies) Acquire(ctx context.Context, dbPool *pgxpool.Pool) (*pgxpool.Conn, error) {
	return dbPool.Acquire(ctx)
}

func (sd *standardDependencies) Query(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) (pgx.Rows, error) {
	return querier.Query(ctx, sql, args...)
}

func (sd *standardDependencies) QueryRow(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) pgx.Row {
	return querier.QueryRow(ctx, sql, args...)
}

func (sd *standardDependencies) WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error) {
	return conn.Conn().WaitForNotification(ctx)
}

func (sd *standardDependencies) MarshalStringJSON(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	return string(data), err
}

func (sd *standardDependencies) UnmarshalStringJSON(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), v)
}
