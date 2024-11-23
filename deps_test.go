package pubsub

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/postgres"
	"github.com/stretchr/testify/mock"
)

type mockDependencies struct {
	mock.Mock
}

var _ dependencies = &mockDependencies{}

func (md *mockDependencies) Acquire(ctx context.Context, dbPool *pgxpool.Pool) (*pgxpool.Conn, error) {
	mockArgs := md.Called(ctx, dbPool)
	return mockArgs.Get(0).(*pgxpool.Conn), mockArgs.Error(1)
}

func (md *mockDependencies) Query(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) (pgx.Rows, error) {
	mockArgs := md.Called(ctx, querier, sql, args)
	return mockArgs.Get(0).(pgx.Rows), mockArgs.Error(1)
}

func (md *mockDependencies) QueryScan(rows pgx.Rows, dest ...interface{}) error {
	mockArgs := md.Called(rows, dest)
	return mockArgs.Error(0)
}

func (md *mockDependencies) QueryRow(ctx context.Context, querier postgres.Querier, sql string, args ...interface{}) pgx.Row {
	mockArgs := md.Called(ctx, querier, sql, args)
	return mockArgs.Get(0).(pgx.Row)
}

func (md *mockDependencies) QueryRowScan(row pgx.Row, dest ...interface{}) error {
	mockArgs := md.Called(row, dest)
	return mockArgs.Error(0)
}

func (md *mockDependencies) WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error) {
	mockArgs := md.Called(ctx, conn)
	return mockArgs.Get(0).(*pgconn.Notification), mockArgs.Error(1)
}

func (md *mockDependencies) MarshalStringJSON(v interface{}) (string, error) {
	mockArgs := md.Called(v)
	return mockArgs.String(0), mockArgs.Error(1)
}

func (md *mockDependencies) UnmarshalStringJSON(data string, v interface{}) error {
	mockArgs := md.Called(data, v)
	return mockArgs.Error(0)
}

type mockRow struct {
	mock.Mock
}

var _ pgx.Row = &mockRow{}

func (mr *mockRow) Scan(dest ...interface{}) error {
	mockArgs := mr.Called(dest)
	return mockArgs.Error(0)
}

type mockRows struct {
	mock.Mock
}

var _ pgx.Rows = &mockRows{}

func (mr *mockRows) Close() {
	mr.Called()
}

func (mr *mockRows) Next() bool {
	mockArgs := mr.Called()
	return mockArgs.Bool(0)
}

func (mr *mockRows) Scan(dest ...interface{}) error {
	mockArgs := mr.Called(dest)
	return mockArgs.Error(0)
}

func (mr *mockRows) CommandTag() pgconn.CommandTag {
	mockArgs := mr.Called()
	return mockArgs.Get(0).(pgconn.CommandTag)
}

func (mr *mockRows) Conn() *pgx.Conn {
	mockArgs := mr.Called()
	return mockArgs.Get(0).(*pgx.Conn)
}

func (mr *mockRows) Err() error {
	mockArgs := mr.Called()
	return mockArgs.Error(0)
}

func (mr *mockRows) FieldDescriptions() []pgconn.FieldDescription {
	mockArgs := mr.Called()
	return mockArgs.Get(0).([]pgconn.FieldDescription)
}

func (mr *mockRows) RawValues() [][]byte {
	mockArgs := mr.Called()
	return mockArgs.Get(0).([][]byte)
}

func (mr *mockRows) Values() ([]interface{}, error) {
	mockArgs := mr.Called()
	return mockArgs.Get(0).([]interface{}), mockArgs.Error(1)
}
