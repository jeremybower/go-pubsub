package pubsub

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client[T any] struct {
	dbPool *pgxpool.Pool
}

func NewClient[T any](dbPool *pgxpool.Pool) *Client[T] {
	if dbPool == nil {
		panic("pubsub: database pool is nil")
	}

	return &Client[T]{
		dbPool: dbPool,
	}
}
