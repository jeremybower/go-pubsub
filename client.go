package pubsub

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/postgres"
)

var _ Publisher = &Client{}

type Publisher interface {
	Publish(ctx context.Context, querier postgres.Querier, topics []string, value any, encoder Encoder) (*PublishReceipt, error)
}

var _ Subscriber = &Client{}

type Subscriber interface {
	Subscribe(ctx context.Context, dbPool *pgxpool.Pool, topics []string, opts ...SubscribeOption) (*Subscription, error)
}

type Client struct {
	dataStore DataStore
}

type ClientOptions struct {
	DataStore DataStore
}

type ClientOption func(*ClientOptions)

func defaultClientOptions() *ClientOptions {
	return &ClientOptions{
		DataStore: NewPostgresDataStore(),
	}
}

func WithDataStore(dataStore DataStore) ClientOption {
	return func(opts *ClientOptions) {
		opts.DataStore = dataStore
	}
}

func NewClient(opts ...ClientOption) *Client {
	options := defaultClientOptions()
	for _, opt := range opts {
		opt(options)
	}

	return &Client{
		dataStore: options.DataStore,
	}
}
