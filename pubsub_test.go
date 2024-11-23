package pubsub

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common/env"
	"github.com/jeremybower/go-pubsub/db"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

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
