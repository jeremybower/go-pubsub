package db

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"testing"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	"github.com/google/uuid"
	"github.com/jeremybower/go-common/env"
	"github.com/jeremybower/go-common/slogw"
	"github.com/neilotoole/slogt"
)

func TestMigrateUpDownUp(t *testing.T) {
	// Setup logger for testing.
	logger := slogt.New(t)

	// Parse the database url.
	u, err := url.Parse(env.Required("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	// Replace the path with a unique database name.
	u.Path = fmt.Sprintf("/test-%s", uuid.NewString())

	// Setup dbmate.
	db := dbmate.New(u)
	db.MigrationsTableName = "pubsub_schema_migrations"
	db.AutoDumpSchema = false
	db.FS = fs
	db.Log = slogw.New(slog.LevelInfo, logger)
	db.MigrationsDir = []string{"migrations"}

	// Create the database.
	if err := db.Create(); err != nil {
		t.Fatal(err)
	}

	// Migrate the database to check that it works initially.
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}

	// Rollback all migrations.
	for {
		if err := db.Rollback(); err != nil {
			if errors.Is(err, dbmate.ErrNoRollback) {
				break
			}

			t.Fatal(err)
		}
	}

	// Migrate the database again to check that all
	// previous migrations were rolled back.
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
}
