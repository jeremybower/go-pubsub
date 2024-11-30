package db

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	"github.com/jeremybower/go-common/env"
	"github.com/jeremybower/go-common/slogw"
	"github.com/neilotoole/slogt"
)

func TestMigrateUpDownUp(t *testing.T) {
	// Setup logger for testing.
	logger := slogt.New(t)

	// Setup dbmate.
	url := env.RequiredURL("DATABASE_URL")
	db := dbmate.New(&url)
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
