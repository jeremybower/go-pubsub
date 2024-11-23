package db

import (
	"context"
	"embed"
	"log/slog"
	"net/url"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	_ "github.com/amacneil/dbmate/v2/pkg/driver/postgres"
	"github.com/jeremybower/go-common/slogw"
)

//go:embed migrations/*.sql
var fs embed.FS

func Create(ctx context.Context, logger *slog.Logger, url *url.URL) error {
	return createDbmateDB(logger, url).Create()
}

func Migrate(ctx context.Context, logger *slog.Logger, url *url.URL) error {
	if err := createDbmateDB(logger, url).Migrate(); err != nil {
		return err
	}

	return nil
}

func createDbmateDB(logger *slog.Logger, url *url.URL) *dbmate.DB {
	db := dbmate.New(url)
	db.AutoDumpSchema = false
	db.FS = fs
	db.Log = slogw.New(slog.LevelInfo, logger)
	db.MigrationsDir = []string{"migrations"}

	return db
}
