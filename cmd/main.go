package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"

	"log"
	"log/slog"
	"os"

	"time"
)

func main() {
	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	if err != nil {
		log.Fatal(err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	db := sqlx.NewDb(sqlDB, "pgx")

	config := &partman.Config{
		SampleRate: 30 * time.Second,
		SchemaName: "convoy",
	}

	clock := partman.NewSimulatedClock(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))
	manager, err := partman.NewAndStart(db, config, slog.New(slog.NewTextHandler(os.Stdout, nil)), clock)
	if err != nil {
		log.Fatal(err)
	}

	// Import existing partitions
	err = manager.ImportExistingPartitions(context.Background(), partman.Table{
		TenantIdColumn:    "project_id",
		PartitionBy:       "created_at",
		PartitionType:     partman.TypeRange,
		PartitionInterval: time.Hour * 24,
		PartitionCount:    10,
		RetentionPeriod:   time.Hour * 24 * 7,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err = manager.Start(context.Background()); err != nil {
		log.Fatal(err)
	}

	time.Sleep(30 * time.Second)
}
