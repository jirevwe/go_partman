package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	partman "github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

func main() {
	logger := partman.NewSlogLogger()

	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/party?sslmode=disable")
	if err != nil {
		logger.Fatal(err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	if err != nil {
		logger.Fatal(err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	db := sqlx.NewDb(sqlDB, "pgx")

	config := &partman.Config{}

	clock := partman.NewRealClock()
	manager, err := partman.NewAndStart(db, config, logger, clock)
	if err != nil {
		logger.Fatal(err)
	}

	// Import existing partitions
	err = manager.ImportExistingPartitions(context.Background(), partman.Table{
		Schema:            "convoy",
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

	// Start the HTTP server
	err = http.ListenAndServe(":8080", partman.UIHandler())
	if err != nil {
		log.Fatal(err)
	}
}
