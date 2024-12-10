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
	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/endpoint_fix?sslmode=disable")
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
		Tables: []partman.Table{
			// {
			// 	Name:              "samples",
			// 	TenantId:          "project_id_asd",
			// 	TenantIdColumn:    "project_id",
			// 	PartitionType:     partman.TypeRange,
			// 	PartitionBy:       "created_at",
			// 	PartitionInterval: partman.OneDay,
			// 	RetentionPeriod:   partman.OneMonth,
			// },
			// {
			// 	Name:              "samples",
			// 	TenantId:          "project_id_124",
			// 	PartitionType:     partman.TypeRange,
			// 	PartitionBy:       "created_at",
			// 	TenantIdColumn:    "project_id",
			// 	PartitionInterval: partman.OneDay,
			// 	RetentionPeriod:   partman.OneMonth * 2,
			// },
			{
				Name:              "events",
				Schema:            "convoy",
				TenantId:          "01J9V16CEJP040Z14489BV8AW3",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     partman.TypeRange,
				RetentionPeriod:   partman.OneMonth,
				PartitionInterval: partman.OneDay,
				PartitionCount:    10,
			},
		},
		SampleRate: 30 * time.Second,
		SchemaName: "convoy",
	}

	clock := partman.NewSimulatedClock(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))
	manager, err := partman.NewAndStart(db, config, slog.New(slog.NewTextHandler(os.Stdout, nil)), clock)
	if err != nil {
		log.Fatal(err)
	}

	// Import existing partitions
	if err = manager.ImportExistingPartitions(context.Background(), partman.Table{
		TenantIdColumn:    "project_id",
		PartitionBy:       "created_at",
		PartitionType:     partman.TypeRange,
		PartitionInterval: partman.OneDay,
		PartitionCount:    10,
		RetentionPeriod:   partman.OneMonth,
	}); err != nil {
		log.Fatal(err)
	}

	if err = manager.Start(context.Background()); err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Minute)
}
