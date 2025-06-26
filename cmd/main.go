package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	partman "github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

func main() {
	logger := partman.NewSlogLogger()
	err := os.Setenv("TZ", "") // Use UTC by default :)
	if err != nil {
		logger.Fatal("failed to set env - ", err)
	}

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

	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{
			SampleRate: time.Second,
			Tables: []partman.Table{
				{
					Name:              "delivery_attempts",
					Schema:            "convoy",
					TenantId:          "tenant1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    10,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
				{
					Name:              "user_logs",
					Schema:            "convoy",
					TenantId:          "tenant1",
					TenantIdColumn:    "user_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    10,
					RetentionPeriod:   time.Hour * 24 * 30,
				},
			},
		}),
		partman.WithClock(partman.NewRealClock()),
	)
	if err != nil {
		logger.Fatal(err)
	}

	// Import existing partitions for both tables
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

	// Import existing partitions for user_logs table
	err = manager.ImportExistingPartitions(context.Background(), partman.Table{
		Schema:            "convoy",
		TenantIdColumn:    "user_id",
		PartitionBy:       "created_at",
		PartitionType:     partman.TypeRange,
		PartitionInterval: time.Hour * 24,
		PartitionCount:    10,
		RetentionPeriod:   time.Hour * 24 * 30,
	})
	if err != nil {
		log.Fatal(err)
	}

	// if err = manager.Start(context.Background()); err != nil {
	// 	log.Fatal(err)
	// }

	// Start the HTTP server
	log.Println("Starting server on :8080")
	log.Println("UI available at: http://localhost:8080/")
	err = http.ListenAndServe(":8080", partman.UIHandler(manager))
	if err != nil {
		log.Fatal(err)
	}
}
