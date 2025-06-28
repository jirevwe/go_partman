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

// ExampleSeparateHandlers demonstrates separate handlers for API and UI
func ExampleSeparateHandlers() {
	logger := partman.NewSlogLogger()
	err := os.Setenv("TZ", "") // Use UTC by default :)
	if err != nil {
		logger.Fatal("failed to set env - ", err)
	}

	// Setup database connection
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

	// Initialize manager with both tables
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
					TenantIdColumn:    "project_id",
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
		TenantIdColumn:    "project_id",
		PartitionBy:       "created_at",
		PartitionType:     partman.TypeRange,
		PartitionInterval: time.Hour * 24,
		PartitionCount:    10,
		RetentionPeriod:   time.Hour * 24 * 30,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create custom mux for more control
	mux := http.NewServeMux()

	// Mount API endpoints at /api/partman
	mux.Handle("/api/partman/", http.StripPrefix("/api/partman", partman.APIHandler(manager)))

	// Mount static files at /partman
	mux.Handle("/partman/", http.StripPrefix("/partman", partman.StaticHandler()))

	// Add custom endpoints
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	log.Println("Starting server on :8080")
	log.Println("API available at: http://localhost:8080/api/partman/")
	log.Println("UI available at: http://localhost:8080/partman/")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
