package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	partman "github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

// ExampleSimpleIntegration demonstrates simple integration with the UI handler
func ExampleSimpleIntegration() {
	logger := partman.NewSlogLogger(slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
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
					Name:              "user_logs",
					Schema:            "convoy",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    10,
					RetentionPeriod:   time.Hour * 24 * 30,
				},
				{
					Name:              "delivery_attempts",
					Schema:            "convoy",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    10,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
			},
		}),
		partman.WithClock(partman.NewRealClock()),
	)
	if err != nil {
		logger.Fatal(err)
	}

	// Mount UI at /partman
	http.Handle("/partman/", http.StripPrefix("/partman", partman.UIHandler(manager)))

	// Add health check
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	log.Println("Starting server on :8080")
	log.Println("UI available at: http://localhost:8080/partman/")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
