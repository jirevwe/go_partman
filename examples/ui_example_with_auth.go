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

// ExampleWithAuth demonstrates using authentication middleware
func ExampleWithAuth() {
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

	// Simple authentication middleware
	authMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check for API key in header
			apiKey := r.Header.Get("X-API-Key")
			if apiKey != "your-secret-key" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Mount UI with authentication
	http.Handle("/partman/", authMiddleware(http.StripPrefix("/partman", partman.UIHandler(manager))))

	log.Println("Starting server on :8080")
	log.Println("UI available at: http://localhost:8080/partman/")
	log.Println("Use X-API-Key header with value 'your-secret-key'")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
