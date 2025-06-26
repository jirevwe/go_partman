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

// Example 1: Simple integration with complete UI handler
func exampleSimpleIntegration() {
	logger := partman.NewSlogLogger()

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

	// Initialize manager
	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{SampleRate: time.Second}),
		partman.WithClock(partman.NewRealClock()),
	)
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

// Example 2: Separate API and static handlers for more control
func exampleSeparateHandlers() {
	logger := partman.NewSlogLogger()

	// Setup database connection (same as above)
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

	// Initialize manager
	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{SampleRate: time.Second}),
		partman.WithClock(partman.NewRealClock()),
	)
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

// Example 3: With authentication middleware
func exampleWithAuth() {
	logger := partman.NewSlogLogger()

	// Setup database connection (same as above)
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

	// Initialize manager
	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{SampleRate: time.Second}),
		partman.WithClock(partman.NewRealClock()),
	)
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

func main() {
	// Uncomment one of the examples to run it

	// exampleSimpleIntegration()
	exampleSeparateHandlers()
	// exampleWithAuth()

	log.Println("Please uncomment one of the example functions to run it")
}
