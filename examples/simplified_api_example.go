package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	partman "github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

// ExampleSimplifiedAPI demonstrates the simplified API usage
func ExampleSimplifiedAPI() {
	logger := partman.NewSlogLogger(slog.HandlerOptions{
		Level: slog.LevelError,
	})

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

	// Initialize manager with parent tables (no tenants yet)
	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{
			SampleRate: time.Second,
			ParentTables: []partman.ParentTable{
				{
					Name:              "delivery_attempts",
					Schema:            "convoy",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    20,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
				{
					Name:              "user_logs",
					Schema:            "convoy",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    20,
					RetentionPeriod:   time.Hour * 24 * 30,
				},
			},
		}),
		partman.WithClock(partman.NewRealClock()),
	)
	if err != nil {
		logger.Fatal(err)
	}

	// Register tenants at startup
	tenants := []partman.Tenant{
		{
			ParentTableName:   "delivery_attempts",
			ParentTableSchema: "convoy",
			TenantId:          "tenant1",
		},
		{
			ParentTableName:   "delivery_attempts",
			ParentTableSchema: "convoy",
			TenantId:          "tenant2",
		},
		{
			ParentTableName:   "user_logs",
			ParentTableSchema: "convoy",
			TenantId:          "tenant1",
		},
		{
			ParentTableName:   "user_logs",
			ParentTableSchema: "convoy",
			TenantId:          "tenant2",
		},
	}

	// Register all tenants
	results, err := manager.RegisterTenants(context.Background(), tenants)
	if err != nil {
		logger.Fatal("failed to register tenants:", err)
	}

	// Log registration results
	for _, result := range results {
		if len(result.Errors) > 0 {
			logger.Error("tenant registration had errors",
				"tenant", result.TenantId,
				"table", result.ParentTableName,
				"errors", result.Errors)
		} else {
			logger.Info("tenant registered successfully",
				"tenant", result.TenantId,
				"table", result.ParentTableName,
				"partitions_created", result.PartitionsCreated,
				"partitions_imported", result.ExistingPartitionsImported)
		}
	}

	// Register a new tenant after it has started
	result, err := manager.RegisterTenant(context.Background(), partman.Tenant{
		ParentTableName:   "user_logs",
		ParentTableSchema: "convoy",
		TenantId:          "tenant3",
	})
	if err != nil {
		return
	}

	logger.Error("tenant registration results",
		"tenant", result.TenantId,
		"table", result.ParentTableName,
		"partitions_created", result.PartitionsCreated,
		"partitions_imported", result.ExistingPartitionsImported,
		"errors", result.Errors)

	// Start the HTTP server
	log.Println("Starting server on :8080")
	log.Println("UI available at: http://localhost:8080/")
	err = http.ListenAndServe(":8080", partman.UIHandler(manager))
	if err != nil {
		log.Fatal(err)
	}
}

// Example of registering a tenant at runtime
func registerTenantAtRuntime(manager *partman.Manager, tenantID string) error {
	ctx := context.Background()

	// Register a new tenant for delivery_attempts
	tenant := partman.Tenant{
		ParentTableName:   "delivery_attempts",
		ParentTableSchema: "convoy",
		TenantId:          tenantID,
	}

	result, err := manager.RegisterTenant(ctx, tenant)
	if err != nil {
		return err
	}

	if len(result.Errors) > 0 {
		log.Printf("Tenant registration had errors: %v", result.Errors)
		return nil
	}

	log.Printf("Tenant %s registered successfully with %d partitions created",
		tenantID, result.PartitionsCreated)
	return nil
}

// Example of getting parent tables and tenants
func listParentTablesAndTenants(manager *partman.Manager) error {
	ctx := context.Background()

	// Get all parent tables
	parentTables, err := manager.GetParentTables(ctx)
	if err != nil {
		return err
	}

	log.Println("Parent Tables:")
	for _, pt := range parentTables {
		log.Printf("  - %s.%s (tenant_column: %s, partition_by: %s)",
			pt.Schema, pt.Name, pt.TenantIdColumn, pt.PartitionBy)

		// Get tenants for this parent table
		tenants, err := manager.GetTenants(ctx, pt.Name, pt.Schema)
		if err != nil {
			log.Printf("    Error getting tenants: %v", err)
			continue
		}

		log.Printf("    Tenants (%d):", len(tenants))
		for _, tenant := range tenants {
			log.Printf("      - %s", tenant.TenantId)
		}
	}

	return nil
}
