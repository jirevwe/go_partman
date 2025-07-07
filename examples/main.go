package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	partman "github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

func main() {
	ExampleSimpleIntegration()
}

// ExampleSimpleIntegration demonstrates simple integration
func ExampleSimpleIntegration() {
	err := os.Setenv("TZ", "") // Use UTC by default :)
	logger := partman.NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	if err != nil {
		logger.Fatal("failed to set env - ", err)
	}

	ctx := context.Background()

	// Setup database connection
	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/party?sslmode=disable")
	if err != nil {
		logger.Fatal(err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
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

	// Register tenants at startup
	tenants := []partman.Tenant{
		{
			TableName:   "delivery_attempts",
			TableSchema: "convoy",
			TenantId:    "tenant1",
		},
		{
			TableName:   "delivery_attempts",
			TableSchema: "convoy",
			TenantId:    "tenant2",
		},
		{
			TableName:   "user_logs",
			TableSchema: "convoy",
			TenantId:    "tenant1",
		},
		{
			TableName:   "user_logs",
			TableSchema: "convoy",
			TenantId:    "tenant2",
		},
	}

	// Register all tenants
	results, err := manager.RegisterTenants(ctx, tenants...)
	if err != nil {
		logger.Fatal("failed to register tenants:", err)
	}

	// Log registration results
	for _, result := range results {
		if len(result.Errors) > 0 {
			logger.Error("tenant registration had errors",
				"tenant", result.TenantId,
				"table", result.TableName,
				"errors", result.Errors)
		} else {
			logger.Info("tenant registered successfully",
				"tenant", result.TenantId,
				"table", result.TableName,
				"partitions_created", result.PartitionsCreated,
				"partitions_imported", result.ExistingPartitionsImported)
		}
	}

	// Register a new tenant after it has started
	result, err := manager.RegisterTenant(ctx, partman.Tenant{
		TableName:   "user_logs",
		TableSchema: "convoy",
		TenantId:    "tenant3",
	})
	if err != nil {
		return
	}

	logger.Info("tenant registration results",
		"tenant", result.TenantId,
		"table", result.TableName,
		"partitions_created", result.PartitionsCreated,
		"partitions_imported", result.ExistingPartitionsImported,
		"errors", result.Errors)

	err = registerTenantAtRuntime(ctx, manager, "tenant4")
	if err != nil {
		logger.Fatal("failed to register tenant at runtime:", err)
	}

	err = listParentTablesAndTenants(ctx, manager)
	if err != nil {
		logger.Fatal("failed to list parent tables and tenants:", err)
	}
}

// Example of registering a tenant at runtime
func registerTenantAtRuntime(ctx context.Context, manager *partman.Manager, tenantID string) error {
	// Register a new tenant for delivery_attempts
	tenant := partman.Tenant{
		TableName:   "delivery_attempts",
		TableSchema: "convoy",
		TenantId:    tenantID,
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
func listParentTablesAndTenants(ctx context.Context, manager *partman.Manager) error {
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
		tenants, err := manager.GetTenants(ctx, pt.Schema, pt.Name)
		if err != nil {
			log.Printf("    Error getting tenants: %v", err)
			continue
		}

		log.Printf("    %s has %d tenants:", pt.Name, len(tenants))
		for _, tenant := range tenants {
			log.Printf("      - %s", tenant.TenantId)
		}
	}

	return nil
}
