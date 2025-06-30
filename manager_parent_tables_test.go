package partman

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

var createParentTableQuery = `
CREATE TABLE if not exists test.user_logs (
    id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data JSONB,
    PRIMARY KEY (id, created_at, project_id)
) PARTITION BY RANGE (project_id, created_at);`

var createParentTableWithoutTenantQuery = `
CREATE TABLE if not exists test.delivery_attempts (
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);`

func createParentTable(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, createParentTableQuery)
	require.NoError(t, err)
}

func createParentTableWithoutTenant(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, createParentTableWithoutTenantQuery)
	require.NoError(t, err)
}

func dropParentTable(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test.user_logs")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS test.delivery_attempts")
	require.NoError(t, err)
}

// cleanupParentTablesTestDB extends the existing cleanupTestDB to also clean up parent tables
func cleanupParentTablesTestDB(t *testing.T, db *sqlx.DB, pool *pgxpool.Pool) {
	t.Helper()
	defer func(db *sqlx.DB) {
		err := db.Close()
		require.NoError(t, err)
	}(db)
	defer pool.Close()

	// Drop tables in the correct order to handle dependencies
	_, err := db.Exec("DROP TABLE IF EXISTS partman.tenants CASCADE")
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE IF EXISTS partman.parent_tables CASCADE")
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE IF EXISTS partman.partition_management CASCADE")
	require.NoError(t, err)
}

func TestParentTablesAPI(t *testing.T) {
	t.Run("CreateParent", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Verify that the parent table was created in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("CreateParentWithInvalidTable", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		parentTable := ParentTable{
			Name:              "nonexistent_table",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.Error(t, err)
		require.Contains(t, err.Error(), "table validation failed")
	})

	t.Run("RegisterTenant", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// First create the parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    3,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register a tenant
		tenant := Tenant{
			ParentTableName:   "user_logs",
			ParentTableSchema: "test",
			TenantId:          "tenant1",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "tenant1", result.TenantId)
		require.Equal(t, "user_logs", result.ParentTableName)
		require.Equal(t, 3, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Verify tenant was registered in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.tenants WHERE tenant_id = $1", "tenant1")
		require.NoError(t, err)
		require.Equal(t, 1, count)

		// Verify partitions were created
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant1%")
		require.NoError(t, err)
		require.Equal(t, 3, partitionCount)
	})

	t.Run("RegisterTenantWithNonExistentParent", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		tenant := Tenant{
			ParentTableName:   "nonexistent_table",
			ParentTableSchema: "test",
			TenantId:          "tenant1",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "tenant1", result.TenantId)
		require.NotEmpty(t, result.Errors)
		require.Contains(t, result.Errors[0].Error(), "parent table not found")
	})

	t.Run("RegisterMultipleTenants", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create the parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    2,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register multiple tenants
		tenants := []Tenant{
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant1",
			},
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant2",
			},
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant3",
			},
		}

		results, err := manager.RegisterTenants(context.Background(), tenants)
		require.NoError(t, err)
		require.Len(t, results, 3)

		// Verify all tenants were registered successfully
		for i, result := range results {
			require.Equal(t, fmt.Sprintf("tenant%d", i+1), result.TenantId)
			require.Equal(t, "user_logs", result.ParentTableName)
			require.Equal(t, 2, result.PartitionsCreated)
			require.Empty(t, result.Errors)
		}

		// Verify all tenants exist in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.tenants WHERE parent_table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Verify partitions were created for all tenants
		for _, tenant := range tenants {
			var partitionCount int
			err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("user_logs_%s%%", tenant.TenantId))
			require.NoError(t, err)
			require.Equal(t, 2, partitionCount)
		}
	})

	t.Run("GetParentTables", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create multiple parent tables
		parentTables := []ParentTable{
			{
				Name:              "user_logs",
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    5,
				RetentionPeriod:   time.Hour * 24 * 7,
			},
			{
				Name:              "delivery_attempts",
				Schema:            "test",
				TenantIdColumn:    "", // No tenant column for this table
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 12,
				PartitionCount:    3,
				RetentionPeriod:   time.Hour * 24 * 3,
			},
		}

		// Create the delivery_attempts table first
		createParentTableWithoutTenant(t, context.Background(), db)

		for _, pt := range parentTables {
			err = manager.CreateParent(context.Background(), pt)
			require.NoError(t, err)
		}

		// Get all parent tables
		retrievedTables, err := manager.GetParentTables(context.Background())
		require.NoError(t, err)
		require.Len(t, retrievedTables, 2)

		// Verify the tables were retrieved correctly
		tableMap := make(map[string]ParentTable)
		for _, table := range retrievedTables {
			tableMap[table.Name] = table
		}

		userLogs, exists := tableMap["user_logs"]
		require.True(t, exists)
		require.Equal(t, "test", userLogs.Schema)
		require.Equal(t, "project_id", userLogs.TenantIdColumn)
		require.Equal(t, uint(5), userLogs.PartitionCount)
		require.Equal(t, time.Hour*24*7, userLogs.RetentionPeriod)

		deliveryAttempts, exists := tableMap["delivery_attempts"]
		require.True(t, exists)
		require.Equal(t, "test", deliveryAttempts.Schema)
		require.Equal(t, "", deliveryAttempts.TenantIdColumn) // No tenant column
		require.Equal(t, uint(3), deliveryAttempts.PartitionCount)
		require.Equal(t, time.Hour*24*3, deliveryAttempts.RetentionPeriod)
	})

	t.Run("GetTenants", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    2,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register multiple tenants
		tenants := []Tenant{
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant1",
			},
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant2",
			},
			{
				ParentTableName:   "user_logs",
				ParentTableSchema: "test",
				TenantId:          "tenant3",
			},
		}

		for _, tenant := range tenants {
			_, err = manager.RegisterTenant(context.Background(), tenant)
			require.NoError(t, err)
		}

		// Get tenants for the parent table
		retrievedTenants, err := manager.GetTenants(context.Background(), "user_logs", "test")
		require.NoError(t, err)
		require.Len(t, retrievedTenants, 3)

		// Verify all tenants were retrieved
		tenantIds := make(map[string]bool)
		for _, tenant := range retrievedTenants {
			tenantIds[tenant.TenantId] = true
			require.Equal(t, "user_logs", tenant.ParentTableName)
			require.Equal(t, "test", tenant.ParentTableSchema)
		}

		require.True(t, tenantIds["tenant1"])
		require.True(t, tenantIds["tenant2"])
		require.True(t, tenantIds["tenant3"])
	})

	t.Run("GetTenantsForNonExistentTable", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		tenants, err := manager.GetTenants(context.Background(), "nonexistent_table", "test")
		require.NoError(t, err)
		require.Empty(t, tenants)
	})

	t.Run("RegisterTenantWithExistingPartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		// Create some existing partitions manually
		existingPartitions := []string{
			`CREATE TABLE test.user_logs_tenant1_20240101 PARTITION OF test.user_logs 
			 FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`,
			`CREATE TABLE test.user_logs_tenant1_20240102 PARTITION OF test.user_logs 
			 FOR VALUES FROM ('tenant1', '2024-01-02') TO ('tenant1', '2024-01-03')`,
		}

		for _, partition := range existingPartitions {
			_, err := db.ExecContext(context.Background(), partition)
			require.NoError(t, err)
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			ParentTableName:   "user_logs",
			ParentTableSchema: "test",
			TenantId:          "tenant1",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "tenant1", result.TenantId)
		require.Equal(t, 5, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Verify total partitions (existing + new)
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant1%")
		require.NoError(t, err)
		require.Equal(t, 7, partitionCount) // 2 existing + 5 new
	})

	t.Run("ParentTableWithConfigInitialization", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			ParentTables: []ParentTable{
				{
					Name:              "user_logs",
					Schema:            "test",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    3,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)
		require.NotNil(t, manager)

		// Verify parent table was created
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("MixedLegacyAndParentTablesAPI", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					TenantId:          "legacy_tenant",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    2,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
			},
			ParentTables: []ParentTable{
				{
					Name:              "user_logs",
					Schema:            "test",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    3,
					RetentionPeriod:   time.Hour * 24 * 7,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)
		require.NotNil(t, manager)

		// Verify both legacy and parent table configurations exist
		var legacyCount int
		err = db.Get(&legacyCount, "SELECT COUNT(*) FROM partman.partition_management WHERE tenant_id = $1", "legacy_tenant")
		require.NoError(t, err)
		require.Equal(t, 1, legacyCount)

		var parentCount int
		err = db.Get(&parentCount, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, parentCount)

		// Register a new tenant using the parent table API
		tenant := Tenant{
			ParentTableName:   "user_logs",
			ParentTableSchema: "test",
			TenantId:          "new_tenant",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "new_tenant", result.TenantId)
		require.Equal(t, 3, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Verify both tenants have partitions
		var legacyPartitions int
		err = db.Get(&legacyPartitions, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_legacy_tenant%")
		require.NoError(t, err)
		require.Equal(t, 2, legacyPartitions)

		var newPartitions int
		err = db.Get(&newPartitions, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_new_tenant%")
		require.NoError(t, err)
		require.Equal(t, 3, newPartitions)
	})

	t.Run("TenantRegistrationWithDataInsertion", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    3,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			ParentTableName:   "user_logs",
			ParentTableSchema: "test",
			TenantId:          "tenant1",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result.Errors)

		// Insert data for the tenant
		insertSQL := `INSERT INTO test.user_logs (id, project_id, created_at, data) VALUES ($1, $2, $3, $4)`
		for i := 0; i < 10; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(),
				"tenant1",
				clock.Now().Add(time.Duration(i)*time.Hour),
				fmt.Sprintf(`{"message": "test %d"}`, i),
			)
			require.NoError(t, err)
		}

		// Verify data was inserted correctly
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM test.user_logs WHERE project_id = $1", "tenant1")
		require.NoError(t, err)
		require.Equal(t, 10, count)

		// Verify data is distributed across partitions
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(DISTINCT schemaname || '.' || tablename) FROM pg_tables WHERE tablename LIKE 'user_logs_tenant1%'")
		require.NoError(t, err)
		require.Equal(t, 3, partitionCount)
	})

	t.Run("TenantRegistrationResultValidation", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupParentTablesTestDB(t, db, pool)

		createParentTable(t, context.Background(), db)
		defer dropParentTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)

		// Create parent table
		parentTable := ParentTable{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    2,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		err = manager.CreateParent(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			ParentTableName:   "user_logs",
			ParentTableSchema: "test",
			TenantId:          "tenant1",
		}

		result, err := manager.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate result structure
		require.Equal(t, "tenant1", result.TenantId)
		require.Equal(t, "user_logs", result.ParentTableName)
		require.Equal(t, "test", result.ParentTableSchema)
		require.Equal(t, 2, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Test with invalid tenant (should return error in result)
		invalidTenant := Tenant{
			ParentTableName:   "nonexistent_table",
			ParentTableSchema: "test",
			TenantId:          "tenant2",
		}

		invalidResult, err := manager.RegisterTenant(context.Background(), invalidTenant)
		require.NoError(t, err)
		require.NotNil(t, invalidResult)
		require.Equal(t, "tenant2", invalidResult.TenantId)
		require.NotEmpty(t, invalidResult.Errors)
		require.Contains(t, invalidResult.Errors[0].Error(), "parent table not found")
	})
}
