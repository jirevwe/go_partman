package partman

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"log/slog"
	"testing"
	"time"
)

var createTestTableQuery = `
CREATE TABLE if not exists test.test_table (
    id VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at, tenant_id)
) PARTITION BY RANGE (created_at);`

var createTestTableWithTenantIdQuery = `
CREATE TABLE if not exists test.test_table (
    id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at, project_id)
) PARTITION BY RANGE (project_id, created_at);`

func createTestTable(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, createTestTableQuery)
	require.NoError(t, err)
}

func dropTestTable(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test.test_table")
	require.NoError(t, err)
}

func setupTestDB(t *testing.T) (*sqlx.DB, *pgxpool.Pool) {
	t.Helper()

	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	require.NoError(t, err)

	sqlDB := stdlib.OpenDBFromPool(pool)
	db := sqlx.NewDb(sqlDB, "pgx")

	return db, pool
}

func cleanupTestDB(t *testing.T, db *sqlx.DB, pool *pgxpool.Pool) {
	t.Helper()
	defer func(db *sqlx.DB) {
		err := db.Close()
		require.NoError(t, err)
	}(db)
	defer pool.Close()

	_, err := db.Exec("DROP TABLE IF EXISTS partman.partition_management")
	require.NoError(t, err)
}

func TestManager(t *testing.T) {
	t.Run("NewAndStart", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(time.Hour),
					PreCreateCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)
		require.NotNil(t, manager)
	})

	t.Run("Initialize", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
					PreCreateCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Start(context.Background())
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.partition_management WHERE table_name = $1", "test_table")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("CreateFuturePartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		tableConfig := TableConfig{
			Name:              "test_table",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PreCreateCount:    10,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		_, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		require.NoError(t, err)
		require.Equal(t, tableConfig.PreCreateCount, partitionCount)
	})

	t.Run("DropOldPartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(time.Hour),
					PreCreateCount:    2,
				},
			},
		}
		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		clock.AdvanceTime(time.Hour + time.Minute)

		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)
	})

	t.Run("TestMaintainer", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
					PreCreateCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = manager.initialize(ctx, config)
		require.NoError(t, err)

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 24)

		// Wait for maintenance to complete
		time.Sleep(2 * time.Second)

		// Stop the manager
		manager.Stop()

		// Verify partitions were created
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		require.NoError(t, err)
		require.Greater(t, partitionCount, 0)
	})

	t.Run("GeneratePartitionName", func(t *testing.T) {
		manager := &Manager{}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		t.Run("without tenant ID", func(t *testing.T) {
			tableConfig := TableConfig{
				Name: "test_table",
			}
			name := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "test_table_20240315", name)
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := TableConfig{
				Name:        "test_table",
				TenantId:    "tenant1",
				PartitionBy: "created_at",
			}
			name := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "test_table_tenant1_20240315", name)
		})
	})

	t.Run("GeneratePartitionSQL", func(t *testing.T) {
		manager := &Manager{}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		t.Run("without tenant ID", func(t *testing.T) {
			tableConfig := TableConfig{
				Name:          "test_table",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SchemaName: "test",
				Tables: []TableConfig{
					tableConfig,
				},
			}

			sql, err := manager.generatePartitionSQL("test_table_20240315", tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.test_table_20240315 PARTITION OF test.test_table FOR VALUES FROM ('2024-03-15') TO ('2024-03-16');")
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := TableConfig{
				Name:          "test_table",
				TenantId:      "tenant1",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SchemaName: "test",
				Tables: []TableConfig{
					tableConfig,
				},
			}

			sql, err := manager.generatePartitionSQL("test_table_20240315", tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.test_table_20240315 PARTITION OF test.test_table FOR VALUES FROM ('tenant1', '2024-03-15') TO ('tenant1', '2024-03-16');")
		})
	})

	t.Run("PartitionExists", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		clock := NewSimulatedClock(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					RetentionPeriod:   OneDay,
					PartitionInterval: OneDay,
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PreCreateCount:    2,
				},
			},
		}

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		exists, err := manager.partitionExists(context.Background(), "test_table_20240315")
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("CreateFuturePartitionsWithTenantId", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		// Use the tenant ID table schema instead
		_, err := db.ExecContext(context.Background(), createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, context.Background(), db)

		tableConfig := TableConfig{
			Name:              "test_table",
			TenantId:          "tenant1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PreCreateCount:    5,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		ctx := context.Background()

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Verify partitions were created
		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_tenant1%")
		require.NoError(t, err)
		require.Equal(t, tableConfig.PreCreateCount, partitionCount)

		// Verify the partition naming format
		var partitionName string
		err = db.GetContext(ctx, &partitionName, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1 LIMIT 1", "test_table_tenant1%")
		require.NoError(t, err)
		require.Contains(t, partitionName, "tenant1", "Partition name should include tenant ID")

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.partition_management where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("CreateFuturePartitionsForMultipleTenants", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		// Use the tenant ID table schema instead
		_, err := db.ExecContext(context.Background(), createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, context.Background(), db)

		tenantOneConfig := TableConfig{
			Name:              "test_table",
			TenantId:          "tenant_1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PreCreateCount:    5,
		}
		tenantTwoConfig := TableConfig{
			Name:              "test_table",
			TenantId:          "tenant_2",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PreCreateCount:    5,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				tenantOneConfig,
				tenantTwoConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		ctx := context.Background()

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		partitionNames := []string{
			"test_table_%s_20240101",
			"test_table_%s_20240102",
			"test_table_%s_20240103",
			"test_table_%s_20240104",
			"test_table_%s_20240105",
		}

		// Verify partitions were created for tenant 1
		for _, tableConfig := range config.Tables {
			var partitionCount uint
			err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("test_table_%s%%", tableConfig.TenantId))
			require.NoError(t, err)
			require.Equal(t, tableConfig.PreCreateCount, partitionCount)

			// Verify the partition naming format
			rows, err := db.QueryxContext(ctx, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("test_table_%s%%", tableConfig.TenantId))
			require.NoError(t, err)

			var partitions []string
			for rows.Next() {
				var name string
				err = rows.Scan(&name)
				require.NoError(t, err)
				partitions = append(partitions, name)
			}

			for i, partition := range partitions {
				require.Equal(t, fmt.Sprintf(partitionNames[i], tableConfig.TenantId), partition)
				require.Contains(t, partition, tableConfig.TenantId, "Partition name should include tenant ID")
			}

			var exists bool
			err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.partition_management where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			err = rows.Close()
			require.NoError(t, err)
		}
	})

	t.Run("TestManagerWithRealClock", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Setup manager config with two tenants
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PreCreateCount:    2,
				},
				{
					Name:              "test_table",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PreCreateCount:    2,
				},
			},
		}

		logger := slog.Default()
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize manager
		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Insert test data for both tenants
		insertSQL := `INSERT INTO test.test_table (id, project_id, created_at) VALUES ($1, $2, $3)`

		for i := 0; i < 10; i++ {
			// Insert for tenant_1
			_, err = db.ExecContext(ctx, insertSQL,
				ulid.Make().String(), "tenant_1",
				now.Add(time.Duration(i)*time.Hour),
			)
			require.NoError(t, err)

			// Insert for tenant_2
			_, err = db.ExecContext(ctx, insertSQL,
				ulid.Make().String(), "tenant_2",
				now.Add(time.Duration(i)*time.Hour),
			)
			require.NoError(t, err)
		}

		// Verify initial data
		for _, tenantID := range []string{"tenant_1", "tenant_2"} {
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM test.test_table WHERE project_id = $1", tenantID)
			require.NoError(t, err)
			require.Equal(t, 10, count)
		}

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 24)

		// Wait for maintenance to complete
		time.Sleep(time.Second)

		// Stop the manager
		manager.Stop()

		// Verify that old data has been cleaned up
		for _, tenantID := range []string{"tenant_1", "tenant_2"} {
			var count int
			err = db.GetContext(ctx, &count,
				"SELECT COUNT(*) FROM test.test_table WHERE project_id = $1",
				tenantID,
			)
			require.NoError(t, err)
			require.Equal(t, 0, count)
		}
	})

	t.Run("AddManagedTable", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Initial configuration with two tenants
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []TableConfig{
				{
					Name:              "test_table",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PreCreateCount:    2,
				},
				{
					Name:              "test_table",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PreCreateCount:    2,
				},
			},
		}

		logger := slog.Default()
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize manager
		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Add a new managed table
		newTableConfig := TableConfig{
			Name:              "test_table",
			TenantId:          "tenant_3",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   TimeDuration(1 * time.Minute),
			PreCreateCount:    2,
		}
		err = manager.AddManagedTable(newTableConfig)
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.test_table (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant_3",
				now.Add(time.Duration(i)*time.Minute),
			)
			require.NoError(t, err)
		}

		clock.AdvanceTime(time.Hour)

		var count int
		err = db.QueryRowxContext(ctx, "select count(*) from test.test_table where project_id = $1;", newTableConfig.TenantId).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, count, 5)

		// Run retention for the tenant
		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.test_table where project_id = $1);", newTableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
