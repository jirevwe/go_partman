package partition

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"log/slog"
	"testing"
	"time"
)

var createTestTableQuery = `
CREATE TABLE if not exists test_table (
    id VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at, tenant_id)
) PARTITION BY RANGE (created_at);`

var createTestTableWithTenantIdQuery = `
CREATE TABLE if not exists test_table (
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
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test_table")
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

	_, err := db.Exec("DROP TABLE IF EXISTS partition_management")
	require.NoError(t, err)
}

func TestManager(t *testing.T) {
	t.Run("NewManager", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		logger := slog.Default()
		config := Config{
			SchemaName: "public",
			Tables: []TableConfig{
				{
					Name:            "test_table",
					RetentionPeriod: OneDay,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)
		require.NoError(t, err)
		require.NotNil(t, manager)
	})

	t.Run("Initialize", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := Config{
			SchemaName: "public",
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

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(context.Background(), config)
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partition_management WHERE table_name = $1", "test_table")
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
		config := Config{
			SchemaName: "public",
			Tables: []TableConfig{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.CreateFuturePartitions(context.Background(), tableConfig, tableConfig.PreCreateCount)
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
		config := Config{
			SchemaName: "public",
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

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(context.Background(), config)
		require.NoError(t, err)

		clock.AdvanceTime(time.Hour + time.Minute)

		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)
	})

	t.Run("Maintain", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := Config{
			SchemaName: "public",
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

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(context.Background(), config)
		require.NoError(t, err)

		err = manager.Maintain(context.Background())
		require.NoError(t, err)

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
			sql, err := manager.generatePartitionSQL("test_table_20240315", tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test_table_20240315 PARTITION OF test_table FOR VALUES FROM ('2024-03-15') TO ('2024-03-16');")
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := TableConfig{
				Name:          "test_table",
				TenantId:      "tenant1",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			sql, err := manager.generatePartitionSQL("test_table_20240315", tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test_table_20240315 PARTITION OF test_table FOR VALUES FROM ('tenant1', '2024-03-15') TO ('tenant1', '2024-03-16');")
		})
	})

	t.Run("PartitionExists", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		clock := NewSimulatedClock(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
		config := Config{
			SchemaName: "public",
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

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(context.Background(), config)
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
		config := Config{
			SchemaName: "public",
			Tables: []TableConfig{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		ctx := context.Background()

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(ctx, config)
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
		err = db.QueryRowxContext(ctx, "select exists(select 1 from partition_management where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
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
		config := Config{
			SchemaName: "public",
			Tables: []TableConfig{
				tenantOneConfig,
				tenantTwoConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		ctx := context.Background()

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(ctx, config)
		require.NoError(t, err)

		// Verify partitions were created for tenant 1
		for _, tableConfig := range config.Tables {
			var partitionCount uint
			err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("test_table_%s%%", tableConfig.TenantId))
			require.NoError(t, err)
			require.Equal(t, tableConfig.PreCreateCount, partitionCount)

			// Verify the partition naming format
			var partitionName string
			err = db.GetContext(ctx, &partitionName, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1 LIMIT 1", fmt.Sprintf("test_table_%s%%", tableConfig.TenantId))
			require.NoError(t, err)
			require.Contains(t, partitionName, tableConfig.TenantId, "Partition name should include tenant ID")

			var exists bool
			err = db.QueryRowxContext(ctx, "select exists(select 1 from partition_management where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)
		}
	})
}
