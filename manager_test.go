package partition

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
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
			Tables:     []TableConfig{},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)
		assert.NoError(t, err)
		assert.NotNil(t, manager)
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
					TenantId:          "test_tenant",
					PartitionBy:       []string{"created_at"},
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					PreCreateCount:    2,
					RetentionPeriod:   OneWeek,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Initialize(context.Background(), config)
		assert.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partition_management WHERE table_name = $1", "test_table")
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("CreateFuturePartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		tableConfig := TableConfig{
			Name:              "test_table",
			TenantId:          "test_tenant",
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
		assert.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		assert.NoError(t, err)
		assert.Equal(t, tableConfig.PreCreateCount, partitionCount)
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
					TenantId:          "test_tenant",
					PartitionType:     TypeRange,
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
		assert.NoError(t, err)

		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		assert.NoError(t, err)
		assert.Equal(t, 2, partitionCount)
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
					TenantId:          "test_tenant",
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

		err = manager.Maintain(context.Background())
		assert.NoError(t, err)

		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "test_table_%")
		assert.NoError(t, err)
		assert.Greater(t, partitionCount, 0)
	})

	t.Run("GeneratePartitionName", func(t *testing.T) {
		manager := &Manager{}
		tableConfig := TableConfig{
			Name: "test_table",
		}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		name := manager.generatePartitionName(tableConfig, bounds)
		assert.Equal(t, "test_table_20240315", name)
	})

	t.Run("GeneratePartitionSQL", func(t *testing.T) {
		manager := &Manager{}
		tableConfig := TableConfig{
			Name:          "test_table",
			PartitionType: TypeRange,
		}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		sql, err := manager.generatePartitionSQL("test_table_20240315", tableConfig, bounds)
		assert.NoError(t, err)
		assert.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test_table_20240315 PARTITION OF test_table FOR VALUES FROM ('2024-03-15') TO ('2024-03-16');")
	})

	t.Run("PartitionExists", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		logger := slog.Default()
		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(db, Config{}, logger, clock)
		require.NoError(t, err)

		exists, err := manager.partitionExists(context.Background(), "nonexistent_partition")
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}
