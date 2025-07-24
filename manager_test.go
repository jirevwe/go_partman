package partman

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

var createParentTableQuery = `
CREATE TABLE if not exists test.user_logs (
    id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data JSONB,
    PRIMARY KEY (id, project_id, created_at)
) PARTITION BY RANGE (project_id, created_at);`

var createParentTableWithoutTenantQuery = `
CREATE TABLE if not exists test.delivery_attempts (
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);`

func createParentTable(t *testing.T, ctx context.Context, db *sqlx.DB) {
	dropParentTables(t, ctx, db)
	_, err := db.ExecContext(ctx, createParentTableQuery)
	require.NoError(t, err)
}

func createParentTableWithoutTenant(t *testing.T, ctx context.Context, db *sqlx.DB) {
	_, err := db.ExecContext(ctx, createParentTableWithoutTenantQuery)
	require.NoError(t, err)
}

func dropParentTables(t *testing.T, ctx context.Context, db *sqlx.DB) {
	tx, err := db.Beginx()
	if err != nil {
		require.NoError(t, err)
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS test.user_logs")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS test.delivery_attempts")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func setupTestDB(t *testing.T, ctx context.Context) *sqlx.DB {
	t.Helper()

	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/test?sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	require.NoError(t, err)

	sqlDB := stdlib.OpenDBFromPool(pool)
	db := sqlx.NewDb(sqlDB, "pgx")

	cleanupPartManDBs(t, db)

	return db
}

// cleanupPartManDBs clean up parent tables
func cleanupPartManDBs(t *testing.T, db *sqlx.DB) {
	t.Helper()

	tx, err := db.Beginx()
	if err != nil {
		require.NoError(t, err)
		t.Fatal(err)
	}

	ctx := context.Background()

	// Drop tables in the correct order to handle dependencies
	_, err = tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS partman CASCADE")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP TRIGGER IF EXISTS validate_tenant_id_trigger ON partman.partitions")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP FUNCTION IF EXISTS partman.validate_tenant_id()")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS partman.partitions CASCADE")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS partman.tenants CASCADE")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS partman.parent_tables CASCADE")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func TestManager(t *testing.T) {
	t.Run("CreateNewManager", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)
		require.NotNil(t, m)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("Initialize", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Schema:            "test",
					Name:              "user_logs",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		_, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, count)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("Error - DB must not be nil", func(t *testing.T) {
		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.Error(t, err)
		require.Nil(t, manager)
		require.Equal(t, ErrDbDriverMustNotBeNil, err)
	})

	t.Run("Error - Logger must not be nil", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithConfig(config),
			WithClock(clock),
		)
		require.Error(t, err)
		require.Nil(t, manager)
		require.Equal(t, ErrLoggerMustNotBeNil, err)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("Error - Config must not be nil", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithClock(clock),
		)
		require.Error(t, err)
		require.Nil(t, manager)
		require.Equal(t, ErrConfigMustNotBeNil, err)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("Error - Clock must not be nil", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(config),
		)
		require.Error(t, err)
		require.Nil(t, manager)
		require.Equal(t, ErrClockMustNotBeNil, err)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateFuturePartitions", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    10,
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		result, err := m.RegisterTenant(ctx, Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		})
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateFuturePartitionsWithExistingPartitionsInRange", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		_, err := db.ExecContext(ctx, `
			CREATE TABLE if not exists test.user_logs_TENANT1_20240101 PARTITION OF test.user_logs
	    	FOR VALUES FROM ('TENANT1', '2024-01-01 00:00:00+00'::timestamptz) TO ('TENANT1', '2024-01-02 00:00:00+00'::timestamptz);`)
		require.NoError(t, err)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    10,
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		err = m.Maintain(ctx)
		require.NoError(t, err)

		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateFuturePartitionsWithExistingPartitionsOutOfRange", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		_, err := db.ExecContext(ctx, `
			CREATE TABLE if not exists test.user_logs_tenant2_20231201 PARTITION OF test.user_logs
	    	FOR VALUES FROM ('tenant2', '2024-11-01 00:00:00+00'::timestamptz) TO ('tenant2','2024-11-02 00:00:00+00'::timestamptz);`)
		require.NoError(t, err)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 365,
			PartitionCount:    10,
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount+1, partitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("DropOldPartitions", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		_, err := db.ExecContext(ctx, `
			CREATE TABLE if not exists test.user_logs_tenant1_20231201 PARTITION OF test.user_logs
	    	FOR VALUES FROM ('tenant1', '2024-11-01 00:00:00+00'::timestamptz) TO ('tenant1','2024-11-02 00:00:00+00'::timestamptz);`)
		require.NoError(t, err)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour,
			PartitionCount:    2,
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: 0,
			Tables: []Table{
				tableConfig,
			},
		}
		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		result, err := m.RegisterTenant(ctx, Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		})
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}
		clock.AdvanceTime(time.Hour * 2)

		var oldPartitionCount int
		err = db.GetContext(ctx, &oldPartitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, 3, oldPartitionCount)

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		var newPartitionCount int
		err = db.GetContext(ctx, &newPartitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, 2, newPartitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("TestMaintainer", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Schema:            "test",
			PartitionType:     TypeRange,
			Name:              "user_logs",
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		result, err := m.RegisterTenant(ctx, Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		})
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		// Verify partitions were created
		var partitionCount int
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Greater(t, partitionCount, 0)
		require.Equal(t, 2, partitionCount)

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 24)

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		// Verify that a new partition was created
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", fmt.Sprintf("%s_%%", tableConfig.Name), tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, 3, partitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("GeneratePartitionName", func(t *testing.T) {
		m := &Manager{}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		t.Run("without tenant ID", func(t *testing.T) {
			tableConfig := Tenant{
				TableName: "user_logs",
			}
			name := m.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "user_logs_20240315", name)
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := Tenant{
				TableName: "user_logs",
				TenantId:  "TENANT1",
			}
			name := m.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "user_logs_TENANT1_20240315", name)
		})
	})

	t.Run("GeneratePartitionSQL", func(t *testing.T) {
		m := &Manager{}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		t.Run("without tenant ID", func(t *testing.T) {
			tenant := Tenant{
				TableName:   "user_logs",
				TableSchema: "test",
			}

			tableConfig := Table{
				Name:          "user_logs",
				Schema:        "test",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}

			m.config = &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := m.generatePartitionName(tenant, bounds)
			require.Equal(t, "user_logs_20240315", partitionName)

			sql, err := m.generatePartitionSQL(partitionName, tableConfig, tenant, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.user_logs_20240315 PARTITION OF test.user_logs FOR VALUES FROM ('2024-03-15 00:00:00+00'::timestamptz) TO ('2024-03-16 00:00:00+00'::timestamptz);")
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tenant := Tenant{
				TableName:   "user_logs",
				TableSchema: "test",
				TenantId:    "TENANT1",
			}

			tableConfig := Table{
				Name:          "user_logs",
				Schema:        "test",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			m.config = &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := m.generatePartitionName(tenant, bounds)
			require.Equal(t, "user_logs_TENANT1_20240315", partitionName)

			sql, err := m.generatePartitionSQL(partitionName, tableConfig, tenant, bounds)
			require.NoError(t, err)
			require.Equal(t, "CREATE TABLE IF NOT EXISTS test.user_logs_TENANT1_20240315 PARTITION OF test.user_logs FOR VALUES FROM ('TENANT1', '2024-03-15 00:00:00+00'::timestamptz) TO ('TENANT1', '2024-03-16 00:00:00+00'::timestamptz);", sql)
		})
	})

	t.Run("PartitionExists", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			RetentionPeriod:   time.Hour * 24,
			PartitionInterval: time.Hour * 24,
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionCount:    2,
		}

		clock := NewSimulatedClock(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		result, err := m.RegisterTenant(ctx, Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant3",
		})
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		exists, err := m.partitionExists(ctx, "user_logs_tenant3_20240315", "test")
		require.NoError(t, err)
		require.True(t, exists)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("RegisterTenant", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24,
					PartitionCount:    2,
				},
			},
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Add a new managed table
		tableConfig := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant3",
		}
		result, err := m.RegisterTenant(ctx, tableConfig)
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.user_logs (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant3",
				now.Add(time.Duration(i)*time.Minute),
			)
			require.NoError(t, err)
		}

		var count int
		err = db.QueryRowxContext(ctx, "select count(*) from test.user_logs where project_id = $1;", tableConfig.TenantId).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, count, 5)

		clock.AdvanceTime(time.Hour * 25)

		// Run retention for the tenant
		err = m.Maintain(ctx)
		require.NoError(t, err)

		t.Log(clock.Now())

		// Verify that the partitions still equal 2
		err = db.Get(&partitionCount, "SELECT count(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.user_logs where project_id = $1);", tableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)

		// Verify that a new partition was created
		var partitionTableNames []string
		err = db.Select(&partitionTableNames, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1 order by tablename", "user_logs_tenant3%")
		require.NoError(t, err)
		require.Equal(t, "user_logs_tenant3_20240102", partitionTableNames[0])
		require.Equal(t, "user_logs_tenant3_20240103", partitionTableNames[1])

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateFuturePartitionsWithTenantId", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}

		tenant := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "TENANT3",
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		result, err := m.RegisterTenant(ctx, tenant)
		require.NoError(t, err)

		if len(result.Errors) > 0 {
			require.Fail(t, "expected no errors, but got some anyway", result.Errors)
		}

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		// Verify partitions were created
		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant3%")
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)

		// Verify the partition naming format
		var partitionName string
		err = db.GetContext(ctx, &partitionName, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1 LIMIT 1", "user_logs_tenant3%")
		require.NoError(t, err)
		require.Contains(t, partitionName, "tenant3", "Partition name should include tenant ID")

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.tenants where id = $1);", tenant.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateFuturePartitionsForMultipleTenants", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}

		tenant1 := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "TENANT1",
		}

		tenant2 := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "TENANT2",
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: 30 * time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithClock(clock),
			WithConfig(config),
		)
		require.NoError(t, err)

		err = m.Start(ctx)
		require.NoError(t, err)

		results, err := m.RegisterTenants(ctx, tenant1, tenant2)
		require.NoError(t, err)

		for _, result := range results {
			if len(result.Errors) > 0 {
				require.Fail(t, "expected no errors, but got some anyway", result.Errors)
			}
		}

		// Wait for maintenance to run
		err = m.Maintain(ctx)
		require.NoError(t, err)

		partitionNames := []string{
			"user_logs_%s_20240101",
			"user_logs_%s_20240102",
			"user_logs_%s_20240103",
			"user_logs_%s_20240104",
			"user_logs_%s_20240105",
		}

		// Verify partitions were created for tenant 1
		for _, tenant := range []Tenant{tenant1, tenant2} {
			var partitionCount uint
			tableName := fmt.Sprintf("user_logs_%s%%", strings.ToLower(tenant.TenantId))
			err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", tableName)
			require.NoError(t, err)
			require.Equal(t, tableConfig.PartitionCount, partitionCount)

			// Verify the partition naming format
			rows, err := db.QueryxContext(ctx, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("user_logs_%s%%", tenant.TenantId))
			require.NoError(t, err)

			var partitions []string
			for rows.Next() {
				var name string
				err = rows.Scan(&name)
				require.NoError(t, err)
				partitions = append(partitions, name)
			}

			for i, partition := range partitions {
				require.Equal(t, fmt.Sprintf(partitionNames[i], tenant1.TenantId), partition)
				require.Contains(t, partition, tenant.TenantId, "Partition name should include tenant ID")
			}

			var exists bool
			err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.tenants where id = $1);", tenant.TenantId).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			err = rows.Close()
			require.NoError(t, err)
		}

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("TestMaintainerWithTwoTenants", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		tableConfig := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24,
			PartitionCount:    2,
		}

		tenant1 := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "TENANT_1",
		}

		tenant2 := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "TENANT_2",
		}

		// Setup manager config with two tenants
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize the manager
		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		results, err := m.RegisterTenants(ctx, tenant1, tenant2)
		require.NoError(t, err)

		for _, result := range results {
			if len(result.Errors) > 0 {
				require.Fail(t, "expected no errors, but got some anyway", result.Errors)
			}
		}

		// Insert test data for both tenants
		insertSQL := `INSERT INTO test.user_logs (id, project_id, created_at, data) VALUES ($1, $2, $3,
					  jsonb_build_object('session_id', 'session_0', 'ip_address', '192.168.1.100'));`

		for i := 0; i < 10; i++ {
			// Insert for tenant_1
			_, err = db.ExecContext(ctx, insertSQL,
				ulid.Make().String(), tenant1.TenantId,
				now.Add(time.Duration(i)*time.Hour),
			)
			require.NoError(t, err)

			// Insert for tenant_2
			_, err = db.ExecContext(ctx, insertSQL,
				ulid.Make().String(), tenant2.TenantId,
				now.Add(time.Duration(i)*time.Hour),
			)
			require.NoError(t, err)
		}

		// Wait for maintenance to complete
		err = m.Maintain(ctx)
		require.NoError(t, err)

		// Verify initial data
		for _, tenant := range []Tenant{tenant1, tenant2} {
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM test.user_logs WHERE project_id = $1", tenant.TenantId)
			require.NoError(t, err)
			require.Equal(t, 10, count)
		}

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 25)

		// Wait for maintenance to complete again
		err = m.Maintain(ctx)
		require.NoError(t, err)

		// Verify that old data has been cleaned up
		for _, tenant := range []Tenant{tenant1, tenant2} {
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM test.user_logs WHERE project_id = $1", tenant.TenantId)
			require.NoError(t, err)
			require.Equal(t, 0, count)
		}

		// Stop the manager
		m.Stop()

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("importExistingPartitions", func(t *testing.T) {
		t.Run("Successfully import existing partitions", func(t *testing.T) {
			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			// Create some existing partitions manually
			partitions := []string{
				`CREATE TABLE test.user_logs_tenant1_20240101 PARTITION OF test.user_logs FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				`CREATE TABLE test.user_logs_tenant1_20240102 PARTITION OF test.user_logs FOR VALUES FROM ('TENANT1', '2024-01-02') TO ('TENANT1', '2024-01-03')`,
				`CREATE TABLE test.user_logs_tenant2_20240101 PARTITION OF test.user_logs FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err := db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			tableConfig := Table{
				Name:              "user_logs",
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24,
				PartitionCount:    2,
			}

			// Initialize manager
			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			_, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			// Verify partitions were imported correctly
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.tenants")
			require.NoError(t, err)
			require.Equal(t, 2, count)

			// Verify specific tenant entries
			var exists bool
			err = db.QueryRowContext(ctx, `
					SELECT EXISTS(
						SELECT 1 FROM partman.partitions
						WHERE tenant_id = 'TENANT1'
					)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			err = db.QueryRowContext(ctx, `
					SELECT EXISTS(
						SELECT 1 FROM partman.partitions
						WHERE tenant_id = 'tenant2'
					)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("Import with invalid partition format", func(t *testing.T) {
			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			// Create an invalid partition (wrong naming format)
			_, err := db.ExecContext(ctx, `
					CREATE TABLE test.user_logs_invalid_format PARTITION OF test.user_logs
					FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`)
			require.NoError(t, err)

			tableConfig := Table{
				Name:              "user_logs",
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24,
				PartitionCount:    2,
			}

			// Initialize manager
			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			_, err = NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.Error(t, err)
			require.ErrorContains(t, err, "parsing time \"\" as \"20060102\": cannot parse \"\" as \"2006\"")

			// Verify no partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 0, count)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("Import partitions with multiple date formats", func(t *testing.T) {
			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			tableConfig := Table{
				Name:              "user_logs",
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24,
				PartitionCount:    2,
			}

			// Create partitions with different date formats
			partitions := []string{
				// Standard format
				`CREATE TABLE test.user_logs_tenant1_20240101 PARTITION OF test.user_logs
					 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Different month and day
				`CREATE TABLE test.user_logs_tenant1_20241231 PARTITION OF test.user_logs
					 FOR VALUES FROM ('TENANT1', '2024-12-31') TO ('TENANT1', '2025-01-01')`,
				// Another tenant
				`CREATE TABLE test.user_logs_tenant2_20240101 PARTITION OF test.user_logs
					 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err := db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			_, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			// Verify all partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 3, count)

			// Verify partition dates were correctly parsed
			var exists bool
			err = db.QueryRowContext(ctx, `
					SELECT EXISTS(
						SELECT 1 FROM pg_tables
						WHERE tablename = 'user_logs_tenant1_20241231'
						AND schemaname = 'test'
					)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("Import partitions with existing management entries", func(t *testing.T) {
			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			// Create a partition
			_, err := db.ExecContext(ctx, `
					CREATE TABLE test.user_logs_tenant1_20240101 PARTITION OF test.user_logs
					FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`)
			require.NoError(t, err)

			// Create partman schema
			_, err = db.ExecContext(ctx, createPartmanSchema)
			require.NoError(t, err)

			// Create parent_tables table
			_, err = db.ExecContext(ctx, createParentsTable)
			require.NoError(t, err)

			// Create an existing management entry
			_, err = db.ExecContext(ctx, `
					INSERT INTO partman.parent_tables (
						id, table_name, schema_name, tenant_column,
						partition_by, partition_type, partition_interval,
						retention_period, partition_count
					) VALUES (
						$1, 'user_logs', 'test', 'project_id',
						'created_at', 'range', '24h', '720h', 10
					)`, ulid.Make().String())
			require.NoError(t, err)

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					{
						Name:              "user_logs",
						Schema:            "test",
						TenantIdColumn:    "project_id",
						PartitionBy:       "created_at",
						PartitionType:     TypeRange,
						PartitionInterval: time.Hour * 24,
						RetentionPeriod:   time.Hour * 24,
					},
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			_, err = NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			// Verify only one management entry exists (no duplicates)
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.parent_tables")
			require.NoError(t, err)
			require.Equal(t, 1, count)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("Import partitions with mixed valid and invalid names", func(t *testing.T) {
			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			// Create mixed partitions
			partitions := []string{
				// Valid partition
				`CREATE TABLE test.user_logs_tenant1_20240101 PARTITION OF test.user_logs
					 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Invalid format but valid partition
				`CREATE TABLE test.user_logs_invalid_tenant1 PARTITION OF test.user_logs
					 FOR VALUES FROM ('TENANT1', '2024-01-02') TO ('TENANT1', '2024-01-03')`,
				// Another valid partition
				`CREATE TABLE test.user_logs_tenant2_20240101 PARTITION OF test.user_logs
					 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err := db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					{
						Name:              "user_logs",
						Schema:            "test",
						TenantIdColumn:    "project_id",
						PartitionBy:       "created_at",
						PartitionType:     TypeRange,
						PartitionInterval: time.Hour * 24,
						RetentionPeriod:   time.Hour * 24,
					},
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			_, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.Error(t, err)
			require.ErrorContains(t, err, "parsing time \"\" as \"20060102\": cannot parse \"\" as \"2006\"")

			// Verify only valid partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 2, count) // Should only import the valid partitions

			// Verify specific valid partitions were imported
			var exists bool
			err = db.QueryRowContext(ctx, `
					SELECT EXISTS(
						SELECT 1 FROM partman.partitions
						WHERE tenant_id IN ('TENANT1', 'tenant2')
					)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("importExistingPartitions with mixed tenant and non-tenant tables", func(t *testing.T) {
			t.Skip("I'll add support for tables without tenant_id columns in the future")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			// Create test tables - one with tenant support, one without
			_, err := db.ExecContext(ctx, `
					CREATE TABLE if not exists test.user_logs_with_tenant (
						id VARCHAR NOT NULL,
						project_id VARCHAR NOT NULL,
						created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
						PRIMARY KEY (id, created_at, project_id)
					) PARTITION BY RANGE (project_id, created_at);
	
					CREATE TABLE if not exists test.user_logs_no_tenant (
						id VARCHAR NOT NULL,
						created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
						PRIMARY KEY (id, created_at)
					) PARTITION BY RANGE (created_at);
				`)
			require.NoError(t, err)
			defer func() {
				_, err = db.ExecContext(ctx, `
						DROP TABLE IF EXISTS test.user_logs_with_tenant;
						DROP TABLE IF EXISTS test.user_logs_no_tenant;
					`)
				require.NoError(t, err)
			}()

			// Create mixed partitions
			partitions := []string{
				// Tenant partitions
				`CREATE TABLE if not exists test.user_logs_with_tenant_tenant1_20240101 PARTITION OF test.user_logs_with_tenant
					 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Non-tenant partitions
				`CREATE TABLE if not exists test.user_logs_no_tenant_20240101 PARTITION OF test.user_logs_no_tenant
					 FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
				`CREATE TABLE if not exists test.user_logs_no_tenant_20240102 PARTITION OF test.user_logs_no_tenant
					 FOR VALUES FROM ('2024-01-02') TO ('2024-01-03')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					{
						Name:              "user_logs_with_tenant",
						Schema:            "test",
						TenantIdColumn:    "project_id",
						PartitionBy:       "created_at",
						PartitionType:     TypeRange,
						PartitionInterval: time.Hour * 24,
						RetentionPeriod:   time.Hour * 24,
					},
					{
						Name:              "user_logs_no_tenant",
						Schema:            "test",
						PartitionBy:       "created_at",
						PartitionType:     TypeRange,
						PartitionInterval: time.Hour * 24,
						RetentionPeriod:   time.Hour * 24,
					},
				},
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			// Import existing partitions
			err = m.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "tenant_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			err = m.Maintain(ctx)
			require.NoError(t, err)

			// Verify both tenant and non-tenant partitions were imported
			var managedTables []struct {
				TableName string `db:"table_name"`
				TenantID  string `db:"tenant_id"`
			}
			err = db.SelectContext(ctx, &managedTables, `
					SELECT pt.table_name, p.tenant_id
					FROM partman.partitions p 
					join partman.parent_tables pt on 
					pt.id = p.parent_table_id`)
			t.Logf("%+v", managedTables)
			require.NoError(t, err)
			require.Len(t, managedTables, 2)

			for _, m := range managedTables {
				if m.TableName == "user_logs_with_tenant" {
					require.Equal(t, "TENANT1", m.TenantID)
				} else {
					require.Empty(t, m.TenantID)
				}
			}

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})

		t.Run("importExistingPartitions with only non-tenant tables", func(t *testing.T) {
			t.Skip("I'll add support for tables without tenant_id columns in the future")

			ctx := context.Background()
			db := setupTestDB(t, ctx)
			createParentTable(t, ctx, db)

			_, err := db.ExecContext(ctx, `
					CREATE TABLE test.user_logs (
						id VARCHAR NOT NULL,
						created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
						PRIMARY KEY (id, created_at)
					) PARTITION BY RANGE (created_at);
				`)
			require.NoError(t, err)
			defer func() {
				_, err = db.ExecContext(ctx, `DROP TABLE IF EXISTS test.user_logs;`)
				require.NoError(t, err)
			}()

			// Create non-tenant partitions
			partitions := []string{
				`CREATE TABLE test.user_logs_20240101 PARTITION OF test.user_logs
					 FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
				`CREATE TABLE test.user_logs_20240102 PARTITION OF test.user_logs
					 FOR VALUES FROM ('2024-01-02') TO ('2024-01-03')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			// Import existing partitions
			err = m.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "tenant_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			// Verify non-tenant partitions were imported
			var count int
			err = db.GetContext(ctx, &count, `
					SELECT COUNT(*)
					FROM partman.parent_tables
					WHERE table_name = 'user_logs'`)
			require.NoError(t, err)
			require.Equal(t, 1, count)

			// Verify partition count
			err = db.GetContext(ctx, &count, `
					SELECT COUNT(*)
					FROM pg_tables
					WHERE schemaname = 'test'
					AND tablename LIKE 'user_logs_%'`)
			require.NoError(t, err)
			require.Equal(t, 2, count)

			dropParentTables(t, ctx, db)
			cleanupPartManDBs(t, db)
		})
	})
}

func TestParentTablesAPI(t *testing.T) {
	t.Run("CreateParentTable", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Verify that the parent table was created in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, count)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("CreateParentWithInvalidTable", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		parentTable := Table{
			Name:              "nonexistent_table",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.Error(t, err)
		require.Contains(t, err.Error(), "table validation failed")

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})

	t.Run("RegisterTenant", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)
		// First, create the parent table
		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    3,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Register a tenant
		tenant := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		}

		result, err := m.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "tenant1", result.TenantId)
		require.Equal(t, "user_logs", result.TableName)
		require.Equal(t, 3, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Verify that the tenant was registered in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.tenants WHERE id = $1", "tenant1")
		require.NoError(t, err)
		require.Equal(t, 1, count)

		// Verify partitions were created
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "user_logs_tenant1%")
		require.NoError(t, err)
		require.Equal(t, 3, partitionCount)
	})

	t.Run("RegisterTenantWithNonExistentParent", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		tenant := Tenant{
			TableName:   "nonexistent_table",
			TableSchema: "test",
			TenantId:    "tenant1",
		}

		result, err := m.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "tenant1", result.TenantId)
		require.NotEmpty(t, result.Errors)
		require.Contains(t, result.Errors[0].Error(), "parent table not found")
	})

	t.Run("RegisterMultipleTenants", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Create the parent table
		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    2,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		parentTableId, err := m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Register multiple tenants
		tenants := []Tenant{
			{
				TableName:   "user_logs",
				TableSchema: "test",
				TenantId:    "tenant1",
			},
			{
				TableName:   "user_logs",
				TableSchema: "test",
				TenantId:    "tenant2",
			},
			{
				TableName:   "user_logs",
				TableSchema: "test",
				TenantId:    "tenant3",
			},
		}

		results, err := m.RegisterTenants(context.Background(), tenants...)
		require.NoError(t, err)
		require.Len(t, results, 3)

		// Verify all tenants were registered successfully
		for i, result := range results {
			require.Equal(t, fmt.Sprintf("tenant%d", i+1), result.TenantId)
			require.Equal(t, "user_logs", result.TableName)
			require.Equal(t, 2, result.PartitionsCreated)
			require.Empty(t, result.Errors)
		}

		// Verify all tenants exist in the database
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.tenants WHERE parent_table_id = $1", parentTableId)
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
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Create multiple parent tables
		parentTables := []Table{
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
			_, err = m.CreateParentTable(context.Background(), pt)
			require.NoError(t, err)
		}

		// Get all parent tables
		retrievedTables, err := m.GetParentTables(context.Background())
		require.NoError(t, err)
		require.Len(t, retrievedTables, 2)

		// Verify the tables were retrieved correctly
		tableMap := make(map[string]Table)
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
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "user_logs",
					Schema:            "test",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 2,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)
		require.NotNil(t, m)

		// Create parent table
		_, err = m.CreateParentTable(ctx, config.Tables[0])
		require.NoError(t, err)

		// Test getting tenants when none exist
		tenants, err := m.GetTenants(ctx, "test", "user_logs")
		require.NoError(t, err)
		require.Empty(t, tenants)

		// Register multiple tenants
		for i := 1; i <= 3; i++ {
			tenant := Tenant{
				TableName:   "user_logs",
				TableSchema: "test",
				TenantId:    fmt.Sprintf("tenant%d", i),
			}
			result, err := m.RegisterTenant(ctx, tenant)
			require.NoError(t, err)
			require.Empty(t, result.Errors)
		}

		// Test getting all tenants
		tenants, err = m.GetTenants(ctx, "test", "user_logs")
		require.NoError(t, err)
		require.Len(t, tenants, 3)
		require.Equal(t, "tenant1", tenants[0].TenantId)
		require.Equal(t, "tenant2", tenants[1].TenantId)
		require.Equal(t, "tenant3", tenants[2].TenantId)

		// Test getting tenants for non-existent table
		tenants, err = m.GetTenants(ctx, "test", "non_existent")
		require.NoError(t, err)
		require.Empty(t, tenants)

		// Test getting tenants for non-existent schema
		tenants, err = m.GetTenants(ctx, "non_existent", "user_logs")
		require.NoError(t, err)
		require.Empty(t, tenants)
	})

	t.Run("GetTenantsForNonExistentTable", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		tenants, err := m.GetTenants(ctx, "test", "nonexistent_table")
		require.NoError(t, err)
		require.Empty(t, tenants)
	})

	t.Run("RegisterTenantWithExistingPartitions", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

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

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Create parent table
		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		}

		result, err := m.RegisterTenant(context.Background(), tenant)
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
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
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

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)
		require.NotNil(t, m)

		// Verify parent table was created
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.parent_tables WHERE table_name = $1", "user_logs")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("TenantRegistrationWithDataInsertion", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Create parent table
		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    3,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		}

		result, err := m.RegisterTenant(context.Background(), tenant)
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
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		createParentTable(t, context.Background(), db)
		defer dropParentTables(t, context.Background(), db)

		logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
		config := &Config{
			SampleRate: time.Second,
		}

		clock := NewSimulatedClock(time.Now())

		m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
		require.NoError(t, err)

		// Create parent table
		parentTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			PartitionCount:    2,
			RetentionPeriod:   time.Hour * 24 * 7,
		}

		_, err = m.CreateParentTable(context.Background(), parentTable)
		require.NoError(t, err)

		// Register tenant
		tenant := Tenant{
			TableName:   "user_logs",
			TableSchema: "test",
			TenantId:    "tenant1",
		}

		result, err := m.RegisterTenant(context.Background(), tenant)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate result structure
		require.Equal(t, "tenant1", result.TenantId)
		require.Equal(t, "user_logs", result.TableName)
		require.Equal(t, "test", result.TableSchema)
		require.Equal(t, 2, result.PartitionsCreated)
		require.Empty(t, result.Errors)

		// Test with invalid tenant (should return error in a result)
		invalidTenant := Tenant{
			TableName:   "nonexistent_table",
			TableSchema: "test",
			TenantId:    "tenant2",
		}

		invalidResult, err := m.RegisterTenant(context.Background(), invalidTenant)
		require.NoError(t, err)
		require.NotNil(t, invalidResult)
		require.Equal(t, "tenant2", invalidResult.TenantId)
		require.NotEmpty(t, invalidResult.Errors)
		require.Contains(t, invalidResult.Errors[0].Error(), "parent table not found")
	})
}

func TestTableDeduplication(t *testing.T) {
	t.Run("initialize deduplicates tables from DB and config", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		createParentTable(t, ctx, db)

		// Create the management table
		migrations := []string{
			createPartmanSchema,
			createParentsTable,
			createTenantsTable,
			createPartitionsTable,
		}
		for _, migration := range migrations {
			_, err := db.ExecContext(ctx, migration)
			require.NoError(t, err)
		}

		// Insert into the existing table in DB
		mTable := Table{
			Name:              "user_logs",
			Schema:            "test",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     "range",
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 31,
		}
		_, err := db.ExecContext(ctx, upsertParentTable,
			ulid.Make().String(),
			mTable.Name,
			mTable.Schema,
			mTable.TenantIdColumn,
			mTable.PartitionBy,
			mTable.PartitionType,
			mTable.PartitionInterval.String(),
			mTable.PartitionCount,
			mTable.RetentionPeriod.String(),
		)
		require.NoError(t, err)

		// Create new manager with same table in config but different settings
		configTable := mTable
		configTable.PartitionCount = 10
		m, err := NewManager(
			WithDB(db),
			WithLogger(NewSlogLogger()),
			WithConfig(&Config{
				SampleRate: time.Second,
				Tables:     []Table{configTable},
			}),
			WithClock(NewSimulatedClock(time.Now())),
		)
		require.NoError(t, err)

		// Verify only one table exists with config taking precedence
		require.Equal(t, 1, len(m.config.Tables))
		require.Equal(t, uint(10), m.config.Tables[0].PartitionCount)

		dropParentTables(t, ctx, db)
		cleanupPartManDBs(t, db)
	})
}

func TestGetConfig(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	expectedConfig := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(expectedConfig), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	actualConfig := m.GetConfig()
	require.Equal(t, *expectedConfig, actualConfig)

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestGetManagedTables(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Create parent table and register a tenant
	_, err = m.CreateParentTable(ctx, config.Tables[0])
	require.NoError(t, err)

	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "test_tenant",
	}
	_, err = m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)

	// Get managed tables
	tables, err := m.GetManagedTables(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tables)
	require.Equal(t, "user_logs", tables[0].Name)
	require.Equal(t, "test", tables[0].Schema)

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestGetPartitions(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Register a tenant and create some partitions
	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    ulid.Make().String(),
	}
	_, err = m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)

	// Get partitions
	partitions, err := m.GetPartitions(ctx, "test", "user_logs", 10, 0)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestGetParentTableInfo(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	info, err := m.GetParentTableInfo(ctx, "test", "user_logs")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, "user_logs", info.Name)

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestPartitionMapOperations(t *testing.T) {
	m := &Manager{
		mu:         &sync.RWMutex{},
		partitions: make(map[tableName]Partition),
	}

	// Add 10 partitions
	for i := 0; i < 10; i++ {
		key := tableName(fmt.Sprintf("test.user_logs_tenant%d", i))
		partition := Partition{
			Name: fmt.Sprintf("user_logs_tenant%d_20240101", i),
			ParentTable: Table{
				Name:   "user_logs",
				Schema: "test",
			},
			Bounds: Bounds{
				From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			TenantId: fmt.Sprintf("tenant%d", i),
		}
		m.addToPartitionMap(key, partition)
	}

	// Test concurrent access to add
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := tableName(fmt.Sprintf("test.user_logs_tenant%d", i))
			partition := Partition{
				Name: fmt.Sprintf("user_logs_tenant%d_20240101", i),
				ParentTable: Table{
					Name:   "user_logs",
					Schema: "test",
				},
				Bounds: Bounds{
					From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				},
				TenantId: fmt.Sprintf("tenant%d", i),
			}
			m.addToPartitionMap(key, partition)
		}(i)
	}
	wg.Wait()

	// Verify map size
	require.Len(t, m.partitions, 10)

	// Test concurrent access to remove
	wg = sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := tableName(fmt.Sprintf("test.user_logs_tenant%d", i))
			m.removePartitionFromMap(key)
		}(i)
	}
	wg.Wait()

	// Verify map size after removal
	require.Len(t, m.partitions, 5)
}

func TestGeneratePartitionSQL(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Test range partition
	sql, err := m.generatePartitionSQL("test_partition", config.Tables[0], Tenant{}, Bounds{
		From: time.Now(),
		To:   time.Now().Add(24 * time.Hour),
	})
	require.NoError(t, err)
	require.Contains(t, sql, "CREATE TABLE")

	// Test unsupported partition type
	_, err = m.generatePartitionSQL("test_partition", Table{PartitionType: "invalid"}, Tenant{}, Bounds{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported partition type")

	// Test list partition type
	_, err = m.generatePartitionSQL("test_partition", Table{PartitionType: "list"}, Tenant{}, Bounds{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "list and hash partitions are not implemented yet")

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestRunMigrations(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Test migration failure case
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS partman.parent_tables CASCADE;")
	require.NoError(t, err)

	err = m.runMigrations(ctx)
	require.NoError(t, err)

	// Test migration success case with existing table
	err = m.runMigrations(ctx)
	require.NoError(t, err)

	// Test migration with invalid SQL
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS partman.parent_tables")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE partman.parent_tables (invalid_column INT)")
	require.NoError(t, err)

	err = m.runMigrations(ctx)
	require.NoError(t, err)
}

func TestMaintain(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24 * 2,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Create parent table and register a tenant
	_, err = m.CreateParentTable(ctx, config.Tables[0])
	require.NoError(t, err)

	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "test_tenant",
	}
	_, err = m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)

	// Test maintain with no existing partitions
	err = m.Maintain(ctx)
	require.NoError(t, err)

	// Verify that partitions were created
	partitions, err := m.GetPartitions(ctx, "test", "user_logs", 10, 0)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	// Test maintain with existing partitions
	err = m.Maintain(ctx)
	require.NoError(t, err)

	// Test maintain after advancing time
	clock.AdvanceTime(time.Hour * 24)
	err = m.Maintain(ctx)
	require.NoError(t, err)

	// Test maintain with retention period
	clock.AdvanceTime(time.Hour * 24 * 3)
	err = m.Maintain(ctx)
	require.NoError(t, err)
}

func TestBuildTableName(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		table    string
		tenantId string
		expected tableName
	}{
		{
			name:     "with schema and tenant",
			schema:   "test",
			table:    "user_logs",
			tenantId: "tenant1",
			expected: tableName("test.user_logs_tenant1"),
		},
		{
			name:     "without schema with tenant",
			schema:   "",
			table:    "user_logs",
			tenantId: "tenant1",
			expected: tableName("public.user_logs_tenant1"),
		},
		{
			name:     "with special characters",
			schema:   "test-schema",
			table:    "user_logs-2023",
			tenantId: "tenant-1",
			expected: tableName("test-schema.user_logs-2023_tenant-1"),
		},
		{
			name:     "empty tenant",
			schema:   "test",
			table:    "user_logs",
			tenantId: "",
			expected: tableName("test.user_logs"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTableName(tt.schema, tt.table, tt.tenantId)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRegisterTenantExtended(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Second,
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24 * 2,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Create parent table
	_, err = m.CreateParentTable(ctx, config.Tables[0])
	require.NoError(t, err)

	// Test registering a valid tenant
	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "test_tenant",
	}
	result, err := m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, tenant.TenantId, result.TenantId)
	require.Equal(t, tenant.TableName, result.TableName)
	require.Equal(t, tenant.TableSchema, result.TableSchema)
	require.Equal(t, 2, result.PartitionsCreated)
	require.Empty(t, result.Errors)

	// Test registering the same tenant again (should return result with no errors since we perform an upsert operation)
	result, err = m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, tenant.TenantId, result.TenantId)
	require.Equal(t, tenant.TableName, result.TableName)
	require.Equal(t, tenant.TableSchema, result.TableSchema)
	require.Equal(t, 2, result.PartitionsCreated)
	require.Empty(t, result.Errors)

	// Test registering a tenant with invalid table name
	invalidTenant := Tenant{
		TableName:   "non_existent",
		TableSchema: "test",
		TenantId:    "test_tenant2",
	}
	result, err = m.RegisterTenant(ctx, invalidTenant)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Errors[0].Error(), "parent table not found")

	// Test registering a tenant with invalid schema
	invalidTenant = Tenant{
		TableName:   "user_logs",
		TableSchema: "non_existent",
		TenantId:    "test_tenant3",
	}
	result, err = m.RegisterTenant(ctx, invalidTenant)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Errors[0].Error(), "parent table not found")

	// Test registering a tenant with empty tenant ID
	invalidTenant = Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "",
	}
	result, err = m.RegisterTenant(ctx, invalidTenant)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid tenant configuration")

	// Verify the tenant was registered
	tenants, err := m.GetTenants(ctx, "test", "user_logs")
	require.NoError(t, err)
	require.Len(t, tenants, 1)
	require.Equal(t, "test_tenant", tenants[0].TenantId)
}

func TestExtractDateFromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid date at end",
			input:    "user_logs_tenant1_20240101",
			expected: "20240101",
		},
		{
			name:     "valid date at end with no tenant id",
			input:    "user_logs_20240201",
			expected: "20240201",
		},
		{
			name:     "valid date in middle",
			input:    "user_logs_20240101_tenant1",
			expected: "",
		},
		{
			name:     "multiple dates",
			input:    "user_logs_20240101_20240102",
			expected: "20240102",
		},
		{
			name:     "no date",
			input:    "user_logs_tenant1",
			expected: "",
		},
		{
			name:     "invalid date format",
			input:    "user_logs_2024-01-01",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractDateFromString(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateTableKey(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		tenantID  string
		expected  string
	}{
		{
			name:      "with tenant",
			tableName: "user_logs",
			tenantID:  "tenant1",
			expected:  "user_logs_tenant1",
		},
		{
			name:      "without tenant",
			tableName: "user_logs",
			tenantID:  "",
			expected:  "user_logs",
		},
		{
			name:      "with special characters",
			tableName: "user-logs",
			tenantID:  "tenant-1",
			expected:  "user-logs_tenant-1",
		},
		{
			name:      "with numbers",
			tableName: "user_logs_2024",
			tenantID:  "tenant_123",
			expected:  "user_logs_2024_tenant_123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateTableKey(tt.tableName, tt.tenantID)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStartStop(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
	config := &Config{
		SampleRate: time.Millisecond * 100, // Fast sample rate for testing
		Tables: []Table{
			{
				Name:              "user_logs",
				Schema:            "test",
				PartitionType:     TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: time.Hour * 24,
				RetentionPeriod:   time.Hour * 24 * 2,
				PartitionCount:    2,
			},
		},
	}

	clock := NewSimulatedClock(time.Now())

	m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
	require.NoError(t, err)
	require.NotNil(t, m)

	// Create parent table and register a tenant
	_, err = m.CreateParentTable(ctx, config.Tables[0])
	require.NoError(t, err)

	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "test_tenant",
	}
	result, err := m.RegisterTenant(ctx, tenant)
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	// Start the manager
	err = m.Start(ctx)
	require.NoError(t, err)

	// Wait for a few maintenance cycles
	time.Sleep(time.Millisecond * 300)

	// Verify partitions were created
	partitions, err := m.GetPartitions(ctx, "test", "user_logs", 10, 0)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	// Stop the manager
	m.Stop()

	// Verify manager has stopped by checking if new partitions are created
	initialPartitionCount := len(partitions)
	clock.AdvanceTime(time.Hour * 24)
	time.Sleep(time.Millisecond * 300)

	partitions, err = m.GetPartitions(ctx, "test", "user_logs", 10, 0)
	require.NoError(t, err)
	require.Len(t, partitions, initialPartitionCount) // No new partitions should be created

	// Clean up
	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}

func TestCreatePartition(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t, ctx)
	createParentTable(t, ctx, db)

	// Define a valid parent table for testing
	parentTable := Table{
		Name:              "user_logs",
		Schema:            "test",
		TenantIdColumn:    "project_id",
		PartitionBy:       "created_at",
		PartitionType:     TypeRange,
		PartitionInterval: time.Hour * 24,
		RetentionPeriod:   time.Hour * 24 * 7,
		PartitionCount:    5,
	}

	// Valid tenant and bounds
	tenant := Tenant{
		TableName:   "user_logs",
		TableSchema: "test",
		TenantId:    "tenant1",
	}
	validBounds := Bounds{
		From: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name           string
		table          func(t *testing.T, pt Table) Table
		tenant         Tenant
		bounds         Bounds
		expectErr      bool
		errContains    string
		setupDB        func(t *testing.T, m *Manager)
		validationFunc func(t *testing.T, m *Manager)
	}{
		{
			name:      "Successful partition creation",
			table:     func(t *testing.T, pt Table) Table { return pt },
			tenant:    tenant,
			bounds:    validBounds,
			expectErr: false,
			setupDB: func(t *testing.T, m *Manager) {
				// No special setup required
			},
			validationFunc: func(t *testing.T, m *Manager) {
				partitionExists, err := m.partitionExists(ctx, "user_logs_tenant1_20240301", "test")
				require.NoError(t, err)
				require.True(t, partitionExists)
			},
		},
		{
			name: "Invalid partition type",
			table: func(t *testing.T, pt Table) Table {
				p := parentTable
				p.PartitionType = "invalid"
				return p
			},
			tenant:      tenant,
			bounds:      validBounds,
			expectErr:   true,
			errContains: "unsupported partition type",
		},
		{
			name: "Partition with overlapping bounds should upsert",
			table: func(t *testing.T, pt Table) Table {
				return pt
			},
			tenant:    tenant,
			bounds:    validBounds,
			expectErr: false,
			setupDB: func(t *testing.T, m *Manager) {
				parentTables, innerErr := m.GetParentTables(ctx)
				require.NoError(t, innerErr)
				require.Len(t, parentTables, 1)

				// Create a partition with matching bounds beforehand
				innerErr = m.createPartition(ctx, parentTables[0], tenant, validBounds)
				require.NoError(t, innerErr)
			},
		},
		{
			name: "Database transaction failure",
			table: func(t *testing.T, pt Table) Table {
				return pt
			},
			tenant:    tenant,
			bounds:    validBounds,
			expectErr: true,
			setupDB: func(t *testing.T, m *Manager) {
				// Simulate a database failure by dropping the parent table
				_, err := db.ExecContext(ctx, "DROP TABLE test.user_logs CASCADE;")
				require.NoError(t, err)
			},
			errContains: "relation \"test.user_logs\" does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configTable := tt.table(t, parentTable)

			logger := NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})
			config := &Config{
				SampleRate: time.Second,
				Tables: []Table{
					configTable,
				},
			}
			clock := NewSimulatedClock(time.Now())

			m, err := NewManager(WithDB(db), WithConfig(config), WithLogger(logger), WithClock(clock))
			require.NoError(t, err)

			err = m.Start(ctx)
			require.NoError(t, err)

			result, err := m.RegisterTenant(ctx, tenant)
			require.NoError(t, err)
			require.Empty(t, result.Errors)

			// Setup database if required
			if tt.setupDB != nil {
				tt.setupDB(t, m)
			}

			parentTables, err := m.GetParentTables(ctx)
			require.NoError(t, err)
			require.Len(t, parentTables, 1)

			configTable.Id = parentTables[0].Id
			t.Log(configTable)

			// Call createPartition
			err = m.createPartition(ctx, configTable, tt.tenant, tt.bounds)

			if tt.expectErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				if tt.validationFunc != nil {
					tt.validationFunc(t, m)
				}
			}
		})
	}

	dropParentTables(t, ctx, db)
	cleanupPartManDBs(t, db)
}
