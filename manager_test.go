package partman

import (
	"context"
	"fmt"

	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

var createTestTableQuery = `
CREATE TABLE if not exists test.sample (
    id VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at, tenant_id)
) PARTITION BY RANGE (created_at);`

var createTestTableWithTenantIdQuery = `
CREATE TABLE if not exists test.sample (
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
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test.sample")
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

	_, err := db.Exec("DROP TABLE IF EXISTS partman.partitions")
	require.NoError(t, err)
}

func TestManager(t *testing.T) {
	t.Run("NewAndStart", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)
		require.NotNil(t, manager)
	})

	t.Run("Initialize", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Schema:            "test",
					Name:              "sample",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		clock := NewSimulatedClock(time.Now())

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		err = manager.Start(context.Background())
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.partitions WHERE table_name = $1", "sample")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("CreateFuturePartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		tableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    10,
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		_, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)
	})

	t.Run("CreateFuturePartitionsWithExistingPartitionsInRange", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		_, err := db.ExecContext(context.Background(), `
		CREATE TABLE if not exists test.sample_20240101 PARTITION OF test.sample
    	FOR VALUES FROM ('2024-01-01') TO ('2024-01-02');`)
		require.NoError(t, err)

		tableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    10,
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)
	})

	t.Run("CreateFuturePartitionsWithExistingPartitionsOutOfRange", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		_, err := db.ExecContext(context.Background(), `
		CREATE TABLE if not exists test.sample_20231201 PARTITION OF test.sample
    	FOR VALUES FROM ('2024-12-01') TO ('2024-12-02');`)
		require.NoError(t, err)

		tableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    10,
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", tableConfig.Schema)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount+1, partitionCount)
	})

	t.Run("DropOldPartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		clock.AdvanceTime(time.Hour + time.Minute)

		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)
	})

	t.Run("TestMaintainer", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 24)

		// Wait for maintenance to complete
		time.Sleep(2 * time.Second)

		// Stop the manager
		manager.Stop()

		// Verify partitions were created
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_%")
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
			tableConfig := Table{
				Name: "sample",
			}
			name := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_20240315", name)
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := Table{
				Name:        "sample",
				TenantId:    "TENANT1",
				PartitionBy: "created_at",
			}
			name := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_TENANT1_20240315", name)
		})
	})

	t.Run("GeneratePartitionSQL", func(t *testing.T) {
		manager := &Manager{}
		bounds := Bounds{
			From: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		}

		t.Run("without tenant ID", func(t *testing.T) {
			tableConfig := Table{
				Name:          "sample",
				Schema:        "test",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_20240315", partitionName)

			sql, err := manager.generatePartitionSQL(partitionName, tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.sample_20240315 PARTITION OF test.sample FOR VALUES FROM ('2024-03-15 00:00:00+00'::timestamptz) TO ('2024-03-16 00:00:00+00'::timestamptz);")
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := Table{
				Name:          "sample",
				TenantId:      "TENANT1",
				Schema:        "test",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SampleRate: time.Second,
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_TENANT1_20240315", partitionName)

			sql, err := manager.generatePartitionSQL(partitionName, tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, "CREATE TABLE IF NOT EXISTS test.sample_TENANT1_20240315 PARTITION OF test.sample FOR VALUES FROM ('TENANT1', '2024-03-15 00:00:00+00'::timestamptz) TO ('TENANT1', '2024-03-16 00:00:00+00'::timestamptz);", sql)
		})
	})

	t.Run("PartitionExists", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		clock := NewSimulatedClock(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					RetentionPeriod:   time.Hour * 24,
					PartitionInterval: time.Hour * 24,
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionCount:    2,
				},
			},
		}

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		exists, err := manager.partitionExists(context.Background(), "sample_20240315", "test")
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

		tableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "TENANT1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		ctx := context.Background()

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Verify partitions were created
		var partitionCount uint
		err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant1%")
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount, partitionCount)

		// Verify the partition naming format
		var partitionName string
		err = db.GetContext(ctx, &partitionName, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1 LIMIT 1", "sample_tenant1%")
		require.NoError(t, err)
		require.Contains(t, partitionName, "tenant1", "Partition name should include tenant ID")

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.partitions where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
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

		tenantOneConfig := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "tenant_1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}
		tenantTwoConfig := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "tenant_2",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5,
		}

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: 30 * time.Second,
			Tables: []Table{
				tenantOneConfig,
				tenantTwoConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		ctx := context.Background()

		_, err = NewManager(
			WithDB(db),
			WithLogger(logger),
			WithClock(clock),
			WithConfig(config),
		)
		require.NoError(t, err)

		partitionNames := []string{
			"sample_%s_20240101",
			"sample_%s_20240102",
			"sample_%s_20240103",
			"sample_%s_20240104",
			"sample_%s_20240105",
		}

		// Verify partitions were created for tenant 1
		for _, tableConfig := range config.Tables {
			var partitionCount uint
			tableName := fmt.Sprintf("sample_%s%%", tableConfig.TenantId)
			err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", tableName)
			require.NoError(t, err)
			require.Equal(t, tableConfig.PartitionCount, partitionCount)

			// Verify the partition naming format
			rows, err := db.QueryxContext(ctx, "SELECT tablename FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("sample_%s%%", tableConfig.TenantId))
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
			err = db.QueryRowxContext(ctx, "select exists(select 1 from partman.partitions where tenant_id = $1);", tableConfig.TenantId).Scan(&exists)
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
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
			},
		}

		logger := NewSlogLogger()
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize manager
		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Insert test data for both tenants
		insertSQL := `INSERT INTO test.sample (id, project_id, created_at) VALUES ($1, $2, $3)`

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
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM test.sample WHERE project_id = $1", tenantID)
			require.NoError(t, err)
			require.Equal(t, 10, count)
		}

		// Advance clock to trigger maintenance
		clock.AdvanceTime(time.Hour * 24)

		// Wait for maintenance to complete
		time.Sleep(2 * time.Second)

		// Stop the manager
		manager.Stop()

		// Verify that old data has been cleaned up
		for _, tenantID := range []string{"tenant_1", "tenant_2"} {
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM test.sample WHERE project_id = $1", tenantID)
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
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "TENANT1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "tenant2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
			},
		}

		logger := NewSlogLogger()
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize manager
		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		// Add a new managed table
		newTableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "tenant3",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   1 * time.Minute,
			PartitionCount:    2,
		}
		err = manager.AddManagedTable(newTableConfig)
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.sample (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant3",
				now.Add(time.Duration(i)*time.Minute),
			)
			require.NoError(t, err)
		}

		clock.AdvanceTime(time.Hour)

		var count int
		err = db.QueryRowxContext(ctx, "select count(*) from test.sample where project_id = $1;", newTableConfig.TenantId).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, count, 5)

		// Run retention for the tenant
		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.sample where project_id = $1);", newTableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("importExistingPartitions", func(t *testing.T) {
		t.Run("Successfully import existing partitions", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create some existing partitions manually
			partitions := []string{
				`CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				`CREATE TABLE test.sample_tenant1_20240102 PARTITION OF test.sample FOR VALUES FROM ('TENANT1', '2024-01-02') TO ('TENANT1', '2024-01-03')`,
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			// Initialize manager
			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			// Verify partitions were imported correctly
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 2, count) // Should have 2 entries (one for each tenant)

			// Verify specific tenant entries
			var exists bool
			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM partman.partitions 
					WHERE tenant_id = 'TENANT1' AND partition_interval = '24h0m0s'
				)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM partman.partitions
					WHERE tenant_id = 'tenant2' AND partition_interval = '24h0m0s'
				)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)
		})

		t.Run("Import with invalid partition format", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create an invalid partition (wrong naming format)
			_, err = db.ExecContext(ctx, `
				CREATE TABLE test.sample_invalid_format PARTITION OF test.sample 
				FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`)
			require.NoError(t, err)

			// Initialize manager
			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.Error(t, err)
			require.ErrorContains(t, err, "parsing time \"\" as \"20060102\": cannot parse \"\" as \"2006\"")

			// Verify no partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 0, count)
		})

		t.Run("Import partitions with multiple date formats", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create partitions with different date formats
			partitions := []string{
				// Standard format
				`CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Different month and day
				`CREATE TABLE test.sample_tenant1_20241231 PARTITION OF test.sample 
				 FOR VALUES FROM ('TENANT1', '2024-12-31') TO ('TENANT1', '2025-01-01')`,
				// Another tenant
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			// Verify all partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 2, count) // Should have 2 entries (one for each tenant)

			// Verify partition dates were correctly parsed
			var exists bool
			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM pg_tables 
					WHERE tablename = 'sample_tenant1_20241231' 
					AND schemaname = 'test'
				)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)
		})

		t.Run("Import partitions with existing management entries", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create a partition
			_, err = db.ExecContext(ctx, `
				CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample 
				FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`)
			require.NoError(t, err)

			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Create an existing management entry
			_, err = db.ExecContext(ctx, `
				INSERT INTO partman.partitions (
					id, table_name, schema_name, tenant_id, tenant_column,
					partition_by, partition_type, partition_interval,
					retention_period, partition_count
				) VALUES (
					$1, 'sample', 'test', 'TENANT1', 'project_id',
					'created_at', 'range', '24h', '720h', 10
				)`, ulid.Make().String())
			require.NoError(t, err)

			// Try to import partitions
			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			// Verify only one management entry exists (no duplicates)
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partitions")
			require.NoError(t, err)
			require.Equal(t, 1, count)
		})

		t.Run("Import partitions with mixed valid and invalid names", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create mixed partitions
			partitions := []string{
				// Valid partition
				`CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Invalid format but valid partition
				`CREATE TABLE test.sample_invalid_tenant1 PARTITION OF test.sample 
				 FOR VALUES FROM ('TENANT1', '2024-01-02') TO ('TENANT1', '2024-01-03')`,
				// Another valid partition
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
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
		})

		t.Run("importExistingPartitions with mixed tenant and non-tenant tables", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test tables - one with tenant support, one without
			_, err := db.ExecContext(ctx, `
				CREATE TABLE if not exists test.sample_with_tenant (
					id VARCHAR NOT NULL,
					project_id VARCHAR NOT NULL,
					created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY (id, created_at, project_id)
				) PARTITION BY RANGE (project_id, created_at);
				
				CREATE TABLE if not exists test.sample_no_tenant (
					id VARCHAR NOT NULL,
					created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY (id, created_at)
				) PARTITION BY RANGE (created_at);
			`)
			require.NoError(t, err)
			defer func() {
				_, err = db.ExecContext(ctx, `
					DROP TABLE IF EXISTS test.sample_with_tenant;
					DROP TABLE IF EXISTS test.sample_no_tenant;
				`)
				require.NoError(t, err)
			}()

			// Create mixed partitions
			partitions := []string{
				// Tenant partitions
				`CREATE TABLE if not exists test.sample_with_tenant_tenant1_20240101 PARTITION OF test.sample_with_tenant
				 FOR VALUES FROM ('TENANT1', '2024-01-01') TO ('TENANT1', '2024-01-02')`,
				// Non-tenant partitions
				`CREATE TABLE if not exists test.sample_no_tenant_20240101 PARTITION OF test.sample_no_tenant
				 FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
				`CREATE TABLE if not exists test.sample_no_tenant_20240102 PARTITION OF test.sample_no_tenant
				 FOR VALUES FROM ('2024-01-02') TO ('2024-01-03')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: time.Hour * 24,
				PartitionCount:    10,
				RetentionPeriod:   time.Hour * 24 * 31,
			})
			require.NoError(t, err)

			time.Sleep(time.Second)
			manager.Stop()

			// Verify both tenant and non-tenant partitions were imported
			var managedTables []struct {
				TableName string `db:"table_name"`
				TenantID  string `db:"tenant_id"`
			}
			err = db.SelectContext(ctx, &managedTables, `
				SELECT table_name, tenant_id 
				FROM partman.partitions`)
			t.Logf("%+v", managedTables)
			require.NoError(t, err)
			require.Len(t, managedTables, 2)

			for _, m := range managedTables {
				if m.TableName == "sample_with_tenant" {
					require.Equal(t, "TENANT1", m.TenantID)
				} else {
					require.Empty(t, m.TenantID)
				}
			}
		})

		t.Run("importExistingPartitions with only non-tenant tables", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table without tenant support
			_, err := db.ExecContext(ctx, `
				CREATE TABLE test.sample (
					id VARCHAR NOT NULL,
					created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY (id, created_at)
				) PARTITION BY RANGE (created_at);
			`)
			require.NoError(t, err)
			defer func() {
				_, err = db.ExecContext(ctx, `DROP TABLE IF EXISTS test.sample;`)
				require.NoError(t, err)
			}()

			// Create non-tenant partitions
			partitions := []string{
				`CREATE TABLE test.sample_20240101 PARTITION OF test.sample
				 FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
				`CREATE TABLE test.sample_20240102 PARTITION OF test.sample
				 FOR VALUES FROM ('2024-01-02') TO ('2024-01-03')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := NewSlogLogger()
			config := &Config{
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.importExistingPartitions(ctx, Table{
				Schema:            "test",
				TenantIdColumn:    "project_id",
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
				FROM partman.partitions 
				WHERE table_name = 'sample'`)
			require.NoError(t, err)
			require.Equal(t, 1, count)

			// Verify partition count
			err = db.GetContext(ctx, &count, `
				SELECT COUNT(*) 
				FROM pg_tables 
				WHERE schemaname = 'test' 
				AND tablename LIKE 'sample_%'`)
			require.NoError(t, err)
			require.Equal(t, 2, count)
		})
	})
}

func TestNewManager(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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
			WithLogger(logger),
			WithConfig(config),
			WithClock(clock),
		)
		require.NoError(t, err)
		require.NotNil(t, manager)
	})

	t.Run("Error - DB must not be nil", func(t *testing.T) {
		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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
	})

	t.Run("Error - Config must not be nil", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		logger := NewSlogLogger()
		clock := NewSimulatedClock(time.Now())

		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithClock(clock),
		)
		require.Error(t, err)
		require.Nil(t, manager)
		require.Equal(t, ErrConfigMustNotBeNil, err)
	})

	t.Run("Error - Clock must not be nil", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		logger := NewSlogLogger()
		config := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
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
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "TENANT1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "tenant2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   1 * time.Minute,
					PartitionCount:    2,
				},
			},
		}

		logger := NewSlogLogger()
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := NewSimulatedClock(now)

		// Create and initialize manager
		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithClock(clock),
			WithConfig(config),
		)
		require.NoError(t, err)

		// Add a new managed table
		newTableConfig := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "tenant3",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   1 * time.Minute,
			PartitionCount:    2,
		}
		err = manager.AddManagedTable(newTableConfig)
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.sample (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant3",
				now.Add(time.Duration(i)*time.Minute),
			)
			require.NoError(t, err)
		}

		clock.AdvanceTime(time.Hour)

		var count int
		err = db.QueryRowxContext(ctx, "select count(*) from test.sample where project_id = $1;", newTableConfig.TenantId).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, count, 5)

		// Run retention for the tenant
		err = manager.DropOldPartitions(context.Background())
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant3%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.sample where project_id = $1);", newTableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestManagerConfigUpdate(t *testing.T) {
	t.Run("AddManagedTable", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()

		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Initial config with one table
		initialTable := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "TENANT1",
			TenantIdColumn:    "project_id",
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		}

		config := &Config{
			SampleRate: time.Second,
			Tables:     []Table{initialTable},
		}

		logger := NewSlogLogger()
		// Create manager
		manager, err := NewAndStart(db, config, logger, NewSimulatedClock(time.Now()))
		require.NoError(t, err)

		// Add a new table
		newTable := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "tenant2",
			TenantIdColumn:    "project_id",
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		}

		err = manager.AddManagedTable(newTable)
		require.NoError(t, err)

		// Verify config was updated
		require.Equal(t, 2, len(manager.config.Tables))
		require.Contains(t, manager.config.Tables, newTable)
	})

	t.Run("importExistingPartitions", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()

		// Create test table
		createTestTable(t, ctx, db)
		defer dropTestTable(t, ctx, db)

		// Initial config with empty tables
		config := &Config{
			SampleRate: time.Second,
		}

		logger := NewSlogLogger()
		// Create manager
		manager, err := NewAndStart(db, config, logger, NewSimulatedClock(time.Now()))
		require.NoError(t, err)

		// Create some existing partitions manually
		partitions := []string{
			`CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
			`CREATE TABLE test.sample_tenant1_20240102 PARTITION OF test.sample FOR VALUES FROM ('2024-01-02') TO ('2024-01-03')`,
		}

		for _, partition := range partitions {
			_, err = db.ExecContext(ctx, partition)
			require.NoError(t, err)
		}

		// Import existing partitions
		err = manager.importExistingPartitions(ctx, Table{
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		})
		t.Log(err)
		require.NoError(t, err)

		// Verify config includes imported tables
		require.Equal(t, 1, len(manager.config.Tables))

		// Verify the imported table has correct configuration
		importedTable := manager.config.Tables[0]
		require.Equal(t, "sample", importedTable.Name)
		require.Equal(t, TypeRange, importedTable.PartitionType)
		require.Equal(t, "created_at", importedTable.PartitionBy)
		require.Equal(t, time.Hour*24, importedTable.PartitionInterval)
	})
}

func TestManagerInitialization(t *testing.T) {
	t.Run("loads existing managed tables", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()

		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Insert some existing managed tables into partitions
		existingTables := []Table{
			{
				Name:              "sample",
				Schema:            "test",
				TenantId:          "TENANT1",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     "range",
				PartitionInterval: time.Hour * 24,
				PartitionCount:    5,
				RetentionPeriod:   time.Hour * 24 * 31,
			},
			{
				Name:              "sample",
				Schema:            "test",
				TenantId:          "tenant2",
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     "range",
				PartitionInterval: time.Hour * 24,
				PartitionCount:    3,
				RetentionPeriod:   time.Hour * 24,
			},
		}

		// Create management table and insert existing configurations
		migrations := []string{
			createSchema,
			createPartitionsTable,
			createUniqueIndex,
		}

		for _, migration := range migrations {
			_, err = db.ExecContext(ctx, migration)
			require.NoError(t, err)
		}

		for _, tc := range existingTables {
			mTable := tc.toManagedTable()
			_, err = db.ExecContext(ctx, upsertSQL,
				ulid.Make().String(),
				mTable.TableName,
				mTable.SchemaName,
				mTable.TenantID,
				mTable.TenantColumn,
				mTable.PartitionBy,
				mTable.PartitionType,
				mTable.PartitionCount,
				mTable.PartitionInterval,
				mTable.RetentionPeriod,
			)
			require.NoError(t, err)
		}

		// Create the manager with one table pre-configured
		newConfig := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "tenant3",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 7,
					PartitionCount:    2,
				},
			},
		}

		logger := NewSlogLogger()
		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(newConfig),
			WithClock(NewSimulatedClock(time.Now())),
		)
		require.NoError(t, err)

		// Verify that manager config contains both existing and new tables
		require.Equal(t, 3, len(manager.config.Tables), "Should have 3 tables (2 existing + 1 new)")

		// Helper function to find table by tenant ID
		findTable := func(tables []Table, tenantID string) *Table {
			for _, table := range tables {
				if table.TenantId == tenantID {
					return &table
				}
			}
			return nil
		}

		// Verify existing tables were loaded with correct configuration
		tenant1Table := findTable(manager.config.Tables, "TENANT1")
		require.NotNil(t, tenant1Table)
		require.Equal(t, uint(5), tenant1Table.PartitionCount)
		require.Equal(t, time.Hour*24*31, tenant1Table.RetentionPeriod)

		tenant2Table := findTable(manager.config.Tables, "tenant2")
		require.NotNil(t, tenant2Table)
		require.Equal(t, uint(3), tenant2Table.PartitionCount)
		require.Equal(t, time.Hour*24, tenant2Table.RetentionPeriod)

		// Verify new table was added
		tenant3Table := findTable(manager.config.Tables, "tenant3")
		require.NotNil(t, tenant3Table)
		require.Equal(t, uint(2), tenant3Table.PartitionCount)
		require.Equal(t, time.Hour*24*7, tenant3Table.RetentionPeriod)
	})

	t.Run("new config overrides existing table config", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()

		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Create management table and insert existing configurations
		migrations := []string{
			createSchema,
			createPartitionsTable,
			createUniqueIndex,
		}

		for _, migration := range migrations {
			_, err = db.ExecContext(ctx, migration)
			require.NoError(t, err)
		}
		_, err = db.ExecContext(ctx, upsertSQL,
			ulid.Make().String(),
			"sample",
			"test",
			"TENANT1",
			"project_id",
			"created_at",
			"range",
			5,
			"24h",
			"168h",
		)
		require.NoError(t, err)

		// Create new manager with overlapping config
		newConfig := &Config{
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					Schema:            "test",
					TenantId:          "TENANT1", // Same tenant as existing
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: time.Hour * 24,
					RetentionPeriod:   time.Hour * 24 * 31, // Different retention period
					PartitionCount:    10,                  // Different partition count
				},
			},
		}

		logger := NewSlogLogger()
		manager, err := NewManager(
			WithDB(db),
			WithLogger(logger),
			WithConfig(newConfig),
			WithClock(NewSimulatedClock(time.Now())),
		)
		require.NoError(t, err)

		// Verify that new config overrode existing config
		require.Equal(t, 1, len(manager.config.Tables))
		table := manager.config.Tables[0]
		require.Equal(t, "TENANT1", table.TenantId)
		require.Equal(t, uint(10), table.PartitionCount)
		require.Equal(t, time.Hour*24*31, table.RetentionPeriod)

		// Verify database was updated with new config
		var dbTable struct {
			PartitionCount  int    `db:"partition_count"`
			RetentionPeriod string `db:"retention_period"`
		}
		err = db.GetContext(ctx, &dbTable,
			"SELECT partition_count, retention_period FROM partman.partitions WHERE tenant_id = $1",
			"TENANT1",
		)
		require.NoError(t, err)
		require.Equal(t, 10, dbTable.PartitionCount)
		require.Equal(t, "744h0m0s", dbTable.RetentionPeriod)
	})
}

func TestTableDeduplication(t *testing.T) {
	t.Run("AddManagedTable deduplicates tables", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()
		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Initial config with one table
		initialTable := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "TENANT1",
			TenantIdColumn:    "project_id",
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		}

		manager, err := NewManager(
			WithDB(db),
			WithLogger(NewSlogLogger()),
			WithConfig(&Config{
				SampleRate: time.Second,
				Tables:     []Table{initialTable},
			}),
			WithClock(NewSimulatedClock(time.Now())),
		)
		require.NoError(t, err)

		// Add the same table again with different config
		updatedTable := initialTable
		updatedTable.PartitionCount = 5
		err = manager.AddManagedTable(updatedTable)
		require.NoError(t, err)

		// Verify only one table exists with updated config
		require.Equal(t, 1, len(manager.config.Tables))
		require.Equal(t, uint(5), manager.config.Tables[0].PartitionCount)

		// Add another table with same name but different tenant
		newTenantTable := initialTable
		newTenantTable.TenantId = "tenant2"
		err = manager.AddManagedTable(newTenantTable)
		require.NoError(t, err)

		// Verify we now have exactly two tables
		require.Equal(t, 2, len(manager.config.Tables))
	})

	t.Run("initialize deduplicates tables from DB and config", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()
		// Create test table with tenant support
		_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
		require.NoError(t, err)
		defer dropTestTable(t, ctx, db)

		// Create management table
		migrations := []string{
			createSchema,
			createPartitionsTable,
			createUniqueIndex,
		}
		for _, migration := range migrations {
			_, err := db.ExecContext(ctx, migration)
			require.NoError(t, err)
		}

		// Insert existing table in DB
		existingTable := Table{
			Name:              "sample",
			Schema:            "test",
			TenantId:          "TENANT1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     "range",
			PartitionInterval: time.Hour * 24,
			PartitionCount:    5,
			RetentionPeriod:   time.Hour * 24 * 31,
		}
		mTable := existingTable.toManagedTable()
		_, err = db.ExecContext(ctx, upsertSQL,
			ulid.Make().String(),
			mTable.TableName,
			mTable.SchemaName,
			mTable.TenantID,
			mTable.TenantColumn,
			mTable.PartitionBy,
			mTable.PartitionType,
			mTable.PartitionCount,
			mTable.PartitionInterval,
			mTable.RetentionPeriod,
		)
		require.NoError(t, err)

		// Create new manager with same table in config but different settings
		configTable := existingTable
		configTable.PartitionCount = 10
		manager, err := NewManager(
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
		require.Equal(t, 1, len(manager.config.Tables))
		require.Equal(t, uint(10), manager.config.Tables[0].PartitionCount)
	})

	t.Run("importExistingPartitions deduplicates tables", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		ctx := context.Background()
		// Create test table with tenant support
		createTestTable(t, ctx, db)
		defer dropTestTable(t, ctx, db)

		// Create initial manager with one table
		initialTable := Table{
			Name:              "sample",
			Schema:            "test",
			PartitionType:     TypeRange,
			PartitionBy:       "created_at",
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    2,
		}
		manager, err := NewManager(
			WithDB(db),
			WithLogger(NewSlogLogger()),
			WithConfig(&Config{
				SampleRate: time.Second,
				Tables:     []Table{initialTable},
			}),
			WithClock(NewSimulatedClock(time.Now())),
		)
		require.NoError(t, err)

		// Create some existing partitions for the same table
		partitions := []string{
			`CREATE TABLE test.sample_20240101 PARTITION OF test.sample FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')`,
		}
		for _, partition := range partitions {
			_, err = db.ExecContext(ctx, partition)
			require.NoError(t, err)
		}

		// Import existing partitions
		err = manager.importExistingPartitions(ctx, Table{
			Schema:            "test",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: time.Hour * 24,
			RetentionPeriod:   time.Hour * 24 * 7,
			PartitionCount:    5, // Different count
		})
		require.NoError(t, err)

		// Verify we still only have one table (deduplication worked)
		require.Equal(t, 1, len(manager.config.Tables))
		// Verify original config was preserved
		require.Equal(t, uint(5), manager.config.Tables[0].PartitionCount)
	})
}
