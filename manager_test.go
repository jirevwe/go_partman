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
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(time.Hour),
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

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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
		err = db.Get(&count, "SELECT COUNT(*) FROM partman.partition_management WHERE table_name = $1", "sample")
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
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    10,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Now())

		_, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", config.SchemaName)
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
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    10,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", config.SchemaName)
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
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    10,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				tableConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		_, err = NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		var partitionCount uint
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1 and schemaname = $2", "sample_%", config.SchemaName)
		require.NoError(t, err)
		require.Equal(t, tableConfig.PartitionCount+1, partitionCount)
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
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(time.Hour),
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

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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
				TenantId:    "tenant1",
				PartitionBy: "created_at",
			}
			name := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_tenant1_20240315", name)
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
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SchemaName: "test",
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_20240315", partitionName)

			sql, err := manager.generatePartitionSQL(partitionName, tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.sample_20240315 PARTITION OF test.sample FOR VALUES FROM ('2024-03-15') TO ('2024-03-16');")
		})

		t.Run("with tenant ID", func(t *testing.T) {
			tableConfig := Table{
				Name:          "sample",
				TenantId:      "tenant1",
				PartitionType: TypeRange,
				PartitionBy:   "created_at",
			}
			manager.config = &Config{
				SchemaName: "test",
				Tables: []Table{
					tableConfig,
				},
			}

			partitionName := manager.generatePartitionName(tableConfig, bounds)
			require.Equal(t, "sample_tenant1_20240315", partitionName)

			sql, err := manager.generatePartitionSQL(partitionName, tableConfig, bounds)
			require.NoError(t, err)
			require.Equal(t, sql, "CREATE TABLE IF NOT EXISTS test.sample_tenant1_20240315 PARTITION OF test.sample FOR VALUES FROM ('tenant1', '2024-03-15') TO ('tenant1', '2024-03-16');")
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
			Tables: []Table{
				{
					Name:              "sample",
					RetentionPeriod:   OneDay,
					PartitionInterval: OneDay,
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionCount:    2,
				},
			},
		}

		manager, err := NewAndStart(db, config, logger, clock)
		require.NoError(t, err)

		exists, err := manager.partitionExists(context.Background(), "sample_20240315")
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
			TenantId:          "tenant1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    5,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
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

		tenantOneConfig := Table{
			Name:              "sample",
			TenantId:          "tenant_1",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    5,
		}
		tenantTwoConfig := Table{
			Name:              "sample",
			TenantId:          "tenant_2",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   OneWeek,
			PartitionCount:    5,
		}

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				tenantOneConfig,
				tenantTwoConfig,
			},
		}

		clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

		ctx := context.Background()

		_, err = NewAndStart(db, config, logger, clock)
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
			err = db.GetContext(ctx, &partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", fmt.Sprintf("sample_%s%%", tableConfig.TenantId))
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
			Tables: []Table{
				{
					Name:              "sample",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
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
		time.Sleep(time.Second)

		// Stop the manager
		manager.Stop()

		// Verify that old data has been cleaned up
		for _, tenantID := range []string{"tenant_1", "tenant_2"} {
			var count int
			err = db.GetContext(ctx, &count,
				"SELECT COUNT(*) FROM test.sample WHERE project_id = $1",
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
			Tables: []Table{
				{
					Name:              "sample",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
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
		newTableConfig := Table{
			Name:              "sample",
			TenantId:          "tenant_3",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   TimeDuration(1 * time.Minute),
			PartitionCount:    2,
		}
		err = manager.AddManagedTable(newTableConfig)
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.sample (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant_3",
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
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.sample where project_id = $1);", newTableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("ImportExistingPartitions", func(t *testing.T) {
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
				`CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`,
				`CREATE TABLE test.sample_tenant1_20240102 PARTITION OF test.sample FOR VALUES FROM ('tenant1', '2024-01-02') TO ('tenant1', '2024-01-03')`,
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			// Initialize manager
			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.NoError(t, err)

			// Verify partitions were imported correctly
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partition_management")
			require.NoError(t, err)
			require.Equal(t, 2, count) // Should have 2 entries (one for each tenant)

			// Verify specific tenant entries
			var exists bool
			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM partman.partition_management 
					WHERE tenant_id = 'tenant1' AND partition_interval = '24h0m0s'
				)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM partman.partition_management
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
				FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`)
			require.NoError(t, err)

			// Initialize manager
			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Import existing partitions
			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.NoError(t, err)

			// Verify no partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partition_management")
			require.NoError(t, err)
			require.Equal(t, 0, count)
		})

		t.Run("Import with non-existent partition column", func(t *testing.T) {
			db, pool := setupTestDB(t)
			defer cleanupTestDB(t, db, pool)

			ctx := context.Background()

			// Create test table with tenant support
			_, err := db.ExecContext(ctx, createTestTableWithTenantIdQuery)
			require.NoError(t, err)
			defer dropTestTable(t, ctx, db)

			// Create a valid partition
			_, err = db.ExecContext(ctx, `
				CREATE TABLE test.sample_tenant1_20240101 PARTITION OF test.sample 
				FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`)
			require.NoError(t, err)

			// Initialize manager
			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Try to import with non-existent partition column
			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "non_existent_column",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "table sample does not have a tenant id column")
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
				 FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`,
				// Different month and day
				`CREATE TABLE test.sample_tenant1_20241231 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant1', '2024-12-31') TO ('tenant1', '2025-01-01')`,
				// Another tenant
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.NoError(t, err)

			// Verify all partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partition_management")
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
				FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`)
			require.NoError(t, err)

			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			// Create an existing management entry
			_, err = db.ExecContext(ctx, `
				INSERT INTO partman.partition_management (
					id, table_name, schema_name, tenant_id, tenant_column,
					partition_by, partition_type, partition_interval,
					retention_period, partition_count
				) VALUES (
					$1, 'sample', 'test', 'tenant1', 'project_id',
					'created_at', 'range', '24h', '720h', 10
				)`, ulid.Make().String())
			require.NoError(t, err)

			// Try to import partitions
			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.NoError(t, err)

			// Verify only one management entry exists (no duplicates)
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partition_management")
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
				 FOR VALUES FROM ('tenant1', '2024-01-01') TO ('tenant1', '2024-01-02')`,
				// Invalid format but valid partition
				`CREATE TABLE test.sample_invalid_tenant1 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant1', '2024-01-02') TO ('tenant1', '2024-01-03')`,
				// Another valid partition
				`CREATE TABLE test.sample_tenant2_20240101 PARTITION OF test.sample 
				 FOR VALUES FROM ('tenant2', '2024-01-01') TO ('tenant2', '2024-01-02')`,
			}

			for _, partition := range partitions {
				_, err = db.ExecContext(ctx, partition)
				require.NoError(t, err)
			}

			logger := slog.Default()
			config := &Config{
				SchemaName: "test",
				SampleRate: time.Second,
			}
			clock := NewSimulatedClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

			manager, err := NewAndStart(db, config, logger, clock)
			require.NoError(t, err)

			err = manager.ImportExistingPartitions(ctx, Table{
				TenantIdColumn:    "project_id",
				PartitionBy:       "created_at",
				PartitionType:     TypeRange,
				PartitionInterval: OneDay,
				PartitionCount:    10,
				RetentionPeriod:   OneMonth,
			})
			require.NoError(t, err)

			// Verify only valid partitions were imported
			var count int
			err = db.GetContext(ctx, &count, "SELECT COUNT(*) FROM partman.partition_management")
			require.NoError(t, err)
			require.Equal(t, 2, count) // Should only import the valid partitions

			// Verify specific valid partitions were imported
			var exists bool
			err = db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM partman.partition_management 
					WHERE tenant_id IN ('tenant1', 'tenant2')
				)`).Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)
		})
	})
}

func TestNewManager(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db, pool := setupTestDB(t)
		defer cleanupTestDB(t, db, pool)

		createTestTable(t, context.Background(), db)
		defer dropTestTable(t, context.Background(), db)

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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
		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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

		logger := slog.Default()
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

		logger := slog.Default()
		config := &Config{
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					PartitionType:     TypeRange,
					PartitionBy:       "created_at",
					PartitionInterval: OneDay,
					RetentionPeriod:   OneWeek,
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
			SchemaName: "test",
			SampleRate: time.Second,
			Tables: []Table{
				{
					Name:              "sample",
					TenantId:          "tenant_1",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
				},
				{
					Name:              "sample",
					TenantId:          "tenant_2",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     TypeRange,
					PartitionInterval: OneDay,
					RetentionPeriod:   TimeDuration(1 * time.Minute),
					PartitionCount:    2,
				},
			},
		}

		logger := slog.Default()
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
			TenantId:          "tenant_3",
			TenantIdColumn:    "project_id",
			PartitionBy:       "created_at",
			PartitionType:     TypeRange,
			PartitionInterval: OneDay,
			RetentionPeriod:   TimeDuration(1 * time.Minute),
			PartitionCount:    2,
		}
		err = manager.AddManagedTable(newTableConfig)
		require.NoError(t, err)

		// Verify that the new tenant's partitions exist
		var partitionCount int
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 2, partitionCount)

		// Insert rows for the new tenant
		insertSQL := `INSERT INTO test.sample (id, project_id, created_at) VALUES ($1, $2, $3)`
		for i := 0; i < 5; i++ {
			_, err = db.ExecContext(context.Background(), insertSQL,
				ulid.Make().String(), "tenant_3",
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
		err = db.Get(&partitionCount, "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE $1", "sample_tenant_3%")
		require.NoError(t, err)
		require.Equal(t, 1, partitionCount)

		var exists bool
		err = db.QueryRowxContext(ctx, "select exists(select 1 from test.sample where project_id = $1);", newTableConfig.TenantId).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
