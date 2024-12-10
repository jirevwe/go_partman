package partman

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

// todo(raymond): add metrics

// Manager Partition manager
type Manager struct {
	db     *sqlx.DB
	logger *slog.Logger
	config *Config
	clock  Clock
	hook   Hook
	wg     *sync.WaitGroup // For testing synchronization
	stop   chan struct{}   // For graceful shutdown
}

func NewManager(options ...Option) (*Manager, error) {
	m := &Manager{
		wg:   &sync.WaitGroup{},
		stop: make(chan struct{}),
	}

	for _, opt := range options {
		err := opt(m)
		if err != nil {
			return nil, err
		}
	}

	if m.db == nil {
		return nil, ErrDbDriverMustNotBeNil
	}

	if m.logger == nil {
		return nil, ErrLoggerMustNotBeNil
	}

	if m.config == nil {
		return nil, ErrConfigMustNotBeNil
	}

	if m.clock == nil {
		return nil, ErrClockMustNotBeNil
	}

	if err := m.runMigrations(context.Background()); err != nil {
		return nil, err
	}

	if err := m.initialize(context.Background(), m.config); err != nil {
		return nil, err
	}

	return m, nil
}

func NewAndStart(db *sqlx.DB, config *Config, logger *slog.Logger, clock Clock) (*Manager, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	m := &Manager{
		db:     db,
		clock:  clock,
		logger: logger,
		config: config,
		wg:     &sync.WaitGroup{},
		stop:   make(chan struct{}),
	}

	ctx := context.Background()

	if err := m.runMigrations(ctx); err != nil {
		return nil, err
	}

	if err := m.initialize(ctx, config); err != nil {
		return nil, err
	}

	// Start the maintenance routine
	if err := m.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start maintenance routine: %w", err)
	}

	return m, nil
}

// runUpgrades runs all the migrations on the management tables while keeping them backwards compatible
func (m *Manager) runMigrations(ctx context.Context) error {
	migrations := []string{
		createSchema,
		createManagementTable,
		createUniqueIndex,
	}

	for _, migration := range migrations {
		if _, err := m.db.ExecContext(ctx, migration); err != nil {
			return fmt.Errorf("failed to run migration: %s, with error %w", migration, err)
		}
	}

	return nil
}

func (m *Manager) initialize(ctx context.Context, config *Config) error {
	// Create management table to track partitioned tables
	if _, err := m.db.ExecContext(ctx, createManagementTable); err != nil {
		return fmt.Errorf("failed to create management table: %w", err)
	}

	for _, table := range config.Tables {
		err := m.checkTableColumnsExist(ctx, table)
		if err != nil {
			return err
		}
	}

	// Insert or update configuration for each table
	for _, table := range config.Tables {
		res, err := m.db.ExecContext(ctx, upsertSQL,
			ulid.Make().String(),
			table.Name,
			config.SchemaName,
			table.TenantId,
			table.TenantIdColumn,
			table.PartitionBy,
			table.PartitionType,
			table.PartitionCount,
			table.PartitionInterval,
			table.RetentionPeriod,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert table config for %s: %w", table.Name, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected for %s: %w", table.Name, err)
		}

		if rowsAffected < int64(1) {
			return fmt.Errorf("failed to upsert table config for %s", table.Name)
		}

		// Create future partitions based on PartitionCount
		if err = m.CreateFuturePartitions(ctx, table); err != nil {
			return fmt.Errorf("failed to create future partitions for %s: %w", table.Name, err)
		}
	}

	return nil
}

func (m *Manager) CreateFuturePartitions(ctx context.Context, tc Table) error {
	// Determine start time for new partitions
	today := m.clock.Now()

	// Create future partitions
	for i := uint(0); i < tc.PartitionCount; i++ {
		bounds := Bounds{
			From: today.Add(time.Duration(i) * tc.PartitionInterval.Duration()),
			To:   today.Add(time.Duration(i+1) * tc.PartitionInterval.Duration()),
		}

		// Check if partition already exists
		partitionName := m.generatePartitionName(tc, bounds)
		exists, err := m.partitionExists(ctx, partitionName)
		if err != nil {
			return fmt.Errorf("failed to check if partition exists: %w", err)
		}

		if exists {
			continue
		}

		// Create the partition
		if err = m.createPartition(ctx, tc, bounds); err != nil {
			return fmt.Errorf("failed to create future partition: %w", err)
		}

		m.logger.Info("created future partition",
			"table", tc.Name,
			"partition", partitionName,
			"from", bounds.From,
			"to", bounds.To)
	}

	return nil
}

// partitionExists checks if a partition table already exists
func (m *Manager) partitionExists(ctx context.Context, partitionName string) (bool, error) {
	var exists bool
	err := m.db.QueryRowContext(ctx, getPartitionExists, m.config.SchemaName, partitionName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check partition existence: %w", err)
	}

	return exists, nil
}

func (m *Manager) DropOldPartitions(ctx context.Context) error {
	// Get all managed tables and their retention periods
	type managedTable struct {
		TableName       string       `db:"table_name"`
		SchemaName      string       `db:"schema_name"`
		TenantId        string       `db:"tenant_id"`
		RetentionPeriod TimeDuration `db:"retention_period"`
	}

	var tables []managedTable
	if err := m.db.SelectContext(ctx, &tables, getManagedTablesRetentionPeriods); err != nil {
		return fmt.Errorf("failed to fetch managed tables: %w", err)
	}

	for _, table := range tables {
		// Find partitions older than the retention period
		cutoffTime := m.clock.Now().Add(-table.RetentionPeriod.Duration())
		pattern := fmt.Sprintf("%s_%%", table.TableName)
		if len(table.TenantId) > 0 {
			pattern = fmt.Sprintf("%s_%s_%%", table.TableName, table.TenantId)
		}

		var partitions []string
		if err := m.db.SelectContext(ctx, &partitions, partitionsQuery, table.SchemaName, pattern); err != nil {
			return fmt.Errorf("failed to fetch partitions for table %s: %w", table.TableName, err)
		}

		for _, partition := range partitions {
			// Extract date from partition name
			datePart, err := extractDateFromString(partition)
			if err != nil {
				return err
			}

			partitionDate, err := time.Parse(DateNoHyphens, datePart)
			if err != nil {
				m.logger.Error("failed to parse partition date",
					"partition", partition,
					"error", err)
				continue
			}

			// Check if partition is older than the retention period
			if partitionDate.Before(cutoffTime) {
				if m.hook != nil {
					// run any pre-drop hooks (backup data, upload to object storage)
					// todo(raymond): pass a context with a deadline to this func
					if err = m.hook(ctx, partition); err != nil {
						m.logger.Error("failed to run pre-drop hooks",
							"partition", partition,
							"error", err)
						continue
					}
				}

				m.logger.Info("no hook func was specified",
					"table", table.TableName,
					"partition", partition,
					"date", partitionDate)

				// Drop the partition
				if _, err = m.db.ExecContext(ctx, fmt.Sprintf(dropPartition, m.config.SchemaName, partition)); err != nil {
					m.logger.Error("failed to drop partition",
						"partition", partition,
						"error", err)
					continue
				}

				m.logger.Info("dropped old partition",
					"table", table.TableName,
					"partition", partition,
					"date", partitionDate)
			}
		}
	}

	return nil
}

// createPartition creates a partition for a table
func (m *Manager) createPartition(ctx context.Context, tableConfig Table, bounds Bounds) error {
	// Generate partition name based on bounds
	partitionName := m.generatePartitionName(tableConfig, bounds)

	// Create SQL for partition
	pQuery, err := m.generatePartitionSQL(partitionName, tableConfig, bounds)
	if err != nil {
		return err
	}

	// Execute partition creation
	_, err = m.db.ExecContext(ctx, pQuery)
	if err != nil {
		return err
	}

	return nil
}

// Maintain defines a regularly run maintenance routine
func (m *Manager) Maintain(ctx context.Context) error {
	// loop all tables and run maintenance

	for i := 0; i < len(m.config.Tables); i++ {
		table := m.config.Tables[i]

		// Drop old partitions if needed
		if err := m.DropOldPartitions(ctx); err != nil {
			return fmt.Errorf("failed to drop old partitions: %w", err)
		}

		// Check for necessary future partitions
		if err := m.CreateFuturePartitions(ctx, table); err != nil {
			return fmt.Errorf("failed to create future partitions: %w", err)
		}
	}

	return nil
}

// generatePartitionSQL generates the name of the partition table
func (m *Manager) generatePartitionSQL(name string, tc Table, b Bounds) (string, error) {
	switch tc.PartitionType {
	case "range":
		return m.generateRangePartitionSQL(name, tc, b), nil
	case "list", "hash":
		return "", fmt.Errorf("list and hash partitions are not implemented yet %q", tc.PartitionType)
	default:
		return "", fmt.Errorf("unsupported partition type %q", tc.PartitionType)
	}
}

func (m *Manager) generateRangePartitionSQL(name string, tc Table, b Bounds) string {
	if len(tc.TenantId) > 0 {
		return fmt.Sprintf(generatePartitionWithTenantIdQuery,
			m.config.SchemaName, name,
			m.config.SchemaName, tc.Name,
			tc.TenantId, b.From.Format(time.DateOnly),
			tc.TenantId, b.To.Format(time.DateOnly))
	}
	return fmt.Sprintf(generatePartitionQuery,
		m.config.SchemaName, name,
		m.config.SchemaName, tc.Name,
		b.From.Format(time.DateOnly),
		b.To.Format(time.DateOnly))
}

func (m *Manager) checkTableColumnsExist(ctx context.Context, tc Table) error {
	if len(tc.TenantIdColumn) > 0 {
		var exists bool
		err := m.db.QueryRowxContext(ctx, checkColumnExists, m.config.SchemaName, tc.Name, tc.TenantIdColumn).Scan(&exists)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("table %s does not have a tenant id column", tc.Name)
		}
	}

	var exists bool
	err := m.db.QueryRowxContext(ctx, checkColumnExists, m.config.SchemaName, tc.Name, tc.PartitionBy).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("table %s does not have a timestamp column named %s", tc.Name, tc.PartitionBy)
	}

	return nil
}

func (m *Manager) generatePartitionName(tc Table, bounds Bounds) string {
	datePart := strings.ReplaceAll(bounds.From.Format(time.DateOnly), "-", "")
	if len(tc.TenantId) > 0 {
		return fmt.Sprintf("%s_%s_%s", tc.Name, tc.TenantId, datePart)
	}
	return fmt.Sprintf("%s_%s", tc.Name, datePart)
}

func extractDateFromString(input string) (string, error) {
	// Regular expression to match exactly 8 digits at the end of the string
	re, err := regexp.Compile(`(\d{8})$`)
	if err != nil {
		return "", err
	}

	// Find the match
	matches := re.FindStringSubmatch(input)

	// If a match is found, return it
	if len(matches) > 1 {
		return matches[1], nil
	}

	// Return empty string if no match
	return "", nil
}

// Start begins the maintenance routine
func (m *Manager) Start(ctx context.Context) error {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.config.SampleRate)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stop:
				return
			case <-ticker.C:
				if err := m.Maintain(ctx); err != nil {
					m.logger.Error("an error occurred while running maintenance", "error", err)
				}
			}
		}
	}()
	return nil
}

// Stop gracefully stops the maintenance routine; used for testing
func (m *Manager) Stop() {
	close(m.stop)
	m.wg.Wait()
}

// AddManagedTable adds a new managed table to the partition manager
func (m *Manager) AddManagedTable(tc Table) error {
	// Validate the new table configuration
	if err := tc.Validate(); err != nil {
		return err
	}

	// Insert the new table configuration into the management table
	ctx := context.Background()
	res, err := m.db.ExecContext(ctx, upsertSQL,
		ulid.Make().String(),
		tc.Name,
		m.config.SchemaName,
		tc.TenantId,
		tc.TenantIdColumn,
		tc.PartitionBy,
		tc.PartitionType,
		tc.PartitionCount,
		tc.PartitionInterval,
		tc.RetentionPeriod,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert new table config for %s (tenant id: %s), error: %w", tc.Name, tc.TenantId, err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for %s (tenant id: %s), error: %w", tc.Name, tc.TenantId, err)
	}

	if rowsAffected < int64(1) {
		return fmt.Errorf("failed to upsert new table config for %s (tenant id: %s), error: %w", tc.Name, tc.TenantId, err)
	}

	// Create future partitions for the new table
	if err = m.CreateFuturePartitions(ctx, tc); err != nil {
		return fmt.Errorf("failed to create future partitions for %s (tenant id: %s), error: %w", tc.Name, tc.TenantId, err)
	}

	return nil
}

// ImportExistingPartitions scans the database for existing partitions and adds them to the partition management table
func (m *Manager) ImportExistingPartitions(ctx context.Context, tc Table) error {
	// Query to get all tables that look like partitions but aren't yet managed
	const findUnmanagedPartitionsQuery = `
	WITH managed_tables AS (
		SELECT table_name, tenant_id 
		FROM partman.partition_management
	)
	SELECT t.tablename, t.schemaname
	FROM pg_tables t
	LEFT JOIN managed_tables mt ON 
		split_part(t.tablename, '_', 1) = mt.table_name AND
		split_part(t.tablename, '_', 2) = mt.tenant_id
	WHERE t.tablename ~ '_[a-z0-9]+_\d{8}$'
	AND mt.table_name IS NULL;`

	type unManagedPartition struct {
		TableName  string `db:"tablename"`
		SchemaName string `db:"schemaname"`
	}

	var unManagedPartitions []unManagedPartition
	if err := m.db.SelectContext(ctx, &unManagedPartitions, findUnmanagedPartitionsQuery); err != nil {
		return fmt.Errorf("failed to query unmanaged partitions: %w", err)
	}

	// Group partitions by base table and tenant
	partitionGroups := make(map[string][]string)
	for _, p := range unManagedPartitions {
		parts := strings.Split(p.TableName, "_")
		if len(parts) != 3 {
			m.logger.Warn("skipping partition with unexpected format", "partition", p.TableName)
			continue
		}

		baseTable := parts[0]
		tenantID := parts[1]
		key := fmt.Sprintf("%s_%s", baseTable, tenantID)
		partitionGroups[key] = append(partitionGroups[key], p.TableName)
	}

	// For each group, create a management entry
	for key, partition := range partitionGroups {
		parts := strings.Split(key, "_")
		if len(parts) != 2 {
			m.logger.Error("invalid partition name format")
			return fmt.Errorf("invalid partition name format: %s", key)
		}

		tableName := parts[0]
		tenantID := parts[1]

		// Get partition interval and retention period from existing table
		const getPartitionInfoQuery = `
		SELECT a.attname as partition_by
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 
		AND c.relname = $2 
		AND a.attnum > 0 
		AND NOT a.attisdropped
		AND EXISTS (
			SELECT 1 FROM pg_partitioned_table pt 
			WHERE pt.partrelid = c.oid 
			AND pt.partattrs[1] = a.attnum
		);`

		var partitionBy string
		err := m.db.GetContext(ctx, &partitionBy, getPartitionInfoQuery, m.config.SchemaName, tableName)
		if err != nil {
			m.logger.Error("failed to get partition info",
				"table", tableName,
				"error", err)
			continue
		}

		err = m.checkTableColumnsExist(ctx, Table{
			Name:              tableName,
			Schema:            m.config.SchemaName,
			TenantId:          tenantID,
			TenantIdColumn:    tc.TenantIdColumn,
			PartitionBy:       partitionBy,
			PartitionType:     tc.PartitionType,
			PartitionInterval: tc.PartitionInterval,
			PartitionCount:    tc.PartitionCount,
			RetentionPeriod:   tc.RetentionPeriod,
		})
		if err != nil {
			return err
		}

		// Insert into partition management table
		res, err := m.db.ExecContext(ctx, upsertSQL,
			ulid.Make().String(),
			tableName,
			m.config.SchemaName,
			tenantID,
			tc.TenantIdColumn,
			partitionBy,
			tc.PartitionType,
			tc.PartitionCount,
			tc.PartitionInterval,
			tc.RetentionPeriod,
		)
		if err != nil {
			m.logger.Error("failed to insert management entry",
				"table", tableName,
				"tenant", tenantID,
				"error", err)
			return err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			m.logger.Error("failed to get rows affected",
				"table", tableName,
				"tenant", tenantID,
				"error", err)
			return err
		}

		if rowsAffected > 0 {
			m.logger.Info("imported existing partitioned table",
				"table", tableName,
				"tenant", tenantID,
				"partition_count", len(partition))
		}
	}

	return nil
}
