package partman

import (
	"context"
	"errors"
	"fmt"
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
	logger Logger
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

func NewAndStart(db *sqlx.DB, config *Config, logger Logger, clock Clock) (*Manager, error) {
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

func (m *Manager) GetConfig() Config {
	return *m.config
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

	var existingTables []managedTable
	if err := m.db.SelectContext(ctx, &existingTables, getManagedTablesQuery); err != nil {
		return fmt.Errorf("failed to load existing managed tables: %w", err)
	}

	// Use a map to deduplicate tables based on name and tenant ID
	uniqueTables := make(map[string]Table)

	// Add existing tables first
	for _, et := range existingTables {
		te := et.toTable()
		key := generateTableKey(te.Name, te.TenantId)
		uniqueTables[key] = te
	}

	// Add or update with new config tables
	for _, nt := range config.Tables {
		key := generateTableKey(nt.Name, nt.TenantId)
		uniqueTables[key] = nt
	}

	// Convert back to slice
	mergedTables := make([]Table, 0, len(uniqueTables))
	for _, table := range uniqueTables {
		mergedTables = append(mergedTables, table)
	}
	m.config.Tables = mergedTables

	// Validate and initialize each table
	for _, table := range m.config.Tables {
		err := m.checkTableColumnsExist(ctx, table)
		if err != nil {
			return err
		}

		mTable := table.toManagedTable()

		// Insert or update configuration
		res, err := m.db.ExecContext(ctx, upsertSQL,
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
			From: today.Add(time.Duration(i) * tc.PartitionInterval),
			To:   today.Add(time.Duration(i+1) * tc.PartitionInterval),
		}

		// Check if partition already exists
		partitionName := m.generatePartitionName(tc, bounds)
		exists, err := m.partitionExists(ctx, partitionName, tc.Schema)
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
func (m *Manager) partitionExists(ctx context.Context, partitionName, partitionSchemaName string) (bool, error) {
	var exists bool
	err := m.db.QueryRowContext(ctx, getPartitionExists, partitionSchemaName, partitionName).Scan(&exists)
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
		cutoffTime := m.clock.Now().Add(time.Duration(-table.RetentionPeriod))
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
				if _, err = m.db.ExecContext(ctx, fmt.Sprintf(dropPartition, table.SchemaName, partition)); err != nil {
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
			tc.Schema, name,
			tc.Schema, tc.Name,
			tc.TenantId, b.From.Format(time.DateOnly),
			tc.TenantId, b.To.Format(time.DateOnly))
	}
	return fmt.Sprintf(generatePartitionQuery,
		tc.Schema, name,
		tc.Schema, tc.Name,
		b.From.Format(time.DateOnly),
		b.To.Format(time.DateOnly))
}

func (m *Manager) checkTableColumnsExist(ctx context.Context, tc Table) error {
	if len(tc.TenantIdColumn) > 0 && len(tc.TenantId) > 0 {
		var exists bool
		err := m.db.QueryRowxContext(ctx, checkColumnExists, tc.Schema, tc.Name, tc.TenantIdColumn).Scan(&exists)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("table %s does not have a tenant id column", tc.Name)
		}
	}

	var exists bool
	err := m.db.QueryRowxContext(ctx, checkColumnExists, tc.Schema, tc.Name, tc.PartitionBy).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("table %s does not have a timestamp column named %s", tc.Name, tc.PartitionBy)
	}

	return nil
}

func (m *Manager) generatePartitionName(tc Table, b Bounds) string {
	datePart := b.From.Format(DateNoHyphens)

	if len(tc.TenantId) > 0 {
		return strings.ToLower(fmt.Sprintf("%s_%s_%s", tc.Name, tc.TenantId, datePart))
	}
	return strings.ToLower(fmt.Sprintf("%s_%s", tc.Name, datePart))
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

// generateTableKey creates a unique key for a table based on its name and tenant ID
func generateTableKey(tableName, tenantID string) string {
	if tenantID != "" {
		return strings.ToLower(fmt.Sprintf("%s_%s", tableName, tenantID))
	}
	return strings.ToLower(tableName)
}

// AddManagedTable adds a new managed table to the partition manager
func (m *Manager) AddManagedTable(tc Table) error {
	if err := tc.Validate(); err != nil {
		return fmt.Errorf("invalid table configuration: %w", err)
	}

	// Use a map to deduplicate tables
	uniqueTables := make(map[string]Table, len(m.config.Tables)+1)

	// Add existing tables
	for _, existing := range m.config.Tables {
		key := generateTableKey(existing.Name, existing.TenantId)
		uniqueTables[key] = existing
	}

	// Add or update new table
	key := generateTableKey(tc.Name, tc.TenantId)
	uniqueTables[key] = tc

	// Convert back to slice
	newTables := make([]Table, 0, len(uniqueTables))
	for _, table := range uniqueTables {
		newTables = append(newTables, table)
	}
	m.config.Tables = newTables

	// Initialize the new table
	ctx := context.Background()
	mTable := tc.toManagedTable()

	// Insert or update configuration
	res, err := m.db.ExecContext(ctx, upsertSQL,
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
	if err != nil {
		return fmt.Errorf("failed to upsert table config: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected < 1 {
		return fmt.Errorf("failed to upsert table config")
	}

	// Create future partitions for the new table
	if err = m.CreateFuturePartitions(ctx, tc); err != nil {
		return fmt.Errorf("failed to create future partitions: %w", err)
	}

	return nil
}

// ImportExistingPartitions scans the database for existing partitions and adds them to the partition management table
func (m *Manager) ImportExistingPartitions(ctx context.Context, tc Table) error {
	errString := make([]string, 0)

	// Query to get all tables that look like partitions but aren't yet managed
	const findUnmanagedPartitionsQuery = `
	WITH bounds AS (
	SELECT
		nmsp_parent.nspname AS parent_schema,
		parent.relname AS parent_table,
		nmsp_child.nspname AS partition_schema,
		child.relname AS partition_name,
		pg_get_expr(child.relpartbound, child.oid) AS partition_expression
	FROM pg_inherits
			 JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
			 JOIN pg_class child ON pg_inherits.inhrelid = child.oid
			 JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
			 JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
	WHERE parent.relkind = 'p'  AND nmsp_parent.nspname = $1
	),
		 parsed_values AS (
			 SELECT
				 *,
				 regexp_matches(partition_expression, 'FROM \(([^)]+)\) TO \(([^)]+)\)', 'g') as extracted_values,
				 (regexp_matches(partition_expression, 'FROM \(([^)]+)\)', 'g'))[1] as from_values,
				 (regexp_matches(partition_expression, 'TO \(([^)]+)\)', 'g'))[1] as to_values
			 FROM bounds
		 )
	SELECT
		parent_schema,
		parent_table,
		partition_name,
		partition_expression,
		CASE
			WHEN from_values LIKE '%,%' THEN replace(split_part(from_values, ', ', 1), '''', '')
			END as tenant_from,
		(CASE
			WHEN from_values LIKE '%,%' THEN split_part(from_values, ', ', 2)
			ELSE from_values
			END)::TIMESTAMP as timestamp_from,
		CASE
			WHEN to_values LIKE '%,%' THEN replace(split_part(to_values, ', ', 1), '''', '')
			END as tenant_to,
		(CASE
			WHEN to_values LIKE '%,%' THEN split_part(to_values, ', ', 2)
			ELSE to_values
			END)::TIMESTAMP as timestamp_to
	FROM parsed_values;`

	type unManagedPartition struct {
		TenantFrom    *string `db:"tenant_from"`
		TenantTo      *string `db:"tenant_to"`
		TimestampFrom string  `db:"timestamp_from"`
		TimestampTo   string  `db:"timestamp_to"`
		PartitionName string  `db:"partition_name"`
		PartitionExpr string  `db:"partition_expression"`
		ParentSchema  string  `db:"parent_schema"`
		ParentTable   string  `db:"parent_table"`
	}

	var unManagedPartitions []unManagedPartition
	if err := m.db.SelectContext(ctx, &unManagedPartitions, findUnmanagedPartitionsQuery, tc.Schema); err != nil {
		return fmt.Errorf("failed to query unmanaged partitions: %w", err)
	}

	// Use a map to deduplicate tables based on name and tenant ID
	uniqueTables := make(map[string]Table)

	// Add existing managed tables first
	for _, existing := range m.config.Tables {
		key := generateTableKey(existing.Name, existing.TenantId)
		uniqueTables[key] = existing
	}

	// Process unmanaged partitions
	for _, p := range unManagedPartitions {
		// check to see if the date part exists
		datePart, err := extractDateFromString(p.PartitionName)
		if err != nil {
			errString = append(errString, err.Error())
			continue
		}

		_, err = time.Parse(DateNoHyphens, datePart)
		if err != nil {
			errString = append(errString, err.Error())
			continue
		}

		parts := strings.Split(p.PartitionName, "_")
		if len(parts) < 2 {
			errString = append(errString, fmt.Sprintf("invalid partition name: %s", p.PartitionName))
			continue
		}

		table := Table{
			Name:              p.ParentTable,
			Schema:            p.ParentSchema,
			TenantId:          nullOrZero(p.TenantFrom),
			TenantIdColumn:    tc.TenantIdColumn,
			PartitionBy:       tc.PartitionBy,
			PartitionType:     tc.PartitionType,
			PartitionInterval: tc.PartitionInterval,
			PartitionCount:    tc.PartitionCount,
			RetentionPeriod:   tc.RetentionPeriod,
		}

		err = m.checkTableColumnsExist(ctx, table)
		if err != nil {
			errString = append(errString, err.Error())
			continue
		}

		mTable := table.toManagedTable()

		// Insert into partition management table
		res, err := m.db.ExecContext(ctx, upsertSQL,
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
		if err != nil {
			m.logger.Error("failed to insert management entry ",
				"table", p.ParentTable,
				"tenant", p.TenantFrom,
				"error", err)
			errString = append(errString, err.Error())
			continue
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			m.logger.Error("failed to get rows affected ",
				"table", p.ParentTable,
				"tenant", p.TenantFrom,
				"error", err)
			errString = append(errString, err.Error())
			continue
		}

		if rowsAffected > 0 {
			m.logger.Info("imported existing partitioned table ",
				"table ", p.ParentTable,
				"tenant ", p.TenantFrom)

			// Add to our map of unique tables
			key := generateTableKey(table.Name, table.TenantId)
			uniqueTables[key] = table
		}
	}

	// Convert map back to slice and update config
	newTables := make([]Table, 0, len(uniqueTables))
	for _, table := range uniqueTables {
		newTables = append(newTables, table)
	}
	m.config.Tables = newTables

	if len(errString) > 0 {
		return errors.New(strings.Join(errString, "; "))
	}
	return nil
}

func nullOrZero(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
