package partition

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

// Manager Core partition manager
type Manager struct {
	db     *sqlx.DB
	logger *slog.Logger
	config Config
	clock  Clock
}

func NewManager(db *sqlx.DB, config Config, logger *slog.Logger, clock Clock) (*Manager, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	m := &Manager{
		db:     db,
		config: config,
		logger: logger,
		clock:  clock,
	}

	err := m.runMigrations(context.Background())
	if err != nil {
		return nil, err
	}

	return m, nil
}

// runUpgrades runs all the migrations on the management tables while keeping them backwards compatible
func (m *Manager) runMigrations(ctx context.Context) error {
	migrations := []string{
		createManagementTable,
	}

	for _, migration := range migrations {
		if _, err := m.db.ExecContext(ctx, migration); err != nil {
			return fmt.Errorf("failed to run migration: %s, with error %w", migration, err)
		}
	}

	return nil
}

func (m *Manager) Initialize(ctx context.Context, config Config) error {
	// Create management table to track partitioned tables
	if _, err := m.db.ExecContext(ctx, createManagementTable); err != nil {
		return fmt.Errorf("failed to create management table: %w", err)
	}

	// Insert or update configuration for each table
	for _, table := range config.Tables {
		_, err := m.db.ExecContext(ctx, upsertSQL,
			table.Name,
			table.TenantId,
			table.PartitionBy,
			table.PartitionType,
			table.PartitionInterval,
			table.RetentionPeriod,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert table config for %s: %w", table.Name, err)
		}

		// Create future partitions based on PreCreateCount
		if err := m.CreateFuturePartitions(ctx, table, table.PreCreateCount); err != nil {
			return fmt.Errorf("failed to create future partitions for %s: %w", table.Name, err)
		}
	}

	return nil
}

func (m *Manager) CreateFuturePartitions(ctx context.Context, tableConfig TableConfig, ahead uint) error {
	var latestPartition string

	// Get the latest partition's end time
	pattern := fmt.Sprintf("%s_%%", tableConfig.Name)
	err := m.db.QueryRowContext(ctx, getlatestPartition, pattern).Scan(&latestPartition)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get latest partition: %w", err)
	}

	// Determine start time for new partitions
	var startTime time.Time
	if errors.Is(err, sql.ErrNoRows) {
		// No existing partitions, start from now
		startTime = m.clock.Now()
	} else {
		// Extract date from partition name (format: table_name_YYYYMMDD or table_name_tenant_id_YYYYMMDD)
		datePart, err := extractDateFromString(latestPartition)
		if err != nil {
			return err
		}

		startTime, err = time.Parse(DateNoHyphens, datePart)
		if err != nil {
			return fmt.Errorf("failed to parse partition date: %w", err)
		}
		startTime = startTime.Add(time.Duration(tableConfig.PartitionInterval))
	}

	// Create future partitions
	for i := uint(0); i < ahead; i++ {
		bounds := Bounds{
			From: startTime.Add(time.Duration(i) * time.Duration(tableConfig.PartitionInterval)),
			To:   startTime.Add(time.Duration(i+1) * time.Duration(tableConfig.PartitionInterval)),
		}

		// Check if partition already exists
		partitionName := m.generatePartitionName(tableConfig, bounds)
		exists, err := m.partitionExists(ctx, partitionName)
		if err != nil {
			return fmt.Errorf("failed to check if partition exists: %w", err)
		}

		if exists {
			continue
		}

		// Create the partition
		if err := m.createPartition(ctx, tableConfig, bounds); err != nil {
			return fmt.Errorf("failed to create future partition: %w", err)
		}

		m.logger.Info("created future partition",
			"table", tableConfig.Name,
			"partition", partitionName,
			"from", bounds.From,
			"to", bounds.To)
	}

	return nil
}

// partitionExists checks if a partition table already exists
func (m *Manager) partitionExists(ctx context.Context, partitionName string) (bool, error) {
	var exists bool
	err := m.db.QueryRowContext(ctx, getPartitionExists, partitionName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check partition existence: %w", err)
	}

	return exists, nil
}

func (m *Manager) DropOldPartitions(ctx context.Context) error {
	// Get all managed tables and their retention periods
	type managedTable struct {
		TableName       string       `db:"table_name"`
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

		var partitions []string
		if err := m.db.SelectContext(ctx, &partitions, partitionsQuery, pattern); err != nil {
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
				// run any pre-drop hooks (backup data, upload to object storage)
				if err := m.runPreDropHooks(ctx, partition); err != nil {
					m.logger.Error("failed to run pre-drop hooks",
						"partition", partition,
						"error", err)
					continue
				}

				// Drop the partition
				if _, err := m.db.ExecContext(ctx, fmt.Sprintf(dropPartition, partition)); err != nil {
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

// runPreDropHooks executes any necessary operations before dropping a partition
func (m *Manager) runPreDropHooks(ctx context.Context, partition string) error {
	// Example hooks:
	// 1. Export data to cold storage
	// 2. Create backup
	// 3. Send notifications
	// 4. Update metrics

	// todo(raymond): implement actual hooks as needed
	return nil
}

// createPartition creates a partition for a table
func (m *Manager) createPartition(ctx context.Context, tableConfig TableConfig, bounds Bounds) error {
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

		// Check for necessary future partitions
		if err := m.CreateFuturePartitions(ctx, table, 1); err != nil {
			return fmt.Errorf("failed to create future partitions: %w", err)
		}

		// Drop old partitions if needed
		if err := m.DropOldPartitions(ctx); err != nil {
			return fmt.Errorf("failed to drop old partitions: %w", err)
		}
	}

	return nil
}

// generatePartitionSQL generates the name of the partition table
func (m *Manager) generatePartitionSQL(name string, tc TableConfig, b Bounds) (string, error) {
	switch tc.PartitionType {
	case "range":
		return m.generateRangePartitionSQL(name, tc, b), nil
	case "list", "hash":
		return "", fmt.Errorf("list and hash partitions are not implemented yet %q", tc.PartitionType)
	default:
		return "", fmt.Errorf("unsupported partition type %q", tc.PartitionType)
	}
}

func (m *Manager) generateRangePartitionSQL(name string, tc TableConfig, b Bounds) string {
	return fmt.Sprintf(generatePartitionQuery, name, tc.Name, b.From.Format(time.DateOnly), b.To.Format(time.DateOnly))
}

func (m *Manager) generatePartitionName(tableConfig TableConfig, bounds Bounds) string {
	datePart := strings.ReplaceAll(bounds.From.Format(time.DateOnly), "-", "")
	if len(tableConfig.TenantId) > 0 {
		return fmt.Sprintf("%s_%s_%s", tableConfig.Name, tableConfig.TenantId, datePart)
	}
	return fmt.Sprintf("%s_%s", tableConfig.Name, datePart)
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
