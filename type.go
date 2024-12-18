package partman

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	ErrHookMustNotBeNil     = errors.New("[partition manager] hook must not be nil")
	ErrClockMustNotBeNil    = errors.New("[partition manager] clock must not be nil")
	ErrLoggerMustNotBeNil   = errors.New("[partition manager] logger must not be nil")
	ErrConfigMustNotBeNil   = errors.New("[partition manager] config must not be nil")
	ErrDbDriverMustNotBeNil = errors.New("[partition manager] db driver must not be nil")
)

// Hook a hook func executes any necessary operations before dropping a partition
// Example hooks:
// 1. Export data to cold storage
// 2. Create backup
// 3. Send notifications
// 4. Update metrics
type Hook func(ctx context.Context, partition string) error

type Option func(m *Manager) error

// WithDB function to set the database
func WithDB(db *sqlx.DB) Option {
	return func(m *Manager) error {
		if db == nil {
			return ErrDbDriverMustNotBeNil
		}

		m.db = db
		return nil
	}
}

// WithLogger function to set the logger
func WithLogger(logger Logger) Option {
	return func(m *Manager) error {
		if logger == nil {
			return ErrLoggerMustNotBeNil
		}

		m.logger = logger
		return nil
	}
}

// WithConfig function to set the config
func WithConfig(config *Config) Option {
	return func(m *Manager) error {
		if config == nil {
			return ErrConfigMustNotBeNil
		}

		if err := config.Validate(); err != nil {
			return err
		}

		m.config = config
		return nil
	}
}

// WithClock function to set the clock
func WithClock(clock Clock) Option {
	return func(m *Manager) error {
		if clock == nil {
			return ErrClockMustNotBeNil
		}

		m.clock = clock
		return nil
	}
}

// WithHook function to set the hook
func WithHook(hook Hook) Option {
	return func(m *Manager) error {
		if hook == nil {
			return ErrHookMustNotBeNil
		}

		m.hook = hook
		return nil
	}
}

type TimeDuration time.Duration

func (t *TimeDuration) Scan(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("unsupported value type %T", value)
	}

	td, err := time.ParseDuration(s)
	if err != nil {
		return err
	}

	*t = TimeDuration(td)

	return nil
}

func (t TimeDuration) Value() (driver.Value, error) {
	duration := time.Duration(t)
	if duration == 0 {
		return nil, nil
	}
	return duration.String(), nil
}

type PartitionerType string

const (
	TypeRange PartitionerType = "range"
)

const (
	DateNoHyphens = "20060102"
)

type Partitioner interface {
	// CreateFuturePartitions Create new partitions ahead of time
	CreateFuturePartitions(ctx context.Context, tableConfig Table) error

	// DropOldPartitions Drop old partitions based on retention policy
	DropOldPartitions(ctx context.Context) error

	// Maintain defines a regularly run maintenance routine
	Maintain(ctx context.Context) error

	// AddManagedTable adds a new managed table to the partition manager
	AddManagedTable(tc Table) error

	// ImportExistingPartitions scans the database for existing partitions and adds them to the partition management table
	ImportExistingPartitions(ctx context.Context, tc Table) error
}

type Bounds struct {
	From, To time.Time
}

type D struct {
	Key   string
	Value string
}

type Table struct {
	// Name of the partitioned table
	Name string

	// Schema of the partitioned table
	Schema string

	// TenantId Tenant ID column value (e.g., 01J2V010NV1259CYWQEYQC8F35)
	TenantId string

	// TenantIdColumn Tenant ID column to partition by (e.g., tenant_id)
	TenantIdColumn string

	// PartitionBy Timestamp column to partition by (e.g., created_at)
	PartitionBy string

	// PartitionType Postgres partition type
	PartitionType PartitionerType // "range", "list", or "hash"

	// PartitionInterval For range partitions (e.g., "1 month", "1 day")
	PartitionInterval time.Duration

	// PartitionCount is the number of partitions a table will have; defaults to 10
	PartitionCount uint

	// RetentionPeriod is how long after which partitions will be dropped (e.g., "1 month", "1 day")
	RetentionPeriod time.Duration
}

type Config struct {
	// SampleRate is how often the internal ticker runs
	SampleRate time.Duration

	// Tables holds all the partitioned tables being managed
	Tables []Table
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.SampleRate == 0 {
		c.SampleRate = time.Minute
	}

	for i, table := range c.Tables {
		if err := table.Validate(); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
		}
	}

	return nil
}

// Validate checks if the table configuration is valid
func (tc *Table) Validate() error {
	if tc.Name == "" {
		return errors.New("name cannot be empty")
	}

	if tc.Schema == "" {
		return errors.New("schema name cannot be empty")
	}

	if tc.RetentionPeriod == 0 {
		return errors.New("retention period must be set")
	}

	if len(tc.TenantId) > 0 {
		if tc.TenantIdColumn == "" {
			return errors.New("the tenant id column cannot be empty if the tenant id value is set")
		}
	}

	if len(tc.TenantIdColumn) > 0 {
		if tc.TenantId == "" {
			return errors.New("the tenant id value cannot be empty if the tenant id column is set")
		}
	}

	// set default value
	if tc.PartitionCount == 0 {
		tc.PartitionCount = 10
	}

	if tc.PartitionType == TypeRange {
		if len(tc.PartitionBy) == 0 {
			return errors.New("partition_by is required for range partitions")
		}

		if tc.PartitionInterval == 0 {
			return errors.New("partition interval must be set for range partitions")
		}
	}

	return nil
}

func (tc *Table) toManagedTable() managedTable {
	return managedTable{
		TableName:         tc.Name,
		SchemaName:        tc.Schema,
		TenantID:          tc.TenantId,
		TenantColumn:      strings.ToLower(tc.TenantIdColumn),
		PartitionBy:       tc.PartitionBy,
		PartitionType:     string(tc.PartitionType),
		PartitionCount:    tc.PartitionCount,
		PartitionInterval: TimeDuration(tc.PartitionInterval),
		RetentionPeriod:   TimeDuration(tc.RetentionPeriod),
	}
}

type StringArray []string

func (a *StringArray) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	var array []string

	switch v := src.(type) {
	case string:
		array = append(array, v)
	case []string:
		array = v
	}

	*a = array

	return nil
}

type managedTable struct {
	TableName         string       `db:"table_name"`
	SchemaName        string       `db:"schema_name"`
	TenantID          string       `db:"tenant_id"`
	TenantColumn      string       `db:"tenant_column"`
	PartitionBy       string       `db:"partition_by"`
	PartitionType     string       `db:"partition_type"`
	PartitionCount    uint         `db:"partition_count"`
	PartitionInterval TimeDuration `db:"partition_interval"`
	RetentionPeriod   TimeDuration `db:"retention_period"`
}

func (m *managedTable) toTable() Table {
	return Table{
		Name:              m.TableName,
		Schema:            m.SchemaName,
		TenantId:          m.TenantID,
		TenantIdColumn:    m.TenantColumn,
		PartitionBy:       m.PartitionBy,
		PartitionCount:    m.PartitionCount,
		PartitionType:     PartitionerType(m.PartitionType),
		RetentionPeriod:   time.Duration(m.RetentionPeriod),
		PartitionInterval: time.Duration(m.PartitionInterval),
	}
}
