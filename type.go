package partman

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

// Hook a hook func executes any necessary operations before dropping a partition
// Example hooks:
// 1. Export data to cold storage
// 2. Create backup
// 3. Send notifications
// 4. Update metrics
type Hook func(ctx context.Context, partition string) error

type TimeDuration time.Duration

func (t *TimeDuration) Duration() time.Duration {
	return time.Duration(*t)
}

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
	OneDay        = TimeDuration(24 * time.Hour)
	OneMonth      = 30 * OneDay
	OneWeek       = 7 * OneDay
)

type Partitioner interface {
	// CreateFuturePartitions Create new partitions ahead of time
	CreateFuturePartitions(ctx context.Context, tableConfig TableConfig, ahead uint) error

	// DropOldPartitions Drop old partitions based on retention policy
	DropOldPartitions(ctx context.Context) error
}

type Bounds struct {
	From, To time.Time
}

type D struct {
	Key   string
	Value string
}

type TableConfig struct {
	// Name of the partitioned table
	Name string

	// TenantId Tenant ID column value (e.g., 01J2V010NV1259CYWQEYQC8F35)
	TenantId string

	// TenantIdColumn Tenant ID column to partition by (e.g., tenant_id)
	TenantIdColumn string

	// PartitionBy Timestamp column to partition by (e.g., created_at)
	PartitionBy string

	// PartitionType Postgres partition type
	PartitionType PartitionerType // "range", "list", or "hash"

	// PartitionInterval For range partitions (e.g., "1 month", "1 day")
	PartitionInterval TimeDuration

	// PreCreateCount is the number of partitions to create ahead when the partition is first created
	PreCreateCount uint

	// RetentionPeriod is how long after which partitions will be dropped (e.g., "1 month", "1 day")
	RetentionPeriod TimeDuration
}

type Config struct {
	// SchemaName is the schema of the tables
	SchemaName string

	// SampleRate is how often the internal ticker runs
	SampleRate time.Duration

	// Tables holds all the partitioned tables being managed
	Tables []TableConfig
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.SchemaName == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	if c.SampleRate == 0 {
		return fmt.Errorf("sample-rate cannot be zero")
	}

	if len(c.Tables) == 0 {
		return fmt.Errorf("at least one table configuration is required")
	}

	for i, table := range c.Tables {
		if err := table.Validate(); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
		}
	}

	return nil
}

// Validate checks if the table configuration is valid
func (tc *TableConfig) Validate() error {
	if tc.Name == "" {
		return errors.New("name cannot be empty")
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
	if tc.PreCreateCount == 0 {
		tc.PreCreateCount = 10
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
