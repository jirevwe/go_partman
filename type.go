package partition

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"
)

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
	TypeHash  PartitionerType = "hash"
	TypeList  PartitionerType = "list"
)

const (
	DateNoHyphens = "20060102"
	OneDay        = TimeDuration(24 * time.Hour)
	OneMonth      = 30 * OneDay
	OneWeek       = 7 * OneDay
)

type Partitioner interface {
	// Initialize Create initial partition structure
	Initialize(ctx context.Context, config Config) error

	// CreateFuturePartitions Create new partitions ahead of time
	CreateFuturePartitions(ctx context.Context, ahead uint) error

	// DropOldPartitions Drop old partitions based on retention policy
	DropOldPartitions(ctx context.Context) error

	// Maintain  Manage partition maintenance
	Maintain(ctx context.Context) error
}

type Bounds struct {
	From, To time.Time
}

type TableConfig struct {
	// Name is the table
	Name string

	// TenantId
	TenantId string

	// Partition type and settings
	PartitionType PartitionerType // "range", "list", or "hash"

	// PartitionBy Columns to partition by, they are applied in order
	PartitionBy []string

	// PartitionInterval For range partitions (e.g., "1 month", "1 day")
	PartitionInterval TimeDuration

	// PreCreateCount is the number of partitions to create ahead when the partition is first created
	PreCreateCount uint

	// Retention settings
	RetentionPeriod TimeDuration
}

type Config struct {
	// SchemaName is the schema of the tables
	SchemaName string

	Tables []TableConfig
}
