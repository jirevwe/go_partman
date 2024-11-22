package partition

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeDuration_Value(t *testing.T) {
	tests := []struct {
		name     string
		duration TimeDuration
		want     driver.Value
		wantErr  bool
	}{
		{
			name:     "24 hours",
			duration: TimeDuration(24 * time.Hour),
			want:     "24h0m0s",
			wantErr:  false,
		},
		{
			name:     "zero duration",
			duration: TimeDuration(0),
			want:     nil,
			wantErr:  false,
		},
		{
			name:     "complex duration",
			duration: TimeDuration(24*time.Hour + 30*time.Minute + 15*time.Second),
			want:     "24h30m15s",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.duration.Value()
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "empty schema name",
			config:  Config{},
			wantErr: "schema name cannot be empty",
		},
		{
			name: "no tables",
			config: Config{
				SchemaName: "test_schema",
			},
			wantErr: "at least one table configuration is required",
		},
		{
			name: "empty table name",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{},
				},
			},
			wantErr: "table[0]: name cannot be empty",
		},
		{
			name: "range partition with no columns",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:            "test_table",
						PartitionType:   TypeRange,
						RetentionPeriod: OneDay,
					},
				},
			},
			wantErr: "table[0]: partition_by is required for range partitions",
		},
		{
			name: "range partition with zero interval",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:            "test_table",
						PartitionType:   TypeRange,
						PartitionBy:     "col1",
						RetentionPeriod: OneMonth,
					},
				},
			},
			wantErr: "table[0]: partition interval must be set for range partitions",
		},
		{
			name: "valid range partition",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:              "test_table",
						PartitionType:     TypeRange,
						PartitionBy:       "col1",
						PartitionInterval: OneDay,
						RetentionPeriod:   OneMonth,
					},
				},
			},
			wantErr: "",
		},
		{
			name: "missing retention period",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:              "test_table",
						PartitionType:     TypeRange,
						PartitionBy:       "col1",
						PartitionInterval: OneDay,
					},
				},
			},
			wantErr: "table[0]: retention period must be set",
		},
		{
			name: "tenant id without column",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:              "test_table",
						PartitionType:     TypeRange,
						PartitionBy:       "col1",
						PartitionInterval: OneDay,
						RetentionPeriod:   OneMonth,
						TenantId:          "tenant1",
						// Missing TenantIdColumn
					},
				},
			},
			wantErr: "table[0]: the tenant id column cannot be empty if the tenant id value is set",
		},
		{
			name: "tenant column without id",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:              "test_table",
						PartitionType:     TypeRange,
						PartitionBy:       "col1",
						PartitionInterval: OneDay,
						RetentionPeriod:   OneMonth,
						TenantIdColumn:    "tenant_id",
						// Missing TenantId
					},
				},
			},
			wantErr: "table[0]: the tenant id value cannot be empty if the tenant id column is set",
		},
		{
			name: "valid config with tenant id",
			config: Config{
				SchemaName: "test_schema",
				Tables: []TableConfig{
					{
						Name:              "test_table",
						PartitionType:     TypeRange,
						PartitionBy:       "col1",
						PartitionInterval: OneDay,
						RetentionPeriod:   OneMonth,
						TenantId:          "tenant1",
						TenantIdColumn:    "tenant_id",
					},
				},
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err == nil || err.Error() != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
