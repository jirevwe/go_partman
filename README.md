# go_partman

A Go native implementation of PostgreSQL table partitioning management, inspired by pg_partman. This library helps you automatically manage and maintain partitioned tables in PostgreSQL databases.

## Features

- Automatic partition creation and management
- Support for time-based range partitioning
- Configurable retention policies
- Automatic cleanup of old partitions
- Pre-creation of future partitions
- Multi-tenant support with tenant-specific partitioning
- Extensible pre-drop hooks for custom cleanup logic
- Simulated clock support for testing

## Installation

```bash
go get github.com/jirevwe/go_partman
```

## Usage

### Basic Setup

```go
import (
    "github.com/jirevwe/go_partman"
    "github.com/jmoiron/sqlx"
    "log/slog"
)

// Configure your partitioning strategy
config := partman.Config{
    SchemaName: "public",
    Tables: []partman.TableConfig{
        {
            Name:              "events",
            PartitionType:     partman.TypeRange,
            PartitionBy:       "created_at",
            PartitionInterval: partman.OneDay,    // Daily partitions
            PreCreateCount:    7,                   // Create 7 days ahead
            RetentionPeriod:   partman.OneMonth,  // Keep 1 month of data
        },
    },
}

// Create the manager
db := sqlx.MustConnect("postgres", "postgres://localhost:5432/postgres?sslmode=disable")
logger := slog.Default()
manager, err := partman.NewManager(db, config, logger, partman.NewRealClock())
if err != nil {
    log.Fatal(err)
}

// Initialize the partition manager
if err = manager.Initialize(context.Background(), config); err != nil {
    log.Fatal(err)
}

// Start the partition manager (runs a background goroutine)
if err = manager.Start(context.Background()); err != nil {
    log.Fatal(err)
}
```

### Multi-tenant Setup

```go
config := partman.Config{
    SchemaName: "public",
    Tables: []partman.TableConfig{
        {
            Name:              "events",
            TenantId:          "tenant1",           // Specify tenant ID
            TenantIdColumn:    "project_id",        // Column name for tenant ID
            PartitionType:     partman.TypeRange,
            PartitionBy:       "created_at",
            PartitionInterval: partman.OneDay,
            PreCreateCount:    7,
            RetentionPeriod:   partman.OneMonth,
        },
    },
}
```

### Adding a Managed Table

You can add a new managed table to the partition manager using the `AddManagedTable` method:

```go
newTableConfig := partman.TableConfig{
    Name:              "new_events",
    TenantId:          "tenant1",           // Specify tenant ID
    TenantIdColumn:    "project_id",        // Column name for tenant ID
    PartitionType:     partman.TypeRange,
    PartitionBy:       "created_at",
    PartitionInterval: partman.OneDay,
    PreCreateCount:    7,
    RetentionPeriod:   partman.OneMonth,
}

// Add the new managed table
if err := manager.AddManagedTable(newTableConfig); err != nil {
    log.Fatal(err)
}
```

### Table Requirements

Your table must be created as a partitioned table before using go_partman. Examples:

```sql
-- Single-tenant table
CREATE TABLE events (
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data JSONB,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Multi-tenant table
CREATE TABLE events (
    id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data JSONB,
    PRIMARY KEY (id, created_at, project_id)
) PARTITION BY RANGE (project_id, created_at);
```

## Features in Detail

### Partition Types

Currently supports:
- **Range Partitioning**: Time-based range partitioning with optional tenant ID support
- **List Partitioning**: Planned for future release
- **Hash Partitioning**: Planned for future release

### Time Intervals

Built-in time intervals:
- `OneDay`: 24 hours
- `OneWeek`: 7 days
- `OneMonth`: 30 days

### Maintenance Operations

- Automatically creates new partitions ahead of time based on `PreCreateCount`
- Drops old partitions based on `RetentionPeriod`
- Supports custom pre-drop hooks for data archival or backup operations

### Multi-tenant Support

- Optional tenant-based partitioning using `TenantId` and `TenantIdColumn`
- Separate partition management per tenant
- Automatic partition naming with tenant ID inclusion

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
