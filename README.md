# go_partman

A Go native implementation of PostgreSQL table partitioning management, inspired by pg_partman. This library helps you automatically manage and maintain partitioned tables in PostgreSQL databases.

## Features

- Automatic partition creation and management
- Support for time-based range partitioning
- Configurable retention policies
- Automatic cleanup of old partitions
- Pre-creation of future partitions
- Multi-tenant support
- Extensible pre-drop hooks for custom cleanup logic

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
)

// Configure your partitioning strategy
config := partition.Config{
    SchemaName: "public",
    Tables: []partition.TableConfig{
        {
            Name:              "events",
            TenantId:          "",                  // Optional: for multi-tenant setups
            PartitionType:     partition.TypeRange,
            PartitionBy:       []string{"created_at"},
            PartitionInterval: partition.OneDay,    // Daily partitions
            PreCreateCount:    7,                   // Create 7 days ahead
            RetentionPeriod:   partition.OneMonth,  // Keep 1 month of data
        },
    },
}

// Create the manager
db := sqlx.MustConnect("postgres", "postgres://localhost/mydb?sslmode=disable")
logger := slog.Default()
manager, err := partition.NewManager(db, config, logger, partition.NewRealClock())
if err != nil {
    log.Fatal(err)
}

// Initialize partitioning
err = manager.Initialize(context.Background(), config)
if err != nil {
    log.Fatal(err)
}

// Run maintenance (typically in a background goroutine)
go func() {
    ticker := time.NewTicker(1 * time.Hour)
    for range ticker.C {
        if err := manager.Maintain(context.Background()); err != nil {
            log.Printf("maintenance error: %v", err)
        }
    }
}()
```

### Table Requirements

Your table must be created as a partitioned table before using go_partman. Example:

```sql
CREATE TABLE events (
    id SERIAL,
    tenant_id TEXT,
    created_at TIMESTAMP NOT NULL,
    data JSONB
) PARTITION BY RANGE (created_at);
```

## Features in Detail

### Partition Types

- **Range Partitioning**: Currently supports time-based range partitioning
- **List Partitioning**: Planned for future release
- **Hash Partitioning**: Planned for future release

### Maintenance Operations

- Automatically creates new partitions ahead of time
- Drops old partitions based on retention policy
- Supports custom pre-drop hooks for data archival or backup

### Multi-tenant Support

- Optional `tenant-id` based partitioning
- Separate partition management per tenant

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
