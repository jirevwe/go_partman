# go_partman

A Go native implementation of PostgreSQL table partitioning management, inspired by [pg_partman](https://github.com/pgpartman/pg_partman). This library helps you automatically manage and maintain partitioned tables in PostgreSQL databases.

> Disclaimers: 
> 1. This library is currently in alpha, hence the public APIs might change.
> 2. This library was built and is currently used to manage retention policies in [Convoy](https://github.com/frain-dev/convoy).
> 3. It is currently behind a feature flag (I'll update this disclaimer once it's GA).
> 4. This is the accompanying [pull request](https://github.com/frain-dev/convoy/pull/2194/files#diff-6c0399450dc8551e4cd42255ec24371c113d5b7771f6c6fdc0387cb0bc3df7f2) in Convoy if you want to see how it is integrated. 

#### Built By
<a href="https://getconvoy.io/?utm_source=go_partman">
<img src="https://getconvoy.io/svg/convoy-logo-full-new.svg" alt="Sponsored by Convoy"></a>

## Features

- **Automatic Partition Management**
  - Time-based range partitioning with configurable intervals
  - Automatic creation of future partitions
  - Scheduled cleanup of old partitions based on retention policies
  - Pre-drop hooks for custom cleanup logic (data archiving, backups, etc.)

- **Multi-tenancy Support**
  - Register and manage multiple tenants per table
  - Dynamic tenant registration at runtime
  - Tenant-specific partition management
  - Easy tenant discovery and querying

- **Flexible Configuration**
  - Builder pattern with functional options
  - Support for multiple database schemas
  - Configurable maintenance intervals
  - Custom logger integration

- **Web UI and API**
  - Built-in web interface for monitoring partitions
  - REST API endpoints for programmatic access
  - Size and usage statistics
  - Easy integration with existing HTTP servers

- **Developer-friendly**
  - Clear and consistent API
  - Detailed error reporting
  - Comprehensive logging
  - Support for testing with clock mocking

## Installation

```bash
go get github.com/jirevwe/go_partman
```

## Usage

### Basic Setup with the New API

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jirevwe/go_partman"
	"github.com/jmoiron/sqlx"
)

func main() {
	// Configure logger
	logger := partman.NewSlogLogger(slog.HandlerOptions{Level: slog.LevelDebug})

	ctx := context.Background()

	// Setup database connection
	pgxCfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/party?sslmode=disable")
	if err != nil {
		logger.Fatal(err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		logger.Fatal(err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	db := sqlx.NewDb(sqlDB, "pgx")

	// Initialize manager with functional options pattern
	manager, err := partman.NewManager(
		partman.WithDB(db),
		partman.WithLogger(logger),
		partman.WithConfig(&partman.Config{
			SampleRate: time.Second,
			Tables: []partman.Table{
				{
					Name:              "user_logs",
					Schema:            "convoy",
					TenantIdColumn:    "project_id",
					PartitionBy:       "created_at",
					PartitionType:     partman.TypeRange,
					PartitionInterval: time.Hour * 24,
					PartitionCount:    10,
					RetentionPeriod:   time.Hour * 24 * 30,
				},
			},
		}),
		partman.WithClock(partman.NewRealClock()),
	)
	if err != nil {
		logger.Fatal(err)
	}

	// Start the partition manager
	if err = manager.Start(ctx); err != nil {
		logger.Fatal(err)
	}
}
```

### Multi-tenant Setup

With the new API, multi-tenancy is handled through tenant registration. First, define your parent tables in the configuration:

```go
config := &partman.Config{
    SampleRate: time.Second,
    Tables: []partman.Table{
        {
            Name:              "user_logs",
            Schema:            "convoy",
            TenantIdColumn:    "project_id",
            PartitionBy:       "created_at",
            PartitionType:     partman.TypeRange,
            PartitionInterval: time.Hour * 24,
            PartitionCount:    10,
            RetentionPeriod:   time.Hour * 24 * 30,
        },
        {
            Name:              "delivery_attempts",
            Schema:            "convoy",
            TenantIdColumn:    "project_id",
            PartitionBy:       "created_at",
            PartitionType:     partman.TypeRange,
            PartitionInterval: time.Hour * 24,
            PartitionCount:    10,
            RetentionPeriod:   time.Hour * 24 * 7,
        },
    },
}
```

### Registering Tenants

You can register multiple tenants at once:

```go
// Define tenants to register
tenants := []partman.Tenant{
    {
        TableName:   "delivery_attempts",
        TableSchema: "convoy",
        TenantId:    "tenant1",
    },
    {
        TableName:   "delivery_attempts",
        TableSchema: "convoy",
        TenantId:    "tenant2",
    },
    {
        TableName:   "user_logs",
        TableSchema: "convoy",
        TenantId:    "tenant1",
    },
    {
        TableName:   "user_logs",
        TableSchema: "convoy",
        TenantId:    "tenant2",
    },
}

// Register all tenants
results, err := manager.RegisterTenants(ctx, tenants...)
if err != nil {
    logger.Fatal("failed to register tenants:", err)
}

// Process registration results
for _, result := range results {
    if len(result.Errors) > 0 {
        logger.Error("tenant registration had errors",
            "tenant", result.TenantId,
            "table", result.TableName,
            "errors", result.Errors)
    } else {
        logger.Info("tenant registered successfully",
            "tenant", result.TenantId,
            "table", result.TableName,
            "partitions_created", result.PartitionsCreated,
            "partitions_imported", result.ExistingPartitionsImported)
    }
}
```

### Registering Tenants at Runtime

You can register additional tenants after the partition manager is running:

```go
// Register a single tenant at runtime
result, err := manager.RegisterTenant(ctx, partman.Tenant{
    TableName:   "user_logs",
    TableSchema: "convoy",
    TenantId:    "tenant3",
})
if err != nil {
    log.Fatal("failed to register tenant:", err)
}

// Check registration results
if len(result.Errors) > 0 {
    log.Printf("Tenant registration had errors: %v", result.Errors)
} else {
    log.Printf("Tenant %s registered successfully with %d partitions created",
        result.TenantId, result.PartitionsCreated)
}
```

### Querying Partition Information

You can get information about parent tables and tenants:

```go
// Get all parent tables
parentTables, err := manager.GetParentTables(ctx)
if err != nil {
    log.Fatal(err)
}

for _, pt := range parentTables {
    log.Printf("Parent table: %s.%s (tenant_column: %s, partition_by: %s)",
        pt.Schema, pt.Name, pt.TenantIdColumn, pt.PartitionBy)

    // Get tenants for this parent table
    tenants, err := manager.GetTenants(ctx, pt.Schema, pt.Name)
    if err != nil {
        log.Printf("Error getting tenants: %v", err)
        continue
    }

    log.Printf("%s has %d tenants:", pt.Name, len(tenants))
    for _, tenant := range tenants {
        log.Printf("  - %s", tenant.TenantId)
    }
}
```

### Web UI Integration

go_partman comes with a built-in web UI for monitoring partitions. You can easily integrate it into your existing HTTP server:

```go
// Option 1: Use the combined UI and API handler
http.Handle("/partman/", http.StripPrefix("/partman", partman.UIHandler(manager)))

// Option 2: Use separate handlers for API and UI
http.Handle("/api/partman/", http.StripPrefix("/api/partman", partman.APIHandler(manager)))
http.Handle("/ui/partman/", http.StripPrefix("/ui/partman", partman.StaticHandler()))

// Start your HTTP server
log.Fatal(http.ListenAndServe(":8080", nil))
```

The web UI provides:
- Table listing with size and row count information
- Partition details with creation date and size statistics
- Easy navigation between parent tables and their partitions

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

### Maintenance Operations

- Automatically creates new partitions ahead of time based on `PartitionCount`
- Drops old partitions based on `RetentionPeriod`
- Supports custom pre-drop hooks for data archival or backup operations

### Multi-tenant Support

- Optional tenant-based partitioning using `TenantId` and `TenantIdColumn`
- Separate partition management per tenant
- Automatic partition naming with tenant ID inclusion

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## TODO

The following features would be great additions to this library. If you're interested in contributing, consider implementing one of these:

### Partition Types
- Implement list partitioning support
- Implement hash partitioning support
- Add composite partitioning capabilities

### Monitoring & Metrics
- Add built-in metrics collection
- Implement Prometheus/OpenTelemetry integration
- Add partition size/growth monitoring
- Create an alerting system for partition-related issues

### Advanced Management
- Implement partition merging capabilities
- Add partition splitting functionality
- Support online partition schema changes
- Implement partition rebalancing features

### Security
- Add authentication to the web UI
- Implement role-based access control
- Add audit logging for partition operations
- Enhance security configuration options

### Backup & Recovery
- Implement built-in backup functionality
- Add point-in-time recovery features
- Implement partition-level backup options
- Create disaster recovery procedures

### Testing Support
- Provide mock implementations for testing
- Add test utilities for partition management
- Enhance integration test helpers

### Performance
- Implement a caching mechanism
- Add batch operation support
- Provide partition access statistics
- Add performance optimization options

### Documentation & Examples
- Expand documentation for advanced use cases
- Add more comprehensive examples
- Create a detailed troubleshooting guide
- Improve API documentation

### High Availability
- Add support for clustered environments
- Implement failover handling
- Enhance distributed operation support
- Create a leader election mechanism

### Data Migration
- Add enhanced data migration tools
- Implement zero-downtime migrations
- Support schema evolution capabilities
- Add cross-database migration support

## License

MIT
