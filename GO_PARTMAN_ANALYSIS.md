# Go Partman - Deep Dive Analysis

## Overview

`go_partman` is a Go-native PostgreSQL table partitioning management library inspired by pg_partman. It provides automatic management of partitioned tables with features like retention policies, multi-tenant support, and a web UI for monitoring.

## Architecture

### Core Components

#### 1. Manager (`manager.go`)

The central orchestrator that handles all partitioning operations.

**Key Responsibilities:**

- Database initialization and migration
- Partition creation and maintenance
- Retention policy enforcement
- Multi-tenant table management
- Background maintenance routines

**Key Methods:**

- `NewManager()` - Creates manager with functional options pattern
- `CreateFuturePartitions()` - Creates partitions ahead of time
- `DropOldPartitions()` - Removes partitions based on retention
- `Maintain()` - Runs maintenance operations
- `ImportExistingPartitions()` - Discovers and imports existing partitions
- `GetManagedTables()` - Returns list of managed tables
- `GetPartitions()` - Returns partition details for UI

#### 2. Configuration (`type.go`)

Defines the data structures and validation logic.

**Key Types:**

- `Table` - Configuration for a partitioned table
- `Config` - Global configuration
- `Bounds` - Time range for partitions
- `TimeDuration` - Custom type for database serialization

**Table Configuration Fields:**

```go
type Table struct {
    Name              string           // Table name
    Schema            string           // Database schema
    TenantId          string           // Tenant identifier
    TenantIdColumn    string           // Column name for tenant ID
    PartitionBy       string           // Timestamp column for partitioning
    PartitionType     PartitionerType  // "range", "list", "hash"
    PartitionInterval time.Duration    // Partition time interval
    PartitionCount    uint             // Number of future partitions
    RetentionPeriod   time.Duration    // How long to keep partitions
}
```

#### 3. Database Layer (`queries.go`)

Contains all SQL queries for partition management.

**Key Queries:**

- `createManagementTable` - Creates the management metadata table
- `upsertSQL` - Inserts/updates table configurations
- `getPartitionDetailsQuery` - Gets partition info for UI
- `getManagedTablesListQuery` - Lists managed tables

#### 4. Utilities

**Clock (`clock.go`)**

- `Clock` interface for time abstraction
- `RealClock` for production
- `SimulatedClock` for testing

**Logger (`logger.go`)**

- `Logger` interface for logging abstraction
- `SlogLogger` implementation using Go's slog package

### Web UI Architecture

#### Frontend (`web/`)

React + TypeScript + Vite application with Tailwind CSS.

**Key Components:**

- `App.tsx` - Main application component
- `api.ts` - API service layer
- `types.ts` - TypeScript type definitions

**Features:**

- Table selection sidebar
- Partition details table
- Real-time data fetching
- Error handling and loading states

#### Backend API (`ui.go`)

HTTP server that serves both the UI and API endpoints.

**API Endpoints:**

- `GET /api/tables` - Returns list of managed tables
- `GET /api/partitions?table=<name>` - Returns partition details

**Features:**

- CORS support
- JSON content-type enforcement
- Embedded static file serving
- Error handling

## Key Features

### 1. Partition Management

- **Automatic Creation**: Creates future partitions based on `PartitionCount`
- **Retention Policies**: Automatically drops old partitions
- **Multi-tenant Support**: Separate partitions per tenant
- **Range Partitioning**: Time-based partitioning with optional tenant ID

### 2. Database Integration

- **Migration System**: Automatic schema and table creation
- **Metadata Tracking**: Stores configuration in `partman.partition_management`
- **Existing Partition Import**: Discovers and imports existing partitions

### 3. Extensibility

- **Hooks**: Pre-drop hooks for custom cleanup logic
- **Functional Options**: Flexible configuration pattern
- **Interface-based Design**: Pluggable clock and logger implementations

### 4. Web UI

- **Real-time Monitoring**: View partition status and details
- **Table Management**: Browse managed tables and their partitions
- **Modern Interface**: Clean, responsive design with Tailwind CSS

## Current Changes (add-web-ui branch)

### Backend Changes

#### 1. Manager Extensions

```go
// New methods added to Manager
func (m *Manager) GetManagedTables(ctx context.Context) ([]string, error)
func (m *Manager) GetPartitions(ctx context.Context, schema, tableName string) ([]uiPartitionInfo, error)
```

#### 2. Database Queries

```sql
-- New query for partition details
var getPartitionDetailsQuery = `
WITH partition_info AS (
    SELECT
        schemaname, tablename,
        pg_total_relation_size(...) as size_bytes,
        (SELECT reltuples::bigint FROM pg_class...) as estimated_rows,
        pg_get_expr(c.relpartbound, c.oid) as partition_expression,
        c.relcreated as created_at
    FROM pg_tables t
    JOIN pg_class c ON c.relname = t.tablename
    WHERE schemaname = 'convoy' AND tablename LIKE 'delivery_attempts'
)
SELECT
    tablename as name,
    pg_size_pretty(size_bytes) as size,
    estimated_rows as rows,
    partition_expression as range,
    to_char(created_at, 'YYYY-MM-DD') as created
FROM partition_info
ORDER BY tablename;`

-- New query for managed tables list
var getManagedTablesListQuery = `
SELECT DISTINCT table_name
FROM partman.partition_management
ORDER BY table_name;`
```

#### 3. Type Extensions

```go
// New struct for UI partition information
type uiPartitionInfo struct {
    Name    string `json:"name"`
    Size    string `json:"size"`
    Rows    int64  `json:"rows"`
    Range   string `json:"range"`
    Created string `json:"created"`
}
```

#### 4. HTTP Handler

```go
// Enhanced UIHandler with API endpoints
func UIHandler(manager *Manager) http.Handler {
    api := &apiHandler{manager: manager}
    mux := http.NewServeMux()

    // API routes
    mux.Handle("/api/tables", enforceJSONHandler(setupCORS(http.HandlerFunc(api.handleGetTables))))
    mux.Handle("/api/partitions", enforceJSONHandler(setupCORS(http.HandlerFunc(api.handleGetPartitions))))

    // UI routes
    fileServer := http.FileServer(http.FS(fsys))
    mux.Handle("/", fileServer)

    return mux
}
```

### Frontend Changes

#### 1. React Application

- **App.tsx**: Main component with table selection and partition display
- **api.ts**: Service layer for API communication
- **types.ts**: TypeScript interfaces

#### 2. Build System

- **Vite**: Fast development and build tool
- **Tailwind CSS**: Utility-first CSS framework
- **TypeScript**: Type safety and better developer experience

## Usage Patterns

### 1. Basic Setup

```go
manager, err := partman.NewManager(
    partman.WithDB(db),
    partman.WithLogger(logger),
    partman.WithConfig(&partman.Config{SampleRate: time.Second}),
    partman.WithClock(partman.NewRealClock()),
)
```

### 2. Table Configuration

```go
table := partman.Table{
    Name:              "events",
    Schema:            "convoy",
    TenantId:          "tenant1",
    TenantIdColumn:    "project_id",
    PartitionBy:       "created_at",
    PartitionType:     partman.TypeRange,
    PartitionInterval: time.Hour * 24,
    PartitionCount:    10,
    RetentionPeriod:   time.Hour * 24 * 7,
}
```

### 3. Web UI Integration

```go
// Start HTTP server with UI
err = http.ListenAndServe(":8080", partman.UIHandler(manager))
```

## Database Schema

### Management Table

```sql
CREATE TABLE partman.partition_management (
    id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    tenant_id TEXT,
    tenant_column TEXT,
    partition_by TEXT NOT NULL,
    partition_type TEXT NOT NULL,
    partition_interval TEXT NOT NULL,
    retention_period TEXT NOT NULL,
    partition_count INT NOT NULL DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Testing Strategy

### 1. Unit Tests

- **manager_test.go**: Comprehensive manager functionality tests
- **type_test.go**: Configuration validation tests
- **clock_test.go**: Clock interface tests

### 2. Integration Tests

- Database integration with real PostgreSQL
- Partition creation and management workflows
- Multi-tenant scenarios

### 3. UI Tests

- Frontend component testing
- API integration testing
- End-to-end workflows

## Deployment Considerations

### 1. Database Requirements

- PostgreSQL with partitioning support
- Proper permissions for partition management
- Adequate storage for partition metadata

### 2. Application Deployment

- Go binary with embedded web assets
- Environment-specific configuration
- Health checks and monitoring

### 3. Web UI Deployment

- Build process for production assets
- Static file serving configuration
- CORS and security considerations

## Future Enhancements

### 1. Planned Features

- List and hash partitioning support
- Metrics and monitoring
- Advanced retention policies
- Partition compression

### 2. UI Improvements

- Real-time updates
- Partition management actions
- Configuration editing
- Performance analytics

### 3. Operational Features

- Backup and restore
- Migration tools
- Performance optimization
- Multi-database support

## Key Insights

1. **Functional Options Pattern**: Provides flexible configuration without complex constructors
2. **Interface-based Design**: Enables easy testing and mocking
3. **Embedded Web UI**: Single binary deployment with integrated monitoring
4. **Multi-tenant Architecture**: Scalable design for SaaS applications
5. **PostgreSQL Integration**: Deep integration with PostgreSQL partitioning features
6. **Extensible Hooks**: Custom logic integration points for enterprise use cases

This analysis provides a comprehensive understanding of the go_partman package architecture, features, and implementation details for future reference and development work.
