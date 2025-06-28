package partman

var createSchema = `CREATE SCHEMA IF NOT EXISTS partman;`

var createManagementTable = `
CREATE TABLE IF NOT EXISTS partman.partition_management (
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
);`

var createUniqueIndex = `CREATE UNIQUE INDEX IF NOT EXISTS idx_table_name_tenant_id ON partman.partition_management (table_name, tenant_id);`

var upsertSQL = `
INSERT INTO partman.partition_management (
	id,	table_name, schema_name, tenant_id,
	tenant_column, partition_by, partition_type,
	partition_count, partition_interval,
	retention_period
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (table_name, tenant_id) 
DO UPDATE SET
	partition_interval = EXCLUDED.partition_interval,
	retention_period = EXCLUDED.retention_period,
	partition_count = EXCLUDED.partition_count,
	updated_at = now();`

var getlatestPartition = `
SELECT tablename 
FROM pg_tables 
WHERE schemaname = $1 AND tablename LIKE $2 
ORDER BY tablename DESC 
LIMIT 1;`

// todo(raymond): paginate this query?
var getManagedTablesRetentionPeriods = `
SELECT table_name, schema_name, tenant_id, retention_period
FROM partman.partition_management;`

var getPartitionExists = `
SELECT EXISTS (
	SELECT 1 
	FROM pg_tables 
	WHERE schemaname = $1 AND tablename = $2
);`

var partitionsQuery = `
SELECT tablename 
FROM pg_tables
WHERE schemaname = $1 AND tablename ILIKE $2;`

var dropPartition = `DROP TABLE IF EXISTS %s.%s;`

var generatePartitionQuery = `CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s FOR VALUES FROM ('%s 00:00:00+00'::timestamptz) TO ('%s 00:00:00+00'::timestamptz);`

var generatePartitionWithTenantIdQuery = `CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s FOR VALUES FROM ('%s', '%s 00:00:00+00'::timestamptz) TO ('%s', '%s 00:00:00+00'::timestamptz);`

var checkColumnExists = `
SELECT EXISTS (SELECT 1 
FROM information_schema.columns
WHERE table_schema=$1 AND table_name=$2 AND column_name=$3);`

var getManagedTablesQuery = `
SELECT 
    table_name,
    schema_name,
    tenant_id,
    tenant_column,
    partition_by,
    partition_type,
    partition_count,
    partition_interval,
    retention_period
FROM partman.partition_management;`

var getPartitionDetailsQuery = `
WITH partition_info AS (
    SELECT
        schemaname,
        tablename,
        pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) as size_bytes,
        (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass) as estimated_rows,
        pg_get_expr(c.relpartbound, c.oid) as partition_expression
    FROM pg_tables t
    JOIN pg_class c ON c.relname = t.tablename
    WHERE schemaname = $1 AND tablename LIKE $2
)
SELECT
    tablename as name,
    pg_size_pretty(size_bytes) as size,
    estimated_rows as rows,
    partition_expression as range,
    to_char(now(), 'YYYY-MM-DD') as created,
    size_bytes as size_bytes,
    COUNT(*) OVER() as total_count
FROM partition_info
ORDER BY tablename
LIMIT $3 OFFSET $4;`

var getParentTableInfoQuery = `
WITH parent_table_info AS (
    SELECT
        schemaname,
        tablename,
        (SELECT COUNT(*) FROM pg_tables WHERE schemaname = $1 AND tablename LIKE $2 || '_%') as partition_count
    FROM pg_tables
    WHERE schemaname = $1 AND tablename = $2
),
partition_sizes AS (
    SELECT
        schemaname,
        tablename,
        pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) as size_bytes,
        (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass) as estimated_rows
    FROM pg_tables
    WHERE schemaname = $1 AND tablename LIKE $2 || '_%'
),
totals AS (
    SELECT
        COALESCE(SUM(size_bytes), 0) as total_size_bytes,
        COALESCE(SUM(estimated_rows), 0) as total_rows
    FROM partition_sizes
)
SELECT
    pti.tablename as name,
    pg_size_pretty(t.total_size_bytes) as total_size,
    t.total_rows as total_rows,
    pti.partition_count as partition_count,
    t.total_size_bytes as total_size_bytes
FROM parent_table_info pti
CROSS JOIN totals t;`

var getManagedTablesListQuery = `
SELECT DISTINCT table_name, schema_name 
FROM partman.partition_management 
ORDER BY table_name;`
