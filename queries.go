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

// New queries for parent table and tenant management
var createParentTableTable = `
CREATE TABLE IF NOT EXISTS partman.parent_tables (
    id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    tenant_column TEXT NOT NULL,
    partition_by TEXT NOT NULL,
    partition_type TEXT NOT NULL,
    partition_interval TEXT NOT NULL,
    partition_count INT NOT NULL DEFAULT 10,
    retention_period TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name, schema_name)
);`

var createTenantsTable = `
CREATE TABLE IF NOT EXISTS partman.tenants (
    id TEXT PRIMARY KEY,
    parent_table_name TEXT NOT NULL,
    parent_table_schema TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_table_name, parent_table_schema) REFERENCES partman.parent_tables(table_name, schema_name),
    UNIQUE(parent_table_name, parent_table_schema, tenant_id)
);`

var upsertParentTableSQL = `
INSERT INTO partman.parent_tables (
    id, table_name, schema_name, tenant_column, partition_by, 
    partition_type, partition_interval, partition_count, retention_period
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (table_name, schema_name) 
DO UPDATE SET
    tenant_column = EXCLUDED.tenant_column,
    partition_by = EXCLUDED.partition_by,
    partition_type = EXCLUDED.partition_type,
    partition_interval = EXCLUDED.partition_interval,
    partition_count = EXCLUDED.partition_count,
    retention_period = EXCLUDED.retention_period,
    updated_at = now();`

var insertTenantSQL = `
INSERT INTO partman.tenants (id, parent_table_name, parent_table_schema, tenant_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (parent_table_name, parent_table_schema, tenant_id) 
DO NOTHING;`

var getParentTablesQuery = `
SELECT 
    table_name,
    schema_name,
    tenant_column,
    partition_by,
    partition_type,
    partition_interval,
    partition_count,
    retention_period
FROM partman.parent_tables
ORDER BY table_name, schema_name;`

var getTenantsQuery = `
SELECT 
    parent_table_name,
    parent_table_schema,
    tenant_id
FROM partman.tenants
WHERE parent_table_name = $1 AND parent_table_schema = $2
ORDER BY tenant_id;`

var getParentTableQuery = `
SELECT 
    table_name,
    schema_name,
    tenant_column,
    partition_by,
    partition_type,
    partition_interval,
    partition_count,
    retention_period
FROM partman.parent_tables
WHERE table_name = $1 AND schema_name = $2;`

var findUnmanagedPartitionsQuery = `
WITH bounds AS (
SELECT
	nmsp_parent.nspname AS parent_schema,
	parent.relname AS parent_table,
	nmsp_child.nspname AS partition_schema,
	child.relname AS partition_name,
	pg_get_expr(child.relpartbound, child.oid) AS partition_expression
FROM pg_inherits
		 JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		 JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		 JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
		 JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
WHERE parent.relkind = 'p'  AND nmsp_parent.nspname = $1
),
	 parsed_values AS (
		 SELECT
			 *,
			 regexp_matches(partition_expression, 'FROM \(([^)]+)\) TO \(([^)]+)\)', 'g') as extracted_values,
			 (regexp_matches(partition_expression, 'FROM \(([^)]+)\)', 'g'))[1] as from_values,
			 (regexp_matches(partition_expression, 'TO \(([^)]+)\)', 'g'))[1] as to_values
		 FROM bounds
	 )
SELECT
	parent_schema,
	parent_table,
	partition_name,
	partition_expression,
	CASE
		WHEN from_values LIKE '%,%' THEN replace(split_part(from_values, ', ', 1), '''', '')
		END as tenant_from,
	(CASE
		WHEN from_values LIKE '%,%' THEN split_part(from_values, ', ', 2)
		ELSE from_values
		END)::TIMESTAMP as timestamp_from,
	CASE
		WHEN to_values LIKE '%,%' THEN replace(split_part(to_values, ', ', 1), '''', '')
		END as tenant_to,
	(CASE
		WHEN to_values LIKE '%,%' THEN split_part(to_values, ', ', 2)
		ELSE to_values
		END)::TIMESTAMP as timestamp_to
FROM parsed_values;
`
