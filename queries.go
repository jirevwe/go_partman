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

var generatePartitionQuery = `CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s FOR VALUES FROM ('%s') TO ('%s');`

var generatePartitionWithTenantIdQuery = `CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s FOR VALUES FROM ('%s', '%s') TO ('%s', '%s');`

var checkColumnExists = `
SELECT EXISTS (SELECT 1 
FROM information_schema.columns
WHERE table_schema=$1 AND table_name=$2 AND column_name=$3);`
