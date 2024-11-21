package partition

var createManagementTable = `
CREATE TABLE IF NOT EXISTS partition_management (
	table_name TEXT NOT NULL,
	tenant_id TEXT,
	partition_by TEXT[],
	partition_type TEXT NOT NULL,
	partition_interval TEXT NOT NULL,
	retention_period TEXT NOT NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (table_name, tenant_id)
);`

var upsertSQL = `
INSERT INTO partition_management (
	table_name,
	tenant_id,
	partition_by,
	partition_type, 
	partition_interval,
	retention_period
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (table_name, tenant_id) 
DO UPDATE SET 
	partition_type = EXCLUDED.partition_type,
	partition_interval = EXCLUDED.partition_interval,
	retention_period = EXCLUDED.retention_period;`

var getlatestPartition = `
SELECT tablename 
FROM pg_tables 
WHERE tablename LIKE $1 
ORDER BY tablename DESC 
LIMIT 1;`

// todo(raymond): paginate this query
var getManagedTablesRetentionPeriods = `
SELECT table_name, tenant_id, retention_period 
FROM partition_management;`

var getPartitionExists = `
SELECT EXISTS (
	SELECT 1 
	FROM pg_tables 
	WHERE tablename = $1
);`

var partitionsQuery = `
SELECT tablename 
FROM pg_tables
WHERE tablename ILIKE $1
ORDER BY tablename DESC;`

var dropPartition = `DROP TABLE IF EXISTS %s;`

var generatePartitionQuery = `CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s');`
