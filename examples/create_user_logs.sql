-- Create the parent table
CREATE TABLE IF NOT EXISTS convoy.user_logs (
    id VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    action VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    details JSONB,
    PRIMARY KEY (id, created_at, project_id)
) PARTITION BY RANGE (project_id, created_at);

-- Create partitions for user_logs
CREATE TABLE IF NOT EXISTS convoy.user_logs_tenant1_20240601
PARTITION OF convoy.user_logs 
FOR VALUES FROM ('tenant1', '2024-06-01') TO ('tenant1', '2024-06-02');

CREATE TABLE IF NOT EXISTS convoy.user_logs_tenant1_20240602
PARTITION OF convoy.user_logs 
FOR VALUES FROM ('tenant1', '2024-06-02') TO ('tenant1', '2024-06-03');

CREATE TABLE IF NOT EXISTS convoy.user_logs_tenant1_20250626
PARTITION OF convoy.user_logs 
FOR VALUES FROM ('tenant1', '2025-06-26') TO ('tenant1', '2025-06-27');

CREATE TABLE IF NOT EXISTS convoy.user_logs_tenant1_20250627
PARTITION OF convoy.user_logs 
FOR VALUES FROM ('tenant1', '2025-06-27') TO ('tenant1', '2025-06-28');

CREATE TABLE IF NOT EXISTS convoy.user_logs_tenant1_20250628
PARTITION OF convoy.user_logs 
FOR VALUES FROM ('tenant1', '2025-06-28') TO ('tenant1', '2025-06-29');

-- Insert test data into user_logs partitions

-- 2024-06-01 data
INSERT INTO convoy.user_logs_tenant1_20240601 (id, user_id, project_id, action, created_at, details)
SELECT 
    'log_' || i,
    'user_' || (i % 100),
    'tenant1',
    CASE (i % 5) 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'view_page'
        WHEN 3 THEN 'click_button'
        WHEN 4 THEN 'submit_form'
    END,
    '2024-06-01 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('session_id', 'session_' || i, 'ip_address', '192.168.1.' || (i % 255))
FROM generate_series(1, 500) i;

-- 2024-06-02 data
INSERT INTO convoy.user_logs_tenant1_20240602 (id, user_id, project_id, action, created_at, details)
SELECT 
    'log_' || i,
    'user_' || (i % 100),
    'tenant1',
    CASE (i % 5) 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'view_page'
        WHEN 3 THEN 'click_button'
        WHEN 4 THEN 'submit_form'
    END,
    '2024-06-02 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('session_id', 'session_' || i, 'ip_address', '192.168.1.' || (i % 255))
FROM generate_series(1, 500) i;

-- 2025-06-26 data
INSERT INTO convoy.user_logs_tenant1_20250626 (id, user_id, project_id, action, created_at, details)
SELECT 
    'log_' || i,
    'user_' || (i % 100),
    'tenant1',
    CASE (i % 5) 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'view_page'
        WHEN 3 THEN 'click_button'
        WHEN 4 THEN 'submit_form'
    END,
    '2025-06-26 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('session_id', 'session_' || i, 'ip_address', '192.168.1.' || (i % 255))
FROM generate_series(1, 100) i;

-- 2025-06-27 data
INSERT INTO convoy.user_logs_tenant1_20250627 (id, user_id, project_id, action, created_at, details)
SELECT 
    'log_' || i,
    'user_' || (i % 100),
    'tenant1',
    CASE (i % 5) 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'view_page'
        WHEN 3 THEN 'click_button'
        WHEN 4 THEN 'submit_form'
    END,
    '2025-06-27 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('session_id', 'session_' || i, 'ip_address', '192.168.1.' || (i % 255))
FROM generate_series(1, 200) i;

-- 2025-06-28 data
INSERT INTO convoy.user_logs_tenant1_20250628 (id, user_id, project_id, action, created_at, details)
SELECT 
    'log_' || i,
    'user_' || (i % 100),
    'tenant1',
    CASE (i % 5) 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'view_page'
        WHEN 3 THEN 'click_button'
        WHEN 4 THEN 'submit_form'
    END,
    '2025-06-28 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('session_id', 'session_' || i, 'ip_address', '192.168.1.' || (i % 255))
FROM generate_series(1, 200) i;

-- Update table statistics
ANALYZE convoy.user_logs;
ANALYZE convoy.user_logs_tenant1_20240601;
ANALYZE convoy.user_logs_tenant1_20240602;
ANALYZE convoy.user_logs_tenant1_20250627;
ANALYZE convoy.user_logs_tenant1_20250626;
ANALYZE convoy.user_logs_tenant1_20250628;

-- Show summary of all tables
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) as size,
    (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass) as estimated_rows
FROM pg_tables 
WHERE schemaname = 'convoy' 
  AND (tablename LIKE 'delivery_attempts%' OR tablename LIKE 'user_logs%')
ORDER BY tablename; 