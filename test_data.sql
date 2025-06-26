-- Insert test data into delivery_attempts partitions
-- This will help validate that the UI correctly displays row counts and sizes

-- Insert data into different partitions to test the UI
-- Each partition will have different amounts of data

-- Partition: delivery_attempts_tenant1_20240601 (June 1, 2024)
INSERT INTO convoy.delivery_attempts_tenant1_20240601 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2024-06-01 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1500) i;

-- Partition: delivery_attempts_tenant1_20240602 (June 2, 2024)
INSERT INTO convoy.delivery_attempts_tenant1_20240602 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2024-06-02 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'failed')
FROM generate_series(1, 800) i;

-- Partition: delivery_attempts_tenant1_20250626 (June 26, 2025) - Today
INSERT INTO convoy.delivery_attempts_tenant1_20250626 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-26 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'pending')
FROM generate_series(1, 2500) i;

-- Partition: delivery_attempts_tenant1_20250627 (June 27, 2025) - Tomorrow
INSERT INTO convoy.delivery_attempts_tenant1_20250627 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-27 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1200) i;

-- Partition: delivery_attempts_tenant1_20250628 (June 28, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250628 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-28 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'retrying')
FROM generate_series(1, 300) i;

-- Partition: delivery_attempts_tenant1_20250629 (June 29, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250629 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-29 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1800) i;

-- Partition: delivery_attempts_tenant1_20250630 (June 30, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250630 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-30 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'failed')
FROM generate_series(1, 950) i;

-- Partition: delivery_attempts_tenant1_20250701 (July 1, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250701 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-01 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 2100) i;

-- Partition: delivery_attempts_tenant1_20250702 (July 2, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250702 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-02 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'pending')
FROM generate_series(1, 750) i;

-- Partition: delivery_attempts_tenant1_20250703 (July 3, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250703 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-03 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1600) i;

-- Partition: delivery_attempts_tenant1_20250704 (July 4, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250704 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-04 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'failed')
FROM generate_series(1, 400) i;

-- Partition: delivery_attempts_tenant1_20250705 (July 5, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250705 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-05 12:00:00+01'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1100) i;

-- Update table statistics for accurate row counts
ANALYZE convoy.delivery_attempts;
ANALYZE convoy.delivery_attempts_tenant1_20240601;
ANALYZE convoy.delivery_attempts_tenant1_20240602;
ANALYZE convoy.delivery_attempts_tenant1_20250626;
ANALYZE convoy.delivery_attempts_tenant1_20250627;
ANALYZE convoy.delivery_attempts_tenant1_20250628;
ANALYZE convoy.delivery_attempts_tenant1_20250629;
ANALYZE convoy.delivery_attempts_tenant1_20250630;
ANALYZE convoy.delivery_attempts_tenant1_20250701;
ANALYZE convoy.delivery_attempts_tenant1_20250702;
ANALYZE convoy.delivery_attempts_tenant1_20250703;
ANALYZE convoy.delivery_attempts_tenant1_20250704;
ANALYZE convoy.delivery_attempts_tenant1_20250705;

-- Show summary of inserted data
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) as size,
    (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass) as estimated_rows
FROM pg_tables 
WHERE schemaname = 'convoy' 
  AND tablename LIKE 'delivery_attempts%'
ORDER BY tablename; 