create table convoy.delivery_attempts
(
    id         VARCHAR                                            not null,
    project_id VARCHAR                                            not null,
    created_at TIMESTAMP WITH TIME ZONE default CURRENT_TIMESTAMP not null,
    data       jsonb,
    primary key (id, created_at, project_id)
)
    partition by RANGE (project_id, created_at);

CREATE TABLE convoy.delivery_attempts_tenant1_20250601
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-01') TO ('tenant1', '2025-06-02');

CREATE TABLE convoy.delivery_attempts_tenant1_20250602
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-02') TO ('tenant1', '2025-06-03');

CREATE TABLE convoy.delivery_attempts_tenant1_20250626
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-26') TO ('tenant1', '2025-06-27');

CREATE TABLE convoy.delivery_attempts_tenant1_20250627
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-27') TO ('tenant1', '2025-06-28');

CREATE TABLE convoy.delivery_attempts_tenant1_20250628
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-28') TO ('tenant1', '2025-06-29');

CREATE TABLE convoy.delivery_attempts_tenant1_20250629
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-29') TO ('tenant1', '2025-06-30');

CREATE TABLE convoy.delivery_attempts_tenant1_20250630
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-06-30') TO ('tenant1', '2025-07-01');

CREATE TABLE convoy.delivery_attempts_tenant1_20250701
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-07-01') TO ('tenant1', '2025-07-02');

CREATE TABLE convoy.delivery_attempts_tenant1_20250702
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-07-02') TO ('tenant1', '2025-07-03');

CREATE TABLE convoy.delivery_attempts_tenant1_20250703
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-07-03') TO ('tenant1', '2025-07-04');

CREATE TABLE convoy.delivery_attempts_tenant1_20250704
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-07-04') TO ('tenant1', '2025-07-05');

CREATE TABLE convoy.delivery_attempts_tenant1_20250705
    PARTITION OF convoy.delivery_attempts
        FOR VALUES FROM ('tenant1', '2025-07-05') TO ('tenant1', '2025-07-06');

-- Insert data into different partitions to test the UI
-- Each partition will have different amounts of data

-- Partition: delivery_attempts_tenant1_20250601 (June 1, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250601 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-01 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 500) i;

-- Partition: delivery_attempts_tenant1_20250602 (June 2, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250602 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-02 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'failed')
FROM generate_series(1, 800) i;

-- Partition: delivery_attempts_tenant1_20250626 (June 26, 2025) - Today
INSERT INTO convoy.delivery_attempts_tenant1_20250626 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-26 12:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'pending')
FROM generate_series(1, 500) i;

-- Partition: delivery_attempts_tenant1_20250627 (June 27, 2025) - Tomorrow
INSERT INTO convoy.delivery_attempts_tenant1_20250627 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-27 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 200) i;

-- Partition: delivery_attempts_tenant1_20250628 (June 28, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250628 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-28 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'retrying')
FROM generate_series(1, 300) i;

-- Partition: delivery_attempts_tenant1_20250629 (June 29, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250629 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-06-29 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
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
    '2025-07-01 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 100) i;

-- Partition: delivery_attempts_tenant1_20250702 (July 2, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250702 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-02 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'pending')
FROM generate_series(1, 750) i;

-- Partition: delivery_attempts_tenant1_20250703 (July 3, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250703 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-03 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 600) i;

-- Partition: delivery_attempts_tenant1_20250704 (July 4, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250704 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-04 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'failed')
FROM generate_series(1, 400) i;

-- Partition: delivery_attempts_tenant1_20250705 (July 5, 2025)
INSERT INTO convoy.delivery_attempts_tenant1_20250705 (id, project_id, created_at, data)
SELECT 
    'attempt_' || i,
    'tenant1',
    '2025-07-05 00:00:00+00'::timestamptz + (interval '30 seconds' * i),
    jsonb_build_object('event_id', 'event_' || i, 'status', 'delivered')
FROM generate_series(1, 1100) i;

-- Update table statistics for accurate row counts
ANALYZE convoy.delivery_attempts;
ANALYZE convoy.delivery_attempts_tenant1_20250601;
ANALYZE convoy.delivery_attempts_tenant1_20250602;
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