-- PostgreSQL public.orders -> ClickHouse, as a change log.
--
-- Submitted to the local cluster by the `submit` service in
-- docker-compose.yml. On any cluster:
--   clink run pipeline.sql --coordinator-host <host> --coordinator-port <port> \
--     --checkpoint-dir <dir> --checkpoint-interval-ms 2000

-- Source: the orders table, its 20,000 existing rows first (the initial
-- snapshot), then every committed change. changelog_mode='append' makes each
-- change one row; the __ columns describe it. on_decode_error='fail' stops
-- the job on a value it cannot carry exactly instead of dropping the change,
-- and is the default here, written out so it is seen.
CREATE TABLE orders_cdc (
    id         BIGINT PRIMARY KEY,
    customer   VARCHAR,
    status     VARCHAR,
    amount     DECIMAL(12, 2),
    updated_at TIMESTAMP(3) WITH TIME ZONE,
    rev        INTEGER,
    __op       VARCHAR,
    __version  BIGINT,
    __deleted  BOOLEAN
) WITH (
    connector         = 'postgres',
    format            = 'json',
    mode              = 'cdc',
    changelog_mode    = 'append',
    snapshot_mode     = 'initial',
    on_decode_error   = 'fail',
    conninfo          = 'host=postgres port=5432 dbname=shop user=clink_cdc password=clink',
    slot_name         = 'clink_orders',
    publication_names = 'clink_orders',
    "table"           = 'public.orders'
);

-- Sink: the change log in ClickHouse (clickhouse-init.sql), through the native
-- protocol, at least once.
CREATE TABLE orders_changes (
    id         BIGINT,
    customer   VARCHAR,
    status     VARCHAR,
    amount     DECIMAL(12, 2),
    updated_at TIMESTAMP(3) WITH TIME ZONE,
    rev        INTEGER,
    __op       VARCHAR,
    __version  BIGINT,
    __deleted  BOOLEAN
) WITH (
    connector         = 'clickhouse',
    insert_format     = 'native',
    host              = 'clickhouse',
    port              = '9000',
    database          = 'default',
    table             = 'orders_changes',
    user              = 'clink',
    password          = 'clink',
    batch_interval_ms = '500'
);

INSERT INTO orders_changes SELECT * FROM orders_cdc;
