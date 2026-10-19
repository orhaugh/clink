-- Runs once, when the PostgreSQL container first starts, in database `shop`.

-- The table clink captures.
CREATE TABLE orders (
    id         bigint PRIMARY KEY,
    customer   text           NOT NULL,
    status     text           NOT NULL,
    amount     numeric(12, 2) NOT NULL,
    -- Millisecond precision: clink holds timestamps as epoch milliseconds and
    -- refuses a value with finer digits rather than truncate it.
    updated_at timestamptz(3) NOT NULL,
    -- Incremented by every update, so each row version can be told apart.
    rev        integer        NOT NULL
);

-- Updates and deletes carry the whole old row: a delete then lands in
-- ClickHouse with the deleted row's values, and an update that leaves a large
-- (out-of-line) value unchanged still carries it.
ALTER TABLE orders REPLICA IDENTITY FULL;

-- What the replication slot streams. TRUNCATE is left out on purpose: it
-- cannot be expressed as row changes, and a published one stops the job.
CREATE PUBLICATION clink_orders FOR TABLE orders WITH (publish = 'insert, update, delete');

-- clink connects as a role that may replicate and read the table, nothing more.
CREATE ROLE clink_cdc LOGIN REPLICATION PASSWORD 'clink';
GRANT SELECT ON orders TO clink_cdc;

-- The rows that exist before clink starts: they arrive as the initial snapshot.
INSERT INTO orders
SELECT g,
       'customer-' || (g % 997),
       'new',
       round((g % 500) * 1.37 + 5, 2),
       timestamptz '2026-10-01 09:00:00+00' + g * interval '1 second',
       1
FROM generate_series(1, 20000) g;

-- The workload: one transaction after another of inserts, updates and a
-- delete, every fiftieth updating 500 rows at once, for `duration`.
CREATE PROCEDURE workload(duration interval)
LANGUAGE plpgsql AS $$
DECLARE
    stop    timestamptz := clock_timestamp() + duration;
    next_id bigint;
    n       bigint := 0;
BEGIN
    SELECT coalesce(max(id), 0) + 1 INTO next_id FROM orders WHERE id < 1000000000;
    WHILE clock_timestamp() < stop LOOP
        n := n + 1;
        INSERT INTO orders
        SELECT g, 'customer-' || (g % 997), 'new', round((random() * 900 + 5)::numeric, 2),
               date_trunc('milliseconds', clock_timestamp()), 1
        FROM generate_series(next_id, next_id + 2) g;
        next_id := next_id + 3;
        UPDATE orders
        SET status     = (ARRAY['paid', 'shipped', 'cancelled'])[1 + floor(random() * 3)::int],
            amount     = amount + 1.25,
            updated_at = date_trunc('milliseconds', clock_timestamp()),
            rev        = rev + 1
        WHERE id IN (SELECT (1 + floor(random() * (next_id - 1)))::bigint
                     FROM generate_series(1, CASE WHEN n % 50 = 0 THEN 500 ELSE 4 END));
        DELETE FROM orders WHERE id = (1 + floor(random() * (next_id - 1)))::bigint;
        COMMIT;
        PERFORM pg_sleep(0.01);
    END LOOP;
END
$$;
