-- Orders in MongoDB, read from the collection's change stream as typed rows,
-- and two results written back to MongoDB by clink.
--
-- The URI is an env:// reference: each Worker reads MONGODB_URI from its own
-- environment, so the connection string (and any credentials or TLS options in
-- it) is never part of this file, the submitted job or the Coordinator's API.

-- One row per change to shop.orders: an insert, update, replace or delete, its
-- kind in __op and the order's _id in __id. Each field is converted strictly:
-- a value its column cannot hold stops the job rather than being rounded.
CREATE TABLE order_changes (
    __op      VARCHAR,
    __id      VARCHAR,
    customer  VARCHAR,
    amount    DECIMAL(10, 2),
    placed_at TIMESTAMP(3)
) WITH (
    connector  = 'mongodb',
    uri        = 'env://MONGODB_URI',
    database   = 'shop',
    collection = 'orders'
);

-- Each customer's orders placed and their total, replaced by key as orders
-- arrive. mode='upsert' writes by the single-column PRIMARY KEY, here _id.
CREATE TABLE customer_totals (
    _id    VARCHAR PRIMARY KEY,
    orders BIGINT,
    total  DECIMAL(38, 2)
) WITH (
    connector  = 'mongodb',
    uri        = 'env://MONGODB_URI',
    database   = 'shop',
    collection = 'customer_totals',
    mode       = 'upsert'
);

-- Every change as it happened, appended.
CREATE TABLE order_log (
    op        VARCHAR,
    order_id  VARCHAR,
    customer  VARCHAR,
    amount    DECIMAL(10, 2),
    placed_at TIMESTAMP(3)
) WITH (
    connector  = 'mongodb',
    uri        = 'env://MONGODB_URI',
    database   = 'shop',
    collection = 'order_log'
);

-- Orders placed: the insert events. A change stream carries no before-image of
-- an update, so the source is append-only; totals over the inserts are exact,
-- whatever later changes the orders.
INSERT INTO customer_totals
SELECT customer AS _id, COUNT(*) AS orders, SUM(amount) AS total
FROM order_changes
WHERE __op = 'insert'
GROUP BY customer;

INSERT INTO order_log
SELECT __op, __id, customer, amount, placed_at
FROM order_changes;
