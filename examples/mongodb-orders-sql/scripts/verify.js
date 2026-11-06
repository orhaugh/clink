// Checks clink's output against the workload's own record (shop.orders_placed),
// reading MongoDB directly. Decimals are compared on the server, by value, and
// their BSON type is checked. Exits 1 on the first run that finds a mismatch.
const shop = db.getSiblingDB("shop");
const problems = [];
const placed = shop.orders_placed.find().toArray();
if (placed.length !== 300) {
    problems.push(`orders_placed holds ${placed.length} orders, not 300: did load.js run?`);
}

// customer_totals: one document per customer, its count and exact decimal sum.
const expected = shop.orders_placed
    .aggregate([{ $group: { _id: "$customer", orders: { $sum: 1 }, total: { $sum: "$amount" } } }])
    .toArray();
if (shop.customer_totals.countDocuments({}) !== expected.length) {
    problems.push(`customer_totals holds ${shop.customer_totals.countDocuments({})} documents, ` +
                  `not ${expected.length}`);
}
for (const e of expected) {
    const match = shop.customer_totals.countDocuments({
        _id: e._id,
        orders: e.orders,
        total: { $type: "decimal" },
        $expr: { $eq: ["$total", e.total] },
    });
    if (match !== 1) {
        const got = shop.customer_totals.findOne({ _id: e._id });
        problems.push(`customer ${e._id}: expected ${e.orders} orders totalling ${e.total}, ` +
                      `found ${got ? `${got.orders} totalling ${got.total}` : "nothing"}`);
    }
}

// order_log: exactly one row per change, with the values the change carried.
let events = 0;
const one = (what, filter) => {
    events++;
    const n = shop.order_log.countDocuments(filter);
    if (n !== 1) {
        problems.push(`${what}: ${n} rows in order_log, not 1`);
    }
};
for (const o of placed) {
    const id = o._id.toHexString();
    one(`insert of order ${o.number}`, {
        op: "insert", order_id: id, customer: o.customer, placed_at: o.placed_at,
        amount: { $type: "decimal" }, $expr: { $eq: ["$amount", o.amount] },
    });
    if (o.updated_amount) {
        one(`update of order ${o.number}`, {
            op: "update", order_id: id, $expr: { $eq: ["$amount", o.updated_amount] },
        });
    }
    if (o.deleted) {
        one(`delete of order ${o.number}`, { op: "delete", order_id: id, customer: null });
    }
}
const logged = shop.order_log.countDocuments({});
if (logged !== events) {
    problems.push(`order_log holds ${logged} rows for ${events} changes`);
}

if (problems.length > 0) {
    print(`FAIL: ${problems.length} problem(s)`);
    problems.slice(0, 20).forEach((p) => print(`  ${p}`));
    quit(1);
}
print(`PASS: ${expected.length} customer totals exact; ${events} changes logged exactly once`);
