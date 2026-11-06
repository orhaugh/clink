// The workload, run by mongosh against the replica set: 300 orders placed
// across six customers, then the amount of every tenth raised by 1.00 and every
// order whose number ends 3 in 25 deleted (the two sets do not meet). What it
// did is recorded in shop.orders_placed, which clink never reads: the ground
// truth verify.js checks clink's output against.
const shop = db.getSiblingDB("shop");
const customers = ["ada", "brian", "chen", "dara", "emeka", "farah"];
const placed = [];
for (let i = 1; i <= 300; i++) {
    const cents = 100 + ((i * 7919) % 50000);
    const order = {
        _id: new ObjectId(),
        number: i,
        customer: customers[i % customers.length],
        amount: Decimal128((cents / 100).toFixed(2)),
        placed_at: new Date(Date.UTC(2026, 10, 1) + i * 60000),
    };
    shop.orders.insertOne(order);
    placed.push(order);
}
for (const o of placed.filter((o) => o.number % 10 === 0)) {
    shop.orders.updateOne({ _id: o._id }, { $inc: { amount: Decimal128("1.00") } });
    o.updated_amount = shop.orders.findOne({ _id: o._id }).amount;
}
for (const o of placed.filter((o) => o.number % 25 === 3)) {
    shop.orders.deleteOne({ _id: o._id });
    o.deleted = true;
}
shop.orders_placed.insertMany(placed);
print(`placed ${placed.length} orders, updated ${placed.filter((o) => o.updated_amount).length}, ` +
      `deleted ${placed.filter((o) => o.deleted).length}`);
