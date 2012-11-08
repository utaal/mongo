db.jstests_commands.drop();
db.createCollection("jstests_commands");

t = db.jstests_commands;

for (var i = 0; i < 3000; ++i) {
    t.insert({i: i, d: i % 13});
}

var result = t.indexStats({index: "_id_"});
if (result["bad cmd"]) {
    print("storageDetails command not available in this build: skipping");
    quit();
}

assert(result.ok);

assert(result.index === "_id_");
assert(result.isIdIndex === true);
assert(typeof result.keyPattern == 'object' && result.keyPattern);
assert(typeof result.storageNs == 'string');
assert(typeof result.bucketBodyBytes == 'number');
assert(result.depth === 1);
assert(typeof result.overall === 'object' && result.overall);

function isNonNullObject(o) {
    return typeof o == 'object' && o != null;
}

function checkStats(data) {
    assert(typeof data.count == 'number');
    assert(typeof data.mean == 'number');
    assert(typeof data.stddev == 'number');
    assert(typeof data.min == 'number');
    assert(typeof data.max == 'number');
}

function checkAreaStats(data) {
    assert(typeof data.numBuckets == 'number');

    assert(isNonNullObject(data.keyCount));
    checkStats(data.keyCount);

    assert(isNonNullObject(data.usedKeyCount));
    checkStats(data.usedKeyCount);

    assert(isNonNullObject(data.bsonRatio));
    checkStats(data.bsonRatio);

    assert(isNonNullObject(data.keyNodeRatio));
    checkStats(data.keyNodeRatio);

    assert(isNonNullObject(data.fillRatio));
    checkStats(data.fillRatio);
}

assert(isNonNullObject(result.overall));
checkAreaStats(result.overall);

assert(result.perLevel instanceof Array);
for (var i = 0; i < result.perLevel.length; ++i) {
    assert(isNonNullObject(result.perLevel[i]));
    checkAreaStats(result.perLevel[i]);
}

result = t.indexStats();
assert(!result.ok);
assert(result.errmsg.match(/index name is required/));

result = t.indexStats({index: "nonexistent"})
assert(!result.ok);
assert(result.errmsg.match(/index does not exist/));

result = t.indexStats({index: "_id_", expandNodes: ['string']})
assert(!result.ok);
assert(result.errmsg.match(/expandNodes.*numbers/));
