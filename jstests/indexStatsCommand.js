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
assert(isObject(result.keyPattern));
assert.neq(result.keyPattern, null);
assert(isString(result.storageNs));
assert(isNumber(result.bucketBodyBytes));
assert.eq(result.depth, 1);
assert(isObject(result.overall));
assert.neq(result.overall, null);

function checkStats(data) {
    assert(isNumber(data.count));
    assert(isNumber(data.mean));
    assert(isNumber(data.stddev));
    assert(isNumber(data.min));
    assert(isNumber(data.max));
}

function checkAreaStats(data) {
    assert(isNumber(data.numBuckets));

    assert(isObject(data.keyCount));
    assert.neq(data.keyCount, null);
    checkStats(data.keyCount);

    assert(isObject(data.usedKeyCount));
    assert.neq(data.usedKeyCount, null);
    checkStats(data.usedKeyCount);

    assert(isObject(data.bsonRatio));
    assert.neq(data.bsonRatio, null);
    checkStats(data.bsonRatio);

    assert(isObject(data.keyNodeRatio));
    assert.neq(data.keyNodeRatio, null);
    checkStats(data.keyNodeRatio);

    assert(isObject(data.fillRatio));
    assert.neq(data.fillRatio, null);
    checkStats(data.fillRatio);
}

assert(isObject(result.overall));
checkAreaStats(result.overall);

assert(result.perLevel instanceof Array);
for (var i = 0; i < result.perLevel.length; ++i) {
    assert(isObject(result.perLevel[i]));
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
