db.jstests_commands.drop();
db.createCollection("jstests_commands");

t = db.jstests_commands;

for (var i = 0; i < 3000; ++i) {
    t.insert({i: i, d: i % 13});
}

var result = t.diskStorageStats({numberOfChunks: 100});
if (result["bad cmd"]) {
    print("storageDetails command not available in this build: skipping");
    quit();
}

assert(result.ok);

function checkDiskStats(data) {
    assert(typeof data.extentHeaderBytes == 'number');
    assert(typeof data.recordHeaderBytes == 'number');
    assert(typeof data.numEntries == 'number');
    assert(data.bsonBytes instanceof NumberLong);
    assert(data.recBytes instanceof NumberLong);
    assert(data.onDiskBytes instanceof NumberLong);
    assert(typeof data.outOfOrderRecs == 'number');
    assert(typeof data.characteristicCount == 'number');
    assert(typeof data.characteristicAvg == 'number');
    assert(data.freeRecsPerBucket instanceof Array);
}

assert(result.extents && result.extents instanceof Array);

var extents = result.extents;

for (var i = 0; i < extents.length; ++i) {
    assert(typeof extents[i] == 'object' && extents[i]);
    assert(extents[i].range instanceof Array);
    assert(extents[i].range.length == 2);
    assert(extents[i].isCapped === false);
    checkDiskStats(extents[i]);
    assert(extents[i].chunks instanceof Array);
    for (var c = 0; c < extents[i].chunks[c]; ++c) {
        assert(typeof extents[i].chunks[c] == 'object' && extents[i].chunks[c]);
        checkStats(extents[i].chunks[c]);
    }
}

result = t.pagesInRAM({numberOfChunks: 100});
assert(result.ok);

assert(result.extents instanceof Array);
var extents = result.extents;

for (var i = 0; i < result.extents.length; ++i) {
    assert(typeof extents[i] == 'object' && extents[i]);
    assert(typeof extents[i].pageBytes == 'number');
    assert(typeof extents[i].onDiskBytes == 'number');
    assert(typeof extents[i].inMem == 'number');

    assert(extents[i].chunks instanceof Array);
    for (var c = 0; c < extents[i].chunks.length; ++c) {
        assert(typeof extents[i].chunks[c] == 'number');
    }
}

function checkErrorConditions(helper) {
    var result = helper.apply(t, [{extent: 'a'}]);
    assert(!result.ok);
    assert(result.errmsg.match(/extent.*must be a number/));

    result = helper.apply(t, [{range: [2, 4]}]);
    assert(!result.ok);
    assert(result.errmsg.match(/range is only allowed.*extent/));

    result = helper.apply(t, [{extent: 3, range: [3, 'a']}]);
    assert(!result.ok);
    assert(result.errmsg.match(/must be an array.*numeric elements/));

    result = helper.apply(t, [{granularity: 'a'}]);
    assert(!result.ok);
    assert(result.errmsg.match(/granularity.*number/));

    result = helper.apply(t, [{numberOfChunks: 'a'}]);
    assert(!result.ok);
    assert(result.errmsg.match(/numberOfChunks.*number/));

    result = helper.apply(t, [{extent: 100}]);
    assert(!result.ok);
    assert(result.errmsg.match(/extent.*does not exist/));
}

checkErrorConditions(t.diskStorageStats);
checkErrorConditions(t.pagesInRAM);
