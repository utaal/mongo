db.jstests_commands.drop();
db.createCollection("jstests_commands");

t = db.jstests_commands;

for (var i = 0; i < 3000; ++i) {
    t.insert({i: i, d: i % 13});
}

var result = t.diskStorageStats();
if (result["bad cmd"]) {
    print("storageDetails command not available in this build: skipping");
    quit();
}

assert(result.ok);

result = t.pagesInRAM();
assert(result.ok);


