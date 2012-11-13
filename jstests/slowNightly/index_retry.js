// Check index rebuild when MongoDB is killed

var ports = allocatePorts(1);
mongod = new MongodRunner(ports[0], "/data/db/index_retry", null, null, ["--journal"]);
var conn = mongod.start();

var test = conn.getDB("test");

var name = 'jstests_slownightly_index_retry';
t = test.getCollection(name);
t.drop();

// Insert a large number of documents, enough to ensure that an index build on these documents can
// be interrupted before complete.
for (i = 0; i < 1e6; ++i) {
    t.save( { a:i } );
    if (i % 10000 == 0) {
        print("i: " + i);
    }
}
test.getLastError();

function debug(x) {
    printjson(x);
}

/**
 * @return if there's a current running index build
 */
function indexBuildInProgress() {
    inprog = test.currentOp().inprog;
    debug(inprog);
    indexBuildOpId = -1;
    inprog.forEach(
        function( op ) {
            // Identify the index build as an insert into the 'test.system.indexes'
            // namespace.  It is assumed that no other clients are concurrently
            // accessing the 'test' database.
            if ( op.op == 'insert' && op.ns == 'test.system.indexes' ) {
                debug(op.opid);
                indexBuildOpId = op.opid;
            }
        }
    );
    return indexBuildOpId != -1;
}

function abortDuringIndexBuild(options) {

    // Create an index asynchronously by using a new connection.
    new Mongo(test.getMongo().host ).getCollection( t.toString() ).createIndex( { a:1 }, options);

    // Wait for the index build to start.
    var times = 0;
    assert.soon(
        function() {
            return indexBuildInProgress() && times++ >= 2;
        }
    );

    print("killing the mongod");
    stopMongod(ports[0], /* signal */ 9);
}

abortDuringIndexBuild({background:true});

print("sleeping");
sleep(2000);

conn = mongod.start(/* reuseData */ true);

assert.soon(
    function() {
        try {
            printjson(conn.getDB("test").getCollection(name).find({a:42}).hint({a:1}).next());
        } catch (e) {
            print(e);
            return false;
        }
        return true;
    },
    'index builds successfully'
);

print("Index built");

stopMongod(ports[0]);
print("SUCCESS!");
