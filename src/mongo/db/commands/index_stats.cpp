/** @file storage_details.cpp
 * collection.storageDetails({...}) command
 */

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "mongo/pch.h"

#include <iostream>
#include <string>

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/btree.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

namespace {

    class IndexStatsCmd : public Command {
    public:
        IndexStatsCmd() : Command( "indexStats" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "TODO. Slow."; }

        //TODO(andrea.lattuada) verify this is enough
        virtual LockType locktype() const { return READ; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl);

        bool runInternal(string& errmsg, NamespaceDetails* nsd, BSONObjBuilder& result);
    } indexStatsCmd;

    bool IndexStatsCmd::run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                                BSONObjBuilder& result, bool fromRepl) {
        string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
        NamespaceDetails* nsd = nsdetails(ns.c_str());
        if (!cmdLine.quiet) {
            tlog() << "CMD: indexStats " << ns << endl;
        }
        if (!nsd) {
            errmsg = "ns not found";
            return false;
        }

        return runInternal(errmsg, nsd, result);
    }

    bool inspectBucket(DiskLoc& dl, int depth, BSONObjBuilder& bucketBuilder) {
        const BtreeBucket<V1>* bucket = dl.btree<V1>();
        int usedKeyCount = 0;
        int totalKeyCount = 0;
        BSONArrayBuilder childsArrayBuilder(bucketBuilder.subarrayStart("childs"));
        for ( int i = 0; i < bucket->getN(); i++ ) {
            const BucketBasics<V1>::_KeyNode& kn = bucket->k(i);

            if ( kn.isUsed() ) {
                ++usedKeyCount;
            }
            ++totalKeyCount;
            if ( !kn.prevChildBucket.isNull() ) {
                DiskLoc left = kn.prevChildBucket;
                BSONObjBuilder childBucketBuilder(childsArrayBuilder.subobjStart());
                inspectBucket(left, depth + 1, childBucketBuilder);
                childBucketBuilder.doneFast();
            }
            //const BtreeBucket *b = left.btree<V>();
            //if ( strict ) {
                //verify( b->parent == thisLoc );
            //}
            //else {
                //wassert( b->parent == thisLoc );
            //}
            //kc += b->fullValidate(kn.prevChildBucket, order, unusedCount, strict, depth+1);
            //}
        }
        childsArrayBuilder.doneFast();
        //if ( !this->nextChild.isNull() ) {
            //DiskLoc ll = this->nextChild;
            //const BtreeBucket *b = ll.btree<V>();
            //if ( strict ) {
                //verify( b->parent == thisLoc );
            //}
            //else {
                //wassert( b->parent == thisLoc );
            //}
            //kc += b->fullValidate(this->nextChild, order, unusedCount, strict, depth+1);
        //}
        bucketBuilder << "n" << bucket->getN()
                      << "depth" << depth
                      << "usedKeys" << usedKeyCount
                      << "totalKeys" << totalKeyCount;
        if (bucket->getN() > 0) {
            const BucketBasics<V1>::KeyNode& firstKeyNode = bucket->keyNode(0);
            const KeyV1& key = firstKeyNode.key;
            bucketBuilder.append("firstKey", key.toBson());
        }
        return true;
    }

    bool IndexStatsCmd::runInternal(string& errmsg, NamespaceDetails* nsd, BSONObjBuilder& result) {

        {
            BSONArrayBuilder indexesArrayBuilder(result.subarrayStart("indexes"));
            for (NamespaceDetails::IndexIterator it = nsd->ii(); it.more(); ) {
                IndexDetails& details = it.next();
                BSONObjBuilder indexBuilder(indexesArrayBuilder.subobjStart());
                indexBuilder << "name" << details.indexName()
                             << "version" << details.version()
                             << "isIdIndex" << details.isIdIndex()
                             << "keyPattern" << details.keyPattern();
                IndexInterface& interface = details.idxInterface();
                interface.fullValidate(details.head, details.keyPattern());
                if (details.version() != 1) {
                    return true;
                }
                const BtreeBucket<V1>* headBucket = details.head.btree<V1>();
                {
                    BSONObjBuilder headBuilder(indexBuilder.subobjStart("head"));
                    inspectBucket(details.head, 0, headBuilder);
                }
            }
        }
        return true;
    }

} // namespace

} // namespace mongo
