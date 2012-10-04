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

    struct IndexStatsParams {
        IndexStatsParams() : dumpTree(false) {
        }

        string indexName;
        bool dumpTree;
    };

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

        bool runInternal(string& errmsg, NamespaceDetails* nsd, IndexStatsParams params,
                         BSONObjBuilder& result);
    } indexStatsCmd;

    struct BtreeStats;

    struct LevelStats {
        unsigned int numBuckets;
        unsigned int totalBsonSize;
        unsigned int totalEmptySize;
        unsigned int totalKeyNodeSize;
        unsigned int totalKeys;
        unsigned int usedKeys;

        LevelStats() : numBuckets(0), totalBsonSize(0), totalEmptySize(0), totalKeyNodeSize(0), totalKeys(0), usedKeys(0) {
        }
        virtual ~LevelStats() {
        }

        void appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const;
    };

    struct BtreeStats : public LevelStats {
        unsigned int bucketBodySize;
        unsigned int depth;
        vector<LevelStats> perLevel;

        BtreeStats() : bucketBodySize(0), depth(0), perLevel(0) {
        }

        void appendTo(BSONObjBuilder& builder) const {
            LevelStats::appendTo(builder, this);
            builder << "bucketBodySize" << bucketBodySize;
            builder << "depth" << depth;
            BSONArrayBuilder perLevelArrayBuilder(builder.subarrayStart("perLevel"));
            for (vector<LevelStats>::const_iterator it = perLevel.begin();
                 it != perLevel.end();
                 ++it) {
                BSONObjBuilder levelBuilder(perLevelArrayBuilder.subobjStart());
                it->appendTo(levelBuilder, this);
            }
            perLevelArrayBuilder.doneFast();
        }
    };

    void LevelStats::appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const {
        builder << "numBuckets" << numBuckets
                << "usedKeys" << usedKeys
                << "totalKeys" << totalKeys
                << "avgUsedKeysPerBucket" << (double) usedKeys / numBuckets
                << "totalBsonSize" << totalBsonSize
                << "avgBsonSize" << (double) totalBsonSize / numBuckets
                << "totalEmptySize" << totalEmptySize
                << "avgEmptySize" << (double) totalEmptySize / numBuckets
                << "totalKeyNodeSize" << totalKeyNodeSize
                << "avgKeyNodeSize" << (double) totalKeyNodeSize / numBuckets
                << "avgBsonRatio" << (double) totalBsonSize / globalStats->bucketBodySize / numBuckets
                << "avgEmptyRatio" << (double) totalEmptySize / globalStats->bucketBodySize / numBuckets
                << "avgKeyNodesRatio" << (double) totalKeyNodeSize / globalStats->bucketBodySize / numBuckets;
    }

    class BtreeInspector {
    public:
        virtual bool inspect(DiskLoc& head, BSONObjBuilder* treeBuilder) = 0;
        virtual ~BtreeInspector() {
        }

        BtreeStats stats;
    };

    template <class Version>
    class BtreeInspectorImpl : public BtreeInspector {
    public:
        typedef typename mongo::BucketBasics<Version> BucketBasics;
        typedef typename mongo::BucketBasics<Version>::_KeyNode _KeyNode;
        typedef typename mongo::BucketBasics<Version>::KeyNode KeyNode;
        typedef typename mongo::BucketBasics<Version>::Key Key;

        BtreeInspectorImpl() {
        }

        virtual bool inspect(DiskLoc& head, BSONObjBuilder* treeBuilder) /*override*/;

    private:
        bool inspectBucket(const DiskLoc& dl, unsigned int depth, BSONObjBuilder* bucketBuilder);
        bool inspectChild(const DiskLoc dl, unsigned int curDepth, BSONArrayBuilder* childsArrayBuilder);
    };

    typedef BtreeInspectorImpl<V0> BtreeInspectorV0;
    typedef BtreeInspectorImpl<V1> BtreeInspectorV1;

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspect(DiskLoc& head, BSONObjBuilder* treeBuilder) {
        stats.bucketBodySize = BucketBasics::bodySize();
        return this->inspectBucket(head, 0, treeBuilder);
    }

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspectChild(const DiskLoc loc, unsigned int curDepth, BSONArrayBuilder* childsArrayBuilder) {
        if ( !loc.isNull() ) {
            if (childsArrayBuilder != NULL) {
                BSONObjBuilder childBucketBuilder(childsArrayBuilder->subobjStart());
                return inspectBucket(loc, curDepth + 1, &childBucketBuilder);
            }
            else {
                return inspectBucket(loc, curDepth + 1, NULL);
            }
        }
        return true;
    }

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspectBucket(const DiskLoc& dl, unsigned int depth, BSONObjBuilder* bucketBuilder) {
        const BtreeBucket<Version>* bucket = dl.btree<Version>();
        int usedKeyCount = 0;
        int totalKeyCount = 0;
        {
            scoped_ptr<BSONArrayBuilder> childsArrayBuilder(NULL);
            if (bucketBuilder != NULL) {
                childsArrayBuilder.reset(new BSONArrayBuilder(bucketBuilder->subarrayStart("childs")));
            }
            for ( int i = 0; i < bucket->getN(); i++ ) {
                const _KeyNode& kn = bucket->k(i);

                if ( kn.isUsed() ) {
                    ++usedKeyCount;
                    this->inspectChild(kn.prevChildBucket, depth, childsArrayBuilder.get());
                }
                ++totalKeyCount;
            }
            this->inspectChild(bucket->getNextChild(), depth, childsArrayBuilder.get());
        }

        if (depth > stats.depth) stats.depth = depth;
        stats.numBuckets += 1;
        stats.totalKeys += totalKeyCount;
        stats.usedKeys += usedKeyCount;
        stats.totalBsonSize += bucket->getBsonSize();
        stats.totalEmptySize += bucket->getEmptySize();
        stats.totalKeyNodeSize += sizeof(_KeyNode) * totalKeyCount;
        while (stats.perLevel.size() < depth + 1)
            stats.perLevel.push_back(LevelStats());
        LevelStats& level = stats.perLevel.at(depth);
        level.numBuckets += 1;
        level.totalKeys += totalKeyCount;
        level.usedKeys += usedKeyCount;
        level.totalBsonSize += bucket->getBsonSize();
        level.totalEmptySize += bucket->getEmptySize();
        level.totalKeyNodeSize += sizeof(_KeyNode) * totalKeyCount;

        if (bucketBuilder != NULL) {
            *bucketBuilder << "n" << bucket->getN()
                           << "depth" << depth
                           << "usedKeys" << usedKeyCount
                           << "totalKeys" << totalKeyCount;
        }

        if (bucketBuilder != NULL) {
            if (bucket->getN() > 0) {
                const KeyNode& firstKeyNode = bucket->keyNode(0);
                const Key& key = firstKeyNode.key;
                bucketBuilder->append("firstKey", key.toBson());
                if (bucket->getN() > 1) {
                    const KeyNode& lastKeyNode =
                            bucket->keyNode(bucket->getN() - 1);
                    const Key& key = lastKeyNode.key;
                    bucketBuilder->append("lastKey", key.toBson());
                }
            }
        }
        return true;
    }

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

        IndexStatsParams params;

        // { name: _index_name }
        BSONElement name = cmdObj["name"];
        if (!name.ok() || name.type() != String) {
            errmsg = "an index name is required, use {name: \"indexname\"}";
            return false;
        }
        params.indexName = name.String();

        // { dumpTree: true/false }
        BSONElement dumpTreeElm = cmdObj["dumpTree"];
        if (dumpTreeElm.ok()) params.dumpTree = dumpTreeElm.trueValue();

        return runInternal(errmsg, nsd, params, result);
    }

    bool IndexStatsCmd::runInternal(string& errmsg, NamespaceDetails* nsd, IndexStatsParams params, BSONObjBuilder& result) {
        {
            IndexDetails* detailsPtr = NULL;
            for (NamespaceDetails::IndexIterator it = nsd->ii(); it.more(); ) {
                IndexDetails& cur = it.next();
                if (cur.indexName() == params.indexName) detailsPtr = &cur;
            }
            if (detailsPtr == NULL) {
                errmsg = "the requested index does not exist";
                return false;
            }
            IndexDetails& details = *detailsPtr;
            // IndexInterface& interface = details.idxInterface();
            result << "name" << details.indexName()
                   << "version" << details.version()
                   << "isIdIndex" << details.isIdIndex()
                   << "keyPattern" << details.keyPattern();
            {
                scoped_ptr<BSONObjBuilder> headBuilder(NULL);
                if (params.dumpTree) {
                    headBuilder.reset(new BSONObjBuilder(result.subobjStart("head")));
                }
                scoped_ptr<BtreeInspector> inspector(NULL);
                switch (details.version()) {
                  case 1:
                    inspector.reset(new BtreeInspectorV1()); break;
                  case 0:
                    inspector.reset(new BtreeInspectorV0()); break;
                  default:
                    errmsg = str::stream() << "index version " << details.version() << " is "
                                           << "not supported";
                    return false;
                }
                inspector->inspect(details.head, headBuilder.get());
                inspector->stats.appendTo(result);
            }
        }
        return true;
    }

} // namespace mongo
