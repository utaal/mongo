/** @file storage_details.cpp
 * collection.storageDetails({...}) command
 */

/*    Copyright 2012 10gen Inc.
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
#include <list>
#include <string>

#include "mongo/base/disallow_copying.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/btree.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    struct IndexStatsParams {
        IndexStatsParams() : dumpTree(false), analyzeStorage(false) {
        }

        string indexName;
        bool dumpTree;
        bool analyzeStorage;
        vector<int> expandNodes;
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

    struct AreaStats {
        unsigned int numBuckets;
        unsigned int totalBsonSize;
        unsigned int totalEmptySize;
        unsigned int totalKeyNodeSize;
        unsigned int totalKeys;
        unsigned int usedKeys;

        AreaStats() : numBuckets(0), totalBsonSize(0), totalEmptySize(0), totalKeyNodeSize(0), totalKeys(0), usedKeys(0) {
        }
        virtual ~AreaStats() {
        }

        void appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const;
    };

    struct BtreeStats {
        unsigned int bucketBodySize;
        unsigned int depth;
        vector<AreaStats> perLevel;
        vector<vector<AreaStats> > branch;

        BtreeStats() : bucketBodySize(0), depth(0) {
            branch.push_back(vector<AreaStats>(1));
        }

        AreaStats& root() {
            return branch.at(0).at(0);
        }

        const AreaStats& root() const {
            return branch.at(0).at(0);
        }

        vector<AreaStats>& branchAtDepth(int depth) {
            return branch.at(depth + 1);
        }

        vector<AreaStats>& newBranchLevel(int depth, int childrenCount) {
            verify(branch.size() == depth + 1);
            branch.push_back(vector<AreaStats>(childrenCount));
            return branchAtDepth(depth + 1);
        }

        void appendTo(BSONObjBuilder& builder) const {
            root().appendTo(builder, this);
            builder << "bucketBodySize" << bucketBodySize;
            builder << "depth" << depth;
            BSONArrayBuilder perLevelArrayBuilder(builder.subarrayStart("perLevel"));
            for (vector<AreaStats>::const_iterator it = perLevel.begin();
                 it != perLevel.end();
                 ++it) {
                BSONObjBuilder levelBuilder(perLevelArrayBuilder.subobjStart());
                it->appendTo(levelBuilder, this);
            }
            perLevelArrayBuilder.doneFast();
        }
    };

    inline double average(unsigned int sum, unsigned int count) {
        return (double) sum / count;
    }

    class StatsCalc {
    public:
        StatsCalc(unsigned int numBuckets, unsigned int bucketBodySize) :
            _numBuckets(numBuckets), _bucketBodySize(bucketBodySize) {
        }

        inline double avg(unsigned int val) {
            return (double) val / _numBuckets;
        }

        inline double ratio(unsigned int size) {
            return (double) size / _bucketBodySize / _numBuckets;
        }

    private:
        unsigned int _numBuckets;
        unsigned int _bucketBodySize;
    };

    void AreaStats::appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const {
        StatsCalc calc(numBuckets, globalStats->bucketBodySize);
        builder << "numBuckets" << numBuckets
                << "usedKeys" << usedKeys
                << "totalKeys" << totalKeys
                << "avgUsedKeysPerBucket" << calc.avg(usedKeys)
                << "totalBsonSize" << totalBsonSize
                << "avgBsonSize" << calc.avg(totalBsonSize)
                << "totalEmptySize" << totalEmptySize
                << "avgEmptySize" << calc.avg(totalEmptySize)
                << "totalKeyNodeSize" << totalKeyNodeSize
                << "avgKeyNodeSize" << calc.avg(totalKeyNodeSize)
                << "bsonRatio" << calc.ratio(totalBsonSize)
                << "emptyRatio" << calc.ratio(totalEmptySize)
                << "keyNodesRatio" << calc.ratio(totalKeyNodeSize);
    }

    class BtreeInspector {
    public:
        BtreeInspector(/*int numExtents*/) /*: extentStats(numExtents)*/ {
        }

        virtual ~BtreeInspector() {
        }

        virtual bool inspect(DiskLoc& head) = 0;
        virtual BtreeStats& stats() = 0;

        //vector<BtreeStats> extentStats;
        MONGO_DISALLOW_COPYING(BtreeInspector);
    };

    template <class Version>
    class BtreeInspectorImpl : public BtreeInspector {
    public:
        typedef typename mongo::BucketBasics<Version> BucketBasics;
        typedef typename mongo::BucketBasics<Version>::_KeyNode _KeyNode;
        typedef typename mongo::BucketBasics<Version>::KeyNode KeyNode;
        typedef typename mongo::BucketBasics<Version>::Key Key;

        BtreeInspectorImpl(vector<int> expandNodes/*int numExtents*/) : _expandNodes(expandNodes) /*: BtreeInspector(numExtents)*/ {
        }

        virtual bool inspect(DiskLoc& head) /*override*/;

        virtual BtreeStats& stats() {
            return _stats;
        }

    private:
        bool inspectBucket(const DiskLoc& dl, int depth, int childNum, bool expandedBranch, list<AreaStats*> branchStats);

        vector<int> _expandNodes;
        BtreeStats _stats;
    };

    typedef BtreeInspectorImpl<V0> BtreeInspectorV0;
    typedef BtreeInspectorImpl<V1> BtreeInspectorV1;

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspect(DiskLoc& head) {
        _stats.bucketBodySize = BucketBasics::bodySize();
        list<AreaStats*> branchStats;
        branchStats.push_back(&_stats.root());
        return this->inspectBucket(head, 0, 0, true, branchStats);
    }

    template <class Version>
    void addTo(AreaStats* stats, int totalKeyCount, int usedKeyCount, const BtreeBucket<Version>* bucket, int keyNodeSize) {
        stats->numBuckets += 1;
        stats->totalKeys += totalKeyCount;
        stats->usedKeys += usedKeyCount;
        stats->totalBsonSize += bucket->getBsonSize();
        stats->totalEmptySize += bucket->getEmptySize();
        stats->totalKeyNodeSize += keyNodeSize * totalKeyCount;
    }

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspectBucket(const DiskLoc& dl, int depth, int childNum, bool expandedBranch, list<AreaStats*> branchStats) {
        if (dl.isNull()) return true;

        const BtreeBucket<Version>* bucket = dl.btree<Version>();
        PRINT("+++");
        int usedKeyCount = 0;

        killCurrentOp.checkForInterrupt();

        int keyCount = bucket->getN();
        int childrenCount = keyCount + 1;

        if (expandedBranch) {
            if (depth < _expandNodes.size() && _expandNodes[depth] == childNum) {
                _stats.newBranchLevel(depth, childrenCount);
            } else {
                expandedBranch = false;
            }
        }

        for ( int i = 0; i < keyCount; i++ ) {
            const _KeyNode& kn = bucket->k(i);

            if ( kn.isUsed() ) {
                ++usedKeyCount;
                this->inspectBucket(kn.prevChildBucket, depth + 1, i, expandedBranch, branchStats);
            }
        }
        this->inspectBucket(bucket->getNextChild(), depth + 1, keyCount, expandedBranch, branchStats);

        if (depth > _stats.depth) _stats.depth = depth;
        for (list<AreaStats*>::iterator it = branchStats.begin(); it != branchStats.end(); ++it) {
            addTo(*it, keyCount, usedKeyCount, bucket, sizeof(_KeyNode));
        }
        while (_stats.perLevel.size() < depth + 1)
            _stats.perLevel.push_back(AreaStats());
        AreaStats& level = _stats.perLevel.at(depth);
        addTo(&level, keyCount, usedKeyCount, bucket, sizeof(_KeyNode));

        // if (bucketBuilder != NULL) {
        //     *bucketBuilder << "n" << bucket->getN()
        //                    << "depth" << depth
        //                    << "usedKeys" << usedKeyCount
        //                    << "totalKeys" << totalKeyCount
        //                    << "diskLoc" << dl.toBSONObj();
        // }

        // if (bucketBuilder != NULL) {
        //     if (bucket->getN() > 0) {
        //         const KeyNode& firstKeyNode = bucket->keyNode(0);
        //         const Key& key = firstKeyNode.key;
        //         bucketBuilder->append("firstKey", key.toBson());
        //         if (bucket->getN() > 1) {
        //             const KeyNode& lastKeyNode =
        //                     bucket->keyNode(bucket->getN() - 1);
        //             const Key& key = lastKeyNode.key;
        //             bucketBuilder->append("lastKey", key.toBson());
        //         }
        //     }
        // }
        return true;
    }

    bool IndexStatsCmd::run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {

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

        BSONElement analyzeStorageElm = cmdObj["analyzeStorage"];
        if (analyzeStorageElm.ok()) params.analyzeStorage = analyzeStorageElm.trueValue();

        BSONElement expandNodes = cmdObj["expandNodes"];
        if (expandNodes.ok()) {
            vector<BSONElement> arr = expandNodes.Array();
            for (vector<BSONElement>::const_iterator it = arr.begin(); it != arr.end(); ++it) {
                int el = int(it->Number());
                params.expandNodes.push_back(el);
            }
        }

        return runInternal(errmsg, nsd, params, result);
    }

    bool IndexStatsCmd::runInternal(string& errmsg, NamespaceDetails* nsd, IndexStatsParams params, BSONObjBuilder& result) {
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
               << "keyPattern" << details.keyPattern()
               << "storageNs" << details.indexNamespace();

        if (params.analyzeStorage) {
            NamespaceDetails* indexNsd = nsdetails(details.indexNamespace().c_str());
            BSONArrayBuilder extentsArrayBuilder(result.subarrayStart("extents"));
            unsigned int totRecordCount = 0;
            unsigned int totRecLen = 0;
            unsigned int totExtentSpace = 0;
            for (Extent* ex = DataFileMgr::getExtent(indexNsd->firstExtent);
                 ex != NULL; ex = ex->getNextExtent()) {

                killCurrentOp.checkForInterrupt();

                BSONObjBuilder extentBuilder(extentsArrayBuilder.subobjStart());
                extentBuilder << "diskLoc" << ex->myLoc.toBSONObj()
                              << "length" << ex->length;

                unsigned int recordCount = 0;
                unsigned int recLen = 0;
                Record* r;
                for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
                    killCurrentOp.checkForInterrupt();
                    r = dl.rec();

                    recLen += r->lengthWithHeaders();
                    totRecLen += r->lengthWithHeaders();
                    ++recordCount;
                    ++totRecordCount;
                }
                totExtentSpace += (ex->length - Extent::HeaderSize());

                extentBuilder << "entries" << recordCount
                              << "recLen" << recLen
                              << "usage" << (double) recLen / (ex->length - Extent::HeaderSize());
            }
            extentsArrayBuilder.doneFast();
            result << "numRecords" << totRecordCount
                   << "overallStorageUsage" << (double) totRecLen / totExtentSpace;
        }

        scoped_ptr<BtreeInspector> inspector(NULL);
        switch (details.version()) {
          case 1:
            inspector.reset(new BtreeInspectorV1(params.expandNodes)); break;
          case 0:
            inspector.reset(new BtreeInspectorV0(params.expandNodes)); break;
          default:
            errmsg = str::stream() << "index version " << details.version() << " is "
                                   << "not supported";
            return false;
        }
        inspector->inspect(details.head);
        inspector->stats().appendTo(result);

        return true;
    }

} // namespace mongo
