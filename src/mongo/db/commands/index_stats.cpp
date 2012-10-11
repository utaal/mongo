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
#include <string>
#include <vector>

#include <boost/optional.hpp>

#include "mongo/base/disallow_copying.h"
#include "mongo/db/btree.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    struct IndexStatsParams {
        IndexStatsParams() : analyzeStorage(false) {
        }

        string indexName;
        bool analyzeStorage;
        vector<int> expandNodes;
    };

    class IndexStatsCmd : public Command {
    public:
        IndexStatsCmd() : Command("indexStats") {}

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
        unsigned int totalBsonBytes;
        unsigned int totalEmptyBytes;
        unsigned int totalKeyNodeBytes;
        unsigned int totalKeys;
        unsigned int usedKeys;
        boost::optional<BSONObj> firstKey;
        boost::optional<BSONObj> lastKey;
        boost::optional<BSONObj> diskLoc;

        AreaStats() : numBuckets(0), totalBsonBytes(0), totalEmptyBytes(0), totalKeyNodeBytes(0),
                      totalKeys(0), usedKeys(0) {
        }
        virtual ~AreaStats() {
        }

        void appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const;
    };

    struct BtreeStats {
        unsigned int bucketBodyBytes;
        unsigned int depth;
        vector<AreaStats> perLevel;
        vector<vector<AreaStats> > branch;

        BtreeStats() : bucketBodyBytes(0), depth(0) {
            branch.push_back(vector<AreaStats>(1));
        }

        AreaStats& root() {
            dassert(branch.size() > 0); dassert(branch[0].size() > 0);
            return branch[0][0];
        }

        const AreaStats& root() const {
            dassert(branch.size() > 0); dassert(branch[0].size() > 0);
            return branch[0][0];
        }

        AreaStats& nodeAt(unsigned int depth_, unsigned int childNum) {
            dassert(branch.size() > depth_); dassert(branch[depth_].size() > childNum);
            return branch[depth_][childNum];
        }

        void newBranchLevel(unsigned int depth_, unsigned int childrenCount) {
            dassert(branch.size() == depth_ + 1);
            branch.push_back(vector<AreaStats>(childrenCount));
        }

        void appendTo(BSONObjBuilder& builder) const {
            {
                BSONObjBuilder rootBuilder(builder.subobjStart("root"));
                root().appendTo(rootBuilder, this);
            }
            builder << "bucketBodyBytes" << bucketBodyBytes;
            builder << "depth" << depth;
            {
                BSONArrayBuilder perLevelArrayBuilder(builder.subarrayStart("perLevel"));
                for (vector<AreaStats>::const_iterator it = perLevel.begin();
                     it != perLevel.end();
                     ++it) {
                    BSONObjBuilder levelBuilder(perLevelArrayBuilder.subobjStart());
                    it->appendTo(levelBuilder, this);
                }
            }
            if (branch.size() > 1) {
                BSONArrayBuilder expandedNodesArrayBuilder(builder.subarrayStart("expandedNodes"));
                for (unsigned int depth = 1; depth < branch.size(); ++depth) {

                    BSONArrayBuilder childrenArrayBuilder(
                            expandedNodesArrayBuilder.subarrayStart());
                    const vector<AreaStats>& children = branch[depth];
                    for (unsigned int child = 1; child < children.size(); ++child) {
                        BSONObjBuilder childBuilder(childrenArrayBuilder.subobjStart());
                        children[child].appendTo(childBuilder, this);
                    }
                }
            }
        }
    };

    inline double average(unsigned int sum, unsigned int count) {
        return (double) sum / count;
    }

    class StatsCalc {
    public:
        StatsCalc(unsigned int numBuckets, unsigned int bucketBodyBytes) :
            _numBuckets(numBuckets), _bucketBodyBytes(bucketBodyBytes) {
        }

        inline double avg(unsigned int val) {
            return (double) val / _numBuckets;
        }

        inline double ratio(unsigned int size) {
            return (double) size / _bucketBodyBytes / _numBuckets;
        }

    private:
        unsigned int _numBuckets;
        unsigned int _bucketBodyBytes;
    };

    void AreaStats::appendTo(BSONObjBuilder& builder, const BtreeStats* globalStats) const {
        StatsCalc calc(numBuckets, globalStats->bucketBodyBytes);
        builder << "numBuckets" << numBuckets
                << "usedKeys" << usedKeys
                << "totalKeys" << totalKeys
                << "avgUsedKeysPerBucket" << calc.avg(usedKeys)
                << "totalBsonBytes" << totalBsonBytes
                << "avgBsonBytes" << calc.avg(totalBsonBytes)
                << "totalEmptyBytes" << totalEmptyBytes
                << "avgEmptyBytes" << calc.avg(totalEmptyBytes)
                << "totalKeyNodeBytes" << totalKeyNodeBytes
                << "avgKeyNodeBytes" << calc.avg(totalKeyNodeBytes)
                << "bsonRatio" << calc.ratio(totalBsonBytes)
                << "emptyRatio" << calc.ratio(totalEmptyBytes)
                << "keyNodesRatio" << calc.ratio(totalKeyNodeBytes);
        if (firstKey) builder << "firstKey" << *firstKey;
        if (lastKey) builder << "lastKey" << *lastKey;
        if (diskLoc) builder << "diskLoc" << *diskLoc;
    }

    class BtreeInspector {
    public:
        BtreeInspector() {
        }

        virtual ~BtreeInspector() {
        }

        virtual bool inspect(DiskLoc& head) = 0;
        virtual BtreeStats& stats() = 0;

        MONGO_DISALLOW_COPYING(BtreeInspector);
    };

    template <class Version>
    class BtreeInspectorImpl : public BtreeInspector {
    public:
        typedef typename mongo::BucketBasics<Version> BucketBasics;
        typedef typename mongo::BucketBasics<Version>::_KeyNode _KeyNode;
        typedef typename mongo::BucketBasics<Version>::KeyNode KeyNode;
        typedef typename mongo::BucketBasics<Version>::Key Key;

        BtreeInspectorImpl(vector<int> expandNodes) : _expandNodes(expandNodes) {
        }

        virtual bool inspect(DiskLoc& head) /*override*/;

        virtual BtreeStats& stats() {
            return _stats;
        }

    private:
        bool inspectBucket(const DiskLoc& dl, unsigned int depth, int childNum,
                           bool parentIsExpanded, vector<int> expandedAncestors);

        vector<int> _expandNodes;
        BtreeStats _stats;
    };

    typedef BtreeInspectorImpl<V0> BtreeInspectorV0;
    typedef BtreeInspectorImpl<V1> BtreeInspectorV1;

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspect(DiskLoc& head) {
        _stats.bucketBodyBytes = BucketBasics::bodySize();
        vector<int> expandedAncestors;
        return this->inspectBucket(head, 0, 0, true, expandedAncestors);
    }

    template <class Version>
    void addTo(AreaStats& stats, int totalKeyCount, int usedKeyCount,
               const BtreeBucket<Version>* bucket, int keyNodeBytes) {
        stats.numBuckets += 1;
        stats.totalKeys += totalKeyCount;
        stats.usedKeys += usedKeyCount;
        stats.totalBsonBytes += bucket->getBsonSize();
        stats.totalEmptyBytes += bucket->getEmptySize();
        stats.totalKeyNodeBytes += keyNodeBytes * totalKeyCount;
    }

    template <class Version>
    bool BtreeInspectorImpl<Version>::inspectBucket(const DiskLoc& dl, unsigned int depth,
                                                    int childNum, bool parentIsExpanded,
                                                    vector<int> expandedAncestors) {
        if (dl.isNull()) return true;

        const BtreeBucket<Version>* bucket = dl.btree<Version>();
        int usedKeyCount = 0;

        killCurrentOp.checkForInterrupt();

        int keyCount = bucket->getN();
        int childrenCount = keyCount + 1;

        bool curNodeIsExpanded = false;
        if (parentIsExpanded) {
            expandedAncestors.push_back(childNum);
            if (depth < _expandNodes.size() && _expandNodes[depth] == childNum) {
                _stats.newBranchLevel(depth, childrenCount);
                curNodeIsExpanded = true;
            }

            AreaStats& curNodeStats = _stats.nodeAt(depth, childNum);
            // TODO(andrea.lattuada) make sure key node is used (non-empty)
            if (bucket->getN() > 0)
                curNodeStats.firstKey = bucket->keyAt(0).toBson();
            if (bucket->getN() > 1)
                curNodeStats.lastKey = bucket->keyAt(bucket->getN() - 1).toBson();
            curNodeStats.diskLoc = dl.toBSONObj();
        }

        for (int i = 0; i < keyCount; i++ ) {
            const _KeyNode& kn = bucket->k(i);

            if ( kn.isUsed() ) {
                ++usedKeyCount;
                this->inspectBucket(kn.prevChildBucket, depth + 1, i, curNodeIsExpanded,
                                    expandedAncestors);
            }
        }
        this->inspectBucket(bucket->getNextChild(), depth + 1, keyCount, curNodeIsExpanded,
                            expandedAncestors);

        if (depth > _stats.depth) _stats.depth = depth;
        for (unsigned int d = 0; d < expandedAncestors.size(); ++d) {
            AreaStats& nodeStats = _stats.nodeAt(d, expandedAncestors.at(d));
            addTo(nodeStats, keyCount, usedKeyCount, bucket, sizeof(_KeyNode));
        }
        while (_stats.perLevel.size() < depth + 1)
            _stats.perLevel.push_back(AreaStats());
        dassert(_stats.perLevel.size() > depth);
        AreaStats& level = _stats.perLevel[depth];
        addTo(level, keyCount, usedKeyCount, bucket, sizeof(_KeyNode));

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

        params.analyzeStorage = cmdObj["analyzeStorage"].trueValue();

        BSONElement expandNodes = cmdObj["expandNodes"];
        if (expandNodes.ok()) {
            vector<BSONElement> arr = expandNodes.Array();
            for (vector<BSONElement>::const_iterator it = arr.begin(); it != arr.end(); ++it) {
                params.expandNodes.push_back(int(it->Number()));
            }
        }

        bool success = false;
        BSONObjBuilder resultBuilder;
        success = runInternal(errmsg, nsd, params, resultBuilder);
        if (!success) return false;
        result.appendElements(resultBuilder.obj());
        return true;
    }

    bool IndexStatsCmd::runInternal(string& errmsg, NamespaceDetails* nsd, IndexStatsParams params,
                                    BSONObjBuilder& result) {
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
