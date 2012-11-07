/** @file storage_details.cpp
 * collection.indexStats({...}) command
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
#include "mongo/db/kill_current_op.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/descriptive_stats.h"
#include "mongo/util/descriptive_stats_bson.h"

namespace mongo {

    /**
     * Holds operation parameters.
     */
    struct IndexStatsParams {
        IndexStatsParams() {
        }

        string indexName;
        vector<int> expandNodes;
    };

    /**
     * Holds information about a single btree bucket (not its subtree).
     */
    struct NodeInfo {
        boost::optional<BSONObj> firstKey;
        boost::optional<BSONObj> lastKey;
        BSONObj diskLoc;
        unsigned int childNum;
        unsigned int keyCount;
        unsigned int usedKeyCount;
        unsigned int depth;
        double fillRatio;
    };

    /**
     * Aggregates and statistics for some part of the tree:
     *     the entire tree, a level or a certain subtree.
     */
    class AreaStats {
    public:
        static const int QUANTILES = 99;

        boost::optional<NodeInfo> nodeInfo;

        unsigned int numBuckets;
        SummaryEstimators<double, QUANTILES> bsonRatio;
        SummaryEstimators<double, QUANTILES> fillRatio;
        SummaryEstimators<double, QUANTILES> keyNodeRatio;
        SummaryEstimators<unsigned int, QUANTILES> keyCount;
        SummaryEstimators<unsigned int, QUANTILES> usedKeyCount;

        AreaStats() : numBuckets(0) {
        }

        virtual ~AreaStats() {
        }

        /**
         * add the provided values as a sample to the computed statistics for this tree / level /
         * subtree
         *
         * @param keyCount number of keys in the bucket
         * @param usedKeyCount number of used (non-empty) keys in the bucket
         * @param bucket current bucket
         * @param keyNodeBytes size (number of bytes) of a KeyNode
         */
        template<class Version>
        void addStats(int keyCount, int usedKeyCount, const BtreeBucket<Version>* bucket,
                      int keyNodeBytes) {
            this->numBuckets += 1;
            this->bsonRatio << double(bucket->getTopSize()) / bucket->bodySize();
            this->keyNodeRatio << double(keyNodeBytes * keyCount) / bucket->bodySize();
            this->fillRatio << (1. - double(bucket->getEmptySize()) / bucket->bodySize());
            this->keyCount << keyCount;
            this->usedKeyCount << usedKeyCount;
        }

        void appendTo(BSONObjBuilder& builder) const {
            if (nodeInfo) {
                BSONObjBuilder nodeInfoBuilder(builder.subobjStart("nodeInfo"));
                nodeInfoBuilder << "childNum" << nodeInfo->childNum
                                << "keyCount" << nodeInfo->keyCount
                                << "usedKeyCount" << nodeInfo->usedKeyCount
                                << "diskLoc" << nodeInfo->diskLoc
                                << "depth" << nodeInfo->depth
                                << "fillRatio" << nodeInfo->fillRatio;
                if (nodeInfo->firstKey) nodeInfoBuilder << "firstKey" << *(nodeInfo->firstKey);
                if (nodeInfo->lastKey) nodeInfoBuilder << "lastKey" << *(nodeInfo->lastKey);
            }

            builder << "numBuckets" << numBuckets
                    << "keyCount" << statisticSummaryToBSONObj(keyCount)
                    << "usedKeyCount" << statisticSummaryToBSONObj(usedKeyCount)
                    << "bsonRatio" << statisticSummaryToBSONObj(bsonRatio)
                    << "keyNodeRatio" << statisticSummaryToBSONObj(keyNodeRatio)
                    << "fillRatio" << statisticSummaryToBSONObj(fillRatio);
        }
    };

    /**
     * Holds statistics and aggregates for the entire tree and its parts.
     */
    class BtreeStats {
    public:
        unsigned int bucketBodyBytes;
        unsigned int depth;
        AreaStats wholeTree;
        vector<AreaStats> perLevel;
        vector<vector<AreaStats> > branch;

        BtreeStats() : bucketBodyBytes(0), depth(0) {
            branch.push_back(vector<AreaStats>(1));
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
            builder << "bucketBodyBytes" << bucketBodyBytes;
            builder << "depth" << depth;

            { // ensure done() is called by invoking destructor when done with the builder
                BSONObjBuilder wholeTreeBuilder(builder.subobjStart("overall"));
                wholeTree.appendTo(wholeTreeBuilder);
                wholeTreeBuilder.doneFast(); //TODO(andrea.lattuada) can be removed when SERVER-7459
                                             //                      is fixed
            }

            { // ensure done() is called by invoking destructor when done with the builder
                BSONArrayBuilder perLevelArrayBuilder(builder.subarrayStart("perLevel"));
                for (vector<AreaStats>::const_iterator it = perLevel.begin();
                     it != perLevel.end();
                     ++it) {
                    BSONObjBuilder levelBuilder(perLevelArrayBuilder.subobjStart());
                    it->appendTo(levelBuilder);
                    levelBuilder.doneFast(); //TODO(andrea.lattuada) can be removed when SERVER-7459
                                             //                      is fixed
                }
                perLevelArrayBuilder.doneFast(); //TODO(andrea.lattuada) can be removed when
                                                 //                      SERVER-7459 is fixed
            }

            if (branch.size() > 1) {
                BSONArrayBuilder expandedNodesArrayBuilder(builder.subarrayStart("expandedNodes"));
                for (unsigned int depth = 0; depth < branch.size(); ++depth) {

                    BSONArrayBuilder childrenArrayBuilder(
                            expandedNodesArrayBuilder.subarrayStart());
                    const vector<AreaStats>& children = branch[depth];
                    for (unsigned int child = 0; child < children.size(); ++child) {
                        BSONObjBuilder childBuilder(childrenArrayBuilder.subobjStart());
                        children[child].appendTo(childBuilder);
                        childBuilder.doneFast(); //TODO(andrea.lattuada) can be removed when
                    }                            //                      SERVER-7459 is fixed
                }
                expandedNodesArrayBuilder.doneFast(); //TODO(andrea.lattuada) can be removed when
            }                                         //                      SERVER-7459 is fixed
        }
    };

    inline double average(unsigned int sum, unsigned int count) {
        return (double) sum / count;
    }

    /**
     * Performs the btree analysis for a generic btree version. After inspect() is called on the
     * tree root, statistics are available through stats().
     *
     * Template-based implementation in BtreeInspectorImpl.
     */
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

    // See BtreeInspector.
    template <class Version>
    class BtreeInspectorImpl : public BtreeInspector {
    public:
        typedef typename mongo::BucketBasics<Version> BucketBasics;
        typedef typename mongo::BucketBasics<Version>::_KeyNode _KeyNode;
        typedef typename mongo::BucketBasics<Version>::KeyNode KeyNode;
        typedef typename mongo::BucketBasics<Version>::Key Key;

        BtreeInspectorImpl(vector<int> expandNodes) : _expandNodes(expandNodes) {
        }

        virtual bool inspect(DiskLoc& head)  {
            _stats.bucketBodyBytes = BucketBasics::bodySize();
            vector<int> expandedAncestors;
            return this->inspectBucket(head, 0, 0, true, expandedAncestors);
        }

        virtual BtreeStats& stats() {
            return _stats;
        }

    private:
        /**
         * Recursively inspect btree buckets.
         * @param dl DiskLoc for the current bucket
         * @param depth depth for the current bucket (root is 0)
         * @param childNum so that the current bucket is the childNum-th child of its parent
         *                 (the right child is numbered as the last left child + 1)
         * @param parentIsExpanded bucket expansion was requested for the parent bucket so the
         *                         statistics and information for this bucket will appear in the
         *                         subtree
         * @param expandedAncestors if the d-th element is k, the k-th child of an expanded parent
         *                          at depth d is expanded
         *                          [0, 4, 1] means that root is expanded, its 4th child is expanded
         *                          and, in turn, the first child of the 4th child of the root is
         *                          expanded
         * @return true on success, false otherwise
         */
        bool inspectBucket(const DiskLoc& dl, unsigned int depth, int childNum,
                           bool parentIsExpanded, vector<int> expandedAncestors) {

            if (dl.isNull()) return true;
            killCurrentOp.checkForInterrupt();

            const BtreeBucket<Version>* bucket = dl.btree<Version>();
            int usedKeyCount = 0; // number of used keys in this bucket

            killCurrentOp.checkForInterrupt();

            int keyCount = bucket->getN();
            int childrenCount = keyCount + 1; // maximum number of children of this bucket
                                              // including the right child

            if (depth > _stats.depth) _stats.depth = depth;

            bool curNodeIsExpanded = false;
            if (parentIsExpanded) {
                // if the parent node is expanded, statistics and info will be outputted for this
                // bucket as well
                expandedAncestors.push_back(childNum);

                    // if the expansion of this node was requested
                if (depth < _expandNodes.size() && _expandNodes[depth] == childNum) {
                    _stats.newBranchLevel(depth, childrenCount);
                    curNodeIsExpanded = true;
                }
            }

            const _KeyNode* firstKeyNode = NULL;
            const _KeyNode* lastKeyNode = NULL;
            for (int i = 0; i < keyCount; i++ ) {
                const _KeyNode& kn = bucket->k(i);

                if ( kn.isUsed() ) {
                    ++usedKeyCount;
                    if (i == 0) {
                        firstKeyNode = &kn;
                    }
                    else if (i == keyCount - 1) {
                        lastKeyNode = &kn;
                    }

                    this->inspectBucket(kn.prevChildBucket, depth + 1, i, curNodeIsExpanded,
                                        expandedAncestors);
                }
            }
            this->inspectBucket(bucket->getNextChild(), depth + 1, keyCount, curNodeIsExpanded,
                                expandedAncestors);

            killCurrentOp.checkForInterrupt();

            if (parentIsExpanded) {
                // stats for the children of this bucket have been added in the recursive calls,
                // avoid including the current bucket in the stats for its subtree
                expandedAncestors.pop_back();
            }


            // add the stats for the current bucket to the aggregates for all its ancestors and
            // the entire tree
            for (unsigned int d = 0; d < expandedAncestors.size(); ++d) {
                AreaStats& nodeStats = _stats.nodeAt(d, expandedAncestors[d]);
                nodeStats.addStats(keyCount, usedKeyCount, bucket, sizeof(_KeyNode));
            }
            _stats.wholeTree.addStats(keyCount, usedKeyCount, bucket, sizeof(_KeyNode));

            if (parentIsExpanded) {
                NodeInfo nodeInfo;
                if (firstKeyNode != NULL) {
                    nodeInfo.firstKey = KeyNode(*bucket, *firstKeyNode).key.toBson();
                }
                if (lastKeyNode != NULL) {
                    nodeInfo.lastKey = KeyNode(*bucket, *lastKeyNode).key.toBson();
                }

                nodeInfo.childNum = childNum;
                nodeInfo.depth = depth;
                nodeInfo.diskLoc = dl.toBSONObj();
                nodeInfo.keyCount = keyCount;
                nodeInfo.usedKeyCount = bucket->getN();
                nodeInfo.fillRatio =
                    (1. - double(bucket->getEmptySize()) / BucketBasics::bodySize());

                _stats.nodeAt(depth, childNum).nodeInfo = nodeInfo;
            }

            // add the stats for this bucket to the aggregate for a certain depth
            while (_stats.perLevel.size() < depth + 1)
                _stats.perLevel.push_back(AreaStats());
            dassert(_stats.perLevel.size() > depth);
            AreaStats& level = _stats.perLevel[depth];
            level.addStats(keyCount, usedKeyCount, bucket, sizeof(_KeyNode));

            return true;
        } 

        vector<int> _expandNodes;
        BtreeStats _stats;
    };

    typedef BtreeInspectorImpl<V0> BtreeInspectorV0;
    typedef BtreeInspectorImpl<V1> BtreeInspectorV1;

    /**
     * Run analysis with the provided parameters. See IndexStatsCmd for in-depth expanation of
     * output.
     *
     * @return true on success, false otherwise
     */
    bool runInternal(const NamespaceDetails* nsd, IndexStatsParams params, string& errmsg,
                     BSONObjBuilder& result) {

        IndexDetails* detailsPtr = NULL;

        // casting away const, we are not going to modify NamespaceDetails
        // but ii() is not marked const
        for (NamespaceDetails::IndexIterator it = const_cast<NamespaceDetails*>(nsd)->ii();
             it.more();) {
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

    // Command

    /**
     * This command provides detailed and aggreate information and statistics for a btree. 
     * Stats are aggregated for the entire tree, per-depth and, if requested through the expandNodes
     * option, per-subtree.
     * The entire btree is walked depth-first on every call. This command takes a read lock and may
     * be slow for large indexes if the underlying extents arent't already in physical memory.
     *
     * The output has the form:
     *     { name: <index name>,
     *       version: <index version (0 or 1),
     *       isIdKey: <true if this is the default _id index>,
     *       keyPattern: <bson object describing the key pattern>,
     *       storageNs: <namespace of the index's underlying storage>,
     *       bucketBodyBytes: <bytes available for keynodes and bson objects in the bucket's body>,
     *       depth: <index depth (root excluded)>
     *       overall: { (statistics for the entire tree)
     *           numBuckets: <number of buckets (samples)>
     *           keyCount: { (stats about the number of keys in a bucket)
     *               count: <number of samples>,
     *               mean: <mean>
     *    (optional) stddev: <standard deviation>
     *    (optional) min: <minimum value (number of keys for the bucket that has the least)>
     *    (optional) max: <maximum value (number of keys for the bucket that has the most)>
     *    (optional) quantiles: {
     *                   0.01: <1st percentile>, 0.02: ..., 0.09: ..., 0.25: <1st quartile>,
     *                   0.5: <median>, 0.75: <3rd quartile>, 0.91: ..., 0.98: ..., 0.99: ...
     *               }
     *    (optional fields are only present if there are enough samples to compute sensible
     *     estimates)
     *           }
     *           usedKeyCount: <stats about the number of used keys in a bucket>
     *               (same structure as keyCount)
     *           bsonRatio: <stats about how much of the bucket body is occupied by bson objects>
     *               (same structure as keyCount)
     *           keyNodeRatio: <stats about how much of the bucket body is occupied by KeyNodes>
     *               (same structure as keyCount)
     *           fillRatio: <stats about how full is the bucket body (bson objects + KeyNodes)>
     *               (same structure as keyCount)
     *       },
     *       perLevel: [ (statistics aggregated per depth)
     *           (one element with the same structure as 'overall' for each btree level,
     *            the first refers to the root)
     *       ]
     *     }
     *
     * If 'expandNodes: [array]' was specified in the parameters, an additional field named
     * 'expandedNodes' is included in the output. It contains two nested arrays, such that the
     * n-th element of the outer array contains stats for nodes at depth n (root is included) and
     * the i-th element (0-based) of the inner array at depth n contains stats for the subtree
     * rooted at the i-th child of the expanend node at depth (n - 1).
     * Each element of the inner array has the same structure as 'overall' in the description above:
     * it includes the aggregate stats for all the nodes in the subtree excluding the current
     * bucket.
     * It also contains an additional field 'nodeInfo' representing information for the current
     * node:
     *     { childNum: <i so that this is the (i + 1)-th child of the parent node>
     *       keyCount: <number of keys in this bucket>
     *       usedKeyCount: <number of non-empty KeyNodes>
     *       diskLoc: { (bson representation of the disk location for this bucket)
     *           file: <num>
     *           offset: <bytes>
     *       }
     *       depth: <depth of this bucket, root is at depth 0>
     *       fillRatio: <a value between 0 and 1 representing how full this bucket is>
     *       firstKey: <bson object containing the value for the first key>
     *       lastKey: <bson object containing the value for the last key>
     *     }
     *
     */
    class IndexStatsCmd : public Command {
    public:
        IndexStatsCmd() : Command("indexStats") {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "TODO. Slow."; }

        virtual LockType locktype() const { return READ; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
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

            BSONElement expandNodes = cmdObj["expandNodes"];
            if (expandNodes.ok()) {
                if (expandNodes.type() != mongo::Array) {
                    errmsg = "expandNodes must be an array of numbers";
                    return false;
                }
                vector<BSONElement> arr = expandNodes.Array();
                for (vector<BSONElement>::const_iterator it = arr.begin(); it != arr.end(); ++it) {
                    if (!it->isNumber()) {
                        errmsg = "expandNodes must be an array of numbers";
                        return false;
                    }
                    params.expandNodes.push_back(int(it->Number()));
                }
            }

            bool success = false;
            BSONObjBuilder resultBuilder;
            try {
                success = runInternal(nsd, params, errmsg, resultBuilder);
            } catch (DBException& e) {
                errmsg = str::stream() << "unexpected error: code " << e.getCode();
                return false;
            }
            if (!success) return false;
            result.appendElements(resultBuilder.obj());
            return true;
        }

    } indexStatsCmd;

} // namespace mongo
