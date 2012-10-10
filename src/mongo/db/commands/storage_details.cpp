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

#include <ctime>
#include <fstream>
#include <iostream>
#include <list>
#include <string>

#include <boost/array.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

//TODO(andrea.lattuada) cleanup imports
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/tools/tool.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {

    static size_t PAGE_SIZE = 4 << 10;

    // Helper classes

    /**
     * Available subcommands.
     */
    enum SubCommand {
        SUBCMD_DISK_STORAGE,
        SUBCMD_MEM_IN_CORE
    };

    /**
     * Simple struct to store various operation parameters to be passed around during analysis.
     */
    struct AnalyzeParams {
        // startOfs and endOfs are extent-relative
        int startOfs;
        int endOfs;
        int length;
        int numberOfChunks;
        int granularity;
        int lastChunkLength;
        string charactField;
        bool charactFieldIsObjId;
        bool showRecords;
        time_t startTime;

        AnalyzeParams() : startOfs(0), endOfs(INT_MAX), length(INT_MAX), numberOfChunks(0),
                          granularity(0), lastChunkLength(0), charactField("_id"),
                          charactFieldIsObjId(true), showRecords(false), startTime(time(NULL)) {
        }
    };

    /**
     * Aggregated information per chunk / extent.
     */
    struct DiskStorageData {
        long double numEntries;
        long long bsonBytes;
        long long recBytes;
        long long onDiskBytes;
        double charactSum;
        long double charactCount;
        vector<double> freeRecords;

        DiskStorageData(long long diskBytes) : numEntries(0), bsonBytes(0), recBytes(0),
                                               onDiskBytes(diskBytes), charactSum(0),
                                               charactCount(0), freeRecords(mongo::Buckets, 0) {
        }

        const DiskStorageData& operator += (const DiskStorageData& rhs);

        void appendToBSONObjBuilder(BSONObjBuilder& b, bool includeFreeRecords) const;
    };

    /**
     * Helper to calculate which chunks the current record overlaps and how much of the record
     * is in each of them.
     * E.g.
     *                 3.5M      4M     4.5M      5M      5.5M       6M
     *     chunks ->    |   12   |   13   |   14   |   15   |   16   |
     *     record ->         [-------- 1.35M --------]
     *
     * results in something like:
     *     firstChunkNum = 12
     *     lastChunkNum = 15
     *     sizeInFirstChunk = 0.25M
     *     sizeInLastChunk = 0.10M
     *     sizeInMiddleChunk = 0.5M (== size of chunk)
     *     inFirstChunkRatio = 0.25M / 1.35M = 0.185...
     *     inLastChunkRatio = 0.10M / 1.35M = 0.074...
     *     inMiddleChunkRatio = 0.5M / 1.35M = 0.37...
     *
     * The quasi-iterator ChunkIterator is available to easily iterate over the chunks spanned
     * by the record and to obtain how much of the records belongs to each.
     *
     *    for (RecPos::ChunkIterator it = pos.iterateChunks(); !it.end(); ++it) {
     *        RecPos::ChunkInfo res = *it;
     *        // res contains the current chunk number, the number of bytes belonging to the current
     *        // chunk, and the ratio with the full size of the record
     *    }
     *
     */
    struct RecPos {
        bool outOfRange;
        int firstChunkNum;
        int lastChunkNum;
        int sizeInFirstChunk;
        int sizeInLastChunk;
        int sizeInMiddleChunk;
        double inFirstChunkRatio;
        double inLastChunkRatio;
        double inMiddleChunkRatio;
        int numberOfChunks;

        /**
         * Calculate position of record among chunks.
         * @param recOfs record offset as reported by DiskLoc
         * @param recLen record on-disk size with headers included
         * @param extentOfs extent offset as reported by DiskLoc
         * @param params operation parameters (see AnalyzeParams for details)
         */
        static RecPos from(int recOfs, int recLen, int extentOfs,
                                         const AnalyzeParams& params);

        // See RecPos class description
        struct ChunkInfo {
            int chunkNum;
            int sizeHere;
            double ratioHere;
        };

        /**
         * Iterates over chunks spanned by the record.
         */
        class ChunkIterator {
        public:
            ChunkIterator(RecPos& pos) : _pos(pos), _fresh(false) {
                _curChunk.chunkNum = pos.firstChunkNum >= 0 ? _pos.firstChunkNum : 0;
            }

            bool end() const;

            ChunkInfo* operator->();

            // preincrement
            ChunkIterator& operator++();

        private:
            RecPos& _pos;
            ChunkInfo _curChunk;

            // if _fresh, data in _curChunk refers to the current chunk, otherwise it needs
            // to be computed
            bool _fresh;
        };

        ChunkIterator iterateChunks();
    };

    inline unsigned ceilingDiv(unsigned dividend, unsigned divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    // Command

    /**
     * This command provides detailed and aggreate information regarding record and deleted record
     * layout in storage files and in memory.
     */
    class StorageDetailsCmd : public Command {
    public:
        StorageDetailsCmd() : Command( "storageDetails" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const {
            h << "Provides detailed and aggreate information regarding record and deleted record "
              << "layout in storage files and in memory. Slow if run with {allExtents: true}.";
        }

        virtual LockType locktype() const { return READ; }

    private:
        /**
         * @return the requested extent if it exists, otherwise NULL
         */
        static const Extent* getNthExtent(int extentNum, const NamespaceDetails* nsd);

        /**
         * Provides aggregate and (if requested) detailed information regarding the layout of
         * records and deleted records in the extent.
         * The extent is split in params.numberOfChunks chunks of params.granularity bytes each
         * (except the last one which could be shorter).
         * Iteration is performed over all records and deleted records in the specified (part of)
         * extent and the output contains aggregate information for the entire record and per-chunk.
         * The typical output has the form:
         *
         *     { extentHeaderBytes: <size>,
         *       recordHeaderBytes: <size>,
         *       range: [startOfs, endOfs],     // extent-relative
         *       numEntries: <number of records>,
         *       bsonBytes: <total size of the bson objects>,
         *       recBytes: <total size of the valid records>,
         *       onDiskBytes: <length of the extent or range>,
         * (opt) charactCount: <number of records containing the field used to characterize them>
         * (opt) charactSum: <sum of the values of the characteristic field>
         *       charactAvg: <average value of the characteristic field>
         *       freeRecsPerBucket: [ ... ],
         * The nth element in the freeRecsPerBucket array is the count of deleted records in the
         * nth bucket of the deletedList.
         * The characteristic field is specified in params.charactField and may or may not be
         * a standard object id (params.charactFieldIsObjId).
         *
         * The list of chunks follows, with similar information aggregated per-chunk:
         *       chunks: [
         *           { numEntries: <number of records>,
         *             ...
         *             freeRecsPerBucket: [ ... ]
         *           },
         *           ...
         *       ]
         *     }
         *
         * If params.showRecords is set two additional fields are added to the outer document:
         *       records: [
         *           { ofs: <record offset from start of extent>,
         *             recBytes: <record size>,
         *             bsonBytes: <bson document size>,
         *  (optional) charact: <value of the characteristic field>
         *           }, 
         *           ... (one element per record)
         *       ],
         *       deletedRecords: [
         *           { ofs: <offset from start of extent>,
         *             recBytes: <deleted record size>
         *           },
         *           ... (one element per deleted record)
         *       ]
         *
         * @return true on success, false on failure (partial output may still be present)
         */
        static bool analyzeDiskStorage(const NamespaceDetails* nsd, const Extent* ex,
                                       const AnalyzeParams& params, string& errmsg,
                                       BSONObjBuilder& result);

        /**
         * Outputs which percentage of pages are in memory for the entire extent and per-chunk.
         * Refer to analyzeDiskStorage for a description of what chunks are.
         *
         * The output has the form:
         *     { pageBytes: <system page size>,
         *       inMem: <ratio of pages in memory for the entire extent>,
         *       chunks: [ ... ]
         *     }
         *
         * The nth element in the chunks array is the ratio of pages in memory for the nth chunk.
         *
         * @return true on success, false on failure (partial output may still be present)
         */
        static bool analyzeMemInCore(const Extent* ex, const AnalyzeParams& params,
                                     string& errmsg, BSONObjBuilder& result);

        /**
         * Extracts the characteristic field from the document, if present and of the right type.
         * If charactFieldIsObjId is true the field has to have type ObjectId, otherwise it must be
         * any kind of numeric type.
         * @param obj the document
         * @param charactField dotted path to the characteristic field inside the document
         * @param charactFieldIsObjId if true, the charact. field is assumed to be a standard
         *                            object id
         * @param value out: characteristic field value, only valid if true is returned
         * @return true if field was correctly extracted, false otherwise (missing or of wrong type)
         */
        static bool extractCharactFieldValue(BSONObj& obj, const AnalyzeParams& params,
                                             double& value);

        /**
         * analyzeDiskStorage helper which processes a single record.
         */
        static void processRecord(const DiskLoc& dl, const Record* r, int extentOfs,
                                  const AnalyzeParams& params,
                                  vector<DiskStorageData>& chunkData,
                                  BSONArrayBuilder* recordsArrayBuilder);

        /**
         * analyzeDiskStorage helper which processes a single record.
         */
        static void processDeletedRecord(const DiskLoc& dl, const DeletedRecord* dr,
                                         const Extent* ex, const AnalyzeParams& params,
                                         int bucketNum, vector<DiskStorageData>& chunkData,
                                         BSONArrayBuilder* deletedRecordsArrayBuilder);

        /**
         * Entry point, parses command parameters and invokes runInternal.
         */
        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl);

        /**
         * @param params analysis parameters, will be updated with computed number of chunks or
         *               granularity
         */
        bool runInternal(const NamespaceDetails* nsd, const Extent* ex, SubCommand subCommand,
                         AnalyzeParams& params, string& errmsg, BSONObjBuilder& result);

    } storageDetailsCmd;

    const DiskStorageData& DiskStorageData::operator+= (const DiskStorageData& rhs) {
        this->numEntries += rhs.numEntries;
        this->recBytes += rhs.recBytes;
        this->bsonBytes += rhs.bsonBytes;
        this->onDiskBytes += rhs.onDiskBytes;
        this->charactSum += rhs.charactSum;
        this->charactCount += rhs.charactCount;
        verify(freeRecords.size() == rhs.freeRecords.size());
        vector<double>::const_iterator rhsit = rhs.freeRecords.begin();
        for (vector<double>::iterator thisit = this->freeRecords.begin();
                 thisit != this->freeRecords.end(); thisit++, rhsit++) {
            *thisit += *rhsit;
        }
        return *this;
    }

    void DiskStorageData::appendToBSONObjBuilder(BSONObjBuilder& b, bool includeFreeRecords) const {
        b.append("numEntries", double(numEntries));
        b.append("bsonBytes", bsonBytes);
        b.append("recBytes", recBytes);
        b.append("onDiskBytes", onDiskBytes);
        if (charactCount > 0) {
            b.append("charactSum", charactSum);
            b.append("charactCount", (double) charactCount);
        }
        if (includeFreeRecords) {
            b.append("freeRecsPerBucket", freeRecords);
        }
    }

    /**
     * @param recOfs file-relative record offset
     * @param extentOfs file-relative extent offset
     */
    RecPos RecPos::from(int recOfs, int recLen, int extentOfs, const AnalyzeParams& params) {
        RecPos res;
        res.numberOfChunks = params.numberOfChunks;
        // startsAt and endsAt are extent-relative
        int startsAt = recOfs - extentOfs;
        int endsAt = startsAt + recLen;
        if (endsAt < params.startOfs || startsAt >= params.endOfs) {
            res.outOfRange = true;
            return res;
        }
        else {
            res.outOfRange = false;
        }
        res.firstChunkNum = (startsAt - params.startOfs) / params.granularity;
        res.lastChunkNum = (endsAt - params.startOfs) / params.granularity;

        // extent-relative
        int endOfFirstChunk = (res.firstChunkNum + 1) * params.granularity + params.startOfs;
        res.sizeInFirstChunk = min(endOfFirstChunk - startsAt, recLen);
        res.sizeInMiddleChunk = params.granularity;
        res.sizeInLastChunk = recLen - res.sizeInFirstChunk -
                              params.granularity * (res.lastChunkNum - res.firstChunkNum
                                                    - 1);
        if (res.sizeInLastChunk < 0) {
            res.sizeInLastChunk = 0;
        }
        res.inFirstChunkRatio = (double) res.sizeInFirstChunk / recLen;
        res.inMiddleChunkRatio = (double) res.sizeInMiddleChunk / recLen;
        res.inLastChunkRatio = (double) res.sizeInLastChunk / recLen;
        return res;
    }

    bool RecPos::ChunkIterator::end() const {
        return _pos.outOfRange 
            || _curChunk.chunkNum >= _pos.numberOfChunks
            || _curChunk.chunkNum > _pos.lastChunkNum;
    }

    RecPos::ChunkIterator RecPos::iterateChunks() {
        return ChunkIterator(*this);
    }

    RecPos::ChunkInfo* RecPos::ChunkIterator::operator->() {
        verify(!end());
        if (!_fresh) {
            //TODO(andrea.lattuada) remove DEV block
            DEV { // defensive, see verify at end of function
                _curChunk.sizeHere = -1;
                _curChunk.ratioHere = -1;
            }
            if (_curChunk.chunkNum == _pos.firstChunkNum) {
                _curChunk.sizeHere = _pos.sizeInFirstChunk;
                _curChunk.ratioHere = _pos.inFirstChunkRatio;
            }
            else if (_curChunk.chunkNum == _pos.lastChunkNum) {
                _curChunk.sizeHere = _pos.sizeInLastChunk;
                _curChunk.ratioHere = _pos.inLastChunkRatio;
            }
            else {
                DEV verify(_pos.firstChunkNum < _curChunk.chunkNum &&
                           _curChunk.chunkNum < _pos.lastChunkNum);
                _curChunk.sizeHere = _pos.sizeInMiddleChunk;
                _curChunk.ratioHere = _pos.inMiddleChunkRatio;
            }
            verify(_curChunk.sizeHere >= 0 && _curChunk.ratioHere >= 0);
            _fresh = true;
        }
        return &_curChunk;
    }

    RecPos::ChunkIterator& RecPos::ChunkIterator::operator++() {
        _curChunk.chunkNum++;
        _fresh = false;
        return *this;
    }

    bool StorageDetailsCmd::analyzeDiskStorage(const NamespaceDetails* nsd, const Extent* ex,
                                               const AnalyzeParams& params, string& errmsg,
                                               BSONObjBuilder& result) {
        bool isCapped = nsd->isCapped();

        result.append("extentHeaderBytes", Extent::HeaderSize());
        result.append("recordHeaderBytes", Record::HeaderSize);
        result.append("range", BSON_ARRAY(params.startOfs << params.endOfs));
        result.append("isCapped", isCapped);

        vector<DiskStorageData> chunkData(params.numberOfChunks,
                                          DiskStorageData(params.granularity));
        chunkData[params.numberOfChunks - 1].onDiskBytes = params.lastChunkLength;
        Record* r;
        int extentOfs = ex->myLoc.getOfs();

        { // ensure done() is called by invoking destructor when done with the builder
            scoped_ptr<BSONArrayBuilder> recordsArrayBuilder;
            if (params.showRecords) {
                recordsArrayBuilder.reset(new BSONArrayBuilder(result.subarrayStart("records")));
            }

            for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
                r = dl.rec();
                processRecord(dl, r, extentOfs, params, chunkData, recordsArrayBuilder.get());
            }
        }

        { // ensure done() is called by invoking destructor when done with the builder
            scoped_ptr<BSONArrayBuilder> deletedRecordsArrayBuilder;
            if (params.showRecords) {
                deletedRecordsArrayBuilder.reset(
                        new BSONArrayBuilder(result.subarrayStart("deletedRecords")));
            }

            if (!isCapped) {
                for (int bucketNum = 0; bucketNum < mongo::Buckets; bucketNum++) {
                    DiskLoc dl = nsd->deletedList[bucketNum];
                    while (!dl.isNull()) {
                        DeletedRecord* dr = dl.drec();
                        processDeletedRecord(dl, dr, ex, params, bucketNum, chunkData,
                                             deletedRecordsArrayBuilder.get());
                        dl = dr->nextDeleted();
                    }
                }
            }
        }

        DiskStorageData extentData(0);
        {
            BSONArrayBuilder chunkArrayBuilder (result.subarrayStart("chunks"));
            for (vector<DiskStorageData>::iterator it = chunkData.begin();
                 it != chunkData.end(); ++it) {

                killCurrentOp.checkForInterrupt();
                extentData += *it;
                BSONObjBuilder chunkBuilder;
                it->appendToBSONObjBuilder(chunkBuilder, !isCapped);
                chunkArrayBuilder.append(chunkBuilder.obj());
            }
        }
        extentData.appendToBSONObjBuilder(result, !isCapped);
        return true;
    }

    bool StorageDetailsCmd::analyzeMemInCore(const Extent* ex, const AnalyzeParams& params,
                                             string& errmsg, BSONObjBuilder& result) {
        verify(sizeof(char) == 1);
        result.append("pageBytes", int(PAGE_SIZE));
        char* startAddr = (char*) ex + params.startOfs;

        int extentPages = ceilingDiv(params.endOfs - params.startOfs, int(PAGE_SIZE));
        int extentInMemCount = 0;

        BSONArrayBuilder arr(result.subarrayStart("chunks"));
        int chunkLength = params.granularity;
        for (int chunk = 0; chunk < params.numberOfChunks; ++chunk) {
            if (chunk == params.numberOfChunks - 1) {
                chunkLength = params.lastChunkLength;
            }
            int pagesInChunk = ceilingDiv(chunkLength, PAGE_SIZE);
            //TODO: remove
            DEV dlog(LL_DEBUG) << "pages in chunk # " << chunk << ": " << pagesInChunk << endl;
            int inMemCount = 0;
            for (int page = 0; page < pagesInChunk; ++page) {
                char* curPageAddr = startAddr + (chunk * params.granularity) +
                                    (page * PAGE_SIZE);
                //TODO: remove
                DEV if (page == 0) {
                    DEV tlog() << (void*) curPageAddr << endl;
                }
                if (ProcessInfo::blockInMemory(curPageAddr)) {
                    inMemCount++;
                    extentInMemCount++;
                }
            }
            arr.append(double(inMemCount) / pagesInChunk);
        }
        arr.done();
        result.append("inMem", double(extentInMemCount) / extentPages);

        return true;
    }

    bool StorageDetailsCmd::extractCharactFieldValue(BSONObj& obj, const AnalyzeParams& params,
                                                     double& value) {
        BSONElement elem = obj.getFieldDotted(params.charactField);
        if (elem.eoo()) {
            return false;
        }
        bool hasval = false;
        if (params.charactFieldIsObjId) {
            OID oid = elem.OID();
            value = params.startTime - oid.asTimeT();
            hasval = true;
        }
        else if (elem.isNumber()) {
            value = elem.numberDouble();
            hasval = true;
        }
        return hasval;
    }

    const Extent* StorageDetailsCmd::getNthExtent(int extentNum,
                                                  const NamespaceDetails* nsd) {
        int curExtent = 0;
        for (Extent* ex = DataFileMgr::getExtent(nsd->firstExtent);
             ex != NULL;
             ex = ex->getNextExtent()) {

            if (curExtent == extentNum) return ex;
            curExtent++;
        }
        return NULL;
    }

    void StorageDetailsCmd::processDeletedRecord(const DiskLoc& dl, const DeletedRecord* dr,
                                                 const Extent* ex, const AnalyzeParams& params,
                                                 int bucketNum,
                                                 vector<DiskStorageData>& chunkData,
                                                 BSONArrayBuilder* deletedRecordsArrayBuilder) {
        killCurrentOp.checkForInterrupt();

        int extentOfs = ex->myLoc.getOfs();

        if (! (dl.a() == ex->myLoc.a() &&
               dl.getOfs() + dr->lengthWithHeaders() >= extentOfs &&
               dl.getOfs() < extentOfs + ex->length) ) {

            return;
        }

        RecPos pos = RecPos::from(dl.getOfs(), dr->lengthWithHeaders(), extentOfs, params);
        bool spansRequestedArea = false;
        for (RecPos::ChunkIterator it = pos.iterateChunks(); !it.end(); ++it) {

            //TODO(andrea.lattuada) use operator[] when this is tested
            DiskStorageData& chunk = chunkData.at(it->chunkNum);
            chunk.freeRecords.at(bucketNum) += it->ratioHere;
            spansRequestedArea = true;
        }

        if (deletedRecordsArrayBuilder != NULL && spansRequestedArea) {
            BSONObjBuilder(deletedRecordsArrayBuilder->subobjStart())
                .append("ofs", dl.getOfs() - extentOfs)
                .append("recBytes", dr->lengthWithHeaders());
        }
    }

    void StorageDetailsCmd::processRecord(const DiskLoc& dl, const Record* r, int extentOfs,
                                          const AnalyzeParams& params,
                                          vector<DiskStorageData>& chunkData,
                                          BSONArrayBuilder* recordsArrayBuilder) {
        killCurrentOp.checkForInterrupt();

        BSONObj obj = dl.obj();
        int recBytes = r->lengthWithHeaders();
        double charactFieldValue;
        bool hasCharactField = extractCharactFieldValue(obj, params, charactFieldValue);

        RecPos pos = RecPos::from(dl.getOfs(), recBytes, extentOfs, params);
        bool spansRequestedArea = false;
        for (RecPos::ChunkIterator it = pos.iterateChunks(); !it.end(); ++it) {
            spansRequestedArea = true;
            DiskStorageData& chunk = chunkData.at(it->chunkNum);
            chunk.numEntries += it->ratioHere;
            chunk.recBytes += it->sizeHere;
            chunk.bsonBytes += it->ratioHere * obj.objsize();
            if (hasCharactField) {
                chunk.charactCount += it->ratioHere;
                chunk.charactSum += it->ratioHere * charactFieldValue;
            }
        }

        if (recordsArrayBuilder != NULL && spansRequestedArea) {
            DEV {
                int startsAt = dl.getOfs() - extentOfs;
                int endsAt = startsAt + recBytes;
                verify((startsAt < params.startOfs && endsAt > params.startOfs) ||
                       (startsAt < params.endOfs && endsAt >= params.endOfs) ||
                       (startsAt >= params.startOfs && endsAt < params.endOfs));
            }
            BSONObjBuilder recordBuilder(recordsArrayBuilder->subobjStart());
            recordBuilder.append("ofs", dl.getOfs() - extentOfs);
            recordBuilder.append("recBytes", recBytes);
            recordBuilder.append("bsonBytes", obj.objsize());
            recordBuilder.append("_id", obj["_id"]);
            if (hasCharactField) {
                recordBuilder.append("charact", charactFieldValue);
            }
        }
    }

    bool StorageDetailsCmd::run(const string& dbname , BSONObj& cmdObj, int, string& errmsg,
                                BSONObjBuilder& result, bool fromRepl) {

        // { analyze: subcommand }
        BSONElement analyzeElm = cmdObj["analyze"];
        if (analyzeElm.eoo()) {
            errmsg = "no subcommand specified, use {analyze: 'diskStorage' | 'memInCore'}";
            return false;
        }
        string subCommandStr = analyzeElm.String();
        SubCommand subCommand;
        if (!subCommandStr.compare("diskStorage")) {
            subCommand = SUBCMD_DISK_STORAGE;
        }
        else if (!subCommandStr.compare("memInCore")) {
            subCommand = SUBCMD_MEM_IN_CORE;
        }
        else {
            errmsg = str::stream() << subCommandStr << " is not a valid subcommand, "
                                   << "use 'diskStorage' or 'memInCore'";
            return false;
        }

        const string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
        NamespaceDetails * nsd = nsdetails(ns.c_str());
        if (!cmdLine.quiet) {
            tlog() << "CMD: storageDetails " << ns << ", analyze " << subCommandStr << endl;
        }
        if (!nsd) {
            errmsg = "ns not found";
            return false;
        }

        // { extent: num }
        BSONElement extentElm = cmdObj["extent"];
        if (extentElm.eoo() || !extentElm.isNumber()) {
            errmsg = "no extent specified, use {extent: extentNum}";
            return false;
        }
        int extentNum = extentElm.Number();
        const Extent* extent = getNthExtent(extentNum, nsd);
        if (extent == NULL) {
            errmsg = str::stream() << "extent " << extentNum << " does not exist";
            return false;
        }

        AnalyzeParams params;

        // { range: [from, to] }, extent-relative
        BSONElement rangeElm = cmdObj["range"];
        if (rangeElm.ok()) {
            params.startOfs = rangeElm["0"].Number();
            params.endOfs = rangeElm["1"].Number();
        }

        // { granularity: bytes }
        params.granularity = cmdObj["granularity"].number();

        // { numberOfChunks: bytes }
        params.numberOfChunks = cmdObj["numberOfChunks"].number();

        if (params.granularity == 0 && params.numberOfChunks == 0) {
            errmsg = "either granularity or numberOfChunks must be specified in options";
            return false;
        }

        BSONElement charactFieldElm = cmdObj["charactField"];
        if (charactFieldElm.ok()) {
            params.charactField = charactFieldElm["name"].String();
            params.charactFieldIsObjId = false;
            BSONElement isStdObjIdElm = charactFieldElm["isStdObjId"];
            if (!isStdObjIdElm.eoo()) {
                params.charactFieldIsObjId = isStdObjIdElm.Bool();
            }
        }

        params.showRecords = cmdObj["showRecords"].trueValue();

        return runInternal(nsd, extent, subCommand, params, errmsg, result);
    }

    bool StorageDetailsCmd::runInternal(const NamespaceDetails* nsd, const Extent* ex,
                                        SubCommand subCommand, AnalyzeParams& params,
                                        string& errmsg, BSONObjBuilder& result) {
        params.startOfs = max(0, params.startOfs);
        params.endOfs = min(params.endOfs, ex->length);
        params.length = params.endOfs - params.startOfs;
        if (params.numberOfChunks != 0) {
            params.granularity = (params.endOfs - params.startOfs + params.numberOfChunks
                                  - 1) / params.numberOfChunks;
        }
        params.numberOfChunks = ceilingDiv(params.length, params.granularity);
        params.lastChunkLength = params.length -
                (params.granularity * (params.numberOfChunks - 1));
        log(LL_DEBUG) << "this extent or part of extent (" << params.length << " bytes)"
                      << " will be split in " << params.numberOfChunks << " chunks" << endl;
        BSONObjBuilder outputBuilder;
        bool success = false;
        switch (subCommand) {
            case SUBCMD_DISK_STORAGE:
                success = analyzeDiskStorage(nsd, ex, params, errmsg, outputBuilder);
                break;
            case SUBCMD_MEM_IN_CORE:
                success = analyzeMemInCore(ex, params, errmsg, outputBuilder);
                break;
        }
        if (!success) return false;
        result.appendElements(outputBuilder.obj());
        return true;
    }

}  // namespace

}  // namespace mongo

