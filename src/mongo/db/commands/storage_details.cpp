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

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/tools/tool.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/mongoutils/math.h"

namespace mongo {

namespace {

    // Helper classes

    /**
     * Enum-like class to store which subcommand was requested.
     */
    class SubCommand {
    public:
        static const int diskStorage = 1 << 0;
        static const int memInCore = 1 << 1;

        static int fromStr(string& str);
    };

    /**
     * Simple struct to store various operation parameters to be passed around during analysis.
     */
    struct AnalyzeParams {
        int startOfs;
        int endOfs;
        int length;
        int numberOfChunks;
        int granularity;
        int lastChunkLength;
        string charactField;
        //TODO(andrea.lattuada) rename to charactFieldIsStdObjId and explain
        bool charactFieldIsObjId;
        bool showRecords;

        AnalyzeParams() : startOfs(0), endOfs(INT_MAX), length(INT_MAX), numberOfChunks(-1),
                          granularity(-1), lastChunkLength(0), charactField("_id"),
                          charactFieldIsObjId(true), showRecords(false) {
        }
    };

    /**
     * Aggregated information per chunk / extent.
     */
    struct DiskStorageData {
        long double numEntries;
        long long bsonSize;
        long long recSize;
        long long onDiskSize;
        double charactSum;
        long double charactCount;
        vector<double> freeRecords;

        DiskStorageData(long long diskSize) : numEntries(0), bsonSize(0), recSize(0),
                                              onDiskSize(diskSize), charactSum(0), charactCount(0),
                                              freeRecords(mongo::Buckets, 0) {
        }

        const DiskStorageData operator += (const DiskStorageData& rhs);

        void appendToBSONObjBuilder(BSONObjBuilder& b) const;
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
     *     endOfFirstChunk = 4M (4 000 000)
     *     sizeInFirstChunk = 0.25M
     *     sizeInLastChunk = 0.10M
     *     sizeInMiddleChunk = 0.5M (== size of chunk)
     *     inFirstChunkRatio = 0.25M / 1.35M = 0.185...
     *     inLastChunkRatio = 0.10M / 1.35M = 0.074...
     *     inMiddleChunkRatio = 0.5M / 1.35M = 0.37...
     */
    struct RecPosInChunks {
        int firstChunkNum;
        int lastChunkNum;
        int endOfFirstChunk;
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
        static const RecPosInChunks from(int recOfs, int recLen, int extentOfs,
                                         const AnalyzeParams& params);

        struct Result {
            int chunkNum;
            int sizeHere;
            double ratioHere;
        };

        int curChunk;

        Result start() {
            curChunk = max(firstChunkNum, 0);
            return next();
        }

        bool hasNext() {
            return curChunk < numberOfChunks;
        }

        /**
         * Calculate position of record among chunks.
         * @param recOfs record offset as reported by DiskLoc
         * @param recLen record on-disk size with headers included
         * @param extentOfs extent offset as reported by DiskLoc
         * @param params operation parameters (see AnalyzeParams for details)
         */
        Result next() {
            Result res;
            res.chunkNum = curChunk;
            if (curChunk < numberOfChunks) {
                DEV res.sizeHere = -1;
                DEV res.ratioHere = -1;
                if (res.chunkNum == firstChunkNum) {
                    res.sizeHere = sizeInFirstChunk;
                    res.ratioHere = inFirstChunkRatio;
                    return res;
                }
                if (res.chunkNum == lastChunkNum) {
                    res.sizeHere = sizeInLastChunk;
                    res.ratioHere = inLastChunkRatio;
                    return res;
                }
                DEV verify(firstChunkNum < res.chunkNum && res.chunkNum < lastChunkNum);
                res.sizeHere = sizeInMiddleChunk;
                res.ratioHere = inMiddleChunkRatio;
                DEV verify(res.sizeHere >= 0 && res.ratioHere >= 0 && res.ratioHere <= 1);
            }
            ++curChunk;
            return res;
        }
    };


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

        virtual void help(stringstream& h) const { h << "TODO. Slow."; }

        //TODO(andrea.lattuada) verify this is enough
        virtual LockType locktype() const { return READ; }

    private:
        /**
         * @return the requested extent if it exists, otherwise NULL
         */
        const Extent* getExtentNum(int extentNum, const NamespaceDetails* nsd);

        bool analyzeDiskStorage(const NamespaceDetails* nsd, const Extent* ex,
                                AnalyzeParams& params, string& errmsg, BSONObjBuilder& result);

        bool analyzeMemInCore(const Extent* ex, AnalyzeParams& params,
                              string& errmsg, BSONObjBuilder& result);

        bool extractCharactFieldValue(BSONObj& obj, const string& charactField,
                                      bool charactFieldIsObjId, time_t now, double& value);

        void processRecord(const DiskLoc& dl, const Record* r, int extentOfs,
                           const AnalyzeParams& params, time_t now,
                           vector<DiskStorageData>& chunkData,
                           BSONArrayBuilder* recordsArrayBuilder);

        void processDeletedRecord(const DiskLoc& dl, const DeletedRecord* dr, const Extent* ex,
                                  const AnalyzeParams& params, int bucketNum,
                                  vector<DiskStorageData>& chunkData,
                                  BSONArrayBuilder* deletedRecordsArrayBuilder);

        /**
         * Entry point, parses command parameters and invokes runInternal.
         */
        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl);

        bool runInternal(const NamespaceDetails* nsd, const Extent* ex, int subCommand,
                         AnalyzeParams& params, string& errmsg, BSONObjBuilder& result);

    } storageDetailsCmd;


    int SubCommand::fromStr(string& str) {
        if (str == "diskStorage") {
            return diskStorage;
        }
        else if (str == "memInCore") {
            return memInCore;
        }
        else {
            //TODO(andrea.lattuada) throw proper exception
            verify(false);
        }
    }

    const DiskStorageData DiskStorageData::operator+= (const DiskStorageData& rhs) {
        DiskStorageData result = *this;
        this->numEntries += rhs.numEntries;
        this->recSize += rhs.recSize;
        this->bsonSize += rhs.bsonSize;
        this->onDiskSize += rhs.onDiskSize;
        this->charactSum += rhs.charactSum;
        this->charactCount += rhs.charactCount;
        vector<double>::const_iterator rhsit = rhs.freeRecords.begin();
        for (vector<double>::iterator thisit = this->freeRecords.begin();
                 thisit != this->freeRecords.end(); thisit++, rhsit++) {
            *thisit += *rhsit;
        }
        return result;
    }

    void DiskStorageData::appendToBSONObjBuilder(BSONObjBuilder& b) const {
        b.append("numEntries", (double) numEntries);
        b.append("bsonSize", bsonSize);
        b.append("recSize", recSize);
        b.append("onDiskSize", onDiskSize);
        if (charactCount > 0) {
            b.append("charactSum", charactSum);
            b.append("charactCount", (double) charactCount);
        }
        b.append("freeRecsPerBucket", freeRecords);
    }

    const RecPosInChunks RecPosInChunks::from(int recOfs, int recLen, int extentOfs,
                                              const AnalyzeParams& config) {
        RecPosInChunks res;
        res.numberOfChunks = config.numberOfChunks;
        res.firstChunkNum = (recOfs - extentOfs - config.startOfs) / config.granularity;
        res.lastChunkNum = (recOfs + recLen - extentOfs - config.startOfs) /
                           config.granularity;
        res.endOfFirstChunk = (res.firstChunkNum + 1) * config.granularity + config.startOfs +
                              extentOfs;
        res.sizeInFirstChunk = min(res.endOfFirstChunk - recOfs, recLen);
        res.sizeInLastChunk = recLen - res.sizeInFirstChunk -
                              config.granularity * (res.lastChunkNum - res.firstChunkNum
                                                    - 1);
        res.sizeInMiddleChunk = config.granularity;
        if (res.sizeInLastChunk < 0) {
            res.sizeInLastChunk = 0;
        }
        res.inFirstChunkRatio = (double) res.sizeInFirstChunk / recLen;
        res.inLastChunkRatio = (double) res.sizeInLastChunk / recLen;
        res.inMiddleChunkRatio = (double) res.sizeInMiddleChunk / recLen;
        return res;
    }

    bool StorageDetailsCmd::analyzeDiskStorage(const NamespaceDetails* nsd, const Extent* ex,
                                               AnalyzeParams& params, string& errmsg,
                                               BSONObjBuilder& result) {
        result.append("extentHeaderSize", Extent::HeaderSize());
        result.append("recordHeaderSize", Record::HeaderSize);
        result.append("range", BSON_ARRAY(params.startOfs << params.endOfs));

        time_t now = time(NULL);
        vector<DiskStorageData> chunkData(params.numberOfChunks,
                                          DiskStorageData(params.granularity));
        chunkData[params.numberOfChunks - 1].onDiskSize = params.lastChunkLength;
        Record* r;
        int extentOfs = ex->myLoc.getOfs();

        { // ensure done() is called by invoking destructor when done with the builder
            scoped_ptr<BSONArrayBuilder> recordsArrayBuilder(NULL);
            if (params.showRecords) {
                recordsArrayBuilder.reset(new BSONArrayBuilder(result.subarrayStart("records")));
            }

            for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
                killCurrentOp.checkForInterrupt();
                r = dl.rec();

                processRecord(dl, r, extentOfs, params, now, chunkData, recordsArrayBuilder.get());
            }
        }

        { // ensure done() is called by invoking destructor when done with the builder
            scoped_ptr<BSONArrayBuilder> deletedRecordsArrayBuilder(NULL);
            if (params.showRecords) {
                deletedRecordsArrayBuilder.reset(
                        new BSONArrayBuilder(result.subarrayStart("deletedRecords")));
            }

            //TODO(andrea.lattuada) refactor
            if (nsd->isCapped()) {
                errmsg = "capped collections are not supported";
                return false;
            }
            else {
                for (int bucketNum = 0; bucketNum < mongo::Buckets; bucketNum++) {
                    DiskLoc dl = nsd->deletedList[bucketNum];
                    DeletedRecord* dr;
                    for (; !dl.isNull(); dl = dr->nextDeleted()) {
                        dr = dl.drec();
                        processDeletedRecord(dl, dr, ex, params, bucketNum, chunkData,
                                             deletedRecordsArrayBuilder.get());
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
                it->appendToBSONObjBuilder(chunkBuilder);
                chunkArrayBuilder.append(chunkBuilder.obj());
            }
        }
        extentData.appendToBSONObjBuilder(result);
        return true;
    }

    bool StorageDetailsCmd::analyzeMemInCore(const Extent* ex, AnalyzeParams& params,
                                             string& errmsg, BSONObjBuilder& result) {
        verify(sizeof(char) == 1);
        size_t pageSize = ProcessInfo::pageSize();
        result.append("pageSize", (int) pageSize);
        char* startAddr = (char*) ex + params.startOfs;

        BSONObjBuilder extentBuilder(result.subobjStart("extent"));
        BSONArrayBuilder arr(result.subarrayStart("chunks"));
        int chunkLength = params.granularity;
        for (int chunk = 0; chunk < params.numberOfChunks; ++chunk) {
            if (chunk == params.numberOfChunks - 1) {
                chunkLength = params.lastChunkLength;
            }
            int pagesInChunk = ceilingDiv(chunkLength, pageSize);
            DEV tlog() << "pages in chunk # " << chunk << ": " << pagesInChunk << endl;
            int inMemCount = 0;
            for (int page = 0; page < pagesInChunk; ++page) {
                char* curPageAddr = startAddr + (chunk * params.granularity) +
                                    (page * pageSize);
                if (page == 0) {
                    DEV tlog() << (void*) curPageAddr << endl;
                }
                if(ProcessInfo::blockInMemory(curPageAddr)) {
                    ++inMemCount;
                }
            }
            arr.append((double) inMemCount / pagesInChunk);
        }
        arr.done();
        extentBuilder.done();

        return true;
    }

    bool StorageDetailsCmd::extractCharactFieldValue(BSONObj& obj, const string& charactField,
                                                     bool charactFieldIsObjId, time_t now,
                                                     double& value) {
        BSONElement elem = obj.getFieldDotted(charactField);
        if (elem.eoo()) {
            return false;
        }
        bool hasval = false;
        if (charactFieldIsObjId) {
            OID oid = elem.OID();
            value = now - oid.asTimeT();
            hasval = true;
        }
        else if (elem.isNumber()) {
            value = elem.numberDouble();
            hasval = true;
        }
        return hasval;
    }

    const Extent* StorageDetailsCmd::getExtentNum(int extentNum, const NamespaceDetails* nsd) {
        Extent* ex;
        int curExtent;
        for (ex = DataFileMgr::getExtent(nsd->firstExtent), curExtent = 0;
             ex != NULL && curExtent < extentNum;
             ex = ex->getNextExtent(), ++curExtent) {
            continue;
        }
        if (curExtent != extentNum) {
            return NULL;
        }
        return ex;
    }

    void StorageDetailsCmd::processDeletedRecord(const DiskLoc& dl, const DeletedRecord* dr,
                                                 const Extent* ex, const AnalyzeParams& params,
                                                 int bucketNum,
                                                 vector<DiskStorageData>& chunkData,
                                                 BSONArrayBuilder* deletedRecordsArrayBuilder) {
        int extentOfs = ex->myLoc.getOfs();

        if (! (dl.a() == ex->myLoc.a() &&
               dl.getOfs() + dr->lengthWithHeaders() >= extentOfs &&
               dl.getOfs() < extentOfs + ex->length) ) {

            return;
        }
        

        RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), dr->lengthWithHeaders(),
                                                  extentOfs, params);
        for (RecPosInChunks::Result curChunk = pos.start(); pos.hasNext();
             curChunk = pos.next()) {

            DiskStorageData& chunk = chunkData.at(curChunk.chunkNum);
            chunk.freeRecords.at(bucketNum) += curChunk.ratioHere;
        }

        if (deletedRecordsArrayBuilder != NULL) {
            BSONObjBuilder(deletedRecordsArrayBuilder->subobjStart())
                .append("ofs", dl.getOfs() - extentOfs)
                .append("diskSize", dr->lengthWithHeaders());
        }
    }

    void StorageDetailsCmd::processRecord(const DiskLoc& dl, const Record* r, int extentOfs,
                                          const AnalyzeParams& params, time_t now,
                                          vector<DiskStorageData>& chunkData,
                                          BSONArrayBuilder* recordsArrayBuilder) {
        BSONObj obj = dl.obj();
        int recSize = r->lengthWithHeaders();
        double charactFieldValue;
        bool hasCharactField = extractCharactFieldValue(obj, params.charactField,
                                                        params.charactFieldIsObjId, now,
                                                        charactFieldValue);
        RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), recSize, extentOfs, params);
        bool touchesRequestedArea = false;
        for (RecPosInChunks::Result curChunk = pos.start(); pos.hasNext();
             curChunk = pos.next()) {

            touchesRequestedArea = true;
            DiskStorageData& chunk = chunkData.at(curChunk.chunkNum);
            chunk.numEntries += curChunk.ratioHere;
            chunk.recSize += curChunk.sizeHere;
            chunk.bsonSize += curChunk.ratioHere * obj.objsize();
            if (hasCharactField) {
                chunk.charactCount += curChunk.ratioHere;
                chunk.charactSum += curChunk.ratioHere * charactFieldValue;
            }
        }

        if (recordsArrayBuilder != NULL && touchesRequestedArea) {
            BSONObjBuilder recordBuilder(recordsArrayBuilder->subobjStart());
            recordBuilder.append("ofs", dl.getOfs() - extentOfs);
            recordBuilder.append("diskSize", recSize);
            recordBuilder.append("bsonSize", obj.objsize());
            BSONElement objIDElm;
            obj.getObjectID(objIDElm);
            recordBuilder.append("id", objIDElm.OID().toString());
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
        int subCommand = SubCommand::fromStr(subCommandStr);

        string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
        NamespaceDetails * nsd = nsdetails( ns.c_str() );
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
        const Extent* extent = getExtentNum(extentNum, nsd);
        if (extent == NULL) {
            errmsg = str::stream() << "extent " << extentNum << " does not exist";
            return false;
        }

        AnalyzeParams params;

        // { range: [from, to] }
        BSONElement rangeElm = cmdObj["range"];
        if (!rangeElm.eoo()) {
            vector<BSONElement> rangeVector = rangeElm.Array();
            params.startOfs = rangeVector[0].Number();
            params.endOfs = rangeVector[1].Number();
            // result.append("range", rangeElm);
        }

        // { granularity: bytes }
        BSONElement granularityElm = cmdObj["granularity"];
        if (!granularityElm.eoo()) {
            params.granularity = granularityElm.Number();
            // result.append("granularity", params.granularity);
        }

        BSONElement numChunksElm = cmdObj["numberOfChunks"];
        if (!numChunksElm.eoo()) {
            params.numberOfChunks = numChunksElm.Number();
            // result.append("numberOfChunks", params.numberOfChunks);
        }

        if (params.granularity == -1 && params.numberOfChunks == -1) {
            errmsg = "either granularity or numberOfChunks must be specified in options";
            return false;
        }

        BSONElement charactFieldElm = cmdObj["charactField"];
        if (!charactFieldElm.eoo()) {
            params.charactField = std::string(charactFieldElm.Obj()["name"].String());
            params.charactFieldIsObjId = false;
            BSONElement isStdObjIdElm = charactFieldElm.Obj()["isStdObjId"];
            if (!isStdObjIdElm.eoo()) {
                params.charactFieldIsObjId = isStdObjIdElm.Bool();
            }
        }

        BSONElement showRecsElm = cmdObj["showRecords"];
        if (!showRecsElm.eoo() && showRecsElm.Bool()) {
            params.showRecords = true;
        }

        killCurrentOp.checkForInterrupt();
        return runInternal(nsd, extent, subCommand, params, errmsg, result);
    }

    bool StorageDetailsCmd::runInternal(const NamespaceDetails* nsd, const Extent* ex,
                                        int subCommand, AnalyzeParams& params, string& errmsg,
                                        BSONObjBuilder& result) {
        params.startOfs = max(0, params.startOfs);
        params.endOfs = min(params.endOfs, ex->length);
        params.length = params.endOfs - params.startOfs;
        if (params.numberOfChunks != -1) {
            params.granularity = (params.endOfs - params.startOfs + params.numberOfChunks
                                  - 1) / params.numberOfChunks;
        }
        params.numberOfChunks = ceilingDiv(params.length, params.granularity);
        params.lastChunkLength = params.length -
                (params.granularity * (params.numberOfChunks - 1));
        DEV tlog() << "this extent or part of extent (" << params.length << " bytes)"
                   << " will be split in " << params.numberOfChunks << " chunks" << endl;
        switch (subCommand) {
            case SubCommand::diskStorage:
                return analyzeDiskStorage(nsd, ex, params, errmsg, result);
                break;
            case SubCommand::memInCore:
                return analyzeMemInCore(ex, params, errmsg, result);
                break;
        }
        errmsg = "no such subcommand";
        return false;
    }

}  // namespace

}  // namespace mongo

