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

        AnalyzeParams() : startOfs(0), endOfs(INT_MAX), length(INT_MAX), numberOfChunks(-1),
                          granularity(-1), lastChunkLength(0), charactField("_id"),
                          charactFieldIsObjId(true) {
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

        static const RecPosInChunks from(int recOfs, int recLen, int extentOfs,
                                         const AnalyzeParams& config);

        void inChunk(int chunkNum, /*out*/ int& sizeHere, double& ratioHere) {
            DEV sizeHere = -1;
            DEV ratioHere = -1;
            if (chunkNum == firstChunkNum) {
                sizeHere = sizeInFirstChunk;
                ratioHere = inFirstChunkRatio;
                return;
            }
            if (chunkNum == lastChunkNum) {
                sizeHere = sizeInLastChunk;
                ratioHere = inLastChunkRatio;
                return;
            }
            DEV verify(firstChunkNum < chunkNum && chunkNum < lastChunkNum);
            sizeHere = sizeInMiddleChunk;
            ratioHere = inMiddleChunkRatio;
            DEV verify(sizeHere >= 0 && ratioHere >= 0 && ratioHere <= 1);
        }
    };


    // Command

    class StorageDetailsCmd : public Command {
    public:
        StorageDetailsCmd() : Command( "storageDetails" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "TODO. Slow."; }

        virtual LockType locktype() const { return READ; }
        //{ validate: "collectionnamewithoutthedbpart" [, scandata: <bool>] [, full: <bool> } */

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
        time_t now = time(NULL);
        vector<DiskStorageData> chunkData(params.numberOfChunks,
                                          DiskStorageData(params.granularity));
        chunkData[params.numberOfChunks - 1].onDiskSize = params.lastChunkLength;
        Record* r;
        int extentOfs = ex->myLoc.getOfs();
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
            killCurrentOp.checkForInterrupt();
            r = dl.rec();
            BSONObj obj = dl.obj();
            int recSize = r->lengthWithHeaders();
            double charactFieldValue;
            bool hasCharactField = extractCharactFieldValue(obj, params.charactField,
                                                            params.charactFieldIsObjId, now,
                                                            charactFieldValue);
            RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), recSize, extentOfs, params);
            for (int chunkNum = pos.firstChunkNum; chunkNum <= pos.lastChunkNum; ++chunkNum) {
                if (chunkNum < 0) { // the record starts before the beginning of the requested
                                    // offset ranage
                    continue;
                }
                if (chunkNum >= params.numberOfChunks) { // the records ends after the end of the
                                                         // requested offset range
                    break;
                }
                DiskStorageData& chunk = chunkData.at(chunkNum);
                int sizeHere;
                double ratioHere;
                pos.inChunk(chunkNum, sizeHere, ratioHere);
                chunk.numEntries += ratioHere;
                chunk.recSize += sizeHere;
                chunk.bsonSize += ratioHere * obj.objsize();
                if (hasCharactField) {
                    chunk.charactCount += ratioHere;
                    chunk.charactSum += ratioHere * charactFieldValue;
                }
            }
        }

        //TODO(andrea.lattuada) refactor
        if (nsd->isCapped()) {

        }
        else {
            for (int bucketNum = 0; bucketNum < mongo::Buckets; bucketNum++) {
                DiskLoc dl = nsd->deletedList[bucketNum];
                while (!dl.isNull()) {
                    DeletedRecord* dr = dl.drec();
                    if (dl.a() == ex->myLoc.a() && dl.getOfs() >= extentOfs &&
                            dl.getOfs() < extentOfs + ex->length) {

                        RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), dr->lengthWithHeaders(),
                                                                  extentOfs, params);
                        for (int chunkNum = pos.firstChunkNum; chunkNum <= pos.lastChunkNum; ++chunkNum) {
                            if (chunkNum < 0) { // the record starts before the beginning of the requested
                                                // offset ranage
                                continue;
                            }
                            if (chunkNum >= params.numberOfChunks) { // the records ends after the end of the
                                                                     // requested offset range
                                break;
                            }
                            DiskStorageData& chunk = chunkData.at(chunkNum);
                            int sizeHere;
                            double ratioHere;
                            pos.inChunk(chunkNum, sizeHere, ratioHere);
                            chunk.freeRecords.at(bucketNum) += ratioHere;
                        }
                    }
                    dl = dr->nextDeleted();
                }
            }
        }

        DiskStorageData extentData(0);
        for (vector<DiskStorageData>::iterator it = chunkData.begin();
             it != chunkData.end(); ++it) {

            killCurrentOp.checkForInterrupt();
            extentData += *it;
            BSONObjBuilder chunkBuilder;
            it->appendToBSONObjBuilder(chunkBuilder);
            chunkArrayBuilder.append(chunkBuilder.obj());
        }
        chunkArrayBuilder.done();
        extentData.appendToBSONObjBuilder(extentBuilder);
        extentBuilder.done();
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
            BSONElement isStdObjIdElm = charactFieldElm.Obj()["isStdObjId"];
            if (!isStdObjIdElm.eoo()) {
                params.charactFieldIsObjId = isStdObjIdElm.Bool();
            }
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

