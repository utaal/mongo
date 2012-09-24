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
     * POD class to store various operation parameters to be passed around during analysis.
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

        DiskStorageData(long long diskSize) : numEntries(0), bsonSize(0), recSize(0),
                                   onDiskSize(diskSize), charactSum(0), charactCount(0) {
        }

        const DiskStorageData operator += (const DiskStorageData& rhs);

        void appendToBSONObjBuilder(BSONObjBuilder& b) const;
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

        bool analyzeDiskStorage(const Extent* ex, AnalyzeParams& params,
                                string& errmsg, BSONObjBuilder& result);

        bool analyzeMemInCore(const Extent* ex, AnalyzeParams& params,
                              string& errmsg, BSONObjBuilder& result);

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
        return result;
    }

    void DiskStorageData::appendToBSONObjBuilder(BSONObjBuilder& b) const {
        b.append("numEntries", (double) numEntries);
        b.append("bsonSize", bsonSize);
        b.append("recSize", recSize);
        b.append("onDiskSize", onDiskSize);
        b.append("charactSum", charactSum);
        b.append("charactCount", (double) charactCount);
    }

    bool StorageDetailsCmd::analyzeDiskStorage(const Extent* ex, AnalyzeParams& params,
                                               string& errmsg, BSONObjBuilder& result) {
        vector<DiskStorageData> chunkData(params.numberOfChunks,
                                          DiskStorageData(params.granularity));
        chunkData[params.numberOfChunks - 1].onDiskSize = params.lastChunkLength;
        Record* r;
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
            killCurrentOp.checkForInterrupt();
            r = dl.rec();
            //TODO(andrea.lattuada) do actual work
            errmsg = "not implemented";
            return false;
        }

        BSONObjBuilder extentBuilder (result.subobjStart("extent"));
        BSONArrayBuilder chunkArrayBuilder (extentBuilder.subarrayStart("chunks"));
        DiskStorageData extentData(0);
        for (vector<DiskStorageData>::iterator it = chunkData.begin();
             it != chunkData.end(); ++it) {

            killCurrentOp.checkForInterrupt();
            extentData = *it;
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
        // DEV tlog() << "extent starts at " << ex << endl;
        // DEV tlog() << "start address " << (void*) startAddr << endl;
        BSONObjBuilder extentBuilder(result.subobjStart("extent"));
        BSONArrayBuilder arr(result.subarrayStart("chunks"));
        int chunkLength = params.granularity;
        for (int chunk = 0; chunk < params.numberOfChunks; ++chunk) {
            if (chunk == params.numberOfChunks - 1) {
                chunkLength = params.lastChunkLength;
            }
            int pagesInChunk = ceilDiv(chunkLength, pageSize);
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
        params.numberOfChunks = ceilDiv(params.length, params.granularity);
        params.lastChunkLength = params.length -
                (params.granularity * (params.numberOfChunks - 1));
        DEV tlog() << "this extent or part of extent (" << params.length << " bytes)"
                   << " will be split in " << params.numberOfChunks << " chunks" << endl;
        switch (subCommand) {
            case SubCommand::diskStorage:
                return analyzeDiskStorage(ex, params, errmsg, result);
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

