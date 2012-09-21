// vis.cpp

/**
*    Copyright (C) 2012 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#error TODO(andrea.lattuada) this file will go away.

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
#include "mongo/db/db.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/tools/tool.h"
#include "mongo/util/processinfo.h"

using namespace mongo;

namespace po = boost::program_options;
time_t now = time(NULL);

class Vis : public Tool {
public :
    Vis() : Tool ("vis") {
        add_options()
        ("extent", po::value<int>(), "extent number to analyze")
        ("freeRecords", "report number of free records of each size")
        ("granularity", po::value<int>(),
         "granularity in bytes for the detailed space usage reports")
        ("jsonOut", po::value<string>(), "where to write the detailed json report")
        ("namespaces", "loop over all namespace to find an map of namespaces over extents on disk")
        ("numChunks", po::value<int>(),
         "number of chunks the namespace should be split into for deltailed usage reports")
        ("charactField", po::value<string>(),
         "a dotted notation path to a numeric field that characterized documents")
        ("objIdAsCharactField",
         "use the timestamp in the object-id to characterise the document")
        ("ofsFrom", po::value<int>(), "first offset inside the extent to analyze")
        ("ofsTo", po::value<int>(), "offset after the last one to analyze")
        ("orderExtent,e", po::value<int>(),
         "rearrange record pointers so that they are in the same order as they are on disk")
        ("pagesInMemory",
         "show which pages comprised in the extent are loaded in core memory")
        ("showExtents", "show detailed info for each extent")
        ;
    }

    /**
     * Contains aggregate data regarding (a part of) an extent or collection.
     */
    struct Data {
        long double numEntries;
        long long bsonSize;
        long long recSize;
        long long onDiskSize;
        double charactSum;
        long double charactCount;
        vector<int> freeRecords; // per bucket

        Data(long long diskSize) : numEntries(0), bsonSize(0), recSize(0), onDiskSize(diskSize),
                                   charactSum(0), charactCount(0),
                                   freeRecords(mongo::Buckets, 0) {
        }

        const Data operator += (const Data &rhs) {
            Data result = *this;

            this->numEntries += rhs.numEntries;
            this->recSize += rhs.recSize;
            this->bsonSize += rhs.bsonSize;
            this->onDiskSize += rhs.onDiskSize;
            this->charactSum += rhs.charactSum;
            this->charactCount += rhs.charactCount;
            vector<int>::const_iterator rhsit = rhs.freeRecords.begin();
            for (vector<int>::iterator thisit = this->freeRecords.begin();
                     thisit != this->freeRecords.end(); thisit++) {
                *thisit += *rhsit;
                rhsit++;
            }
            return result;
        }

        /** Note: ownership is retained by the caller */
        void appendToBSONObjBuilder(BSONObjBuilder& b) const {
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
    };

    virtual void preSetup() {
        // write output to standard error to avoid mangling output
        // must happen early to avoid sending junk to stdout
        useStandardOutput(false);
    }

    virtual void printExtraHelp(ostream& out) {
        out << "View statistics for data and journal files.\n" << endl;
    }

    /**
     * Print out statistics related to (a part of) an extent or collection.
     */
    void printStats(ostream& out, string name, Data data) {
        //TODO(andrea.lattuada) more compact summary
        out << name << ':'
            << "\n\tsize: " << data.onDiskSize
            << "\n\tnumber of records: " << data.numEntries
            << "\n\tsize used by records: " << data.recSize
            << "\n\tfree by records: " << data.onDiskSize - data.recSize
            << "\n\t% of " << name << " used: "
            << (float)data.recSize / (float)data.onDiskSize * 100
            << "\n\taverage record size: "
            << (data.numEntries > 0 ? data.recSize / data.numEntries : 0)
            << "\n\tsize used by BSONObjs: " << data.bsonSize
            << "\n\tfree by BSON calc: "
            << data.onDiskSize - data.bsonSize - 16 * data.numEntries
            << "\n\t% of " << name << " used (BSON): "
            << (float)data.bsonSize / (float)data.onDiskSize * 100
            << "\n\taverage BSONObj size: "
            << (data.numEntries > 0 ? data.bsonSize / data.numEntries : 0);
        if (data.charactCount > 0) {
            out << "\n\taverage object characteristic value: "
                << (data.charactCount > 0 ? data.charactSum / data.charactCount : 0);
        }
        out << endl;
    }

    /**
     * Reorder the records inside the specified extent.
     */
    int reorderExtent(ostream& out, int extentNum, NamespaceDetails const * const nsd) {
        //TODO(andrea.lattuada) review and test
        Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
        if (extentNum < 0) {
            out << "ERROR: extent number must be non-negative" << endl;
            return -1;
        }
        for (int i = 1; i < extentNum; i++) {
            ex = ex->getNextExtent();
            if (ex == 0) {
                out << "ERROR: extent " << extentNum << " does not exist" << endl;
                return -1;
            }
        }

        set<DiskLoc> dls;
        DEV log() << "extent contents:" << endl;
        for (DiskLoc dl = ex->firstRecord; !dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            DEV log() << dl.toString() << endl;
            dls.insert(dl);
        }

        set<DiskLoc>::iterator it;
        DEV log() << "set contents:" << endl;
        DiskLoc prev = DiskLoc();
        DiskLoc cur = DiskLoc();
        for (it = dls.begin(); it != dls.end(); it++) {
            prev = cur;
            cur = *it;
            if (prev.isNull()) {
                getDur().writingDiskLoc(ex->firstRecord) = cur;
            }
            else {
                getDur().writingInt(prev.rec()->np()->nextOfs) = cur.getOfs();
            }
            getDur().writingInt(cur.rec()->np()->prevOfs) = prev.getOfs();
            DEV log() << cur.toString() << endl;
        }
        getDur().writingInt(cur.rec()->np()->nextOfs) = DiskLoc::NullOfs;

        // TODO(dannenberg.matt) result loop to see if its ordered now only valuable for debugging
        DEV log() << "resulting extent contents:" << endl;
        for (DiskLoc dl = ex->firstRecord; !dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            DEV log() << dl.toString() << endl;
        }
        return 0;
    }

    /**
     * Print out the number of free records bucketed per size.
     */
    int freeRecords(ostream& out, ostream* jsonOut, NamespaceDetails const * const nsd) {
        // TODO(andrea.lattuada): modify behaviour when referring to capped collections
        // (see NamespaceDetails::deletedList)
        for (int i = 0; i < mongo::Buckets; i++) {
            DiskLoc dl = nsd->deletedList[i];
            out << "Bucket " << i << " (max size " << bucketSizes[i] << "): ";
            int count = 0;
            long totsize = 0;
            while (!dl.isNull()) {
                count++;
                DeletedRecord *r = dl.drec();
                totsize += r->lengthWithHeaders();
                dl = r->nextDeleted();
            }
            int averageSize = count > 0 ? totsize / count : 0;
            out << count << " records, average size " << averageSize << endl;
            if (jsonOut != NULL) {
                BSONObjBuilder b;
                b.append("bucket", i);
                b.append("bucketSize", bucketSizes[i]);
                b.append("count", count);
                b.append("totsize", (int) totsize);
                BSONObj bson = b.obj();
                *jsonOut << bson.jsonString() << endl;
            }
        }
        return 0;
    }

    void pagesInMemory(ostream& out, BSONObjBuilder& b, const Extent* ex) {
        //TODO(andrea.lattuada) review
        verify(sizeof(char) == 1);
        size_t pageSize = ProcessInfo::pageSize();
        int pageCount = (ex->length + pageSize - 1) / pageSize;
        BSONArrayBuilder arr(b.subarrayStart("pagesInMemory"));
        int inMemCount = 0;
        DEV log() << "extent starts at " << ex << endl;
        DEV log() << "first record at " << ex->firstRecord.rec() << endl;
        DEV log() << "last record at " << ex->lastRecord.rec() << endl;
        for (int page = 0; page < pageCount; ++page) {
            bool inMem = ProcessInfo::blockInMemory((char *) ex + page * pageSize);
            arr.append(inMem ? 1 : 0);
            if (inMem) {
                ++inMemCount;
            }
        }
        arr.done();
        out << inMemCount << " of " << pageCount << " pages in core" << endl;
    }

    /**
     * Print out all the namespaces in the database and general information about the extents they
     * refer to.
     * @param out output stream for aggregate, human readable info
     */
    int crawlNamespaces(ostream& out, string ns) {
        NamespaceIndex * nsi = nsindex(ns.c_str());
        list<string> namespaces;
        nsi->getNamespaces(namespaces, true);
        for (list<string>::iterator itr = namespaces.begin(); itr != namespaces.end(); itr++) {
            out << "----------------------------------\n" << "namespace " << *itr << ':' << endl;
            NamespaceDetails * nsd = nsdetails(itr->c_str());
            if (nsd->firstExtent.isNull()) {
                out << "ERROR: firstExtent is null" << endl;
                return -1;
            }
            if (!nsd->firstExtent.isValid()) {
                out << "ERROR: firstExtent is invalid" << endl;
                return -1;
            }
            int extentNum = 0;
            for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
                 ex != 0;
                 ex = ex->getNextExtent()) { // extent loop
                out << "\textent number " << extentNum << ':'
                    << "\n\t\tstarting loc: " << ex->myLoc.a() << '.' << ex->myLoc.getOfs()
                    << "\n\t\tsize: " << ex->length
                    << endl;
                extentNum++;
            }
        }
        return 0;
    }

    struct ExamineConfig {
        int startOfs;
        int endOfs;
        int numberOfChunks;
        int granularity;
        const string* charactField;
        bool charactFieldIsObjId;
    };

    struct RecPosInChunks {
        int chunkNum;
        int nextChunkNum;
        int endOfChunk;
        int sizeInCurChunk;
        bool curChunkExists;
        int sizeInNextChunk;
        bool nextChunkExists;
        bool overlapsBoundary;
        double inCurChunkRatio;

        RecPosInChunks() : overlapsBoundary(false), inCurChunkRatio(1) {
        }

        static const RecPosInChunks from(int recOfs, int recLen, int extentOfs, int numberOfChunks,
                                         const ExamineConfig& config) {
            RecPosInChunks res;
            res.chunkNum = (recOfs - extentOfs - config.startOfs) / config.granularity;
            res.nextChunkNum = res.chunkNum + 1;
            res.endOfChunk = (res.nextChunkNum) * config.granularity + config.startOfs + extentOfs
                             - 1;
            res.sizeInCurChunk = min(res.endOfChunk - recOfs, recLen);
            res.sizeInNextChunk = recLen - res.sizeInCurChunk;
            if (res.sizeInNextChunk > 0) {
                res.overlapsBoundary = true;
                res.inCurChunkRatio = res.sizeInCurChunk / recLen;
            } else {
                res.sizeInNextChunk = 0;
            }
            res.curChunkExists = (res.chunkNum >= 0 && res.chunkNum < numberOfChunks);
            res.nextChunkExists = (res.nextChunkNum >= 0 && res.nextChunkNum < numberOfChunks);
            return res;
        }
    };


    bool extractCharactFieldValue(BSONObj& obj, const string* charactField,
                                  bool charactFieldIsObjId, /* out */ double* value) {
        Data result(0);
        if (charactField == NULL) {
            return false;
        }
        BSONElement elem = obj.getFieldDotted(*charactField);
        if (elem.eoo()) {
            return false;
        }
        bool hasval = false;
        if (charactFieldIsObjId) {
            OID oid = elem.OID();
            *value = now - oid.asTimeT();
            hasval = true;
        }
        else if (elem.isNumber()) {
            *value = elem.numberDouble();
            hasval = true;
        }
        return hasval;
    }

    void addRecToChunk(Data& chunk, Data& extentData, double count, int recSize, int bsonSize,
                       double charactCount, double charactSum) {
         chunk.numEntries += count;
         extentData.numEntries += count;
         chunk.recSize += recSize;
         extentData.recSize += recSize;
         chunk.bsonSize += bsonSize;
         extentData.bsonSize += bsonSize;
         chunk.charactSum += charactSum;
         extentData.charactSum += charactSum;
         chunk.charactCount += charactCount;
         extentData.charactSum += charactSum;
    }

    void examineRecord(const Record* r, DiskLoc& dl, int extentOfs, int numberOfChunks,
                       const ExamineConfig& config, vector<Data>& chunkData, Data& extentData) {
        //int chunkNum = (dl.getOfs() - extentOfs - config.startOfs) / config.granularity;
        //int endOfChunk = (chunkNum + 1) * config.granularity + config.startOfs + extentOfs - 1;
        //int leftInChunk = endOfChunk - dl.getOfs();
        int recSize = r->lengthWithHeaders();
        RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), recSize, extentOfs,
                                                  numberOfChunks, config);
        //TODO(andrea.lattuada) accout for records overlapping the beginning of chunk boundary
        //TODO(andrea.lattuada) count partial records for numEntries
        BSONObj obj = dl.obj();
        int bsonSize = obj.objsize();
        double charactValue = 0;
        bool hasCharactValue = extractCharactFieldValue(obj, config.charactField,
                                                        config.charactFieldIsObjId,
                                                        /*out*/ &charactValue);
        if (pos.curChunkExists) {
            // avoid conversion of sizes to double if not needed
            if (!pos.overlapsBoundary) {
                Data& chunk = chunkData.at(pos.chunkNum);
                addRecToChunk(chunk, extentData, 1, recSize, bsonSize, hasCharactValue ? 1 : 0,
                              charactValue);
            } else {
                Data& chunk = chunkData.at(pos.chunkNum);
                int bsonSizeAccountingHere = pos.inCurChunkRatio * bsonSize;
                double charactValueAccountingHere = pos.inCurChunkRatio * charactValue;
                addRecToChunk(chunk, extentData, pos.inCurChunkRatio, pos.sizeInCurChunk,
                              bsonSizeAccountingHere, hasCharactValue ? pos.inCurChunkRatio : 0,
                              charactValueAccountingHere);
            }
        }
        if (pos.nextChunkExists && pos.overlapsBoundary) {
            Data& chunk = chunkData.at(pos.nextChunkNum);
            int bsonSizeAccountingHere = (1.0l - pos.inCurChunkRatio) * bsonSize;
            double charactValueAccountingHere = (1.0l - pos.inCurChunkRatio) * charactValue;
            addRecToChunk(chunk, extentData, (1.0l - pos.inCurChunkRatio), pos.sizeInNextChunk,
                          bsonSizeAccountingHere,
                          hasCharactValue ? (1.0l - pos.inCurChunkRatio) : 0,
                          charactValueAccountingHere);
        }
    }

    void examineDeletedRecord(const DiskLoc& dl, const DeletedRecord* dr, int bucketNum,
                              int extentOfs, int numberOfChunks, const ExamineConfig& config,
                              vector<Data>& chunkData, Data& extentData) {
        RecPosInChunks pos = RecPosInChunks::from(dl.getOfs(), dr->lengthWithHeaders(), extentOfs,
                                                  numberOfChunks, config);
        if (pos.curChunkExists) {
            Data& chunk = chunkData.at(pos.chunkNum);
            chunk.freeRecords.at(bucketNum) += pos.inCurChunkRatio;
            extentData.freeRecords.at(bucketNum) += pos.inCurChunkRatio;
        }
        if (pos.nextChunkExists && pos.overlapsBoundary) {
            Data& chunk = chunkData.at(pos.nextChunkNum);
            chunk.freeRecords.at(bucketNum) += 1.0l - pos.inCurChunkRatio;
            extentData.freeRecords.at(bucketNum) += 1.0l - pos.inCurChunkRatio;
        }
    }

    /**
     * Note: should not be called directly. Use one of the examineExtent overloads.
     */
    Data examineExtentInternal(const NamespaceDetails* nsd, const Extent* ex,
                               BSONObjBuilder& extentBuilder, ExamineConfig config) {
        config.startOfs = (config.startOfs > 0) ? config.startOfs : 0;
        config.endOfs = (config.endOfs <= ex->length) ? config.endOfs : ex->length;
        int length = config.endOfs - config.startOfs;
        Data extentData(length);
        Record * r;
        int numberOfChunks = (length + config.granularity - 1) / config.granularity;
        DEV log() << "this extent or part of extent (" << length << " long) will be split in "
          << numberOfChunks << " chunks" << endl;
        vector<Data> chunkData(numberOfChunks, Data(config.granularity));
        chunkData[numberOfChunks - 1].onDiskSize = length -
                                                   (config.granularity * (numberOfChunks - 1));

        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
            r = dl.rec();
            examineRecord(r, dl, ex->myLoc.getOfs(), numberOfChunks, config, chunkData, extentData);
        }

        for (int bucketNum = 0; bucketNum < mongo::Buckets; bucketNum++) {
            //TODO(andrea.lattuada) move this to examineCollection (ensure invocation when using
            //                      examinePartOfExtent directly
            DiskLoc dl = nsd->deletedList[bucketNum];
            while (!dl.isNull()) {
                DeletedRecord* dr = dl.drec();
                if (dl.a() == ex->myLoc.a()) {
                  examineDeletedRecord(dl, dr, bucketNum, ex->myLoc.getOfs(), numberOfChunks, config,
                                       chunkData, extentData);
                }
                dl = dr->nextDeleted();
            }
        }

        BSONArrayBuilder chunkArrayBuilder (extentBuilder.subarrayStart("chunks"));
        for (vector<Data>::iterator it = chunkData.begin(); it != chunkData.end(); ++it) {
            BSONObjBuilder chunkBuilder;
            it->appendToBSONObjBuilder(chunkBuilder);
            chunkArrayBuilder.append(chunkBuilder.obj());
        }
        chunkArrayBuilder.done();
        extentData.appendToBSONObjBuilder(extentBuilder);
        return extentData;
    }

    /**
     * Examine the entire extent by slicing it in chunks.
     * @param granularity size of the chunks the extent should be split into for analysis
     * @return aggregate data related to the entire extent
     */
    inline Data examineEntireExtent(const NamespaceDetails* nsd, const Extent* ex,
                                    BSONObjBuilder& extentBuilder,
                                    int granularity, const string* charactField,
                                    bool charactFieldIsObjId) {
        ExamineConfig config;
        config.startOfs = 0;
        config.endOfs = INT_MAX;
        config.granularity = granularity;
        config.charactField = charactField;
        config.charactFieldIsObjId = charactFieldIsObjId;
        return examineExtentInternal(nsd, ex, extentBuilder, config);
    }

    /**
     * Examine the specified part of the extent (between startOfs and endOfs).
     * @param useNumChunks if true, ignore granularity and use the requested number of chunks to
     *                     determine their size
     * @param granularity size of the chunks the extent should be split into
     * @param numChunks number of chunks this part of extent should be split into
     * @return aggregate data related to the part of extent requested
     */
    inline Data examinePartOfExtent(const NamespaceDetails* nsd, const Extent* ex,
                                    BSONObjBuilder& extentBuilder,
                                    bool useNumChunks, int granularity, int numChunks, int startOfs,
                                    int endOfs, const string* charactField,
                                    bool charactFieldIsObjId) {
        ExamineConfig config;
        config.startOfs = startOfs;
        config.endOfs = min(endOfs, ex->length);
        if (useNumChunks) {
            config.granularity = (config.endOfs - config.startOfs + numChunks - 1) / numChunks;
        } else {
            config.granularity = granularity;
        }
        config.charactField = charactField;
        config.charactFieldIsObjId = charactFieldIsObjId;
        return examineExtentInternal(nsd, ex, extentBuilder, config);
    }

    /**
     * Examine an entire namespace.
     * @param useNumChunks if true, ignore granularity and use the requested number of chunks to
     *                     determine their size
     * @param granularity size of the chunks the namespace extents should be split into
     * @param numChunks number of chunks in which the namespace extents should be split into
     */
    Data examineCollection(ostream& out, ofstream* jsonOut, NamespaceDetails* nsd,
                           bool useNumChunks, int granularity, int numChunks, bool showExtents,
                           const string* charactField, bool charactFieldIsObjId) {
        int extentNum = 0;
        BSONObjBuilder collectionBuilder;
        BSONArrayBuilder extentArrayBuilder (collectionBuilder.subarrayStart("extents"));
        Data collectionData(0);
        if (useNumChunks) {
            int totsize = 0;
            int count = 0;
            for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
                 ex != 0;
                 ex = ex->getNextExtent()) {
                totsize += ex->length;
                count++;
            }
            granularity = (totsize + (numChunks - count - 1)) / (numChunks - count);
            DEV log() << "granularity will be " << granularity << endl;
        }

        int curExtent = 0;
        for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
             ex != 0;
             ex = ex->getNextExtent(), ++curExtent) {
            BSONObjBuilder extentBuilder (extentArrayBuilder.subobjStart());
            Data extentData = examineEntireExtent(nsd, ex, extentBuilder, granularity, charactField,
                                                  charactFieldIsObjId);
            extentBuilder.done();
            if (showExtents) {
                printStats(out, str::stream() << "extent " << curExtent, extentData);
            }
            collectionData += extentData;
            extentNum++;
        }
        extentArrayBuilder.done();

        if (jsonOut != NULL) {
            collectionData.appendToBSONObjBuilder(collectionBuilder);
            *jsonOut << collectionBuilder.obj().jsonString() << endl;
        }
        return collectionData;
    }

    int run() {
        string ns;
        ostream& out = cout;

        if (!hasParam("dbpath")) {
            out << "mongovis only works with --dbpath" << endl;
            return -1;
        }

        // TODO(dannenberg.matt) other safety checks and possibly checks for other connection types
        string dbname = getParam ("db");
        Client::ReadContext cx (dbname);

        scoped_ptr<ofstream> jsonOut (NULL);
        if (hasParam("jsonOut")) {
            jsonOut.reset(new ofstream);
            jsonOut->open(getParam("jsonOut").c_str(), ios_base::trunc);
        }

        if (hasParam("namespaces")) {
            if (crawlNamespaces(out, dbname) == 0) {
                return 0;
            }
            else {
                return -1;
            }
        }

        try {
            ns = getNS();
        } 
        catch (...) {
            printHelp(cerr);
            return -1;
        }

        NamespaceDetails * nsd = nsdetails(ns.c_str());
        //TODO(dannenberg.matt) make sure nsd is valid/safe/etc

        if (nsd->firstExtent.isNull()) {
            out << "ERROR: firstExtent is null" << endl;
            return -1;
        }

        if (!nsd->firstExtent.isValid()) {
            out << "ERROR: firstExtent is invalid" << endl;
            return -1;
        }

        // --freeRecords
        if (hasParam("freeRecords")) {
            if (freeRecords(out, jsonOut.get(), nsd) == 0) {
                return 0;
            }
            else {
                out << "Error: failed to successfully traverse free blocks" <<endl;
                return -1;
            }
        }


        // --orderExtent
        if (hasParam("orderExtent")) {
            int extentNum = getParam("orderExtent", 0);
            if (reorderExtent(out, extentNum, nsd) == 0) {
                out << "extent " << extentNum << " reordered" << endl;
                return 0;
            }
            else {
                out << "An error occurred while repairing extent " << extentNum << endl;
                return -1;
            }
        }

        int granularity = getParam("granularity", 1<<20); // 1 MB by default
        int numChunks = getParam("numChunks", 1000);

        bool charactFieldIsObjId = false;
        scoped_ptr<string> charactField(NULL);
        if (hasParam("objIdAsCharactField")) {
            charactFieldIsObjId = true;
            charactField.reset(new string("_id"));
        }
        else if (hasParam("charactField")) {
            charactField.reset(new string(getParam("charactField")));
            DEV log() << "using characteristic record field " << *charactField << endl;
        }

        // --extent num
        if (hasParam("extent")) {
            int extentNum = getParam("extent", 0);
            int curExtent;

            Extent * ex;
            //TODO(andrea.lattuada) it looks like looping is not stopped when the last available
            //                      extent is reached
            for (ex = DataFileMgr::getExtent(nsd->firstExtent), curExtent = 0;
                 ex != NULL && curExtent < extentNum;
                 ex = ex->getNextExtent(), ++curExtent) {
                continue;
            }
            if (curExtent != extentNum) {
                out << "extent " << extentNum << " does not exist";
                return -1;
            }
            BSONObjBuilder extentBuilder;
            // --pagesInMemory
            if (hasParam("pagesInMemory")) {
                BSONObjBuilder extentBuilder;
                pagesInMemory(out, extentBuilder, ex);
                if (jsonOut != NULL) {
                    *jsonOut << extentBuilder.obj().jsonString() << endl;
                }
                return 0;
            }

            Data extentData = examinePartOfExtent(nsd, ex, extentBuilder, hasParam("numChunks"),
                                                  granularity, numChunks, getParam("ofsFrom", 0),
                                                  getParam("ofsTo", INT_MAX), charactField.get(),
                                                  charactFieldIsObjId);
            printStats(out, str::stream() << "extent " << extentNum, extentData);
            if (jsonOut != NULL) {
                *jsonOut << extentBuilder.obj().jsonString() << endl;
            }
            return 0;
        }

        // otherwise (no specific options)
        {
            Data collData = examineCollection(out, jsonOut.get(), nsd, hasParam("numChunks"),
                                              granularity, numChunks, hasParam("showExtents"),
                                              charactField.get(), charactFieldIsObjId);
            printStats(out, "collection", collData);
        }
        return 0;
    }
};

int main(int argc, char ** argv) {
    Vis v;
    return v.main(argc, argv);
}
