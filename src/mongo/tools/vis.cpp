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

#include "mongo/pch.h"

#include <fstream>
#include <iostream>
#include <list>
#include <string>

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/db.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/tools/tool.h"

using namespace mongo;

namespace po = boost::program_options;

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
        ("ofsFrom", po::value<int>(), "first offset inside the extent to analyze")
        ("ofsTo", po::value<int>(), "offset after the last one to analyze")
        ("orderExtent,e", po::value<int>(),
         "rearrange record pointers so that they are in the same order as they are on disk")
        ("showExtents", "show detailed info for each extent")
        ;
    }

    /**
     * Contains aggregate data regarding (a part of) an extent or collection.
     */
    struct Data {
        long long numEntries;
        long long bsonSize;
        long long recSize;
        long long onDiskSize;

        const Data operator += (const Data &rhs) {
            Data result = *this;

            this->numEntries += rhs.numEntries;
            this->recSize += rhs.recSize;
            this->bsonSize += rhs.bsonSize;
            this->onDiskSize += rhs.onDiskSize;

            return result;
        }

        /** Note: ownership is retained by the caller */
        void appendToBSONObjBuilder(BSONObjBuilder& b) const {
            b.append("numEntries", numEntries);
            b.append("bsonSize", bsonSize);
            b.append("recSize", recSize);
            b.append("onDiskSize", onDiskSize);
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
            << (data.numEntries > 0 ? data.bsonSize / data.numEntries : 0)
            << endl;
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

    /**
     * Note: should not be called directly. Use one of the examineExtent overloads.
     */
    Data examineExtentInternal(const Extent * ex, BSONObjBuilder& extentBuilder, int granularity,
                               int startOfs, int endOfs) {
        startOfs = (startOfs > 0) ? startOfs : 0;
        endOfs = (endOfs <= ex->length) ? endOfs : ex->length;
        int length = endOfs - startOfs;
        Data extentData = {0, 0, 0, endOfs - startOfs};
        Record * r;
        int numberOfChunks = (length + granularity - 1) / granularity;
        DEV log() << "this extent or part of extent (" << length << " long) will be split in "
          << numberOfChunks << " chunks" << endl;
        vector<Data> chunkData(numberOfChunks, (Data) {0, 0, 0, granularity});
        chunkData[numberOfChunks - 1].onDiskSize = length - (granularity * (numberOfChunks - 1));
        //TODO(andrea.lattuada) refactor?
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) {
            r = dl.rec();
            int chunkNum = (dl.getOfs() - ex->myLoc.getOfs() - startOfs) / granularity;
            int endOfChunk = (chunkNum + 1) * granularity + startOfs + ex->myLoc.getOfs() - 1;
            int leftInChunk = endOfChunk - dl.getOfs();
            //TODO(andrea.lattuada) accout for records overlapping the beginning of chunk boundary
            //TODO(andrea.lattuada) count partial records for numEntries
            if (chunkNum >= 0 && chunkNum < numberOfChunks) {
                Data& chunk = chunkData.at(chunkNum);
                chunk.numEntries++;
                extentData.numEntries++;
                int recSize = r->lengthWithHeaders();
                int exceedsChunkBy = r->lengthWithHeaders() - leftInChunk;
                int bsonSize = dl.obj().objsize();
                if (exceedsChunkBy <= 0) {
                    chunk.recSize += recSize;
                    extentData.recSize += recSize;
                    chunk.bsonSize += bsonSize;
                    extentData.bsonSize += bsonSize;
                } else { // record overlaps the end of chunk boundary
                    chunk.recSize += leftInChunk;
                    extentData.recSize += leftInChunk;
                    int bsonSizeAccountingHere = ((double) leftInChunk / recSize) * bsonSize;
                    chunk.bsonSize += bsonSizeAccountingHere;
                    extentData.bsonSize += bsonSizeAccountingHere;
                    if (chunkNum + 1 < numberOfChunks) {
                        Data& nextChunk = chunkData.at(chunkNum + 1);
                        nextChunk.recSize += exceedsChunkBy;
                        nextChunk.bsonSize += bsonSize - bsonSizeAccountingHere;
                        extentData.recSize += exceedsChunkBy;
                        extentData.bsonSize += bsonSize - bsonSizeAccountingHere;
                    }
                }
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
    inline Data examineExtent(const Extent * ex, BSONObjBuilder& extentBuilder, int granularity) {
        return examineExtentInternal(ex, extentBuilder, granularity, 0, INT_MAX);
    }

    /**
     * Examine the specified part of the extent (between startOfs and endOfs).
     * @param useNumChunks if true, ignore granularity and use the requested number of chunks to
     *                     determine their size
     * @param granularity size of the chunks the extent should be split into
     * @param numChunks number of chunks this part of extent should be split into
     * @return aggregate data related to the part of extent requested
     */
    inline Data examineExtent(const Extent * ex, BSONObjBuilder& extentBuilder, bool useNumChunks,
                              int granularity, int numChunks, int startOfs, int endOfs) {
        if (endOfs > ex->length) {
            endOfs = ex->length;
        }
        if (useNumChunks) {
            granularity = (endOfs - startOfs + numChunks - 1) / numChunks;
        }
        return examineExtentInternal(ex, extentBuilder, granularity, startOfs, endOfs);
    }

    /**
     * Examine an entire namespace.
     * @param useNumChunks if true, ignore granularity and use the requested number of chunks to
     *                     determine their size
     * @param granularity size of the chunks the namespace extents should be split into
     * @param numChunks number of chunks in which the namespace extents should be split into
     */
    Data examineCollection(ostream& out, ofstream* jsonOut, NamespaceDetails* nsd,
                           bool useNumChunks, int granularity, int numChunks, bool showExtents) {
        int extentNum = 0;
        BSONObjBuilder collectionBuilder;
        BSONArrayBuilder extentArrayBuilder (collectionBuilder.subarrayStart("extents"));
        Data collectionData = {0, 0, 0, 0};
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
            Data extentData = examineExtent(ex, extentBuilder, granularity);
            extentBuilder.done();
            if (showExtents) {
                printStats(out, str::stream() << "extent " << curExtent, extentData);
            }
            collectionData += extentData;
            extentNum++;
        }
        extentArrayBuilder.done();

        if (jsonOut != NULL) {
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
            Data extentData = examineExtent(ex, extentBuilder, hasParam("numChunks"), granularity,
                                            numChunks, getParam("ofsFrom", 0),
                                            getParam("ofsTo", INT_MAX));
            printStats(out, str::stream() << "extent " << extentNum, extentData);
            if (jsonOut != NULL) {
                *jsonOut << extentBuilder.obj().jsonString() << endl;
            }
            return 0;
        }

        // otherwise (no specific options)
        {
            Data collData = examineCollection(out, jsonOut.get(), nsd, hasParam("numChunks"),
                                              granularity, numChunks, hasParam("showExtents"));
            printStats(out, "collection", collData);
        }
        return 0;
    }
};

int main(int argc, char ** argv) {
    Vis v;
    return v.main(argc, argv);
}
