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

#include <iostream>

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
        ("freeRecords", "report number of free records of each size")
        ("mincore", po::value<string>(), "report which files are in core memory")
        ("granularity", po::value<int>(), "granularity in bytes for the detailed space usage reports")
        ("numChunks", po::value<int>(), "number of chunks the namespace should be split into for deltailed usage reports")
        ("showExtents", "show detailed info for each extent")
        ("namespaces", "loop over all namespace to find an map of namespaces over extents on disk")
        ("orderExtent,e", po::value<int>(), "rearrange record pointers so that they are in the same order as they are on disk")
        ;
    }

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

        // NOTE: ownership stays with the caller
        void appendToBSONObjBuilder(BSONObjBuilder* b) const {
            b->append("numEntries", numEntries);
            b->append("bsonSize", bsonSize);
            b->append("recSize", recSize);
            b->append("onDiskSize", onDiskSize);
        }

        // Data & operator = (const Data &rhs) {
        //     this->numEntries = rhs.numEntries;
        //     this->recSize = rhs.recSize;
        //     this->bsonSize = rhs.bsonSize;
        //     this->onDiskSize = rhs.onDiskSize;

        //     return *this;
        // }
    } ;

    virtual void preSetup() {
        string out = getParam("out");
        if (out == "-") {
            // write output to standard error to avoid mangling output
            // must happen early to avoid sending junk to stdout
            useStandardOutput(false);
        }
    }

    virtual void printExtraHelp(ostream& out) {
        out << "View statistics for data and journal files.\n" << endl;
    }

    void printStats(ostream& out, string name, Data data) {
        out << name << ':'
            << "\n\tsize: " << data.onDiskSize 
            << "\n\tnumber of records: " << data.numEntries 
            << "\n\tsize used by records: " << data.recSize 
            << "\n\tfree by records: " << data.onDiskSize - data.recSize
            << "\n\t\% of " << name << " used: " << (float)data.recSize / (float)data.onDiskSize * 100 
            << "\n\taverage record size: " << (data.numEntries > 0 ? data.recSize / data.numEntries : 0)
            << "\n\tsize used by BSONObjs: " << data.bsonSize 
            << "\n\tfree by BSON calc: " << data.onDiskSize - data.bsonSize - 16 * data.numEntries
            << "\n\t\% of " << name << " used (BSON): " << (float)data.bsonSize / (float)data.onDiskSize * 100 
            << "\n\taverage BSONObj size: " << (data.numEntries > 0 ? data.bsonSize / data.numEntries : 0)
            << endl;
    }

    int reorderExtent(ostream& out, int extentNum, NamespaceDetails const * const nsd) {
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
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
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
                getDur().writingDiskLoc( ex->firstRecord ) = cur;
            }
            else {
                getDur().writingInt( prev.rec()->np()->nextOfs ) = cur.getOfs();
            }

            getDur().writingInt( cur.rec()->np()->prevOfs ) = prev.getOfs();

            DEV log() << cur.toString() << endl;
        }
        getDur().writingInt( cur.rec()->np()->nextOfs ) = DiskLoc::NullOfs;

        // result loop to see if its ordered now only valuable for debugging
        DEV log() << "resulting extent contents:" << endl;
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            DEV log() << dl.toString() << endl;
        }

        return 0;
    }

    int freeRecords(ostream& out, NamespaceDetails const * const nsd) {
        for (int i = 0; i < mongo::Buckets; i++) { // TODO: modify behaviour when referring to capped collections
                                                   // see NamespaceDetails::deletedList
            DiskLoc dl = nsd->deletedList[i];
            out << "Bucket " << i << " (max size " << bucketSizes[i] << "): ";
            int count = 0;
            long totsize = 0;
            while (!dl.isNull()) {
                count++;
                DeletedRecord *r = dl.drec();
                totsize += r->lengthWithHeaders();
                //DEV log() << "vis: deleted record of size " << r->lengthWithHeaders() << endl;
                //DiskLoc extLoc(dl.a(), r->extentOfs()); this could be useful if we want blocks by size per extent
                dl = r->nextDeleted();
            }
            int averageSize = count > 0 ? totsize / count : 0;
            BSONObjBuilder b;
            b.append("bucket", i);
            b.append("bucketSize", bucketSizes[i]);
            b.append("count", count);
            b.append("totsize", (long long int) totsize);
            BSONObj bson = b.obj();
            out << count << " records, average size " << averageSize << endl;
            out << "BSON " << bson.toString() << endl;
        }
        return 0;
    }

    int crawlNamespaces(ostream& out, string ns) {
        NamespaceIndex * nsi = nsindex(ns.c_str());
        list<string> namespaces;
        nsi->getNamespaces(namespaces, true);
        for (list<string>::iterator itr = namespaces.begin(); itr != namespaces.end(); itr++) { // namespace loop
            out << "---------------------------------------\n" << "namespace " << *itr << ':' << endl;
            NamespaceDetails * nsd = nsdetails(itr->c_str());

            if (nsd->firstExtent.isNull()) {
                out << "ERROR: firstExtent is null" << endl;
                return -1;
            }

            if (! nsd->firstExtent.isValid()) {
                out << "ERROR: firstExtent is invalid" << endl;
                return -1;
            }

            int extentNum = 0;
            for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent); ex != 0; ex = ex->getNextExtent()) { // extent loop
                out << "\textent number " << extentNum << ':' 
                    << "\n\t\tstarting loc: " << ex->myLoc.a() << '.' << ex->myLoc.getOfs()
                    << "\n\t\tsize: " << ex->length
                    << endl;
                extentNum++;
            }
        }
        return 0;
    }

    void examineCollection(ostream& out, NamespaceDetails * nsd, bool useNumChunks, int granularity, int numChunks, bool showExtents) {
        int extentNum = 0;
        
        BSONArrayBuilder bExtentArray;
        Data collectionData = {0, 0, 0, 0};

        if (useNumChunks) {
            long long totsize = 0;
            int count = 0;
            for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent); ex != 0; ex = ex->getNextExtent()) {
                totsize += ex->length;
                count++;
            }
            //granularity = (totsize + (numChunks - count - 1)) / (numChunks - count);
            granularity = (totsize + (numChunks - count - 1)) / (numChunks - count);
            DEV log() << "granularity will be " << granularity << endl;
        }

        int totNumberOfChunks = 0;
        for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent); ex != 0; ex = ex->getNextExtent()) { // extent loop
            Data extentData = {0, 0, 0, ex->length};

            Record * r;
            int numberOfChunks = (ex->length + granularity - 1) / granularity;
            totNumberOfChunks += numberOfChunks;
            DEV log() << "this extent (" << ex->length << " long) will be split in " << numberOfChunks << " chunks" << endl;
            vector<Data> chunkData(numberOfChunks);
            for (vector<Data>::iterator it = chunkData.begin(); it != chunkData.end(); ++it) {
                *it = (Data) {0, 0, 0, granularity};
            }
            chunkData[numberOfChunks - 1].onDiskSize = ex->length - (granularity * (numberOfChunks - 1));

            for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) { // record loop
                // if (showExtents) {
                //     printStats(out, str::stream() << "extent " << extentNum << ", chunk " << currentChunk, chunkData);
                // }
                r = dl.rec();
                Data& chunk = chunkData.at((dl.getOfs() - ex->myLoc.getOfs()) / granularity);
                chunk.numEntries++;
                extentData.numEntries++;
                chunk.recSize += r->lengthWithHeaders();
                extentData.recSize += r->lengthWithHeaders();
                chunk.bsonSize += dl.obj().objsize();
                extentData.bsonSize += dl.obj().objsize();
            }
            BSONArrayBuilder bChunkArray;
            for (vector<Data>::iterator it = chunkData.begin(); it != chunkData.end(); ++it) {
                BSONObjBuilder bChunk;
                it->appendToBSONObjBuilder(&bChunk);
                bChunkArray.append(bChunk.obj());
                // if (showExtents) {
                //     printStats(out, str::stream() << "extent " << extentNum << ", chunk" << currentChunk, chunkData);
                // }
            }

            if (showExtents) {
                printStats(out, str::stream() << "extent number " << extentNum, extentData);
            }
            BSONObjBuilder bExtent;
            extentData.appendToBSONObjBuilder(&bExtent);
            
            bExtent.append("chunks", bChunkArray.obj());
            bExtentArray.append(bExtent.obj());
            collectionData += extentData;
            extentNum++;
        }
        DEV log() << " tot num of chunks: " << totNumberOfChunks << endl;
        
        printStats(out, "collection", collectionData);
        BSONObj collObj = bExtentArray.obj();
        out << "BSON " << collObj.jsonString() << endl;
    }

    int run() {
        string ns;

        // TODO allow out to be things from STDOUT
        ostream& out = cout;

        if (! hasParam("dbpath")) {
            out << "mongovis only works with --dbpath" << endl;
            return -1;
        }

        // TODO other safety checks and possibly checks for other connection types
        string dbname = getParam ("db");
        Client::ReadContext cx (dbname);

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
        //TODO make sure nsd is valid/safe/etc

        if (nsd->firstExtent.isNull()) {
            out << "ERROR: firstExtent is null" << endl;
            return -1;
        }

        if (! nsd->firstExtent.isValid()) {
            out << "ERROR: firstExtent is invalid" << endl;
            return -1;
        }

        if (hasParam("freeRecords")) {
            if (freeRecords(out, nsd) == 0) {
                return 0;
            }
            else {
                out << "Error: failed to successfully traverse free blocks" <<endl;
                return -1;
            }
        }

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

        examineCollection(out, nsd, hasParam("numChunks"), granularity, numChunks, hasParam("showExtents"));

        return 0;
    }
};

int main(int argc, char ** argv) {
    Vis v;
    return v.main(argc, argv);
}
