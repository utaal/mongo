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
#include "mongo/tools/tool.h"

#if 0
#define VISDEBUG(x) cout << x << endl
#else
#define VISDEBUG(x)
#endif
using namespace mongo;

namespace po = boost::program_options;

class Vis : public Tool {
public :
    Vis() : Tool ("vis") {
        add_options()
        ("freeRecords", "report number of free records of each size")
        ("mincore", po::value<string>(), "report which files are in core memory")
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
            << "\n\taverage record size: " << data.recSize / data.numEntries 
            << "\n\tsize used by BSONObjs: " << data.bsonSize 
            << "\n\tfree by BSON calc: " << data.onDiskSize - data.bsonSize - 16 * data.numEntries
            << "\n\t\% of " << name << " used (BSON): " << (float)data.bsonSize / (float)data.onDiskSize * 100 
            << "\n\taverage BSONObj size: " << data.bsonSize / data.numEntries 
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

        VISDEBUG("extent contents:");
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            VISDEBUG(dl.toString());
            dls.insert(dl);
        }

        set<DiskLoc>::iterator it;
        VISDEBUG("set contents:");
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

            VISDEBUG(cur.toString());
        }
        getDur().writingInt( cur.rec()->np()->nextOfs ) = DiskLoc::NullOfs;

        // result loop to see if its ordered now only valuable for debugging
        VISDEBUG("resulting extent contents:");
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            VISDEBUG(dl.toString());
        }

        return 0;
    }

        for (int i = 0; i < 19; i++) { // 19 should be Buckets, not sure where to find this and stop being so magical
    int freeRecords(ostream& out, NamespaceDetails const * const nsd) {
            DiskLoc dl = nsd->deletedList[i];
            out << "Bucket " << i << ": ";
            int count = 0;
            while (!dl.isNull()) {
                count++;
                DeletedRecord *r = dl.drec();
                //DiskLoc extLoc(dl.a(), r->extentOfs()); this could be useful if we want blocks by size per extent
                dl = r->nextDeleted();
            }
            out << count << endl;
        }
        return 0;
    }

    int crawlNamespaces(ostream& out, string ns) {
        NamespaceIndex * nsi = nsindex(ns.c_str());
        list<string> namespaces;
        nsi->getNamespaces(namespaces, true);
        for (list<string>::iterator itr = namespaces.begin(); itr != namespaces.end(); itr++) { // namespace loop
            out << "namespace " << *itr << ':' << endl;
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

    int run() {
        string ns;

        // TODO allow out to be things from STDOUT
        ostream& out = cout;

        if (! hasParam("dbpath")) {
            out << "mongovis only works with --dbpath" << endl;
            return -1;
        }

        // TODO other safety checks and possibly checks for other connection types

        try {
            ns = getNS();
        } 
        catch (...) {
            printHelp(cerr);
            return -1;
        }

        string dbname = getParam ("db");
        Client::ReadContext cx (dbname);

        if (hasParam("namespaces")) {
            if (crawlNamespaces(out, ns) == 0) {
                return 0;
            }
            else {
                return -1;
            }
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

        int extentNum = 0;
        
        Data collectionData = {0, 0, 0, 0};

        for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent); ex != 0; ex = ex->getNextExtent()) { // extent loop
            Data extentData = {0, 0, 0, ex->length};
            Record * r;

            for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) { // record loop
                extentData.numEntries++;
                r = dl.rec();
                extentData.recSize += r->lengthWithHeaders();
                extentData.bsonSize += dl.obj().objsize();
            }

            printStats(out, str::stream() << "extent number " << extentNum, extentData);
            collectionData += extentData;
            extentNum++;
        }
        
        printStats(out, "collection", collectionData);
        return 0;
    }
};

int main(int argc, char ** argv) {
    Vis v;
    return v.main(argc, argv);
}
