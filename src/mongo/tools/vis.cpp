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
 
#include "../pch.h"
#include "../db/db.h"
#include "mongo/client/dbclientcursor.h"
#include "tool.h"

#include <iostream>

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

using namespace mongo;

namespace po = boost::program_options;

class Vis : public Tool {
public :
    Vis() : Tool ("vis") {
        add_options()
        ("orderExtent,e", po::value<int>(), "rearrange record pointers so that they are in the same order as they are on disk");
    }

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

    int reorderExtent(ostream& out, Extent * ex) {
        set<DiskLoc> dls;

        out << "extent contents:" << endl;
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            out << dl.toString() << endl;
            dls.insert(dl);
        }

        set<DiskLoc>::iterator it;
        out << "set contents:" << endl;
        DiskLoc prev = DiskLoc();
        DiskLoc cur = DiskLoc();

        for (it = dls.begin(); it != dls.end(); it++) {
            prev = cur;
            cur = *it;
            if (prev.isNull()) {
                out << "new first" << endl;
                getDur().writingDiskLoc( ex->firstRecord ) = cur;
            }
            else {
                getDur().writingInt( prev.rec()->np()->nextOfs ) = cur.getOfs();
            }
            getDur().writingInt( cur.rec()->np()->prevOfs ) = prev.getOfs();

            out << cur.toString() << endl;
        }
        getDur().writingInt( cur.rec()->np()->nextOfs ) = DiskLoc::NullOfs;

        // result loop to see if its ordered now..

        //TODO look at geo to see how single file deve blocks work
        out << "resulting extent contents:"<< endl;
        for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = dl.rec()->nextInExtent(dl)) {
            out << dl.toString() << endl;
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

        if (hasParam("orderExtent")) {
            int extent_num = getParam("orderExtent", 0);
            Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
            //out << "extent branch " << extent_num << endl;

            if (extent_num < 0) {
                out << "ERROR: extent number must be non-negative" << endl;
            }

            for (int i = 1; i < extent_num; i++) {
                ex = ex->getNextExtent();
                if (ex == 0) {
                    out << "ERROR: extent " << extent_num << " does not exist" << endl;
                    return -1;
                }
            }

            //out << "size of extent pre passing: " << ex->length << endl;
            if (reorderExtent(out, ex) == 0) {
                out << "extent " << extent_num << "reordered" << endl;
                return 0;
            }
            else {
                out << "An error occurred while repairing extent " << extent_num << endl;
                return -1;
            }
        }

        
        
        int extent_num = 0;

        for (Extent * ex = DataFileMgr::getExtent(nsd->firstExtent); ex != 0; ex = ex->getNextExtent()) { // extent loop

            
            //TODO make struct, external struct for real totals, make += op
            int rec_num = 0;
            int total_rec_size = 0;
            int total_bson_size = 0;
            Record * r;

            for (DiskLoc dl = ex->firstRecord; ! dl.isNull(); dl = r->nextInExtent(dl)) { // record loop
                rec_num++;
                r = dl.rec();
                total_rec_size += r->lengthWithHeaders();
                total_bson_size += dl.obj().objsize();
            }

            //TODO compute/print stats
            out << "extent " << extent_num << ':'
                << "\n\tsize: " << ex->length 
                << "\n\tnumber of records: " << rec_num 
                << "\n\tsize used by records: " << total_rec_size 
                << "\n\tfree by records: " << ex->length - total_rec_size
                << "\n\t\% of extent used: " << (float)total_rec_size / (float)ex->length * 100 
                << "\n\taverage record size: " << total_rec_size / rec_num 
                << "\n\tsize used by BSONObjs: " << total_bson_size 
                << "\n\tfree by BSON calc: " << ex->length - total_bson_size - 16 * rec_num
                << "\n\t\% of extent used (BSON): " << (float)total_bson_size / (float)ex->length * 100 
                << "\n\taverage BSONObj size: " << total_bson_size / rec_num 
                << endl;

            extent_num++;
        }
        return 0;
    }
};

int main(int argc, char ** argv) {
    Vis v;
    return v.main(argc, argv);
}
