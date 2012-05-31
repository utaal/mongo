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
    Vis() : Tool ( "vis" ) {
        add_options();
    }

    virtual void preSetup() {
        string out = getParam("out");
        if ( out == "-" ) {
                // write output to standard error to avoid mangling output
                // must happen early to avoid sending junk to stdout
                useStandardOutput(false);
        }
    }

    virtual void printExtraHelp(ostream& out) {
        out << "View statistics for data and journal files.\n" << endl;
    }

    int run() {
        string ns;

        // TODO allow out to be things from STDOUT
        ostream &out = cout;
        

        if ( ! hasParam( "dbpath" ) ) {
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

        string dbname = getParam ( "db" );
        Client::ReadContext cx ( dbname );
        Database * db = cx.ctx().db();

        NamespaceDetails * nsd = nsdetails( ns.c_str() );

        if ( nsd->firstExtent.isNull() ) {
            out << "ERROR: firstExtent is null" << endl;
            return -1;
        }

        if ( ! nsd->firstExtent.isValid() ) {
            out << "ERROR: firstExtent is invalid" << endl;
            return -1;
        }
        
        Extent * ex = DataFileMgr::getExtent(nsd->firstExtent);
        int extent_num = 0;

        while ( ex != 0 ) { // extent loop
            extent_num++;

            DiskLoc dl = ex->firstRecord;
            int rec_num = 0;
            int total_rec_size = 0;
            int total_bson_size = 0;

            while ( ! dl.isNull() ) { // record loop
                rec_num++;
                Record * r = dl.rec();
                //out << "\trecord " << rec_num << ": " << r->lengthWithHeaders() << endl;
                total_rec_size += r->lengthWithHeaders();
                total_bson_size += dl.obj().objsize();
                dl = r->nextInExtent( dl );
            }

            out << "extent " << extent_num << ":\n\tsize: " << ex->length 
                << "\n\tnumber of records: " << rec_num 
                << "\n\tsize used by records: " << total_rec_size 
                << "\n\tfree by records: " << ex->length - total_rec_size
                << "\n\t\% of extent used: " << (float)total_rec_size / (float)ex->length * 100 
                << "\n\taverage record size: " << total_rec_size / rec_num 
                << "\n\tsize used by BSONObjs: " << total_bson_size 
                << "\n\tfree by BSON calc: " << ex->length - total_bson_size - 16 * rec_num
                << "\n\t\% of extent used (BSON): " << (float)total_bson_size / (float)ex->length * 100 
                << "\n\taverage BSONObj size: " << total_bson_size / rec_num << endl;

            ex = ex->getNextExtent();
        }

        out << "Hello, world!" << endl;
        
        return 0;
    }
};

int main( int argc , char ** argv ) {
    Vis v;
    return v.main( argc , argv );
}
