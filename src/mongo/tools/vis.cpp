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
        string dbname;

        // TODO allow out to be things from STDOUT
        ostream &out = cout;
        

        if ( ! hasParam( "dbpath" ) ) {
            out << "mongovis only works with --dbpath" << endl;
            return -1;
        }

        // TODO other safety checks

        // TODO don't require an active mongo process?

        try {
            ns = getNS();
        } 
        catch (...) {
            printHelp(cerr);
            return -1;
        }

        dbname = getParam ( "db" );
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

        out << DataFileMgr::getExtent(nsd->firstExtent)->length <<endl;

        out << "Hello, world!" << endl;
        
        return 0;
    }
};

int main( int argc , char ** argv ) {
    Vis v;
    return v.main( argc , argv );
}
