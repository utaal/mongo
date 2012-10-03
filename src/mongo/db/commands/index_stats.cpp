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
#include "mongo/util/processinfo.h"

namespace mongo {

namespace {

    class IndexStatsCmd : public Command {
    public:
        IndexStatsCmd() : Command( "indexStats" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "TODO. Slow."; }

        //TODO(andrea.lattuada) verify this is enough
        virtual LockType locktype() const { return READ; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl);

        bool runInternal(string& errmsg, NamespaceDetails* nsd, BSONObjBuilder& result);
    } indexStatsCmd;

    bool IndexStatsCmd::run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                                BSONObjBuilder& result, bool fromRepl) {
        string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
        NamespaceDetails* nsd = nsdetails(ns.c_str());
        if (!cmdLine.quiet) {
            tlog() << "CMD: indexStats " << ns << endl;
        }
        if (!nsd) {
            errmsg = "ns not found";
            return false;
        }

        return runInternal(errmsg, nsd, result);
    }

    bool IndexStatsCmd::runInternal(string& errmsg, NamespaceDetails* nsd, BSONObjBuilder& result) {

        {
            BSONArrayBuilder indexesBuilder(result.subarrayStart("indexes"));
            for (NamespaceDetails::IndexIterator it = nsd->ii(); it.more(); ) {
                IndexDetails& details = it.next();
                indexesBuilder.append(details.indexName());
            }
        }
        return true;
    }

} // namespace

} // namespace mongo
