// @file storage_details.cpp

/**
 */

#include "mongo/db/commands.h"

namespace mongo {

    namespace {

        class StorageDetailsCmd : public Command {
        public:
            StorageDetailsCmd() : Command( "storageDetails" ) {}

            virtual bool slaveOk() const {
                return true;
            }

            virtual void help(stringstream& h) const { h << "TODO. Slow."; }

            virtual LockType locktype() const { return READ; }
            //{ validate: "collectionnamewithoutthedbpart" [, scandata: <bool>] [, full: <bool> } */

            bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg,
                     BSONObjBuilder& result, bool fromRepl ) {
                result.append("dummy", "yes");
                return true;
            }
        } storageDetailsCmd;

    }  // namespace

}  // namespace mongo

