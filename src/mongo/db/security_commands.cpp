/*
 *    Copyright (C) 2010 10gen Inc.
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

#include "mongo/db/auth/authentication_session.h"
#include "mongo/db/auth/mongo_authentication_session.h"
#include "mongo/db/client_common.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/pdfile.h"
#include "mongo/platform/random.h"
#include "mongo/util/md5.hpp"

namespace mongo {

    /* authentication

       system.users contains
         { user : <username>, pwd : <pwd_digest>, ... }

       getnonce sends nonce to client

       client then sends { authenticate:1, nonce64:<nonce_str>, user:<username>, key:<key> }

       where <key> is md5(<nonce_str><username><pwd_digest_str>) as a string
    */

    class CmdGetNonce : public Command {
    public:
        CmdGetNonce() : Command("getnonce") {
            _random = SecureRandom::create();
        }
        
        virtual bool requiresAuth() { return false; }
        virtual bool logTheOp() { return false; }
        virtual bool slaveOk() const {
            return true;
        }
        void help(stringstream& h) const { h << "internal"; }
        virtual LockType locktype() const { return NONE; }
        bool run(const string&, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            nonce64 n = _random->nextInt64();
            stringstream ss;
            ss << hex << n;
            result.append("nonce", ss.str() );
            ClientBasic::getCurrent()->resetAuthenticationSession(
                    new MongoAuthenticationSession(n));
            return true;
        }

        SecureRandom* _random;
    } cmdGetNonce;

    CmdLogout cmdLogout;

    bool CmdAuthenticate::run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
        log() << " authenticate db: " << dbname << " " << cmdObj << endl;

        string user = cmdObj.getStringField("user");
        string key = cmdObj.getStringField("key");
        string received_nonce = cmdObj.getStringField("nonce");

        if( user.empty() || key.empty() || received_nonce.empty() ) {
            log() << "field missing/wrong type in received authenticate command "
                  << dbname
                  << endl;
            errmsg = "auth fails";
            sleepmillis(10);
            return false;
        }

        stringstream digestBuilder;

        {
            bool reject = false;
            ClientBasic *client = ClientBasic::getCurrent();
            AuthenticationSession *session = client->getAuthenticationSession();
            if (!session || session->getType() != AuthenticationSession::SESSION_TYPE_MONGO) {
                reject = true;
                LOG(1) << "auth: No pending nonce" << endl;
            }
            else {
                nonce64 nonce = static_cast<MongoAuthenticationSession*>(session)->getNonce();
                digestBuilder << hex << nonce;
                reject = digestBuilder.str() != received_nonce;
                if ( reject ) {
                    LOG(1) << "auth: Authentication failed for " << dbname << '$' << user << endl;
                }
            }
            client->resetAuthenticationSession(NULL);

            if ( reject ) {
                log() << "auth: bad nonce received or getnonce not called. could be a driver bug or a security attack. db:" << dbname << endl;
                errmsg = "auth fails";
                sleepmillis(30);
                return false;
            }
        }

        BSONObj userObj;
        string pwd;
        if (!getUserObj(dbname, user, userObj, pwd)) {
            errmsg = "auth fails";
            return false;
        }

        md5digest d;
        {
            digestBuilder << user << pwd;
            string done = digestBuilder.str();

            md5_state_t st;
            md5_init(&st);
            md5_append(&st, (const md5_byte_t *) done.c_str(), done.size());
            md5_finish(&st, d);
        }

        string computed = digestToString( d );

        if ( key != computed ) {
            log() << "auth: key mismatch " << user << ", ns:" << dbname << endl;
            errmsg = "auth fails";
            return false;
        }

        bool readOnly = userObj["readOnly"].trueValue();
        authenticate(dbname, user, readOnly );
        
        
        result.append( "dbname" , dbname );
        result.append( "user" , user );
        result.appendBool( "readOnly" , readOnly );
        

        return true;
    }

    CmdAuthenticate cmdAuthenticate;

} // namespace mongo
