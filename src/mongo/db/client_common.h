// client_common.h

/**
*    Copyright (C) 2008 10gen Inc.
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

#pragma once

#include <boost/scoped_ptr.hpp>

#include "mongo/db/auth/authentication_session.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/message_port.h"

namespace mongo {

    class AuthenticationInfo;

    /**
     * this is the base class for Client and ClientInfo
     * Client is for mongod
     * ClientInfo is for mongos
     * They should converge slowly
     * The idea is this has the basic api so that not all code has to be duplicated
     */
    class ClientBasic : boost::noncopyable {
    public:
        virtual ~ClientBasic(){}
        virtual const AuthenticationInfo * getAuthenticationInfo() const = 0;
        virtual AuthenticationInfo * getAuthenticationInfo() = 0;
        AuthenticationSession* getAuthenticationSession() { return _authenticationSession.get(); }
        void resetAuthenticationSession(AuthenticationSession* newSession) {
            _authenticationSession.reset(newSession);
        }
        void swapAuthenticationSession(boost::scoped_ptr<AuthenticationSession>& other) {
            _authenticationSession.swap(other);
        }
        AuthorizationManager* getAuthorizationManager() {
            massert(16481,
                    "No AuthorizationManager has been set up for this connection",
                    _authorizationManager != NULL);
            return _authorizationManager.get();
        }
        // setAuthorizationManager must be called in the initialization of any ClientBasic that
        // corresponds to an incoming client connection.
        void setAuthorizationManager(AuthorizationManager* authorizationManager) {
            massert(16477,
                    "An AuthorizationManager has already been set up for this connection",
                    _authorizationManager == NULL);
            _authorizationManager.reset(authorizationManager);
        }
        bool getIsLocalHostConnection() { return getRemote().isLocalHost(); }

        virtual bool hasRemote() const { return _messagingPort; }
        virtual HostAndPort getRemote() const {
            verify( _messagingPort );
            return _messagingPort->remote();
        }
        AbstractMessagingPort * port() const { return _messagingPort; }

        static ClientBasic* getCurrent();

    protected:
        ClientBasic(AbstractMessagingPort* messagingPort) : _messagingPort(messagingPort) {}

    private:
        boost::scoped_ptr<AuthenticationSession> _authenticationSession;
        boost::scoped_ptr<AuthorizationManager> _authorizationManager;
        AbstractMessagingPort* const _messagingPort;
    };
}
