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

#pragma once

#include "mongo/db/auth/capability.h"
#include "mongo/db/auth/principal.h"

namespace mongo {

    /**
     * A representation that a given principal has the permission to perform a set of actions on a
     * specific resource.
     */
    class AcquiredCapability {
    public:

        AcquiredCapability(const Capability& capability, Principal* principal) :
            _capability(capability), _principal(principal) {}
        ~AcquiredCapability() {}

        const Principal* getPrincipal() const { return _principal; }

        const Capability& getCapability() const { return _capability; }

    private:

        Capability _capability;
        Principal* _principal;
    };

} // namespace mongo
