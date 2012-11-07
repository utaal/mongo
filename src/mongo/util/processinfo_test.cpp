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

#include <vector>

#include "mongo/unittest/unittest.h"
#include "mongo/util/processinfo.h"

using mongo::ProcessInfo;

namespace mongo_test {
    TEST(ProcessInfo, SysInfoIsInitialized) {
        ProcessInfo processInfo;
        if (processInfo.supported()) {
            ASSERT_FALSE(processInfo.getOsType().empty());
        }
    }

    TEST(ProcessInfo, NonZeroPageSize) {
        if (ProcessInfo::blockCheckSupported()) {
            ASSERT_GREATER_THAN(ProcessInfo::getPageSize(), 0u);
        }
    }

    const size_t PAGES = 10;

    TEST(ProcessInfo, BlockInMemoryDoesNotThrow) {
        if (ProcessInfo::blockCheckSupported()) {
            char* ptr = new char[ProcessInfo::getPageSize() * PAGES];
            ProcessInfo::blockInMemory(ptr + ProcessInfo::getPageSize() * 2);
            delete[] ptr;
        }
    }

    TEST(ProcessInfo, PagesInMemoryIsSensible) {
        if (ProcessInfo::blockCheckSupported()) {
            char* ptr = new char[ProcessInfo::getPageSize() * PAGES];
            ptr[1] = 'a';
            std::vector<bool> result(PAGES);
            ASSERT_TRUE(ProcessInfo::pagesInMemory(ptr, PAGES, &result));
            ASSERT_TRUE(result[0]);
            ASSERT_FALSE(result[2]);
            delete[] ptr;
        }
    }
}
