/** @file math.h */

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

#pragma once

namespace mongoutils {

    /**
     * @return the result of division rounded up to next integer, avoids conversion to floating
     *         point
     * NOTE: may overflow without warning, use with caution
     */
    inline unsigned ceilingDiv(unsigned dividend, unsigned divisor) {
        return (dividend + divisor - 1) / divisor;
    }

}