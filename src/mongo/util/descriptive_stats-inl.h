/*    Copyright 2012 10gen Inc.
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


namespace mongo {

namespace stats {

    template <class Sample, unsigned int NumQuantiles>
    DistributionEstimators::DistributionEstimators() :
            _heights({}),
            _desired_positions({}) {

        for(std::size_t i = 0; i < NumMarkers; ++i)
        {
            this->_actual_positions[i] = i + 1;
        }

        for(std::size_t i = 0; i < NumMarkers; ++i)
        {
            this->_desired_positions[i] = 1. + 2. * (NumQuantiles + 1.) * this->_positions_increments[i];
        }
    }

    private:
        template <unsigned int N>
        double _positions_increments() const { return double(N) / (2 * (NumQuantiles + 1)); }

    };

} // namespace _descriptive_stats

} // namespace mongo
