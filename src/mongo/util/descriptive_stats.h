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

namespace _descriptive_stats {

    template <class Sample>
    class BasicEstimators {
    public:
        inline int count() const { return _count; }
        inline double mean() const { return _mean; }
        inline double stddev() const { return _stddev; }
        inline Sample min() const { return _min; }
        inline Sample max() const { return _max; }

    private:
        int _count;
        double _mean;
        double _stddev;
        Sample _min;
        Sample _max;
    };

    template <class Sample, std::size_t NumQuantiles>
    class DistributionEstimators {
    public:
        DistributionEstimators();

        DistributionEstimators& operator <<(const Sample sample);

        inline double quantile(std::size_t i) const;

    private:
        inline double _positions_increments(std::size_t i) const;

        int _count;
        enum { NumMarkers = 2 * NumQuantiles + 3 };
        double _heights[NumMarkers];              // q_i
        double _actual_positions[NumMarkers];     // n_i
        double _desired_positions[NumMarkers];    // d_i
    };

} // namespace _descriptive_stats

} // namespace mongo

#include "mongo/util/descriptive_stats.h"

namespace mongo {
    using _descriptive_stats::BasicEstimators;
    using _descriptive_stats::DistributionEstimators;
}
