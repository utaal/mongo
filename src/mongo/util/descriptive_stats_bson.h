/*    Copyright 2012 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use self file except in compliance with the License.
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

#include "mongo/db/jsobj.h"
#include "mongo/util/descriptive_stats.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    template <class Sample>
    void appendBasicEstimatorsToBSONObjBuilder(
            const BasicEstimators<Sample>& e,
            BSONObjBuilder& b) {

        b << "count" << static_cast<long long>(e.count())
          << "mean" << e.mean()
          << "stddev" << e.stddev()
          << "min" << e.min()
          << "max" << e.max();
    }

    /**
     * REQUIRES e.quantilesReady() == true
     */
    template <std::size_t NumQuantiles>
    void appendQuantilesToBSONArrayBuilder(
            const DistributionEstimators<NumQuantiles>& e,
            BSONArrayBuilder& arr) {

        verify(e.quantilesReady());

        for (std::size_t i = 0; i <= NumQuantiles + 1; i++) {
            arr << e.quantile(i);
        }
    }

    template <class Sample, std::size_t NumQuantiles>
    inline BSONObj statisticSummaryToBSONObj(const SummaryEstimators<Sample, NumQuantiles>& e) {
        BSONObjBuilder b;
        appendBasicEstimatorsToBSONObjBuilder(e, b);
        if (e.quantilesReady()) {
            BSONObjBuilder quantilesBuilder(b.subobjStart("quantiles"));
            for (size_t i = 1; i <= NumQuantiles; i++) {
                quantilesBuilder.append(std::string(mongoutils::str::stream() << e.probability(i)),
                                        e.quantile(i));
            }
            quantilesBuilder.doneFast();
        }
        return b.obj();
    }

}
