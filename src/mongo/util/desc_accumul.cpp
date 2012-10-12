/** @file descriptive_stats_accumulator.h
 */

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

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/util/desc_accumul.h"
#include "mongo/util/mongoutils/str.h"

using namespace boost::accumulators;
using namespace mongoutils;

namespace mongo {

    boost::array<double, 9> QUANTILES = {{.01, .02, .09, .25, .50, .75, .91, .98, .99}};

    template <class T>
    double DescAccumul<T>::median() const {
        return hasSensibleQuantiles() ? quantile(.5) : NAN;
    }

    template <class T>
    double DescAccumul<T>::quantile(double prob) const {
        for (unsigned int i = 0; i < QUANTILES.size(); ++i) {
            if (prob == QUANTILES[i]) {
                return extract::extended_p_square(_acc)[i];
            }
        }
        verify(false);
        return NAN;
    }

    template <class T>
    bool DescAccumul<T>::hasSensibleQuantiles() const {
        double prev = extract::extended_p_square(_acc)[0];
        for (unsigned int i = 1; i < QUANTILES.size(); ++i) {
            double cur = extract::extended_p_square(_acc)[i];
            if (prev > cur) {
                return false;
            }
            prev = cur;
        }
        return true;
    }

    template <class T>
    BSONObj DescAccumul<T>::toBSONObj() const {
        BSONObjBuilder b;
        b << "count" << count()
          << "mean" << mean();

        if (count() <= 1) {
            return b.obj();
        }

        b << "min" << min()
          << "max" << max()
          << "stddev" << stddev()
          << "skewness" << skewness()
          << "kurtosis" << kurtosis();

        if (hasSensibleQuantiles()) {
            b << "median" << median();
        }

        if (hasSensibleQuantiles()) {
            BSONObjBuilder quantilesObjBuilder(b.subobjStart("quantiles"));
            for (unsigned int i = 0; i < QUANTILES.size(); ++i) {
                string qnt = str::stream() << QUANTILES[i];
                quantilesObjBuilder << qnt << extract::extended_p_square(_acc)[i];
            }
        }

        return b.obj();
    }

    template class DescAccumul<unsigned int>;
    template class DescAccumul<double>;

}
