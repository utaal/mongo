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

    template class DescAccumul<unsigned int>;
    template class DescAccumul<double>;

    boost::array<double, 7> QUANTILES = {{0.02, 0.09, 0.25, 0.50, 0.75, 0.91, 0.98}};

    template <class T>
    double DescAccumul<T>::median() const {
        if (hasSensibleQuantiles()) return quantile(.5);
        else if (densityIsReady()) return extract::median(_acc);
        else {
            verify(false);
            return NAN;
        }
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
          << "mean" << mean()
          << "variance" << variance()
          << "skewness" << skewness()
          << "kurtosis" << kurtosis();

        if (densityIsReady() || hasSensibleQuantiles()) {
            b << "median" << median();
        }

        if (hasSensibleQuantiles()) {
            BSONObjBuilder quantilesObjBuilder(b.subobjStart("quantiles"));
            for (unsigned int i = 0; i < QUANTILES.size(); ++i) {
                string qnt = str::stream() << QUANTILES[i];
                quantilesObjBuilder << qnt << extract::extended_p_square(_acc)[i];
            }
        }

        if (densityIsReady()) {
            BSONObjBuilder densityBuilder(b.subobjStart("density"));
            boost::iterator_range<typename vector<pair<double, double> >::iterator> rng =
                    extract::density(_acc);

            for (typename vector<pair<double, double> >::iterator it = rng.begin();
                 it != rng.end();
                 ++it) {

                string bin = str::stream() << it->first;
                densityBuilder << bin << it->second;
            }
        }
        return b.obj();
    }

}
