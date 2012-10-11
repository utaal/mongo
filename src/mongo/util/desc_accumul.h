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

#include <string>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/density.hpp>
#include <boost/accumulators/statistics/extended_p_square.hpp>
#include <boost/accumulators/statistics/kurtosis.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/moment.hpp>
#include <boost/accumulators/statistics/skewness.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/array.hpp>

using namespace boost::accumulators;

namespace mongo {

    boost::array<double, 7> QUANTILES =
            {{0.02, 0.09, 0.25, 0.50, 0.75, 0.91, 0.98}};

    /**
     * Descriptive stats calculator. Cleaner, specialized facade over boost.accumulators.
     * Add values using put(val), retrieve stats using accessors or toBSONObj().
     * Density will only be available after _densityCache_ values have been inserted.
     */
    template <class T>
    class DescAccumul {
    public:
        DescAccumul(unsigned int numBins, unsigned int densityCache) :
            _acc(tag::density::cache_size = densityCache,
                 tag::density::num_bins = numBins,
                 tag::extended_p_square::probabilities = QUANTILES),
            _densityCacheToGo(densityCache), _numBins(numBins) {

            verify(numBins >= 10 && densityCache >= 10);
        }


        inline void put(T x) {
            _acc(x);
            if (_densityCacheToGo > 0) {
                --_densityCacheToGo;
            }
        }

        inline bool densityIsReady() const {
            return _densityCacheToGo <= 0;
        }

        inline int count() const { return extract::count(_acc); }
        inline double mean() const { return extract::mean(_acc); }
        inline double median() const { 
            if (hasSensibleQuantiles()) return quantile(.5);
            else if (densityIsReady()) return extract::median(_acc);
            else {
                verify(false);
                return NAN;
            }
        }
        inline double variance() const { return extract::variance(_acc); }
        inline double skewness() const { return extract::skewness(_acc); }
        inline double kurtosis() const { return extract::kurtosis(_acc); }
        double quantile(double prob) const {
            for (unsigned int i = 0; i < QUANTILES.size(); ++i) {
                if (prob == QUANTILES[i]) {
                    return extract::extended_p_square(_acc)[i];
                }
            }
            verify(false);
            return NAN;
        }
        bool hasSensibleQuantiles() const {
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

        BSONObj toBSONObj() const {
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

    private:
        accumulator_set<T, stats<tag::count,
                                 tag::density,
                                 tag::extended_p_square,
                                 tag::mean,
                                 tag::median(with_density),
                                 tag::variance,
                                 tag::kurtosis,
                                 tag::skewness> > _acc;
        unsigned int _densityCacheToGo;
        unsigned int _numBins;
    };

}

