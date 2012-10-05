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
#include <boost/accumulators/statistics/kurtosis.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/moment.hpp>
#include <boost/accumulators/statistics/skewness.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>

using namespace boost::accumulators;

namespace mongo {

    /**
     * Descriptive stats calculator. Cleaner, specialized facade over boost.accumulators.
     * Add values using put(val), retrieve stats using accessors or toBSONObj().
     * Density will only be available after _densityCache_ values have been inserted.
     */
    template <class T>
    class DescAccumul {
    public:
        DescAccumul(unsigned int numBins, unsigned int densityCache) :
            _acc(tag::density::cache_size = densityCache, tag::density::num_bins = numBins),
            _densityCacheToGo(densityCache), _numBins(numBins) {

        }

        inline void put(T x) {
            _acc(x);
            if (_densityCacheToGo > 0) {
                --_densityCacheToGo;
            }
        }

        inline bool densityNeedsMore() const {
            return _densityCacheToGo > 0;
        }


        // T quant(int num) const;
        //inline std::vector density(int num) const {

        inline int count() const { return extract::count(_acc); }
        inline double mean() const { return extract::mean(_acc); }
        inline double median() const { return extract::median(_acc); }
        inline double variance() const { return extract::variance(_acc); }
        inline double skewness() const { return extract::skewness(_acc); }
        inline double kurtosis() const { return extract::kurtosis(_acc); }

        BSONObj toBSONObj() const {
            BSONObjBuilder b;
            b << "count" << count()
              << "mean" << mean()
              << "variance" << variance()
              << "skewness" << skewness()
              << "kurtosis" << kurtosis();

            if (!densityNeedsMore()) {
                b << "median" << median();

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
                                 tag::mean,
                                 tag::median(with_density),
                                 tag::variance,
                                 tag::kurtosis,
                                 tag::skewness> > _acc;
        unsigned int _densityCacheToGo;
        unsigned int _numBins;
    };

}
