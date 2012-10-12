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

#pragma once

#include <cmath>
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

namespace mongo {

    using boost::accumulators::accumulator_set;
    using boost::accumulators::stats;

    extern boost::array<double, 9> QUANTILES;

    /**
     * Descriptive stats calculator. Cleaner, specialized facade over boost.accumulators.
     * Add values using put(val), retrieve stats using accessors or toBSONObj().
     */
    template <class T>
    class DescAccumul {
    public:
        DescAccumul() :
                _acc(boost::accumulators::tag::extended_p_square::probabilities = QUANTILES) {
        }

        inline const DescAccumul& operator+=(T x) {
            _acc(x);
            return *this;
        }

        inline int count() const { return boost::accumulators::extract::count(_acc); }

        inline double mean() const { return boost::accumulators::extract::mean(_acc); }

        double median() const;

        inline double stddev() const {
            return std::sqrt(boost::accumulators::extract::variance(_acc));
        }

        inline double skewness() const { return boost::accumulators::extract::skewness(_acc); }

        inline double kurtosis() const { return boost::accumulators::extract::kurtosis(_acc); }

        inline double max() const { return boost::accumulators::extract::max(_acc); }

        inline double min() const { return boost::accumulators::extract::min(_acc); }

        double quantile(double prob) const;

        bool hasSensibleQuantiles() const;

        const string toString() const;

        BSONObj toBSONObj() const;

    private:
        accumulator_set<T, stats<boost::accumulators::tag::count,
                                 boost::accumulators::tag::extended_p_square,
                                 boost::accumulators::tag::mean,
                                 boost::accumulators::tag::max,
                                 boost::accumulators::tag::min,
                                 boost::accumulators::tag::variance,
                                 boost::accumulators::tag::kurtosis,
                                 boost::accumulators::tag::skewness> > _acc;
        unsigned int _numBins;
    };

}


