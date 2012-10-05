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

namespace mongo {

    template <class T>
    class DescAccumul {
    public:
        DescAccumul(T lower, T upper, int numBins) :
            _lower(lower), _upper(upper), _numBins(numBins), _bins(numBins), _clipped(false) {

        }

        /*DescAccumul& operator-=(const DescAccumul &rhs);*/

        void put(T x);
        //void takeOut(T x);

        inline int clippedOver() const {
            return _clipped;
        }
        inline int clippedUnder() const {
            return _clipped;
        }
        T quant(int num) const;
        inline T density(int num) const { return _bins.at(num); }
        T median() const;
        inline int count() const { return _count; }
        inline double mean() const { return _mean; }
        inline double variance() const { return _variance; }
        inline T skewness() const { return _skewness; }
        inline T kurtosis() const { return _kurtosis; }
        //T moment(int num);
        //
    private:
        inline int binFor(T val) const {
            return (val - lower) / ((double) (_upper - _lower) / _numBins);
        }

        T _lower;
        T _upper;
        int _numBins;
        vector<int> _bins;
        int _clippedOver;
        int _clippedUnder;
        int _count;
        T _mean;
        T _variance;
        T _skewness;
        T _kurtosis;
    };

}
