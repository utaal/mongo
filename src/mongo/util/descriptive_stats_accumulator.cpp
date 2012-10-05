/** @file descriptive_stats_accumulator.cpp
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

namespace mongo {

    template <class T>
    void DescAccumul::put(T x) {
        int bin = _bins.at(binFor(x));
        if (bin < 0) _clippedUnder += 1;
        else if (bin >= _numBins) _clippedOver += 1;
        else _bins.at(binFor(x)) += 1;

        _mean = _mean + ((double) x - _mean) / (_count + 1);

        double delta = x - _mean
        _variance = _variance + (delta * delta - variance) / (_count + 1);

        _count = _count + 1;
    }

    //template <class T>
    //void DescAccumul::takeOut(T x) {
    //    int bin = _bins.at(binFor(x));
    //    if (bin < 0) _clippedUnder -= 1;
    //    else if (bin >= _numBins) _clippedOver -= 1;
    //    else _bins.at(binFor(x)) -= 1;

    //    _mean = (_mean * _count - (double) x) / (_count - 1);

    //    _count = _count - 1;
    //}

    //template <class T>
    //DescAccumul<T>& DescAccumul::operator+=(const DescAccumul<T> &rhs) {
    //}

    //template <class T>
    //DescAccumul<T>& DescAccumul::operator-=(const DescAccumul<T> &rhs) {
    //}
}
