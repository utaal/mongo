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
 *
 *
 *    Based upon boost.accumulators (www.boost.org/libs/accumulators/),
 *    distributed under the Boost Software License, Version 1.0.
 *    See distrc/THIRD_PARTY_NOTICES for the full License Notice for Boost.
 *
 */

#pragma once

#include <algorithm>
#include <limits>

namespace mongo {

    template <class Sample>
    BasicEstimators<Sample>::BasicEstimators() :
            _count(0),
            _mean(0),
            _M2(0),
            _min(std::numeric_limits<Sample>::max()),
            _max(std::numeric_limits<Sample>::min()) {

    }

    template <class Sample>
    BasicEstimators<Sample>& BasicEstimators<Sample>::operator <<(const Sample sample) {
        _min = std::min(sample, _min);
        _max = std::max(sample, _max);

        // Online estimation of mean and variance using Knuth's algorithm
        // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
        _count++;
        const double delta = static_cast<double>(sample) - _mean;
        _mean += delta / _count;
        _M2 += delta * (static_cast<double>(sample) - _mean);

        return *this;
    }

    template <std::size_t NumQuantiles>
    DistributionEstimators<NumQuantiles>::DistributionEstimators() :
            _count(0) {

        for(std::size_t i = 0; i < NumMarkers; i++) {
            _actual_positions[i] = i + 1;
        }

        for(std::size_t i = 0; i < NumMarkers; i++) {
            _desired_positions[i] = 1.0 + (2.0 * (NumQuantiles + 1.0) * _positions_increments(i));
        }
    }

    /*
     * The quantile estimation follows the extended_p_square implementation in boost.accumulators.
     * It differs by removing the ability to request arbitrary quantiles and computing exactly
     * 'NumQuantiles' equidistant quantiles (plus minimum and maximum) instead.
     * See http://www.boost.org/doc/libs/1_51_0/doc/html/boost/accumulators/impl/extended_p_square_impl.html ,
     * R. Jain and I. Chlamtac, The P^2 algorithmus for dynamic calculation of quantiles and histograms without storing observations, Communications of the ACM, Volume 28 (October), Number 10, 1985, p. 1076-1085. and
     * K. E. E. Raatikainen, Simultaneous estimation of several quantiles, Simulation, Volume 49, Number 4 (October), 1986, p. 159-164.
     */
    template <std::size_t NumQuantiles>
    DistributionEstimators<NumQuantiles>&
    DistributionEstimators<NumQuantiles>::operator <<(const double sample) {

        // first accumulate num_markers samples
        if (_count++ < NumMarkers) {
            _heights[_count - 1] = sample;

            if (_count == NumMarkers)
            {
                std::sort(_heights, _heights + NumMarkers);
            }
        }
        else {
            std::size_t sample_cell = 1;

            // find cell k = sample_cell such that heights[k-1] <= sample < heights[k]
            if(sample < _heights[0])
            {
                _heights[0] = sample;
                sample_cell = 1;
            }
            else if (sample >= _heights[NumMarkers - 1])
            {
                _heights[NumMarkers - 1] = sample;
                sample_cell = NumMarkers - 1;
            }
            else {
                double* it = std::upper_bound(_heights,
                                              _heights + NumMarkers,
                                              sample);

                sample_cell = std::distance(_heights, it);
            }

            // update actual positions of all markers above sample_cell index
            for(std::size_t i = sample_cell; i < NumMarkers; i++) {
                _actual_positions[i]++;
            }

            // update desired positions of all markers
            for(std::size_t i = 0; i < NumMarkers; i++) {
                _desired_positions[i] += _positions_increments(i);
            }

            // adjust heights and actual positions of markers 1 to num_markers-2 if necessary
            for(std::size_t i = 1; i <= NumMarkers - 2; i++) {
                // offset to desired position
                double d = _desired_positions[i] - _actual_positions[i];

                // offset to next position
                double dp = _actual_positions[i + 1] - _actual_positions[i];

                // offset to previous position
                double dm = _actual_positions[i - 1] - _actual_positions[i];

                // height ds
                double hp = (_heights[i + 1] - _heights[i]) / dp;
                double hm = (_heights[i - 1] - _heights[i]) / dm;

                if((d >= 1 && dp > 1) || (d <= -1 && dm < -1))
                {
                    short sign_d = static_cast<short>(d / std::abs(d));

                    double h = _heights[i] + sign_d / (dp - dm) * ((sign_d - dm)*hp
                               + (dp - sign_d) * hm);

                    // try adjusting heights[i] using p-squared formula
                    if(_heights[i - 1] < h && h < _heights[i + 1])
                    {
                        _heights[i] = h;
                    }
                    else
                    {
                        // use linear formula
                        if(d > 0)
                        {
                            _heights[i] += hp;
                        }
                        if(d < 0)
                        {
                            _heights[i] -= hm;
                        }
                    }
                    _actual_positions[i] += sign_d;
                }
            }
        }

        return *this;
    }

    template <std::size_t NumQuantiles>
    inline double DistributionEstimators<NumQuantiles>::_positions_increments(std::size_t i) const {
        return static_cast<double>(i) / (2 * (NumQuantiles + 1));
    }

} // namespace mongo
