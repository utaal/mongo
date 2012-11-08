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
 *
 *    Boost Software License - Version 1.0 - August 17th, 2003
 *
 *    Permission is hereby granted, free of charge, to any person or organization
 *    obtaining a copy of the software and accompanying documentation covered by
 *    this license (the "Software") to use, reproduce, display, distribute,
 *    execute, and transmit the Software, and to prepare derivative works of the
 *    Software, and to permit third-parties to whom the Software is furnished to
 *    do so, all subject to the following:
 *
 *    The copyright notices in the Software and this entire statement, including
 *    the above license grant, this restriction and the following disclaimer,
 *    must be included in all copies of the Software, in whole or in part, and
 *    all derivative works of the Software, unless such copies or derivative
 *    works are solely in the form of machine-executable object code generated by
 *    a source language processor.
 *
 *    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *    FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
 *    SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
 *    FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
 *    ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *    DEALINGS IN THE SOFTWARE.
 */

#pragma once

namespace mongo {

// avoid namespace pollution
namespace _descriptive_stats {

    #include <algorithm>

    template <class Sample>
    BasicEstimators<Sample>::BasicEstimators() : _count(0) {
    }

    template <class Sample>
    BasicEstimators<Sample>& BasicEstimators<Sample>::operator <<(const Sample sample) {

        if (this->_count++ == 0) {
            this->_min = sample;
            this->_max = sample;
            this->_mean = double(sample);
            this->_variance = 0.;
            return *this;
        }

        this->_min = std::min(sample, this->_min);
        this->_max = std::max(sample, this->_max);

        // count already incremented

        // iterative calculation of the mean
        this->_mean = double(this->_mean * (this->_count - 1) + sample) / this->_count;
        double tmp = sample - this->_mean;

        // iterative calculation of the variance
        // (for the recurrence used refer to
        //  http://www.boost.org/doc/libs/1_51_0/doc/html/boost/accumulators/impl/variance_impl.html)
        this->_variance = double(this->_variance * (this->_count - 1)) / this->_count +
                          double(tmp * tmp) / (this->_count - 1);

        return *this;
    }

    template <std::size_t NumQuantiles>
    DistributionEstimators<NumQuantiles>::DistributionEstimators() :
            _count(0) {

        for(std::size_t i = 0; i < NumMarkers; ++i)
        {
            this->_actual_positions[i] = i + 1;
        }

        for(std::size_t i = 0; i < NumMarkers; ++i)
        {
            this->_desired_positions[i] =
                        1. + 2. * (NumQuantiles + 1.) * this->_positions_increments(i);
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
            this->_heights[_count - 1] = sample;

            if (_count == NumMarkers)
            {
                std::sort(this->_heights, this->_heights + NumMarkers);
            }
        }
        else {
            std::size_t sample_cell = 1;

            // find cell k = sample_cell such that heights[k-1] <= sample < heights[k]
            if(sample < this->_heights[0])
            {
                this->_heights[0] = sample;
                sample_cell = 1;
            }
            else if (sample >= this->_heights[NumMarkers - 1])
            {
                this->_heights[NumMarkers - 1] = sample;
                sample_cell = NumMarkers - 1;
            }
            else {
                double* it = std::upper_bound(this->_heights,
                                              this->_heights + NumMarkers,
                                              sample);

                sample_cell = std::distance(this->_heights, it);
            }

            // update actual positions of all markers above sample_cell index
            for(std::size_t i = sample_cell; i < NumMarkers; ++i)
            {
                ++this->_actual_positions[i];
            }

            // update desired positions of all markers
            for(std::size_t i = 0; i < NumMarkers; ++i)
            {
                this->_desired_positions[i] += this->_positions_increments(i);
            }

            // adjust heights and actual positions of markers 1 to num_markers-2 if necessary
            for(std::size_t i = 1; i <= NumMarkers - 2; ++i)
            {
                // offset to desired position
                double d = this->_desired_positions[i] - this->_actual_positions[i];

                // offset to next position
                double dp = this->_actual_positions[i + 1] - this->_actual_positions[i];

                // offset to previous position
                double dm = this->_actual_positions[i - 1] - this->_actual_positions[i];

                // height ds
                double hp = (this->_heights[i + 1] - this->_heights[i]) / dp;
                double hm = (this->_heights[i - 1] - this->_heights[i]) / dm;

                if((d >= 1 && dp > 1) || (d <= -1 && dm < -1))
                {
                    short sign_d = static_cast<short>(d / std::abs(d));

                    double h = this->_heights[i] + sign_d / (dp - dm) * ((sign_d - dm)*hp
                               + (dp - sign_d) * hm);

                    // try adjusting heights[i] using p-squared formula
                    if(this->_heights[i - 1] < h && h < this->_heights[i + 1])
                    {
                        this->_heights[i] = h;
                    }
                    else
                    {
                        // use linear formula
                        if(d > 0)
                        {
                            this->_heights[i] += hp;
                        }
                        if(d < 0)
                        {
                            this->_heights[i] -= hm;
                        }
                    }
                    this->_actual_positions[i] += sign_d;
                }
            }
        }

        return *this;
    }

    template <std::size_t NumQuantiles>
    inline double DistributionEstimators<NumQuantiles>::_positions_increments(std::size_t i) const {
        return double(i) / (2 * (NumQuantiles + 1));
    }

} // namespace _descriptive_stats

} // namespace mongo
