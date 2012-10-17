/**
 * Tests for mongo/util/descriptive_stats.h
 */

#include "pch.h"

#include <cstdlib>
#include <cmath>
#include <limits>
#include <string>

#include "dbtests.h"
#include "mongo/util/descriptive_stats.h"

namespace {

    bool areClose(double a, double b, double tolerance) {
        return std::abs(a - b) < tolerance;
    }

    TEST(descriptive_stats, DoNothing) {
    }

    TEST(descriptive_stats, TestDistributionEstimators) {
        DistributionEstimators<double, 99> d;

        for (int i = 0; i < 100000; ++i) {
            d << double(i) / 100000;
        }
        for (size_t quant = 1; quant <= 99; ++quant) {
            ASSERT_TRUE(areClose(d.quantile(quant), double(quant) / 100, .05));
            double prob = double(quant) / 100;
            ASSERT_TRUE(areClose(d.icdf(prob), prob, .05));
        }
        ASSERT_TRUE(areClose(d.min(), 0., .05));
        ASSERT_TRUE(areClose(d.max(), 1., .05));
        ASSERT_TRUE(areClose(d.median(), .5, .05));
    }

    TEST(descriptive_stats, TestBasicEstimators) {
        BasicEstimators<unsigned int> d;

        // [50, 51, 52, ..., 99949, 99950]
        for (int i = 50; i <= 100000 - 50; ++i) {
            d << unsigned(i);
        }
        ASSERT_EQUALS(d.min(), 50);
        ASSERT_EQUALS(d.max(), 100000 - 50);
        ASSERT_TRUE(areClose(d.mean(), 100000 / 2, .01));
        ASSERT_TRUE(areClose(d.stddev(), 28838.93461, .0001));
    }

}  // namespace

