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

    unsigned int VALUES[] = {2, 3, 5, 6, 8};
    const int COUNT = 5;
    // unsigned int MORE_VALUES[] = {4, 7, 8, 12, 6};
    // const int MORE_COUNT = 5;

    TEST(descriptive_stats, TestDistributionEstimators) {
        DistributionEstimators<double, 99> d;
        // DescAccumul<unsigned int> t;

        // for (int i = 0; i < COUNT; ++i) { t << VALUES[i]; }
        // ASSERT_EQUALS(t.count(), COUNT);
        // ASSERT_TRUE(areClose(t.mean(), 4.8, 1e-5));
        // ASSERT_TRUE(areClose(t.stddev(), 2.1354, 1e-3));
        // ASSERT_TRUE(areClose(t.skewness(), 0.138023, 1e-5));
        // ASSERT_TRUE(areClose(t.kurtosis(), -1.27932, 1e-5));

        for (int i = 0; i < 100000; ++i) { d << double(std::rand()) / RAND_MAX; }
        for (size_t quant = 1; quant <= 99; ++quant) {
            ASSERT_TRUE(areClose(d.quantile(quant), double(quant) / 100, .05));
            double prob = double(quant) / 100;
            ASSERT_TRUE(areClose(d.icdf(prob), prob, .05));
        }
        DEV log() << d.min() << " " << d.max() << endl;
        ASSERT_TRUE(areClose(d.min(), 0., .05));
        ASSERT_TRUE(areClose(d.max(), 1., .05));
        ASSERT_TRUE(areClose(d.median(), .5, .05));
    }

}  // namespace

