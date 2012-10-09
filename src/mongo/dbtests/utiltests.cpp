/**
 * Tests for mongo/util/ *
 */

#include "pch.h"

#include <cmath>
#include <limits>
#include <string>

#include "dbtests.h"
#include "mongo/util/descriptive_stats_accumulator.h"

namespace {

    bool areClose(double a, double b) {
        return std::fabs(a - b) < std::numeric_limits<double>::epsilon();
    }

    TEST(DescAccumulTest, DoNothing) {
    }

    const int NUM_BINS = 10;
    const int DENSITY_CACHE = 10;

    unsigned int VALUES[] = {2, 3, 5, 6, 7, 7.5};
    const int COUNT = 6;

    TEST(DescAccumulTest, TestNominalBehaviour) {
        DescAccumul<unsigned int> t(NUM_BINS, DENSITY_CACHE);
        for (int i = 0; i < COUNT; ++i) t.put(VALUES[i]);
        ASSERT_EQUALS(t.count(), COUNT);
        ASSERT_TRUE(areClose(t.mean(), 5.08333));
        ASSERT_TRUE(areClose(t.variance(), 4.84167));
        ASSERT_TRUE(areClose(t.skewness(), -0.337894));
        ASSERT_TRUE(areClose(t.kurtosis(), 1.61282));
    }

}  // namespace
