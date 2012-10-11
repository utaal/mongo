/**
 * Tests for mongo/util/ *
 */

#include "pch.h"

#include <cmath>
#include <limits>
#include <string>

#include "dbtests.h"
#include "mongo/util/desc_accumul.h"

namespace {

    bool areClose(double a, double b, double tolerance) {
        return std::abs(a - b) < tolerance;
    }

    TEST(DescAccumulTest, DoNothing) {
    }

    const int NUM_BINS = 10;
    const int DENSITY_CACHE = 10;

    unsigned int VALUES[] = {2, 3, 5, 6, 8};
    const int COUNT = 5;
    unsigned int MORE_VALUES[] = {4, 7, 8, 12, 6};
    const int MORE_COUNT = 5;

    TEST(DescAccumulTest, TestNominalResults) {
        DescAccumul<unsigned int> t(NUM_BINS, DENSITY_CACHE);
        for (int i = 0; i < COUNT; ++i) t.put(VALUES[i]);
        ASSERT_EQUALS(t.count(), COUNT);
        ASSERT_TRUE(areClose(t.mean(), 4.8, 1e-5));
        ASSERT_TRUE(areClose(t.variance(), 4.56, 1e-5));
        ASSERT_TRUE(areClose(t.skewness(), 0.138023, 1e-5));
        ASSERT_TRUE(areClose(t.kurtosis(), -1.27932, 1e-5));

        ASSERT_TRUE(t.densityNeedsMore());
        for (int i = 0; i < MORE_COUNT; ++i) t.put(MORE_VALUES[i]);
        ASSERT_FALSE(t.densityNeedsMore());
        DEV log() << t.toBSONObj().toString() << endl;
        ASSERT_TRUE(areClose(t.median(), 6, 1));
    }

}  // namespace