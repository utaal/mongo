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

    TEST(desc_accumul, DoNothing) {
    }

    unsigned int VALUES[] = {2, 3, 5, 6, 8};
    const int COUNT = 5;
    // unsigned int MORE_VALUES[] = {4, 7, 8, 12, 6};
    // const int MORE_COUNT = 5;

    TEST(desc_accumul, TestNominalResults) {
        DescAccumul<unsigned int> t;

        for (int i = 0; i < COUNT; ++i) { t += VALUES[i]; }
        ASSERT_EQUALS(t.count(), COUNT);
        ASSERT_TRUE(areClose(t.mean(), 4.8, 1e-5));
        ASSERT_TRUE(areClose(t.stddev(), 2.1354, 1e-3));
        ASSERT_TRUE(areClose(t.skewness(), 0.138023, 1e-5));
        ASSERT_TRUE(areClose(t.kurtosis(), -1.27932, 1e-5));

        for (int i = 0; i < 100; ++i) { t += i % 10; }
        DEV log() << t.toBSONObj().toString() << endl;
        ASSERT_TRUE(areClose(t.median(), 4.5, 1e-1));
    }

}  // namespace
