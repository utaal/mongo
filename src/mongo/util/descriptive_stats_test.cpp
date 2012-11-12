/**
 * Tests for mongo/util/descriptive_stats.h
 */

#include "pch.h"

#include <cstdlib>
#include <cmath>
#include <limits>
#include <string>

#include "mongo/unittest/unittest.h"
#include "mongo/util/descriptive_stats.h"

namespace {

    TEST(DistributionEstimators, TestNominalResults) {
        DistributionEstimators<99> d;

        for (int i = 0; i < 100000; ++i) {
            d << double(i) / 100000;
        }
        ASSERT_TRUE(d.quantilesReady());
        for (size_t quant = 1; quant <= 99; ++quant) {
            ASSERT_EQUALS(d.probability(quant), double(quant) / 100);
            ASSERT_CLOSE(d.quantile(quant), double(quant) / 100, 0.05)
            double prob = double(quant) / 100;
            ASSERT_CLOSE(d.icdf(prob), prob, 0.05);
        }
        ASSERT_CLOSE(d.min(), 0.0, 0.05);
        ASSERT_CLOSE(d.max(), 1.0, 0.05);
        ASSERT_CLOSE(d.median(), 0.5, 0.05);
    }

    TEST(BasicEstimators, TestNominalResults) {
        BasicEstimators<unsigned int> d;

        // [50, 51, 52, ..., 99949, 99950]
        for (int i = 50; i <= 100000 - 50; ++i) {
            d << unsigned(i);
        }
        ASSERT_EQUALS(d.min(), 50u);
        ASSERT_EQUALS(d.max(), 100000u - 50u);
        ASSERT_CLOSE(d.mean(), 100000 / 2, 0.01);
        ASSERT_CLOSE(d.stddev(), 28838.9346, 0.0001);
    }

    TEST(SummaryEstimators, TestNominalResults) {
        SummaryEstimators<int, 99> d;

        for (int a = -200; a <= 200; ++a) {
            d << a;
        }
        ASSERT_TRUE(d.quantilesReady());
        ASSERT_EQUALS(d.min(), -200);
        ASSERT_EQUALS(d.max(), 200);
        ASSERT_CLOSE(d.mean(), 0, 0.001);
        ASSERT_CLOSE(d.icdf(.25), -100, 1);
    }

}  // namespace

