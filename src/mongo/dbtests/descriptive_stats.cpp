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
        DistributionEstimators<99> d;

        for (int i = 0; i < 100000; ++i) {
            d << double(i) / 100000;
        }
        ASSERT_TRUE(d.quantilesReady());
        for (size_t quant = 1; quant <= 99; ++quant) {
            ASSERT_EQUALS(d.probability(quant), double(quant) / 100);
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
        ASSERT_EQUALS(d.min(), 50u);
        ASSERT_EQUALS(d.max(), 100000u - 50u);
        ASSERT_TRUE(areClose(d.mean(), 100000 / 2, .01));
        ASSERT_TRUE(areClose(d.stddev(), 28838.93461, .0001));
    }

    TEST(descriptive_stats, SummaryEstimators) {
        SummaryEstimators<int, 99> d;

        for (int a = -200; a <= 200; ++a) {
            d << a;
        }
        ASSERT_TRUE(d.quantilesReady());
        ASSERT_EQUALS(d.min(), -200);
        ASSERT_EQUALS(d.max(), 200);
        ASSERT_TRUE(areClose(d.mean(), 0, .001));
        ASSERT_TRUE(areClose(d.icdf(.25), -100, 1));
    }

    TEST(descriptive_stats, DensityFromDistributionEstimators) {
        DistributionEstimators<49> d;

        for (double a = -.7; a <= .3; a += .001) {
            d << a;
        }
        ASSERT_TRUE(d.quantilesReady());

        DensityFromDistributionEstimators density(d, 1000);

        double cumulativeProbability = 0.;
        for (vector<double>::const_iterator it = density.result().begin();
             it != density.result().end();
             ++it) {

            ASSERT_TRUE(areClose(*it, 1. / 1000, 1. / 1000));
            cumulativeProbability += *it;
        }
        ASSERT_TRUE(areClose(cumulativeProbability, 1., .00001));
    }

}  // namespace

