/**
 * Tests for mongo/util/descriptive_stats.h
 */

#include "mongo/pch.h"

#include <cstdlib>
#include <cmath>
#include <limits>
#include <string>

#include "mongo/unittest/unittest.h"
#include "mongo/util/descriptive_stats.h"
#include "mongo/util/descriptive_stats_bson.h"

using namespace std;

namespace {

    const int NumQuantiles = 99;

    TEST(BasicEstimators, TestBSONOutput) {
        mongo::BasicEstimators<unsigned int> b;

        for (int i = 0; i < 10000; i++) {
            b << i;
        }

        mongo::BSONObjBuilder builder;
        mongo::appendBasicEstimatorsToBSONObjBuilder(b, builder);
        mongo::BSONObj obj = builder.obj();

        ASSERT_EQUALS(obj["count"].Number(), b.count());
        ASSERT_EQUALS(obj["mean"].Number(), b.mean());
        ASSERT_EQUALS(obj["stddev"].Number(), b.stddev());
        ASSERT_EQUALS(obj["min"].Number(), b.min());
        ASSERT_EQUALS(obj["max"].Number(), b.max());
    }

    TEST(DistributionEstimators, TestBSONOutput) {
        mongo::DistributionEstimators<NumQuantiles> d;

        for (int i = 0; i < 10000; i++) {
            d << static_cast<double>(i) / 10000;
        }

        mongo::BSONArrayBuilder arrayBuilder;
        mongo::appendQuantilesToBSONArrayBuilder(d, arrayBuilder);
        mongo::BSONArray arr = arrayBuilder.arr();

        for (size_t i = 0; i <= NumQuantiles + 1; i++) {
            ASSERT_EQUALS(arr[i].Number(), d.quantile(i));
        }
    }

    TEST(SummaryEstimators, TestBSONOutput) {
        mongo::SummaryEstimators<double, NumQuantiles> e;

        for (int i = 0; i < 10000; i++) {
            e << static_cast<double>(i) / 100;
        }
        verify(e.quantilesReady());

        mongo::BSONObj obj = mongo::statisticSummaryToBSONObj(e);

        ASSERT_EQUALS(obj["count"].Number(), e.count());
        ASSERT_EQUALS(obj["mean"].Number(), e.mean());
        ASSERT_EQUALS(obj["stddev"].Number(), e.stddev());
        ASSERT_EQUALS(obj["min"].Number(), e.min());
        ASSERT_EQUALS(obj["max"].Number(), e.max());

        mongo::BSONObj quantiles = obj["quantiles"].Obj();
        ASSERT_EQUALS(quantiles.nFields(), NumQuantiles);
        for (mongo::BSONObjIterator it = quantiles.begin(); it.more(); ++it) {
            ASSERT_EQUALS((*it).Number(), e.icdf(atof((*it).fieldName())));
        }
    }

}  // namespace

