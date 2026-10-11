#include "inc/Test.h"
#include "inc/Core/Common.h"
#include "inc/Core/Common/Dataset.h"
#include "inc/Core/Common/VersionLabel.h"

#include <cstdint>

BOOST_AUTO_TEST_SUITE(DatasetLifetimeTest)

BOOST_AUTO_TEST_CASE(DestroysAnUninitializedDataset)
{
    SPTAG::COMMON::Dataset<std::uint8_t> dataset;
    BOOST_CHECK(!dataset.IsReady());
    BOOST_CHECK_EQUAL(dataset.R(), 0);
}

BOOST_AUTO_TEST_CASE(DestroysAnUninitializedVersionLabel)
{
    SPTAG::COMMON::VersionLabel label;
    BOOST_CHECK_EQUAL(label.GetVectorNum(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
