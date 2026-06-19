// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Core/SPANN/IExtraSearcher.h"

#include <array>
#include <cstdint>

BOOST_AUTO_TEST_SUITE(VectorFilterTest)

BOOST_AUTO_TEST_CASE(BitmapVectorFilterAcceptsOnlySetBits)
{
    std::array<std::uint64_t, 2> bits = { 0, 0 };
    bits[0] = (std::uint64_t(1) << 1) | (std::uint64_t(1) << 63);
    bits[1] = (std::uint64_t(1) << 0) | (std::uint64_t(1) << 6);

    SPTAG::SPANN::BitmapVectorFilter filter(bits.data(), bits.size());

    BOOST_CHECK(!filter.Contains(-1));
    BOOST_CHECK(!filter.Contains(0));
    BOOST_CHECK(filter.Contains(1));
    BOOST_CHECK(filter.Contains(63));
    BOOST_CHECK(filter.Contains(64));
    BOOST_CHECK(!filter.Contains(65));
    BOOST_CHECK(filter.Contains(70));
    BOOST_CHECK(!filter.Contains(128));
}

BOOST_AUTO_TEST_CASE(ExtraWorkSpaceChecksDenseBitmapFastPath)
{
    std::array<std::uint64_t, 1> bits = { 0 };
    bits[0] = (std::uint64_t(1) << 2) | (std::uint64_t(1) << 5);

    SPTAG::SPANN::ExtraWorkSpace::Reset();
    SPTAG::SPANN::ExtraWorkSpace workspace;
    workspace.Initialize(16, 4, 1, 1, false);
    workspace.SetVectorFilterBitmap(bits.data(), bits.size());

    BOOST_CHECK(!workspace.CheckVectorFilter(-1));
    BOOST_CHECK(!workspace.CheckVectorFilter(1));
    BOOST_CHECK(workspace.CheckVectorFilter(2));
    BOOST_CHECK(!workspace.CheckVectorFilter(3));
    BOOST_CHECK(workspace.CheckVectorFilter(5));
    BOOST_CHECK(!workspace.CheckVectorFilter(64));

    workspace.ClearVectorFilter();
    BOOST_CHECK(workspace.CheckVectorFilter(1));
    BOOST_CHECK(workspace.CheckVectorFilter(64));
}

BOOST_AUTO_TEST_SUITE_END()
