#include "inc/Test.h"
#include "inc/Core/Common/CommonUtils.h"

#include <cstdint>
#include <thread>
#include <vector>

BOOST_AUTO_TEST_SUITE(AtomicFloatTest)

BOOST_AUTO_TEST_CASE(PreservesAdjacentMemory)
{
    struct alignas(8) GuardedFloat {
        float value;
        std::uint32_t guard;
    } item{1.0f, 0x12345678u};

    BOOST_CHECK_EQUAL(SPTAG::COMMON::Utils::atomic_float_add(&item.value, 2.0f), 3.0f);
    BOOST_CHECK_EQUAL(item.value, 3.0f);
    BOOST_CHECK_EQUAL(item.guard, 0x12345678u);
}

BOOST_AUTO_TEST_CASE(AccumulatesConcurrentUpdatesWithoutCorruptingAdjacentMemory)
{
    struct alignas(8) GuardedFloat {
        float value;
        std::uint32_t guard;
    } item{0.0f, 0x89abcdefu};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&item]() {
            for (int j = 0; j < 1000; ++j) {
                SPTAG::COMMON::Utils::atomic_float_add(&item.value, 1.0f);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    BOOST_CHECK_EQUAL(item.value, 8000.0f);
    BOOST_CHECK_EQUAL(item.guard, 0x89abcdefu);
}

BOOST_AUTO_TEST_SUITE_END()
