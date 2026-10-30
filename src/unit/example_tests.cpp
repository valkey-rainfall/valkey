#include "generated_wrappers.hpp"

extern "C" {
#include "server.h"
}

// Use a class name descriptive of your test unit
class ExampleTest : public ::testing::Test {
    // standard boilerplate supporting mocked functions
  protected:
    MockValkey mock;
    RealValkey real;

    // The SetUp() function is called before each test.
    void SetUp() override {
        memset(&server, 0, sizeof(valkeyServer));
        server.hz = CONFIG_DEFAULT_HZ;
    }

    // The TearDown() function is called after each test.
    void TearDown() override {
    }
};

// Include this (should end in "DeathTest") if testing that code asserts/dies.
using ExampleDeathTest = ExampleTest;

// Example of a DeathTest, which passes only if the code crashes.
TEST_F(ExampleDeathTest, TestSimpleDeath) {
    EXPECT_DEATH(
        {
            serverAssert(false);
        },
        "");
}

// Simple assertions test
TEST_F(ExampleTest, TestAssertions) {
    int a = 5, b = 3;
    const char *str = "hello";
    // Use EXPECT_ macros to test a condition.  If the value is not as expected, the test will fail.
    // Use ASSERT_ macros to test a condition AND immediately end the test.
    // Prefer to use EXPECT_ macros unless the test can't reasonably continue. This allows multiple
    // conditions to be tested and reported rather than ending at the first unexpected value.
    EXPECT_EQ(8, a + b);
    EXPECT_LE(b, a);
    EXPECT_GT(a, b);
    EXPECT_STREQ(str, "hello");
    ASSERT_EQ(2, a - b);
}

// Test matcher works in custom_matchers.hpp
TEST_F(ExampleTest, TestMatchers) {
    robj *robj_str = createStringObject("test", 4);
    ASSERT_NE(robj_str, nullptr); // "ASSERT" is correct here, because the test can't reasonably continue
    EXPECT_THAT(robj_str, robjEqualsStr("test"));
    decrRefCount(robj_str);
}

// Verify mocking works
TEST_F(ExampleTest, TestMocking) {
    // verifies that aeCreateTimeEvent() is called at least once in startEvictionTimeProc.
    aeTimeProc *proc = NULL;
    void *client_data = NULL;
    EXPECT_CALL(mock, aeCreateTimeEvent(_, _, _, _, _))
        .Times(AtLeast(1))
        .WillRepeatedly(DoAll(SaveArg<2>(&proc), SaveArg<3>(&client_data), Return(1)));
    startEvictionTimeProc();

    // startEvictionTimeProc() remembers that the timer is running until the timer
    // proc reports that there is nothing left to evict; a second call then creates
    // no event. Run the proc once so the next run of this test, in the same
    // process, starts from the idle state again. While loading, eviction is not
    // safe and the proc stops at once without touching the (empty) server state.
    ASSERT_NE(proc, nullptr);
    server.loading = 1;
    EXPECT_EQ(proc(server.el, 1, client_data), AE_NOMORE);
    server.loading = 0;
}
