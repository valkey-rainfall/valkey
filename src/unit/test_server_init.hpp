/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Helpers for test suites that bring up parts of the server in SetUpTestSuite.
 *
 * All suites run in one process, and --gtest_repeat runs SetUpTestSuite again
 * for every iteration. Some server init functions are written to run once per
 * process: calling them a second time deadlocks or aborts. The helpers below
 * make the second and later calls safe. Every helper is an inline function, so
 * its function-local static is shared by all test files (C++17 [dcl.inline]).
 */

#ifndef VALKEY_UNIT_TEST_SERVER_INIT_HPP
#define VALKEY_UNIT_TEST_SERVER_INIT_HPP

extern "C" {
#include "module.h"
#include "server.h"
}

/* moduleInitModulesSystem() ends by locking the module GIL. The GIL is a plain
 * (non-recursive) mutex that the server releases only when it enters the event
 * loop. A second call in the same process blocks forever on that lock. Release
 * the lock taken by the previous call before initialising again. */
inline void unitTestInitModulesSystem(void) {
    static bool gil_locked = false;
    if (gil_locked) moduleReleaseGIL();
    moduleInitModulesSystem();
    gil_locked = true;
}

#endif
