# REPRO for the stale curCmdMissingKeys sibling of valkey-io/valkey#5205 (not a PR).
#
# Requires the forkless full-sync branch (PR #4797) with #5205 applied, since only
# that branch creates a BGITERATOR_CONSISTENCY_EVENTUAL iterator from a running
# server.  Run with:  ./runtest --single integration/probe-5205 --timeout 120
#
# Observed on fc911fb41 + 06970860a (2026-10-30): cases 1-3 pass, case 4 aborts
# the primary with "bgiteration.c:2550 'wasAdded' is not true", backtrace
# getCommand -> lookupKey -> deleteExpiredKeyAndPropagateWithDictIndex -> ...
# -> bgIteration_handleCommandReplication -> _serverAssert.
# Without #5205, case 2 already aborts at :2546 (the bug #5205 fixed).


proc primary_alive {primary} {
    if {[catch {$primary ping} e]} { return 0 }
    return 1
}

start_server {tags {"repl external:skip"} overrides {forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 save "" appendonly no}} {
    start_server {overrides {save "" appendonly no}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        populate 2000 key: 10 -1
        $primary config set rdb-key-save-delay 5000 ;# 5 ms per key -> ~10 s of iteration

        set loglines [count_log_lines -1]
        $replica replicaof $primary_host $primary_port
        wait_for_log_messages -1 {"*method: forkless*"} $loglines 100 100
        wait_for_condition 50 100 {
            [s -1 rdb_bgsave_in_progress] eq 1
        } else {
            fail "forkless full sync did not start"
        }

        test "PROBE: plain SET of a new key during forkless full sync" {
            $primary set plainnew v
            assert_equal 1 [primary_alive $primary]
        }

        test "PROBE: MULTI/EXEC creating a new key during forkless full sync (5205 case)" {
            $primary multi
            $primary set newkey v
            $primary exec
            assert_equal 1 [primary_alive $primary]
            assert_equal v [$primary get newkey]
        }

        test "PROBE: EVAL creating a new key during forkless full sync (5205 case)" {
            $primary eval {redis.call('set', 'newkey2', 'v')} 0
            assert_equal 1 [primary_alive $primary]
        }

        test "PROBE: SET new key, then lazy expire of another key on a READ (stale missing list)" {
            $primary set expk v px 1
            $primary set newkey3 v     ;# new key: replicated, added to early_iterate_entries
            after 50                   ;# let expk expire (probe only)
            catch {$primary get expk} e ;# read -> lazy expire -> DEL propagated to bgIteration
            assert_equal 1 [primary_alive $primary]
        }

        test "PROBE: sync completes" {
            $primary config set rdb-key-save-delay 0
            wait_for_sync $replica 200 100
            assert_equal 1 [primary_alive $primary]
        }
    }
}
