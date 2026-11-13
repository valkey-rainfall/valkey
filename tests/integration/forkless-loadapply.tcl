# Forkless full sync, prototype: the replica applies the inline plain SETs of the RDB stream
# (opcode 242) on its staged threads during the load. Every other inline command runs serially at
# its stream position, after the held SETs. The replica must end equal to the primary.

proc la_stat {r field} {
    return [getInfoProperty [$r info stats] $field]
}

# Mixed writes on the primary while its scan runs: plain SETs (most on new keys, so they go inline),
# SET EX (propagated as SET PXAT), INCR, HSET and DEL on held keys, MULTI/EXEC, and one SWAPDB
# right after a SET burst.
proc la_mixed_writes {primary rounds} {
    set rd [valkey_deferring_client -1]
    $rd select 9
    set n 1
    for {set i 0} {$i < $rounds} {incr i} {
        $rd set new:$i v$i
        $rd set ttl:$i t$i ex 1000
        $rd set ctr:$i 5
        $rd incr ctr:$i
        $rd set h:$i x
        $rd del h:$i
        $rd hset h:$i f $i
        $rd set tmp:$i gone
        $rd del tmp:$i
        $rd multi
        $rd set m:$i a
        $rd incr mc:$i
        $rd exec
        $rd set m:$i b
        $rd set key:[expr {$i * 7}] over$i
        incr n 15
        if {$i == $rounds / 2} {
            # SETs held just before a SWAPDB must land in db 9's old contents.
            for {set j 0} {$j < 200} {incr j} { $rd set sw:$j s$j }
            $rd swapdb 9 8
            $rd set after-swap:$i y
            incr n 202
        }
    }
    for {set i 0} {$i < $n} {incr i} { $rd read }
    $rd close
}

proc la_primary_overrides {} {
    return {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes
            repl-diskless-sync-delay 0 repl-ping-replica-period 60}
}

proc la_replica_overrides {debug} {
    return [list save {} io-threads 8 io-threads-fast-path yes keyspace-shard-bits 8 replica-parallel-apply yes \
                loading-inline-apply-debug $debug]
}

# Start a forkless sync with a slow scan; run body while the scan runs; end the scan.
proc la_sync_during {primary replica ph pp body {delay 30}} {
    $primary config set rdb-key-save-delay $delay
    $replica replicaof $ph $pp
    wait_for_condition 100 50 {
        [s -1 rdb_bgsave_in_progress] == 1
    } else {
        fail "Forkless save didn't start"
    }
    uplevel 1 $body
    assert_equal 1 [s -1 rdb_bgsave_in_progress]
    $primary config set rdb-key-save-delay 0
    wait_for_condition 500 100 {
        [s 0 master_link_status] eq {up}
    } else {
        fail "Replica didn't sync"
    }
}

foreach replica_load {on-empty-db disabled} {
    # L-1 (socket load) and L-3 (disk load from replicationCron): a plain SET load.
    start_server [list tags {"repl external:skip"} overrides [la_primary_overrides]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary select 9
        $primary debug populate 100000
        start_server [list overrides [la_replica_overrides 1]] {
            set replica [srv 0 client]
            # The test default turns on keyspace events, which keep every write serial.
            $replica config set notify-keyspace-events ""
            $replica config set repl-diskless-load $replica_load
            test "Forkless load apply: inline SETs run on the lanes during the load (load=$replica_load)" {
                set loads {}
                for {set i 0} {$i < 4} {incr i} { lappend loads [start_write_load $ph $pp 20] }
                wait_for_condition 50 100 {
                    [regexp -all {name=LOAD_HANDLER} [$primary client list]] == 4
                } else {
                    fail "Write load handlers didn't connect"
                }
                la_sync_during $primary $replica $ph $pp { after 2000 }
                after 500
                foreach l $loads { stop_write_load $l }
                wait_load_handlers_disconnected -1
                wait_for_ofs_sync $primary $replica
                assert_equal [$primary dbsize] [$replica dbsize]
                assert_equal [$primary debug digest] [$replica debug digest]
                assert_equal 0 [s -1 total_forks]
                assert_morethan [la_stat $replica loading_inline_parallel_applied] 0
                assert_morethan [la_stat $replica loading_inline_apply_phases] 0
            }
        }
    }

    # L-2 (and L-3 for the disk load): mixed inline commands; flush points keep stream order.
    start_server [list tags {"repl external:skip"} overrides [la_primary_overrides]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary select 8
        for {set i 0} {$i < 300} {incr i} { $primary set db8:$i $i }
        $primary select 9
        $primary debug populate 100000
        $primary select 0
        start_server [list overrides [la_replica_overrides 1]] {
            set replica [srv 0 client]
            $replica config set notify-keyspace-events ""
            $replica config set repl-diskless-load $replica_load
            test "Forkless load apply: mixed inline commands keep stream order (load=$replica_load)" {
                la_sync_during $primary $replica $ph $pp { la_mixed_writes $primary 600 }
                wait_for_ofs_sync $primary $replica
                assert_equal [$primary debug digest] [$replica debug digest]
                assert_morethan [la_stat $replica loading_inline_parallel_applied] 0
                assert_morethan [la_stat $replica loading_inline_serial_applied] 0
            }
        }
    }
}

# L-4: each mutation breaks one flush point, and the replica must then differ from the primary.
# A test that passes with a mutation on would not catch that bug.
foreach {mutation name} {4 "no flush before another inline command" 16 "SETs held past a SWAPDB"} {
    start_server [list tags {"repl external:skip"} overrides [la_primary_overrides]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary select 8
        for {set i 0} {$i < 300} {incr i} { $primary set db8:$i $i }
        $primary select 9
        $primary debug populate 100000
        start_server [list overrides [la_replica_overrides $mutation]] {
            set replica [srv 0 client]
            $replica config set notify-keyspace-events ""
            $replica config set repl-diskless-load on-empty-db
            test "Forkless load apply: mutation '$name' makes the replica differ" {
                la_sync_during $primary $replica $ph $pp { la_mixed_writes $primary 600 }
                wait_for_ofs_sync $primary $replica
                assert_not_equal [$primary debug digest] [$replica debug digest]
            }
        }
    }
}

# The end of the load: SETs held at the end are applied by the load itself, not left for the
# stream's next command. The RDB is below loading-process-events-interval-bytes (2 MB), so no
# progress callback flushes, and no write follows the cut, so the stream applies nothing in
# parallel. Mutation 8 (no flush at the end) must fail this check.
foreach {debug name} {1 "the load applies the SETs it holds at its end" 8 "mutation 'no flush at the end of the load' is caught"} {
    start_server [list tags {"repl external:skip"} overrides [la_primary_overrides]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary select 9
        $primary debug populate 20000
        start_server [list overrides [la_replica_overrides $debug]] {
            set replica [srv 0 client]
            $replica config set notify-keyspace-events ""
            $replica config set repl-diskless-load on-empty-db
            test "Forkless load apply: $name" {
                la_sync_during $primary $replica $ph $pp {
                    set rd [valkey_deferring_client -1]
                    $rd select 9
                    for {set i 0} {$i < 2000} {incr i} { $rd set last:$i v$i }
                    for {set i 0} {$i < 2001} {incr i} { $rd read }
                    $rd close
                } 150
                wait_for_ofs_sync $primary $replica
                set ok [expr {[$primary debug digest] eq [$replica debug digest] &&
                              [la_stat $replica repl_parallel_applied] == 0 &&
                              [la_stat $replica loading_inline_parallel_applied] > 0}]
                assert_equal [expr {$debug == 8 ? 0 : 1}] $ok
            }
        }
    }
}

# L-5: the load fails in the middle (the primary drops the link during the scan). The replica drops
# the SETs it holds, syncs again and ends equal to the primary.
start_server [list tags {"repl external:skip"} overrides [la_primary_overrides]] {
    set primary [srv 0 client]
    set ph [srv 0 host]
    set pp [srv 0 port]
    $primary select 9
    $primary debug populate 100000
    $primary select 0
    start_server [list overrides [la_replica_overrides 1]] {
        set replica [srv 0 client]
        $replica config set notify-keyspace-events ""
        $replica config set repl-diskless-load on-empty-db
        test "Forkless load apply: a failed load drops its held SETs and the next sync is equal" {
            set loglines [count_log_lines 0]
            set loads {}
            for {set i 0} {$i < 4} {incr i} { lappend loads [start_write_load $ph $pp 30] }
            wait_for_condition 50 100 {
                [regexp -all {name=LOAD_HANDLER} [$primary client list]] == 4
            } else {
                fail "Write load handlers didn't connect"
            }
            $primary config set rdb-key-save-delay 30
            $replica replicaof $ph $pp
            wait_for_condition 100 50 {
                [s -1 rdb_bgsave_in_progress] == 1
            } else {
                fail "Forkless save didn't start"
            }
            # Kill the replica link a few times until one kill lands while SETs are held.
            set dropped 0
            for {set k 0} {$k < 20 && !$dropped} {incr k} {
                wait_for_condition 100 50 {
                    [s 0 loading] == 1
                } else {
                    fail "Replica didn't start loading"
                }
                after 300
                $primary client kill type replica
                after 200
                set dropped [expr {[catch {wait_for_log_messages 0 {"*Dropped * held inline SETs*"} $loglines 1 10}] == 0}]
                wait_for_condition 100 50 {
                    [s -1 rdb_bgsave_in_progress] == 1
                } else {
                    fail "Forkless save didn't restart"
                }
            }
            assert_equal 1 $dropped
            $primary config set rdb-key-save-delay 0
            wait_for_condition 500 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replica didn't sync"
            }
            foreach l $loads { stop_write_load $l }
            wait_load_handlers_disconnected -1
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
        }
    }
}
