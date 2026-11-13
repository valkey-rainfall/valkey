# Forkless full sync with write lanes on (prototype, candidate B): the primary runs single-key
# writes on its lanes during the sync, applying the sync's per-key rule there. After the sync and
# catch-up the replica must equal the primary. The load is pipelined valkey-benchmark clients (the
# fast path admits clients without a name, so the stock write-load helper never reaches the lanes).

proc fkl_start_load {host port cmd {range 80000}} {
    exec src/valkey-benchmark -h $host -p $port -r $range -n 100000000 -P 8 -c 2 -q {*}$cmd > /dev/null 2>@1 &
}

proc fkl_fp {r field} {
    getInfoProperty [$r info fastpath] $field
}

proc fkl_stop_load {pids} {
    foreach pid $pids { catch {exec /bin/kill -9 $pid} }
}

# Single-key commands of every lane kind, on existing keys (k:, c:, h:, l:, s:) and new ones, deletes,
# expiry (key TTLs and hash-field TTLs short enough to fire during the run), lazy expiry by reads,
# and multi-key commands that READ sends to the main path.
set ::fkl_loads {
    {SET k:__rand_int__ v__rand_int__}
    {INCR c:__rand_int__}
    {HSET h:__rand_int__ f__rand_int__ v}
    {LPUSH l:__rand_int__ x}
    {RPOP l:__rand_int__}
    {SADD s:__rand_int__ m__rand_int__}
    {DEL k:__rand_int__}
    {PEXPIRE k:__rand_int__ 300}
    {GET k:__rand_int__}
    {HSETEX h:__rand_int__ PX 200 FIELDS 1 e__rand_int__ v}
    {HGET h:__rand_int__ e__rand_int__}
    {MSET k:__rand_int__ a k:__rand_int__ b}
    {SMOVE s:__rand_int__ s:__rand_int__ m__rand_int__}
}

proc fkl_populate {host port} {
    foreach cmd {
        {SET k:__rand_int__ v}
        {INCR c:__rand_int__}
        {HSET h:__rand_int__ f v}
        {RPUSH l:__rand_int__ a b c}
        {SADD s:__rand_int__ m}
    } {
        exec src/valkey-benchmark -h $host -p $port -r 50000 -n 60000 -P 64 -c 8 -q {*}$cmd > /dev/null 2>@1
    }
}

foreach gate {off 25 1} {
    start_server {tags {"repl external:skip tls:skip"} overrides {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}}} {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        if {$gate eq "off"} {
            $primary config set forkless-lanes no
        } else {
            $primary config set forkless-lanes-gate-pct $gate
        }
        # No replica limit: a replica that loads for seconds while the primary writes at lane rates
        # would be dropped after the cut (the receiver bound of design.md section 3). The gate then
        # reads 256 MB as the limit.
        $primary config set client-output-buffer-limit "replica 0 0 0"
        fkl_populate $ph $pp
        start_server {overrides {save {} repl-diskless-load disabled io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}}} {
            set replica [srv 0 client]
            test "Forkless sync with write lanes: replica equals primary (gate $gate percent)" {
                set pids {}
                foreach cmd $::fkl_loads { lappend pids [fkl_start_load $ph $pp $cmd] }
                wait_for_condition 50 100 {
                    [fkl_fp $primary fastpath_clients] >= 20
                } else {
                    fail "load clients did not join the fast path"
                }
                after 300
                # About 1 us per key: the scan takes some seconds, so most of the load runs beside it.
                $primary config set rdb-key-save-delay 1
                $replica replicaof $ph $pp
                wait_for_condition 50 100 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Forkless save didn't start"
                }
                wait_for_condition 600 100 {
                    [status $replica master_link_status] eq {up}
                } else {
                    fail "Replica didn't sync"
                }
                after 1000
                fkl_stop_load $pids
                wait_for_condition 50 100 {
                    [status $primary connected_clients] <= 3
                } else {
                    fail "load clients still connected"
                }
                # Let short TTLs fire on the primary and their deletes reach the replica. The replica
                # catches up on what the primary wrote while it loaded, which takes seconds at lane rates.
                after 1500
                set t0 [clock milliseconds]
                wait_for_condition 1200 100 {
                    [status $primary master_repl_offset] eq [status $replica master_repl_offset]
                } else {
                    fail "replica offset didn't match in 120 s"
                }
                set catchup [expr {[clock milliseconds] - $t0}]
                wait_for_condition 50 100 {
                    [$primary debug digest] eq [$replica debug digest]
                } else {
                    fail "replica differs: primary [$primary dbsize] keys, replica [$replica dbsize] keys"
                }
                assert_equal 0 [status $primary total_forks]
                set lw [fkl_fp $primary fastpath_forkless_lane_writes]
                set li [fkl_fp $primary fastpath_forkless_lane_inline]
                set lr [fkl_fp $primary fastpath_forkless_routed]
                set lg [fkl_fp $primary fastpath_forkless_gate_on]
                puts "gate=$gate lane_writes=$lw lane_inline=$li routed=$lr gate_on=$lg catchup_ms=$catchup"
                if {$gate ne "off"} {
                    assert_morethan $lw 0
                    assert_morethan $li 0
                    assert_morethan $lr 0
                }
                if {$gate eq "1"} { assert_morethan $lg 0 }
            }
        }
    }
}

# A gate that never vetoes and a small replica limit: the inline queue passes the limit, the replica is
# dropped and the sync starts again; with the load stopped the next sync completes.
start_server {tags {"repl external:skip tls:skip"} overrides {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}}} {
    set primary [srv 0 client]
    set ph [srv 0 host]
    set pp [srv 0 port]
    $primary config set forkless-lanes-gate-pct 10000
    $primary config set client-output-buffer-limit "replica 16mb 0 0"
    fkl_populate $ph $pp
    start_server {overrides {save {} repl-diskless-load disabled io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}}} {
        set replica [srv 0 client]
        test "Forkless sync with write lanes: a replica dropped for the inline queue syncs again" {
            set pids {}
            foreach cmd $::fkl_loads { lappend pids [fkl_start_load $ph $pp $cmd] }
            wait_for_condition 50 100 {
                [fkl_fp $primary fastpath_clients] >= 20
            } else {
                fail "load clients did not join the fast path"
            }
            $primary config set rdb-key-save-delay 1
            $replica replicaof $ph $pp
            wait_for_condition 300 100 {
                [status $primary client_output_buffer_limit_disconnections] > 0
            } else {
                fail "replica was not dropped"
            }
            fkl_stop_load $pids
            $primary config set rdb-key-save-delay 0
            wait_for_condition 600 100 {
                [status $replica master_link_status] eq {up}
            } else {
                fail "Replica didn't sync"
            }
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal 0 [status $primary total_forks]
            assert_morethan [status $primary sync_full] 1
        }
    }
}
