# Forkless full sync under a mixed write load, replica loading straight from the socket
# (repl-diskless-load on-empty-db) or from disk, primary with and without IO threads. After the sync
# the replica's link must stay up (the live stream that follows the RDB must parse as RESP) and,
# after catch-up, the replica must equal the primary.

proc fsl_start_load {host port cmd {range 80000}} {
    exec src/valkey-benchmark -h $host -p $port -r $range -n 100000000 -P 8 -c 2 -q {*}$cmd > /dev/null 2>@1 &
}

proc fsl_stop_load {pids} {
    foreach pid $pids { catch {exec /bin/kill -9 $pid} }
}

set ::fsl_loads {
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

proc fsl_populate {host port} {
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

foreach {pio rio rload} [expr {[info exists ::env(FSL_CASES)] ? $::env(FSL_CASES) : {4 4 on-empty-db 4 1 on-empty-db 1 4 on-empty-db 1 1 on-empty-db 4 4 disabled}}] {
    start_server [list tags {"repl external:skip tls:skip"} overrides [list save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 io-threads $pio]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary config set client-output-buffer-limit "replica 512mb 0 0"
        fsl_populate $ph $pp
        start_server [list overrides [list save {} repl-diskless-load $rload io-threads $rio]] {
            set replica [srv 0 client]
            test "Forkless sync under load keeps the link (primary io $pio replica io $rio replica load $rload)" {
                set pids {}
                foreach cmd $::fsl_loads { lappend pids [fsl_start_load $ph $pp $cmd] }
                wait_for_condition 50 100 {
                    [status $primary connected_clients] >= 14
                } else {
                    fail "load clients did not connect"
                }
                after 300
                $primary config set rdb-key-save-delay 1
                set loglines [count_log_lines 0]
                $replica replicaof $ph $pp
                wait_for_condition 50 100 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Forkless save didn't start"
                }
                assert_equal forkless [status $primary rdb_current_bgsave_type]
                wait_for_condition 600 100 {
                    [status $replica master_link_status] eq {up}
                } else {
                    fail "Replica didn't sync"
                }
                after 1000
                fsl_stop_load $pids
                after 1500
                wait_for_condition 100 100 {
                    [status $primary master_repl_offset] eq [status $replica master_repl_offset]
                } else {
                    fail "offsets differ: primary [status $primary master_repl_offset] replica [status $replica master_repl_offset]; link [status $replica master_link_status]; digests [$primary debug digest] [$replica debug digest]; dbsize [$primary dbsize] [$replica dbsize]"
                }
                # One full sync only: the link never dropped after the first one.
                assert_equal 1 [status $primary sync_full]
                assert_equal 0 [count_log_message 0 "Receiving inline protocol from primary"]
                wait_for_condition 50 100 {
                    [$primary debug digest] eq [$replica debug digest]
                } else {
                    fail "replica differs: primary [$primary dbsize] keys, replica [$replica dbsize] keys"
                }
            }
        }
    }
}
