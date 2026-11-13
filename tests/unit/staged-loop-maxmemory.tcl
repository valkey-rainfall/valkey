# Write lanes with maxmemory set (maxmemory-lanes yes, a prototype setting): a segment runs on the
# lanes only when its estimated growth fits under the limit with a margin; near the limit main
# pre-evicts with the stock loop or runs the segment serially, where stock code evicts and answers
# OOM. Fast-path clients here are raw db-0 sockets.

proc mml_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

proc mml_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc mml_read_line {fd} {
    return [string trimright [gets $fd] "\r"]
}

proc mml_fp {field} {
    return [getInfoProperty [r info fastpath] $field]
}

# Pipelined writes from several raw clients at once: round r of client i sends $n commands built by
# $cmdproc {i r k}. Returns the list of replies in send order per client, flattened.
proc mml_drive {nclients rounds n cmdproc} {
    set fds {}
    for {set i 0} {$i < $nclients} {incr i} { lappend fds [mml_socket] }
    set replies {}
    for {set r 0} {$r < $rounds} {incr r} {
        set i 0
        foreach fd $fds {
            set buf ""
            for {set k 0} {$k < $n} {incr k} { append buf [{*}$cmdproc $i $r $k] }
            puts -nonewline $fd $buf
            flush $fd
            incr i
        }
        foreach fd $fds {
            for {set k 0} {$k < $n} {incr k} { lappend replies [mml_read_line $fd] }
        }
    }
    foreach fd $fds { close $fd }
    return $replies
}

proc mml_set_new {val ttl i r k} {
    if {$ttl} { return [mml_resp set mml:$i:$r:$k $val ex 100000] }
    return [mml_resp set mml:$i:$r:$k $val]
}

proc mml_count {replies pattern} {
    set n 0
    foreach rep $replies { if {[string match $pattern $rep]} { incr n } }
    return $n
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 maxmemory-lanes yes save ""}} {
    set val [string repeat x 1000]
    # The test default turns on keyspace events, which keep every write serial; the raw clients use db 0.
    r config set notify-keyspace-events ""
    r select 0

    test {maxmemory lanes: lanes run with maxmemory set far above the dataset} {
        r flushall
        r config set maxmemory-policy allkeys-lru
        r config set maxmemory 1gb
        set lanes [mml_fp fastpath_write_lane_entries]
        set admitted [mml_fp fastpath_maxmemory_admitted]
        set replies [mml_drive 4 10 200 [list mml_set_new $val 0]]
        assert_equal 8000 [mml_count $replies {+OK}]
        assert_morethan [mml_fp fastpath_write_lane_entries] $lanes
        assert_morethan [mml_fp fastpath_maxmemory_admitted] $admitted
        assert_equal 0 [s evicted_keys]
        assert_equal 0 [mml_fp fastpath_maxmemory_overrun]
        r config set maxmemory 0
    }

    test {maxmemory lanes: maxmemory-lanes no keeps the veto} {
        r config set maxmemory-lanes no
        r config set maxmemory 1gb
        set lanes [mml_fp fastpath_write_lane_entries]
        set replies [mml_drive 4 2 200 [list mml_set_new $val 0]]
        assert_equal $lanes [mml_fp fastpath_write_lane_entries]
        assert_equal maxmemory [getInfoProperty [r info fastpath] staged_lanes_veto_clients]
        r config set maxmemory 0
        r config set maxmemory-lanes yes
    }

    test {maxmemory lanes: noeviction at the limit answers OOM as stock, no write past it} {
        r flushall
        r config set maxmemory-policy noeviction
        set limit [expr {[s used_memory] + 4 * 1024 * 1024}]
        r config set maxmemory $limit
        set oom_before [mml_fp fastpath_maxmemory_overrun_oom]
        set replies [mml_drive 4 40 200 [list mml_set_new $val 0]]
        set ok [mml_count $replies {+OK}]
        set oom [mml_count $replies {-OOM*}]
        assert_equal [llength $replies] [expr {$ok + $oom}]
        assert_morethan $oom 0
        # Every +OK is a key, no write was lost or invented, nothing was evicted.
        assert_equal $ok [r dbsize]
        assert_equal 0 [s evicted_keys]
        # No lane phase could have run past the limit, and memory is within one command of it.
        assert_equal $oom_before [mml_fp fastpath_maxmemory_overrun_oom]
        assert {[s used_memory] < $limit + 1024 * 1024}
        assert_morethan [mml_fp fastpath_maxmemory_fallback] 0
        # Once full, more new keys are refused again (a few may fit in what the closed clients freed).
        set replies [mml_drive 2 2 50 {apply {{i r k} { mml_resp set mml:more:$i:$r:$k [string repeat z 1000] }}}]
        set ok2 [mml_count $replies {+OK}]
        assert_morethan [mml_count $replies {-OOM*}] 0
        assert_equal [expr {$ok + $ok2}] [r dbsize]
        assert_equal $oom_before [mml_fp fastpath_maxmemory_overrun_oom]
        r config set maxmemory 0
    }

    foreach policy {allkeys-lru allkeys-lfu allkeys-random volatile-lru volatile-lfu volatile-random volatile-ttl} {
        test "maxmemory lanes: $policy evicts at the limit under lane load" {
            r flushall
            r config resetstat
            r config set maxmemory-policy $policy
            set ttl [string match volatile-* $policy]
            set limit [expr {[s used_memory] + 4 * 1024 * 1024}]
            r config set maxmemory $limit
            set pre [mml_fp fastpath_maxmemory_preevict_admitted]
            set lanes [mml_fp fastpath_write_lane_entries]
            set replies [mml_drive 4 40 200 [list mml_set_new $val $ttl]]
            # An evicting policy never refuses these writes.
            assert_equal 32000 [mml_count $replies {+OK}]
            set evicted [s evicted_keys]
            assert_morethan $evicted 0
            # Every key written is in the dataset or was evicted, once.
            assert_equal 32000 [expr {[r dbsize] + $evicted}]
            assert {[s used_memory] < $limit + 1024 * 1024}
            # Lanes kept running near the limit after main made room.
            assert_morethan [mml_fp fastpath_maxmemory_preevict_admitted] $pre
            assert_morethan [mml_fp fastpath_write_lane_entries] $lanes
            r config set maxmemory 0
        }
    }

    test {maxmemory lanes: replies larger than the estimate are counted as an overrun} {
        r flushall
        r config set maxmemory-policy noeviction
        # A list whose LRANGE reply is about 50 KB: far above what the estimate allows per entry.
        for {set j 0} {$j < 500} {incr j} { r rpush mml:list [string repeat l 100] }
        set limit [expr {[s used_memory] + 4 * 1024 * 1024}]
        r config set maxmemory $limit
        set before [mml_fp fastpath_maxmemory_overrun_oom]
        mml_drive 4 2 200 {apply {{i r k} { mml_resp lrange mml:list 0 -1 }}}
        assert_morethan [mml_fp fastpath_maxmemory_overrun_oom] $before
        # The measured allowance followed the replies.
        assert_morethan [mml_fp fastpath_maxmemory_reply_allowance] 10000
        r config set maxmemory 0
    }

    test {maxmemory lanes: APPEND and SETRANGE run on main with maxmemory set} {
        r flushall
        r config set maxmemory-policy allkeys-lru
        r config set maxmemory 1gb
        set lanes [mml_fp fastpath_write_lane_entries]
        set replies [mml_drive 4 2 100 {apply {{i r k} { mml_resp append mml:a:$i:$k abc }}}]
        set replies [concat $replies [mml_drive 4 2 100 {apply {{i r k} { mml_resp setrange mml:r:$i:$k 10 abc }}}]]
        assert_equal 0 [mml_count $replies {-*}]
        assert_equal $lanes [mml_fp fastpath_write_lane_entries]
        assert_equal abcabc [r get mml:a:0:0]
        r config set maxmemory 0
    }
}

# Eviction on a primary whose lanes run near the limit, with a replica that ignores maxmemory and
# applies the stream in parallel: the replica must reach the primary's dataset.
start_server {tags {"staged-loop repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 maxmemory-lanes yes save ""}} {
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save ""}} {
        set replica [srv 0 client]
        set primary [srv -1 client]
        $replica config set notify-keyspace-events ""
        $primary config set notify-keyspace-events ""
        $primary config set repl-backlog-size 1mb
        $replica replicaof [srv -1 host] [srv -1 port]
        wait_for_sync $replica
        $primary select 0
        $replica select 0
        set val [string repeat y 1000]
        # Fill the backlog first: it counts against the limit.
        for {set i 0} {$i < 2000} {incr i} { $primary set warm:$i $val }

        test {maxmemory lanes: replica of an evicting primary converges, applying in parallel} {
            $primary config set maxmemory-policy allkeys-lru
            set limit [expr {[getInfoProperty [$primary info memory] used_memory] + 4 * 1024 * 1024}]
            $primary config set maxmemory $limit
            $replica config set maxmemory $limit
            set applied [getInfoProperty [$replica info stats] repl_parallel_applied]
            set fds {}
            for {set i 0} {$i < 4} {incr i} {
                set fd [socket [srv -1 host] [srv -1 port]]
                fconfigure $fd -translation binary -blocking 1 -buffering full
                lappend fds $fd
            }
            for {set r 0} {$r < 40} {incr r} {
                set i 0
                foreach fd $fds {
                    set buf ""
                    for {set k 0} {$k < 200} {incr k} { append buf [mml_resp set mml:$i:$r:$k $val] }
                    puts -nonewline $fd $buf
                    flush $fd
                    incr i
                }
                foreach fd $fds { for {set k 0} {$k < 200} {incr k} { assert_equal {+OK} [mml_read_line $fd] } }
            }
            foreach fd $fds { close $fd }
            assert_morethan [getInfoProperty [$primary info stats] evicted_keys] 0
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary dbsize] [$replica dbsize]
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal 0 [getInfoProperty [$replica info stats] evicted_keys]
            assert_morethan [getInfoProperty [$replica info stats] repl_parallel_applied] $applied
            $primary config set maxmemory 0
            $replica config set maxmemory 0
        }
    }
}
