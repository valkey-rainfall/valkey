# Write lanes with maxmemory set. A segment runs on the lanes only when used memory plus a bound on what
# the segment can add stays under maxmemory; otherwise it runs serially through processCommand, which
# makes every eviction and OOM decision. Fast-path clients here are raw db-0 sockets;
# io-threads-parallel-writes-min 1 sends every segment that may to the lanes.

set ::mml_open {}

proc mml_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    lappend ::mml_open $fd
    return $fd
}

proc mml_close_all {} {
    foreach fd $::mml_open { catch {close $fd} }
    set ::mml_open {}
}

proc mml_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc mml_read_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        {_} { return {(nil)} }
        default { return $line }
    }
}

proc mml_fp {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

proc mml_wait_fastpath_clients {c n} {
    wait_for_condition 200 20 {
        [mml_fp $c fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
}

# nsock fast-path sockets, each answered a PING.
proc mml_open_fast {c host port nsock} {
    mml_close_all
    mml_wait_fastpath_clients $c 0
    set fds {}
    for {set s 0} {$s < $nsock} {incr s} {
        set fd [mml_socket $host $port]
        puts -nonewline $fd [mml_resp ping]
        flush $fd
        assert_equal {+PONG} [mml_read_reply $fd]
        lappend fds $fd
    }
    mml_wait_fastpath_clients $c $nsock
    return $fds
}

# One pipeline per socket, all sent before any reply is read; returns every socket's replies.
proc mml_pipelines {fds bufs counts} {
    foreach fd $fds buf $bufs {
        puts -nonewline $fd $buf
        flush $fd
    }
    set out {}
    foreach fd $fds n $counts {
        set r {}
        for {set i 0} {$i < $n} {incr i} { lappend r [mml_read_reply $fd] }
        lappend out $r
    }
    return $out
}

# SETs of val on nsock sockets, n per socket, keys prefix:s:i; returns {ok oom other}.
proc mml_set_load {c host port prefix nsock n val} {
    set fds [mml_open_fast $c $host $port $nsock]
    set bufs {}
    set counts {}
    for {set s 0} {$s < $nsock} {incr s} {
        set buf ""
        for {set i 0} {$i < $n} {incr i} { append buf [mml_resp set $prefix:$s:$i $val] }
        lappend bufs $buf
        lappend counts $n
    }
    set ok 0
    set oom 0
    set other {}
    foreach r [mml_pipelines $fds $bufs $counts] {
        foreach reply $r {
            if {$reply eq {+OK}} {
                incr ok
            } elseif {$reply eq {-OOM command not allowed when used memory > 'maxmemory'.}} {
                incr oom
            } else {
                lappend other $reply
            }
        }
    }
    mml_close_all
    mml_wait_fastpath_clients $c 0
    return [list $ok $oom $other]
}

start_server {tags {"staged-loop maxmemory external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save "" enable-debug-command yes}} {
    r client setname mml-control
    r debug fastpath-exclude 1
    r select 0
    r config set notify-keyspace-events ""
    set host [srv 0 host]
    set port [srv 0 port]

    test {Lanes under maxmemory: with room to spare, writes and reads run on the lanes} {
        r flushall
        r config set maxmemory-policy noeviction
        r config set maxmemory 1gb
        set phases [mml_fp r fastpath_write_maxmemory_lane_phases]
        set entries [mml_fp r fastpath_write_lane_entries]
        set fds [mml_open_fast r $host $port 4]
        set bufs {}
        set counts {}
        set expect {}
        for {set s 0} {$s < 4} {incr s} {
            set buf ""
            set e {}
            for {set i 0} {$i < 300} {incr i} {
                append buf [mml_resp set mml:a:$s:$i v$s.$i]
                append buf [mml_resp get mml:a:$s:$i]
                append buf [mml_resp incr mml:n:$s]
                lappend e {+OK} v$s.$i :[expr {$i + 1}]
            }
            lappend bufs $buf
            lappend counts 900
            lappend expect $e
        }
        assert_equal $expect [mml_pipelines $fds $bufs $counts]
        mml_close_all
        assert {[mml_fp r fastpath_write_maxmemory_lane_phases] > $phases}
        assert {[mml_fp r fastpath_write_lane_entries] - $entries >= 1200}
        assert_equal 1204 [r dbsize]
        assert_equal 0 [s evicted_keys]
    }

    test {Lanes under maxmemory: a command without a growth bound runs as a barrier at its place} {
        r flushall
        set barriers [mml_fp r fastpath_write_maxmemory_barriers]
        set fds [mml_open_fast r $host $port 1]
        set buf ""
        set e {}
        for {set i 0} {$i < 100} {incr i} {
            append buf [mml_resp set mml:b:$i x]
            append buf [mml_resp append mml:b:$i y]
            append buf [mml_resp get mml:b:$i]
            append buf [mml_resp zadd mml:z $i m$i]
            lappend e {+OK} {:2} xy {:1}
        }
        assert_equal [list $e] [mml_pipelines $fds [list $buf] {400}]
        mml_close_all
        assert {[mml_fp r fastpath_write_maxmemory_barriers] - $barriers >= 200}
        assert_equal 100 [r zcard mml:z]
    }

    test {Lanes under maxmemory: hash, set and list writes run on the lanes, conversions included} {
        r flushall
        set phases [mml_fp r fastpath_write_maxmemory_lane_phases]
        set barriers [mml_fp r fastpath_write_maxmemory_barriers]
        set fds [mml_open_fast r $host $port 2]
        set bufs {}
        set counts {}
        for {set s 0} {$s < 2} {incr s} {
            set buf ""
            # 300 fields, members and elements per key: past the listpack and intset limits.
            for {set i 0} {$i < 300} {incr i} {
                append buf [mml_resp hset mml:h:$s f$i $i]
                append buf [mml_resp sadd mml:s:$s m$i]
                append buf [mml_resp rpush mml:l:$s $i]
                append buf [mml_resp hincrby mml:h:$s n 1]
            }
            lappend bufs $buf
            lappend counts 1200
        }
        mml_pipelines $fds $bufs $counts
        mml_close_all
        for {set s 0} {$s < 2} {incr s} {
            assert_equal 301 [r hlen mml:h:$s]
            assert_equal 300 [r hget mml:h:$s n]
            assert_equal 300 [r scard mml:s:$s]
            assert_equal 300 [r llen mml:l:$s]
            assert_equal hashtable [r object encoding mml:s:$s]
            assert_equal hashtable [r object encoding mml:h:$s]
        }
        assert {[mml_fp r fastpath_write_maxmemory_lane_phases] > $phases}
        assert_equal $barriers [mml_fp r fastpath_write_maxmemory_barriers]
    }

    test {Lanes under maxmemory: the kill switch and maxmemory-clients keep the veto} {
        r flushall
        r config set io-threads-parallel-writes-maxmemory no
        set phases [mml_fp r fastpath_write_lane_phases]
        mml_set_load r $host $port mml:k 2 200 v
        assert_equal $phases [mml_fp r fastpath_write_lane_phases]
        assert_equal maxmemory [mml_fp r staged_lanes_veto_clients]
        r config set io-threads-parallel-writes-maxmemory yes
        r config set maxmemory-clients 1gb
        mml_set_load r $host $port mml:k 2 200 v
        assert_equal $phases [mml_fp r fastpath_write_lane_phases]
        assert_equal maxmemory [mml_fp r staged_lanes_veto_clients]
        r config set maxmemory-clients 0
        mml_set_load r $host $port mml:k 2 200 v
        assert {[mml_fp r fastpath_write_lane_phases] > $phases}
    }

    test {Lanes under maxmemory: over the limit with noeviction, the same OOM replies as with lanes off} {
        r flushall
        r config set maxmemory-policy noeviction
        r debug populate 1000 mml:base 100
        r config set maxmemory [expr {[s used_memory] / 2}]
        set replies {}
        foreach mode {yes no} {
            r config set io-threads-parallel-writes-maxmemory $mode
            set fds [mml_open_fast r $host $port 3]
            set bufs {}
            set counts {}
            for {set s 0} {$s < 3} {incr s} {
                set buf ""
                for {set i 0} {$i < 200} {incr i} {
                    append buf [mml_resp set mml:base:$i new]
                    append buf [mml_resp get mml:base:[expr {$i + 1}]]
                    append buf [mml_resp del mml:none:$s:$i]
                    append buf [mml_resp incr mml:base:[expr {$i + 2}]]
                }
                lappend bufs $buf
                lappend counts 800
            }
            lappend replies [mml_pipelines $fds $bufs $counts]
            mml_close_all
            mml_wait_fastpath_clients r 0
        }
        assert_equal [lindex $replies 0] [lindex $replies 1]
        assert_match {-OOM command not allowed*} [lindex [lindex $replies 0] 0 0]
        assert_equal 1000 [r dbsize]
        r config set io-threads-parallel-writes-maxmemory yes
        r config set maxmemory 0
    }

    test {Lanes under maxmemory: near the limit with noeviction, writes stop at the limit} {
        r flushall
        r config set maxmemory-policy noeviction
        r debug populate 20000 mml:base 100
        set limit [expr {[s used_memory] + 512 * 1024}]
        r config set maxmemory $limit
        set serial [mml_fp r fastpath_write_maxmemory_serial_segments]
        set val [string repeat x 100]
        lassign [mml_set_load r $host $port mml:c 4 3000 $val] ok oom other
        assert_equal {} $other
        assert {$oom > 0}
        assert {$ok > 0}
        # Every SET adds more than its value; 512 KB of headroom holds fewer than 512*1024/100 of them.
        assert {$ok < 512 * 1024 / 100}
        # The serial run stops one write past the limit at most.
        assert {[s used_memory] <= $limit + 64 * 1024}
        assert {[mml_fp r fastpath_write_maxmemory_serial_segments] > $serial}
        r config set maxmemory 0
    }

    test {Lanes under maxmemory: near the limit with noeviction, hash writes stop at the limit} {
        r flushall
        r config set maxmemory-policy noeviction
        r debug populate 20000 mml:base 100
        set limit [expr {[s used_memory] + 512 * 1024}]
        r config set maxmemory $limit
        set val [string repeat x 100]
        set fds [mml_open_fast r $host $port 4]
        set bufs {}
        set counts {}
        for {set s 0} {$s < 4} {incr s} {
            set buf ""
            for {set i 0} {$i < 3000} {incr i} { append buf [mml_resp hset mml:hh:$s:[expr {$i % 50}] f$i $val] }
            lappend bufs $buf
            lappend counts 3000
        }
        set ok 0
        set oom 0
        foreach rs [mml_pipelines $fds $bufs $counts] {
            foreach reply $rs {
                if {$reply eq {:1}} { incr ok } elseif {[string match {-OOM *} $reply]} { incr oom } else { fail "unexpected reply $reply" }
            }
        }
        mml_close_all
        mml_wait_fastpath_clients r 0
        assert {$ok > 0 && $oom > 0}
        assert {[s used_memory] <= $limit + 64 * 1024}
        r config set maxmemory 0
    }

    test {Lanes under maxmemory: allkeys-lru evicts at the limit and no write is refused} {
        r flushall
        r config set maxmemory-policy allkeys-lru
        set limit [expr {[s used_memory] + 4 * 1024 * 1024}]
        r config set maxmemory $limit
        set val [string repeat x 1000]
        for {set round 0} {$round < 10} {incr round} {
            lassign [mml_set_load r $host $port mml:e$round 4 200 $val] ok oom other
            assert_equal 800 $ok
            assert_equal 0 $oom
            assert_equal {} $other
        }
        assert {[s evicted_keys] > 0}
        assert {[s used_memory] < $limit + 1024 * 1024}
        r config set maxmemory 0
        r flushall
    }
}

start_server {tags {"staged-loop maxmemory repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary client setname mml-control
    $primary debug fastpath-exclude 1
    $primary select 0
    $primary config set notify-keyspace-events ""
    start_server {overrides {io-threads 3 keyspace-shard-bits 10 save ""}} {
        set replica [srv 0 client]
        $replica select 0
        $replica config set notify-keyspace-events ""
        $replica config set maxmemory 1gb
        $replica replicaof $primary_host $primary_port
        wait_for_condition 100 50 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "replica did not connect"
        }

        test {Lanes under maxmemory: a replica that ignores maxmemory applies the stream on its lanes} {
            assert_equal yes [lindex [$replica config get replica-ignore-maxmemory] 1]
            $primary config set maxmemory 1gb
            set before [getInfoProperty [$replica info stats] repl_parallel_applied]
            mml_set_load $primary $primary_host $primary_port mml:r 4 500 v
            wait_for_ofs_sync $primary $replica
            assert {[getInfoProperty [$replica info stats] repl_parallel_applied] > $before}
            assert_equal [$primary dbsize] [$replica dbsize]
            assert_equal 2000 [$replica dbsize]
            $primary config set maxmemory 0
        }
    }
}
