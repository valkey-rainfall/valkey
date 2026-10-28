# Plan item 26 (PER-18, PER-02): appendfsync always with the write lanes forced on. A write a fast-path
# client got a reply for must be in the AOF when the process is killed with SIGKILL right after, and a
# split MSET (keys on two lanes) is all-or-nothing in the AOF. Second test: AOF turned on at run time
# (CONFIG SET appendonly yes), then the same check once the rewrite finished and the state is "on".
#
# Fast-path clients are raw sockets with no CLIENT SETNAME (a named client stays on the main path).

proc w5_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w5_read_reply {fd} {
    set line [gets $fd]
    if {[eof $fd]} { error "eof" }
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w5_read_reply $fd] }
            return $out
        }
        {_} { return {(nil)} }
        default { return $line }
    }
}

proc w5_fastpath_clients {c} {
    return [getInfoProperty [$c info fastpath] fastpath_clients]
}

proc w5_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

# n raw sockets, each confirmed on the fast path by the INFO counter.
proc w5_fast_sockets {c host port n} {
    wait_for_condition 200 20 {
        [w5_fastpath_clients $c] == 0
    } else {
        fail "leftover fast-path clients: [$c info fastpath]"
    }
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [socket $host $port]
        fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
        puts -nonewline $fd [w5_resp ping]
        flush $fd
        assert_equal {+PONG} [w5_read_reply $fd]
        lappend fds $fd
    }
    wait_for_condition 200 20 {
        [w5_fastpath_clients $c] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
    return $fds
}

# Each socket pipelines n split MSETs (two keys, two shards) and one INCR per step. The server is
# killed with SIGKILL while the lanes are still working through the pipelines (the AOF flush is slowed
# so the kill lands mid-stream). Returns {acked lane_entries_grown} where acked is one count per socket.
proc w5_kill9_load {c fds n} {
    set lanes0 [w5_lane_entries $c]
    $c debug aof-flush-sleep 5000
    set i 0
    foreach fd $fds {
        set p ""
        for {set k 0} {$k < $n} {incr k} {
            append p [w5_resp mset w5:a:$i:$k $k w5:b:$i:$k $k]
            append p [w5_resp incr w5:n:$i]
        }
        puts -nonewline $fd $p
        flush $fd
        incr i
    }
    # Kill only once the lanes have run part of the load: proves engagement and leaves replies unsent.
    wait_for_condition 1000 10 {
        [w5_lane_entries $c] - $lanes0 >= 300
    } else {
        fail "lanes did not run the load: [$c info fastpath]"
    }
    set grown [expr {[w5_lane_entries $c] - $lanes0}]
    exec kill -9 [srv 0 pid]
    set acked {}
    foreach fd $fds {
        set m 0
        set last_incr 0
        while {![catch {w5_read_reply $fd} reply]} {
            if {$reply eq {+OK}} {
                incr m
            } elseif {[regexp {^:(\d+)$} $reply -> v]} {
                set last_incr $v
            } else {
                fail "unexpected reply: $reply"
            }
        }
        # Replies are in pipeline order: the INCR of step k follows the MSET of step k.
        assert {$last_incr == $m || $last_incr == $m - 1}
        lappend acked [list $m $last_incr]
        catch {close $fd}
    }
    return [list $acked $grown]
}

# After the restart every acknowledged MSET has both keys, every acknowledged INCR is counted, and no
# MSET is half present.
proc w5_check_after_restart {c nsock n acked} {
    set total 0
    set present 0
    for {set i 0} {$i < $nsock} {incr i} {
        lassign [lindex $acked $i] m last_incr
        incr total $m
        set cnt [$c get w5:n:$i]
        if {$cnt eq {}} { set cnt 0 }
        assert {$cnt >= $last_incr}
        for {set k 0} {$k < $n} {incr k} {
            set a [$c get w5:a:$i:$k]
            set b [$c get w5:b:$i:$k]
            if {$k < $m} {
                assert_equal $k $a
                assert_equal $k $b
            } else {
                # Not acknowledged: the AOF may or may not hold it, but never half of it.
                assert_equal [expr {$a eq {}}] [expr {$b eq {}}]
                if {$a ne {}} { assert_equal $a $b }
            }
            if {$a ne {}} { incr present }
        }
    }
    # The kill interrupted the load (otherwise the check is trivial) and some writes were acknowledged.
    assert {$present < $nsock * $n}
    assert {$total > 0}
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync always enable-debug-command yes}} {
    r client setname w5-control
    r config set notify-keyspace-events ""
    r select 0
    set host [srv 0 host]
    set port [srv 0 port]
    set nsock 4
    set n 2000

    test {Lanes + appendfsync always: acknowledged split MSETs and INCRs survive kill -9} {
        r flushall
        set fds [w5_fast_sockets r $host $port $nsock]
        lassign [w5_kill9_load r $fds $n] acked grown
        assert {$grown >= 300}
        restart_server 0 true false
        wait_done_loading r
        r client setname w5-control
        r select 0
        w5_check_after_restart r $nsock $n $acked
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly no appendfsync always enable-debug-command yes}} {
    r client setname w5-control
    r config set notify-keyspace-events ""
    r select 0
    set host [srv 0 host]
    set port [srv 0 port]
    set nsock 4
    set n 2000

    test {Lanes + appendfsync always: AOF turned on at run time, acknowledged writes survive kill -9} {
        r flushall
        # A few lane writes before the AOF exists, so the rewrite has a base to produce.
        set fds [w5_fast_sockets r $host $port 1]
        set fd [lindex $fds 0]
        set p ""
        for {set k 0} {$k < 200} {incr k} { append p [w5_resp set w5:pre:$k $k] }
        puts -nonewline $fd $p
        flush $fd
        for {set k 0} {$k < 200} {incr k} { assert_equal {+OK} [w5_read_reply $fd] }
        close $fd
        r config set appendonly yes
        r config rewrite
        # Writes made while the first rewrite runs (AOF_WAIT_REWRITE) are not durable in stock Valkey
        # either: the check starts once the AOF is on.
        wait_for_condition 500 20 {
            [s aof_rewrite_in_progress] == 0 && [s aof_rewrite_scheduled] == 0 && [s aof_enabled] == 1
        } else {
            fail "AOF rewrite did not finish: [r info persistence]"
        }
        assert_equal {ok} [s aof_last_bgrewrite_status]
        set fds [w5_fast_sockets r $host $port $nsock]
        lassign [w5_kill9_load r $fds $n] acked grown
        assert {$grown >= 300}
        restart_server 0 true false
        wait_done_loading r
        r client setname w5-control
        r select 0
        assert_equal 1 [s aof_enabled]
        for {set k 0} {$k < 200} {incr k} { assert_equal $k [r get w5:pre:$k] }
        w5_check_after_restart r $nsock $n $acked
    }
}
