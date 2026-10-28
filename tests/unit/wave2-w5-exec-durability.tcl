# Plan item 39 (TXS-13, PER-01, PER-23): durability of acknowledged writes with the write lanes on.
# Test 1, appendfsync always: clients admitted to the fast path pipeline MULTI / INCR / SET / EXEC while
# other fast-path clients keep the lanes busy. The server is killed with SIGKILL in the middle. Every EXEC
# the client got a reply for must be in the AOF after the restart, and every transaction is all or nothing.
# Test 2, appendfsync everysec: acknowledged lane writes older than 2 s (the window everysec allows when
# a background fsync delays the write) survive SIGKILL.
#
# Fast-path clients are raw sockets with no CLIENT SETNAME (a named client stays on the main path).

proc w5t_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w5t_read_reply {fd} {
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
            for {set i 0} {$i < $n} {incr i} { lappend out [w5t_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc w5t_fastpath_clients {c} {
    return [getInfoProperty [$c info fastpath] fastpath_clients]
}

proc w5t_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

proc w5t_fast_sockets {c host port n} {
    wait_for_condition 200 20 {
        [w5t_fastpath_clients $c] == 0
    } else {
        fail "leftover fast-path clients: [$c info fastpath]"
    }
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [socket $host $port]
        fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
        puts -nonewline $fd [w5t_resp ping]
        flush $fd
        assert_equal {+PONG} [w5t_read_reply $fd]
        lappend fds $fd
    }
    wait_for_condition 200 20 {
        [w5t_fastpath_clients $c] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
    return $fds
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync always enable-debug-command yes}} {
    r client setname w5-control
    # The raw sockets work in database 0; the test client starts in another one.
    r select 0
    r config set notify-keyspace-events ""
    set host [srv 0 host]
    set port [srv 0 port]

    test {Lanes + appendfsync always: an acknowledged EXEC from a fast-path client survives kill -9} {
        r flushall
        set nlane 3
        set ntx 2
        set n 1500
        # All sockets are fast-path clients when the load starts; the MULTI moves a socket to main.
        set fds [w5t_fast_sockets r $host $port [expr {$nlane + $ntx}]]
        set lanes0 [w5t_lane_entries r]
        r debug aof-flush-sleep 5000
        set i 0
        foreach fd $fds {
            set p ""
            for {set k 0} {$k < $n} {incr k} {
                if {$i < $nlane} {
                    append p [w5t_resp set l:$i:$k $k]
                } else {
                    append p [w5t_resp multi] [w5t_resp incr t:$i] [w5t_resp set t:$i:$k $k] [w5t_resp exec]
                }
            }
            puts -nonewline $fd $p
            flush $fd
            incr i
        }
        wait_for_condition 1000 10 {
            [w5t_lane_entries r] - $lanes0 >= 300
        } else {
            fail "lanes did not run the load: [r info fastpath]"
        }
        exec kill -9 [srv 0 pid]
        # Per socket: the number of acknowledged SETs (lane sockets) or EXECs (transaction sockets).
        set acked {}
        set i 0
        foreach fd $fds {
            set m 0
            while {![catch {w5t_read_reply $fd} reply]} {
                if {$i < $nlane} {
                    assert_equal {+OK} $reply
                    incr m
                } elseif {[llength $reply] == 2} {
                    # The EXEC reply: {INCR result, +OK}.
                    incr m
                    assert_equal ":$m" [lindex $reply 0]
                } elseif {$reply ne {+OK} && $reply ne {+QUEUED}} {
                    fail "unexpected reply: $reply"
                }
            }
            lappend acked $m
            catch {close $fd}
            incr i
        }
        restart_server 0 true false
        wait_done_loading r
        r client setname w5-control
        # The raw sockets work in database 0; the test client starts in another one.
        r select 0
        set tx_total 0
        set present 0
        for {set i 0} {$i < $nlane + $ntx} {incr i} {
            set m [lindex $acked $i]
            if {$i < $nlane} {
                for {set k 0} {$k < $m} {incr k} { assert_equal $k [r get l:$i:$k] }
                if {[r get l:$i:[expr {$n - 1}]] eq {}} { incr present }
                continue
            }
            incr tx_total $m
            set cnt [r get t:$i]
            if {$cnt eq {}} { set cnt 0 }
            # Every acknowledged EXEC is there; an unacknowledged one may be, but whole.
            assert {$cnt >= $m}
            for {set k 0} {$k < $n} {incr k} {
                set v [r get t:$i:$k]
                if {$k < $cnt} { assert_equal $k $v } else { assert_equal {} $v }
            }
            if {$cnt < $n} { incr present }
        }
        # The kill interrupted the load and some transactions were acknowledged before it.
        assert {$present > 0}
        assert {$tx_total > 0}
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync everysec enable-debug-command yes}} {
    r client setname w5-control
    # The raw sockets work in database 0; the test client starts in another one.
    r select 0
    r config set notify-keyspace-events ""
    set host [srv 0 host]
    set port [srv 0 port]

    test {Lanes + appendfsync everysec: acknowledged lane writes older than 2 s survive kill -9} {
        r flushall
        set nsock 4
        set n 2000
        set fds [w5t_fast_sockets r $host $port $nsock]
        set lanes0 [w5t_lane_entries r]
        set i 0
        foreach fd $fds {
            set p ""
            for {set k 0} {$k < $n} {incr k} { append p [w5t_resp set e:$i:$k $k] [w5t_resp incr e:n:$i] }
            puts -nonewline $fd $p
            flush $fd
            incr i
        }
        foreach fd $fds {
            for {set k 0} {$k < 2 * $n} {incr k} {
                set reply [w5t_read_reply $fd]
                if {[string index $reply 0] eq "-"} { fail "error reply: $reply" }
            }
            close $fd
        }
        assert {[w5t_lane_entries r] - $lanes0 >= $nsock * $n}
        # everysec may hold an acknowledged write back for up to 2 s while a background fsync runs.
        # This wait is the guarantee's own window, not a synchronisation.
        after 2100
        exec kill -9 [srv 0 pid]
        restart_server 0 true false
        wait_done_loading r
        r client setname w5-control
        # The raw sockets work in database 0; the test client starts in another one.
        r select 0
        for {set i 0} {$i < $nsock} {incr i} {
            assert_equal $n [r get e:n:$i]
            for {set k 0} {$k < $n} {incr k} { assert_equal $k [r get e:$i:$k] }
        }
    }
}
