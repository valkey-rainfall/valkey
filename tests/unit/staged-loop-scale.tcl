# Staged event loop: the IO threads that take part in the phases follow the work. At a trickle main
# runs the phases alone and the IO threads park; a pipelined load brings them back within a few
# milliseconds; io-threads-always-active keeps them all in.

proc sls_active {} {
    return [getInfoProperty [r info fastpath] staged_active_workers]
}

proc sls_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active no}} {
    r select 0 ;# the raw sockets below write to db 0
    test {Staged loop: at a trickle the IO threads park and main runs the phases} {
        for {set i 0} {$i < 50} {incr i} { r set sls:k:$i v }
        wait_for_condition 100 20 {
            [sls_active] == 0
        } else {
            fail "IO threads stayed in at a trickle: [r info fastpath]"
        }
        assert_equal v [r get sls:k:7]
    }

    test {Staged loop: a pipelined load brings the IO threads back, and the data is intact} {
        set fds {}
        for {set c 0} {$c < 8} {incr c} { lappend fds [sls_socket] }
        set cmd {}
        for {set i 0} {$i < 2000} {incr i} { append cmd "*3\r\n\$3\r\nSET\r\n\$9\r\nsls:p:[format %03d [expr {$i % 1000}]]\r\n\$1\r\nx\r\n" }
        set seen 0
        for {set round 0} {$round < 200 && !$seen} {incr round} {
            foreach fd $fds { puts -nonewline $fd $cmd; flush $fd }
            foreach fd $fds { set got 0; while {$got < 2000 * 5} { incr got [string length [read $fd [expr {2000 * 5 - $got}]]] } }
            if {[sls_active] > 0} { set seen 1 }
        }
        foreach fd $fds { close $fd }
        assert_equal 1 $seen
        assert_equal x [r get sls:p:999]
        assert_equal 1000 [llength [r keys sls:p:*]]
    }

    test {Staged loop: io-threads-always-active keeps every IO thread in} {
        r config set io-threads-always-active yes
        wait_for_condition 100 20 {
            [sls_active] == 3
        } else {
            fail "always-active did not bring every IO thread in: [r info fastpath]"
        }
        after 300
        assert_equal 3 [sls_active]
        r config set io-threads-always-active no
    }
}
