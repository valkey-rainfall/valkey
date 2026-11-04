# A client that a command sends to main rejoins the fast path once nothing keeps it there: a
# server-wide gate that lifted, session state that ended, or replies its socket could not take at
# the time. Each client is a raw db-0 socket: the test framework's client sends SELECT first.

proc fprm_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc fprm_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc fprm_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

# One command and its one-line reply.
proc fprm_cmd {fd args} {
    puts -nonewline $fd [fprm_resp {*}$args]
    flush $fd
    return [string trim [gets $fd]]
}

proc fprm_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [fprm_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# The client stays off the fast path: main runs its commands and it does not join.
proc fprm_assert_stays_on_main {fd} {
    assert_equal PONG [string trimleft [fprm_cmd $fd ping] +]
    after 100
    assert_equal 0 [fprm_fastpath_clients]
}

start_server {tags {"fastpath readmit external:skip tls:skip"} overrides {io-threads 4}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    r client setname fprm-control
    r select 0 ;# the db of the raw sockets

    test {Fast path readmit: a client sent to main by a pause rejoins once the pause lifts} {
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        r client pause 30000 write
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 0
        fprm_assert_stays_on_main $a
        r client unpause
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
    }

    test {Fast path readmit: a client accepted during a pause joins once the pause lifts} {
        r client pause 30000 write
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        assert_equal 0 [fprm_fastpath_clients]
        r client unpause
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
    }

    test {Fast path readmit: a client rejoins once its name is cleared} {
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        assert_equal +OK [fprm_cmd $a client setname named]
        fprm_wait_fastpath_clients 0
        fprm_assert_stays_on_main $a
        assert_equal +OK [fprm_cmd $a client setname ""]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
    }

    test {Fast path readmit: a client rejoins after EXEC and after UNWATCH, not while they last} {
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        assert_equal +OK [fprm_cmd $a multi]
        fprm_wait_fastpath_clients 0
        assert_equal +QUEUED [fprm_cmd $a set fprm:k 1]
        after 100
        assert_equal 0 [fprm_fastpath_clients]
        assert_equal *1 [fprm_cmd $a exec]
        assert_equal +OK [string trim [gets $a]]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1

        assert_equal +OK [fprm_cmd $a watch fprm:k]
        fprm_wait_fastpath_clients 0
        fprm_assert_stays_on_main $a
        assert_equal +OK [fprm_cmd $a unwatch]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
    }

    test {Fast path readmit: a client whose socket could not take its replies rejoins later} {
        r set fprm:big [string repeat x 20000000]
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        # The GET reply fills the socket, and ECHO sends the client to main with most of it unwritten.
        puts -nonewline $a "[fprm_resp get fprm:big][fprm_resp echo hi]"
        flush $a
        fprm_wait_fastpath_clients 0
        after 200
        assert_equal 0 [fprm_fastpath_clients]
        assert_equal {$20000000} [string trim [gets $a]]
        assert_equal 20000002 [string length [read $a 20000002]]
        assert_equal {$2} [string trim [gets $a]]
        assert_equal hi [string trim [gets $a]]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
        r del fprm:big
    }

    test {Fast path readmit: a client that subscribed stays on main until it unsubscribes} {
        set a [fprm_socket]
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        puts -nonewline $a [fprm_resp subscribe fprm:ch]
        flush $a
        assert_equal *3 [string trim [gets $a]]
        for {set i 0} {$i < 5} {incr i} { gets $a }
        fprm_wait_fastpath_clients 0
        puts -nonewline $a [fprm_resp unsubscribe fprm:ch]
        flush $a
        assert_equal *3 [string trim [gets $a]]
        for {set i 0} {$i < 5} {incr i} { gets $a }
        assert_equal +PONG [fprm_cmd $a ping]
        fprm_wait_fastpath_clients 1
        close $a
        fprm_wait_fastpath_clients 0
    }
}
