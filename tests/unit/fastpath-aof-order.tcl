# Replies and replica bytes leave only after the AOF write the appendfsync policy promises. DEBUG
# AOF-FLUSH-SLEEP delays each flush that has data before its write, so anything that arrives sooner
# than that delay left before the write.

proc fao_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc fao_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc fao_read_line {fd} {
    if {[gets $fd line] < 0} { error "EOF" }
    set line [string trimright $line "\r"]
    if {[string index $line 0] eq {$} && $line ne {$-1}} {
        set n [string range $line 1 end]
        set v [read $fd [expr {$n + 2}]]
        return [string range $v 0 end-2]
    }
    return $line
}

# A raw db-0 socket that ran one command and sits on the fast path.
proc fao_fp_client {{host {}} {port {}}} {
    if {$host eq {}} { set host [srv 0 host] }
    if {$port eq {}} { set port [srv 0 port] }
    set before [fao_fastpath_clients]
    set fd [socket $host $port]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    puts -nonewline $fd [fao_resp ping]
    flush $fd
    assert_equal {+PONG} [fao_read_line $fd]
    wait_for_condition 200 25 {
        [fao_fastpath_clients] >= $before + 1
    } else {
        fail "client did not join the fast path: [r info fastpath]"
    }
    return $fd
}

# Milliseconds from sending cmd on fd until its reply, with every AOF flush delayed by sleep_ms.
proc fao_reply_ms {fd sleep_ms args} {
    r debug aof-flush-sleep [expr {$sleep_ms * 1000}]
    set t0 [clock milliseconds]
    puts -nonewline $fd [fao_resp {*}$args]
    flush $fd
    set reply [fao_read_line $fd]
    set ms [expr {[clock milliseconds] - $t0}]
    r debug aof-flush-sleep 0
    return [list $ms $reply]
}

set fao_sleep 400

start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync always enable-debug-command yes save ""}} {
    r select 0
    waitForBgrewriteaof r

    test {AOF order: always, a readmitted fast-path client gets an EVAL write's reply only after the fsync (#36)} {
        set fd [fao_fp_client]
        lassign [fao_reply_ms $fd $fao_sleep eval {return redis.call('set', KEYS[1], 'v')} 1 fao:eval] ms reply
        assert_equal {+OK} $reply
        assert_morethan_equal $ms [expr {$fao_sleep - 50}]
        # It rejoins the fast path once its reply has been written.
        wait_for_condition 100 20 {
            [fao_fastpath_clients] >= 1
        } else {
            fail "client did not rejoin the fast path"
        }
        lassign [fao_reply_ms $fd 0 get fao:eval] ms reply
        assert_equal v $reply
        close $fd
    }

}

start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync always enable-debug-command yes save "" forkless-infrastructure-enabled yes bgsave-default-method forkless}} {
    r select 0
    waitForBgrewriteaof r

    test {AOF order: always, during a forkless save fast-path writes run on main and acknowledge after the fsync (#37)} {
        set fd [fao_fp_client]
        populate 2000 fao:pop: 10
        r config set rdb-key-save-delay 2000
        r bgsave
        wait_for_condition 50 20 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "bgsave did not start"
        }
        for {set i 0} {$i < 3} {incr i} {
            lassign [fao_reply_ms $fd $fao_sleep set fao:forkless $i] ms reply
            assert_equal {+OK} $reply
            assert_morethan_equal $ms [expr {$fao_sleep - 50}]
        }
        assert_equal 1 [s rdb_bgsave_in_progress]
        r config set rdb-key-save-delay 0
        waitForBgsave r
        close $fd
    }
}
