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

# An everysec write postponed behind a background fsync sends its replies first, as upstream does.
proc fao_wait_no_bio_fsync {} {
    wait_for_condition 500 10 {
        [getInfoProperty [r info persistence] aof_pending_bio_fsync] == 0
    } else {
        fail "background AOF fsync did not finish"
    }
}

# Milliseconds from sending cmd on fd until its reply, with every AOF flush delayed by sleep_ms.
proc fao_reply_ms {fd sleep_ms args} {
    fao_wait_no_bio_fsync
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
    r client setname fp-control
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
    r client setname fp-control
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

start_server {tags {"fastpath aof modules external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync always enable-debug-command yes save ""}} {
    r client setname fp-control
    r select 0
    waitForBgrewriteaof r
    r module load [file normalize tests/modules/blockedclient.so]

    test {AOF order: always, a fast-path GET does not return a module thread's write before its fsync (#40)} {
        set fd [fao_fp_client]
        r set fao:bg 0
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        set violations 0
        for {set round 1} {$round <= 5} {incr round} {
            r debug aof-flush-sleep [expr {$fao_sleep * 1000}]
            set t0 [clock milliseconds]
            # The module thread takes the GIL while main sleeps in its poll, after beforeSleep's flush.
            $rd do_bg_rm_call_format ! incr fao:bg
            while 1 {
                puts -nonewline $fd [fao_resp get fao:bg]
                flush $fd
                set v [fao_read_line $fd]
                if {$v == $round} break
                if {[clock milliseconds] - $t0 > 5000} { fail "module write never became visible" }
            }
            set ms [expr {[clock milliseconds] - $t0}]
            if {$ms < $fao_sleep - 50} { incr violations }
            assert_equal $round [$rd read]
            r debug aof-flush-sleep 0
        }
        assert_equal 0 $violations
        $rd close
        close $fd
    }
}

start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly no appendfsync always enable-debug-command yes save ""}} {
    r client setname fp-control
    r select 0

    test {AOF order: always, while AOF waits for its first rewrite a fast-path write acknowledges after the fsync (#38)} {
        set fd [fao_fp_client]
        populate 2000 fao:pop: 10
        r config set rdb-key-save-delay 2000
        r config set appendonly yes
        assert_equal 1 [s aof_rewrite_in_progress]
        for {set i 0} {$i < 3} {incr i} {
            lassign [fao_reply_ms $fd $fao_sleep set fao:waitrw $i] ms reply
            assert_equal {+OK} $reply
            assert_morethan_equal $ms [expr {$fao_sleep - 50}]
        }
        assert_equal 1 [s aof_rewrite_in_progress]
        r config set rdb-key-save-delay 0
        waitForBgrewriteaof r
        close $fd
    }
}

foreach policy {everysec no} {
    start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes enable-debug-command yes save ""}} {
        r config set appendfsync $policy
        r client setname fp-control
        r select 0
        waitForBgrewriteaof r

        test "AOF order: $policy, a fast-path write's reply follows its AOF write (#41)" {
            set fd [fao_fp_client]
            for {set i 0} {$i < 3} {incr i} {
                lassign [fao_reply_ms $fd $fao_sleep set fao:es $i] ms reply
                assert_equal {+OK} $reply
                assert_morethan_equal $ms [expr {$fao_sleep - 50}]
            }
            # Pipelined writes, which the staged loop may stream while it executes.
            fao_wait_no_bio_fsync
            r debug aof-flush-sleep [expr {$fao_sleep * 1000}]
            set t0 [clock milliseconds]
            set p {}
            for {set i 0} {$i < 200} {incr i} { append p [fao_resp incr fao:es:n] }
            puts -nonewline $fd $p
            flush $fd
            assert_equal {:1} [fao_read_line $fd]
            assert_morethan_equal [expr {[clock milliseconds] - $t0}] [expr {$fao_sleep - 50}]
            for {set i 2} {$i <= 200} {incr i} { assert_equal ":$i" [fao_read_line $fd] }
            r debug aof-flush-sleep 0
            close $fd
        }
    }

    start_server {tags {"aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-fast-path no appendonly yes enable-debug-command yes save ""}} {
        r config set appendfsync $policy
        waitForBgrewriteaof r

        test "AOF order: $policy, IO threads without the staged loop: a write's reply follows its AOF write (#45)" {
            set rd [valkey_deferring_client]
            for {set i 0} {$i < 5} {incr i} {
                fao_wait_no_bio_fsync
                r debug aof-flush-sleep [expr {$fao_sleep * 1000}]
                set t0 [clock milliseconds]
                $rd set fao:io $i
                assert_equal OK [$rd read]
                assert_morethan_equal [expr {[clock milliseconds] - $t0}] [expr {$fao_sleep - 50}]
                r debug aof-flush-sleep 0
            }
            $rd close
        }
    }
}

foreach mode {append iteration} {
    start_server {tags {"repl aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync everysec enable-debug-command yes save "" repl-diskless-sync no}} {
        set primary [srv 0 client]
        $primary config set repl-stream-sender $mode
        waitForBgrewriteaof $primary
        start_server {overrides {io-threads 2 save ""}} {
            set replica [srv 0 client]
            $replica replicaof [srv -1 host] [srv -1 port]
            wait_for_sync $replica
            wait_replica_online $primary
            wait_for_condition 50 100 {
                [getInfoProperty [$primary info replication] repl_sender_replicas] == 1
            } else {
                fail "the sender did not take the replica"
            }

            test "AOF order: everysec, repl-stream-sender $mode sends a replica a write only after the AOF write (#42)" {
                set rd [valkey_deferring_client -1]
                for {set i 0} {$i < 3} {incr i} {
                    wait_for_condition 500 10 {
                        [getInfoProperty [$primary info persistence] aof_pending_bio_fsync] == 0
                    } else {
                        fail "background AOF fsync did not finish"
                    }
                    $primary debug aof-flush-sleep [expr {$fao_sleep * 1000}]
                    $rd set fao:snd $i
                    after [expr {$fao_sleep / 2}]
                    set seen [$replica get fao:snd]
                    assert_equal OK [$rd read]
                    $primary debug aof-flush-sleep 0
                    if {$i == 0} { assert_equal $seen {} } else { assert_equal $seen [expr {$i - 1}] }
                    wait_for_condition 50 20 {
                        [$replica get fao:snd] eq $i
                    } else {
                        fail "replica did not get the write"
                    }
                }
                $rd close
            }
        }
    }
}

start_server {tags {"aof wait external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync everysec save ""}} {
    waitForBgrewriteaof r

    test {AOF order: everysec, a pipelined write and WAITAOF return once the fsync completes, not at the next timer (#64)} {
        set fd [fao_fp_client]
        r config set hz 1
        set times {}
        for {set i 0} {$i < 5} {incr i} {
            after 1100
            set t0 [clock milliseconds]
            puts -nonewline $fd "[fao_resp set fao:waitaof $i][fao_resp waitaof 1 0 0]"
            flush $fd
            assert_equal {+OK} [fao_read_line $fd]
            assert_equal {*2} [fao_read_line $fd]
            assert_equal {:1} [fao_read_line $fd]
            assert_equal {:0} [fao_read_line $fd]
            lappend times [expr {[clock milliseconds] - $t0}]
        }
        r config set hz 10
        # With hz 1 the next timer is up to a second away; the fsync itself takes milliseconds.
        set sorted [lsort -integer $times]
        assert_lessthan [lindex $sorted 2] 200
        close $fd
    }
}
