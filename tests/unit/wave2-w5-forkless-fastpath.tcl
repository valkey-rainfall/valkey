# Plan item 25 (PER-13, PER-14): fast-path clients during a slow forkless BGSAVE, keyspace shards on
# (keyspace-shard-bits 8). The sockets are fast-path clients when the save starts; during the save they
# pipeline GET and SET over the keys the save has not written yet. Checks: every reply is correct during
# the save, the save finishes with status ok, and the RDB is the point-in-time image from the start of the
# save (DEBUG RELOAD NOSAVE loads it back: every key has its old value).
#
# Fast-path clients are raw sockets with no CLIENT SETNAME (a named client stays on the main path).

proc w5f_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w5f_read_reply {fd} {
    set line [gets $fd]
    if {[eof $fd]} { error "eof" }
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        default { return $line }
    }
}

proc w5f_fp {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 8 io-threads-parallel-writes-min 1 forkless-infrastructure-enabled yes save "" enable-debug-command yes}} {
    r client setname w5-control
    r config set notify-keyspace-events ""
    r select 0
    set host [srv 0 host]
    set port [srv 0 port]

    test {Fast-path GET and SET during a slow forkless BGSAVE with keyspace shards: replies and RDB image are correct} {
        r flushall
        set nkeys 400
        for {set k 0} {$k < $nkeys} {incr k} { r set f:$k old$k }
        set nsock 4
        set fds {}
        for {set i 0} {$i < $nsock} {incr i} {
            set fd [socket $host $port]
            fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
            puts -nonewline $fd [w5f_resp ping]
            flush $fd
            assert_equal {+PONG} [w5f_read_reply $fd]
            lappend fds $fd
        }
        wait_for_condition 200 20 {
            [w5f_fp r fastpath_clients] == $nsock
        } else {
            fail "expected $nsock fast-path clients: [r info fastpath]"
        }
        # Before the save: the sockets' GETs run on the fast path (speculated reads).
        set reads0 [w5f_fp r fastpath_speculated]
        set fd [lindex $fds 0]
        for {set k 0} {$k < 50} {incr k} { puts -nonewline $fd [w5f_resp get f:$k] }
        flush $fd
        for {set k 0} {$k < 50} {incr k} { assert_equal old$k [w5f_read_reply $fd] }
        assert {[w5f_fp r fastpath_speculated] - $reads0 >= 50}

        # 400 keys with 50 ms each: the save runs for about 20 s unless the delay is lowered.
        r config set rdb-key-save-delay 50000
        r config set bgsave-default-method forkless
        r bgsave
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "forkless bgsave did not start"
        }
        assert_equal forkless [s rdb_current_bgsave_type]
        set lanes1 [w5f_fp r fastpath_write_lane_entries]
        set batches1 [w5f_fp r fastpath_batches]
        set spec1 [w5f_fp r fastpath_speculated]
        # Each socket owns a quarter of the keys: GET old, SET new, GET new.
        set i 0
        foreach fd $fds {
            set p ""
            for {set k $i} {$k < $nkeys} {incr k $nsock} {
                append p [w5f_resp get f:$k] [w5f_resp set f:$k new$k] [w5f_resp get f:$k]
            }
            puts -nonewline $fd $p
            flush $fd
            incr i
        }
        set i 0
        foreach fd $fds {
            for {set k $i} {$k < $nkeys} {incr k $nsock} {
                assert_equal old$k [w5f_read_reply $fd]
                assert_equal {+OK} [w5f_read_reply $fd]
                assert_equal new$k [w5f_read_reply $fd]
            }
            incr i
        }
        # The writes were made while the save was still running.
        assert_equal 1 [s rdb_bgsave_in_progress]
        set lanes_during [expr {[w5f_fp r fastpath_write_lane_entries] - $lanes1}]
        set batches_during [expr {[w5f_fp r fastpath_batches] - $batches1}]
        set spec_during [expr {[w5f_fp r fastpath_speculated] - $spec1}]
        r config set rdb-key-save-delay 0
        waitForBgsave r
        assert_equal ok [s rdb_last_bgsave_status]
        assert_equal forkless [s rdb_last_bgsave_type]
        foreach fd $fds { close $fd }

        # The RDB is the image from when the save started.
        r debug reload nosave
        r select 0
        assert_equal $nkeys [r dbsize]
        for {set k 0} {$k < $nkeys} {incr k} { assert_equal old$k [r get f:$k] }
        if {$::verbose} { puts "during the save: lane entries $lanes_during, fast-path batches $batches_during, speculated reads $spec_during, fast-path clients [w5f_fp r fastpath_clients]" }
    } {} {needs:debug}
}
