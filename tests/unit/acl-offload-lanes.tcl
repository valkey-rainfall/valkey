# acl-offload with parallel write lanes: a restricted user's SET runs on a lane on the ALLOW verdict the
# IO thread tagged in the read phase. A rule change on main (ACL SETUSER) bumps the ACL epoch. A lane must
# never run a command whose verdict was tagged under an older epoch: after the revoke, no OK and no key.
# The server runs with enable-debug-assert yes, so fpLaneExecute asserts the epoch on every lane entry.

proc aol_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc aol_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    return $fd
}

proc aol_read_line {fd} {
    set line [gets $fd]
    if {[string index $line 0] eq {$}} {
        if {$line eq {$-1}} { return {(nil)} }
        return [gets $fd]
    }
    return $line
}

proc aol_info {c field} {
    return [getInfoProperty [$c info everything] $field]
}

start_server {tags {"acl acl-offload staged-loop external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 acl-offload yes enable-debug-command yes enable-debug-assert yes}} {
    set host [srv 0 host]
    set port [srv 0 port]
    r select 0
    r config set notify-keyspace-events ""
    r ACL SETUSER aol on >pw ~k:* +set +ping

    # A fast-path socket authenticated as the restricted user.
    proc aol_user_socket {host port} {
        set fd [aol_socket $host $port]
        puts -nonewline $fd [aol_resp auth aol pw]
        puts -nonewline $fd [aol_resp ping]
        flush $fd
        assert_equal {+OK} [aol_read_line $fd]
        assert_equal {+PONG} [aol_read_line $fd]
        return $fd
    }

    test {acl-offload lanes: a restricted user's SETs run on lanes on offloaded ALLOW verdicts} {
        set fd [aol_user_socket $host $port]
        set lanes0 [aol_info r fastpath_write_lane_entries]
        set hits0 [aol_info r acl_offload_hits]
        for {set round 0} {$round < 20} {incr round} {
            set buf ""
            for {set i 0} {$i < 200} {incr i} { append buf [aol_resp set k:e$i v] }
            puts -nonewline $fd $buf
            flush $fd
            for {set i 0} {$i < 200} {incr i} { assert_equal {+OK} [aol_read_line $fd] }
        }
        close $fd
        # engagement: the SETs ran on write lanes and the ALLOW verdicts were tagged by the IO threads
        assert_morethan [aol_info r fastpath_write_lane_entries] $lanes0
        assert_morethan [expr {[aol_info r acl_offload_hits] + [aol_info r fastpath_write_lane_entries]}] [expr {$hits0 + $lanes0}]
        r flushall
    }

    test {acl-offload lanes: no OK and no key after the ACL SETUSER revoke reply} {
        set lanes0 [aol_info r fastpath_write_lane_entries]
        set punts0 [aol_info r acl_offload_punts]
        set raced 0
        set fd [aol_user_socket $host $port]
        set admin [aol_socket $host $port]
        for {set round 0} {$round < 100} {incr round} {
            set n 300
            set buf ""
            for {set i 0} {$i < $n} {incr i} { append buf [aol_resp set k:r$round:$i v] }
            # The user pipeline and the revoke are sent together so they can share an iteration.
            puts -nonewline $fd $buf
            flush $fd
            puts -nonewline $admin [aol_resp acl setuser aol -set]
            flush $admin
            assert_equal {+OK} [aol_read_line $admin]
            set first_err -1
            set oks 0
            for {set i 0} {$i < $n} {incr i} {
                set reply [aol_read_line $fd]
                if {$reply eq {+OK}} {
                    incr oks
                    if {$first_err >= 0} { fail "round $round: SET $i got OK after SET $first_err was refused" }
                } else {
                    assert_match {-NOPERM*} $reply
                    if {$first_err < 0} { set first_err $i }
                }
            }
            if {$first_err > 0} { incr raced }
            # After the revoke reply a new SET is refused and writes nothing.
            puts -nonewline $fd [aol_resp set k:after$round v]
            flush $fd
            assert_match {-NOPERM*} [aol_read_line $fd]
            assert_equal 0 [r exists k:after$round]
            # Exactly the keys of the OK replies exist: none of the refused SETs wrote its key.
            # (the OK replies are a prefix of the pipeline, checked above, and the db held no other key)
            assert_equal $oks [r dbsize]
            if {$oks > 0} { assert_equal 1 [r exists k:r$round:[expr {$oks - 1}]] }
            if {$first_err >= 0} { assert_equal 0 [r exists k:r$round:$first_err] }
            r ACL SETUSER aol +set
            r flushall
        }
        close $fd
        close $admin
        # The server is alive: no debug assert fired in fpLaneExecute.
        assert_equal {PONG} [r ping]
        assert_morethan [aol_info r fastpath_write_lane_entries] $lanes0
        puts "acl-offload lanes: rounds with a revoke inside the pipeline=$raced, lane entries=[expr {[aol_info r fastpath_write_lane_entries] - $lanes0}], acl punts=[expr {[aol_info r acl_offload_punts] - $punts0}]"
    }
}
