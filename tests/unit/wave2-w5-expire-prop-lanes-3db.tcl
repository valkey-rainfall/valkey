# Plan item 22 (EXP-03, PER-03, REPL-06): write lanes with lazy expiry over three databases, AOF on and
# a replica attached. Fast-path clients pipeline SELECT / SET k n PX p / INCR k / GETEX k PX p over the
# same keys (PX 1..50 ms, active expiry off during the load, so every expiry is a lazy one found by a
# command, on a lane or on main). Checks: DEBUG DIGEST equal on primary, replica and after DEBUG LOADAOF;
# in the AOF, every command runs in the database its key belongs to (a SELECT precedes each change) and,
# inside a MULTI/EXEC block, a DEL of a key comes before that key's next write.
#
# Fast-path clients are raw sockets with no CLIENT SETNAME (a named client stays on the main path).

proc w5x_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w5x_read_reply {fd} {
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
            for {set i 0} {$i < $n} {incr i} { lappend out [w5x_read_reply $fd] }
            return $out
        }
        {_} { return {(nil)} }
        default { return $line }
    }
}

proc w5x_fastpath_clients {c} {
    return [getInfoProperty [$c info fastpath] fastpath_clients]
}

proc w5x_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

proc w5x_fast_sockets {c host port n} {
    wait_for_condition 200 20 {
        [w5x_fastpath_clients $c] == 0
    } else {
        fail "leftover fast-path clients: [$c info fastpath]"
    }
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [socket $host $port]
        fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
        puts -nonewline $fd [w5x_resp ping]
        flush $fd
        assert_equal {+PONG} [w5x_read_reply $fd]
        lappend fds $fd
    }
    wait_for_condition 200 20 {
        [w5x_fastpath_clients $c] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
    return $fds
}

# Volatile keys over all databases.
proc w5x_total_expires {c} {
    set total 0
    foreach line [split [$c info keyspace] "\r\n"] {
        if {[regexp {expires=(\d+)} $line -> e]} { incr total $e }
    }
    return $total
}

# Parse a RESP command stream (an incr AOF) into a list of commands, each a list of arguments.
proc w5x_parse_aof {path} {
    set fd [open $path rb]
    set data [read $fd]
    close $fd
    set cmds {}
    set pos 0
    set len [string length $data]
    while {$pos < $len} {
        set nl [string first "\r\n" $data $pos]
        if {$nl < 0} { break }
        set line [string range $data $pos [expr {$nl - 1}]]
        set pos [expr {$nl + 2}]
        if {[string index $line 0] ne "*"} { error "AOF: expected array at $pos, got [string range $line 0 20]" }
        set n [string range $line 1 end]
        set cmd {}
        for {set i 0} {$i < $n} {incr i} {
            set nl [string first "\r\n" $data $pos]
            set line [string range $data $pos [expr {$nl - 1}]]
            if {[string index $line 0] ne "\$"} { error "AOF: expected bulk at $pos" }
            set blen [string range $line 1 end]
            set pos [expr {$nl + 2}]
            lappend cmd [string range $data $pos [expr {$pos + $blen - 1}]]
            set pos [expr {$pos + $blen + 2}]
        }
        lappend cmds $cmd
    }
    return $cmds
}

# Every key of the load is named d<db>:... Check the selected database and the DEL-before-write order
# inside MULTI blocks. Returns {commands_with_keys blocks_with_del}.
proc w5x_check_aof {cmds} {
    set db 0
    set in_multi 0
    set keyed 0
    set del_blocks 0
    set block_writes {}
    set block_had_del 0
    foreach cmd $cmds {
        set name [string toupper [lindex $cmd 0]]
        switch -- $name {
            SELECT { set db [lindex $cmd 1]; continue }
            MULTI { set in_multi 1; set block_writes {}; set block_had_del 0; continue }
            EXEC { set in_multi 0; if {$block_had_del} { incr del_blocks }; continue }
            FLUSHALL { continue }
        }
        # One key at argv[1] for every command of the load and of expiry (SET, INCR, GETEX, DEL, UNLINK,
        # PEXPIREAT ...).
        set key [lindex $cmd 1]
        if {![regexp {^d(\d+):} $key -> kdb]} { error "AOF: unexpected command [lrange $cmd 0 1]" }
        incr keyed
        if {$kdb != $db} { error "AOF: $name $key while database $db is selected" }
        if {!$in_multi} continue
        if {$name eq "DEL" || $name eq "UNLINK"} {
            set block_had_del 1
            # A DEL after a write of the same key inside one block would undo that write.
            if {[lsearch -exact $block_writes $key] >= 0} { error "AOF: DEL $key after its write in one MULTI block" }
        } else {
            lappend block_writes $key
        }
    }
    return [list $keyed $del_blocks]
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync everysec enable-debug-command yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary client setname w5-control
    $primary config set notify-keyspace-events ""
    $primary select 0
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
        set replica [srv 0 client]
        $replica client setname w5-replica-control
        # Keyspace events (on in the test default config) keep the replica off the parallel apply path.
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_condition 100 100 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "replica did not sync"
        }

        test {Lanes + lazy expiry in 3 dbs with AOF and a replica: digests agree, AOF order is sound} {
            set nsock 12
            set rounds 10
            set nkeys 150
            $primary flushall
            $primary debug set-active-expire 0
            set lanes0 [w5x_lane_entries $primary]
            set expired0 [getInfoProperty [$primary info stats] expired_keys]
            set fds [w5x_fast_sockets $primary $primary_host $primary_port $nsock]
            # Each socket works in one database. The SELECT is sent alone: a SELECT inside a pipeline is
            # not a fast-path command, so it and the rest of that pipeline would run on main.
            set i 0
            foreach fd $fds {
                puts -nonewline $fd [w5x_resp select [expr {$i % 3}]]
                flush $fd
                assert_equal {+OK} [w5x_read_reply $fd]
                incr i
            }
            for {set round 0} {$round < $rounds} {incr round} {
                set i 0
                foreach fd $fds {
                    set db [expr {$i % 3}]
                    set buf ""
                    set ncmd 0
                    for {set j 0} {$j < $nkeys} {incr j} {
                        set px [expr {($i * 7 + $j * 13 + $round * 3) % 50 + 1}]
                        set px2 [expr {($i * 11 + $j * 5 + $round) % 50 + 1}]
                        append buf [w5x_resp set d$db:k$j 5 px $px]
                        append buf [w5x_resp incr d$db:k$j]
                        append buf [w5x_resp getex d$db:k$j px $px2]
                        append buf [w5x_resp incr d$db:c$j]
                        incr ncmd 4
                    }
                    puts -nonewline $fd $buf
                    flush $fd
                    lappend pending($i) $ncmd
                    incr i
                }
                set i 0
                foreach fd $fds {
                    set ncmd [lindex $pending($i) end]
                    for {set k 0} {$k < $ncmd} {incr k} {
                        set reply [w5x_read_reply $fd]
                        if {[string index $reply 0] eq "-"} { fail "error reply: $reply" }
                    }
                    incr i
                }
            }
            foreach fd $fds { close $fd }
            set lanes_grown [expr {[w5x_lane_entries $primary] - $lanes0}]
            set expired [expr {[getInfoProperty [$primary info stats] expired_keys] - $expired0}]
            # Engagement: lanes ran the load and lazy expiry happened during it.
            assert {$lanes_grown >= 1000}
            assert {$expired >= 50}

            # Active expiry removes what is left, each DEL propagated; then the three views must agree.
            $primary debug set-active-expire 1
            wait_for_condition 500 20 {
                [w5x_total_expires $primary] == 0
            } else {
                fail "volatile keys remain: [$primary info keyspace]"
            }
            wait_for_ofs_sync $primary $replica
            set digest [$primary debug digest]
            assert_equal $digest [$replica debug digest]
            assert {[getInfoProperty [$replica info stats] repl_parallel_applied] > 0}

            # The AOF: selected database and DEL order.
            set dir [lindex [$primary config get dir] 1]
            set keyed 0
            set del_blocks 0
            foreach path [glob -directory [file join $dir appendonlydir] *.incr.aof] {
                lassign [w5x_check_aof [w5x_parse_aof $path]] k d
                incr keyed $k
                incr del_blocks $d
            }
            assert {$keyed >= $nsock * $rounds * $nkeys * 4}
            # Lazy expiry on a write is propagated as MULTI / DEL / write / EXEC.
            assert {$del_blocks > 0}

            # Reload from the AOF: same data on the primary, and still the replica's.
            $primary debug loadaof
            wait_for_condition 500 20 {
                [w5x_total_expires $primary] == 0
            } else {
                fail "volatile keys remain after loadaof: [$primary info keyspace]"
            }
            assert_equal $digest [$primary debug digest]
            wait_for_ofs_sync $primary $replica
            assert_equal $digest [$replica debug digest]
            if {$::verbose} { puts "lanes_grown=$lanes_grown expired=$expired aof_cmds=$keyed del_blocks=$del_blocks" }
        }
    }
}
