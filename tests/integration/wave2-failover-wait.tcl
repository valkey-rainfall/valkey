# Wave 2, worker w4: replication guarantees seen from fast-path clients.
#   WAIT / WAITAOF after lane writes, split MSET, a write behind a barrier and serial writes (CB-16,
#   TXS-26, TXS-27, PER-06, CLU-16); FAILOVER under pipelined INCR from fast-path clients (PLAT-09,
#   REPL-21); a replica with replica-parallel-apply while io-threads is resized (PLAT-02); SIGTERM
#   and SHUTDOWN under fast-path load (PLAT-08, REPL-02).
# Fast-path clients are raw db-0 sockets without a client name. Every test asserts the fast path,
# the lanes or the parallel apply moved with an INFO counter. Synchronisation uses SIGSTOP/SIGCONT
# of the replica, offsets, log lines and wait_for_condition; no sleeps.

proc w4_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w4_read_reply {fd} {
    set line [gets $fd]
    if {[eof $fd]} { return {EOF} }
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w4_read_reply $fd] }
            return $out
        }
        {_} { return {(nil)} }
        default { return $line }
    }
}

proc w4_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [w4_read_reply $fd] }
    return $out
}

proc w4_fp {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

# A raw socket admitted to the fast path of the server behind $c (its PING is answered, then the
# server counts one more fast-path client).
proc w4_fast_socket {c host port} {
    set before [w4_fp $c fastpath_clients]
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    puts -nonewline $fd [w4_resp ping]
    flush $fd
    assert_equal {+PONG} [w4_read_reply $fd]
    wait_for_condition 200 20 {
        [w4_fp $c fastpath_clients] > $before
    } else {
        fail "the raw socket was not admitted to the fast path: [$c info fastpath]"
    }
    return $fd
}

# Close a fast-path socket and wait until the server no longer counts it, so the next
# w4_fast_socket sees a stable baseline.
proc w4_close {c fd} {
    set before [w4_fp $c fastpath_clients]
    close $fd
    wait_for_condition 200 20 {
        [w4_fp $c fastpath_clients] < $before
    } else {
        fail "the server still counts the closed fast-path socket: [$c info fastpath]"
    }
}

proc w4_send {fd buf} {
    puts -nonewline $fd $buf
    flush $fd
}

# A pipeline of lane writes over many shards, replies checked, on an open fast socket.
proc w4_lane_burst {fd prefix n} {
    set buf ""
    for {set i 0} {$i < $n} {incr i} { append buf [w4_resp set $prefix:k$i v$i] }
    w4_send $fd $buf
    set got [w4_read_replies $fd $n]
    assert_equal [lrepeat $n {+OK}] $got
}

# WAIT on the SAME fast socket that did the writes (WAIT counts the caller's own write offset).
proc w4_wait {fd numreplicas timeout} {
    w4_send $fd [w4_resp wait $numreplicas $timeout]
    set r [w4_read_reply $fd]
    assert_match {:*} $r
    return [string range $r 1 end]
}

proc w4_delta {c field before} {
    return [expr {[w4_fp $c $field] - $before}]
}

# ----------------------------------------------------------------------------------------------
# Plan item 16: WAIT and WAITAOF see lane writes, split MSET, a write behind a barrier and serial
# writes. The replica is SIGSTOPped so it cannot ACK: WAIT 1 100 must return 0; after SIGCONT it
# must return 1.
# ----------------------------------------------------------------------------------------------
start_server {tags {"wave2 repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes}} {
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set primary_pid [srv -1 pid]
        set replica [srv 0 client]
        set replica_pid [srv 0 pid]
        $primary client setname w4-control
        $replica client setname w4-control
        # The raw fast-path sockets use db 0; the harness clients start in db 9.
        $primary select 0
        $replica select 0
        $primary config set notify-keyspace-events ""
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        test {WAIT after lane writes from a fast-path client: 0 while the replica is stopped, 1 after} {
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set lanes [w4_fp $primary fastpath_write_lane_entries]
            pause_process $replica_pid
            w4_lane_burst $fd w4:lane 300
            assert_morethan [w4_delta $primary fastpath_write_lane_entries $lanes] 0
            assert_equal 0 [w4_wait $fd 1 100]
            resume_process $replica_pid
            assert_equal 1 [w4_wait $fd 1 10000]
            assert_equal v299 [$replica get w4:lane:k299]
            w4_close $primary $fd
        }

        test {WAIT after a split MSET from a fast-path client} {
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set lanes [w4_fp $primary fastpath_write_lane_entries]
            pause_process $replica_pid
            set args {}
            for {set i 0} {$i < 64} {incr i} { lappend args w4:mset:$i m$i }
            w4_send $fd [w4_resp mset {*}$args]
            assert_equal {+OK} [w4_read_reply $fd]
            assert_morethan [w4_delta $primary fastpath_write_lane_entries $lanes] 0
            assert_equal 0 [w4_wait $fd 1 100]
            resume_process $replica_pid
            assert_equal 1 [w4_wait $fd 1 10000]
            assert_equal m63 [$replica get w4:mset:63]
            w4_close $primary $fd
        }

        test {WAIT after a write in the segment behind a main-only barrier} {
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set barriers [w4_fp $primary fastpath_write_barriers]
            set lanes [w4_fp $primary fastpath_write_lane_entries]
            pause_process $replica_pid
            set buf [w4_resp set w4:bar:a 1]
            append buf [w4_resp setbit w4:bar:bits 7 1]
            append buf [w4_resp set w4:bar:b 2]
            append buf [w4_resp set w4:bar:c 3]
            w4_send $fd $buf
            assert_equal {+OK :0 +OK +OK} [w4_read_replies $fd 4]
            assert_morethan [w4_delta $primary fastpath_write_barriers $barriers] 0
            assert_morethan [w4_delta $primary fastpath_write_lane_entries $lanes] 0
            assert_equal 0 [w4_wait $fd 1 100]
            resume_process $replica_pid
            assert_equal 1 [w4_wait $fd 1 10000]
            assert_equal 3 [$replica get w4:bar:c]
            w4_close $primary $fd
        }

        test {WAIT after serial executor writes from a fast-path client} {
            $primary config set io-threads-parallel-writes-min 1000000
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set serial [w4_fp $primary fastpath_write_serial_segments]
            pause_process $replica_pid
            w4_lane_burst $fd w4:serial 300
            assert_morethan [w4_delta $primary fastpath_write_serial_segments $serial] 0
            assert_equal 0 [w4_wait $fd 1 100]
            resume_process $replica_pid
            assert_equal 1 [w4_wait $fd 1 10000]
            assert_equal v299 [$replica get w4:serial:k299]
            w4_close $primary $fd
            $primary config set io-threads-parallel-writes-min 1
        }

        test {WAITAOF 1 0 after lane writes from a fast-path client returns 1 local} {
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set lanes [w4_fp $primary fastpath_write_lane_entries]
            w4_lane_burst $fd w4:aof 300
            assert_morethan [w4_delta $primary fastpath_write_lane_entries $lanes] 0
            w4_send $fd [w4_resp waitaof 1 0 10000]
            assert_equal {:1 :0} [w4_read_reply $fd]
            w4_close $primary $fd
        }
    }
}

# ----------------------------------------------------------------------------------------------
# Plan item 4: FAILOVER TO the replica while two fast-path clients pipeline INCR on one key.
# Every INCR the old primary acknowledged must be in the new primary's value, both sides must end
# equal, and a read from a fast-path client during and after the failover is served.
# ----------------------------------------------------------------------------------------------
start_server {tags {"wave2 repl failover external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]
        set replica [srv -1 client]
        set replica_host [srv -1 host]
        set replica_port [srv -1 port]
        $primary client setname w4-control
        $replica client setname w4-control
        # The raw fast-path sockets use db 0; the harness clients start in db 9.
        $primary select 0
        $replica select 0
        $primary config set notify-keyspace-events ""
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        test {FAILOVER under pipelined INCR from fast-path clients loses no acknowledged write} {
            set tclsh [info nameofexecutable]
            set outs {}
            set writes [w4_fp $primary fastpath_writes]
            foreach i {0 1} {
                set out [file join [lindex [$primary config get dir] 1] w4_failover_$i.out]
                file delete -force $out $out.stop
                exec $tclsh tests/helpers/w4_incr_load.tcl $primary_host $primary_port w4:ctr $out 64 &
                lappend outs $out
            }
            wait_for_condition 500 20 {
                [$primary get w4:ctr] ne {} && [$primary get w4:ctr] > 5000
            } else {
                fail "INCR load did not start: [$primary get w4:ctr]"
            }
            assert_morethan [w4_delta $primary fastpath_writes $writes] 0
            # A third fast-path client, admitted before the failover, reads while it runs.
            set rfd [w4_fast_socket $primary $primary_host $primary_port]
            set reads [w4_fp $primary fastpath_reads]
            $primary failover to $replica_host $replica_port TIMEOUT 20000
            w4_send $rfd [w4_resp get w4:ctr]
            set during [w4_read_reply $rfd]
            assert {[string is integer -strict $during]}
            wait_for_condition 1000 20 {
                [s 0 master_failover_state] eq "no-failover"
            } else {
                fail "failover did not finish: [$primary info replication]"
            }
            assert_equal slave [s 0 role]
            assert_equal master [s -1 role]
            # The load processes stop on the first -READONLY reply from the demoted server.
            foreach out $outs {
                wait_for_condition 500 20 { [file exists $out] } else { fail "load $out did not stop" }
            }
            wait_for_sync $primary
            wait_for_ofs_sync $replica $primary
            set new_val [$replica get w4:ctr]
            set old_val [$primary get w4:ctr]
            assert_equal $new_val $old_val
            set max_acked 0
            set total_acked 0
            foreach out $outs {
                set ch [open $out r]
                set lines [split [string trim [read $ch]] "\n"]
                close $ch
                lassign $lines m a reason
                if {$m > $max_acked} { set max_acked $m }
                incr total_acked $a
                assert_match {-READONLY*} $reason
            }
            # The largest value acknowledged to any client must be in the new primary.
            assert_morethan $max_acked 5000
            assert {$max_acked <= $new_val}
            assert {$total_acked <= $new_val}
            # The demoted server still serves a fast-path read of the final value.
            w4_send $rfd [w4_resp get w4:ctr]
            assert_equal $new_val [w4_read_reply $rfd]
            close $rfd
            assert {$during <= $new_val}
            puts "    acked max $max_acked total $total_acked, read during failover $during, new primary $new_val, fast-path writes +[w4_delta $primary fastpath_writes $writes], fast-path reads +[w4_delta $primary fastpath_reads $reads]"
        }
    }
}

# ----------------------------------------------------------------------------------------------
# Plan item 28: a replica applying in parallel while its io-threads are resized 8 -> 2 -> 8 twice
# under lane load ends with the primary's digest.
# ----------------------------------------------------------------------------------------------
start_server {tags {"wave2 repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    start_server {overrides {io-threads 8 keyspace-shard-bits 10 replica-parallel-apply yes}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        $primary client setname w4-control
        $replica client setname w4-control
        # The raw fast-path sockets use db 0; the harness clients start in db 9.
        $primary select 0
        $replica select 0
        $primary config set notify-keyspace-events ""
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        proc w4_mixed_burst {fd prefix n} {
            set buf ""
            for {set i 0} {$i < $n} {incr i} {
                switch [expr {$i % 5}] {
                    0 { append buf [w4_resp set $prefix:k$i v$i] }
                    1 { append buf [w4_resp hset $prefix:h[expr {$i % 97}] f$i $i] }
                    2 { append buf [w4_resp sadd $prefix:s[expr {$i % 89}] m$i] }
                    3 { append buf [w4_resp zadd $prefix:z[expr {$i % 83}] $i m$i] }
                    4 { append buf [w4_resp rpush $prefix:l[expr {$i % 79}] $i] }
                }
            }
            w4_send $fd $buf
            w4_read_replies $fd $n
        }

        test {Replica parallel apply survives io-threads 8 -> 2 -> 8 twice under lane load: digests equal} {
            set applied [getInfoProperty [$replica info stats] repl_parallel_applied]
            set lanes [w4_fp $primary fastpath_write_lane_entries]
            set fd [w4_fast_socket $primary $primary_host $primary_port]
            set round 0
            foreach n {2 8 2 8} {
                w4_mixed_burst $fd w4:pa:$round 3000
                assert_equal OK [$replica config set io-threads $n]
                w4_mixed_burst $fd w4:pa:$round:after 3000
                incr round
            }
            close $fd
            assert_morethan [w4_delta $primary fastpath_write_lane_entries $lanes] 0
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            set applied_delta [expr {[getInfoProperty [$replica info stats] repl_parallel_applied] - $applied}]
            assert_morethan $applied_delta 0
            puts "    repl_parallel_applied +$applied_delta, lane entries +[w4_delta $primary fastpath_write_lane_entries $lanes]"
        }
    }
}

# ----------------------------------------------------------------------------------------------
# Plan item 30: SIGTERM under fast-path pipeline load exits with the ready-to-exit line; SHUTDOWN
# with shutdown-timeout 2 while the replica is SIGSTOPped keeps every acknowledged SET on the
# replica, with the replication stream sender off and on (REPL-02 / F10 when on).
# ----------------------------------------------------------------------------------------------
foreach sender {no yes} {
    start_server {tags {"wave2 repl shutdown external:skip tls:skip"} overrides {io-threads 4}} {
        start_server [list overrides [list io-threads 4 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 shutdown-timeout 2 repl-diskless-sync no repl-stream-sender $sender]] {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set replica [srv -1 client]
            set replica_pid [srv -1 pid]
            $primary client setname w4-control
            $replica client setname w4-control
            $primary select 0
            $replica select 0
            $primary config set notify-keyspace-events ""
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            wait_for_ofs_sync $primary $replica

            test "SHUTDOWN (shutdown-timeout 2, replica stopped) after fast-path SETs keeps every acked SET, repl-stream-sender $sender" {
                set n 200
                set fd [w4_fast_socket $primary $primary_host $primary_port]
                set lanes [w4_fp $primary fastpath_write_lane_entries]
                # A disk-based sync keeps the replica eligible for the sender thread.
                set sender_on [getInfoProperty [$primary info replication] repl_sender_replicas]
                assert_equal [expr {$sender eq "yes" ? 1 : 0}] $sender_on
                pause_process $replica_pid
                set buf ""
                for {set i 0} {$i < $n} {incr i} { append buf [w4_resp set w4:sd:$i [string repeat v 100]] }
                w4_send $fd $buf
                set got [w4_read_replies $fd $n]
                assert_equal [lrepeat $n {+OK}] $got
                set lane_delta [w4_delta $primary fastpath_write_lane_entries $lanes]
                assert_morethan $lane_delta 0
                # Second half: the same lane writes and SHUTDOWN in one pipeline, so the server
                # starts the shutdown in the event loop iteration that produced the stream.
                set buf ""
                for {set i $n} {$i < 2 * $n} {incr i} { append buf [w4_resp set w4:sd:$i [string repeat v 100]] }
                append buf [w4_resp shutdown]
                w4_send $fd $buf
                assert_equal [lrepeat $n {+OK}] [w4_read_replies $fd $n]
                set n [expr {2 * $n}]
                # The server waits shutdown-timeout for the stopped replica, then exits.
                wait_for_log_messages 0 {"*ready to exit*"} 0 200 50
                wait_for_condition 200 50 { ![is_alive [srv 0 pid]] } else { fail "primary still alive" }
                catch {close $fd}
                resume_process $replica_pid
                wait_for_condition 200 50 {
                    [status $replica master_link_status] eq "down"
                } else { fail "replica link still up" }
                # Everything the primary acknowledged must have reached the replica's socket before exit.
                set missing {}
                for {set i 0} {$i < $n} {incr i} {
                    if {[$replica exists w4:sd:$i] != 1} { lappend missing $i }
                }
                puts "    repl-stream-sender $sender (repl_sender_replicas $sender_on), lane entries +$lane_delta, missing [llength $missing] of $n"
                assert_equal {} $missing
            }
        }
    }
}

# SHUTDOWN NOW in the same pipeline as lane writes, replica running (no wait): every SET the
# primary acknowledged must reach the replica. With repl-stream-sender on this is F10 (REPL-02):
# finishShutdown stops the sender and the final flush skips replicas it still marks as owned.
foreach sender {no yes} {
    start_server {tags {"wave2 repl shutdown external:skip tls:skip"} overrides {io-threads 4}} {
        start_server [list overrides [list io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 repl-diskless-sync no repl-stream-sender $sender]] {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set replica [srv -1 client]
            $primary client setname w4-control
            $replica client setname w4-control
            $primary select 0
            $replica select 0
            # The harness default.conf turns keyspace notifications on, which keeps writes off the lanes.
            $primary config set notify-keyspace-events ""
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            wait_for_ofs_sync $primary $replica

            test "SHUTDOWN NOW in a pipeline after fast-path SETs keeps every acked SET on the replica, repl-stream-sender $sender" {
                set n 500
                set fd [w4_fast_socket $primary $primary_host $primary_port]
                assert_equal [expr {$sender eq "yes" ? 1 : 0}] [getInfoProperty [$primary info replication] repl_sender_replicas]
                # Engagement: the same pipeline shape without SHUTDOWN uses the lanes.
                set lanes [w4_fp $primary fastpath_write_lane_entries]
                w4_lane_burst $fd w4:warm $n
                set lane_delta [w4_delta $primary fastpath_write_lane_entries $lanes]
                assert_morethan $lane_delta 0
                wait_for_ofs_sync $primary $replica
                set buf ""
                for {set i 0} {$i < $n} {incr i} { append buf [w4_resp set w4:now:$i [string repeat v 100]] }
                append buf [w4_resp shutdown now]
                w4_send $fd $buf
                set acked {}
                for {set i 0} {$i < $n} {incr i} {
                    if {[w4_read_reply $fd] ne {+OK}} break
                    lappend acked $i
                }
                catch {close $fd}
                wait_for_condition 200 50 { ![is_alive [srv 0 pid]] } else { fail "primary still alive" }
                wait_for_condition 200 50 {
                    [status $replica master_link_status] eq "down"
                } else { fail "replica link still up" }
                set missing 0
                foreach i $acked {
                    if {[$replica exists w4:now:$i] != 1} { incr missing }
                }
                puts "    repl-stream-sender $sender, lane entries +$lane_delta, acked [llength $acked] of $n, missing on replica $missing"
                assert_morethan [llength $acked] 0
                assert_equal 0 $missing
            }
        }
    }
}

# SIGTERM while fast-path pipelines are in flight: exit status 0 and the ready-to-exit line.
start_server {tags {"wave2 shutdown external:skip tls:skip"} overrides {io-threads 4 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    test {SIGTERM under fast-path pipeline load exits 0 with the ready-to-exit line} {
        # Our own server, started with open so the exit status is observable.
        set port [find_available_port [expr {$::baseport + 50}] $::portcount]
        set dir [lindex [r config get dir] 1]
        set log [file join $dir w4_sigterm.log]
        set ch [open [list | src/valkey-server --port $port --bind 127.0.0.1 --protected-mode yes --save "" \
                     --appendonly no --io-threads 4 --keyspace-shard-bits 10 --io-threads-parallel-writes-min 1 \
                     --enable-debug-command yes --dir $dir --logfile $log --loglevel notice] r]
        set pid [pid $ch]
        wait_for_condition 200 50 {
            [catch {set c [valkey 127.0.0.1 $port 0 0]; $c ping}] == 0
        } else { fail "server did not start" }
        $c client setname w4-control
        set tclsh [info nameofexecutable]
        set outs {}
        foreach i {0 1} {
            set out [file join $dir w4_sigterm_$i.out]
            file delete -force $out $out.stop
            exec $tclsh tests/helpers/w4_incr_load.tcl 127.0.0.1 $port w4:term $out 64 &
            lappend outs $out
        }
        wait_for_condition 500 20 {
            [$c get w4:term] ne {} && [$c get w4:term] > 5000
        } else { fail "INCR load did not start" }
        set writes [getInfoProperty [$c info fastpath] fastpath_writes]
        assert_morethan $writes 0
        if {[catch {exec kill -0 $pid}]} { fail "server not running" }
        exec kill -SIGTERM $pid
        # close reports the child's exit status; a 0 exit returns normally.
        set rc 0
        if {[catch {close $ch} err opts]} {
            set ec [dict get $opts -errorcode]
            if {[lindex $ec 0] eq "CHILDSTATUS"} { set rc [lindex $ec 2] } else { set rc "error: $err" }
        }
        foreach out $outs {
            wait_for_condition 500 20 { [file exists $out] } else { fail "load $out did not stop" }
        }
        set fh [open $log r]
        set text [read $fh]
        close $fh
        assert_match {*ready to exit*} $text
        assert_equal 0 $rc
        puts "    exit status $rc, fastpath_writes $writes before SIGTERM"
    }
}
