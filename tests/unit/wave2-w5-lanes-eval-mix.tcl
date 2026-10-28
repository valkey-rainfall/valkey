# Plan item 31 (TXS-16, REPL-25): write-lane commands from fast-path clients and EVAL scripts from
# main-path clients on the same keys, for a fixed time (default 30 s, the environment variable W5_MIX_SECONDS overrides).
# The scripts read keys the lanes write and store a value built from what they read, so a propagation
# order that differs from the execution order gives a different value on the replica or after a reload.
# Checks: DEBUG DIGEST equal on the primary, on the replica and after DEBUG LOADAOF.
#
# Fast-path clients are raw sockets with no CLIENT SETNAME; the script clients are named, so they stay
# on the main path.

set ::w5_mix_seconds 30
if {[info exists ::env(W5_MIX_SECONDS)]} { set ::w5_mix_seconds $::env(W5_MIX_SECONDS) }

proc w5m_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w5m_read_reply {fd} {
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
            for {set i 0} {$i < $n} {incr i} { lappend out [w5m_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc w5m_fastpath_clients {c} {
    return [getInfoProperty [$c info fastpath] fastpath_clients]
}

proc w5m_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

proc w5m_eval_calls {c} {
    set line [getInfoProperty [$c info commandstats] cmdstat_evalsha]
    if {![regexp {calls=(\d+)} $line -> n]} { return 0 }
    return $n
}

# Open n raw sockets and wait until the server counts them all as fast-path clients.
proc w5m_fast_sockets {c host port n} {
    set base [w5m_fastpath_clients $c]
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [socket $host $port]
        fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
        puts -nonewline $fd [w5m_resp ping]
        flush $fd
        assert_equal {+PONG} [w5m_read_reply $fd]
        lappend fds $fd
    }
    wait_for_condition 200 20 {
        [w5m_fastpath_clients $c] == $base + $n
    } else {
        fail "expected $n more fast-path clients: [$c info fastpath]"
    }
    return $fds
}

set w5m_script {
    local a = redis.call('INCR', KEYS[1])
    local b = redis.call('GET', KEYS[2])
    if not b then b = '-' end
    redis.call('SET', KEYS[3], a .. ':' .. b)
    redis.call('APPEND', KEYS[2], 's')
    return a
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip valgrind:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync everysec enable-debug-command yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary client setname w5-control
    $primary config set notify-keyspace-events ""
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

        test "Lane writes and EVAL on the same keys for $::w5_mix_seconds s: primary, replica and AOF digests agree" {
            set nfast 6
            set nkeys 64
            $primary flushall
            set lanes0 [w5m_lane_entries $primary]
            set evals0 [w5m_eval_calls $primary]
            set applied0 [getInfoProperty [$replica info stats] repl_parallel_applied]
            set fds [w5m_fast_sockets $primary $primary_host $primary_port $nfast]
            set ev {}
            for {set i 0} {$i < 2} {incr i} {
                set d [valkey_deferring_client -1]
                $d client setname w5-eval-$i
                $d read
                lappend ev $d
            }
            set sha [$primary script load $w5m_script]
            set end [expr {[clock milliseconds] + $::w5_mix_seconds * 1000}]
            set round 0
            while {[clock milliseconds] < $end} {
                set i 0
                foreach fd $fds {
                    set buf ""
                    for {set j 0} {$j < 50} {incr j} {
                        set k [expr {($i * 17 + $j * 5 + $round) % $nkeys}]
                        append buf [w5m_resp incr n:$k]
                        append buf [w5m_resp set s:$k $round.$i.$j]
                        append buf [w5m_resp append s:$k f]
                    }
                    puts -nonewline $fd $buf
                    flush $fd
                    incr i
                }
                set i 0
                foreach d $ev {
                    for {set j 0} {$j < 20} {incr j} {
                        set k [expr {($i * 29 + $j * 3 + $round) % $nkeys}]
                        $d evalsha $sha 3 n:$k s:$k r:$k
                    }
                    incr i
                }
                foreach fd $fds {
                    for {set k 0} {$k < 150} {incr k} {
                        set reply [w5m_read_reply $fd]
                        if {[string index $reply 0] eq "-"} { fail "error reply: $reply" }
                    }
                }
                foreach d $ev {
                    for {set j 0} {$j < 20} {incr j} { $d read }
                }
                incr round
            }
            foreach fd $fds { close $fd }
            foreach d $ev { $d close }
            set lanes [expr {[w5m_lane_entries $primary] - $lanes0}]
            set evals [expr {[w5m_eval_calls $primary] - $evals0}]
            # Engagement: the lanes ran the fast-path writes and the scripts ran in the same time.
            assert {$lanes >= $round * $nfast * 100}
            assert {$evals == $round * 40}

            wait_for_ofs_sync $primary $replica
            set digest [$primary debug digest]
            assert_equal $digest [$replica debug digest]
            set applied [expr {[getInfoProperty [$replica info stats] repl_parallel_applied] - $applied0}]
            assert {$applied > 0}

            $primary debug loadaof
            assert_equal $digest [$primary debug digest]
            if {$::verbose} { puts "rounds=$round lanes=$lanes evals=$evals replica_parallel_applied=$applied" }
        }
    }
}
