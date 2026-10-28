# Wave 2 semantics tests for the superset fast path and write lanes.
# Every test asserts engagement with an INFO fastpath counter; a test that does not engage the
# fast path proves nothing. Fast-path clients are raw sockets with no name, on db 0 (the test
# harness clients use db 9, so every control client here selects db 0 and keeps a name, which
# keeps it off the fast path). notify-keyspace-events must be empty (set at runtime, the harness
# default is KEA): with notifications on, every write runs on main and the lanes never engage. Servers run io-threads 5 and
# io-threads-parallel-writes-min 1 so every lane-eligible segment goes to the lanes.

set ::w6_open {}

proc w6_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    lappend ::w6_open [list $fd [lindex [fconfigure $fd -sockname] 2]]
    return $fd
}

proc w6_close_leftovers {} {
    foreach entry $::w6_open {
        lassign $entry fd lport
        if {[catch {lindex [fconfigure $fd -sockname] 2} cur] || $cur ne $lport} continue
        catch {close $fd}
    }
    set ::w6_open {}
}

proc w6_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w6_read_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            set n [string range $line 1 end]
            fconfigure $fd -translation binary
            set data [read $fd [expr {$n + 2}]]
            fconfigure $fd -translation {crlf binary}
            return [string range $data 0 end-2]
        }
        {*} - {~} - {>} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w6_read_reply $fd] }
            return $out
        }
        {%} {
            set out {}
            for {set i 0} {$i < 2 * [string range $line 1 end]} {incr i} { lappend out [w6_read_reply $fd] }
            return $out
        }
        {_} { return {(nil)} }
        {:} { return [string range $line 1 end] }
        {,} { return [string range $line 1 end] }
        default { return $line }
    }
}

proc w6_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [w6_read_reply $fd] }
    return $out
}

proc w6_info {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

proc w6_wait_fastpath_clients {c n} {
    wait_for_condition 200 20 {
        [w6_info $c fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
}

# n raw db-0 sockets, all on the fast path, the only ones open on that server.
proc w6_fast_sockets {c host port n} {
    w6_close_leftovers
    w6_wait_fastpath_clients $c 0
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [w6_socket $host $port]
        puts -nonewline $fd [w6_resp ping]
        flush $fd
        assert_equal {+PONG} [w6_read_reply $fd]
        lappend fds $fd
    }
    w6_wait_fastpath_clients $c $n
    return $fds
}

proc w6_send {fd buf} {
    puts -nonewline $fd $buf
    flush $fd
}

set ::w6_overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}

# ---------------------------------------------------------------------------------------------
# Item 5 (TXS-04, TXS-05): a MULTI; INCR a; INCR b; EXEC writer on main against fast GET readers
# and split MGET readers on the lanes. a and b live in different shards (checked with DEBUG).
# ---------------------------------------------------------------------------------------------
start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
    r client setname w6-control
    r select 0
    r config set notify-keyspace-events {}
    set host [srv 0 host]
    set port [srv 0 port]

    test {Wave2 #5: MULTI INCR a INCR b EXEC is atomic for fast GET pairs and split MGET readers} {
        r del w6:a w6:b
        r mset w6:a 0 w6:b 0
        set rounds 300
        # writer (main path: MULTI is not lane eligible), 2 split-MGET readers, 2 GET-pair readers
        set fds [w6_fast_sockets r $host $port 5]
        set spec0 [w6_info r fastpath_speculated]
        set lane0 [w6_info r fastpath_write_lane_entries]
        set bar0 [w6_info r fastpath_write_barriers]
        set wbuf ""
        for {set i 0} {$i < $rounds} {incr i} {
            append wbuf [w6_resp multi] [w6_resp incr w6:a] [w6_resp incr w6:b] [w6_resp exec]
        }
        set mbuf ""
        set gbuf ""
        for {set i 0} {$i < $rounds} {incr i} {
            append mbuf [w6_resp mget w6:a w6:b]
            append gbuf [w6_resp get w6:a] [w6_resp get w6:b]
        }
        w6_send [lindex $fds 1] $mbuf
        w6_send [lindex $fds 2] $mbuf
        w6_send [lindex $fds 3] $gbuf
        w6_send [lindex $fds 4] $gbuf
        w6_send [lindex $fds 0] $wbuf
        set bad 0
        foreach c {1 2} {
            set fd [lindex $fds $c]
            for {set i 0} {$i < $rounds} {incr i} {
                set got [w6_read_reply $fd]
                if {[lindex $got 0] ne [lindex $got 1]} { incr bad; set example $got }
            }
        }
        foreach c {3 4} {
            set fd [lindex $fds $c]
            for {set i 0} {$i < $rounds} {incr i} {
                set a [w6_read_reply $fd]
                set b [w6_read_reply $fd]
                # a was read first: b, read later, can only be larger or equal
                if {$b < $a} { incr bad; set example [list $a $b] }
            }
        }
        set fd [lindex $fds 0]
        for {set i 0} {$i < $rounds} {incr i} {
            assert_equal {+OK} [w6_read_reply $fd]
            assert_equal {+QUEUED} [w6_read_reply $fd]
            assert_equal {+QUEUED} [w6_read_reply $fd]
            set ex [w6_read_reply $fd]
            assert_equal [lindex $ex 0] [lindex $ex 1]
        }
        if {$bad} { fail "$bad reader observations with a != b, e.g. $example" }
        assert_equal $rounds [r get w6:a]
        assert_equal $rounds [r get w6:b]
        # engagement: fast GETs were speculated and the split MGETs ran on the lanes
        assert_morethan [w6_info r fastpath_speculated] $spec0
        assert_morethan [w6_info r fastpath_write_lane_entries] $lane0
        foreach fd $fds { close $fd }
    }
}

# ---------------------------------------------------------------------------------------------
# Item 15 (CB-14): blocking list and zset pops served FIFO when the push comes from a lane pipeline.
# ---------------------------------------------------------------------------------------------
start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
    r client setname w6-control
    r select 0
    r config set notify-keyspace-events {}
    set host [srv 0 host]
    set port [srv 0 port]

    test {Wave2 #15: three BLPOP clients on one key are served FIFO by RPUSH k a b c from a lane pipeline} {
        r del w6:bl
        set blockers {}
        for {set i 0} {$i < 3} {incr i} {
            set rd [valkey_deferring_client]
            $rd client setname w6-blocker$i
            $rd select 0
            $rd read; $rd read
            $rd blpop w6:bl 0
            lappend blockers $rd
            wait_for_blocked_clients_count [expr {$i + 1}]
        }
        set fds [w6_fast_sockets r $host $port 1]
        set fd [lindex $fds 0]
        set lane0 [w6_info r fastpath_write_lane_entries]
        set buf ""
        for {set i 0} {$i < 50} {incr i} { append buf [w6_resp set w6:bl:pad$i $i] }
        append buf [w6_resp rpush w6:bl a b c]
        w6_send $fd $buf
        set got [w6_read_replies $fd 51]
        assert_equal 3 [lindex $got 50]
        set i 0
        foreach rd $blockers {
            assert_equal [list w6:bl [lindex {a b c} $i]] [$rd read]
            $rd close
            incr i
        }
        assert_equal 0 [r exists w6:bl]
        assert_morethan [w6_info r fastpath_write_lane_entries] $lane0
        close $fd
    }

    test {Wave2 #15: BZPOPMIN, BLMPOP and BLMOVE are woken by lane pipelines with stock results} {
        r del w6:z w6:l w6:dst
        set rz [valkey_deferring_client]
        $rz client setname w6-rz
        $rz select 0
        $rz read; $rz read
        $rz bzpopmin w6:z 0
        wait_for_blocked_clients_count 1
        set rm [valkey_deferring_client]
        $rm client setname w6-rm
        $rm select 0
        $rm read; $rm read
        $rm blmpop 0 1 w6:l LEFT COUNT 2
        wait_for_blocked_clients_count 2
        set rv [valkey_deferring_client]
        $rv client setname w6-rv
        $rv select 0
        $rv read; $rv read
        $rv blmove w6:l w6:dst RIGHT LEFT 0
        wait_for_blocked_clients_count 3
        set fds [w6_fast_sockets r $host $port 2]
        set lane0 [w6_info r fastpath_write_lane_entries]
        # the destination of BLMOVE is also written by lane commands on another connection
        set b1 ""
        for {set i 0} {$i < 20} {incr i} { append b1 [w6_resp rpush w6:dst d$i] }
        w6_send [lindex $fds 1] $b1
        set b0 [w6_resp zadd w6:z 5 m5 1 m1 3 m3]
        append b0 [w6_resp rpush w6:l x1 x2 x3 x4]
        w6_send [lindex $fds 0] $b0
        assert_equal {3 4} [w6_read_replies [lindex $fds 0] 2]
        w6_read_replies [lindex $fds 1] 20
        assert_equal {w6:z m1 1} [$rz read]
        assert_equal {w6:l {x1 x2}} [$rm read]
        assert_equal x4 [$rv read]
        assert_equal {x3} [r lrange w6:l 0 -1]
        assert_equal x4 [r lindex w6:dst 0]
        assert_equal 21 [r llen w6:dst]
        assert_equal {m3 3 m5 5} [r zrange w6:z 0 -1 withscores]
        $rz close; $rm close; $rv close
        assert_morethan [w6_info r fastpath_write_lane_entries] $lane0
        foreach fd $fds { close $fd }
    }
}

# ---------------------------------------------------------------------------------------------
# Item 37 (STR-01, STR-02): OBJECT ENCODING and MEMORY USAGE of strings written through the lanes
# (io-threads 5) equal those written on main (io-threads 1), over key and value length boundaries.
# ---------------------------------------------------------------------------------------------
proc w6_str_write_buf {} {
    set buf ""
    foreach kl {1 2 3 7 8 15 16 23 24 31 32 39 40 43 44 45 47 48 55 56 63 64 65 100} {
        set key [string repeat k $kl]
        for {set vl 0} {$vl <= 200} {incr vl} {
            set v [string repeat v $vl]
            append buf [w6_resp set s:$kl:$vl:$key $v]
            append buf [w6_resp set x:$kl:$vl:$key $v ex 1000]
            append buf [w6_resp setex e:$kl:$vl:$key 1000 $v]
            append buf [w6_resp psetex p:$kl:$vl:$key 1000000 $v]
            append buf [w6_resp mset m:$kl:$vl:$key $v]
            append buf [w6_resp set g:$kl:$vl:$key old] [w6_resp getset g:$kl:$vl:$key $v]
        }
    }
    return $buf
}

proc w6_str_query {host port} {
    # pipelined OBJECT ENCODING / MEMORY USAGE / STRLEN for every key, on one raw db-0 socket
    set keys {}
    set buf ""
    foreach kl {1 2 3 7 8 15 16 23 24 31 32 39 40 43 44 45 47 48 55 56 63 64 65 100} {
        set key [string repeat k $kl]
        for {set vl 0} {$vl <= 200} {incr vl} {
            foreach pfx {s x e p m g} {
                set k $pfx:$kl:$vl:$key
                lappend keys $k
                append buf [w6_resp object encoding $k] [w6_resp memory usage $k] [w6_resp strlen $k]
            }
        }
    }
    set fd [w6_socket $host $port]
    w6_send $fd $buf
    set out {}
    foreach k $keys { lappend out [list $k {*}[w6_read_replies $fd 3]] }
    close $fd
    return $out
}

start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 1}} {
    set stock [srv 0 client]
    $stock client setname w6-stock
    $stock select 0
    $stock config set notify-keyspace-events {}
    set shost [srv 0 host]
    set sport [srv 0 port]
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
        r client setname w6-control
    r select 0
    r config set notify-keyspace-events {}
        set host [srv 0 host]
        set port [srv 0 port]
        test {Wave2 #37: SET family at io-threads 5 (lanes) gives the same encoding and memory usage as io-threads 1} {
            set buf [w6_str_write_buf]
            set n [expr {24 * 201 * 7}]
            set sfd [w6_socket $shost $sport]
            w6_send $sfd $buf
            w6_read_replies $sfd $n
            close $sfd
            set fds [w6_fast_sockets r $host $port 1]
            set lane0 [w6_info r fastpath_write_lane_entries]
            w6_send [lindex $fds 0] $buf
            w6_read_replies [lindex $fds 0] $n
            close [lindex $fds 0]
            assert_morethan [w6_info r fastpath_write_lane_entries] [expr {$lane0 + $n / 2}]
            set a [w6_str_query $shost $sport]
            set b [w6_str_query $host $port]
            set diff 0
            foreach x $a y $b {
                if {$x ne $y} { incr diff; if {$diff <= 3} { puts "  encoding/memory differs: stock=$x lanes=$y" } }
            }
            assert_equal 0 $diff
            assert_equal {s:1:0:k embstr} [lrange [lindex $b 0] 0 1]
            assert_equal [llength $a] [llength $b]
            assert_equal [$stock dbsize] [r dbsize]
        }
    }
}
