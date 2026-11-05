# Introspection of fast-path clients: CLIENT LIST, INFO, the commandlog and latency samples report
# fast-path and write-lane commands as the main path would. Fast-path clients here are raw db-0
# sockets: the test framework's client sends SELECT first, which moves a connection off the fast path.

proc fpi_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    return $fd
}

proc fpi_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc fpi_read_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            # The channel reads CRLF as a line end: gather lines until the bulk's bytes are in.
            set len [string range $line 1 end]
            set out [gets $fd]
            while {[string length $out] < $len} { append out "\r\n" [gets $fd] }
            return $out
        }
        {*} {
            set out {}
            for {set i 0} {$i < [string range $line 1 end]} {incr i} { lappend out [fpi_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc fpi_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [fpi_read_reply $fd] }
    return $out
}

proc fpi_info_field {section field} {
    return [getInfoProperty [r info $section] $field]
}

proc fpi_wait_fastpath_clients {n} {
    wait_for_condition 200 20 {
        [fpi_info_field fastpath fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# The CLIENT LIST line of the raw socket fd.
proc fpi_client_line {fd} {
    set lport [lindex [fconfigure $fd -sockname] 2]
    foreach line [split [r client list] "\n"] {
        if {[string match "* addr=*:$lport *" $line]} { return $line }
    }
    fail "no CLIENT LIST line for local port $lport"
}

proc fpi_client_field {fd field} {
    if {![regexp " $field=(\[^ \]*)" [fpi_client_line $fd] -> v]} { fail "no $field= field" }
    return $v
}

start_server {tags {"fastpath external:skip tls:skip logreqres:skip"} overrides {io-threads 3}} {
    # A named client stays off the fast path.
    r client setname fpi-control
    r select 0

    test {CLIENT LIST cmd= names the last command a fast-path client ran} {
        set fd [fpi_socket]
        puts -nonewline $fd [fpi_resp set fpi:k 1]
        flush $fd
        assert_equal {+OK} [fpi_read_reply $fd]
        fpi_wait_fastpath_clients 1
        assert_equal set [fpi_client_field $fd cmd]
        # Executed on the fast path.
        puts -nonewline $fd [fpi_resp incr fpi:k]
        flush $fd
        assert_equal {:2} [fpi_read_reply $fd]
        assert_equal incr [fpi_client_field $fd cmd]
        # Answered on the IO thread that read it.
        set spec [fpi_info_field fastpath fastpath_speculated]
        puts -nonewline $fd "[fpi_resp get fpi:k][fpi_resp get fpi:k]"
        flush $fd
        assert_equal {2 2} [fpi_read_replies $fd 2]
        assert_morethan [fpi_info_field fastpath fastpath_speculated] $spec
        assert_equal get [fpi_client_field $fd cmd]
        assert_equal 1 [fpi_info_field fastpath fastpath_clients]
        close $fd
        fpi_wait_fastpath_clients 0
    }
}

start_server {tags {"fastpath external:skip tls:skip logreqres:skip"} overrides {io-threads 3}} {
    # A named client stays off the fast path.
    r client setname fpi-control
    r select 0

    test {INFO fastpath_clients counts the fast-path client that asks} {
        set fd [fpi_socket]
        puts -nonewline $fd [fpi_resp set fpi:k 1]
        flush $fd
        assert_equal {+OK} [fpi_read_reply $fd]
        fpi_wait_fastpath_clients 1
        puts -nonewline $fd [fpi_resp info fastpath]
        flush $fd
        assert_equal 1 [getInfoProperty [fpi_read_reply $fd] fastpath_clients]
        # It rejoins after INFO, and asks again.
        fpi_wait_fastpath_clients 1
        puts -nonewline $fd "[fpi_resp incr fpi:k][fpi_resp info fastpath]"
        flush $fd
        assert_equal {:2} [fpi_read_reply $fd]
        assert_equal 1 [getInfoProperty [fpi_read_reply $fd] fastpath_clients]
        close $fd
        fpi_wait_fastpath_clients 0
    }
}

proc fpi_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

# The keys of the commandlog's SET entries whose key starts with prefix, oldest entry first.
proc fpi_logged_set_keys {c prefix} {
    set keys {}
    foreach e [lreverse [$c slowlog get -1]] {
        set argv [lindex $e 3]
        if {[string tolower [lindex $argv 0]] eq {set} && [string match $prefix* [lindex $argv 1]]} {
            lappend keys [lindex $argv 1]
        }
    }
    return $keys
}

start_server {tags {"fastpath external:skip tls:skip logreqres:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    r client setname fpi-control
    r select 0
    r config set notify-keyspace-events ""

    test {Write lanes: commandlog entries follow command order} {
        r config set slowlog-log-slower-than 0
        r config set slowlog-max-len 1000
        r slowlog reset
        set fd [fpi_socket]
        puts -nonewline $fd [fpi_resp ping]
        flush $fd
        assert_equal {+PONG} [fpi_read_reply $fd]
        fpi_wait_fastpath_clients 1
        set before [fpi_lane_entries r]
        set buf ""
        set expect {}
        for {set i 0} {$i < 200} {incr i} {
            append buf [fpi_resp set fpi:o$i $i]
            lappend expect fpi:o$i
        }
        puts -nonewline $fd $buf
        flush $fd
        fpi_read_replies $fd 200
        close $fd
        assert_morethan_equal [expr {[fpi_lane_entries r] - $before}] 100
        assert_equal $expect [fpi_logged_set_keys r fpi:o]
        r config set slowlog-log-slower-than 10000
    }
}

start_server {tags {"fastpath repl external:skip tls:skip logreqres:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary client setname fpi-control
    $primary select 0
    $primary config set notify-keyspace-events ""
    start_server {overrides {io-threads 3 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
        set replica [srv 0 client]
        $replica client setname fpi-replica-control
        $replica select 0
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_condition 100 50 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "replica did not connect"
        }

        test {Replica lanes: commandlog entries follow the stream's order} {
            $replica config set slowlog-log-slower-than 0
            $replica config set slowlog-max-len 1000
            $replica slowlog reset
            set before [getInfoProperty [$replica info stats] repl_parallel_applied]
            set fd [socket $primary_host $primary_port]
            fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
            set buf ""
            set expect {}
            for {set i 0} {$i < 300} {incr i} {
                append buf [fpi_resp set fpi:r$i $i]
                lappend expect fpi:r$i
            }
            puts -nonewline $fd $buf
            flush $fd
            fpi_read_replies $fd 300
            close $fd
            wait_for_ofs_sync $primary $replica
            assert_morethan_equal [expr {[getInfoProperty [$replica info stats] repl_parallel_applied] - $before}] 100
            assert_equal $expect [fpi_logged_set_keys $replica fpi:r]
            $replica config set slowlog-log-slower-than 10000
        }
    }
}

proc fpi_histogram_calls {c cmd} {
    set h [$c latency histogram $cmd]
    if {[llength $h] == 0} { return 0 }
    return [dict get [lindex $h 1] calls]
}

start_server {tags {"fastpath external:skip tls:skip logreqres:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1}} {
    r client setname fpi-control
    r select 0
    r config set notify-keyspace-events ""

    test {Write lanes: a command split over lanes is logged and timed once, whole} {
        r config set slowlog-log-slower-than 0
        r config set slowlog-max-len 1000
        r config set commandlog-reply-larger-than 0
        r config resetstat
        r slowlog reset
        r commandlog reset large-reply
        set fd [fpi_socket]
        puts -nonewline $fd [fpi_resp ping]
        flush $fd
        assert_equal {+PONG} [fpi_read_reply $fd]
        fpi_wait_fastpath_clients 1
        set before [fpi_lane_entries r]
        set mset {mset}
        set mget {mget}
        for {set k 0} {$k < 12} {incr k} {
            lappend mset fpi:m$k v$k
            lappend mget fpi:m$k
        }
        # A long argument is kept as the commandlog keeps it: its first bytes and its length.
        set long [string repeat x 200]
        lappend mset fpi:mlong $long
        lappend mget fpi:mlong
        set buf ""
        for {set i 0} {$i < 20} {incr i} { append buf [fpi_resp set fpi:s$i $i] }
        append buf [fpi_resp {*}$mset] [fpi_resp {*}$mget]
        puts -nonewline $fd $buf
        flush $fd
        fpi_read_replies $fd 22
        close $fd
        # Each split command added one entry per part to the lanes.
        assert_morethan [expr {[fpi_lane_entries r] - $before}] 22
        set logged_mset {}
        set logged_mget {}
        foreach e [r slowlog get -1] {
            set argv [lindex $e 3]
            switch [string tolower [lindex $argv 0]] {
                mset { lappend logged_mset $argv }
                mget { lappend logged_mget $argv }
            }
        }
        set kept "[string repeat x 128]... (72 more bytes)"
        assert_equal [list [lreplace $mset end end $kept]] $logged_mset
        assert_equal [list $mget] $logged_mget
        assert_equal 1 [fpi_histogram_calls r mset]
        assert_equal 1 [fpi_histogram_calls r mget]
        assert_match {*cmdstat_mset:calls=1,*} [r info commandstats]
        # The reply size is the whole reply's: *13, ten 2-byte values, two 3-byte ones and the long one.
        set sizes {}
        foreach e [r commandlog get -1 large-reply] {
            if {[string tolower [lindex $e 3 0]] eq {mget}} { lappend sizes [lindex $e 2] }
        }
        assert_equal [expr {5 + 10 * 8 + 2 * 9 + 208}] $sizes
        r config set commandlog-reply-larger-than 1048576
        r config set slowlog-log-slower-than 10000
    }
}
