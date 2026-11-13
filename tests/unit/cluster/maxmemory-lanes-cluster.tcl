# Write lanes with maxmemory set in cluster mode: a segment whose growth bound fits under the limit runs
# on slot lanes; remote keys still get MOVED in order.

proc mmc_socket {idx} {
    set fd [socket [srv $idx host] [srv $idx port]]
    fconfigure $fd -translation binary -blocking 1 -buffering none
    return $fd
}

proc mmc_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc mmc_read {fd} {
    set line [string trimright [gets $fd] "\r"]
    switch -- [string index $line 0] {
        {$} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set v [read $fd [expr {$n + 2}]]
            return [string range $v 0 end-2]
        }
        default { return $line }
    }
}

proc mmc_pipeline {fd cmds} {
    set out ""
    foreach c $cmds { append out [mmc_resp {*}$c] }
    puts -nonewline $fd $out
    flush $fd
    set replies {}
    foreach c $cmds { lappend replies [mmc_read $fd] }
    return $replies
}

proc mmc_fp {idx field} {
    return [getInfoProperty [[srv $idx client] info fastpath] $field]
}

start_cluster 2 0 {tags {external:skip cluster staged-loop maxmemory tls:skip logreqres:skip} overrides {io-threads 4 io-threads-parallel-writes-min 1 cluster-slot-stats-enabled no maxmemory 1gb maxmemory-policy allkeys-lru}} {
    test {Write lanes in cluster mode under maxmemory: local writes run on slot lanes, remote ones get MOVED in order} {
        set port1 [srv -1 port]
        set local {}
        set remote {}
        for {set i 0} {[llength $local] < 200 || [llength $remote] < 20} {incr i} {
            set k mmc:$i
            if {[catch {[srv 0 client] exists $k} err]} {
                if {[llength $remote] < 20} { lappend remote $k }
            } elseif {[llength $local] < 200} {
                lappend local $k
            }
        }
        set fd [mmc_socket 0]
        assert_equal +PONG [lindex [mmc_pipeline $fd {{PING}}] 0]
        set phases [mmc_fp 0 fastpath_write_maxmemory_lane_phases]
        set cmds {}
        set expect {}
        foreach k $local {
            lappend cmds [list SET $k v.$k] [list GET $k] [list INCR "{$k}:n"]
            lappend expect {+OK} v.$k {:1}
        }
        foreach r [mmc_pipeline $fd $cmds] e $expect { assert_equal $e $r }
        set cmds {}
        foreach k $remote { lappend cmds [list SET $k x] [list GET [lindex $local 0]] }
        set i 0
        foreach r [mmc_pipeline $fd $cmds] {
            if {$i % 2 == 0} { assert_match "-MOVED * *:$port1" $r } else { assert_equal v.[lindex $local 0] $r }
            incr i
        }
        close $fd
        assert {[mmc_fp 0 fastpath_write_maxmemory_lane_phases] > $phases}
        assert {[mmc_fp 0 staged_lanes_veto_clients] ne {maxmemory}}
    }
}
