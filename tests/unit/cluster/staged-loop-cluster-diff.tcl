# Differential test: the same pipeline on a fast-path client (raw socket, nothing sent before its
# commands) and on a main-path client (CLIENT SETNAME keeps it on main, see fpSessionEligible) must
# give byte-identical replies in cluster mode: owned slot, remote slot (MOVED), unbound slot
# (CLUSTERDOWN Hash slot not served), cluster FAIL with cluster-allow-reads-when-down yes and no,
# and a READONLY replica whose primary is failed. Each case asserts fast-path engagement with INFO
# counters, so a pass means the fast path, the lanes or the fast GET really answered. Slot stats are off
# here because with cluster-slot-stats-enabled yes every GET runs on main (fastget.c).

proc scd_socket {idx} {
    set fd [socket [srv $idx host] [srv $idx port]]
    fconfigure $fd -translation binary -blocking 1 -buffering none
    return $fd
}

proc scd_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# One RESP2 reply as its exact bytes (line with CRLF, plus the bulk payload with CRLF).
proc scd_read_raw {fd} {
    set line [gets $fd]
    append line "\n"
    if {[string index $line 0] eq "$"} {
        set n [string range [string trimright $line "\r\n"] 1 end]
        if {$n >= 0} { append line [read $fd [expr {$n + 2}]] }
    }
    return $line
}

proc scd_pipeline_raw {fd cmds} {
    set out ""
    foreach c $cmds { append out [scd_resp {*}$c] }
    puts -nonewline $fd $out
    set replies {}
    foreach c $cmds { lappend replies [scd_read_raw $fd] }
    return $replies
}

proc scd_info {idx section field} {
    return [getInfoProperty [[srv $idx client] info $section] $field]
}

# A fast-path client: it has sent only PING, so it joins the fast path (fastpath_clients grows).
proc scd_fast_client {idx} {
    set before [scd_info $idx fastpath fastpath_clients]
    set fd [scd_socket $idx]
    assert_equal "+PONG\r\n" [lindex [scd_pipeline_raw $fd {{PING}}] 0]
    wait_for_condition 100 20 {[scd_info $idx fastpath fastpath_clients] > $before} else {
        fail "client did not join the fast path"
    }
    return $fd
}

# A main-path client: CLIENT SETNAME is a session exclusion, so main executes all its commands.
proc scd_main_client {idx} {
    set fd [scd_socket $idx]
    assert_equal "+OK\r\n" [lindex [scd_pipeline_raw $fd {{CLIENT SETNAME mainpath}}] 0]
    return $fd
}

# Run $cmds on both clients and compare every reply byte for byte. Returns the fast replies.
proc scd_compare {idx cmds} {
    set fast [scd_fast_client $idx]
    set main [scd_main_client $idx]
    set rf [scd_pipeline_raw $fast $cmds]
    set rm [scd_pipeline_raw $main $cmds]
    close $fast
    close $main
    for {set i 0} {$i < [llength $cmds]} {incr i} {
        if {[lindex $rf $i] ne [lindex $rm $i]} {
            fail "reply $i differs for [lindex $cmds $i]: fast {[lindex $rf $i]} main {[lindex $rm $i]}"
        }
    }
    return $rf
}

proc scd_key_on {idx prefix} {
    set myid [[srv $idx client] cluster myid]
    for {set i 0} {$i < 10000} {incr i} {
        set key "$prefix$i"
        set slot [[srv $idx client] cluster keyslot $key]
        if {[scd_slot_owner $idx $slot] eq $myid} { return $key }
    }
    fail "no key found on node $idx"
}

proc scd_slot_owner {idx slot} {
    foreach n [get_cluster_nodes [expr {abs($idx)}]] {
        foreach r [dict get $n slots] {
            set lo [lindex [split $r -] 0]
            set hi [lindex [split $r -] end]
            if {[string is integer -strict $lo] && $slot >= $lo && $slot <= $hi} { return [dict get $n id] }
        }
    }
    return ""
}

start_cluster 3 1 {tags {external:skip cluster staged-loop tls:skip logreqres:skip} overrides {io-threads 5 io-threads-parallel-writes-min 1 cluster-slot-stats-enabled no cluster-replica-no-failover yes}} {

    test {Differential: staged loop with lanes runs in cluster mode} {
        wait_for_cluster_state ok
        foreach idx {0 -1 -2 -3} {
            wait_for_log_messages $idx {"*Staged event loop started*"} 0 100 50
            [srv $idx client] config set notify-keyspace-events ""
        }
    }

    set local [scd_key_on 0 "local"]
    set remote [scd_key_on -1 "remote"]
    set lslot [[srv 0 client] cluster keyslot $local]

    test {Differential: owned slot and MOVED, fast GET and lanes vs main, byte-identical} {
        set p [srv 0 client]
        set spec0 [scd_info 0 fastget fastget_speculated_commands]
        set lane0 [scd_info 0 fastpath fastpath_write_lane_entries]
        set cmds {}
        for {set i 0} {$i < 10} {incr i} {
            lappend cmds [list SET $local v$i] [list GET $local] [list GET $remote] [list SET $remote x] \
                [list EXISTS "{$local}nokey"] [list GET "{$local}nokey"]
        }
        set rf [scd_compare 0 $cmds]
        assert_equal "+OK\r\n" [lindex $rf 0]
        assert_equal "\$2\r\nv0\r\n" [lindex $rf 1]
        assert_match "-MOVED * 127.0.0.1:[srv -1 port]\r\n" [lindex $rf 2]
        assert_match "-MOVED * 127.0.0.1:[srv -1 port]\r\n" [lindex $rf 3]
        assert_equal ":0\r\n" [lindex $rf 4]
        assert_equal "\$-1\r\n" [lindex $rf 5]
        assert {[scd_info 0 fastpath fastpath_write_lane_entries] > $lane0}
        # Pure GET pipelines are speculated on the IO thread (fast GET); compare them too.
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] }
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] [list GET $remote] [list GET "{$local}nokey"] }
        set rf [scd_compare 0 $cmds]
        assert_equal "\$2\r\nv9\r\n" [lindex $rf 0]
        assert_match "-MOVED * 127.0.0.1:[srv -1 port]\r\n" [lindex $rf 11]
        assert_equal "\$-1\r\n" [lindex $rf 12]
        assert {[scd_info 0 fastget fastget_speculated_commands] > $spec0}
    }

    test {Differential: unbound slot gives CLUSTERDOWN Hash slot not served on both paths} {
        set p [srv 0 client]
        foreach idx {0 -1 -2 -3} { [srv $idx client] config set cluster-require-full-coverage no }
        $p set $local bound
        assert_equal OK [$p cluster delslots $lslot]
        wait_for_condition 100 20 {[scd_slot_owner 0 $lslot] eq ""} else { fail "slot still bound" }
        set spec0 [scd_info 0 fastget fastget_speculated_commands]
        set batches0 [scd_info 0 fastpath fastpath_batches]
        set cmds {}
        for {set i 0} {$i < 10} {incr i} {
            lappend cmds [list GET $local] [list SET $local x] [list GET $remote] [list EXISTS "{$local}nokey"]
        }
        set rf [scd_compare 0 $cmds]
        assert_equal "-CLUSTERDOWN Hash slot not served\r\n" [lindex $rf 0]
        assert_equal "-CLUSTERDOWN Hash slot not served\r\n" [lindex $rf 1]
        assert_match "-MOVED *\r\n" [lindex $rf 2]
        assert_equal "-CLUSTERDOWN Hash slot not served\r\n" [lindex $rf 3]
        # The fast GET punts an unbound slot to main; the batch still runs on the staged loop.
        assert {[scd_info 0 fastpath fastpath_batches] > $batches0}
        assert_equal OK [$p cluster addslots $lslot]
        wait_for_condition 100 20 {[scd_slot_owner 0 $lslot] eq [$p cluster myid]} else { fail "slot not re-added" }
        assert_equal bound [$p get $local]
        foreach idx {0 -1 -2 -3} { [srv $idx client] config set cluster-require-full-coverage yes }
    }

    test {Differential: cluster FAIL with allow-reads-when-down no and yes, byte-identical} {
        set p [srv 0 client]
        $p set $local down
        # Pause the third primary: the two others agree it failed, and every node goes to FAIL.
        set third_id [[srv -2 client] cluster myid]
        pause_process [srv -2 pid]
        wait_for_condition 1000 50 {[cluster_has_flag [cluster_get_node_by_id 0 $third_id] fail]} else {
            fail "node 0 did not mark the third primary as failed"
        }
        wait_for_condition 1000 50 {[CI 0 cluster_state] eq "fail"} else { fail "node 0 not in FAIL" }
        $p config set cluster-allow-reads-when-down no
        set batches0 [scd_info 0 fastpath fastpath_batches]
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] [list SET $local x] [list EXISTS "{$local}nokey"] }
        set rf [scd_compare 0 $cmds]
        foreach r $rf { assert_equal "-CLUSTERDOWN The cluster is down\r\n" $r }
        assert {[scd_info 0 fastpath fastpath_batches] > $batches0}
        $p config set cluster-allow-reads-when-down yes
        set batches0 [scd_info 0 fastpath fastpath_batches]
        set rf [scd_compare 0 $cmds]
        for {set i 0} {$i < [llength $rf]} {incr i} {
            if {$i % 3 == 0} {
                assert_equal "\$4\r\ndown\r\n" [lindex $rf $i]
            } elseif {$i % 3 == 1} {
                assert_equal "-CLUSTERDOWN The cluster is down and only accepts read commands\r\n" [lindex $rf $i]
            } else {
                assert_equal ":0\r\n" [lindex $rf $i]
            }
        }
        assert {[scd_info 0 fastpath fastpath_batches] > $batches0}
        assert_equal down [$p get $local]
        $p config set cluster-allow-reads-when-down no
        resume_process [srv -2 pid]
        wait_for_condition 1000 50 {![cluster_has_flag [cluster_get_node_by_id 0 $third_id] fail]} else {
            fail "third primary still failed"
        }
        wait_for_cluster_state ok
    }

    test {Differential: READONLY replica whose primary is failed, byte-identical} {
        set p [srv 0 client]
        set replica [srv -3 client]
        $p set $local onreplica
        wait_for_ofs_sync $p $replica
        set primary_id [$p cluster myid]
        pause_process [srv 0 pid]
        wait_for_condition 1000 50 {[cluster_has_flag [cluster_get_node_by_id 3 $primary_id] fail]} else {
            fail "replica did not mark its primary as failed"
        }
        wait_for_condition 1000 50 {[CI 3 cluster_state] eq "fail"} else { fail "replica not in FAIL" }
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] [list SET $local x] [list EXISTS "{$local}nokey"] }
        foreach allow {no yes} {
            $replica config set cluster-allow-reads-when-down $allow
            set fast [scd_fast_client -3]
            set main [scd_main_client -3]
            assert_equal "+OK\r\n" [lindex [scd_pipeline_raw $fast {{READONLY}}] 0]
            assert_equal "+OK\r\n" [lindex [scd_pipeline_raw $main {{READONLY}}] 0]
            wait_for_condition 100 20 {[scd_info -3 fastpath fastpath_clients] >= 1} else { fail "fast client left" }
            set batches0 [scd_info -3 fastpath fastpath_batches]
            set rf [scd_pipeline_raw $fast $cmds]
            set rm [scd_pipeline_raw $main $cmds]
            close $fast
            close $main
            for {set i 0} {$i < [llength $cmds]} {incr i} {
                if {[lindex $rf $i] ne [lindex $rm $i]} {
                    fail "allow=$allow reply $i differs: fast {[lindex $rf $i]} main {[lindex $rm $i]}"
                }
            }
            if {$allow eq "no"} {
                foreach r $rf { assert_equal "-CLUSTERDOWN The cluster is down\r\n" $r }
            } else {
                assert_equal "\$9\r\nonreplica\r\n" [lindex $rf 0]
                assert_equal "-CLUSTERDOWN The cluster is down and only accepts read commands\r\n" [lindex $rf 1]
                assert_equal ":0\r\n" [lindex $rf 2]
            }
            assert {[scd_info -3 fastpath fastpath_batches] > $batches0}
        }
        $replica config set cluster-allow-reads-when-down no
        resume_process [srv 0 pid]
        wait_for_condition 1000 50 {![cluster_has_flag [cluster_get_node_by_id 3 $primary_id] fail]} else {
            fail "primary still failed"
        }
        wait_for_cluster_state ok
    }
}
