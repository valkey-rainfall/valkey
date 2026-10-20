# IO-thread reads answer a read only when every command in it is a GET: a read that mixes GETs with
# another command goes to main whole, so one pipeline's replies are never split between the IO
# thread and main.

proc fps_speculated {} { getInfoProperty [r info fastpath] fastpath_speculated }

# A raw db-0 connection (a SELECT would move it off the fast path) that sends a pipeline in one write.
proc fps_open {} {
    set s [socket [srv 0 host] [srv 0 port]]
    fconfigure $s -translation binary -buffering full
    return $s
}

proc fps_resp {args} {
    set out "*[llength $args]\r\n"
    foreach a $args { append out "\$[string length $a]\r\n$a\r\n" }
    return $out
}

proc fps_send {s cmds} {
    set payload ""
    foreach c $cmds { append payload [fps_resp {*}$c] }
    puts -nonewline $s $payload
    flush $s
}

# Reads one RESP2 bulk string or simple-string reply.
proc fps_read {s} {
    set line [string trimright [gets $s] "\r"]
    switch -- [string index $line 0] {
        + { return [string range $line 1 end] }
        $ {
            set n [string range $line 1 end]
            if {$n < 0} { return {} }
            set v [read $s [expr {$n + 2}]]
            return [string range $v 0 end-2]
        }
        default { fail "unexpected reply: $line" }
    }
}

start_server {tags {"fastpath external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes save ""}} {
    r select 0
    r set k v

    test {IO-thread reads answer an all-GET pipeline} {
        set s [fps_open]
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_clients] >= 1
        } else { fail "client did not attach: [r info fastpath]" }
        set before [fps_speculated]
        for {set round 0} {$round < 50 && [fps_speculated] == $before} {incr round} {
            fps_send $s [lrepeat 10 {get k}]
            for {set i 0} {$i < 10} {incr i} { assert_equal v [fps_read $s] }
        }
        assert_morethan [fps_speculated] $before
        close $s
    }

    test {IO-thread reads leave a pipeline that mixes GETs with a write to main} {
        set s [fps_open]
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_clients] >= 1
        } else { fail "client did not attach: [r info fastpath]" }
        # Engage the fast path first, so the mixed pipelines below run where speculation could apply.
        set before [fps_speculated]
        for {set round 0} {$round < 50 && [fps_speculated] == $before} {incr round} {
            fps_send $s [lrepeat 10 {get k}]
            for {set i 0} {$i < 10} {incr i} { assert_equal v [fps_read $s] }
        }
        assert_morethan [fps_speculated] $before
        set before [fps_speculated]
        for {set round 0} {$round < 50} {incr round} {
            fps_send $s [list {get k} {get k} {get k} {get k} [list set k v$round] {get k} {get k}]
            for {set i 0} {$i < 4} {incr i} { assert_equal [expr {$round == 0 ? "v" : "v[expr {$round - 1}]"}] [fps_read $s] }
            assert_equal OK [fps_read $s]
            assert_equal v$round [fps_read $s]
            assert_equal v$round [fps_read $s]
        }
        assert_equal $before [fps_speculated]
        close $s
        r set k v
    }
}
