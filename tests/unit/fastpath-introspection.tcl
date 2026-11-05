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
            return [gets $fd]
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
