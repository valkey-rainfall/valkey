# Background INCR load from a raw fast-path socket (db 0, no client name, so the server admits
# it to the fast path). Pipelines INCR in chunks and reads every reply. Stops on the first error
# reply (for example -READONLY after a failover demoted the server), on EOF, or when the stop
# file exists. Writes two lines to outfile: the largest integer reply seen (the largest value the
# server acknowledged to this client) and the number of acknowledged INCRs.
#
# usage: tclsh w4_incr_load.tcl host port key outfile [chunk]

proc w4_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w4_read_line {fd} {
    set line [gets $fd]
    if {[eof $fd]} { return {EOF} }
    return $line
}

proc w4_load {host port key outfile chunk} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    puts -nonewline $fd [w4_resp ping]
    flush $fd
    set pong [w4_read_line $fd]
    set max 0
    set acked 0
    set stop_reason running
    set burst ""
    for {set i 0} {$i < $chunk} {incr i} { append burst [w4_resp incr $key] }
    while {$stop_reason eq "running"} {
        if {[file exists $outfile.stop]} { set stop_reason stopfile; break }
        if {[catch {puts -nonewline $fd $burst; flush $fd}]} { set stop_reason writefail; break }
        for {set i 0} {$i < $chunk} {incr i} {
            set line [w4_read_line $fd]
            switch -- [string index $line 0] {
                {:} {
                    set v [string range $line 1 end]
                    incr acked
                    if {$v > $max} { set max $v }
                }
                default {
                    set stop_reason [string map {"\r" "" "\n" ""} $line]
                    break
                }
            }
        }
    }
    catch {close $fd}
    set out [open $outfile w]
    puts $out $max
    puts $out $acked
    puts $out $stop_reason
    close $out
}

w4_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [expr {[llength $argv] > 4 ? [lindex $argv 4] : 64}]
