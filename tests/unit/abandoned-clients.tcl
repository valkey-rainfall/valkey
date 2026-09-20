# A client that sends a command, then closes its socket while the main thread
# is stalled, has left before the server reaches its input. With
# client-abandon-threshold-ms set, the server frees such a client instead of
# executing the command and writing a reply nobody will read. A client that
# half-closes (shutdown of its write side) on a healthy server is legal and
# must still be served.

proc abandoned_skipped {} {
    return [s abandoned_clients_skipped]
}

# Open a raw socket, select the test client's database, send $cmd, then close
# it (or half-close when $how is "write"). Returns the channel when
# half-closed so the caller can read the SELECT reply and then the command's.
proc send_then_close {cmd how} {
    set db [expr {$::singledb ? 0 : 9}]
    set fd [socket [srv host] [srv port]]
    fconfigure $fd -translation binary
    puts -nonewline $fd "SELECT $db\r\n$cmd\r\n"
    flush $fd
    if {$how eq "write"} {
        chan close $fd write
        return $fd
    }
    close $fd
    return {}
}

# Read the two replies of a half-closed channel: +OK for SELECT, then the
# command's reply, returned trimmed.
proc read_half_closed_reply {fd} {
    fconfigure $fd -blocking 1
    assert_equal "+OK" [string trim [gets $fd]]
    set reply [string trim [gets $fd]]
    close $fd
    return $reply
}

foreach io_threads {1 2} {
    start_server [list tags {"abandoned-clients needs:debug external:skip"} overrides [list io-threads $io_threads client-abandon-threshold-ms 200]] {
        test "Peer-closed client's pending command is skipped after a stall (io-threads $io_threads)" {
            r set ctr 0
            set before [abandoned_skipped]

            # Stall the main thread, then let a client send INCR and hang up
            # during the stall. Its input is ~0.8 s old when main resumes,
            # older than the 200 ms threshold.
            set rd [valkey_deferring_client]
            $rd debug sleep 0.8
            after 100
            send_then_close "INCR ctr" full
            assert_equal OK [$rd read]
            $rd close

            wait_for_condition 50 100 {
                [abandoned_skipped] == $before + 1
            } else {
                fail "abandoned client was not skipped"
            }
            assert_equal 0 [r get ctr]
        }

        test "Half-closed client on a healthy server still gets its reply (io-threads $io_threads)" {
            r set ctr 0
            set before [abandoned_skipped]
            set fd [send_then_close "INCR ctr" write]
            assert_equal ":1" [read_half_closed_reply $fd]
            assert_equal 1 [r get ctr]
            assert_equal $before [abandoned_skipped]
        }

        test "client-abandon-threshold-ms 0 disables the skip (io-threads $io_threads)" {
            r config set client-abandon-threshold-ms 0
            r set ctr 0
            set before [abandoned_skipped]

            set rd [valkey_deferring_client]
            $rd debug sleep 0.8
            after 100
            send_then_close "INCR ctr" full
            assert_equal OK [$rd read]
            $rd close

            # The command is executed; the write to the dead socket fails and
            # the client is freed by the stock path.
            wait_for_condition 50 100 {
                [r get ctr] == 1
            } else {
                fail "command was not executed with the skip disabled"
            }
            assert_equal $before [abandoned_skipped]
            r config set client-abandon-threshold-ms 200
        }

        test "Threshold guards a half-close whose input is still fresh (io-threads $io_threads)" {
            # A stall shorter than the threshold: the half-closed client's
            # input is younger than the threshold when main resumes, so it is
            # served, not skipped.
            r config set client-abandon-threshold-ms 5000
            r set ctr 0
            set before [abandoned_skipped]

            set rd [valkey_deferring_client]
            $rd debug sleep 0.5
            after 100
            set fd [send_then_close "INCR ctr" write]
            assert_equal OK [$rd read]
            $rd close

            assert_equal ":1" [read_half_closed_reply $fd]
            assert_equal 1 [r get ctr]
            assert_equal $before [abandoned_skipped]
            r config set client-abandon-threshold-ms 200
        }
    }
}
