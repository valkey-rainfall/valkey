set testmodule [file normalize tests/modules/eventloop.so]

# Both servers load the module at startup: with IO threads a MODULE LOAD can run after the event loop's
# before-sleep event, so the new listener's first event is an unmatched after-sleep, which this module counts.
start_server [list tags {"modules"} overrides [list loadmodule $testmodule]] {
    test "Module eventloop sendbytes" {
        assert_match "OK" [r test.sendbytes 5000000]
        assert_match "OK" [r test.sendbytes 2000000]
    }

    test "Module eventloop iteration" {
        set iteration [r test.iteration]
        set next_iteration [r test.iteration]
        assert {$next_iteration > $iteration}
    }

    test "Module eventloop sanity" {
        r test.sanity
    }

    test "Module eventloop oneshot" {
        r test.oneshot
    }

    test "Unload the module - eventloop" {
        assert_equal {OK} [r module unload eventloop]
    }
}

start_server [list tags {"modules external:skip tls:skip"} overrides [list io-threads 2 loadmodule $testmodule]] {
    r client setname control ;# excluded from the fast path below
    r debug fastpath-exclude 1

    test "Module eventloop iteration for a client read by IO threads" {
        set rr [valkey [srv 0 host] [srv 0 port] 0 $::tls]
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_clients] == 1
        } else {
            fail "a fresh client did not join the fast path"
        }
        # Each call asserts that no command runs between the loop's before-sleep and after-sleep events.
        for {set i 0} {$i < 200} {incr i} {
            $rr test.iteration
        }
        $rr close
    }
}

# A GET answered on an IO thread is sent in the read phase, before the iteration's own AOF write. A write
# made after the previous flush (here an INCR in every after-sleep event, as a module thread holding the GIL
# or an after-sleep hook may make) reaches the AOF before the read phase opens, so no reply shows a value
# the AOF does not have yet, as on the stock loop. With appendfsync always the file has it when we read it.
start_server [list tags {"modules external:skip tls:skip"} overrides [list io-threads 4 appendonly yes appendfsync always save {} loadmodule $testmodule]] {
    r client setname control ;# named clients stay on the main path
    r select 0 ;# the db of the fast-path client and of the module's writes

    proc el_speculated {} {
        return [getInfoProperty [r info fastget] fastget_speculated_commands]
    }

    proc el_aof_incrs {path} {
        set fd [open $path r]
        fconfigure $fd -translation binary
        set data [read $fd]
        close $fd
        return [regexp -all {\*2\r\n\$4\r\nINCR\r\n} $data]
    }

    test "Reads on the reading thread never show an after-sleep write the AOF does not have" {
        set rd [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        r test.aftersleepincr el:ctr
        wait_for_condition 100 10 {[r get el:ctr] ne {}} else { fail "the after-sleep writer did not run" }
        set aof [get_last_incr_aof_path r]
        set before [el_speculated]
        set last 0
        for {set round 0} {$round < 100} {incr round} {
            for {set i 0} {$i < 10} {incr i} { $rd get el:ctr }
            for {set i 0} {$i < 10} {incr i} {
                set v [$rd read]
                assert {$v >= $last}
                set last $v
            }
            set n [el_aof_incrs $aof]
            assert {$n >= $last}
        }
        r test.aftersleepincr off
        # The replies came from the reading thread, not from main.
        assert_morethan [el_speculated] $before
        $rd close
    }
}
