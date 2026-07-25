proc wait_for_io_threads_to_go_idle {} {
    set io_threads_always_active [dict get [r config get io-threads-always-active] io-threads-always-active]
    if {$io_threads_always_active eq {yes}} {
        # Polling INFO while io-threads-always-active is enabled wakes the
        # workers in afterSleep(), so observe the idle transition with that
        # policy disabled and then restore the original test setting.
        assert_equal {OK} [r config set io-threads-always-active no]
    }

    set errcode [catch {
        wait_for_condition 1000 50 {
            [getInfoProperty [r info server] io_threads_active] eq 0
        } else {
            fail "Failed to wait until no io_threads are active"
        }
    } result]

    if {$io_threads_always_active eq {yes}} {
        assert_equal {OK} [r config set io-threads-always-active yes]
    }
    if {$errcode != 0} {
        return -code $errcode $result
    }
}

proc activate_io_threads_and_wait {} {
    set server_pid [s process_id]
    set client_count 16
    set requests_per_client 32
    for {set i 0} {$i < $client_count} {incr i} {
        set rd($i) [valkey_deferring_client]
    }
    r set a 0
    # Create a batch of commands by suspending the server for a while
    # before responding to the first command
    pause_process $server_pid
    # Send a pipeline of INCR commands for all clients except the first.
    for {set i 1} {$i < $client_count} {incr i} {
        for {set j 0} {$j < $requests_per_client} {incr j} {
            $rd($i) incr a
        }
        $rd($i) flush
    }
    # Resume the server
    resume_process $server_pid

    # Wait until all the client commands have executed
    wait_for_condition 1000 50 {
        [r get a] eq [expr {($client_count - 1) * $requests_per_client}]
    } else {
        fail "Failed to apply the incr command for all clients"
    }

    for {set i 0} {$i < $client_count} {incr i} {
        $rd($i) close
    }

    wait_for_io_threads_to_go_idle
}

start_server {config "minimal.conf" tags {"external:skip" "valgrind:skip"} overrides {enable-debug-command {yes} io-threads 5}} {
    # Skip if non io-threads mode - as it is relevant only for io-threads mode
    assert_equal {io-threads 5} [r config get io-threads]
    test {Force the use of IO threads and assert active IO thread usage} {
        # Ensure all configured IO threads activate on any event, bypassing CPU-based ignition thresholds.
        r config set io-threads-always-active yes
        activate_io_threads_and_wait
        set info [r info]
        set io_threads_count [dict get [r config get io-threads] io-threads]
        for {set i 1} {$i <= $io_threads_count} {incr i} {
            set used_active_time [getInfoProperty $info used_active_time_io_thread_$i]
            if {$i < $io_threads_count} {
                assert_morethan $used_active_time 0
            } else {
                assert_equal $used_active_time {}
            }
        }

        # Adjust io-threads to a lower value and assert that active io_threads fields are >= values found initially
        assert_equal {OK} [r config set io-threads 1]
        set info [r info]
        wait_for_io_threads_to_go_idle
        set used_active_time_1 [getInfoProperty $info used_active_time_io_thread_1]
        assert_equal $used_active_time_1 {}

        # Re-adjust io-threads to the previous value.
        assert_equal {OK} [r config set io-threads 5]

        set info [r info]
        for {set i 1} {$i <= $io_threads_count} {incr i} {
            set used_active_time [getInfoProperty $info used_active_time_io_thread_$i]
            if {$i < $io_threads_count} {
                # Assert active thread usage isn't reset to 0.
                assert_morethan $used_active_time 0
            } else {
                assert_equal $used_active_time {}
            }
        }

        # Verify idle time is never attributed to used_active_time_io_thread:
        # the counter must stay flat while the workers are parked, and
        # reactivating them must not absorb the parked interval retroactively.
        set sleep_time_ms 1000
        # Park the workers for the idle window. With io-threads-always-active
        # enabled, the INFO reads below would wake them in afterSleep() (see
        # #3509), so disable it while sampling.
        assert_equal {OK} [r config set io-threads-always-active no]
        wait_for_io_threads_to_go_idle
        array set pre_sleep_active_times {}
        set idle_start_ms [clock milliseconds]
        set info [r info]
        for {set i 1} {$i < $io_threads_count} {incr i} {
            set pre_sleep_active_times($i) [getInfoProperty $info used_active_time_io_thread_$i]
        }
        after $sleep_time_ms

        # Step 1: parked workers must not accumulate active time (#3727).
        set info [r info]
        for {set i 1} {$i <= $io_threads_count} {incr i} {
            set used_active_time [getInfoProperty $info used_active_time_io_thread_$i]
            if {$i < $io_threads_count} {
                assert {($used_active_time - $pre_sleep_active_times($i)) < ($sleep_time_ms/1000.0)}
            } else {
                assert_equal $used_active_time {}
            }
        }

        # Step 2: reactivate the workers and verify wakeup did not count the
        # parked interval. Bound the delta by measured wall-clock time minus
        # the parked window, so slow runs (sanitizer) inflate both sides.
        assert_equal {OK} [r config set io-threads-always-active yes]
        activate_io_threads_and_wait
        set info [r info]
        set elapsed_sec [expr {([clock milliseconds] - $idle_start_ms) / 1000.0}]
        for {set i 1} {$i < $io_threads_count} {incr i} {
            set used_active_time [getInfoProperty $info used_active_time_io_thread_$i]
            assert {($used_active_time - $pre_sleep_active_times($i)) < ($elapsed_sec - $sleep_time_ms/1000.0)}
        }
    }
}

start_server {config "minimal.conf" tags {"external:skip" "valgrind:skip"} overrides {io-threads 5 enable-debug-assert yes}} {
    test {Prebuilt SET entries: pipelined overwrite churn preserves values and encodings} {
        # Plain SETs parsed on IO threads carry a speculatively built entry.
        # Heavy pipelining exercises both the current-command and queued-command
        # prebuild paths; enable-debug-assert verifies key/value match at consume.
        r config set io-threads-always-active yes
        r flushall
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 2000} {incr i} {
            $rd set pk[expr {$i % 20}] "prebuilt-value-$i"
        }
        $rd flush
        for {set i 0} {$i < 2000} {incr i} { $rd read }
        for {set k 0} {$k < 20} {incr k} {
            assert_equal "prebuilt-value-[expr {1980 + $k}]" [r get pk$k]
        }
        assert_encoding embstr pk0
        # The consume path must actually engage: 2000 SETs, 1980 overwrites,
        # each eligible (embstr, no TTL). Allow headroom for early inserts.
        assert_morethan [s prebuilt_entries_used] 1000
        $rd close
        # Integer values must keep INT encoding (prebuild refuses them).
        r set intk 1234567
        assert_encoding int intk
        # Large values must keep RAW encoding.
        r set rawk [string repeat x 200]
        assert_encoding raw rawk
        assert_equal 22 [r dbsize]
    }

    test {Prebuilt SET entries: TTL carry falls back to the normal path} {
        # Overwriting a key that has a TTL cannot consume the prebuilt entry
        # (it is built without an expire field). KEEPTTL must preserve the TTL.
        r set ttlk v1 EX 100
        r set ttlk v2 KEEPTTL
        assert_range [r ttl ttlk] 90 100
        assert_equal "v2" [r get ttlk]
        # Plain SET clears the TTL, whichever path built the entry.
        r set ttlk v3
        assert_equal -1 [r ttl ttlk]
        assert_equal "v3" [r get ttlk]
    }

    test {Prebuilt SET entries: unconsumed entries leak nothing} {
        # A plain SET queued inside MULTI parses (and prebuilds) on the IO
        # thread, but the command is queued rather than executed, so the
        # prebuilt entry must be released by resetClient. A leak of ~64B x
        # 10000 iterations (~640KB) would be visible in used_memory.
        r set mk original
        set mem_before [s used_memory]
        for {set i 0} {$i < 10000} {incr i} {
            r multi
            r set mk "multi-value-$i"
            r exec
        }
        assert_equal "multi-value-9999" [r get mk]
        set mem_after [s used_memory]
        assert {[expr {$mem_after - $mem_before}] < 300000}
        # Wrong-type overwrite consumes the prebuilt entry correctly.
        r del listk
        r rpush listk a
        r set listk plainstring
        assert_equal "plainstring" [r get listk]
        assert_encoding embstr listk
    }
}
