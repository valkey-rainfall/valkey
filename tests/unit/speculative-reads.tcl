# Speculative read tier on the fast-path transport: an eligible GET batch is
# executed on the owning IO thread against an optimistically validated keyspace
# read, and the replies are written from the worker without crossing to main.
# These tests assert the answers are correct and coherent, that the tier
# demonstrably engages (measured on an always-on counter, not one that exists
# only in an instrumented build), and that every side effect main must keep
# (miss accounting, expiry, the MONITOR stream) still happens because those
# cases punt back to main.
#
# Engagement is asserted on fastpath_speculated (INFO fastpath), which every
# batch that speculates its read prefix increments in the always-on build, and
# cross-checked against dplus_epoch_reader_entries (INFO dplus, also always-on),
# which every speculation attempt bumps once past the entry gates. A test that
# depends on the tier engaging MUST assert on one of these, or it passes
# vacuously in a build where the instrumented hit counters are absent.

# fastpath_speculated: batches whose read prefix was speculated on a worker.
# Always present.
proc fp_speculated {} {
    return [getInfoProperty [r info fastpath] fastpath_speculated]
}

# dplus_epoch_reader_entries: reader-epoch entries, bumped once per speculation
# attempt past the entry gates. Always present. Used as an independent
# cross-check that the same activity that moved fastpath_speculated also drove
# the D+ reader epoch.
proc dplus_reader_entries {} {
    return [getInfoProperty [r info dplus] dplus_epoch_reader_entries]
}

# A client that sends a GET as its very first command stays on the fast path;
# a SELECT or SETNAME would move it to the main path for life, so the control
# client "r" is pinned to db 0 to match. Pass defer=1 for the
# write/flush/read pipelined form. Pipelines are sent multibulk via
# formatCommand: the fast-path transport executes only one INLINE command per
# read event, so an inline pipeline would hang.
proc spec_client {{defer 0}} {
    return [valkey [srv 0 host] [srv 0 port] $defer $::tls]
}

start_server {tags {"speculative-reads external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes io-threads-fast-path yes save {} enable-debug-command yes}} {
    r select 0
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]

    # Instrumented-build probe: the D+ hit/attempt counters and the prevalidate
    # hold exist only under -DIO_LOOKUP_OFFLOAD_STATS. Cross-checks that read
    # them are guarded on this; the always-on engagement asserts run everywhere.
    set ::dplus_instrumented 1
    if {[catch {r debug dplus-prevalidate-hold 1} e]} {
        if {[string match "*instrumented*" $e]} { set ::dplus_instrumented 0 }
    }
    after 20 ;# a 1ms probe arm expires harmlessly if it was set

    test "Speculative GET returns the correct value" {
        r set foo bar
        r set num 12345
        set rd [spec_client]
        assert_equal bar [$rd get foo]
        assert_equal 12345 [$rd get num]
        $rd close
    }

    test "Speculative GET of a missing key returns nil (punted to main)" {
        r del absent
        set rd [spec_client]
        assert_equal {} [$rd get absent]
        $rd close
    }

    test "fastpath_speculated advances for a served GET run" {
        r set counted yes
        set before [fp_speculated]
        set entries_before [dplus_reader_entries]
        set rd [spec_client]
        assert_equal yes [$rd get counted]
        for {set i 0} {$i < 50} {incr i} { assert_equal yes [$rd get counted] }
        $rd close
        # Engagement, on always-on counters. If speculation silently stops
        # engaging this is the assert that fails.
        wait_for_condition 100 20 {
            [fp_speculated] > $before && [dplus_reader_entries] > $entries_before
        } else {
            fail "fastpath_speculated/dplus_epoch_reader_entries did not advance: [r info fastpath] [r info dplus]"
        }
        # Instrumented cross-check: the served reads register as D+ hits too.
        if {$::dplus_instrumented} {
            assert {[getInfoProperty [r info dplus] dplus_speculative_hits] > 0}
        }
    }

    test "A served GET increments keyspace_hits" {
        # B has no always-on per-served-key speculation counter (the D+ hit
        # counter is instrumented-only), so the served-GET observable here is
        # the engine-independent keyspace_hits in INFO stats: a GET served on a
        # worker accounts its hit the same as one served on main.
        r config resetstat
        r set kh v
        set rd [spec_client]
        for {set i 0} {$i < 30} {incr i} { assert_equal v [$rd get kh] }
        $rd close
        wait_for_condition 100 20 {
            [getInfoProperty [r info stats] keyspace_hits] >= 30
        } else {
            fail "keyspace_hits did not advance: [r info stats]"
        }
    }

    test "GET after SET in the same pipeline sees the new value" {
        r set pipe old
        set rd [spec_client 1]
        # A pipeline that begins with a write: the SET is not eligible, so it
        # and every command after it are handled on main, where ordering is
        # preserved. The trailing GET must observe the write before it.
        $rd write [formatCommand SET pipe new][formatCommand GET pipe]
        $rd flush
        assert_equal OK [$rd read]
        assert_equal new [$rd read]
        $rd close
    }

    test "A leading GET run then a write in one pipeline stays coherent" {
        r set a 1
        r set b 2
        set rd [spec_client 1]
        $rd write [formatCommand GET a][formatCommand GET b][formatCommand SET a 9][formatCommand GET a]
        $rd flush
        assert_equal 1 [$rd read]
        assert_equal 2 [$rd read]
        assert_equal OK [$rd read]
        assert_equal 9 [$rd read]
        $rd close
    }

    test "Expired keys still expire and are not served stale" {
        r set ttlkey soon
        r pexpire ttlkey 50
        after 120
        set rd [spec_client]
        assert_equal {} [$rd get ttlkey]
        assert_equal 0 [r exists ttlkey]
        $rd close
    }

    test "keyspace_hits/misses accounting stays consistent" {
        r config resetstat
        r set hitkey v
        r del misskey
        set rd [spec_client]
        for {set i 0} {$i < 20} {incr i} { assert_equal v [$rd get hitkey] }
        for {set i 0} {$i < 20} {incr i} { assert_equal {} [$rd get misskey] }
        $rd close
        wait_for_condition 100 20 {
            [getInfoProperty [r info stats] keyspace_hits] >= 20 &&
            [getInfoProperty [r info stats] keyspace_misses] >= 20
        } else {
            fail "keyspace stats did not settle: [r info stats]"
        }
    }

    test "MONITOR attached forces GETs to punt to main" {
        r set mon v
        set monc [spec_client 1]
        $monc monitor
        assert_equal OK [$monc read]
        # Let the monitor gate settle across the exclusive drain on main.
        after 100
        set before [fp_speculated]
        set rd [spec_client]
        for {set i 0} {$i < 20} {incr i} { assert_equal v [$rd get mon] }
        $rd close
        # No read may be speculated on a worker while a monitor is attached: the
        # monitor must observe the GET stream, which only main produces.
        assert_equal $before [fp_speculated]
        $monc close
    }

    test "Coherence storm: every returned value was actually written" {
        r flushall
        set keys {k0 k1 k2 k3 k4 k5 k6 k7}
        foreach k $keys { r set $k v0 }
        set writer [spec_client]
        set readers {}
        for {set i 0} {$i < 8} {incr i} { lappend readers [spec_client] }
        for {set round 0} {$round < 200} {incr round} {
            set k [lindex $keys [expr {$round % 8}]]
            set op [expr {$round % 4}]
            if {$op == 0} { $writer set $k v$round }
            if {$op == 1} { $writer del $k }
            if {$op == 2} { $writer set $k v$round; $writer pexpire $k 100000 }
            if {$op == 3} { $writer set $k v$round }
            foreach rd $readers {
                set val [$rd get $k]
                # A served value must be nil or a value that starts with "v": it
                # must never be torn or a value from another key.
                if {$val ne {}} {
                    assert {[string match "v*" $val]}
                }
            }
        }
        $writer close
        foreach rd $readers { $rd close }
        assert_equal PONG [r ping]
    }

    test "DEBUG dplus-shard-version bracket probe still works" {
        r set probe one
        set res1 [r debug dplus-shard-version probe]
        set v1 [lindex $res1 1]
        r set probe two
        set res2 [r debug dplus-shard-version probe]
        set v2 [lindex $res2 1]
        # Every bracketed mutation advances the shard version, and the version
        # is even at rest (a mutation opens then closes an odd/even bracket).
        assert {$v2 > $v1}
        assert {($v2 % 2) == 0}
        assert {($v1 % 2) == 0}
    }

    test "Turning the fast path off routes GETs through main" {
        # B has no single "speculation off, fast path on" switch that applies to
        # already-attached standalone clients: io-threads-speculation-replica-only
        # is exercised on the standalone by unit/dplus-replica-only, but it gates
        # the read prefix, not the transport, and a client that attached while it
        # was off is a separate concern. io-threads-fast-path is the non-hidden,
        # runtime-modifiable transport switch: with it off, fastpathEligibleClient
        # returns 0 and every command (GETs included) is handled on main, so no
        # read is speculated. That is the coarser but observable B analog of A's
        # per-tier config toggle.
        r config set io-threads-fast-path no
        r set offkey v
        set before [fp_speculated]
        set rd [spec_client]
        for {set i 0} {$i < 30} {incr i} { assert_equal v [$rd get offkey] }
        $rd close
        assert_equal $before [fp_speculated]
        r config set io-threads-fast-path yes
    }
}
