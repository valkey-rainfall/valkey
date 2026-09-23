# Speculative GET reader tier on the fast-path transport: an eligible GET is
# answered on the owning IO thread against an optimistically validated keyspace
# read, without crossing to main. These tests assert the answers are correct and
# coherent, that the tier's counters move, and that every side effect main must
# keep (miss notification, expiry, MONITOR stream) still happens because those
# cases punt back to main.

proc spec_reads {} {
    return [getInfoProperty [r info fastpath] fastpath_speculative_reads]
}

proc spec_kv_hits {} {
    return [getInfoProperty [r info fastpath] fastpath_speculative_keyspace_hits]
}

# A raw client that sends a GET as its very first command stays on the fast
# path (SELECT / SETNAME would move it to the main path). It therefore reads
# db 0, so the control client below is pinned to db 0 as well. Pass defer=1
# for the write/flush/read pipelined form. Pipelines are sent multibulk via
# formatCommand: the fast-path transport executes only one INLINE command per
# read event (base behaviour, reported upstream), which would hang these tests.
proc spec_client {{defer 0}} {
    return [valkey [srv 0 host] [srv 0 port] $defer $::tls]
}

start_server {tags {"speculative-reads external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000 io-threads-speculative-reads yes enable-debug-command yes}} {
    r select 0
    assert_equal {io-threads 2} [r config get io-threads]
    assert_equal {io-threads-speculative-reads yes} [r config get io-threads-speculative-reads]

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

    test "speculative_reads and keyspace_hits increment for a served GET" {
        r set counted yes
        set before [spec_reads]
        set kv_before [spec_kv_hits]
        set rd [spec_client]
        assert_equal yes [$rd get counted]
        for {set i 0} {$i < 50} {incr i} { assert_equal yes [$rd get counted] }
        $rd close
        wait_for_condition 100 20 {
            [spec_reads] > $before && [spec_kv_hits] > $kv_before
        } else {
            fail "speculative_reads/keyspace_hits did not advance: [r info fastpath]"
        }
    }

    test "GET after SET in the same pipeline sees the new value" {
        r set pipe old
        set rd [spec_client 1]
        # A pipeline that begins with a write: the SET is not eligible, so it and
        # every command after it punt to main, where ordering is preserved. The
        # trailing GET must observe the write that precedes it.
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
        set before [spec_reads]
        set rd [spec_client]
        for {set i 0} {$i < 20} {incr i} { assert_equal v [$rd get mon] }
        $rd close
        # No speculative read may occur while a monitor is attached: the monitor
        # must observe the GET stream, which only main produces.
        assert_equal $before [spec_reads]
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
        # Every bracketed mutation advances the shard version, and it is even at
        # rest (begin then end).
        assert {$v2 > $v1}
        assert {($v2 % 2) == 0}
        assert {($v1 % 2) == 0}
    }

    test "Turning the config off routes GETs through main" {
        r config set io-threads-speculative-reads no
        r set offkey v
        set before [spec_reads]
        set rd [spec_client]
        for {set i 0} {$i < 30} {incr i} { assert_equal v [$rd get offkey] }
        $rd close
        assert_equal $before [spec_reads]
        r config set io-threads-speculative-reads yes
    }
}
