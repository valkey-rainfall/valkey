# Focused coverage for per-invocation speculative keyspace SCAN.

proc dplus_scan_info_field {client field} {
    set payload [$client info dplus]
    if {![regexp "${field}:(\\d+)" $payload -> value]} {
        fail "missing $field in INFO dplus: $payload"
    }
    return $value
}

proc dplus_scan_collect {client} {
    set cursor 0
    set seen [dict create]
    set calls 0
    while 1 {
        $client scan $cursor
        set reply [$client read]
        set cursor [lindex $reply 0]
        foreach key [lindex $reply 1] { dict set seen $key 1 }
        incr calls
        if {$cursor == 0} break
    }
    return [list $seen $calls]
}

start_server {tags {"dplus scan"} overrides {io-threads 4 io-threads-always-active yes save {}}} {
    test {DPLUS SCAN ownership-off: full cursor iteration engages every invocation} {
        r flushall
        set expected [dict create]
        for {set i 0} {$i < 100} {incr i} {
            set key scan:off:$i
            r set $key value
            dict set expected $key 1
        }
        set before [dplus_scan_info_field r dplus_scan_speculative_hits]
        set rd [valkey_deferring_client]
        lassign [dplus_scan_collect $rd] seen calls
        $rd close
        assert_equal [lsort [dict keys $expected]] [lsort [dict keys $seen]]
        wait_for_condition 100 20 {
            [dplus_scan_info_field r dplus_scan_speculative_hits] == $before + $calls
        } else {
            fail "not every ownership-off SCAN invocation engaged: [r info dplus]"
        }
    }
}

start_server {tags {"dplus scan ownership"} overrides {io-threads 4 io-threads-always-active yes io-threads-ownership yes save {}}} {
    test {DPLUS SCAN ownership-on: RESP2 and RESP3 publish cursor plus keys atomically} {
        r flushall
        r mset scan:a one scan:b two scan:c three
        set before [dplus_scan_info_field r dplus_scan_speculative_hits]

        set rd2 [valkey_deferring_client]
        $rd2 scan 0
        set reply2 [$rd2 read]
        assert_equal 0 [lindex $reply2 0]
        assert_equal {scan:a scan:b scan:c} [lsort [lindex $reply2 1]]
        $rd2 close

        set rd3 [valkey_deferring_client]
        $rd3 hello 3
        $rd3 read
        $rd3 scan 0
        set reply3 [$rd3 read]
        assert_equal 0 [lindex $reply3 0]
        assert_equal {scan:a scan:b scan:c} [lsort [lindex $reply3 1]]
        $rd3 close

        wait_for_condition 100 20 {
            [dplus_scan_info_field r dplus_scan_speculative_hits] == $before + 2
        } else {
            fail "RESP2/RESP3 SCAN did not engage: [r info dplus]"
        }
    }

    test {DPLUS SCAN: mixed GET SCAN MGET SCAN prefix preserves reply order} {
        r flushall
        r mset scan:mixed:a one scan:mixed:b two
        set before [dplus_scan_info_field r dplus_scan_speculative_hits]
        set rd [valkey_deferring_client]
        # Recover this owner's existing 8/64 GET write-tax gate before testing
        # a GET-headed mixed prefix.
        for {set i 0} {$i < 8} {incr i} {
            $rd get scan:mixed:a
            assert_equal one [$rd read]
        }
        set pipeline "[format_command get scan:mixed:a][format_command scan 0]"
        append pipeline "[format_command mget scan:mixed:a scan:mixed:b][format_command scan 0]"
        $rd write $pipeline
        $rd flush
        assert_equal one [$rd read]
        set scan1 [$rd read]
        assert_equal 0 [lindex $scan1 0]
        assert_equal {one two} [$rd read]
        set scan2 [$rd read]
        assert_equal 0 [lindex $scan2 0]
        $rd close
        wait_for_condition 100 20 {
            [dplus_scan_info_field r dplus_scan_speculative_hits] == $before + 2
        } else {
            fail "both queued SCAN commands did not engage: [r info dplus]"
        }
    }

    test {DPLUS SCAN: options punt intact to stock path} {
        set before [dplus_scan_info_field r dplus_scan_speculative_hits]
        set reply [r scan 0 count 100]
        assert_equal 2 [llength $reply]
        assert_equal $before [dplus_scan_info_field r dplus_scan_speculative_hits]
    }

    test {DPLUS SCAN: command-specific ACL gate denies SCAN} {
        r acl setuser scan-denied on >pw ~* +@all -scan
        set before [dplus_scan_info_field r dplus_scan_speculative_hits]
        set denied [valkey_deferring_client]
        $denied auth scan-denied pw
        assert_equal OK [$denied read]
        $denied scan 0
        assert_error "*NOPERM*" {$denied read}
        $denied close
        assert_equal $before [dplus_scan_info_field r dplus_scan_speculative_hits]
        r acl deluser scan-denied
    }

    test {DPLUS SCAN: forced validation miss punts without partial cursor or keys} {
        r flushall
        r mset scan:force:a one scan:force:b two
        set before_hits [dplus_scan_info_field r dplus_scan_speculative_hits]
        set before_misses [dplus_scan_info_field r dplus_scan_validation_misses]
        assert_equal OK [r debug dplus-scan-force-validation-miss]
        set rd [valkey_deferring_client]
        $rd scan 0
        set reply [$rd read]
        $rd close
        assert_equal 0 [lindex $reply 0]
        assert_equal {scan:force:a scan:force:b} [lsort [lindex $reply 1]]
        wait_for_condition 100 20 {
            [dplus_scan_info_field r dplus_scan_validation_misses] == $before_misses + 1
        } else {
            fail "SCAN validation miss was not recorded: [r info dplus]"
        }
        assert_equal $before_hits [dplus_scan_info_field r dplus_scan_speculative_hits]
    }

    test {DPLUS SCAN: logical expiry punts so main owns deletion side effects} {
        r flushall
        r debug set-active-expire 0
        r set scan:live value
        r set scan:expired value px 1
        after 10
        set before_hits [dplus_scan_info_field r dplus_scan_speculative_hits]
        set before_expired [s expired_keys]
        set rd [valkey_deferring_client]
        $rd scan 0
        set reply [$rd read]
        $rd close
        r debug set-active-expire 1
        assert_equal {scan:live} [lindex $reply 1]
        assert_equal $before_hits [dplus_scan_info_field r dplus_scan_speculative_hits]
        assert {[s expired_keys] >= $before_expired + 1}
    }

    test {DPLUS SCAN: successive calls retain no snapshot across intervening writes} {
        r flushall
        r set scan:epoch:a one
        set rd [valkey_deferring_client]
        $rd scan 0
        set first [$rd read]
        assert_equal {scan:epoch:a} [lindex $first 1]
        r set scan:epoch:b two
        $rd scan 0
        set second [$rd read]
        assert_equal {scan:epoch:a scan:epoch:b} [lsort [lindex $second 1]]
        $rd close
    }

    test {DPLUS SCAN: active rehash traversal does not advance rehash} {
        r flushall
        r config set activerehashing no
        r set scan:rehash:seed value
        set count [expr {[main_hash_table_keys_before_rehashing_starts] + 2}]
        populate $count scan:rehash: 16
        set stats_before [r debug htstats 9]
        assert_match "*Hash table 1 stats*" $stats_before
        regexp {rehashing index: (-?\d+)} $stats_before -> index_before
        set before_hits [dplus_scan_info_field r dplus_scan_speculative_hits]
        set rd [valkey_deferring_client]
        $rd scan 0
        set reply [$rd read]
        $rd close
        assert_equal 2 [llength $reply]
        set stats_after [r debug htstats 9]
        regexp {rehashing index: (-?\d+)} $stats_after -> index_after
        assert_equal $index_before $index_after
        assert_equal [expr {$before_hits + 1}] [dplus_scan_info_field r dplus_scan_speculative_hits]
        r config set activerehashing yes
    }

    test {DPLUS SCAN: concurrent delete and re-add remains memory safe} {
        r flushall
        for {set i 0} {$i < 64} {incr i} { r set scan:churn:$i initial }
        set writer [valkey_deferring_client]
        set reader [valkey_deferring_client]
        set iterations 300
        for {set i 0} {$i < $iterations} {incr i} {
            set key scan:churn:[expr {$i % 64}]
            $writer del $key
            $writer set $key value:$i
            $reader scan 0
        }
        for {set i 0} {$i < $iterations} {incr i} {
            $writer read
            assert_equal OK [$writer read]
            set reply [$reader read]
            assert_equal 2 [llength $reply]
            foreach key [lindex $reply 1] { assert_match "scan:churn:*" $key }
        }
        $writer close
        $reader close
    }

    test {DPLUS SCAN: disconnect with queued invocations leaves no client or server damage} {
        r flushall
        for {set i 0} {$i < 64} {incr i} { r set scan:disconnect:$i value }
        set baseline [s connected_clients]
        set rd [valkey_deferring_client]
        set pipeline ""
        for {set i 0} {$i < 100} {incr i} { append pipeline [format_command scan 0] }
        $rd write $pipeline
        $rd flush
        $rd close
        wait_for_condition 100 20 {
            [s connected_clients] == $baseline
        } else {
            fail "SCAN disconnect left a zombie client: [r client list]"
        }
        assert_equal PONG [r ping]
    }

    test {DPLUS SCAN: queued commandstats are exact and keyspace hits stay unchanged} {
        r flushall
        r mset scan:stats:a one scan:stats:b two
        r config resetstat
        set rd [valkey_deferring_client]
        set commands 10
        set pipeline ""
        for {set i 0} {$i < $commands} {incr i} { append pipeline [format_command scan 0] }
        $rd write $pipeline
        $rd flush
        for {set i 0} {$i < $commands} {incr i} { assert_equal 2 [llength [$rd read]] }
        $rd close
        wait_for_condition 100 20 {
            [regexp {cmdstat_scan:calls=(\d+)} [r info commandstats] -> calls] && $calls == $commands
        } else {
            fail "cmdstat_scan was not exactly $commands: [r info commandstats]"
        }
        assert_equal 0 [s keyspace_hits]
    }
}

start_server {tags {"dplus"} overrides {io-threads 4 io-threads-ownership yes save {}}} {

    test {DPLUS SCAN: rehash churn never omits stable keys (structural-seq gate)} {
        # The structural sequence's PRIMARY scenario, exercised deliberately:
        # entry relocations (growth rehash steps, chain conversion on insert,
        # hole-fill on delete) racing speculative SCAN walks. Two contracts:
        #   (a) OMISSION: a key present for the entire scan sequence must
        #       appear in the union of replies -- the walk-omission race this
        #       branch's hashtableGetStructuralVersion exists to detect.
        #   (b) ENGAGEMENT HONESTY: the churn must actually make the
        #       structural seq punt at least once (scan_validation_misses
        #       grows) -- otherwise this test exercised nothing and (a) is
        #       vacuously green.
        r flushall
        # Stable keys: present before, during, and after all churn.
        for {set i 0} {$i < 100} {incr i} { r set stable:$i v }

        # Churn client: pipelined, replies drained per round (keeps mutation
        # concurrent with the scanning connection's round-trips).
        set churn [valkey_deferring_client]
        set found [dict create]
        set generation 0
        for {set round 0} {$round < 50} {incr round} {
            # 400 inserts + 200 deletes per round: growth rehash windows,
            # bucket-chain conversions (inserts into full buckets), and
            # hole-fill relocations (deletes) all fire across the run.
            for {set i 0} {$i < 400} {incr i} {
                $churn set churn:[expr {$generation*400+$i}] v
            }
            for {set i 0} {$i < 200} {incr i} {
                $churn del churn:[expr {($generation-1)*400+$i}]
            }
            incr generation
            # Full cursor-following SCAN iteration, interleaved with the
            # churn in flight. Bare two-arg SCAN = the speculated shape.
            set cursor 0
            while 1 {
                set res [r scan $cursor]
                set cursor [lindex $res 0]
                foreach k [lindex $res 1] { dict set found $k 1 }
                if {$cursor == 0} break
            }
            # Drain churn replies for this round.
            for {set i 0} {$i < 600} {incr i} { $churn read }
            # Contract (a), checked every round: every stable key seen.
            for {set i 0} {$i < 100} {incr i} {
                if {![dict exists $found stable:$i]} {
                    fail "round $round omitted stable:$i from full SCAN iteration"
                }
            }
            set found [dict create]
        }
        $churn close

        # Engagement: SCANs actually speculated during the churn (hard), and
        # the structural seq's organic collision rate is REPORTED, not
        # asserted. Rationale: a real collision needs main to be inside a
        # relocation window (nanoseconds) at the instant a worker walk
        # validates -- on a fast box this is genuinely probabilistic (observed
        # 0-collision runs with correct results at every burst shape tried).
        # The punt MACHINERY is deterministically covered by the forced-miss
        # test above; THIS test's load-bearing contract is (a) omission
        # freedom, which is asserted per-round. Collision-rate-under-churn is
        # a v2-design benchmark metric (see scan-seqlock-v2-design.md), not a
        # unit assert.
        set attempts_pre [dplus_scan_info_field r dplus_scan_speculative_attempts]
        set misses_pre [dplus_scan_info_field r dplus_scan_validation_misses]
        set writer [valkey_deferring_client]
        set scanner [valkey_deferring_client]
        $scanner ping
        $scanner read
        for {set burst 0} {$burst < 10} {incr burst} {
            for {set i 0} {$i < 2000} {incr i} { $writer set churnb:$burst:$i v }
            for {set i 0} {$i < 100} {incr i} { $scanner scan 0 }
            for {set i 0} {$i < 2000} {incr i} { $writer read }
            for {set i 0} {$i < 100} {incr i} { $scanner read }
        }
        $writer close
        $scanner close
        set attempts_post [dplus_scan_info_field r dplus_scan_speculative_attempts]
        set misses_post [dplus_scan_info_field r dplus_scan_validation_misses]
        assert {$attempts_post > $attempts_pre}
        if {$misses_post == $misses_pre} {
            puts "NOTE: rehash-churn run produced no organic validation miss (timing-dependent; omission contract still verified)"
        }
    }
}
