# Focused coverage for command-level speculative MGET reads.

proc dplus_mget_info_field {client field} {
    set payload [$client info dplus]
    if {![regexp "${field}:(\\d+)" $payload -> value]} {
        fail "missing $field in INFO dplus: $payload"
    }
    return $value
}

start_server {tags {"dplus mget"} overrides {io-threads 4 io-threads-always-active yes save {}}} {
    test {DPLUS MGET ownership-off: ordered duplicates engage worker path} {
        r mset mget:a one mget:b two mget:c 3
        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        set rd [valkey_deferring_client]
        set pipeline ""
        for {set i 0} {$i < 32} {incr i} {
            append pipeline [format_command mget mget:b mget:a mget:b mget:c]
        }
        $rd write $pipeline
        $rd flush
        for {set i 0} {$i < 32} {incr i} {
            assert_equal {two one two 3} [$rd read]
        }
        $rd close
        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] == $before + 32
        } else {
            fail "every pipelined MGET did not engage with ownership disabled: [r info dplus]"
        }
    }
}

start_server {tags {"dplus mget ownership"} overrides {io-threads 4 io-threads-always-active yes io-threads-ownership yes save {}}} {
    test {DPLUS MGET ownership-on: RESP2 and RESP3 preserve ordering and duplicates} {
        r mset mget:a one mget:b two mget:c 3
        set before [dplus_mget_info_field r dplus_mget_speculative_hits]

        set rd2 [valkey_deferring_client]
        $rd2 mget mget:c mget:a mget:c mget:b
        assert_equal {3 one 3 two} [$rd2 read]
        $rd2 close

        set rd3 [valkey_deferring_client]
        $rd3 hello 3
        $rd3 read
        $rd3 mget mget:a mget:b mget:a
        assert_equal {one two one} [$rd3 read]
        $rd3 close

        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] >= $before + 2
        } else {
            fail "RESP2/RESP3 MGETs did not engage: [r info dplus]"
        }
    }

    test {DPLUS MGET: byte budget replaces the old eight-key cliff} {
        set short_keys {}
        set short_expected {}
        for {set i 0} {$i < 129} {incr i} {
            set key mget:budget:short:$i
            r set $key v$i
            lappend short_keys $key
            lappend short_expected v$i
        }
        set value_1k [string repeat x 1000]
        set wide_keys {}
        for {set i 0} {$i < 17} {incr i} {
            set key mget:budget:wide:$i
            r set $key $value_1k
            lappend wide_keys $key
        }

        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        set rd [valkey_deferring_client]
        $rd mget {*}[lrange $short_keys 0 8]
        assert_equal [lrange $short_expected 0 8] [$rd read]
        $rd mget {*}[lrange $wide_keys 0 15]
        assert_equal 16 [llength [$rd read]]
        $rd mget {*}[lrange $short_keys 0 127]
        assert_equal [lrange $short_expected 0 127] [$rd read]
        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] == $before + 3
        } else {
            fail "eligible byte-budgeted MGET boundaries did not engage: [r info dplus]"
        }

        # Seventeen 1000-byte values exceed the 16KiB staging budget, while
        # 129 tiny keys exceed only the defensive command-width ceiling.
        $rd mget {*}$wide_keys
        assert_equal 17 [llength [$rd read]]
        $rd mget {*}$short_keys
        assert_equal $short_expected [$rd read]
        $rd close
        assert_equal [expr {$before + 3}] [dplus_mget_info_field r dplus_mget_speculative_hits]
    }

    test {DPLUS MGET: mixed GET and MGET commands preserve one speculative prefix} {
        r mset mget:mixed:a one mget:mixed:b two
        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        set rd [valkey_deferring_client]
        # A fresh owner may start below the existing 8/64 GET write-tax gate.
        # Eight observed GET batches recover that same owner's gate before the
        # coalesced GET->MGET transition being tested.
        for {set i 0} {$i < 8} {incr i} {
            $rd get mget:mixed:a
            assert_equal one [$rd read]
        }
        set pipeline "[format_command get mget:mixed:a][format_command mget mget:mixed:a mget:mixed:b]"
        append pipeline "[format_command get mget:mixed:b][format_command mget mget:mixed:b mget:mixed:a]"
        $rd write $pipeline
        $rd flush
        assert_equal one [$rd read]
        assert_equal {one two} [$rd read]
        assert_equal two [$rd read]
        assert_equal {two one} [$rd read]
        $rd close
        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] == $before + 2
        } else {
            fail "mixed GET/MGET prefix did not consume both MGETs: [r info dplus]"
        }
    }

    test {DPLUS MGET: first failed queued command closes the speculative prefix} {
        r mset mget:stop:a one mget:stop:b two
        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        set rd [valkey_deferring_client]
        set pipeline "[format_command mget mget:stop:a mget:stop:b][format_command mget mget:stop:a mget:stop:missing]"
        append pipeline [format_command mget mget:stop:b mget:stop:a]
        $rd write $pipeline
        $rd flush
        assert_equal {one two} [$rd read]
        assert_equal [list one {}] [$rd read]
        assert_equal {two one} [$rd read]
        $rd close
        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] == $before + 1
        } else {
            fail "speculation did not stop exactly at the failed queued MGET: [r info dplus]"
        }
    }

    test {DPLUS MGET: unsupported values punt the whole command to stock semantics} {
        r flushall
        r mset mget:a one mget:b two
        r lpush mget:list item
        r set mget:expiring old px 1
        set large [string repeat x 1025]
        r set mget:large $large
        after 10

        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        assert_equal [list one {} two] [r mget mget:a mget:missing mget:b]
        assert_equal [list one {} two] [r mget mget:a mget:list mget:b]
        assert_equal [list one {} two] [r mget mget:a mget:expiring mget:b]
        assert_equal [list one $large two] [r mget mget:a mget:large mget:b]
        r ping
        assert_equal $before [dplus_mget_info_field r dplus_mget_speculative_hits]
    }

    test {DPLUS MGET: command-specific ACL gate never substitutes GET permission} {
        r mset mget:acl:a one mget:acl:b two
        r acl setuser mget-denied on >pw ~* +@all -mget
        set before [dplus_mget_info_field r dplus_mget_speculative_hits]
        set denied [valkey_deferring_client]
        $denied auth mget-denied pw
        assert_equal OK [$denied read]
        $denied mget mget:acl:a mget:acl:b
        assert_error "*NOPERM*" {$denied read}
        $denied close
        assert_equal $before [dplus_mget_info_field r dplus_mget_speculative_hits]

        r acl setuser mget-only on >pw ~* +@all -get
        set allowed [valkey_deferring_client]
        $allowed auth mget-only pw
        assert_equal OK [$allowed read]
        $allowed mget mget:acl:a mget:acl:b
        assert_equal {one two} [$allowed read]
        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_speculative_hits] > $before
        } else {
            fail "MGET-specific ACL gate did not allow MGET: [r info dplus]"
        }
        $allowed get mget:acl:a
        assert_error "*NOPERM*" {$allowed read}
        $allowed close
        r acl deluser mget-denied mget-only
    }

    test {DPLUS MGET: commandstats and keyspace hits are attributed exactly} {
        r flushall
        r mset mget:stats:a one mget:stats:b two mget:stats:c three
        r config resetstat
        set before_engagement [dplus_mget_info_field r dplus_mget_speculative_hits]
        set rd [valkey_deferring_client]
        set commands 10
        set pipeline ""
        for {set i 0} {$i < $commands} {incr i} {
            append pipeline [format_command mget mget:stats:a mget:stats:b mget:stats:c]
        }
        $rd write $pipeline
        $rd flush
        for {set i 0} {$i < $commands} {incr i} {
            assert_equal {one two three} [$rd read]
        }
        $rd close
        wait_for_condition 100 20 {
            [regexp {cmdstat_mget:calls=(\d+)} [r info commandstats] -> calls] && $calls == $commands
        } else {
            fail "cmdstat_mget was not exactly $commands: [r info commandstats]"
        }
        assert_equal [expr {$commands * 3}] [s keyspace_hits]
        assert_equal [expr {$before_engagement + $commands}] [dplus_mget_info_field r dplus_mget_speculative_hits]
        set commandstats [r info commandstats]
        if {[regexp {cmdstat_get:calls=(\d+)} $commandstats -> get_calls]} {
            assert_equal 0 $get_calls
        }
    }

    test {DPLUS MGET: forced validation miss punts without a partial reply} {
        r mset mget:race:a old-a mget:race:b old-b
        set before_hits [dplus_mget_info_field r dplus_mget_speculative_hits]
        set before_misses [dplus_mget_info_field r dplus_mget_validation_misses]
        assert_equal OK [r debug dplus-mget-force-validation-miss]
        set rd [valkey_deferring_client]
        $rd mget mget:race:a mget:race:b
        assert_equal {old-a old-b} [$rd read]
        $rd close

        wait_for_condition 100 20 {
            [dplus_mget_info_field r dplus_mget_validation_misses] == $before_misses + 1
        } else {
            fail "MGET validation miss was not recorded: [r info dplus]"
        }
        assert_equal $before_hits [dplus_mget_info_field r dplus_mget_speculative_hits]
    }

    test {DPLUS MGET: concurrent MSET never exposes a mixed generation} {
        r mset mget:atomic:a 0 mget:atomic:b 0
        set writer [valkey_deferring_client]
        set reader [valkey_deferring_client]
        set iterations 500
        for {set i 1} {$i <= $iterations} {incr i} {
            $writer mset mget:atomic:a $i mget:atomic:b $i
            $reader mget mget:atomic:a mget:atomic:b
        }
        for {set i 1} {$i <= $iterations} {incr i} {
            assert_equal OK [$writer read]
            set values [$reader read]
            assert_equal [lindex $values 0] [lindex $values 1]
        }
        $writer close
        $reader close
    }
}
