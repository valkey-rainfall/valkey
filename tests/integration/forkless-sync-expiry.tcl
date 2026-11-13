# Key and hash-field expiry while a forkless full sync scans the keyspace. Keys and fields expire
# on both sides of the scan cursor (the hashtable order mixes them), by active expiry on the primary
# and by lazy expiry in reads. Other client writes run at the same time, so the write just before an
# expiry is sometimes one that is not sent inline (a key the scan has not reached yet). After the sync
# the replica must equal the primary: no expired key or field may survive on the replica.

proc forkless_expiry_writer {host port seconds} {
    # Background client writes to plain keys for $seconds: both sides of the cursor.
    set rd [valkey $host $port 1 $::tls]
    $rd select 9
    set end [expr {[clock milliseconds] + $seconds * 1000}]
    set n 0
    while {[clock milliseconds] < $end} {
        for {set i 0} {$i < 50} {incr i} { $rd set a:[expr {int(rand() * 2000)}] w$n }
        for {set i 0} {$i < 50} {incr i} { $rd read }
        incr n
        after 5
    }
    $rd close
}

foreach mode {active lazy} {
start_server {tags {"repl external:skip needs:debug"} overrides {save "" forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    start_server {overrides {save ""}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test "Forkless sync with key and hash-field expiry during the scan ($mode expiry)" {
            $primary flushall
            if {$mode eq "lazy"} { $primary debug set-active-expire 0 }
            $primary debug populate 2000 a 10
            for {set h 0} {$h < 200} {incr h} {
                $primary hset h:$h f0 v f1 v f2 v f3 v f4 v keep1 v keep2 v
            }
            for {set k 0} {$k < 200} {incr k} { $primary set e:$k v }

            # About 2400 keys x 2 ms: the scan takes about 5 s.
            $primary config set rdb-key-save-delay 2000
            $replica replicaof $primary_host $primary_port
            wait_for_condition 100 50 {
                [s -1 rdb_bgsave_in_progress] == 1
            } else {
                fail "forkless sync did not start"
            }
            assert_equal forkless [s -1 rdb_current_bgsave_type]

            # TTLs set after about 1.5 s of scan: about a third of the keys are behind the cursor.
            after 1500
            for {set h 0} {$h < 200} {incr h} {
                $primary hpexpire h:$h 200 FIELDS 5 f0 f1 f2 f3 f4
            }
            for {set k 0} {$k < 200} {incr k} { $primary pexpire e:$k 200 }

            # Writes on both sides of the cursor while the TTLs run out.
            forkless_expiry_writer $primary_host $primary_port 1
            if {$mode eq "lazy"} {
                for {set h 0} {$h < 200} {incr h} { $primary hget h:$h f0 }
                for {set k 0} {$k < 200} {incr k} { $primary get e:$k }
            }

            $primary config set rdb-key-save-delay 0
            wait_for_condition 200 100 {
                [s -1 rdb_bgsave_in_progress] == 0 &&
                [status $replica master_link_status] eq "up"
            } else {
                fail "sync did not finish"
            }
            if {$mode eq "lazy"} {
                # After the sync, active expiry removes the rest through the normal stream.
                $primary debug set-active-expire 1
            }
            wait_for_condition 50 100 {
                [$primary dbsize] == 2200 && [$primary hlen h:0] == 2
            } else {
                fail "primary did not expire the keys and fields"
            }
            wait_for_ofs_sync $primary $replica

            # The replica keeps what the primary did not send; compare raw state, not reads.
            assert_equal [$primary dbsize] [$replica dbsize]
            set bad {}
            for {set h 0} {$h < 200} {incr h} {
                if {[$replica hlen h:$h] != 2} { lappend bad h:$h }
            }
            assert_equal {} $bad
            assert_equal [$primary debug digest] [$replica debug digest]
        }
    }
}
}
