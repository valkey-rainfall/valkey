# A replica that runs its own forkless save while it applies the primary's stream. The save holds
# keys, so a replicated write to a held key blocks the primary client with BLOCKED_INUSE. With
# parallel apply on, that client must never be a lane executor (blockClient asserts it), so the
# replica must apply serially while its save runs, and still end equal to the primary.

proc parallel_applied {r} {
    return [getInfoProperty [$r info stats] repl_parallel_applied]
}

start_server {tags {"repl external:skip needs:debug"} overrides {io-threads 4 save ""}} {
    start_server {overrides {io-threads 4 keyspace-shard-bits 8 io-threads-parallel-writes-min 1 save ""
                             forkless-infrastructure-enabled yes forkless-policy prefer-forkless}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        # The test default turns on keyspace events, which keep every write serial.
        $replica config set notify-keyspace-events ""

        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        $primary debug populate 2000 key 10
        wait_for_ofs_sync $primary $replica

        test {Parallel apply engages on the replica before its own save} {
            set before [parallel_applied $replica]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 20000} {incr i} { $rd set key:[expr {$i % 2000}] a$i }
            for {set i 0} {$i < 20000} {incr i} { $rd read }
            $rd close
            wait_for_ofs_sync $primary $replica
            assert_morethan [expr {[parallel_applied $replica] - $before}] 0
        }

        test {Replicated writes to keys held by the replica's forkless save apply serially and converge} {
            # About 2000 keys x 1 ms: the save holds keys for about 2 s.
            $replica config set rdb-key-save-delay 1000
            $replica bgsave
            wait_for_condition 100 10 {
                [getInfoProperty [$replica info debug] forkless_current_item_ms] > 0
            } else {
                fail "replica forkless save did not take a key"
            }
            assert_equal forkless [s 0 rdb_current_bgsave_type]
            set before [parallel_applied $replica]

            # Every key, so some writes hit the key the save holds and block the primary client.
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 6000} {incr i} { $rd set key:[expr {$i % 2000}] b$i }
            for {set i 0} {$i < 6000} {incr i} { $rd read }
            $rd close

            # While the save runs, nothing is applied on the lanes.
            assert_equal 1 [s 0 rdb_bgsave_in_progress]
            assert_equal $before [parallel_applied $replica]

            $replica config set rdb-key-save-delay 0
            waitForBgsave $replica
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal PONG [$replica ping]
        }

        test {Parallel apply resumes on the replica after its save} {
            set before [parallel_applied $replica]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 20000} {incr i} { $rd set key:[expr {$i % 2000}] c$i }
            for {set i 0} {$i < 20000} {incr i} { $rd read }
            $rd close
            wait_for_ofs_sync $primary $replica
            assert_morethan [expr {[parallel_applied $replica] - $before}] 0
            assert_equal [$primary debug digest] [$replica debug digest]
        }
    }
}
