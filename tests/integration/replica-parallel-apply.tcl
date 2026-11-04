# Replica parallel apply: with keyspace-shard-bits set on the replica, eligible replicated SETs
# are applied on the staged threads by shard. The replica must stay identical to the primary, and
# any feature that observes individual writes must see them.

proc parallel_applied {r} {
    return [getInfoProperty [$r info stats] repl_parallel_applied]
}

proc assert_in_sync {primary replica} {
    wait_for_ofs_sync $primary $replica
    assert_equal [$primary debug digest] [$replica debug digest]
    assert_equal [$primary dbsize] [$replica dbsize]
}

start_server {tags {"repl external:skip"} overrides {io-threads 4 io-threads-fast-path yes}} {
    start_server {overrides {io-threads 4 io-threads-fast-path yes keyspace-shard-bits 8}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        # The test default turns on keyspace events, which keep every write serial.
        $replica config set notify-keyspace-events ""

        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        test {Parallel apply: pipelined SETs converge and run in APPLY phases} {
            set before [parallel_applied $replica]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 20000} {incr i} {
                $rd set key:[expr {$i % 3000}] v$i
            }
            for {set i 0} {$i < 20000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_morethan [expr {[parallel_applied $replica] - $before}] 0
        }

        test {Parallel apply: TTLs, serial commands and other dbs keep stream order} {
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 3000} {incr i} {
                $rd set k:$i a$i px 100000
                $rd incr ctr
                $rd set k:$i b$i
                $rd select [expr {$i % 3}]
                $rd set k:$i c$i ex 1000
                $rd del k:[expr {$i - 1}]
                $rd select 9
            }
            for {set i 0} {$i < 21000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            $replica select 1
            assert_equal [$replica get k:1] c1
            assert_range [$replica ttl k:1] 900 1000
            $replica select 9
            assert_equal [$replica get k:2999] b2999
            assert_equal [$replica ttl k:2999] -1
        }

        test {Parallel apply: MULTI/EXEC from the primary stays atomic} {
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 500} {incr i} {
                $rd set m:$i x
                $rd multi
                $rd set m:$i y
                $rd set n:$i y
                $rd exec
                $rd set n:$i z
            }
            for {set i 0} {$i < 3000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [$replica get m:499] y
            assert_equal [$replica get n:499] z
        }

        test {Parallel apply: WATCH on the replica sees a replicated SET} {
            set rr [valkey_client]
            $rr readonly
            $rr watch w:1
            $primary set w:1 changed
            wait_for_ofs_sync $primary $replica
            $rr multi
            $rr get w:1
            assert_equal [$rr exec] {}
            $rr close
        }

        test {Parallel apply: MONITOR on the replica sees every replicated SET} {
            set before [parallel_applied $replica]
            set mon [valkey_deferring_client]
            $mon monitor
            assert_equal [$mon read] OK
            $primary set mon:1 a
            $primary set mon:2 b
            wait_for_ofs_sync $primary $replica
            assert_match {*"set"*"mon:1"*} [$mon read]
            assert_match {*"set"*"mon:2"*} [$mon read]
            $mon close
            assert_equal [parallel_applied $replica] $before
        }

        test {Parallel apply: keyspace notifications on the replica fire for replicated SETs} {
            $replica config set notify-keyspace-events KEA
            set sub [valkey_deferring_client]
            $sub psubscribe __keyspace@9__:note
            $sub read
            $primary set note v
            assert_equal [$sub read] {pmessage __keyspace@9__:note __keyspace@9__:note set}
            $sub close
            $replica config set notify-keyspace-events ""
            assert_in_sync $primary $replica
            set before [parallel_applied $replica]
            # Pipelined, so the stream carries them together.
            set pc [valkey_deferring_client -1]
            for {set i 0} {$i < 200} {incr i} { $pc set note:$i w }
            for {set i 0} {$i < 200} {incr i} { assert_equal OK [$pc read] }
            $pc close
            assert_in_sync $primary $replica
            # A read too small for a lane phase runs on main, so only check that parallel apply resumed.
            assert_morethan [parallel_applied $replica] $before
        }

        test {Parallel apply: off by config} {
            $replica config set replica-parallel-apply no
            set before [parallel_applied $replica]
            for {set i 0} {$i < 100} {incr i} { $primary set off:$i v }
            assert_in_sync $primary $replica
            assert_equal [parallel_applied $replica] $before
            $replica config set replica-parallel-apply yes
        }

        test {Parallel apply: held SETs reach the latency histogram and the commandlog} {
            # One client without pipelining: the primary's reads are too small for a planned phase,
            # so every SET is held.
            $replica config resetstat
            $replica config set slowlog-log-slower-than 0
            $replica config set slowlog-max-len 10000
            $replica slowlog reset
            set before [parallel_applied $replica]
            for {set i 0} {$i < 200} {incr i} { $primary set held:$i v }
            assert_in_sync $primary $replica
            assert_morethan [expr {[parallel_applied $replica] - $before}] 0
            set calls [getInfoProperty [$replica info commandstats] cmdstat_set]
            assert_match {calls=200,*} $calls
            assert_match {*p50=*} [getInfoProperty [$replica info latencystats] latency_percentiles_usec_set]
            set logged 0
            foreach e [$replica slowlog get -1] {
                if {[string match -nocase {set held:*} [lindex $e 3]]} { incr logged }
            }
            assert_equal 200 $logged
            $replica config set slowlog-log-slower-than 10000
            $replica slowlog reset
        }

        test {Parallel apply: held SETs stop under every state that stops planned lanes} {
            # maxmemory-clients keeps planned lanes off; held SETs must stay off too.
            $replica config set maxmemory-clients 100mb
            set before [parallel_applied $replica]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 2000} {incr i} { $rd set mmc:$i v }
            for {set i 0} {$i < 2000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [parallel_applied $replica] $before
            $replica config set maxmemory-clients 0
        }

        test {Parallel apply: sharded replica survives a full resync and SCAN sees every key} {
            $replica replicaof no one
            $primary set post-resync 1
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            assert_in_sync $primary $replica
            $replica select 9
            set cursor 0
            set keys {}
            while 1 {
                set res [$replica scan $cursor count 500]
                set cursor [lindex $res 0]
                lappend keys {*}[lindex $res 1]
                if {$cursor == 0} break
            }
            assert_equal [llength [lsort -unique $keys]] [$replica dbsize]
        }
    }
}

# With every local client in a priority subnet, replies to many subscribers queue ahead of the
# primary's planned read, which then completes after main-path commands of the same iteration ran.
# A WATCH among them was not seen when the read was classified.
start_server {tags {"repl external:skip"} overrides {io-threads 4 io-threads-fast-path yes}} {
    start_server {overrides {io-threads 4 io-threads-fast-path yes keyspace-shard-bits 8 priority-subnets 127.0.0.0/8 enable-debug-assert yes}} {
        set primary [srv -1 client]
        set replica [srv 0 client]
        $replica config set notify-keyspace-events ""
        $replica replicaof [srv -1 host] [srv -1 port]
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        test {Parallel apply: WATCHes racing planned lane reads leave the watch state intact} {
            set lane_reads [getInfoProperty [$replica info stats] repl_lane_reads]
            set subs {}
            for {set j 0} {$j < 200} {incr j} {
                set s [valkey_deferring_client]
                $s subscribe ch
                $s read
                lappend subs $s
            }
            set watched {}
            for {set k 0} {$k < 16} {incr k} { lappend watched w:$k }
            set w [valkey_client]
            set rd [valkey_deferring_client -1]
            set pending 0
            for {set round 0} {$round < 2000} {incr round} {
                $rd publish ch x
                for {set k 0} {$k < 40} {incr k} { $rd set w:[expr {$k % 16}] $round }
                $rd flush
                incr pending 41
                $w watch {*}$watched
                $w unwatch
                if {$pending >= 4100} {
                    for {set i 0} {$i < $pending} {incr i} { $rd read }
                    set pending 0
                }
            }
            for {set i 0} {$i < $pending} {incr i} { $rd read }
            $rd close
            $w close
            foreach s $subs { $s close }
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_morethan [getInfoProperty [$replica info stats] repl_lane_reads] $lane_reads
            assert_equal 0 [getInfoProperty [$replica info clients] watching_clients]
            set rr [valkey_client]
            $rr watch w:0
            $primary set w:0 changed
            wait_for_ofs_sync $primary $replica
            $rr multi
            $rr get w:0
            assert_equal [$rr exec] {}
            $rr close
            assert_equal 0 [getInfoProperty [$replica info clients] watching_clients]
        }
    }
}
