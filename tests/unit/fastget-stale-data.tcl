# replica-serve-stale-data no: once the link with the primary is down, a replica must
# answer GET with -MASTERDOWN on every path. Fast GET on IO threads skipped this
# check, so a fast-path client got stale values (audit item REPL-15).

start_server {tags {"repl fastget tls:skip"} overrides {save {}}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set primary_pid [srv 0 pid]
    $primary set stale:k v1

    start_server {overrides {io-threads 4 io-threads-always-active yes save {} replica-serve-stale-data no}} {
        set replica [srv 0 client]
        $replica replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [status $replica master_link_status] eq {up} &&
            [$replica get stale:k] eq {v1}
        } else {
            fail "replication never came up"
        }

        test {Fast-path GET on a replica with a down link and replica-serve-stale-data no returns MASTERDOWN} {
            # A fresh client that sends only GETs is admitted to the fast path.
            set rd [valkey_deferring_client]
            for {set i 0} {$i < 20} {incr i} { $rd get stale:k }
            for {set i 0} {$i < 20} {incr i} { assert_equal v1 [$rd read] }
            regexp {fastpath_speculated:(\d+)} [$replica info fastpath] -> spec
            assert_morethan $spec 0

            # Take the link down without changing the replica's role.
            pause_process $primary_pid
            $replica client kill type master
            wait_for_condition 50 100 {
                [status $replica master_link_status] eq {down}
            } else {
                resume_process $primary_pid
                fail "link did not go down"
            }

            set errors 0
            for {set i 0} {$i < 10} {incr i} { $rd get stale:k }
            for {set i 0} {$i < 10} {incr i} {
                catch {$rd read} reply
                if {[string match {MASTERDOWN*} $reply]} { incr errors }
            }
            $rd close
            resume_process $primary_pid
            assert_equal 10 $errors
        }
    }
}
