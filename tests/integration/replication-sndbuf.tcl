# repl-sndbuf-bytes sets SO_SNDBUF on each replica link when it is established.
# The kernel may clamp the request (net.core.wmem_max on Linux), so these tests
# ask for 64 KB, which is below every default ceiling, and check the primary's
# log rather than the raw socket.
start_server {tags {"repl network external:skip cluster:skip"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test {repl-sndbuf-bytes defaults to 0 and does not touch the replica link} {
            assert_equal 0 [lindex [$primary config get repl-sndbuf-bytes] 1]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            assert_equal 0 [count_log_message -1 "SO_SNDBUF on replica"]
        }

        test {repl-sndbuf-bytes applies to the next replica link after CONFIG SET} {
            set loglines [count_log_lines -1]
            $primary config set repl-sndbuf-bytes 64kb
            assert_equal 65536 [lindex [$primary config get repl-sndbuf-bytes] 1]

            # Already-attached links keep their buffer; only a new link gets the setting.
            $primary set before-reattach 1
            wait_for_ofs_sync $primary $replica
            assert_equal 0 [count_log_message -1 "SO_SNDBUF on replica"]

            $replica replicaof no one
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to 65536*"} $loglines 100 50
        }

        test {Replication works with a per-socket send buffer} {
            set value [string repeat x 4096]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 2000} {incr i} {
                $rd set sndbuf:$i $value
            }
            for {set i 0} {$i < 2000} {incr i} {
                assert_equal OK [$rd read]
            }
            $rd close
            wait_for_ofs_sync $primary $replica
            assert_equal $value [$replica get sndbuf:1999]
            assert_equal [$primary dbsize] [$replica dbsize]
        }

        test {repl-sndbuf-bytes rejects out-of-range values} {
            catch {$primary config set repl-sndbuf-bytes -1} err
            assert_match {*ERR*} $err
            catch {$primary config set repl-sndbuf-bytes 3gb} err
            assert_match {*ERR*} $err
            assert_equal 65536 [lindex [$primary config get repl-sndbuf-bytes] 1]
        }
    }
}
