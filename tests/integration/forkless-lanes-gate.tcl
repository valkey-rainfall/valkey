# Flow-control gate at a high write rate (T-7 at realistic rates, prototype): default replica output
# buffer limits, replica loads from disk (default), SET load from pipelined clients for the whole
# sync. Reports how often the gate turned on, how many times the replica was dropped and whether the
# sync completed while the load ran. After the load stops the replica must sync and equal the primary.

proc fkg_fp {r field} {
    getInfoProperty [$r info fastpath] $field
}

foreach {mode lanes load hold} {A no disabled no B yes disabled no A no disabled yes B yes disabled yes A no on-empty-db no B yes on-empty-db no} {
    start_server [list tags {"repl external:skip tls:skip"} overrides [list save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}]] {
        set primary [srv 0 client]
        set ph [srv 0 host]
        set pp [srv 0 port]
        $primary config set forkless-lanes $lanes
        catch {$primary config set forkless-load-hold $hold} ;# exists on the combined branch only
        exec src/valkey-benchmark -h $ph -p $pp -r 1000000 -n 2000000 -P 64 -c 16 --threads 4 -q SET k:__rand_int__ 0123456789abcdef > /dev/null 2>@1
        start_server [list overrides [list save {} repl-diskless-load $load io-threads 4 io-threads-parallel-writes-min 1 notify-keyspace-events {""}]] {
            set replica [srv 0 client]
            test "Forkless lanes gate at a high SET rate ($mode, replica load $load, load hold $hold)" {
                set pids {}
                for {set i 0} {$i < 6} {incr i} {
                    lappend pids [exec src/valkey-benchmark -h $ph -p $pp -r 1000000 -n 1000000000 -P 32 -c 4 --threads 2 -q SET k:__rand_int__ 0123456789abcdef > /dev/null 2>@1 &]
                }
                after 1000
                set c0 [status $primary total_commands_processed]
                after 2000
                set rate [expr {([status $primary total_commands_processed] - $c0) / 2}]
                $primary config set rdb-key-save-delay 1
                $replica replicaof $ph $pp
                set t0 [clock milliseconds]
                set up_ms -1
                for {set s 0} {$s < 180} {incr s} {
                    after 1000
                    if {$s % 5 == 4} {
                        set omem 0
                        regexp {omem=([0-9]+)} [$primary client list type replica] -> omem
                        puts "  t=[expr {$s + 1}]s bgsave=[status $primary rdb_bgsave_in_progress] replica_omem=$omem gate_on=[fkg_fp $primary fastpath_forkless_gate_on] drops=[status $primary client_output_buffer_limit_disconnections] sync_full=[status $primary sync_full] replica_link=[status $replica master_link_status] holds=[fkg_fp $primary forkless_load_holds] ops=[status $primary instantaneous_ops_per_sec]"
                    }
                    if {[status $replica master_link_status] eq {up}} {
                        set up_ms [expr {[clock milliseconds] - $t0}]
                        break
                    }
                }
                set drops [status $primary client_output_buffer_limit_disconnections]
                set fulls [status $primary sync_full]
                set gate [fkg_fp $primary fastpath_forkless_gate_on]
                set lw [fkg_fp $primary fastpath_forkless_lane_writes]
                set holds [fkg_fp $primary forkless_load_holds]
                set hold_ms [fkg_fp $primary forkless_load_hold_ms]
                foreach pid $pids { catch {exec /bin/kill -9 $pid} }
                $primary config set rdb-key-save-delay 0
                puts "mode=$mode load=$load hold=$hold presync_set_per_s=$rate link_up_under_load_ms=$up_ms drops=$drops sync_full=$fulls gate_on=$gate lane_writes=$lw load_holds=$holds load_hold_ms=$hold_ms"
                wait_for_condition 1200 100 {
                    [status $replica master_link_status] eq {up} &&
                    [status $primary master_repl_offset] eq [status $replica master_repl_offset]
                } else {
                    fail "replica did not catch up after the load stopped"
                }
                assert_equal [$primary debug digest] [$replica debug digest]
            }
        }
    }
}
