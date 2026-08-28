# Door-2 ownership regression tests.
# One test per bug fixed during Phase-1 development and the gate battery.
# This file forces its own io-threads + ownership overrides, so it exercises
# the ownership path regardless of how the suite was invoked.

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-ownership yes save {}}} {

    test {OWNERSHIP: basic sanity - server serves owned clients} {
        r set ownkey ownval
        assert_equal "ownval" [r get ownkey]
        r ping
    } {PONG}

    test {OWNERSHIP: MULTI over split round-trips queues instead of speculating} {
        # Regression: dplus speculated a GET arriving in a LATER read while
        # flag.multi was set - executed early + stranded from the transaction
        # (slice-4 fix, also cherry-picked to the D+ lineage). The standard
        # test client does one round-trip per command: exactly the split-RT
        # shape that was broken. Pipelined MULTI was accidentally safe.
        r set mkey v1
        assert_equal "OK" [r multi]
        assert_equal "QUEUED" [r get mkey]
        assert_equal "QUEUED" [r set mkey v2]
        set res [r exec]
        assert_equal {v1 OK} $res
        # Post-EXEC the client must behave normally again (disowned to main).
        assert_equal "v2" [r get mkey]
    }

    test {OWNERSHIP: disconnecting owned clients fully release (no zombies)} {
        # Regression: owned clients releasing io_read_state only in
        # beforeNextClient left EOF/error paths stuck at COMPLETED_IO ->
        # freeClientAsync deferred forever: phantom connected_clients and
        # module DISCONNECTED hooks never fired (gate-battery hooks.tcl find).
        set baseline [s connected_clients]
        set clients {}
        for {set i 0} {$i < 20} {incr i} {
            set rd [valkey_client]
            $rd get ownkey
            lappend clients $rd
        }
        foreach rd $clients { $rd close }
        wait_for_condition 100 50 {
            [s connected_clients] == $baseline
        } else {
            fail "connected_clients did not return to baseline: [s connected_clients] vs $baseline (zombie owned clients)"
        }
    }

    test {OWNERSHIP: commandstats counts speculated reads exactly} {
        # Regression: speculated GETs bypassed call(), so cmd->calls was never
        # incremented (INFO showed calls=1 where 100 ran). Aggregation lags at
        # most one event-loop cycle - poll for exactness.
        r config resetstat
        set n 200
        set rd [valkey_deferring_client]
        for {set i 0} {$i < $n} {incr i} { $rd get ownkey }
        for {set i 0} {$i < $n} {incr i} { assert_equal "ownval" [$rd read] }
        $rd close
        wait_for_condition 100 50 {
            [regexp {cmdstat_get:calls=(\d+)} [r info commandstats] -> calls] && $calls == $n
        } else {
            fail "cmdstat_get calls never reached $n exactly (got: [r info commandstats])"
        }
    }

    test {OWNERSHIP: client stays correct through SUBSCRIBE disown} {
        # Disown trigger: pubsub state migrates the client back to main.
        # The transition must not lose or corrupt traffic.
        set rd [valkey_deferring_client]
        $rd subscribe ch-own
        assert_equal {subscribe ch-own 1} [$rd read]
        r publish ch-own hello
        assert_equal {message ch-own hello} [$rd read]
        $rd unsubscribe ch-own
        $rd read ; # unsubscribe confirmation
        # RESP2 clients leave subscribe mode when count hits 0 - normal
        # commands must work again on the same (now disowned) connection.
        $rd ping
        assert_equal "PONG" [$rd read]
        $rd close
    }

    test {OWNERSHIP: client stays correct through BLPOP block/unblock disown} {
        # Disown trigger: blocking. Replies must be ordered after the unblock.
        set rd [valkey_deferring_client]
        $rd blpop blist-own 0
        wait_for_blocked_client
        r lpush blist-own theval
        assert_equal {blist-own theval} [$rd read]
        # Same connection keeps working post-unblock.
        $rd get ownkey
        assert_equal "ownval" [$rd read]
        $rd close
    }

    test {OWNERSHIP: CLIENT KILL by id of an owned client} {
        set rd [valkey_deferring_client]
        $rd client id
        set victim_id [$rd read]
        $rd get ownkey
        assert_equal "ownval" [$rd read]
        assert_equal 1 [r client kill id $victim_id]
        # Server alive and consistent afterwards.
        assert_equal "PONG" [r ping]
        assert_equal "ownval" [r get ownkey]
        catch {$rd close}
    }

    test {OWNERSHIP: rapid connect-first-command cycles} {
        # Mini boot-storm: fresh connections must get first replies promptly.
        # (The full parallel-density stress test lands once the reconnect race
        # is root-caused; this is the cheap always-on smoke.)
        for {set i 0} {$i < 30} {incr i} {
            set rd [valkey_client]
            assert_equal "PONG" [$rd ping]
            $rd close
        }
    }

    test {OWNERSHIP: WATCH aborts EXEC when watched key changes (speculated reads active)} {
        # Design-doc risk 5 ('believed clean; verify in gauntlet'): watch
        # lists are writer-side state; speculated GETs must neither trip nor
        # miss dirty-CAS. A watching client's GET may take the speculated
        # path; the subsequent modification by ANOTHER client must still
        # abort EXEC.
        r set wkey v0
        set rd [valkey_deferring_client]
        $rd watch wkey
        assert_equal "OK" [$rd read]
        # Speculated read on the watching client — must not clear/damage
        # the watch.
        $rd get wkey
        assert_equal "v0" [$rd read]
        # Another client modifies the watched key.
        r set wkey v1
        $rd multi
        assert_equal "OK" [$rd read]
        $rd get wkey
        assert_equal "QUEUED" [$rd read]
        $rd exec
        assert_equal {} [$rd read] ; # nil multi-bulk = aborted
        $rd close
    }

    test {OWNERSHIP: WATCH allows EXEC when watched key untouched (reads are not dirtying)} {
        # The inverse property: heavy speculated READS of the watched key by
        # other clients must NOT dirty the CAS — only writes do.
        r set wkey2 stable
        set rd [valkey_deferring_client]
        $rd watch wkey2
        assert_equal "OK" [$rd read]
        # Other clients hammer speculated reads on the watched key.
        for {set i 0} {$i < 3} {incr i} {
            set rr [valkey_deferring_client]
            for {set j 0} {$j < 50} {incr j} { $rr get wkey2 }
            for {set j 0} {$j < 50} {incr j} { assert_equal "stable" [$rr read] }
            $rr close
        }
        $rd multi
        assert_equal "OK" [$rd read]
        $rd get wkey2
        assert_equal "QUEUED" [$rd read]
        $rd exec
        assert_equal {stable} [$rd read] ; # committed
        $rd close
    }

    test {OWNERSHIP: pipeline integrity under mixed read-write} {
        # Slice-3 reply-ordering guarantee: consumed GET replies precede the
        # main-executed remainder, per batch, in order.
        set rd [valkey_deferring_client]
        $rd set pk 1
        $rd get pk
        $rd incr pk
        $rd get pk
        $rd set pk 10
        $rd get pk
        assert_equal "OK" [$rd read]
        assert_equal "1" [$rd read]
        assert_equal "2" [$rd read]
        assert_equal "2" [$rd read]
        assert_equal "OK" [$rd read]
        assert_equal "10" [$rd read]
        $rd close
    }
}

# The same file must also pass with ownership OFF (differential sanity: these
# behaviors are mode-independent).
start_server {tags {"ownership"} overrides {io-threads 4 save {}}} {
    test {OWNERSHIP-OFF control: MULTI split round-trips} {
        r set mkey v1
        r multi
        assert_equal "QUEUED" [r get mkey]
        assert_equal {v1} [r exec]
    }
    test {OWNERSHIP-OFF control: commandstats exactness} {
        r set ownkey ownval
        r config resetstat
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 200} {incr i} { $rd get ownkey }
        for {set i 0} {$i < 200} {incr i} { $rd read }
        $rd close
        wait_for_condition 100 50 {
            [regexp {cmdstat_get:calls=(\d+)} [r info commandstats] -> calls] && $calls == 200
        } else {
            fail "OFF-mode cmdstat_get exactness failed"
        }
    }
}

# --- D+ ACL/AUTH gate regression tests (B-ACL fix) ---
# Bug: dplusSpeculateBatch executed GETs with NO auth/ACL checks — speculation
# bypasses processCommand entirely (both the NOAUTH check and ACLCheckAllPerm).
# Found by acl-v2.tcl failing 3/3 deterministic under ownership.
# Tests use PIPELINED batches at depth (encoded-buffer lesson: engagement
# differs at P>1) and prove path engagement via dplus_speculative_hits.

proc dplus_spec_hits {r} {
    regexp {dplus_speculative_hits:(\d+)} [$r info dplus] -> hits
    return $hits
}

proc dplus_info_field {r field} {
    set payload [$r info dplus]
    if {![regexp "${field}:(\\d+)" $payload -> value]} {
        fail "missing $field in INFO dplus: $payload"
    }
    return $value
}

proc dplus_epoch_debug_stats {r} {
    return [$r debug dplus-epoch-stats]
}

proc dplus_epoch_is_blocked {r pinned_epoch minimum_retired} {
    lassign [dplus_epoch_debug_stats $r] epoch retired reclaimed forced advances scans
    return [expr {$epoch > $pinned_epoch && $retired >= $minimum_retired}]
}

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-ownership yes save {}}} {

    test {OWNERSHIP ACL: engagement sanity - default user GETs do speculate} {
        r set akey aval
        set before [dplus_spec_hits r]
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 50} {incr i} { $rd get akey }
        for {set i 0} {$i < 50} {incr i} { assert_equal "aval" [$rd read] }
        $rd close
        # Owned-client pipelined GETs must ride the speculative path.
        wait_for_condition 100 50 {
            [dplus_spec_hits r] > $before
        } else {
            fail "speculative path never engaged - test preconditions broken"
        }
    }

    test {OWNERSHIP EPOCH: reader state engages and returns quiescent} {
        r set epoch:key epoch:value
        set before [dplus_info_field r dplus_epoch_reader_entries]
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 64} {incr i} { $rd get epoch:key }
        for {set i 0} {$i < 64} {incr i} { assert_equal "epoch:value" [$rd read] }
        $rd close

        wait_for_condition 100 50 {
            [dplus_info_field r dplus_epoch_reader_entries] > $before &&
            [dplus_info_field r dplus_epoch_workers_active] == 0
        } else {
            fail "epoch reader path did not engage and quiesce: [r info dplus]"
        }
        assert {[dplus_info_field r dplus_reclaim_epoch] >= 1}
        assert_equal 3 [dplus_info_field r dplus_epoch_workers_online]
        assert_equal 3 [dplus_info_field r dplus_epoch_workers_quiescent]
    }

    test {OWNERSHIP EPOCH: pinned reader blocks reclamation until quiescent} {
        r set epoch:retire value:0
        set reclaimed_before [dplus_info_field r dplus_reclaimed_entries]
        set forced_before [dplus_info_field r dplus_forced_reclaims]
        set pinned_epoch [r debug dplus-epoch-pin]
        try {
            for {set i 1} {$i <= 64} {incr i} {
                r set epoch:retire value:$i
            }
            wait_for_condition 100 10 {
                [dplus_epoch_is_blocked r $pinned_epoch 64]
            } else {
                fail "retirements did not accumulate behind pinned reader"
            }
            lassign [dplus_epoch_debug_stats r] epoch retired reclaimed forced advances scans
            assert_equal $reclaimed_before $reclaimed
            assert_equal $forced_before $forced
        } finally {
            assert_equal OK [r debug dplus-epoch-unpin]
        }
        wait_for_condition 100 10 {
            [dplus_info_field r dplus_retired_entries] == 0 &&
            [dplus_info_field r dplus_reclaimed_entries] >= $reclaimed_before + 64
        } else {
            fail "retirements did not reclaim after synthetic quiescence: [r info dplus]"
        }
        assert {[dplus_info_field r dplus_epoch_advances] > 0}
        assert {[dplus_info_field r dplus_epoch_scans] > 0}
    }

    test {OWNERSHIP EPOCH: hard pressure drains a preempted real reader at the watermark} {
        r set epoch:held epoch:value
        r set epoch:pressure value:0
        set mset_args {}
        for {set i 1} {$i <= 40000} {incr i} {
            lappend mset_args epoch:pressure value:$i
        }
        set forced_before [dplus_info_field r dplus_forced_reclaims]
        set pressure_forced_before [dplus_info_field r dplus_pressure_forced_drains]
        set wait_us_before [dplus_info_field r dplus_pressure_forced_wait_us]
        set activations_before [dplus_info_field r dplus_pressure_activations]

        set rd [valkey_deferring_client]
        $rd debug dplus-owner
        set reader_owner [$rd read]
        set writer ""
        set spare_writers {}
        for {set i 0} {$i < 6} {incr i} {
            set candidate [valkey_deferring_client]
            $candidate debug dplus-owner
            set candidate_owner [$candidate read]
            if {$writer eq "" && $candidate_owner != $reader_owner} { set writer $candidate } else { lappend spare_writers $candidate }
        }
        assert {$writer ne ""}
        foreach candidate $spare_writers { $candidate close }
        for {set i 0} {$i < 64} {incr i} { $rd get epoch:held }
        for {set i 0} {$i < 64} {incr i} { assert_equal "epoch:value" [$rd read] }
        assert_equal OK [r debug dplus-epoch-hold 200]
        for {set i 0} {$i < 64} {incr i} { $rd get epoch:held }

        set start_ms [clock milliseconds]
        $writer mset {*}$mset_args
        assert_equal OK [$writer read]
        set elapsed_ms [expr {[clock milliseconds] - $start_ms}]
        for {set i 0} {$i < 64} {incr i} { assert_equal "epoch:value" [$rd read] }
        $rd close
        $writer close

        wait_for_condition 100 10 {
            [dplus_info_field r dplus_retired_entries] == 0 &&
            [dplus_info_field r dplus_reclaim_pressure_gate] == 0 &&
            [dplus_info_field r dplus_epoch_debug_reader_holding] == 0
        } else {
            fail "hard-pressure drain did not reach a clean state: [r info dplus]"
        }
        assert {$elapsed_ms >= 100}
        assert_equal 32768 [dplus_info_field r dplus_retired_peak]
        assert_equal [expr {$forced_before + 1}] [dplus_info_field r dplus_forced_reclaims]
        assert_equal [expr {$pressure_forced_before + 1}] [dplus_info_field r dplus_pressure_forced_drains]
        assert {[dplus_info_field r dplus_pressure_forced_wait_us] >= $wait_us_before + 100000}
        assert {[dplus_info_field r dplus_pressure_activations] > $activations_before}
    }

    test {OWNERSHIP ACL: -get user is denied NOPERM, pipelined at depth} {
        r set akey aval
        r acl setuser nogetter on >pw ~* +@all -get
        set rd [valkey_deferring_client]
        $rd auth nogetter pw
        assert_equal "OK" [$rd read]
        # Pipelined depth > 1: every GET must be NOPERM, never a value.
        for {set i 0} {$i < 20} {incr i} { $rd get akey }
        for {set i 0} {$i < 20} {incr i} {
            assert_error "*NOPERM*" {$rd read}
        }
        $rd close
        r acl deluser nogetter
    } {1}

    test {OWNERSHIP ACL: key-pattern-restricted user punts - correct per-key verdicts} {
        r set allowed:k v1
        r set secret:k v2
        r acl setuser scoped on >pw ~allowed:* +@all
        set rd [valkey_deferring_client]
        $rd auth scoped pw
        assert_equal "OK" [$rd read]
        # Mixed batch: in-pattern key succeeds, out-of-pattern is NOPERM.
        for {set i 0} {$i < 10} {incr i} {
            $rd get allowed:k
            $rd get secret:k
        }
        for {set i 0} {$i < 10} {incr i} {
            assert_equal "v1" [$rd read]
            assert_error "*NOPERM*" {$rd read}
        }
        $rd close
        r acl deluser scoped
    } {1}

    test {OWNERSHIP ACL: revocation mid-connection - next batch is denied} {
        r set akey aval
        r acl setuser revokee on >pw ~* +@all
        set rd [valkey_deferring_client]
        $rd auth revokee pw
        assert_equal "OK" [$rd read]
        $rd get akey
        assert_equal "aval" [$rd read]
        # Revoke GET while the connection is idle-owned.
        r acl setuser revokee -get
        for {set i 0} {$i < 10} {incr i} { $rd get akey }
        for {set i 0} {$i < 10} {incr i} {
            assert_error "*NOPERM*" {$rd read}
        }
        $rd close
        r acl deluser revokee
    } {1}

    test {OWNERSHIP ACL: ACL LOAD-free deluser kick re-gates survivors} {
        # deluser on a connected user kicks it to DefaultUser unauthenticated;
        # a subsequent GET on that connection must not be served speculatively.
        r set akey aval
        r acl setuser doomed on >pw ~* +@all
        set rd [valkey_deferring_client]
        $rd auth doomed pw
        assert_equal "OK" [$rd read]
        $rd get akey
        assert_equal "aval" [$rd read]
        r acl deluser doomed
        $rd get akey
        # Connection is closed or the command is rejected - never a value
        # via the speculative path.
        catch {$rd read} e
        assert {$e ne "aval"}
        catch {$rd close}
    }
}

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-ownership yes requirepass secret save {}}} {

    test {OWNERSHIP ACL: requirepass without AUTH gets NOAUTH, never values} {
        r auth secret
        r set akey aval
        # Raw deferring connection (no implicit SELECT), pipelined at depth.
        set rd [valkey [srv "host"] [srv "port"] 1 $::tls]
        for {set i 0} {$i < 20} {incr i} { $rd get akey }
        for {set i 0} {$i < 20} {incr i} {
            assert_error "*NOAUTH*" {$rd read}
        }
        # After AUTH the same connection is served normally. (Both this raw
        # connection and r are on db 0 - r's implicit SELECT 9 failed NOAUTH.)
        $rd auth secret
        assert_equal "OK" [$rd read]
        $rd get akey
        assert_equal "aval" [$rd read]
        $rd close
    }
}

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-always-active yes save {}}} {

    test {OFF-mode forced-offload ACL: -get user denied at depth} {
        # door-1 config (workers parse, no ownership): the same speculation
        # path runs on worker-parsed batches - same gate must hold.
        r set akey aval
        r acl setuser nogetter on >pw ~* +@all -get
        set rd [valkey_deferring_client]
        $rd auth nogetter pw
        assert_equal "OK" [$rd read]
        for {set i 0} {$i < 20} {incr i} { $rd get akey }
        for {set i 0} {$i < 20} {incr i} {
            assert_error "*NOPERM*" {$rd read}
        }
        $rd close
        r acl deluser nogetter
    } {1}
}

# --- D+ delete→re-add refcount regression tests (B10 fix) ---
# Bug: dplusDeferFree deferred the table's decref at dbDelete, so RENAME/MOVE
# (incrRefCount → dbDelete → dbAdd) re-embedded the key while refcount was
# still inflated — serverPanic("Not implemented") in objectSetKeyAndExpire
# for ANY non-string type. Invisible pre-Aug-26: ownership batteries always
# truncated at client-eviction, so keyspace/hashexpire never ran.

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-ownership yes save {}}} {

    test {OWNERSHIP B10: RENAME of non-string types survives} {
        r hset bh f1 v1 f2 v2
        r rename bh bh2
        assert_equal 2 [r hlen bh2]
        r sadd bs m1
        r rename bs bs2
        r rpush bl a
        r rename bl bl2
        r zadd bz 1 m
        r rename bz bz2
        r ping
    } {PONG}

    test {OWNERSHIP B10: RENAME preserves hash-field TTLs} {
        r del th
        r hset th f1 v1 f2 v2
        r hexpire th 300 FIELDS 1 f1
        r rename th th2
        assert_morethan [lindex [r httl th2 FIELDS 1 f1] 0] 290
        assert_equal 2 [r hlen th2]
    }

    test {OWNERSHIP B10: RENAME overwriting an existing dest key} {
        r hset src f v
        r hset dst g w
        r rename src dst
        assert_equal 1 [r hlen dst]
        assert_equal v [r hget dst f]
    }

    test {OWNERSHIP B10: MOVE of non-string type across dbs} {
        r select 9
        r del mh
        r hset mh f v
        assert_equal 1 [r move mh 5]
        r select 5
        assert_equal v [r hget mh f]
        r del mh
        r select 9
        r ping
    } {PONG}
}

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-always-active yes save {}}} {

    test {door-1 B10: RENAME of hash survives under always-active io-threads} {
        r hset bh f1 v1 f2 v2
        r rename bh bh2
        assert_equal 2 [r hlen bh2]
        r ping
    } {PONG}
}

# --- D+ eviction-vs-limbo regression test (B15 fix) ---
# Bug: performEvictions measures used-memory deltas around each delete; the
# limbo deferred frees so deltas read ~0 and eviction returned spurious OOM
# under deep-pipelined write bursts (limbo accumulates within one event-loop
# iteration). Fix: exclusive window + limbo flush across the eviction pass.

start_server {tags {"ownership"} overrides {io-threads 4 io-threads-ownership yes maxmemory 20mb maxmemory-policy allkeys-lru save {}}} {

    test {OWNERSHIP B15: pipelined write burst under maxmemory does not OOM} {
        set rd [valkey_deferring_client]
        set val [string repeat x 4096]
        set batches 40
        set per_batch 100
        for {set b 0} {$b < $batches} {incr b} {
            for {set i 0} {$i < $per_batch} {incr i} {
                $rd set key:[expr {$b*$per_batch+$i}] $val
            }
            for {set i 0} {$i < $per_batch} {incr i} {
                set res [$rd read]
                assert_equal "OK" $res
            }
        }
        $rd close
        # Eviction kept us under the limit and no OOM errors were returned.
        assert {[s used_memory] < [expr {30*1024*1024}]}
        assert {![string match "*errorstat_OOM*" [r info errorstats]]}
    }

    test {OWNERSHIP C8: eviction with epoch backlog and held reader stays exact} {
        # C8 accounting contract under epochs: an eviction pass must (1) enter
        # exclusive mode (waiting out any active reader), (2) force-reclaim the
        # retirement backlog FIRST so its memory counts before live keys are
        # evicted, and (3) never return spurious OOM. Deterministic shape:
        # build a real retired backlog with big-value overwrites, hold a real
        # reader past its final recheck, then push past maxmemory from a
        # different-owner writer.
        r flushall
        wait_for_condition 100 10 {
            [dplus_info_field r dplus_retired_entries] == 0
        } else { fail "retirement backlog did not drain after flushall" }
        r config set maxmemory 0
        r set epoch:held epoch:value
        set payload [string repeat x 4096]
        set rd [valkey_deferring_client]
        $rd debug dplus-owner
        set reader_owner [$rd read]
        set writer ""
        set spares {}
        for {set i 0} {$i < 6} {incr i} {
            set c [valkey_deferring_client]
            $c debug dplus-owner
            if {$writer eq "" && [$c read] != $reader_owner} { set writer $c } else { lappend spares $c }
        }
        assert {$writer ne ""}
        foreach c $spares { $c close }
        # Live working set ~12MB, pipelined (sync per-op writes cost ~51 ms
        # each through the harness Tcl client -- a client-side artifact).
        for {set i 0} {$i < 3000} {incr i} { $writer set live:$i $payload }
        for {set i 0} {$i < 3000} {incr i} { assert_equal OK [$writer read] }
        # Warm the reader connection, then arm a 2000 ms hold and consume it:
        # the held reader stays ACTIVE in its epoch, so retirements below
        # CANNOT be reclaimed behind it -- guaranteed persistent backlog.
        for {set i 0} {$i < 64} {incr i} { $rd get epoch:held }
        for {set i 0} {$i < 64} {incr i} { assert_equal "epoch:value" [$rd read] }
        assert_equal OK [r debug dplus-epoch-hold 2000]
        for {set i 0} {$i < 64} {incr i} { $rd get epoch:held }
        # Retirement backlog: overwrite 500 big keys 4x = 2000 retired 4KB
        # values (~8MB) pinned behind the held reader.
        for {set rep 0} {$rep < 4} {incr rep} {
            for {set i 0} {$i < 500} {incr i} { $writer set live:$i $payload }
            for {set i 0} {$i < 500} {incr i} { assert_equal OK [$writer read] }
        }
        # Mid-hold observation MUST use the non-exclusive DEBUG stats hook:
        # INFO internally creates/releases a temp hashtable whose release
        # enters exclusive mode (stats[1]=retired, stats[3]=forced reclaims).
        set stats [r debug dplus-epoch-stats]
        assert {[lindex $stats 1] > 0}
        set forced_before [lindex $stats 3]
        # Cap memory BELOW current usage: the next write forces an eviction
        # pass whose exclusive window must wait out the held reader, then
        # force-reclaim the backlog BEFORE evicting live keys.
        r config set maxmemory 10mb
        $writer set trigger:key $payload
        assert_equal OK [$writer read]
        # Liveness: all 64 held-batch replies must arrive. Value-or-nil is
        # acceptable -- allkeys-lru may legitimately evict epoch:held itself
        # during the pass; what C8 forbids is a lost/blocked reply.
        for {set i 0} {$i < 64} {incr i} {
            set v [$rd read]
            assert {$v eq "epoch:value" || $v eq ""}
        }
        $rd close
        $writer close
        # Exactness: forced reclaim ran, backlog fully drained, memory under
        # the limit, and no spurious OOM was returned to any client.
        assert {[dplus_info_field r dplus_forced_reclaims] > $forced_before}
        wait_for_condition 100 10 {
            [dplus_info_field r dplus_retired_entries] == 0 &&
            [dplus_info_field r dplus_epoch_debug_reader_holding] == 0
        } else { fail "eviction pass left retirement backlog: [r info dplus]" }
        assert {[s used_memory] <= [expr {10*1024*1024}]}
        assert {![string match "*errorstat_OOM*" [r info errorstats]]}
        assert {[s evicted_keys] > 0}
        r config set maxmemory 20mb
    }

    test {OWNERSHIP B13: WAITING_WRITABLE preserves reads, inspection, and replies} {
        # A live peer stops reading large replies. The owner write handler must
        # become dormant without making CLIENT LIST wait on socket progress.
        # Reads remain active: a later GET is handed to main while the handler
        # is suspended, then the refreshed handler drains every reply in order.
        r config set min-io-threads-avoid-copy-reply 1
        r config set client-output-buffer-limit {normal 64mb 0 0}
        set payload [string repeat x [expr {1024*1024}]]
        r set b13:key $payload
        set rd [valkey_deferring_client]
        $rd client setname b13_waiter
        assert_equal OK [$rd read]
        $rd client id
        set id [$rd read]

        set waiting0 [dplus_info_field r dplus_b13_waiting_transitions]
        set suspend0 [dplus_info_field r dplus_b13_read_suspends]
        set lock0 [dplus_info_field r dplus_b13_info_lock_calls]
        set sent 0
        # Keep sending after WAITING engages: b13_read_suspends proves the
        # readable event fired while the write retry was armed. CLIENT LIST
        # on every turn is the original panic trigger and must stay bounded.
        while {$sent < 16 && [dplus_info_field r dplus_b13_read_suspends] == $suspend0} {
            $rd get b13:key
            $rd flush
            incr sent
            after 10
            assert_match "*id=$id*name=b13_waiter*" [r client list id $id]
        }
        assert {[dplus_info_field r dplus_b13_waiting_transitions] > $waiting0}
        assert {[dplus_info_field r dplus_b13_read_suspends] > $suspend0}
        assert {[dplus_info_field r dplus_b13_info_lock_calls] > $lock0}

        # Queue a sentinel after the waiting-state handoff and then let the
        # peer read. All large replies plus the sentinel must arrive in order.
        $rd echo B13_DONE
        $rd flush
        for {set i 0} {$i < $sent} {incr i} {
            assert_equal [string length $payload] [string length [$rd read]]
        }
        assert_equal B13_DONE [$rd read]
        wait_for_condition 100 10 {
            [dplus_info_field r dplus_b13_handler_fires] > 0
        } else { fail "owner writable handler never fired: [r info dplus]" }
        assert_match "*id=$id*name=b13_waiter*" [r client list id $id]
        $rd close
        r config set client-output-buffer-limit {normal 0 0 0}
    }

    test {OWNERSHIP B13: runtime shrink migrates a waiting writer without loss} {
        r config set min-io-threads-avoid-copy-reply 1
        r config set client-output-buffer-limit {normal 64mb 0 0}
        set payload [string repeat z [expr {1024*1024}]]
        r set b13:resize:key $payload
        set rd [valkey_deferring_client]
        $rd client setname b13_resize
        assert_equal OK [$rd read]
        set waiting0 [dplus_info_field r dplus_b13_waiting_transitions]
        set sent 0
        while {$sent < 16 && [dplus_info_field r dplus_b13_waiting_transitions] == $waiting0} {
            $rd get b13:resize:key
            $rd flush
            incr sent
            after 10
        }
        assert {[dplus_info_field r dplus_b13_waiting_transitions] > $waiting0}
        $rd echo B13_RESIZE_DONE
        $rd flush
        # Shrink removes every owner worker. disownClient must cancel the worker
        # event, preserve buffered output, and install the stock main handler.
        assert_equal OK [r config set io-threads 1]
        for {set i 0} {$i < $sent} {incr i} {
            assert_equal [string length $payload] [string length [$rd read]]
        }
        assert_equal B13_RESIZE_DONE [$rd read]
        assert_equal PONG [r ping]
        assert_equal OK [r config set io-threads 4]
        $rd close
        r config set client-output-buffer-limit {normal 0 0 0}
    }

    test {D+ EPOCH: non-BCAST CLIENT TRACKING registers every read (E7)} {
        # Regression for the E7 correctness gap: reads that speculate bypass
        # trackingRememberKeys, so a non-BCAST tracking client could cache a
        # value with no TrackingTable registration -- a later write would
        # never invalidate it. The eligibility gate must punt such clients.
        set rd_redirection [valkey_deferring_client]
        $rd_redirection client id
        set redir_id [$rd_redirection read]
        $rd_redirection subscribe __redis__:invalidate
        $rd_redirection read
        set rd_sg [valkey_client]
        r CLIENT TRACKING on REDIRECT $redir_id
        set n 50
        for {set i 0} {$i < $n} {incr i} {
            $rd_sg SET e7key$i $i
            r GET e7key$i
        }
        set info [r info]
        regexp "\r\ntracking_total_keys:(.*?)\r\n" $info _ total_keys
        assert_equal $n $total_keys
        # Every overwrite must deliver an invalidation (the pre-fix failure
        # mode was a permanently blocked read here).
        for {set i 0} {$i < $n} {incr i} {
            $rd_sg SET e7key$i again
        }
        for {set i 0} {$i < $n} {incr i} {
            $rd_redirection read
        }
        r CLIENT TRACKING off
        $rd_redirection close
        $rd_sg close
    }
}

start_server {tags {"ownership"} overrides {io-threads 5 io-threads-ownership yes save {}}} {

    test {OWNERSHIP B11: runtime io-threads resize under owned traffic delivers all replies} {
        # Regression for both B11 defects: (1) shrink SIGSEGV -- workers were
        # cancelled while owning client fds (sec-14 sequence); (2) lost-reply
        # wedge -- a client disowned mid-execution (the CONFIG SET issuer)
        # stuck at CLIENT_COMPLETED_IO with its reply buffered forever.
        r set b11:key b11:val
        set clients {}
        for {set i 0} {$i < 4} {incr i} { lappend clients [valkey_deferring_client] }
        # Soak knob: B11_CYCLES repeats the whole transition sequence
        # (default 1 = suite tripwire; crank for revalidation, e.g.
        # B11_CYCLES=50 ./runtest --single unit/ownership --only '/B11').
        set cycles [expr {[info exists ::env(B11_CYCLES)] ? $::env(B11_CYCLES) : 1}]
        for {set cyc 0} {$cyc < $cycles} {incr cyc} {
        foreach n {3 5 2 6 1 5} {
            # In-flight pipelined batches while the resize lands.
            foreach c $clients { for {set j 0} {$j < 32} {incr j} { $c get b11:key } }
            # The issuer itself is the lost-reply victim shape when its own
            # worker is removed (n=1 removes ALL workers): this reply arriving
            # at all is the defect-2 assertion.
            r config set io-threads $n
            foreach c $clients { for {set j 0} {$j < 32} {incr j} { assert_equal "b11:val" [$c read] } }
            assert_equal PONG [r ping]
        }
        }
        foreach c $clients { $c close }
        assert_equal "b11:val" [r get b11:key]
    }

    test {OWNERSHIP B12: giant multibulk under concurrent load parses exactly} {
        # Tripwire for the parser-desync (lost chunk) class: a ~1MB SADD
        # spanning many reads must parse exactly while noise batches keep the
        # workers and main contended. Default 30 iterations = suite tripwire.
        # Soak knob: this test IS the statistical hammer when cranked --
        # pre-fix rate was ~0.5%/iteration, so use e.g.
        # B12_ITERATIONS=4000 ./runtest --single unit/ownership --only '/B12'
        # for a closure-bound revalidation after read-path changes.
        set iters [expr {[info exists ::env(B12_ITERATIONS)] ? $::env(B12_ITERATIONS) : 30}]
        r set noise:k v
        set noise {}
        for {set i 0} {$i < 4} {incr i} { lappend noise [valkey_deferring_client] }
        set args {}
        for {set i 0} {$i < 100000} {incr i} { lappend args m$i }
        for {set it 0} {$it < $iters} {incr it} {
            foreach c $noise { for {set j 0} {$j < 16} {incr j} { $c get noise:k } }
            assert_equal 100000 [r sadd b12:myset {*}$args]
            assert_equal 1 [r unlink b12:myset]
            foreach c $noise { for {set j 0} {$j < 16} {incr j} { assert_equal "v" [$c read] } }
        }
        foreach c $noise { $c close }
    }

    test {OWNERSHIP E4: speculative miss punts so keyspace miss stats fire exactly} {
        # Regression: the worker served nil directly on a speculative miss,
        # silently dropping the keymiss notification AND the stat_keyspace_misses
        # counter (both only happen in main's lookupKey). Fix (E4): punt on miss
        # so main takes the normal path and updates both exactly.
        # stat_keyspace_misses is the robust tripwire here: a worker serving nil
        # never increments it, so N missing GETs must advance it by exactly N.
        # (keymiss keyevent delivery is proven separately in experiment-log.md.)
        r flushall
        # Warm the ownership/speculation read path with real hits.
        r set e4:hit v
        for {set i 0} {$i < 30} {incr i} { assert_equal "v" [r get e4:hit] }
        set before_miss [s keyspace_misses]
        for {set i 0} {$i < 25} {incr i} { assert_equal "" [r get e4:absent:$i] }
        # Every miss must have reached main's lookupKey (a worker serving nil
        # would leave this counter untouched).
        assert_equal [expr {$before_miss + 25}] [s keyspace_misses]
        # A subsequent present-key read must not be miscounted as a miss.
        assert_equal "v" [r get e4:hit]
        assert_equal [expr {$before_miss + 25}] [s keyspace_misses]
    }

    test {OWNERSHIP E3: speculative hits advance keyspace_hits exactly} {
        # Regression: speculative GET hits are served worker-side and bypass
        # main's lookupKey, where stat_keyspace_hits is incremented -- so hits
        # were silently uncounted. Fix (E3): a distinct per-thread keyspace_hits
        # counter is folded into stat_keyspace_hits by dplusAggregateStats.
        r flushall
        r set e3:k v
        set before_hits [s keyspace_hits]
        for {set i 0} {$i < 100} {incr i} { assert_equal "v" [r get e3:k] }
        # Fold has bounded (one event-loop) lag; a following round-trip settles it.
        r ping
        assert_equal [expr {$before_hits + 100}] [s keyspace_hits]
    }

    test {OWNERSHIP F1: attached MONITOR observes GETs (speculation gated off while monitoring)} {
        # Regression: speculative GET hits are served worker-side and bypass
        # call()'s replicationFeedMonitors, so ~all speculated reads were invisible
        # to MONITOR. Fix (F1): gate speculation off while any monitor is attached,
        # so MONITOR sees the exact stock command stream.
        r flushall
        r set f1:k v
        for {set i 0} {$i < 30} {incr i} { assert_equal "v" [r get f1:k] }
        set mon [valkey_deferring_client]
        $mon monitor
        assert_equal "OK" [$mon read]
        r get f1:k
        # Sentinel (never speculated) guarantees the read loop terminates even if
        # a regression re-hid the GET -- then saw_get stays 0 and the assert fails.
        r echo SENTINEL_F1
        set saw_get 0
        while 1 {
            set line [$mon read]
            if {[string match {*"echo"*SENTINEL_F1*} $line]} break
            if {[string match {*"get"*f1:k*} $line]} { set saw_get 1 }
        }
        assert_equal 1 $saw_get
        $mon close
    }
}
