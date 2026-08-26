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
}
