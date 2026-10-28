# Wave 2 semantics tests, part B: items 6, 12, 21, 34.
# Same conventions as wave2-semantics.tcl: fast-path clients are unnamed raw sockets on db 0,
# control clients select db 0 and keep a name, notify-keyspace-events is cleared at runtime,
# and every test asserts that an INFO fastpath counter moved.

set ::w6b_open {}

proc w6b_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    lappend ::w6b_open $fd
    return $fd
}

proc w6b_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w6b_line {fd} {
    set line ""
    while {1} {
        set ch [read $fd 1]
        if {$ch eq ""} { error "connection closed" }
        if {$ch eq "\n"} { return [string range $line 0 end-1] }
        append line $ch
    }
}

proc w6b_read_reply {fd} {
    set line [w6b_line $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            set data [read $fd [expr {[string range $line 1 end] + 2}]]
            return [string range $data 0 end-2]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w6b_read_reply $fd] }
            return $out
        }
        {:} { return [string range $line 1 end] }
        default { return $line }
    }
}

proc w6b_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [w6b_read_reply $fd] }
    return $out
}

proc w6b_send {fd buf} {
    puts -nonewline $fd $buf
    flush $fd
}

proc w6b_info {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

proc w6b_stat {c field} {
    return [getInfoProperty [$c info stats] $field]
}

proc w6b_control {c} {
    $c client setname w6-control
    $c select 0
    $c config set notify-keyspace-events {}
}

# ---------------------------------------------------------------------------------------------
# Item 21 (EV-07): maxmemory-eviction-batch yes, noeviction, the server is over maxmemory.
# FLUSHALL SYNC and a SET in the same read: the SET must succeed, as it does with batch off
# and on stock. The cached OOM verdict must not outlive the memory that caused it.
# ---------------------------------------------------------------------------------------------
proc w6b_evict_case {host port batch} {
    r flushall sync
    r config set maxmemory 0
    r config set maxmemory-policy noeviction
    r config set maxmemory-eviction-batch $batch
    set fd [w6b_socket $host $port]
    set b0 [w6b_info r fastpath_batches]
    set buf ""
    set v [string repeat x 1000]
    for {set i 0} {$i < 20000} {incr i} { append buf [w6b_resp set fill:$i $v] }
    w6b_send $fd $buf
    w6b_read_replies $fd 20000
    assert_morethan [w6b_info r fastpath_batches] $b0
    r config set maxmemory [expr {[s used_memory] / 2}]
    # one round trip while over the limit, so the OOM verdict is computed and cached
    w6b_send $fd [w6b_resp set probe v]
    assert_match {*OOM*} [w6b_read_reply $fd]
    w6b_send $fd "[w6b_resp flushall sync][w6b_resp set after v]"
    set got [w6b_read_replies $fd 2]
    close $fd
    r config set maxmemory 0
    return $got
}

start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save {}}} {
    w6b_control r
    set host [srv 0 host]
    set port [srv 0 port]

    test {Wave2 #21: noeviction, batch off: FLUSHALL then SET in the same read succeeds (baseline)} {
        assert_equal {+OK +OK} [w6b_evict_case $host $port no]
        assert_equal v [r get after]
    }

    test {Wave2 #21: noeviction, maxmemory-eviction-batch yes: FLUSHALL then SET in the same read succeeds} {
        assert_equal {+OK +OK} [w6b_evict_case $host $port yes]
        assert_equal v [r get after]
    }
    r config set maxmemory-eviction-batch no
}

# ---------------------------------------------------------------------------------------------
# Item 6 (EXP-05, STR-18): expire edge cases from lane pipelines are replicated exactly.
# Absolute expiry times are compared (PEXPIRETIME, HPEXPIRETIME) so no clock reading is involved.
# ---------------------------------------------------------------------------------------------
start_server {tags {"wave2 repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save {}}} {
    set primary [srv 0 client]
    w6b_control $primary
    set phost [srv 0 host]
    set pport [srv 0 port]
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save {}}} {
        set replica [srv 0 client]
        $replica client setname w6-replica-control
        $replica select 0
        $replica replicaof $phost $pport
        wait_for_condition 200 50 {
            [string match {*master_link_status:up*} [$replica info replication]]
        } else { fail "replica did not connect" }

        test {Wave2 #6: EXPIRE -1, EXPIRE 100, SET EX KEEPTTL, GETEX PERSIST, HEXPIRE 0 from lanes replicate exactly} {
            set n 200
            set buf ""
            for {set i 0} {$i < $n} {incr i} {
                append buf [w6b_resp set neg:$i v] [w6b_resp expire neg:$i -1]
                append buf [w6b_resp set pos:$i v] [w6b_resp expire pos:$i 100]
                append buf [w6b_resp set kt:$i v ex 100] [w6b_resp set kt:$i v2 keepttl]
                append buf [w6b_resp set ps:$i v ex 100] [w6b_resp getex ps:$i persist]
                append buf [w6b_resp hset hz:$i f v g w] [w6b_resp hexpire hz:$i 0 fields 1 f]
                append buf [w6b_resp hset hd:$i f v] [w6b_resp hexpire hd:$i 0 fields 1 f]
                append buf [w6b_resp set pa:$i v] [w6b_resp pexpireat pa:$i 1]
            }
            set fd [w6b_socket $phost $pport]
            set lane0 [w6b_info $primary fastpath_write_lane_entries]
            w6b_send $fd $buf
            set got [w6b_read_replies $fd [expr {$n * 14}]]
            close $fd
            assert_morethan [w6b_info $primary fastpath_write_lane_entries] $lane0
            for {set i 0} {$i < $n} {incr i} {
                set r [lrange $got [expr {$i * 14}] [expr {$i * 14 + 13}]]
                assert_equal {+OK 1 +OK 1 +OK +OK +OK v 2 2 1 2 +OK 1} $r
            }
            wait_for_ofs_sync $primary $replica
            set diff 0
            foreach c [list $primary $replica] {
                set out($c) {}
                for {set i 0} {$i < $n} {incr i} {
                    lappend out($c) [$c exists neg:$i] [$c exists pa:$i] [$c exists hd:$i] \
                        [$c pexpiretime pos:$i] [$c pexpiretime kt:$i] [$c get kt:$i] \
                        [$c pexpiretime ps:$i] [$c hgetall hz:$i] [$c hpexpiretime hz:$i fields 1 g]
                }
            }
            assert_equal $out($primary) $out($replica)
            for {set i 0} {$i < $n} {incr i} {
                assert_equal {0 0 0} [list [$replica exists neg:$i] [$replica exists pa:$i] [$replica exists hd:$i]]
                assert_equal -1 [$replica pexpiretime ps:$i]
                assert_morethan [$replica pexpiretime kt:$i] 0
                assert_equal {g w} [$replica hgetall hz:$i]
            }
            assert_equal [$primary dbsize] [$replica dbsize]
        }
    }
}

# ---------------------------------------------------------------------------------------------
# Item 34 (CB-06, CB-05): HSETEX across all shards from lane pipelines, HPERSIST half of the
# fields, then active field expiry. expired_fields grows by exactly the volatile half.
# ---------------------------------------------------------------------------------------------
start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes save {}}} {
    w6b_control r
    set host [srv 0 host]
    set port [srv 0 port]

    test {Wave2 #34: HSETEX on every shard, HPERSIST half, active expiry removes exactly the volatile half} {
        r flushall sync
        r debug set-active-expire 0
        set n 2048
        set fd [w6b_socket $host $port]
        set lane0 [w6b_info r fastpath_write_lane_entries]
        set exp0 [w6b_stat r expired_fields]
        set buf ""
        for {set i 0} {$i < $n} {incr i} {
            append buf [w6b_resp hsetex hx:$i px 3000 fields 2 keep v drop v]
        }
        for {set i 0} {$i < $n} {incr i} {
            append buf [w6b_resp hpersist hx:$i fields 1 keep]
        }
        w6b_send $fd $buf
        set got [w6b_read_replies $fd [expr {2 * $n}]]
        close $fd
        assert_morethan [w6b_info r fastpath_write_lane_entries] $lane0
        assert_equal [lrepeat $n 1] [lrange $got 0 [expr {$n - 1}]]
        # every HPERSIST must hit a live field (1); -2 would mean the setup outran the 3 s TTL
        assert_equal [lrepeat $n 1] [lrange $got $n end]
        r debug set-active-expire 1
        wait_for_condition 400 50 {
            [w6b_stat r expired_fields] - $exp0 >= $n
        } else {
            fail "expired_fields grew by [expr {[w6b_stat r expired_fields] - $exp0}], expected $n"
        }
        assert_equal $n [expr {[w6b_stat r expired_fields] - $exp0}]
        assert_equal $n [r dbsize]
        for {set i 0} {$i < $n} {incr i 97} {
            assert_equal {keep v} [r hgetall hx:$i]
        }
    }
}

# ---------------------------------------------------------------------------------------------
# Item 12 (CLI-10, STR-19, TXS-30): seeded random pipelines, split at random TCP offsets, give
# byte-identical reply streams at io-threads 1 and io-threads 5. The seed is printed on failure;
# set ::w6b_seed to replay one.
# ---------------------------------------------------------------------------------------------
proc w6b_gen {seed ncmds} {
    expr {srand($seed)}
    set buf ""
    for {set i 0} {$i < $ncmds} {incr i} {
        set k k[expr {int(rand() * 40)}]
        set k2 k[expr {int(rand() * 40)}]
        set v [string repeat [format %c [expr {97 + int(rand() * 26)}]] [expr {int(rand() * 70)}]]
        switch [expr {int(rand() * 26)}] {
            0 { append buf [w6b_resp set $k $v] }
            1 { append buf [w6b_resp get $k] }
            2 { append buf [w6b_resp incr $k] }
            3 { append buf [w6b_resp mget $k $k2 k99] }
            4 { append buf [w6b_resp mset $k $v $k2 $v] }
            5 { append buf [w6b_resp del $k $k2] }
            6 { append buf [w6b_resp append $k $v] }
            7 { append buf [w6b_resp strlen $k] }
            8 { append buf [w6b_resp getdel $k] }
            9 { append buf [w6b_resp rpush l$k $v] }
            10 { append buf [w6b_resp lrange l$k 0 -1] }
            11 { append buf [w6b_resp hset h$k f[expr {int(rand() * 5)}] $v] }
            12 { append buf [w6b_resp hgetall h$k] }
            13 { append buf [w6b_resp zadd z$k [expr {int(rand() * 9)}] $v] }
            14 { append buf [w6b_resp zrange z$k 0 -1 withscores] }
            15 { append buf [w6b_resp multi] [w6b_resp incr $k] [w6b_resp get $k2] [w6b_resp exec] }
            16 { append buf [w6b_resp lpush $k x] }
            17 { append buf [w6b_resp exists $k $k2] }
            18 { append buf [w6b_resp nosuchcmd $k] }
            19 { append buf [w6b_resp getset $k $v] }
            20 { append buf [w6b_resp setnx $k $v] }
            21 { append buf [w6b_resp incrby $k notanumber] }
            22 { append buf [w6b_resp select [expr {int(rand() * 3)}]] }
            23 { append buf [w6b_resp client setname [expr {rand() < 0.5 ? "" : "w6n"}]] }
            24 { append buf [w6b_resp rpush bl$k x] [w6b_resp blpop bl$k 1] }
            25 { append buf "*1\r\n\$4\r\nPING\r\n" "PING inline\r\n" }
        }
    }
    append buf [w6b_resp select 0] [w6b_resp echo w6-end-marker]
    return $buf
}

proc w6b_run_stream {host port buf seed} {
    set fd [w6b_socket $host $port]
    fconfigure $fd -blocking 0
    expr {srand($seed + 7)}
    set pos 0
    set len [string length $buf]
    set out ""
    set marker "\$13\r\nw6-end-marker\r\n"
    while {$pos < $len} {
        set step [expr {1 + int(rand() * 300)}]
        puts -nonewline $fd [string range $buf $pos [expr {$pos + $step - 1}]]
        flush $fd
        incr pos $step
        append out [read $fd]
    }
    fconfigure $fd -blocking 1
    while {[string range $out end-[expr {[string length $marker] - 1}] end] ne $marker} {
        fconfigure $fd -blocking 1
        set chunk [read $fd 1]
        if {$chunk eq ""} { error "connection closed" }
        append out $chunk
        fconfigure $fd -blocking 0
        append out [read $fd]
    }
    close $fd
    return $out
}

start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 1 save {}}} {
    set stock [srv 0 client]
    w6b_control $stock
    set shost [srv 0 host]
    set sport [srv 0 port]
    start_server {overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save {}}} {
        w6b_control r
        set host [srv 0 host]
        set port [srv 0 port]
        test {Wave2 #12: seeded random pipelines at random TCP split points give identical replies at io-threads 1 and 5} {
            if {[info exists ::w6b_seed]} { set seeds [list $::w6b_seed] } else {
                set seeds {}
                for {set s 0} {$s < 8} {incr s} { lappend seeds [expr {1000 + $s * 7919}] }
            }
            set b0 [w6b_info r fastpath_batches]
            set lane0 [w6b_info r fastpath_write_lane_entries]
            foreach seed $seeds {
                $stock flushall sync
                r flushall sync
                set buf [w6b_gen $seed 3000]
                set a [w6b_run_stream $shost $sport $buf $seed]
                set b [w6b_run_stream $host $port $buf $seed]
                if {$a ne $b} {
                    set i 0
                    while {[string index $a $i] eq [string index $b $i]} { incr i }
                    fail "seed $seed: reply streams differ at byte $i: stock=[string range $a $i [expr {$i + 60}]] lanes=[string range $b $i [expr {$i + 60}]]"
                }
                assert_equal [$stock dbsize] [r dbsize]
            }
            assert_morethan [w6b_info r fastpath_batches] $b0
            assert_morethan [w6b_info r fastpath_write_lane_entries] $lane0
        }
    }
}
