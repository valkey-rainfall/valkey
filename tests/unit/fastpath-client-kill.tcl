# Kill, transaction and accounting interactions with fast-path (IO-owned) clients.
# Each test checks INFO fastpath engagement counters. The control connection `r`
# runs SELECT 9, so it stays on the main path.

proc fpck_info {field} { getInfoProperty [r info fastpath] $field }
proc fpck_stat {field} { getInfoProperty [r info] $field }

proc fpck_wait_fastpath_clients {n} {
    wait_for_condition 300 20 {
        [fpck_info fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc fpck_client {} { return [valkey [srv 0 host] [srv 0 port] 1 $::tls] }

proc fpck_id {c} {
    $c client id
    return [$c read]
}

# One PING round trip per client, then wait until all are on the fast path.
proc fpck_engage_all {clients} {
    foreach c $clients { $c ping }
    foreach c $clients { assert_equal PONG [$c read] }
}

# Kill every normal client but the control connection (leftovers of a failed test).
proc fpck_reset {} {
    r client kill user default
    fpck_wait_fastpath_clients 0
}

# A deferring client kept on the main path (tracking keeps it off the fast path).
proc fpck_main_client {} {
    set c [fpck_client]
    $c client tracking on
    assert_equal OK [$c read]
    $c select 9
    assert_equal OK [$c read]
    return $c
}

proc fpck_alive {c} {
    if {[catch {$c ping; $c read} e]} { return 0 }
    return [expr {$e eq "PONG"}]
}

# ---------------------------------------------------------------------------
# Plan item 13 (CLI-06, CB-19): CLIENT KILL of fast-path victims.
# ---------------------------------------------------------------------------
start_server {tags {"fastpath client-kill external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    r select 9
    # The control connection runs as an admin user, so "KILL USER default" never hits it.
    r acl setuser fpckadmin on nopass ~* &* +@all
    r auth fpckadmin ""

    test {CLI-06: CLIENT KILL ID of a fast-path victim from inside MULTI/EXEC} {
        set v [fpck_client]
        set vid [fpck_id $v]
        fpck_wait_fastpath_clients 1
        # Keep the victim busy with a pipeline the server is still answering.
        for {set i 0} {$i < 200} {incr i} { $v incr ctr }
        assert_equal 1 [fpck_info fastpath_clients]
        r multi
        r client kill id $vid
        r ping
        assert_equal {1 PONG} [r exec]
        # Drain what the victim got before the kill, then the connection must be closed.
        set got 0
        while {$got < 200 && ![catch {$v read}]} { incr got }
        assert {[catch {$v ping; $v read}]}
        $v close
        fpck_wait_fastpath_clients 0
        assert_equal 0 [fpck_stat blocked_clients]
        # The killed pipeline left the counter in a consistent state.
        r select 0
        set n [r get ctr]
        assert {$n eq {} || ($n >= 0 && $n <= 200)}
        r del ctr
        r select 9
    }

    test {CLI-06: CLIENT KILL from EVAL is refused (CLIENT is noscript), victim untouched} {
        fpck_reset
        set v [fpck_client]
        set vid [fpck_id $v]
        fpck_wait_fastpath_clients 1
        assert_error {*not allowed from script*} {r eval {return server.call('client','kill','id',ARGV[1])} 0 $vid}
        assert_equal 1 [fpck_alive $v]
        $v close
        fpck_wait_fastpath_clients 0
    }

    test {CB-19: CLIENT UNBLOCK of a killer does nothing, blocked_clients returns to 0} {
        fpck_reset
        set clients {}
        for {set i 0} {$i < 50} {incr i} { lappend clients [fpck_client] }
        fpck_engage_all $clients
        fpck_wait_fastpath_clients 50
        set k [fpck_main_client] ;# on the main path, so it may block in CLIENT KILL
        $k auth fpckadmin ""
        assert_equal OK [$k read]
        set kid [fpck_id $k]
        foreach c $clients { for {set j 0} {$j < 20} {incr j} { $c incr cb19 } }
        $k client kill user default
        # A killer waiting for IO-owned victims is not CLIENT UNBLOCK-able.
        assert_equal 0 [r client unblock $kid]
        assert_equal 50 [$k read]
        fpck_wait_fastpath_clients 0
        wait_for_condition 100 20 { [fpck_stat blocked_clients] == 0 } else { fail "blocked_clients stuck: [r info clients]" }
        foreach c $clients { catch {$c close} }
        $k close
        r select 0
        r del cb19
        r select 9
    }

    test {CLI-06: CLIENT KILL USER default matching 200 fast-path clients replies 200} {
        fpck_reset
        set clients {}
        for {set i 0} {$i < 200} {incr i} { lappend clients [fpck_client] }
        fpck_engage_all $clients
        fpck_wait_fastpath_clients 200
        assert_equal 200 [r client kill user default]
        fpck_wait_fastpath_clients 0
        assert_equal 0 [fpck_stat blocked_clients]
        foreach c $clients { catch {$c close} }
    }
}

# ---------------------------------------------------------------------------
# Plan item 14 (CLI-05, CLI-30): killer vs 200 pipelining fast-path clients.
# For every client: replies are 1..replied in order, and the applied count
# (the key value) is >= replied and at most one batch more.
# ---------------------------------------------------------------------------
start_server {tags {"fastpath client-kill external:skip tls:skip"} overrides {io-threads 5 io-threads-always-active yes io-threads-parallel-writes-min 1}} {
    r select 9

    test {CLI-05/CLI-30: applied >= replied and applied - replied <= one batch for killed fast-path clients} {
        set nclients 200
        set batch 20
        set rounds 5
        set clients {}
        set ids {}
        for {set i 0} {$i < $nclients} {incr i} {
            set c [fpck_client]
            lappend clients $c
            lappend ids [fpck_id $c]
        }
        fpck_wait_fastpath_clients $nclients
        set e0 [fpck_info fastpath_write_lane_entries]
        set b0 [fpck_info fastpath_batches]
        for {set i 0} {$i < $nclients} {incr i} { set replied($i) 0; set dead($i) 0 }
        set killed 0
        for {set r_ 0} {$r_ < $rounds} {incr r_} {
            for {set i 0} {$i < $nclients} {incr i} {
                if {$dead($i)} continue
                set c [lindex $clients $i]
                for {set j 0} {$j < $batch} {incr j} { $c incr w2ctr:$i }
            }
            # Killer: every 7th live client this round, by ID.
            for {set i [expr {$r_ % 7}]} {$i < $nclients} {incr i 7} {
                if {$dead($i)} continue
                set dead($i) 1
                incr killed [r client kill id [lindex $ids $i]]
            }
            for {set i 0} {$i < $nclients} {incr i} {
                set c [lindex $clients $i]
                if {$dead($i) == 2} continue
                for {set j 0} {$j < $batch} {incr j} {
                    if {[catch {$c read} v]} { set dead($i) 2; break }
                    incr replied($i)
                    assert_equal $replied($i) $v "client $i reply out of order"
                }
                if {$dead($i) == 1} {
                    # Killed this round: drain until the connection reports closed.
                    while {![catch {$c read} v]} { incr replied($i); assert_equal $replied($i) $v }
                    set dead($i) 2
                }
            }
        }
        assert {$killed > 0}
        r select 0
        for {set i 0} {$i < $nclients} {incr i} {
            set applied [r get w2ctr:$i]
            if {$applied eq {}} { set applied 0 }
            assert {$applied >= $replied($i)}
            assert {$applied - $replied($i) <= $batch}
            if {$dead($i) == 0} { assert_equal $applied $replied($i) }
        }
        r flushdb
        r select 9
        foreach c $clients { catch {$c close} }
        fpck_wait_fastpath_clients 0
        # Engagement: the INCRs ran on the fast path.
        assert {[fpck_info fastpath_batches] > $b0}
        puts "fastpath_write_lane_entries delta [expr {[fpck_info fastpath_write_lane_entries] - $e0}] killed $killed"
    }
}

# ---------------------------------------------------------------------------
# Plan item 32 (TXS-18): busy EVAL and a pipelining fast-path client.
# Every OK must be applied, every BUSY must not be, the order is OK* BUSY* OK*,
# and the script sees no write while it runs.
# ---------------------------------------------------------------------------
start_server {tags {"fastpath scripting external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes busy-reply-threshold 50}} {
    r select 9

    test {TXS-18: reply order and isolation for a pipelining fast-path client across a busy EVAL} {
        set a [fpck_client]
        set b [fpck_main_client]
        set probe [fpck_main_client]
        $a ping
        assert_equal PONG [$a read]
        fpck_wait_fastpath_clients 1
        set s0 [fpck_info fastpath_batches]
        # Script spins on its own key space (db 9); a, on db 0, writes keys t:*.
        set script {
            server.call('select', 0)
            local n0 = server.call('dbsize')
            local x = 0
            for i = 1, tonumber(ARGV[1]) do x = x + 1 end
            return {n0, server.call('dbsize')}
        }
        for {set i 0} {$i < 100} {incr i} { $a set t:$i v }
        $b eval $script 0 100000000
        # Wait until the script is busy (the probe gets BUSY).
        wait_for_condition 200 10 {
            [catch {$probe ping; $probe read} e] && [string match {BUSY*} $e]
        } else { fail "script never became busy" }
        for {set i 100} {$i < 110} {incr i} { $a set t:$i v }
        set res [$b read]
        set replies {}
        for {set i 0} {$i < 110} {incr i} {
            catch {$a read} v
            lappend replies $v
        }
        # Order: OK* BUSY* OK*.
        set state 0
        for {set i 0} {$i < 110} {incr i} {
            set v [lindex $replies $i]
            set busy [string match {BUSY*} $v]
            assert {$v eq "OK" || $busy}
            if {$state == 0 && $busy} { set state 1 }
            if {$state == 1 && !$busy} { set state 2 }
            if {$state == 2 && $busy} { fail "reply $i BUSY after OK: $replies" }
        }
        set nbusy [llength [lsearch -all -glob $replies BUSY*]]
        if {$::verbose} { puts "TXS-18: [expr {110 - $nbusy}] OK, $nbusy BUSY, script dbsize before/after: $res" }
        # Whether the ten later SETs are read during the script (BUSY) or after it (OK) is
        # timing; both are valid. The order, the applied-iff-OK rule and isolation are not.
        # Applied iff OK.
        r select 0
        for {set i 0} {$i < 110} {incr i} {
            set v [lindex $replies $i]
            assert_equal [expr {$v eq "OK"}] [r exists t:$i] "key t:$i reply $v"
        }
        # Isolation: nothing was written while the script ran.
        assert_equal [lindex $res 0] [lindex $res 1]
        r flushdb
        r select 9
        assert {[fpck_info fastpath_batches] > $s0}
        $a close; $b close; $probe close
    }
}

# ---------------------------------------------------------------------------
# Plan item 24 (OBS-15, MEM-04): client memory accounting with pending replies.
# 5 clients each GET a 32 MB value and do not read it. With io-threads 1 the
# pending bytes show in mem_clients_normal; the fast path must match.
# ---------------------------------------------------------------------------
proc fpck_pending_reply_test {label fp} {
    test "OBS-15: mem_clients_normal counts pending replies ($label)" {
        # 32 MB replies: the kernel absorbs at most about 10 MB per loopback connection
        # (tcp_wmem max 4 MB + tcp_rmem max 6 MB), so at least 20 MB stays pending per client.
        set size 33554432
        r select 0
        r setrange big [expr {$size - 1}] x
        r select 9
        set n 5
        set clients {}
        for {set i 0} {$i < $n} {incr i} { lappend clients [fpck_client] }
        fpck_engage_all $clients
        if {$fp} { fpck_wait_fastpath_clients $n }
        set m0 [fpck_stat mem_clients_normal]
        set o0 [fpck_info fastpath_net_output_bytes]
        foreach c $clients { $c get big }
        # Wait until the server has produced every reply (written or pending).
        wait_for_condition 200 20 {
            [fpck_stat mem_clients_normal] - $m0 >= $n * ($size - 12582912) / 2
        } else {
            fail "pending replies not counted: mem_clients_normal [fpck_stat mem_clients_normal] (start $m0), client_recent_max_output_buffer [fpck_stat client_recent_max_output_buffer], [r info fastpath]"
        }
        if {$fp} { assert {[fpck_info fastpath_net_output_bytes] > $o0} }
        foreach c $clients { $c close }
        r select 0
        r del big
        r select 9
    }
}

start_server {tags {"fastpath memory external:skip tls:skip"} overrides {io-threads 1 io-threads-fast-path no}} {
    r select 9
    fpck_pending_reply_test baseline 0
}
start_server {tags {"fastpath memory external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    r select 9
    fpck_pending_reply_test fastpath 1
}
