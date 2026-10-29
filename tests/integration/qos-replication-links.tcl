# A replication link gets its priority when it connects (the replica's link to its primary) or at
# PSYNC (the primary's link to a replica), not from priority-subnets. A runtime CONFIG SET
# priority-subnets reclassifies every client by peer address; the links must keep what they have,
# or replication traffic loses QoS until the next reconnect. MONITOR clients are ordinary clients.

proc qos_flags_of {r pattern} {
    foreach line [split [$r client list] "\n"] {
        if {[string match "*$pattern*" $line] && [regexp {flags=(\S+)} $line _ flags]} {
            return $flags
        }
    }
    return ""
}

proc qos_priority_count {r} {
    getInfoProperty [$r info clients] connected_priority_clients
}

start_server {tags {"repl qos external:skip"}} {
    start_server {} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_replica_online $primary

        test {Replication links are priority before any priority-subnets is set} {
            assert_match {*S*H*} [qos_flags_of $primary "flags=S"]
            assert_match {*M*H*} [qos_flags_of $replica "flags=M"]
            # The replica counts its link at connect time. The primary promotes the replica's link at
            # PSYNC without touching connected_priority_clients, so its count is 0 here; the
            # reclassification below recomputes it from the per-client state.
            assert_equal 1 [qos_priority_count $replica]
        }

        test {Runtime CONFIG SET priority-subnets excluding the peers keeps the replication links priority} {
            # A subnet that matches neither side of the link. The admin clients are on loopback too and
            # stay or become normal; only the two links keep H.
            $primary config set priority-subnets "10.255.255.0/24"
            $replica config set priority-subnets "10.255.255.0/24"
            assert_match {*S*H*} [qos_flags_of $primary "flags=S"]
            assert_match {*M*H*} [qos_flags_of $replica "flags=M"]
            assert_match {N} [qos_flags_of $primary "cmd=client|list"]
            assert_equal 1 [qos_priority_count $primary]
            assert_equal 1 [qos_priority_count $replica]
        }

        test {Runtime CONFIG SET priority-subnets including the peers counts the links once} {
            $primary config set priority-subnets "$primary_host/32"
            $replica config set priority-subnets "$primary_host/32"
            assert_match {*S*H*} [qos_flags_of $primary "flags=S"]
            assert_match {*M*H*} [qos_flags_of $replica "flags=M"]
            # Every loopback client is priority now: the link plus the test's own connection.
            assert_equal [llength [split [string trim [$primary client list]] "\n"]] [qos_priority_count $primary]
            assert_equal [llength [split [string trim [$replica client list]] "\n"]] [qos_priority_count $replica]
        }

        test {Runtime CONFIG SET priority-subnets demotes a MONITOR client like any other} {
            set mon [valkey_deferring_client -1]
            $mon monitor
            $mon read
            wait_for_condition 50 100 {[qos_flags_of $primary "flags=O"] ne ""} else {fail "no MONITOR client"}
            assert_match {*O*H*} [qos_flags_of $primary "flags=O"]
            $primary config set priority-subnets "10.255.255.0/24"
            assert_match {*S*H*} [qos_flags_of $primary "flags=S"]
            set mon_flags [qos_flags_of $primary "flags=O"]
            assert {[string first "H" $mon_flags] == -1}
            assert_equal 1 [qos_priority_count $primary]
            $mon close
        }

        test {The link still replicates after the reclassification} {
            $primary set qos:k 1
            wait_for_ofs_sync $primary $replica
            assert_equal 1 [$replica get qos:k]
            $primary config set priority-subnets ""
            $replica config set priority-subnets ""
        }
    }
}
