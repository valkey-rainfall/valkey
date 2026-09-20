# The adaptive io_uring share: each I/O thread tracks how many commands its
# reads carry and stops batching reads through the ring while that number is
# high. The gauge and the decision live outside the liburing build, so this
# file runs on any build; only the suppression counter needs a live ring.
source tests/support/benchmark.tcl

proc io_uring_info {} {
    r info io_uring
}

# Thresholds of the decision (IO_URING_ADAPTIVE_HIGH_CMDS / _LOW_CMDS in
# io_uring_batch.h): batching stops above HIGH commands per read, resumes
# below LOW.
set ::adaptive_high 4
set ::adaptive_low 2

start_server {config "minimal.conf" tags {"external:skip" "valgrind:skip" "tls:skip"} overrides {io-threads 4 io-threads-always-active yes io-uring yes}} {
    set host [srv host]
    set port [srv port]
    assert_equal {io-uring yes} [r config get io-uring]
    assert_equal {io-uring-adaptive-share yes} [r config get io-uring-adaptive-share]

    test {io_uring adaptive share: the gauge follows pipeline depth up and down} {
        # Deep pipelines: every read carries many commands.
        exec {*}[valkeybenchmark $host $port "-c 16 -P 16 -n 64000 -t set -q"]
        set info [io_uring_info]
        set deep [getInfoProperty $info io_uring_cmds_per_read]
        assert_morethan $deep $::adaptive_high

        # Depth one: the EWMA (weight 1/64 per read) decays within a few
        # hundred reads per thread; each connection makes thousands.
        exec {*}[valkeybenchmark $host $port "-c 16 -P 1 -n 64000 -t set -q"]
        set info [io_uring_info]
        set shallow [getInfoProperty $info io_uring_cmds_per_read]
        assert_lessthan $shallow $::adaptive_low
        assert_morethan_equal $shallow 1.0
    }

    test {io_uring adaptive share: suppression engages only while reads are deep} {
        if {[getInfoProperty [io_uring_info] io_uring_active] ne 1} {
            # No ring on this build or kernel: the cap is never consulted.
            assert_equal 0 [getInfoProperty [io_uring_info] io_uring_adaptive_suppressed]
        } else {
            set before [getInfoProperty [io_uring_info] io_uring_adaptive_suppressed]
            exec {*}[valkeybenchmark $host $port "-c 16 -P 1 -n 64000 -t set -q"]
            assert_equal $before [getInfoProperty [io_uring_info] io_uring_adaptive_suppressed]

            exec {*}[valkeybenchmark $host $port "-c 16 -P 16 -n 64000 -t set -q"]
            assert_morethan [getInfoProperty [io_uring_info] io_uring_adaptive_suppressed] $before
        }
    }

    test {io_uring adaptive share: turning it off leaves the gauge and stops the decision} {
        assert_equal {OK} [r config set io-uring-adaptive-share no]
        set suppressed [getInfoProperty [io_uring_info] io_uring_adaptive_suppressed]
        exec {*}[valkeybenchmark $host $port "-c 16 -P 16 -n 64000 -t set -q"]
        set info [io_uring_info]
        assert_morethan [getInfoProperty $info io_uring_cmds_per_read] $::adaptive_high
        assert_equal $suppressed [getInfoProperty $info io_uring_adaptive_suppressed]
        assert_equal {OK} [r config set io-uring-adaptive-share yes]
    }
}
