/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Phase trace of the staged loop. See phase_trace.h. */

#include "server.h"
#include "phase_trace.h"

#include <stdio.h>

int pt_on = 0;
ptRing pt_rings[IO_THREADS_MAX_NUM];

static uint64_t pt_size = 0;     /* records per ring, a power of two; 0 before the first START */
static int pt_nrings = 0;        /* rings 0..pt_nrings-1 are allocated */
static uint64_t pt_t0_ticks = 0; /* clock pairs at START and STOP, for the tick rate on x86 */
static uint64_t pt_t0_ns = 0;
static uint64_t pt_t1_ticks = 0;
static uint64_t pt_t1_ns = 0;

static uint64_t ptRawNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Ticks per second of ptTicks. arm64 reports it; on x86 it is measured between START and STOP. */
static uint64_t ptTickHz(void) {
#if defined(__aarch64__)
    uint64_t f;
    __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(f));
    return f;
#elif defined(__x86_64__)
    if (pt_t1_ns - pt_t0_ns < 10000000ULL) { /* under 10 ms: measure again over 20 ms */
        uint64_t n0 = ptRawNs(), k0 = ptTicks();
        while (ptRawNs() - n0 < 20000000ULL);
        uint64_t n1 = ptRawNs(), k1 = ptTicks();
        return (uint64_t)((double)(k1 - k0) * 1e9 / (double)(n1 - n0));
    }
    return (uint64_t)((double)(pt_t1_ticks - pt_t0_ticks) * 1e9 / (double)(pt_t1_ns - pt_t0_ns));
#else
    return 1000000000ULL;
#endif
}

/* Main. Allocate (or reuse) one ring per thread that exists now and start recording.
 * shortcut: main resets and reallocates rings from a DEBUG command, which runs while the IO threads
 * wait at the phase barrier (they write only inside phases). Good enough for a debug tool; a
 * production version would hand the rings over at a phase boundary. */
static const char *ptStart(void) {
    if (pt_on) return "phase trace is already running";
    long long want = server.debug_phase_trace_events;
    if (want <= 0) return "phase trace is disabled: set debug-phase-trace-events first";
    uint64_t size = 1;
    while (size < (uint64_t)want) size <<= 1;
    int nrings = server.io_threads_num;
    if (nrings > IO_THREADS_MAX_NUM) nrings = IO_THREADS_MAX_NUM;
    if (size != pt_size) {
        for (int i = 0; i < pt_nrings; i++) {
            zfree(pt_rings[i].r);
            pt_rings[i].r = NULL;
        }
        pt_nrings = 0;
        pt_size = size;
    }
    for (int i = pt_nrings; i < nrings; i++) {
        pt_rings[i].r = zmalloc(size * sizeof(ptRec));
        pt_rings[i].mask = size - 1;
    }
    if (nrings > pt_nrings) pt_nrings = nrings;
    for (int i = 0; i < pt_nrings; i++) {
        memset(pt_rings[i].r, 0, size * sizeof(ptRec)); /* fault the pages in now, not while tracing */
        pt_rings[i].n = 0;
    }
    pt_t0_ns = ptRawNs();
    pt_t0_ticks = ptTicks();
    pt_t1_ns = pt_t0_ns;
    pt_t1_ticks = pt_t0_ticks;
    __atomic_store_n(&pt_on, 1, __ATOMIC_RELEASE);
    return NULL;
}

static void ptStop(void) {
    if (!pt_on) return;
    __atomic_store_n(&pt_on, 0, __ATOMIC_RELEASE);
    pt_t1_ns = ptRawNs();
    pt_t1_ticks = ptTicks();
}

static int ptWrite(FILE *f, const void *p, size_t n) {
    return fwrite(p, 1, n, f) == n;
}

/* Layout, little endian:
 *   header: char magic[8] "VKPTRC01"; u32 rec_size (32); u32 nrings; u64 tick_hz; u64 t0_ticks;
 *           u64 t1_ticks; u32 io_threads; u32 reserved
 *   per ring: u32 tid; u32 reserved; u64 written; u64 kept; kept x ptRec, oldest first
 * written > kept means the ring wrapped and the oldest written - kept records are gone. */
static const char *ptDump(const char *path, long long *records) {
    if (pt_on) return "stop the phase trace before DUMP";
    if (!pt_nrings) return "no phase trace to dump";
    FILE *f = fopen(path, "wb");
    if (!f) return "cannot open the dump file";
    int ok = 1;
    uint32_t rs = sizeof(ptRec), nr = (uint32_t)pt_nrings, io = (uint32_t)server.io_threads_num, z = 0;
    uint64_t hz = ptTickHz();
    ok &= ptWrite(f, "VKPTRC01", 8);
    ok &= ptWrite(f, &rs, 4) && ptWrite(f, &nr, 4) && ptWrite(f, &hz, 8);
    ok &= ptWrite(f, &pt_t0_ticks, 8) && ptWrite(f, &pt_t1_ticks, 8) && ptWrite(f, &io, 4) && ptWrite(f, &z, 4);
    *records = 0;
    for (int i = 0; ok && i < pt_nrings; i++) {
        ptRing *g = &pt_rings[i];
        uint32_t tid = (uint32_t)i;
        uint64_t kept = g->n < pt_size ? g->n : pt_size;
        uint64_t first = g->n - kept;
        ok &= ptWrite(f, &tid, 4) && ptWrite(f, &z, 4) && ptWrite(f, &g->n, 8) && ptWrite(f, &kept, 8);
        /* oldest first: from first to the end of the array, then from the start */
        uint64_t a = first & g->mask;
        uint64_t n1 = kept < pt_size - a ? kept : pt_size - a;
        ok &= ptWrite(f, &g->r[a], n1 * sizeof(ptRec));
        if (kept > n1) ok &= ptWrite(f, &g->r[0], (kept - n1) * sizeof(ptRec));
        *records += (long long)kept;
    }
    if (fclose(f) != 0) ok = 0;
    return ok ? NULL : "write to the dump file failed";
}

/* DEBUG PHASETRACE START|STOP|STATUS|DUMP <path> */
void phaseTraceCommand(client *c) {
    const char *sub = objectGetVal(c->argv[2]);
    const char *err = NULL;
    if (!strcasecmp(sub, "start") && c->argc == 3) {
        err = ptStart();
    } else if (!strcasecmp(sub, "stop") && c->argc == 3) {
        ptStop();
    } else if (!strcasecmp(sub, "status") && c->argc == 3) {
        uint64_t written = 0, wrapped = 0;
        for (int i = 0; i < pt_nrings; i++) {
            written += pt_rings[i].n;
            if (pt_rings[i].n > pt_size) wrapped++;
        }
        sds s = sdscatprintf(sdsempty(), "on:%d events_per_thread:%llu rings:%d written:%llu wrapped_rings:%llu", pt_on,
                             (unsigned long long)pt_size, pt_nrings, (unsigned long long)written,
                             (unsigned long long)wrapped);
        addReplyVerbatim(c, s, sdslen(s), "txt");
        sdsfree(s);
        return;
    } else if (!strcasecmp(sub, "dump") && c->argc == 4) {
        long long records = 0;
        err = ptDump(objectGetVal(c->argv[3]), &records);
        if (!err) {
            addReplyLongLong(c, records);
            return;
        }
    } else {
        addReplyError(c, "usage: DEBUG PHASETRACE START|STOP|STATUS|DUMP <path>");
        return;
    }
    if (err)
        addReplyError(c, err);
    else
        addReply(c, shared.ok);
}
