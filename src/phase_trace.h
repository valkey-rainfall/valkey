/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Phase trace of the staged loop (debug only, off by default).
 *
 * Each thread (main is 0, IO thread N is N) owns one fixed-size ring of records. A record is a
 * timestamp at one edge of one block of the loop (begin or end of poll, a lane phase, a barrier
 * wait, ...) plus a few counts. Only the owning thread writes its ring, so there are no locks, and
 * the rings are allocated before tracing starts, so there is no allocation while it runs. When
 * tracing is off every trace point costs one load and one not-taken branch.
 *
 * Enabled by the hidden config debug-phase-trace-events (ring size per thread, 0 = disabled) and
 * driven by DEBUG PHASETRACE START|STOP|STATUS|DUMP <path>. The dump is binary; see
 * phaseTraceDump for the layout. */

#ifndef PHASE_TRACE_H
#define PHASE_TRACE_H

#include <stdint.h>
#include <time.h>

/* Blocks. Main: ITER .. MAINT, OPEN (a point), LANE (main's own share of a phase), WAIT (main waits
 * for every worker). Workers: AWAIT (wait for the next phase), DRAIN (job inboxes), LANE, DONE (a
 * point, right before the worker reports the phase done). */
enum {
    PT_ITER = 1,
    PT_BEFORESLEEP,
    PT_POLL,
    PT_AFTERSLEEP,
    PT_READSTAGE,
    PT_EXEC,
    PT_EVENTS,
    PT_AOF,
    PT_REPLYSTAGE,
    PT_FINISH,
    PT_CRONS,
    PT_MAINT,
    PT_OPEN,
    PT_LANE,
    PT_WAIT,
    PT_AWAIT,
    PT_DRAIN,
    PT_DONE,
};

#define PT_BEGIN 0
#define PT_END 1
#define PT_POINT 2

/* 32 bytes. seq: barrier sequence of the phase (OPEN, LANE, WAIT, AWAIT, DONE) or main's iteration
 * number (ITER). Counts c1..c4 depend on the block; see fastpathTraceLane and the poll trace. */
typedef struct ptRec {
    uint64_t ts;
    uint32_t seq;
    uint32_t c1, c2, c3, c4;
    uint8_t block, edge, phase, tid;
} ptRec;

typedef struct ptRing {
    ptRec *r;
    uint64_t mask;
    uint64_t n; /* records written since start; the ring keeps the last mask + 1 */
} __attribute__((aligned(64))) ptRing;

extern int pt_on;
extern ptRing pt_rings[];

static inline uint64_t ptTicks(void) {
#if defined(__x86_64__)
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#elif defined(__aarch64__)
    uint64_t v;
    __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

/* Only called when pt_on is set. */
static inline void ptRecord(int tid, int block, int edge, int phase, uint32_t seq, uint32_t c1, uint32_t c2, uint32_t c3, uint32_t c4) {
    ptRing *g = &pt_rings[tid];
    if (!g->r) return; /* a thread started after START has no ring */
    ptRec *x = &g->r[g->n & g->mask];
    x->ts = ptTicks();
    x->seq = seq;
    x->c1 = c1;
    x->c2 = c2;
    x->c3 = c3;
    x->c4 = c4;
    x->block = (uint8_t)block;
    x->edge = (uint8_t)edge;
    x->phase = (uint8_t)phase;
    x->tid = (uint8_t)tid;
    g->n++;
}

#define PT_ON() __builtin_expect(__atomic_load_n(&pt_on, __ATOMIC_RELAXED), 0)
#define PT(tid, block, edge, phase, seq, c1, c2, c3, c4)                                                 \
    do {                                                                                                 \
        if (PT_ON()) ptRecord((tid), (block), (edge), (phase), (uint32_t)(seq), (c1), (c2), (c3), (c4)); \
    } while (0)
#define PTB(tid, block) PT(tid, block, PT_BEGIN, 0, 0, 0, 0, 0, 0)
#define PTE(tid, block) PT(tid, block, PT_END, 0, 0, 0, 0, 0, 0)
/* A lane edge with its counts (fastpath.h). */
#define PTLANE(tid, phase, edge, seq)                                            \
    do {                                                                         \
        if (PT_ON()) fastpathTraceLane((tid), (phase), (edge), (uint32_t)(seq)); \
    } while (0)

struct client;
void phaseTraceCommand(struct client *c);

#endif
