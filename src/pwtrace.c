/* pwtrace.c -- see pwtrace.h. DEBUG-ONLY, feature-1 crash instrumentation. */
#include "server.h"
#include "io_threads.h"
#include "pwtrace.h"

#include <stdatomic.h>

int pwtrace_populated = 0;
struct client *pwtrace_last_client = NULL;

static _Atomic uint64_t pwtrace_seq = 0;
static pwTraceEntry pwtrace_global_ring[PWTRACE_GLOBAL_RING_SIZE];
static _Atomic uint32_t pwtrace_global_next = 0;

static const char *pwTraceSiteName(pwTraceSite site) {
    switch (site) {
    case PWTRACE_PUT_QUEUE: return "PUT_QUEUE";
    case PWTRACE_TRYSEND_UNLINK: return "TRYSEND_UNLINK";
    case PWTRACE_HANDLE_PW_UNLINK: return "HANDLE_PW_UNLINK";
    case PWTRACE_HANDLE_PW_HEAL_UNLINK: return "HANDLE_PW_HEAL_UNLINK";
    case PWTRACE_UNLINK_CLIENT: return "UNLINK_CLIENT";
    case PWTRACE_BEFORENEXT_F8_UNLINK: return "BEFORENEXT_F8_UNLINK";
    case PWTRACE_B13_ARM_UNLINK: return "B13_ARM_UNLINK";
    case PWTRACE_MODULE_LINK: return "MODULE_LINK";
    default: return "NONE";
    }
}

void pwTraceRecord(client *c, pwTraceSite site, int is_link) {
    pwtrace_populated = 1;

    pwTraceEntry e;
    e.seq = atomic_fetch_add_explicit(&pwtrace_seq, 1, memory_order_relaxed);
    e.client_id = c ? c->id : 0;
    e.site = site;
    e.is_main = inMainThread();
    e.tid = (uint8_t)getCurTid();
    e.pending_write_before = c ? c->flag.pending_write : 0;
    e.io_write_state = c ? c->io_write_state : 0;
    e.io_read_state = c ? c->io_read_state : 0;
    e.is_link = (uint8_t)is_link;

    /* Global ring: plain non-atomic slot write behind an atomic index
     * increment. A dropped/torn entry under real concurrency is acceptable
     * for a debug trace (we care about the tail leading into a crash, not
     * every single entry). */
    uint32_t idx = atomic_fetch_add_explicit(&pwtrace_global_next, 1, memory_order_relaxed) %
                   PWTRACE_GLOBAL_RING_SIZE;
    pwtrace_global_ring[idx] = e;

    /* Per-client ring: only ever touched by main thread today (every
     * documented call site of putClientInPendingWriteQueue / the unlink
     * sites runs on main) -- if that assumption is wrong, this plain write
     * is itself part of what we're trying to catch, not a new bug to hide. */
    if (c) {
        c->pwtrace_ring[c->pwtrace_next % PWTRACE_RING_SIZE] = e;
        c->pwtrace_next++;
    }
}

static void pwTracePrintEntry(const char *tag, const pwTraceEntry *e) {
    serverLog(LL_WARNING,
              "PWTRACE %s seq=%llu client_id=%llu site=%s %s tid=%u is_main=%u "
              "pending_write_before=%u io_write_state=%u io_read_state=%u",
              tag, (unsigned long long)e->seq, (unsigned long long)e->client_id,
              pwTraceSiteName(e->site), e->is_link ? "LINK" : "UNLINK", e->tid, e->is_main,
              e->pending_write_before, e->io_write_state, e->io_read_state);
}

void pwTraceDump(client *c) {
    if (!pwtrace_populated) return;

    serverLog(LL_WARNING, "------ PWTRACE DUMP START ------");
    serverLog(LL_WARNING,
              "PWTRACE format: seq client_id site LINK|UNLINK tid is_main "
              "pending_write_before io_write_state io_read_state");

    serverLog(LL_WARNING, "PWTRACE global ring (up to %d most recent, any client):",
              PWTRACE_GLOBAL_RING_SIZE);
    uint32_t total = atomic_load_explicit(&pwtrace_global_next, memory_order_relaxed);
    uint32_t count = total < PWTRACE_GLOBAL_RING_SIZE ? total : PWTRACE_GLOBAL_RING_SIZE;
    uint32_t start = total < PWTRACE_GLOBAL_RING_SIZE ? 0 : total % PWTRACE_GLOBAL_RING_SIZE;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t idx = (start + i) % PWTRACE_GLOBAL_RING_SIZE;
        pwTracePrintEntry("GLOBAL", &pwtrace_global_ring[idx]);
    }

    if (c) {
        serverLog(LL_WARNING, "PWTRACE per-client ring for client_id=%llu (up to %d most recent):",
                  (unsigned long long)c->id, PWTRACE_RING_SIZE);
        uint32_t ctotal = c->pwtrace_next;
        uint32_t ccount = ctotal < PWTRACE_RING_SIZE ? ctotal : PWTRACE_RING_SIZE;
        uint32_t cstart = ctotal < PWTRACE_RING_SIZE ? 0 : ctotal % PWTRACE_RING_SIZE;
        for (uint32_t i = 0; i < ccount; i++) {
            uint32_t idx = (cstart + i) % PWTRACE_RING_SIZE;
            pwTracePrintEntry("CLIENT", &c->pwtrace_ring[idx]);
        }
    }
    serverLog(LL_WARNING, "------ PWTRACE DUMP END ------");
}
