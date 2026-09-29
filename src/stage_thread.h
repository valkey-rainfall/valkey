/* Staged event loop: thread identity types, kept dependency-free so any header describing a
 * cross-thread structure (a batch, a control) can name a holder without pulling in the transport.
 * The two socket-owning domain values equal the control plane's CC_OWNER_MAIN/CC_OWNER_IO; stage.c
 * static-asserts that correspondence rather than including fastpath.h here (which would be circular,
 * since fastpath.h includes this to give cmdBatch a holder). */

#ifndef STAGE_THREAD_H
#define STAGE_THREAD_H

#include <stdint.h>

typedef enum {
    STAGE_DOMAIN_MAIN = 0, /* the main event loop; == CC_OWNER_MAIN */
    STAGE_DOMAIN_IO = 1,   /* an IO thread that owns connections end to end; == CC_OWNER_IO */
    STAGE_DOMAIN_BIO = 2,  /* a background thread; owns no connection */
} stageDomain;

/* The identity a thread wears, set once at thread start. tid is the IO thread id for
 * STAGE_DOMAIN_IO (0 for main), matching ClientControl.owner_tid. */
typedef struct stageThreadId {
    stageDomain domain;
    uint16_t tid;
} stageThreadId;

extern __thread stageThreadId stageSelf;

/* Set this thread's identity. Called once from each thread's start (main and IO worker). */
static inline void stageSetSelf(stageDomain domain, uint16_t tid) {
    stageSelf.domain = domain;
    stageSelf.tid = tid;
}

#endif /* STAGE_THREAD_H */
