/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef REPL_APPLY_H
#define REPL_APPLY_H

/* Parallel apply of the replication stream on a replica.
 *
 * With keyspace-shard-bits > 0 each db is split into shards by key slot. While main parses the
 * primary's stream, eligible writes are held back and bucketed by shard into one lane per staged
 * thread (main plus the enrolled IO threads). A lane holds whole shards, so every key's writes stay
 * in stream order and no two threads touch the same hashtable. Before any other command runs, and
 * before the parse loop returns, main opens an APPLY phase and every thread applies its lane.
 * Commands that are not eligible, and every command while a feature that observes individual
 * writes is active, run serially on main in stream order. */

#include <stdint.h>

struct client;

/* Main, parse loop of the primary client: take the parsed command into a lane. Returns 1 when
 * taken; the caller then finishes the command without executing it. */
int replApplyDefer(struct client *c);
/* Main: apply every held command before anything else observes the keyspace. */
void replApplyFlush(struct client *c);
/* Main, before sending the primary's read in the read phase: whether the IO thread sorts the commands it
 * parses into write lanes. Fixes the lanes until the plan runs. */
int replApplyPlanBegin(void);
/* IO thread, a read of the primary: forget the last plan; after parsing, plan what was parsed. */
void replApplyPlanReset(void);
void replApplyPlanRead(struct client *c);
void replApplyPlanReadTime(long long us);
/* Main, a completed read of the primary: run its plan, if any, before the usual processing takes the rest.
 * Returns C_ERR when the client is gone. */
int replApplyRunPlan(struct client *c);
/* Main: the primary's writes may be held for parallel apply, so main need not prefetch them. */
int replApplyMayDefer(void);
/* Main: commands held and not yet applied. */
int replApplyPending(void);
/* Staged thread tid (0 = main): apply its lane during an APPLY phase. */
void replApplyRunLane(int tid);

/* RDB load, prototype only: the loader's client c carries a parsed inline command. Returns 1 when it
 * is a plain SET taken into a lane (the lane owns c's argv). */
int replApplyLoadDefer(struct client *c);
/* RDB load: apply the held SETs now (before another inline command, before events, at the end). */
void replApplyLoadFlush(void);
/* RDB load ends: ok = 0 drops the held SETs unapplied (the load failed). */
void replApplyLoadEnd(int ok);
/* RDB load, debug check: a held SET is for key in db. */
struct serverDb;
int replApplyLoadHeld(struct serverDb *db, char *key);
/* Bits of the hidden loading-inline-apply-debug setting (prototype only). */
#define LOAD_APPLY_CHECK_RECORDS 1 /* assert that no held SET is for a record's key */
#define LOAD_APPLY_FLUSH_RECORDS 2 /* flush before every record, for bisecting */
#define LOAD_APPLY_MUT_NO_CMD_FLUSH 4 /* test mutation: no flush before another inline command */
#define LOAD_APPLY_MUT_NO_EOF_FLUSH 8 /* test mutation: no flush at the end of the load */
#define LOAD_APPLY_MUT_PAST_SWAPDB 16 /* test mutation: no flush before an inline SWAPDB */

/* Set on a thread while it applies a lane. Paths gated off by replApplyDefer must not run. */
extern _Thread_local int repl_apply_in_lane;
/* Set on a thread while it runs a planned read's lane commands. Their keys are not watched, so the
 * WATCH bookkeeping, which only main may touch, is skipped. */
extern _Thread_local int repl_apply_plan_lane;
/* The key a lane is applying and its shard, so the db paths do not rehash it. */
extern _Thread_local const char *repl_apply_key;
extern _Thread_local int repl_apply_shard;

#endif /* REPL_APPLY_H */
