/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Ordering and publication stress for the lock-free queues and their consumer/producer helpers
 * (mpscConsumerHasItem, spscFreeSlots, spscBacklog, spscCommit). Each item is a pointer to a record the
 * producer fills with plain stores before it enqueues the pointer, so the consumer also checks that the
 * enqueue publishes the record (the slot load in mpscDequeueBatch must be an acquire). Small queues force
 * many wrap-arounds and the full / ticket-retry path. Items: 1e6 per test. No sleeps; producers yield. */

#include "generated_wrappers.hpp"

#include <atomic>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdlib.h>

extern "C" {
#include "queues.h"
#include "zmalloc.h"
}

#define ORD_TOTAL_ITEMS 1000000
#define ORD_PRODUCERS 4
#define ORD_QUEUE_SIZE 64

struct ordRecord {
    uint32_t producer;
    uint32_t seq;
    uint64_t check;
};

static uint64_t ordCheck(uint32_t producer, uint32_t seq) {
    uint64_t x = ((uint64_t)producer << 32) | seq;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return x | 1;
}

/* ============== MPSC: N producers, one consumer ============== */

struct ordMpscArg {
    mpscQueue *q;
    ordRecord *recs;
    uint32_t producer;
    uint32_t count;
    std::atomic<int> *start;
    size_t full_retries;
};

static void *ordMpscProducer(void *arg) {
    ordMpscArg *a = (ordMpscArg *)arg;
    while (a->start->load(std::memory_order_acquire) == 0) sched_yield();
    for (uint32_t i = 0; i < a->count; i++) {
        ordRecord *r = &a->recs[i];
        r->producer = a->producer;
        r->seq = i;
        r->check = ordCheck(a->producer, i);
        mpscTicket ticket = {0, false};
        while (!mpscEnqueue(a->q, r, &ticket)) {
            a->full_retries++;
            sched_yield();
        }
    }
    return NULL;
}

TEST(QueueOrderTest, MpscEveryItemOnceInOrderPerProducer) {
    mpscQueue q;
    mpscInit(&q, ORD_QUEUE_SIZE);
    const uint32_t per = ORD_TOTAL_ITEMS / ORD_PRODUCERS;
    ordRecord *recs = (ordRecord *)zcalloc((size_t)per * ORD_PRODUCERS * sizeof(ordRecord));
    ASSERT_NE(recs, nullptr);
    std::atomic<int> start(0);
    pthread_t th[ORD_PRODUCERS];
    ordMpscArg args[ORD_PRODUCERS];
    for (uint32_t p = 0; p < ORD_PRODUCERS; p++) {
        args[p].q = &q;
        args[p].recs = recs + (size_t)p * per;
        args[p].producer = p;
        args[p].count = per;
        args[p].start = &start;
        args[p].full_retries = 0;
        ASSERT_EQ(pthread_create(&th[p], NULL, ordMpscProducer, &args[p]), 0);
    }
    start.store(1, std::memory_order_release);

    uint32_t next[ORD_PRODUCERS] = {0};
    size_t total = 0, bad_check = 0, bad_order = 0, bad_producer = 0, has_item_then_empty = 0;
    void *jobs[16];
    const size_t expected = (size_t)per * ORD_PRODUCERS;
    while (total < expected) {
        bool has = mpscConsumerHasItem(&q);
        size_t n = mpscDequeueBatch(&q, jobs, 16);
        if (n == 0) {
            if (has) has_item_then_empty++; /* allowed: a reserved slot whose data is still landing */
            sched_yield();
            continue;
        }
        for (size_t i = 0; i < n; i++) {
            ordRecord *r = (ordRecord *)jobs[i];
            if (r->producer >= ORD_PRODUCERS) {
                bad_producer++;
                continue;
            }
            if (r->check != ordCheck(r->producer, r->seq)) bad_check++;
            if (r->seq != next[r->producer]) bad_order++;
            next[r->producer] = r->seq + 1;
        }
        total += n;
    }
    for (uint32_t p = 0; p < ORD_PRODUCERS; p++) pthread_join(th[p], NULL);

    size_t retries = 0;
    for (uint32_t p = 0; p < ORD_PRODUCERS; p++) {
        EXPECT_EQ(next[p], per) << "producer " << p;
        retries += args[p].full_retries;
    }
    EXPECT_EQ(total, expected);
    EXPECT_EQ(bad_producer, 0u);
    EXPECT_EQ(bad_check, 0u) << "a record was dequeued before its producer's stores were visible";
    EXPECT_EQ(bad_order, 0u) << "items of one producer came out of order or twice";
    /* Drained: no item left, and the helper agrees. */
    EXPECT_FALSE(mpscConsumerHasItem(&q));
    EXPECT_EQ(mpscDequeueBatch(&q, jobs, 16), 0u);
    /* The run used the full-queue / ticket path, otherwise the small queue proves less. */
    EXPECT_GT(retries, 0u);
    (void)has_item_then_empty;
    zfree(recs);
    mpscFree(&q);
}

/* ============== SPSC: helpers under a concurrent consumer ============== */

struct ordSpscArg {
    spscQueue *q;
    ordRecord *recs;
    uint32_t count;
    std::atomic<int> *start;
    size_t bad_free_slots;
    size_t commits_true;
};

static void *ordSpscProducer(void *arg) {
    ordSpscArg *a = (ordSpscArg *)arg;
    while (a->start->load(std::memory_order_acquire) == 0) sched_yield();
    uint32_t i = 0;
    while (i < a->count) {
        size_t free_slots = spscFreeSlots(a->q);
        if (free_slots > ORD_QUEUE_SIZE) a->bad_free_slots++;
        if (free_slots == 0) {
            sched_yield();
            continue;
        }
        /* Batch up to free_slots items, then publish them with one commit. */
        size_t batch = free_slots < 7 ? free_slots : 7;
        size_t pushed = 0;
        while (pushed < batch && i < a->count) {
            ordRecord *r = &a->recs[i];
            r->producer = 0;
            r->seq = i;
            r->check = ordCheck(0, i);
            spscEnqueue(a->q, r, false);
            pushed++;
            i++;
        }
        if (spscCommit(a->q)) a->commits_true++;
        if (spscCommit(a->q)) a->bad_free_slots++; /* nothing pending: must return false */
    }
    return NULL;
}

TEST(QueueOrderTest, SpscBatchedCommitInOrderAndHelpersBounded) {
    spscQueue q;
    spscInit(&q, ORD_QUEUE_SIZE);
    const uint32_t count = ORD_TOTAL_ITEMS;
    ordRecord *recs = (ordRecord *)zcalloc((size_t)count * sizeof(ordRecord));
    ASSERT_NE(recs, nullptr);
    std::atomic<int> start(0);
    ordSpscArg a = {&q, recs, count, &start, 0, 0};
    pthread_t th;
    ASSERT_EQ(pthread_create(&th, NULL, ordSpscProducer, &a), 0);
    start.store(1, std::memory_order_release);

    uint32_t next = 0;
    size_t bad_check = 0, bad_order = 0, bad_backlog = 0;
    void *jobs[16];
    while (next < count) {
        size_t backlog = spscBacklog(&q);
        if (backlog > ORD_QUEUE_SIZE) bad_backlog++;
        size_t n = spscDequeueBatch(&q, jobs, 16);
        if (n == 0) {
            sched_yield();
            continue;
        }
        if (n > backlog && backlog > 0) {
            /* The backlog can only grow between the two calls (single consumer), so n > backlog is fine. */
        }
        for (size_t i = 0; i < n; i++) {
            ordRecord *r = (ordRecord *)jobs[i];
            if (r->check != ordCheck(0, r->seq)) bad_check++;
            if (r->seq != next) bad_order++;
            next = r->seq + 1;
        }
    }
    pthread_join(th, NULL);
    EXPECT_EQ(next, count);
    EXPECT_EQ(bad_check, 0u);
    EXPECT_EQ(bad_order, 0u);
    EXPECT_EQ(bad_backlog, 0u);
    EXPECT_EQ(a.bad_free_slots, 0u);
    EXPECT_GT(a.commits_true, 0u);
    EXPECT_EQ(spscBacklog(&q), 0u);
    EXPECT_EQ(spscFreeSlots(&q), (size_t)ORD_QUEUE_SIZE);
    EXPECT_FALSE(spscCommit(&q));
    zfree(recs);
    spscFree(&q);
}
