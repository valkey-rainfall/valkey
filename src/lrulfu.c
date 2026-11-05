#include "lrulfu.h"
#include <time.h>

static const uint32_t LRULFU_MASK = (1 << LRULFU_BITS) - 1;

/**************** LRU ****************/
/* LRU uses a 24 bit timestamp of the last access time (in seconds)
 * The LRU value needs to be "touched" within 194 days, or the value will wrap,
 * and the last access time will appear to be recent.
 */

/* The LRU_CLOCK_RESOLUTION is used to support an older ruby program which tests
 * the LRU behavior.  This should be set to 1 if building Valkey to support this
 * ruby test.  Otherwise, the default of 1000 is expected. */
static const uint32_t LRU_CLOCK_RESOLUTION = 1000; /* LRU clock resolution in ms */

int lfu_config_log_factor;
int lfu_config_decay_time;

/* Written by main, read also by other threads (IO threads, lanes, a forkless save), so accessed atomically. */
static uint32_t lru_clock; // Normally seconds (unless LRU_CLOCK_RESOLUTION altered)
static uint16_t lfu_clock_minutes;
static bool is_using_lfu_policy;


// Current time in seconds (24 least significant bits).  Designed to roll over.
static uint32_t LRUGetClockTime(void) {
    return __atomic_load_n(&lru_clock, __ATOMIC_RELAXED);
}


uint32_t lru_import(uint32_t idle_secs) {
    uint32_t now = LRUGetClockTime();
    if (LRU_CLOCK_RESOLUTION != 1000) {
        idle_secs = (uint32_t)((long)idle_secs * 1000 / LRU_CLOCK_RESOLUTION);
    }
    idle_secs = idle_secs & LRULFU_MASK;
    // Underflow is ok/expected
    return (now - idle_secs) & LRULFU_MASK;
}


uint32_t lru_getIdleSecs(uint32_t lru) {
    // Underflow is ok/expected
    uint32_t seconds = (LRUGetClockTime() - lru) & LRULFU_MASK;
    if (LRU_CLOCK_RESOLUTION != 1000) {
        seconds = (uint32_t)((long)seconds * LRU_CLOCK_RESOLUTION / 1000);
    }
    return seconds;
}


/**************** LFU ****************/
/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.
 *
 * We split the 24 bits into two fields:
 *
 *            16 bits           8 bits
 *     +-----------------------+--------+
 *     + Last access (minutes) | LOG_C  |
 *     +-----------------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "access time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter decays every minute by default (depends on lfu-decay-time).
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at LFU_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of LFU_INIT_VAL
 * when incrementing the key, so that keys starting at LFU_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access. (The chance depends on counter and lfu-log-factor.)
 *
 * During decrement, the value of the logarithmic counter is decremented by
 * one when lfu-decay-time minutes elapsed.
 * --------------------------------------------------------------------------*/

#define LFU_INIT_VAL 5


// Current time in minutes (16 least significant bits).  Designed to roll over.
static uint16_t LFUGetTimeInMinutes(void) {
    return __atomic_load_n(&lfu_clock_minutes, __ATOMIC_RELAXED);
}


uint32_t lfu_import(uint8_t freq) {
    return ((uint32_t)LFUGetTimeInMinutes() << 8) | freq;
}


/* Update an LFU to consider decay, but doesn't add a "touch" */
static uint32_t LFUDecay(uint32_t lfu) {
    uint16_t now = LFUGetTimeInMinutes();
    uint16_t prev_time = (uint16_t)(lfu >> 8);
    uint8_t freq = (uint8_t)lfu;
    uint16_t elapsed = now - prev_time; // Wrap-around expected/valid
    uint16_t num_periods = lfu_config_decay_time ? elapsed / lfu_config_decay_time : 0;
    freq = (num_periods > freq) ? 0 : freq - num_periods;
    /* Advance the time only by the periods consumed, so that reading the counter more often than
     * lfu-decay-time does not hold back its decay. */
    uint16_t decayed_time = (uint16_t)(prev_time + (uint32_t)num_periods * lfu_config_decay_time);
    return ((uint32_t)decayed_time << 8) | freq;
}


/* Increment the freq counter with logarithmic probability.
 * Values closer to 0 are more likely to increment.
 * Values closer to 255 are logarithmically less likely to increment. */
static uint8_t LFULogIncr(uint8_t freq, double r) {
    if (freq == 255) return freq;
    double baseval = (int)freq - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0 / (baseval * lfu_config_log_factor + 1);
    if (r < p) freq++;
    return freq;
}


static uint32_t lfu_touchWithRandom(uint32_t lfu, double r) {
    uint8_t freq = (uint8_t)LFUDecay(lfu);
    freq = LFULogIncr(freq, r);
    return lfu_import(freq);
}

/* xorshift64*, one generator per thread: lane threads touch keys too, and rand() is process-wide. */
static _Thread_local uint64_t lrulfu_rng = 0;

static double lrulfu_random(void) {
    uint64_t x = lrulfu_rng;
    if (x == 0) {
        /* splitmix64 of the thread's state address and the time: distinct per thread. */
        x = (uint64_t)(uintptr_t)&lrulfu_rng ^ ((uint64_t)time(NULL) << 32);
        x += UINT64_C(0x9E3779B97F4A7C15);
        x = (x ^ (x >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
        x = (x ^ (x >> 27)) * UINT64_C(0x94D049BB133111EB);
        x = (x ^ (x >> 31)) | 1;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    lrulfu_rng = x;
    return (double)((x * UINT64_C(0x2545F4914F6CDD1D)) >> 11) / (double)(UINT64_C(1) << 53);
}

uint32_t lfu_touch(uint32_t lfu) {
    return lfu_touchWithRandom(lfu, lrulfu_random());
}


uint32_t lfu_getFrequency(uint32_t lfu, uint8_t *freq) {
    lfu = LFUDecay(lfu);
    *freq = (uint8_t)lfu;
    return lfu;
}


/**************** Generic API ****************/

void lrulfu_updateClockAndPolicy(long long mstime, bool is_policy_lfu) {
    __atomic_store_n(&lru_clock, (uint32_t)((mstime / LRU_CLOCK_RESOLUTION) & LRULFU_MASK), __ATOMIC_RELAXED);
    __atomic_store_n(&lfu_clock_minutes, (uint16_t)(mstime / 60000), __ATOMIC_RELAXED);
    __atomic_store_n(&is_using_lfu_policy, is_policy_lfu, __ATOMIC_RELAXED);
}


bool lrulfu_isUsingLFU(void) {
    return __atomic_load_n(&is_using_lfu_policy, __ATOMIC_RELAXED);
}


uint32_t lrulfu_init(void) {
    if (lrulfu_isUsingLFU()) {
        return lfu_import(LFU_INIT_VAL);
    } else {
        return lru_import(0);
    }
}


uint32_t lrulfu_getIdleness(uint32_t lrulfu, uint32_t *idleness) {
    if (lrulfu_isUsingLFU()) {
        uint8_t freq;
        lrulfu = lfu_getFrequency(lrulfu, &freq);
        *idleness = UINT8_MAX - freq;
    } else {
        *idleness = lru_getIdleSecs(lrulfu);
    }
    return lrulfu;
}


uint32_t lrulfu_touch(uint32_t lrulfu) {
    if (lrulfu_isUsingLFU()) {
        return lfu_touch(lrulfu);
    } else {
        return lru_import(0);
    }
}


uint32_t lrulfu_touchWithRandom(uint32_t lrulfu, double r) {
    if (lrulfu_isUsingLFU()) {
        return lfu_touchWithRandom(lrulfu, r);
    } else {
        return lru_import(0);
    }
}


bool lrulfu_touchIsConstant(uint32_t *value) {
    if (lrulfu_isUsingLFU()) return false;
    *value = lru_import(0);
    return true;
}
