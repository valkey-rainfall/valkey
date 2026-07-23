/*
 * jitter_probe.c - standalone CI runner environment characterization probe.
 *
 * Measures scheduler and clock behavior of the host it runs on, independent
 * of any application workload. Intended to compare hosted CI runner VMs
 * (e.g. GitHub Actions) against dedicated cloud instances.
 *
 * Measurements:
 *   - sleep-wakeup jitter: nanosleep(REQ) overshoot distribution
 *   - busy-loop preemption gaps: gaps between consecutive clock reads in a
 *     tight loop (scheduler steals / preemption / vCPU descheduling)
 *   - fork+exit+reap latency, with a configurable touched heap (mimics the
 *     page-table copy cost a bgsave-style fork pays)
 *   - clock drift: CLOCK_REALTIME and CLOCK_MONOTONIC_RAW vs CLOCK_MONOTONIC
 *   - clock resolution and minimum observable clock step
 *   - /proc/stat steal ticks delta across the run
 *
 * Optional --load N spawns N busy-spinning child workers for the duration of
 * the measurement phases, to simulate a parallel test suite sharing the box.
 *
 * Output: a single JSON object on stdout. All latency stats in microseconds.
 *
 * Build: gcc -O2 -Wall -Wextra -o jitter_probe jitter_probe.c
 * Zero dependencies beyond libc + POSIX.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* small helpers                                                      */
/* ------------------------------------------------------------------ */

static uint64_t now_ns(clockid_t clk) {
    struct timespec ts;
    clock_gettime(clk, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* percentile from a sorted array, nearest-rank-ish */
static double pctl_us(const uint64_t *sorted, size_t n, double p) {
    if (n == 0) return 0.0;
    size_t idx = (size_t)(p / 100.0 * (double)(n - 1) + 0.5);
    if (idx >= n) idx = n - 1;
    return (double)sorted[idx] / 1000.0;
}

static double mean_us(const uint64_t *v, size_t n) {
    if (n == 0) return 0.0;
    double s = 0.0;
    for (size_t i = 0; i < n; i++) s += (double)v[i];
    return s / (double)n / 1000.0;
}

static size_t count_over(const uint64_t *v, size_t n, uint64_t thresh_ns) {
    size_t c = 0;
    for (size_t i = 0; i < n; i++)
        if (v[i] > thresh_ns) c++;
    return c;
}

/* read a small file, trim trailing whitespace/newlines; "" on failure */
static void read_file_trim(const char *path, char *buf, size_t bufsz) {
    buf[0] = '\0';
    FILE *f = fopen(path, "r");
    if (!f) return;
    size_t n = fread(buf, 1, bufsz - 1, f);
    fclose(f);
    buf[n] = '\0';
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r' ||
                     buf[n - 1] == ' ' || buf[n - 1] == '\t'))
        buf[--n] = '\0';
}

/* minimal JSON string escaping into out */
static void json_escape(const char *in, char *out, size_t outsz) {
    size_t o = 0;
    for (size_t i = 0; in[i] && o + 2 < outsz; i++) {
        unsigned char c = (unsigned char)in[i];
        if (c == '"' || c == '\\') {
            out[o++] = '\\';
            out[o++] = (char)c;
        } else if (c == '\n' || c == '\r' || c == '\t') {
            out[o++] = ' ';
        } else if (c < 0x20) {
            /* drop other control chars */
        } else {
            out[o++] = (char)c;
        }
    }
    out[o] = '\0';
}

/* first "model name : ..." (x86) or "CPU part"/"Processor" fallback (arm) */
static void cpu_model(char *buf, size_t bufsz) {
    buf[0] = '\0';
    FILE *f = fopen("/proc/cpuinfo", "r");
    if (!f) return;
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        if (!strncmp(line, "model name", 10) ||
            !strncmp(line, "Processor", 9) ||
            !strncmp(line, "CPU part", 8)) {
            char *colon = strchr(line, ':');
            if (colon) {
                colon++;
                while (*colon == ' ' || *colon == '\t') colon++;
                snprintf(buf, bufsz, "%s", colon);
                size_t n = strlen(buf);
                while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r'))
                    buf[--n] = '\0';
                if (!strncmp(line, "model name", 10)) break; /* best match */
            }
        }
    }
    fclose(f);
}

/* /proc/stat "cpu ..." line: returns steal (field 8) and sum of all fields */
static int read_stat_ticks(uint64_t *steal, uint64_t *total) {
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return -1;
    char line[1024];
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return -1;
    }
    fclose(f);
    uint64_t v[10] = {0};
    int n = sscanf(line, "cpu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
                   &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7],
                   &v[8], &v[9]);
    if (n < 8) return -1;
    *steal = v[7];
    *total = 0;
    for (int i = 0; i < n; i++) *total += v[i];
    return 0;
}

/* ------------------------------------------------------------------ */
/* load workers                                                       */
/* ------------------------------------------------------------------ */

static pid_t *g_workers = NULL;
static int g_nworkers = 0;

static void spawn_load_workers(int n) {
    if (n <= 0) return;
    g_workers = calloc((size_t)n, sizeof(pid_t));
    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        if (pid == 0) {
            /* busy spin with a little memory churn until killed */
            volatile uint64_t x = 0;
            size_t bufsz = 4u << 20; /* 4MB working set per worker */
            char *buf = malloc(bufsz);
            if (buf) memset(buf, 1, bufsz);
            size_t j = 0;
            for (;;) {
                x += j;
                if (buf) buf[j % bufsz] = (char)x;
                j++;
            }
        }
        g_workers[i] = pid;
        g_nworkers++;
    }
}

static void kill_load_workers(void) {
    for (int i = 0; i < g_nworkers; i++) {
        if (g_workers[i] > 0) {
            kill(g_workers[i], SIGKILL);
            waitpid(g_workers[i], NULL, 0);
        }
    }
}

/* ------------------------------------------------------------------ */
/* measurement phases                                                 */
/* ------------------------------------------------------------------ */

/* Phase 1: nanosleep overshoot. Requested req_us, iters samples. */
static size_t measure_sleep(uint64_t *out, size_t iters, uint64_t req_us) {
    struct timespec req = {.tv_sec = (time_t)(req_us / 1000000),
                           .tv_nsec = (long)((req_us % 1000000) * 1000)};
    for (size_t i = 0; i < iters; i++) {
        uint64_t t0 = now_ns(CLOCK_MONOTONIC);
        nanosleep(&req, NULL);
        uint64_t t1 = now_ns(CLOCK_MONOTONIC);
        uint64_t elapsed = t1 - t0;
        uint64_t req_ns = req_us * 1000ull;
        out[i] = elapsed > req_ns ? elapsed - req_ns : 0;
    }
    return iters;
}

/* Phase 2: busy loop for dur_s seconds; record clock-read gaps > thresh_ns.
 * Returns number of gaps recorded; iterations/max via out-params. */
static size_t measure_busy_gaps(uint64_t *gaps, size_t gaps_cap,
                                double dur_s, uint64_t thresh_ns,
                                uint64_t *iterations, uint64_t *max_gap,
                                uint64_t *lost_ns) {
    size_t ngaps = 0;
    uint64_t iters = 0, maxg = 0, lost = 0;
    uint64_t start = now_ns(CLOCK_MONOTONIC);
    uint64_t end = start + (uint64_t)(dur_s * 1e9);
    uint64_t prev = start;
    for (;;) {
        uint64_t t = now_ns(CLOCK_MONOTONIC);
        uint64_t gap = t - prev;
        prev = t;
        iters++;
        if (gap > thresh_ns) {
            lost += gap;
            if (gap > maxg) maxg = gap;
            if (ngaps < gaps_cap) gaps[ngaps++] = gap;
        }
        if (t >= end) break;
    }
    *iterations = iters;
    *max_gap = maxg;
    *lost_ns = lost;
    return ngaps;
}

/* Phase 3: fork latency with touched heap. fork_lat = fork() return time in
 * parent; total_lat = fork + child exit + waitpid reap. */
static void measure_fork(uint64_t *fork_lat, uint64_t *total_lat,
                         size_t iters, size_t heap_mb) {
    char *heap = NULL;
    size_t heap_sz = heap_mb << 20;
    if (heap_sz > 0) {
        heap = malloc(heap_sz);
        if (heap) memset(heap, 0xAB, heap_sz); /* fault every page in */
    }
    for (size_t i = 0; i < iters; i++) {
        uint64_t t0 = now_ns(CLOCK_MONOTONIC);
        pid_t pid = fork();
        if (pid == 0) _exit(0);
        uint64_t t1 = now_ns(CLOCK_MONOTONIC);
        if (pid > 0) waitpid(pid, NULL, 0);
        uint64_t t2 = now_ns(CLOCK_MONOTONIC);
        fork_lat[i] = t1 - t0;
        total_lat[i] = t2 - t0;
    }
    free(heap);
}

/* minimum observable clock step over `samples` back-to-back reads */
static uint64_t min_clock_step_ns(size_t samples) {
    uint64_t minstep = UINT64_MAX;
    uint64_t prev = now_ns(CLOCK_MONOTONIC);
    for (size_t i = 0; i < samples; i++) {
        uint64_t t = now_ns(CLOCK_MONOTONIC);
        uint64_t d = t - prev;
        if (d > 0 && d < minstep) minstep = d;
        prev = t;
    }
    return minstep == UINT64_MAX ? 0 : minstep;
}

/* Phase 4: exec-spawn latency — fork + execvp(argv) with stdout/stderr on
 * /dev/null + waitpid. Models test-harness server spawn (fork, exec, dynamic
 * linking, brief child work). Returns number of samples recorded; 0 if the
 * target binary is unavailable (probe spawn exits 127). */
static size_t measure_exec_spawn(char *const argv[], uint64_t *out,
                                 size_t iters) {
    for (size_t i = 0; i < iters; i++) {
        uint64_t t0 = now_ns(CLOCK_MONOTONIC);
        pid_t pid = fork();
        if (pid == 0) {
            int devnull = open("/dev/null", O_WRONLY);
            if (devnull >= 0) {
                dup2(devnull, 1);
                dup2(devnull, 2);
            }
            execvp(argv[0], argv);
            _exit(127);
        }
        int st = 0;
        if (pid > 0) waitpid(pid, &st, 0);
        uint64_t t1 = now_ns(CLOCK_MONOTONIC);
        if (i == 0 && WIFEXITED(st) && WEXITSTATUS(st) == 127)
            return 0; /* target missing; skip this distribution */
        out[i] = t1 - t0;
    }
    return iters;
}

/* ------------------------------------------------------------------ */
/* main                                                               */
/* ------------------------------------------------------------------ */

static void emit_dist(const char *name, uint64_t *v, size_t n) {
    qsort(v, n, sizeof(uint64_t), cmp_u64);
    printf("    \"%s\": {\"n\": %zu, \"mean\": %.2f, \"p50\": %.2f, "
           "\"p90\": %.2f, \"p99\": %.2f, \"p999\": %.2f, \"max\": %.2f, "
           "\"over_1ms\": %zu, \"over_5ms\": %zu, \"over_10ms\": %zu}",
           name, n, mean_us(v, n), pctl_us(v, n, 50), pctl_us(v, n, 90),
           pctl_us(v, n, 99), pctl_us(v, n, 99.9),
           n ? (double)v[n - 1] / 1000.0 : 0.0,
           count_over(v, n, 1000000ull), count_over(v, n, 5000000ull),
           count_over(v, n, 10000000ull));
}

int main(int argc, char **argv) {
    size_t sleep_iters = 2000;
    uint64_t sleep_us = 1000; /* 1ms — same order as latency-monitor SLAs */
    double busy_secs = 5.0;
    size_t fork_iters = 200;
    size_t fork_heap_mb = 64;
    size_t spawn_iters = 200;
    int load = 0;
    const char *label = "";

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--sleep-iters") && i + 1 < argc)
            sleep_iters = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--sleep-us") && i + 1 < argc)
            sleep_us = (uint64_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--busy-secs") && i + 1 < argc)
            busy_secs = atof(argv[++i]);
        else if (!strcmp(argv[i], "--fork-iters") && i + 1 < argc)
            fork_iters = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--fork-heap-mb") && i + 1 < argc)
            fork_heap_mb = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--spawn-iters") && i + 1 < argc)
            spawn_iters = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--load") && i + 1 < argc)
            load = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--label") && i + 1 < argc)
            label = argv[++i];
        else {
            fprintf(stderr,
                    "usage: %s [--sleep-iters N] [--sleep-us US] "
                    "[--busy-secs S] [--fork-iters N] [--fork-heap-mb MB] "
                    "[--spawn-iters N] [--load NWORKERS] [--label STR]\n",
                    argv[0]);
            return 2;
        }
    }

    /* ---- environment capture ---- */
    char hostname[256] = "";
    gethostname(hostname, sizeof(hostname));
    struct utsname uts;
    memset(&uts, 0, sizeof(uts));
    uname(&uts);
    char clocksource[128], cgroup_cpu_max[128], loadavg[128];
    char dmi_vendor[128], dmi_product[128], model[256];
    read_file_trim("/sys/devices/system/clocksource/clocksource0/"
                   "current_clocksource",
                   clocksource, sizeof(clocksource));
    read_file_trim("/sys/fs/cgroup/cpu.max", cgroup_cpu_max,
                   sizeof(cgroup_cpu_max));
    if (!cgroup_cpu_max[0]) /* cgroup v1 fallback */
        read_file_trim("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", cgroup_cpu_max,
                       sizeof(cgroup_cpu_max));
    read_file_trim("/proc/loadavg", loadavg, sizeof(loadavg));
    read_file_trim("/sys/class/dmi/id/sys_vendor", dmi_vendor,
                   sizeof(dmi_vendor));
    read_file_trim("/sys/class/dmi/id/product_name", dmi_product,
                   sizeof(dmi_product));
    cpu_model(model, sizeof(model));

    struct timespec res;
    clock_getres(CLOCK_MONOTONIC, &res);
    long mono_res_ns = res.tv_nsec + res.tv_sec * 1000000000L;

    uint64_t steal_before = 0, total_before = 0;
    int have_stat = read_stat_ticks(&steal_before, &total_before) == 0;

    time_t wall = time(NULL);
    struct tm tmv;
    gmtime_r(&wall, &tmv);
    char iso[64];
    strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tmv);

    /* ---- allocate sample buffers ---- */
    uint64_t *sleep_ov = calloc(sleep_iters, sizeof(uint64_t));
    size_t gaps_cap = 200000;
    uint64_t *gaps = calloc(gaps_cap, sizeof(uint64_t));
    uint64_t *fork_lat = calloc(fork_iters, sizeof(uint64_t));
    uint64_t *fork_total = calloc(fork_iters, sizeof(uint64_t));
    uint64_t *spawn_true = calloc(spawn_iters, sizeof(uint64_t));
    uint64_t *spawn_dl = calloc(spawn_iters, sizeof(uint64_t));
    if (!sleep_ov || !gaps || !fork_lat || !fork_total || !spawn_true ||
        !spawn_dl) {
        fprintf(stderr, "alloc failure\n");
        return 1;
    }

    /* ---- run ---- */
    spawn_load_workers(load);

    uint64_t mono0 = now_ns(CLOCK_MONOTONIC);
    uint64_t real0 = now_ns(CLOCK_REALTIME);
    uint64_t raw0 = now_ns(CLOCK_MONOTONIC_RAW);

    uint64_t min_step = min_clock_step_ns(100000);

    size_t nsleep = measure_sleep(sleep_ov, sleep_iters, sleep_us);

    uint64_t busy_iters = 0, busy_max = 0, busy_lost = 0;
    uint64_t gap_thresh_ns = 2000; /* 2us: well above clock-read cost */
    size_t ngaps = measure_busy_gaps(gaps, gaps_cap, busy_secs, gap_thresh_ns,
                                     &busy_iters, &busy_max, &busy_lost);

    measure_fork(fork_lat, fork_total, fork_iters, fork_heap_mb);

    /* exec-spawn: /bin/true = pure fork+exec; openssl version = adds the
     * dynamic-linking work a TLS-enabled server binary pays at startup. */
    char *argv_true[] = {(char *)"/bin/true", NULL};
    char *argv_dl[] = {(char *)"openssl", (char *)"version", NULL};
    size_t n_spawn_true = measure_exec_spawn(argv_true, spawn_true, spawn_iters);
    size_t n_spawn_dl = measure_exec_spawn(argv_dl, spawn_dl, spawn_iters);

    uint64_t mono1 = now_ns(CLOCK_MONOTONIC);
    uint64_t real1 = now_ns(CLOCK_REALTIME);
    uint64_t raw1 = now_ns(CLOCK_MONOTONIC_RAW);

    kill_load_workers();

    uint64_t steal_after = 0, total_after = 0;
    if (have_stat)
        have_stat = read_stat_ticks(&steal_after, &total_after) == 0;

    /* ---- derived ---- */
    double mono_d = (double)(mono1 - mono0);
    double real_ppm =
        mono_d > 0 ? ((double)(real1 - real0) - mono_d) / mono_d * 1e6 : 0;
    double raw_ppm =
        mono_d > 0 ? ((double)(raw1 - raw0) - mono_d) / mono_d * 1e6 : 0;

    /* ---- emit JSON ---- */
    char e1[512], e2[512], e3[512], e4[512], e5[512], e6[512], e7[512],
        e8[512], e9[512];
    json_escape(hostname, e1, sizeof(e1));
    json_escape(uts.sysname, e2, sizeof(e2));
    json_escape(uts.release, e3, sizeof(e3));
    json_escape(uts.machine, e4, sizeof(e4));
    json_escape(model, e5, sizeof(e5));
    json_escape(clocksource, e6, sizeof(e6));
    json_escape(cgroup_cpu_max, e7, sizeof(e7));
    json_escape(loadavg, e8, sizeof(e8));
    json_escape(label, e9, sizeof(e9));
    char e10[512], e11[512];
    json_escape(dmi_vendor, e10, sizeof(e10));
    json_escape(dmi_product, e11, sizeof(e11));

    printf("{\n");
    printf("  \"probe_version\": 1,\n");
    printf("  \"label\": \"%s\",\n", e9);
    printf("  \"timestamp_utc\": \"%s\",\n", iso);
    printf("  \"host\": {\n");
    printf("    \"hostname\": \"%s\",\n", e1);
    printf("    \"os\": \"%s %s\",\n", e2, e3);
    printf("    \"arch\": \"%s\",\n", e4);
    printf("    \"cpu_model\": \"%s\",\n", e5);
    printf("    \"nproc_online\": %ld,\n", sysconf(_SC_NPROCESSORS_ONLN));
    printf("    \"clocksource\": \"%s\",\n", e6);
    printf("    \"cgroup_cpu_max\": \"%s\",\n", e7);
    printf("    \"loadavg_at_start\": \"%s\",\n", e8);
    printf("    \"dmi_vendor\": \"%s\",\n", e10);
    printf("    \"dmi_product\": \"%s\"\n", e11);
    printf("  },\n");
    printf("  \"config\": {\"sleep_iters\": %zu, \"sleep_us\": %lu, "
           "\"busy_secs\": %.1f, \"fork_iters\": %zu, \"fork_heap_mb\": %zu, "
           "\"spawn_iters\": %zu, "
           "\"load_workers\": %d, \"gap_threshold_us\": %.1f},\n",
           sleep_iters, (unsigned long)sleep_us, busy_secs, fork_iters,
           fork_heap_mb, spawn_iters, load, (double)gap_thresh_ns / 1000.0);
    printf("  \"results\": {\n");
    emit_dist("sleep_overshoot_us", sleep_ov, nsleep);
    printf(",\n");
    emit_dist("busy_gap_us", gaps, ngaps);
    printf(",\n");
    printf("    \"busy_loop\": {\"iterations\": %lu, \"gaps_recorded\": %zu, "
           "\"max_gap_us\": %.2f, \"lost_time_ms\": %.3f, "
           "\"lost_pct\": %.4f},\n",
           (unsigned long)busy_iters, ngaps, (double)busy_max / 1000.0,
           (double)busy_lost / 1e6,
           busy_secs > 0 ? (double)busy_lost / (busy_secs * 1e9) * 100.0 : 0);
    emit_dist("fork_call_us", fork_lat, fork_iters);
    printf(",\n");
    emit_dist("fork_total_us", fork_total, fork_iters);
    printf(",\n");
    emit_dist("spawn_true_us", spawn_true, n_spawn_true);
    printf(",\n");
    emit_dist("spawn_dl_us", spawn_dl, n_spawn_dl);
    printf(",\n");
    printf("    \"clock\": {\"mono_res_ns\": %ld, \"min_step_ns\": %lu, "
           "\"realtime_vs_mono_ppm\": %.3f, \"raw_vs_mono_ppm\": %.3f, "
           "\"run_duration_s\": %.2f},\n",
           mono_res_ns, (unsigned long)min_step, real_ppm, raw_ppm,
           mono_d / 1e9);
    if (have_stat) {
        uint64_t sd = steal_after - steal_before;
        uint64_t td = total_after - total_before;
        printf("    \"steal\": {\"steal_ticks_delta\": %lu, "
               "\"total_ticks_delta\": %lu, \"steal_pct\": %.4f}\n",
               (unsigned long)sd, (unsigned long)td,
               td > 0 ? (double)sd / (double)td * 100.0 : 0.0);
    } else {
        printf("    \"steal\": null\n");
    }
    printf("  }\n");
    printf("}\n");

    free(sleep_ov);
    free(gaps);
    free(fork_lat);
    free(fork_total);
    free(spawn_true);
    free(spawn_dl);
    return 0;
}
