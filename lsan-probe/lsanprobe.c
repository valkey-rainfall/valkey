/* Minimal stand-in for a valkey-server exit under LeakSanitizer:
 * several live threads in different states (condvar wait, nanosleep,
 * blocked syscall, busy spin), some malloc traffic, then exit(0).
 * LSan must stop-the-world via ptrace at exit; the experiment counts
 * how often that hangs or prints the ptrace warning. */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t c = PTHREAD_COND_INITIALIZER;
static volatile int spin = 1;

static void *waiter(void *a) {
    pthread_mutex_lock(&m);
    while (1) pthread_cond_wait(&c, &m);
    return a;
}

static void *sleeper(void *a) {
    while (1) {
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
    return a;
}

static void *blocker(void *a) {
    pause();
    return a;
}

static void *spinner(void *a) {
    while (spin) {
    }
    return a;
}

int main(int argc, char **argv) {
    int n = argc > 1 ? atoi(argv[1]) : 2;
    pthread_t t;
    for (int i = 0; i < n; i++) {
        pthread_create(&t, NULL, waiter, NULL);
        pthread_create(&t, NULL, sleeper, NULL);
        pthread_create(&t, NULL, blocker, NULL);
    }
    pthread_create(&t, NULL, spinner, NULL);
    for (int i = 0; i < 2000; i++) {
        void *p = malloc(64 + (i % 512));
        memset(p, 1, 64 + (i % 512));
        free(p);
    }
    usleep(2000);
    exit(0);
}
