/* specread no-op stubs for standalone tools (valkey-cli, valkey-benchmark).
 *
 * hashtable.c calls the specread quiescence hooks (specreadExclusiveEnter/Leave,
 * specreadDeferFreeRaw), whose real definitions live in specread.c — which depends
 * on the server core and is not linked into the tools. The tools have no
 * speculative walkers, so no-ops are correct.
 *
 * This MUST be a separate translation unit linked ONLY into the tools:
 * same-TU weak stubs get inlined by LTO at hashtable.c's call sites before
 * strong-symbol resolution, silently no-op'ing the server's drains (observed
 * as the expiry-race crash returning). */

void specreadExclusiveEnter(void) {}
void specreadExclusiveLeave(void) {}
int specreadDeferFreeRaw(void *ptr) {
    (void)ptr;
    return 0;
}
