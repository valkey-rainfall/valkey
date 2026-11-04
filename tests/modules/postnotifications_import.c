/* A module that supports atomic slot migration and, from a post-notification job,
 * INCRs a counter key every time a string key other than the counter changes.
 * The counter key is the module argument. Used to test that the writes such a job
 * makes while a slot import client is current reach the replicas. */

#include "valkeymodule.h"

#include <string.h>

static ValkeyModuleString *counter_key = NULL;

static void IncrCounter(ValkeyModuleCtx *ctx, void *pd) {
    VALKEYMODULE_NOT_USED(pd);
    ValkeyModuleCallReply *rep = ValkeyModule_Call(ctx, "incr", "!s", counter_key);
    ValkeyModule_FreeCallReply(rep);
}

static int StringChanged(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(event);
    if (ValkeyModule_StringCompare(key, counter_key) == 0) return VALKEYMODULE_OK;
    ValkeyModule_AddPostNotificationJob(ctx, IncrCounter, NULL, NULL);
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "postnotifications_import", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (argc != 1) return VALKEYMODULE_ERR;

    ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION);
    counter_key = ValkeyModule_CreateStringFromString(NULL, argv[0]);

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_STRING, StringChanged) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    VALKEYMODULE_NOT_USED(ctx);
    ValkeyModule_FreeString(NULL, counter_key);
    counter_key = NULL;
    return VALKEYMODULE_OK;
}
