/* Owner threads (vkevo D3, "owner-steer"): one process, one port per worker core.
 *
 * See owner.c for the design. Off unless owner-threads > 0. */

#ifndef VALKEY_OWNER_H
#define VALKEY_OWNER_H

struct client;
struct owner;

/* The owner thread this thread is, NULL on main and every other thread. */
extern _Thread_local struct owner *own_self;

/* Client residence (client->owner_tid). */
#define OWNER_ON_MAIN (-1)
#define OWNER_IN_TRANSIT (-2) /* handed to main, not yet taken: only inside the owner's window */

/* With owner threads the standalone keyspace has 2^OWNER_SHARD_BITS tables, each a range of
 * 16384 >> OWNER_SHARD_BITS slots; every owner owns whole tables. */
#define OWNER_SHARD_BITS 10
/* The keyspace table of a slot (CLUSTER_SLOT_MASK_BITS is in cluster.h). */
#define OWNER_TABLE_OF_SLOT(slot) ((slot) >> (CLUSTER_SLOT_MASK_BITS - OWNER_SHARD_BITS))

int ownerThreadsEnabled(void);
int ownerValidateConfig(const char **err);
void ownerThreadsInit(void);  /* initServer: listeners and state, before any client exists */
void ownerThreadsStart(void); /* after loading: spawn the threads (they start stopped) */
void ownerAfterSleep(void);   /* main: stop the world, fold stats, take handed-off clients */
void ownerBeforeSleepEnd(void); /* main: return idle guests, resume the world */

/* Hooks from networking.c / server.c. */
int ownerAdoptAccepted(struct client *c); /* clientAcceptHandler: 1 when an owner took the client */
void ownerIOThreadsChanged(void);         /* CONFIG SET io-threads: owners sleep while it is > 1 */
void ownerReclaim(struct client *c);       /* freeClient on main: take the client back from its owner */
void ownerClientFreed(struct client *c);   /* freeClient on main: drop it from the guest list */
void ownerErrorReply(struct client *c, const char *s, size_t len, int flags);
void ownerCountOutbufDisconnect(void);
int ownerResizeAllowed(size_t more_mem);    /* hashtable resizes on an owner thread: within its maxmemory headroom */
int ownerClusterCommand(struct client *c); /* CLUSTER on a standalone server with owner ports */
void ownerInfo(sds *info);
extern int owner_accept_home; /* createClient: home owner of the connection being accepted */

#endif
