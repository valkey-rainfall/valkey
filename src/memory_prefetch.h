#ifndef MEMORY_PREFETCH_H
#define MEMORY_PREFETCH_H

struct client;

void prefetchCommandsBatchInit(void);
void processClientsCommandsBatch(void);
int addCommandToBatchAndProcessIfFull(struct client *c);
void removeClientFromPendingCommandsBatch(struct client *c);
int onMaxBatchSizeChange(const char **err);
void prefetchRingBeforeExecute(struct client *c);
void prefetchRingClientGone(struct client *c);

#endif /* MEMORY_PREFETCH_H */
