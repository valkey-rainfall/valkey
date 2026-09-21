/* Reply scatter/gather helpers shared between networking.c's writevToClient()
 * and the io_uring batch path, which needs to build the same iovec array and
 * apply the same post-write bookkeeping around a batched sendmsg. */
#ifndef REPLY_IOV_H
#define REPLY_IOV_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "util.h" /* LONG_STR_SIZE */

struct client;

/* Bulk string reply requires 3 iov entries -
 * length prefix ($<length>\r\n), string (<data>) and suffix (\r\n) */
#define NUM_OF_IOV_PER_BULK_STR 3
/* Bulk string prefix max size (long + $ + \r\n) */
#define BULK_STR_LEN_PREFIX_MAX_SIZE (LONG_STR_SIZE + 3)

/* This struct is used by writevToClient to prepare iovec array for submitting to connWritev */
typedef struct replyIOV {
    int iovcnt;  /* number of elements in iov array */
    int iovsize; /* capacity of iov array */
    struct iovec *iov;
    ssize_t iov_len_total;   /* Total length of data pointed by iov array */
    size_t last_written_len; /* Length of data in the last written buffer
                              * partially written in previous writevToClient invocation */
    int limit_reached;       /* Non zero if either max iov count or NET_MAX_WRITES_PER_EVENT limit
                              * reached during iovec array preparation  */
    /* Auxiliary fields for scattering BUFSTR_REF chunks from encoded buffers */
    int prfxcnt;                                    /* number of prefixes */
    char (*prefixes)[BULK_STR_LEN_PREFIX_MAX_SIZE]; /* bulk string prefixes */
    char *crlf;                                     /* bulk string suffix */
} replyIOV;

/*  The bufWriteMetadata struct is used by writevToClient to record metadata
 *  about scattering of reply buffer to iov array */
typedef struct bufWriteMetadata {
    char *buf;
    size_t bufpos;
    uint64_t data_len; /* Actual bytes out. Differs from bufpos if buffer encoded */
    int complete;      /* Was the buffer completely scattered to iov or
                          process stopped due encountered limit */
} bufWriteMetadata;

/* Scatter the client's pending replies (static buffer + reply list, from the
 * last written position) into reply->iov. The caller owns the arrays:
 * iov_arr[iovmax], prefixes[iovmax / NUM_OF_IOV_PER_BULK_STR + 1], crlf[2],
 * and metadata[iovmax + 1]. Returns the number of metadata entries filled
 * (0 means nothing to write). Safe from the main thread or an I/O thread. */
int buildReplyIOV(struct client *c, int iovmax, struct iovec *iov_arr, char (*prefixes)[BULK_STR_LEN_PREFIX_MAX_SIZE], char *crlf, replyIOV *reply, bufWriteMetadata *metadata);

/* Record the outcome of writing `totwritten` bytes (or <= 0 on error) of an
 * iov built by buildReplyIOV(): sets c->nwritten / WRITE_FLAGS_WRITE_ERROR
 * and advances c->io_last_written. Returns C_OK if anything was written. */
int applyReplyIOVWritten(struct client *c, replyIOV *reply, bufWriteMetadata *metadata, int bufcnt, ssize_t totwritten);

#endif /* REPLY_IOV_H */
