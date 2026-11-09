/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* The two hand-maintained lists of the fast path, checked against the command table.
 *
 * The lane allowlist (fp_lane_commands): a lane runs a command on the assumption that every key the
 * command touches is one it declares, since the lane owns only the declared keys' hashtables. The key
 * specs are the server's own statement of that: a spec with an UNKNOWN begin-search or find-keys type,
 * or the INCOMPLETE flag, says the command reaches keys the specs cannot name (SORT BY/GET, MIGRATE
 * KEYS); CMD_TOUCHES_ARBITRARY_KEYS says so for the whole command (SCAN, RANDOMKEY).
 *
 * The deny list (fpCommandAllowed): a fast-path client's commands run on an executor client, so a
 * command that changes session state (SELECT, MULTI, WATCH, CLIENT), blocks, observes other clients
 * (MONITOR, pub/sub), or runs with admin or script rights must be refused. The golden list below is the
 * set the gate accepts today; a change to the gate or a new command changes it and gets reviewed. */

#include <gtest/gtest.h>

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

extern "C" {
#include "fastpath.h"
#include "server.h"
extern hashtableType commandSetType;
void createSharedObjects(void);
}

#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <vector>

static const char *fl_logfile = "/dev/null";

/* The commands fpCommandAllowed accepts, sorted, space-separated, one trailing space. */
#define FL_GOLDEN_ACCEPTED \
    "append bitcount bitfield bitfield_ro bitop bitpos copy dbsize decr decrby del delex delifeq " \
    "dump exists expire expireat expiretime flushall flushdb geoadd geodist geohash geopos georadius " \
    "georadius_ro georadiusbymember georadiusbymember_ro geosearch geosearchstore get getbit getdel " \
    "getex getrange getset hdel hexists hexpire hexpireat hexpiretime hget hgetall hgetdel hgetex " \
    "hincrby hincrbyfloat hkeys hlen hmget hmset hpersist hpexpire hpexpireat hpexpiretime hpttl " \
    "hrandfield hscan hset hsetex hsetnx hstrlen httl hvals incr incrby incrbyfloat increx keys lcs " \
    "lindex linsert llen lmove lmpop lolwut lpop lpos lpush lpushx lrange lrem lset ltrim " \
    "memory|usage mget migrate move mset msetex msetnx object|encoding object|freq object|idletime " \
    "object|refcount persist pexpire pexpireat pexpiretime pfadd pfcount pfmerge phcard phdel " \
    "phdelprefix phexists phget phgetall phlongest phmget phmset phprefixes phscan phset ping psetex " \
    "pttl rename renamenx restore restore-asking rpop rpoplpush rpush rpushx sadd scard sdiff " \
    "sdiffstore set setbit setex setnx setrange sinter sintercard sinterstore sismember smembers " \
    "smismember smove sort sort_ro spop srandmember srem sscan strlen substr sunion sunionstore " \
    "swapdb touch ttl type unlink xack xackdel xadd xautoclaim xclaim xdel xdelex xgroup|create " \
    "xgroup|createconsumer xgroup|delconsumer xgroup|destroy xgroup|setid xinfo|consumers " \
    "xinfo|groups xinfo|stream xlen xpending xrange xrevrange xsetid xtrim zadd zcard zcount zdiff " \
    "zdiffstore zincrby zinter zintercard zinterstore zlexcount zmpop zmscore zpopmax zpopmin " \
    "zrandmember zrange zrangebylex zrangebyscore zrangestore zrank zrem zremrangebylex " \
    "zremrangebyrank zremrangebyscore zrevrange zrevrangebylex zrevrangebyscore zrevrank zscan " \
    "zscore zunion zunionstore "

class FastpathListsTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(fl_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        createSharedObjects();
        unitTestInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        unitTestPopulateCommandTable();
        unitTestACLInit();
    }

    /* Every command the parser can resolve: top-level commands and their subcommands. */
    static std::vector<struct serverCommand *> allCommands() {
        std::vector<struct serverCommand *> out;
        hashtableIterator it;
        hashtableInitIterator(&it, server.commands, 0);
        void *next;
        while (hashtableNext(&it, &next)) {
            struct serverCommand *cmd = static_cast<struct serverCommand *>(next);
            if (cmd->subcommands_ht) {
                hashtableIterator sit;
                hashtableInitIterator(&sit, cmd->subcommands_ht, 0);
                void *snext;
                while (hashtableNext(&sit, &snext)) out.push_back(static_cast<struct serverCommand *>(snext));
                hashtableCleanupIterator(&sit);
            } else {
                out.push_back(cmd);
            }
        }
        hashtableCleanupIterator(&it);
        return out;
    }
};

/* Item 4 of the proof-check list: the allowlist declares all keys. */
TEST_F(FastpathListsTest, LaneAllowlistDeclaresAllKeys) {
    const char **names = testOnlyFastpathLaneCommands();
    int n = 0;
    for (; names[n]; n++) {
        struct serverCommand *cmd = lookupCommandByCString(names[n]);
        ASSERT_NE(cmd, nullptr) << names[n];
        EXPECT_EQ(cmd->parent, nullptr) << names[n] << ": a subcommand cannot be a lane command";
        EXPECT_EQ(cmd->subcommands_ht, nullptr) << names[n] << ": a container cannot be a lane command";
        EXPECT_EQ(cmd->module_cmd, nullptr) << names[n];
        EXPECT_GT(cmd->key_specs_num, 0) << names[n] << ": no key specs, so no partition";
        EXPECT_EQ(cmd->flags & CMD_TOUCHES_ARBITRARY_KEYS, 0u) << names[n];
        for (int i = 0; i < cmd->key_specs_num; i++) {
            const keySpec *ks = &cmd->key_specs[i];
            EXPECT_NE(ks->begin_search_type, KSPEC_BS_UNKNOWN) << names[n] << " spec " << i << " cannot name its keys";
            EXPECT_NE(ks->find_keys_type, KSPEC_FK_UNKNOWN) << names[n] << " spec " << i << " cannot name its keys";
            EXPECT_EQ(ks->flags & CMD_KEY_INCOMPLETE, 0u) << names[n] << " spec " << i << " is incomplete";
        }
        /* A lane command the fast path refuses would never reach a lane: a dead entry. */
        EXPECT_TRUE(testOnlyFastpathCommandAllowed(cmd)) << names[n];
    }
    EXPECT_EQ(n, 124) << "the allowlist changed size: review the new entry's key specs";
}

/* The gate must refuse these outright, whatever their flags say. */
static const char *fl_must_refuse[] = {
    /* session and observer state */
    "select", "multi", "exec", "discard", "watch", "unwatch", "client|setname", "client|tracking",
    "client|reply", "client|no-touch", "client|no-evict", "client|caching", "client|capa", "hello", "auth",
    "reset", "quit", "monitor", "readonly", "readwrite", "asking",
    /* pub/sub */
    "subscribe", "psubscribe", "ssubscribe", "unsubscribe", "punsubscribe", "sunsubscribe", "publish",
    /* blocking */
    "blpop", "brpop", "blmove", "blmpop", "brpoplpush", "bzpopmin", "bzpopmax", "bzmpop", "xread",
    "xreadgroup", "wait", "waitaof",
    /* scripts, functions, modules */
    "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro", "script|load", "function|load",
    "module|load",
    /* admin, config, ACL, cluster, replication */
    "config|set", "acl|setuser", "acl|deluser", "cluster|setslot", "cluster|addslots", "replicaof",
    "failover", "shutdown", "debug", NULL};

static std::string joinSorted(std::set<std::string> &s) {
    std::ostringstream o;
    for (const std::string &x : s) o << x << ' ';
    return o.str();
}

/* Item 5: the deny list is complete. The gate accepts exactly the golden set, and nothing it accepts is
 * blocking, admin, a transaction, scripting, pub/sub, or a connection-state command. */
TEST_F(FastpathListsTest, CommandGateRefusesStateChangingCommands) {
    std::set<std::string> accepted;
    for (struct serverCommand *cmd : allCommands()) {
        if (!testOnlyFastpathCommandAllowed(cmd)) continue;
        std::string name(cmd->fullname);
        accepted.insert(name);
        EXPECT_EQ(cmd->flags & (CMD_BLOCKING | CMD_ADMIN | CMD_PUBSUB | CMD_NOSCRIPT | CMD_NO_MULTI), 0u) << name;
        if (cmd->proc == pingCommand) continue; /* accepted by name: @connection, neither write nor read */
        EXPECT_EQ(cmd->acl_categories & (ACL_CATEGORY_BLOCKING | ACL_CATEGORY_ADMIN | ACL_CATEGORY_PUBSUB |
                                         ACL_CATEGORY_TRANSACTION | ACL_CATEGORY_SCRIPTING | ACL_CATEGORY_CONNECTION),
                  0u)
            << name;
        EXPECT_NE(cmd->flags & (CMD_WRITE | CMD_READONLY), 0u) << name;
        EXPECT_EQ(cmd->module_cmd, nullptr) << name;
    }
    for (int i = 0; fl_must_refuse[i]; i++) {
        struct serverCommand *cmd = lookupCommandByCString(fl_must_refuse[i]);
        ASSERT_NE(cmd, nullptr) << fl_must_refuse[i];
        EXPECT_FALSE(testOnlyFastpathCommandAllowed(cmd)) << fl_must_refuse[i];
    }
    std::string golden = FL_GOLDEN_ACCEPTED;
    EXPECT_EQ(joinSorted(accepted), golden) << "the gate's accepted set changed: review the diff above";
}
