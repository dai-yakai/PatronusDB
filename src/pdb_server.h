#ifndef __KV_SOTRE_H__
#define __KV_SOTRE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include "pdb_handler.h"
#include "pdb_malloc.h"
#include "pdb_replication.h"
#include "pdb_conf.h"


#define ENABLE_REACTOR      0
#define ENABLE_NTYCO        1
#define ENABLE_URING        0

#define ENABLE_ARRAY        1
#define ENABLE_RBTREE       1
#define ENABLE_HASH         1
#define ENABLE_SKIPTABLE    0

#define ENABLE_PRINT_KV     0
#define ENABLE_THREADPOOL   0
#define ENABLE_MEMPOOL      1

#if ENABLE_ARRAY
#include "kv_array.h"
#endif

#if ENABLE_RBTREE
#include "kv_rbtree.h"
#endif

#if ENABLE_HASH
#include "kv_hash.h"
#endif

#if ENABLE_THREADPOOL
#include "threadpool.h"
#endif

#define KVS_MAX_TOKENS      128

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif 

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

#if ENABLE_SKIPTABLE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_THREADPOOL
#define KV_WORKER_NUM  4   

static threadPool_t g_kv_pool;
#endif


const char* command[] = {
#if ENABLE_ARRAY
    "SET", "GET", "DEL", "MOD", "EXIST",
#endif

#if ENABLE_RBTREE
    "RSET", "RGET", "RDEL", "RMOD", "REXIST",
#endif

#if ENABLE_HASH
    "HSET", "HGET", "HDEL", "HMOD", "HEXIST",
#endif
    "EXIT", "SAVE", "ASAVE", "MSET", "MGET", "RMSET", "RMGET", "HMSET", "HMGET",
    "SYN"
};

const char* response[] = {
    NULL
};

enum{
#if ENABLE_ARRAY
    // array
    KVS_CMD_START = 0,
    KVS_CMD_SET = KVS_CMD_START,    //0
    KVS_CMD_GET,                    //1
    KVS_CMD_DEL,                    //2
    KVS_CMD_MOD,                    //3
    KVS_CMD_EXIST,                  //4
#endif

#if ENABLE_RBTREE
    // rbtree
    KVS_CMD_RSET,                   //5
    KVS_CMD_RGET,                   //6
    KVS_CMD_RDEL,                   //7
    KVS_CMD_RMOD,                   //8
    KVS_CMD_REXIST,                 //9
#endif

#if ENABLE_HASH
    // hash
    KVS_CMD_HSET,                   //10
    KVS_CMD_HGET,                   //11
    KVS_CMD_HDEL,                   //12
    KVS_CMD_HMOD,                   //13
    KVS_CMD_HEXIST,                 //14
#endif
    KVS_CMD_EXIT,                   //15
    KVS_CMD_SAVE,
    KVS_CMD_ASAVE,
#if ENABLE_ARRAY
    KVS_CMD_MSET,
    KVS_CMD_MGET,
#endif
#if ENABLE_RBTREE
    KVS_CMD_RMSET,
    KVS_CMD_RMGET,
#endif
#if ENABLE_HASH
    KVS_CMD_HMSET,
    KVS_CMD_HMGET,
#endif
#if ENABLE_SKIPTABLE
    // skiptable
    KVS_CMD_SKSET,
    KVS_CMD_SKGET,
    KVS_CMD_SKDEL,
    KVS_CMD_SKMOD,
    KVS_CMD_SKEXIST,
#endif
    KVS_CMD_SYN,
    KVS_CMD_COUNT
};


struct replication_s global_replication = {0};

int kvs_protocol(char* rmsg, int length, char* out);
int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int ntyco_entry(unsigned short port, msg_handler request_handler, msg_handler response_handlerr);
int uring_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int kvs_filter_protocol(char** tokens, int count, char* response);

#endif