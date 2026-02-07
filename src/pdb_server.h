#ifndef __PDB_SOTRE_H__
#define __PDB_SOTRE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include "pdb_handler.h"
#include "pdb_malloc.h"
#include "pdb_replication.h"
#include "pdb_conf.h"
#include "pdb_log.h"


#define ENABLE_REACTOR      0
#define ENABLE_NTYCO        1
#define ENABLE_URING        0

#define ENABLE_ARRAY        0
#define ENABLE_RBTREE       1
#define ENABLE_HASH         1
#define ENABLE_SKIPTABLE    0

#define ENABLE_PRINT_PDB     0
#define ENABLE_THREADPOOL   0
#define ENABLE_MEMPOOL      0

#if ENABLE_ARRAY
#include "pdb_array.h"
#endif

#if ENABLE_RBTREE
#include "pdb_rbtree.h"
#endif

#if ENABLE_HASH
#include "pdb_hash.h"
#endif

#if ENABLE_THREADPOOL
#include "threadpool.h"
#endif

#define PDB_MAX_TOKENS      128

#if ENABLE_ARRAY
extern pdb_array_t global_array;
#endif

#if ENABLE_RBTREE
extern pdb_rbtree_t global_rbtree;
#endif 

#if ENABLE_HASH
extern pdb_hash_t global_hash;
#endif

#if ENABLE_SKIPTABLE
extern pdb_rbtree_t global_rbtree;
#endif

#if ENABLE_THREADPOOL
#define PDB_WORKER_NUM  4   

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
    "EXIT", "SAVE", "ASAVE", 
#if ENABLE_ARRAY  
    "MSET", "MGET", 
#endif
    "RMSET", "RMGET", "HMSET", "HMGET",
    "SYN"
};

const char* response[] = {
    NULL
};

enum{
#if ENABLE_ARRAY
    // array
    PDB_CMD_START = 0,
    PDB_CMD_SET = PDB_CMD_START,    //0
    PDB_CMD_GET,                    //1
    PDB_CMD_DEL,                    //2
    PDB_CMD_MOD,                    //3
    PDB_CMD_EXIST,                  //4
#endif

#if ENABLE_RBTREE
    // rbtree
    PDB_CMD_RSET,                   //5
    PDB_CMD_RGET,                   //6
    PDB_CMD_RDEL,                   //7
    PDB_CMD_RMOD,                   //8
    PDB_CMD_REXIST,                 //9
#endif

#if ENABLE_HASH
    // hash
    PDB_CMD_HSET,                   //10
    PDB_CMD_HGET,                   //11
    PDB_CMD_HDEL,                   //12
    PDB_CMD_HMOD,                   //13
    PDB_CMD_HEXIST,                 //14
#endif
    PDB_CMD_EXIT,                   //15
    PDB_CMD_SAVE,
    PDB_CMD_ASAVE,
#if ENABLE_ARRAY
    PDB_CMD_MSET,
    PDB_CMD_MGET,
#endif
#if ENABLE_RBTREE
    PDB_CMD_RMSET,
    PDB_CMD_RMGET,
#endif
#if ENABLE_HASH
    PDB_CMD_HMSET,
    PDB_CMD_HMGET,
#endif
#if ENABLE_SKIPTABLE
    // skiptable
    PDB_CMD_SKSET,
    PDB_CMD_SKGET,
    PDB_CMD_SKDEL,
    PDB_CMD_SKMOD,
    PDB_CMD_SKEXIST,
#endif
    PDB_CMD_SYN,
    PDB_CMD_COUNT
};


struct replication_s global_replication = {0};

int pdb_protocol(char* rmsg, int length, char* out);
int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int ntyco_entry(unsigned short port, msg_handler request_handler, msg_handler response_handlerr);
int uring_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int pdb_filter_protocol(char** tokens, int count, char* response);

#endif