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
#include "pdb_parse_protocol.h"
#include "pdb_aof.h"
#include "pdb_intset.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"

#define ENABLE_ARRAY        0
#define ENABLE_RBTREE       1
#define ENABLE_HASH         1
#define ENABLE_SKIPTABLE    0

#define ENABLE_PRINT_PDB        0
#define ENABLE_THREADPOOL       0
#define ENABLE_MEMPOOL          1

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

// #define PDB_MAX_TOKENS      128

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





struct replication_s global_replication = {0};

int pdb_protocol(char* rmsg, int length, char* out);
int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int ntyco_entry(unsigned short port, msg_handler request_handler, msg_handler response_handlerr);
int uring_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler);
int pdb_filter_protocol(char** tokens, int count, char* response);

#endif