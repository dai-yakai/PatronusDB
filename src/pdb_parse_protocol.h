#ifndef __PDB_PARSE_PROTOCOL_H__
#define __PDB_PARSE_PROTOCOL_H__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <jemalloc/jemalloc.h>
#include <sys/sendfile.h>
#include <sys/epoll.h>

#include "pdb_array.h"
#include "pdb_hash.h"
#include "pdb_rbtree.h"
#include "pdb_skiptable.h"
#include "pdb_bitmap.h"
#include "pdb_rdb.h"
#include "pdb_dump.h"
#include "pdb_aof.h"
#include "pdb_value.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"
#include "pdb_rdma.h"
#include "pdb_conninfo.h"
#include "pdb_replication.h"
#include "pdb_malloc.h"

#ifdef ENABLE_DPDK
#include "pdb_dpdk_hook.h"
#endif

extern pdb_rdma_conn_ctx* slave_conn;
extern uint64_t remote_vaddr;
extern uint32_t remote_rkey;
extern size_t   pull_size;

extern pdb_rdma_snapshot_ctx* incre_slave_snap;
extern pdb_rdma_snapshot_ctx* incre_master_snap;
extern pdb_rdma_conn_ctx* incre_slave_conn;
extern pdb_rdma_conn_ctx* incre_master_conn;

extern pdb_memory_manager* global_memory_manager;

char* find_crlf(char* start, int remaining_len);
int pdb_split_token(char* msg, int len, char* tokens[]);
int pdb_parser_cmd(const char* cmd_str);
int check_resp_integrity(const char *buf, int len, int* bulk_length);
int pdb_filter_protocol(int fd, char** tokens, int count, char* response);
int pdb_protocol(int fd, char* msg, int length, char* out);

#endif