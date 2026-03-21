#ifndef PDB_RDMA_H
#define PDB_RDMA_H

#include <infiniband/verbs.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <arpa/inet.h>

#include "pdb_serialize.h"
#include "pdb_conf.h"
#include "pdb_conninfo.h"


#define PDB_RDMA_MEMPOOL_FULL   -1
#define PDB_RDMA_PARAM_ERROR    -1
#define PDB_RDMA_OK             0

#define PDB_RDMA_PORT           1
#define PDB_RDMA_GID_IDX        1
#define PDB_BUFFER_FROZEN       -1
#define RDMA_TEST_REPORT        1

#define PDB_RDMA_INCRE_BUFFER_LEN   4*1024*1024
#define PDB_RDMA_MEMPOOL_SIZE       512*1024*1024

typedef struct __attribute__((packed)) pdb_rdma_conn_info {
    uint32_t qpn;          // Queue Pair Number
    uint32_t psn;          // Packet Sequence Number
    uint16_t lid;          // Local ID
    uint8_t  gid[16];      // Global ID
} pdb_rdma_conn_info;

// mempool
typedef struct pdb_rdma_mempool {
    char* base_addr;
    size_t total_size;
    size_t* used_offset;        // ebpf tail
    size_t* send_head;
} pdb_rdma_mempool;

// global
typedef struct pdb_rdma_snapshot_ctx {
    struct ibv_context* ctx;
    struct ibv_pd*      pd;
    struct ibv_mr*      mr;

    pdb_rdma_mempool    pool;      

    int ref_count;
    int is_process_down;
} pdb_rdma_snapshot_ctx;

// connection
typedef struct pdb_rdma_conn_ctx {
    struct ibv_cq* cq;
    struct ibv_qp* qp;
    pdb_rdma_conn_info  local_info;
    
    pdb_rdma_snapshot_ctx* snap;
    volatile int pending_sends;

    unsigned long long vaddr_str;
    unsigned long rkey_str;
} pdb_rdma_conn_ctx;

typedef struct {
    const uint8_t *data;
    size_t pos;
    size_t length;
} PdbReadContext;

extern pdb_rdma_snapshot_ctx* global_master_snapshot;
struct conn_info;

pdb_rdma_snapshot_ctx* pdb_rdma_create_snapshot(const char* dev_name, size_t pool_size);
void* pdb_rdma_pool_alloc(pdb_rdma_snapshot_ctx* snap, size_t size);
void pdb_rdma_release_snapshot(pdb_rdma_snapshot_ctx* snap); // 减少引用计数，归零则物理销毁

pdb_rdma_conn_ctx* pdb_rdma_create_conn(pdb_rdma_snapshot_ctx* snap);
void execute_rdma_read_heist(pdb_rdma_conn_ctx* slave_conn, uint64_t remote_vaddr, uint32_t remote_rkey, size_t data_size);
int pdb_rdma_connect_qp(pdb_rdma_conn_ctx* conn, pdb_rdma_conn_info* remote_info);
void pdb_rdma_destroy_conn(pdb_rdma_conn_ctx* conn);
int pdb_rdma_serialize(pdb_rdma_snapshot_ctx* rdma);
int pdb_rdma_deserialize(pdb_rdma_snapshot_ctx* rdma);

// For increment syn
int pdb_rdma_incremental_append(void* dataStructure, const char* key, uint8_t opcode);
void pdb_rdma_post_recv_monitor(pdb_rdma_conn_ctx* slave_conn);
void pdb_increment_syn();

#endif