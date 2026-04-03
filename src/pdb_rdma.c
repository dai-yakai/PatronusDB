#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pdb_rdma.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"
#include "pdb_bitmap.h"
#include "pdb_value.h"
#include "pdb_rbtree.h"
#include "pdb_array.h"

extern void pdb_ebpf_poll();
pdb_rdma_snapshot_ctx* global_master_snapshot = NULL;

// Performance Configuration Constants
// Divide large data into 16 chunks for parallel fetching
#define NUM_CHUNKS 1024       
// Corresponding to max_rd_atomic in QP configuration  
#define RDMA_READ_DEPTH 32

static double get_delta_ms(struct timeval t_start, struct timeval t_end) {
    return (t_end.tv_sec - t_start.tv_sec) * 1000.0 + (t_end.tv_usec - t_start.tv_usec) / 1000.0;
}


/**
 * **************************************************
 * ************   RDMA mempool   ********************
 * **************************************************
 */
static void* _alloc_aligned_memory(size_t size) {
    void* ptr = NULL;
    long page_size = sysconf(_SC_PAGESIZE); 
    int ret = posix_memalign(&ptr, page_size, size);
    if (ret != 0){
        pdb_log_error("rdma alloc mem error: %s\n", strerror(ret));
        return NULL;
    }

    memset(ptr, 0, size);
    return ptr;
}

void* pdb_rdma_pool_alloc(pdb_rdma_snapshot_ctx* snap, size_t size) {
    if (!snap || size == 0) return NULL;
    size_t aligned_size = (size + 7) & ~7; 
    if (*(snap->pool.used_offset) + aligned_size > snap->pool.total_size) return NULL; 
    void* ptr = snap->pool.base_addr + *(snap->pool.used_offset);
    *(snap->pool.used_offset) += aligned_size;
    return ptr;
}


/**
 * **************************************************
 * ************   RDMA init struct  *****************
 * **************************************************
 */
pdb_rdma_snapshot_ctx* pdb_rdma_create_snapshot(const char* dev_name, size_t pool_size) {
    if (pool_size == 0) return NULL;

    pdb_rdma_snapshot_ctx* snap = (pdb_rdma_snapshot_ctx*)malloc(sizeof(pdb_rdma_snapshot_ctx));
    memset(snap, 0, sizeof(pdb_rdma_snapshot_ctx));
    snap->ref_count = 1; // master owner
    snap->is_process_down = 1;

    // init mempool
    snap->pool.total_size = pool_size;
    snap->pool.used_offset = pdb_malloc(sizeof(int));
    *(snap->pool.used_offset) = 0;
    pdb_log_info("pool_size: %zu\n", pool_size);
    snap->pool.base_addr = (char*)_alloc_aligned_memory(pool_size);
    if (!snap->pool.base_addr) { 
        pdb_log_info("rdma init mempool failed: alloc mem error\n");
        free(snap); 
        return NULL; 
    }

    // init 
    struct ibv_device** dev_list;
    int num_devices;
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) goto cleanup;

    struct ibv_device *ib_dev = NULL;
    for (int i = 0; i < num_devices; i++) {
        if (!dev_name || strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
            ib_dev = dev_list[i]; break;
        }
    }
    if (!ib_dev) goto cleanup_devlist;

    snap->ctx = ibv_open_device(ib_dev);
    if (!snap->ctx) goto cleanup_devlist;
    ibv_free_device_list(dev_list);

    snap->pd = ibv_alloc_pd(snap->ctx);
    if (!snap->pd) goto cleanup_ctx;

    int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    snap->mr = ibv_reg_mr(snap->pd, snap->pool.base_addr, snap->pool.total_size, access_flags);
    if (!snap->mr) goto cleanup_pd;

    return snap;

cleanup_pd:      ibv_dealloc_pd(snap->pd);
cleanup_ctx:     ibv_close_device(snap->ctx);
cleanup_devlist: ibv_free_device_list(dev_list);
cleanup:         free(snap->pool.base_addr); free(snap);
                 return NULL;
}

void pdb_rdma_release_snapshot(pdb_rdma_snapshot_ctx* snap) {
    if (!snap) return;
    snap->ref_count--; 

    if (snap->ref_count <= 0) {
        if (snap->mr) ibv_dereg_mr(snap->mr);
        if (snap->pd) ibv_dealloc_pd(snap->pd);
        if (snap->ctx) ibv_close_device(snap->ctx);
        if (snap->pool.base_addr) free(snap->pool.base_addr);
        free(snap);
    }
}


pdb_rdma_conn_ctx* pdb_rdma_create_conn(pdb_rdma_snapshot_ctx* snap) {
    if (!snap || !snap->ctx || !snap->pd) return NULL;

    pdb_rdma_conn_ctx* conn = (pdb_rdma_conn_ctx*)malloc(sizeof(pdb_rdma_conn_ctx));
    memset(conn, 0, sizeof(pdb_rdma_conn_ctx));
    
    conn->snap = snap;
    snap->ref_count++; 

    conn->cq = ibv_create_cq(snap->ctx, 1024, NULL, NULL, 0);
    if (!conn->cq) {
        pdb_log_error("ibv_create_cq error\n");
        goto cleanup;
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = conn->cq;
    qp_attr.recv_cq = conn->cq;
    qp_attr.qp_type = IBV_QPT_RC;  
    qp_attr.cap.max_send_wr  = 4096;
    qp_attr.cap.max_recv_wr  = 256;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    conn->qp = ibv_create_qp(snap->pd, &qp_attr);
    if (!conn->qp) {
        pdb_log_error("ibv_create_qp error\n");
        goto cleanup_cq;
    }

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state        = IBV_QPS_INIT;
    attr.port_num        = PDB_RDMA_PORT;
    attr.pkey_index      = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(conn->qp, &attr, flags) != 0){
        pdb_log_error("ibv_modify_qp error\n");
        goto cleanup_qp;
    } 

    conn->local_info.qpn = conn->qp->qp_num;
    conn->local_info.psn = 314159 + (rand() % 1000); 

    struct ibv_port_attr port_attr;
    ibv_query_port(snap->ctx, PDB_RDMA_PORT, &port_attr);
    conn->local_info.lid = port_attr.lid;
    ibv_query_gid(snap->ctx, PDB_RDMA_PORT, PDB_RDMA_GID_IDX, (union ibv_gid *)&conn->local_info.gid);

    pdb_log_info("[RDMA CONN] Slave Connection Context Created! QPN: %u\n", conn->local_info.qpn);
    return conn;

cleanup_qp: ibv_destroy_qp(conn->qp);
cleanup_cq: ibv_destroy_cq(conn->cq);
cleanup:    snap->ref_count--; free(conn); return NULL;
}


void pdb_rdma_destroy_conn(pdb_rdma_conn_ctx* conn) {
    if (!conn) return;
    pdb_log_info("[RDMA CONN] Tearing down connection to QPN %u\n", conn->local_info.qpn);
    
    if (conn->qp) ibv_destroy_qp(conn->qp);
    if (conn->cq) ibv_destroy_cq(conn->cq);
    
    if (conn->snap) {
        pdb_rdma_release_snapshot(conn->snap);
    }
    free(conn);
}

/**
 * **************************************************
 * ************   RDMA serialize   ******************
 * **************************************************
 */

int pdb_rdma_serialize(pdb_rdma_snapshot_ctx* rdma){
    char* buf;
    size_t buf_len;
    size_t offset = 0; 
    int ret;
    uint8_t eof_marker;

    if (rdma == NULL || rdma->pool.base_addr == NULL) {
        return PDB_RDMA_PARAM_ERROR; 
    }

    buf = (char*)rdma->pool.base_addr;
    buf_len = rdma->pool.total_size; 

    pdb_log_info("[MASTER] Starting global serialization for RDMA Heist...\n");

    ret = pdb_serialize_hash(buf, buf_len, &offset, &global_hash);
    if (ret != PDB_RDMA_OK) {
        pdb_log_error("[MASTER] Failed to serialize global_hash at offset: %zu\n", offset);
        return ret;
    }

    ret = pdb_serialize_rbtree(buf, buf_len, &offset, &global_rbtree);
    if (ret != PDB_RDMA_OK) {
        pdb_log_error("[MASTER] Failed to serialize global_rbtree at offset: %zu\n", offset);
        return ret;
    }

    ret = pdb_serialize_array(buf, buf_len, &offset, &global_array);
    if (ret != PDB_RDMA_OK) {
        pdb_log_error("[MASTER] Failed to serialize global_array at offset: %zu\n", offset);
        return ret;
    }

    eof_marker = PDB_OPCODE_EOF;
    if (offset + sizeof(uint8_t) > buf_len) {
        pdb_log_error("[MASTER] Buffer overflow while writing EOF marker!\n");
        return PDB_RDMA_PARAM_ERROR;
    }
    memcpy(buf + offset, &eof_marker, sizeof(uint8_t));
    offset += sizeof(uint8_t);

    *(rdma->pool.used_offset) = offset; 

    pdb_log_info("[MASTER] Serialization success. Exact Snapshot size: %zu bytes.\n", offset);
    return PDB_RDMA_OK;
}

/**
 * **************************************************
 * ************   RDMA deserialize   ****************
 * **************************************************
 */

int pdb_rdma_deserialize(pdb_rdma_snapshot_ctx* rdma){
    const char* buf;
    size_t buf_len;
    size_t offset = 0;
    uint8_t opcode;
    int is_eof = 0;
    int ret;

    if (rdma == NULL || rdma->pool.base_addr == NULL || rdma->pool.used_offset == 0) {
        pdb_log_error("Invalid RDMA context or empty memory pool.\n");
        return PDB_RDMA_PARAM_ERROR;
    }

    buf = (const char*)rdma->pool.base_addr;
    buf_len = *(rdma->pool.used_offset); 

    while (!is_eof && offset < buf_len) {
        
        if (offset + sizeof(uint8_t) > buf_len) {
            pdb_log_error("Unexpected end of stream while waiting for Opcode!\n");
            return PDB_RDMA_PARAM_ERROR;
        }
        memcpy(&opcode, buf + offset, sizeof(uint8_t));
        offset += sizeof(uint8_t);

        switch (opcode) {
            case PDB_OPCODE_HASH:
                // printf(" ↳ Routing to Hash Deserializer...\n");
                ret = pdb_deserialize_hash(buf, buf_len, &offset, &global_hash);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_RBTREE:
                // printf(" ↳ Routing to RBTree Deserializer...\n");
                ret = pdb_deserialize_rbtree(buf, buf_len, &offset, &global_rbtree);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_ARRAY:
                // printf(" ↳ Routing to Array Deserializer...\n");
                ret = pdb_deserialize_array(buf, buf_len, &offset, &global_array);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_EOF:
                pdb_log_info("[SLAVE] Reached EOF marker safely. Full synchronization complete!\n");
                is_eof = 1; 
                break;

            default:
                pdb_log_error("Corrupted snapshot! Unknown opcode (0x%02X) at offset: %zu\n", opcode, offset - 1);
                return PDB_RDMA_PARAM_ERROR;
        }
    }

    if (!is_eof) {
        pdb_log_error("Stream ended abruptly without EOF marker! Database state may be corrupted.\n");
        return PDB_RDMA_PARAM_ERROR;
    }

    return PDB_RDMA_OK;
}

/**
 * **************************************************
 * ************   slave RDMA read   *****************
 * **************************************************
 */

void execute_rdma_read_heist(pdb_rdma_conn_ctx* slave_conn, uint64_t remote_vaddr, uint32_t remote_rkey, size_t data_size) {
    if (!slave_conn || !slave_conn->snap || data_size == 0) return;

    struct timeval t_start, t_alloc, t_post, t_poll;
    gettimeofday(&t_start, NULL);

    void* local_buffer = pdb_rdma_pool_alloc(slave_conn->snap, data_size);
    if (!local_buffer) {
        pdb_log_error("Slave RDMA pool out of memory!\n");
        return;
    }
    gettimeofday(&t_alloc, NULL);

    struct ibv_send_wr wr[NUM_CHUNKS];
    struct ibv_sge sge[NUM_CHUNKS];
    struct ibv_send_wr *bad_wr = NULL;
    
    size_t chunk_size = data_size / NUM_CHUNKS;

    for (int i = 0; i < NUM_CHUNKS; i++) {
        size_t current_offset = i * chunk_size;
        size_t current_len = (i == NUM_CHUNKS - 1) ? (data_size - current_offset) : chunk_size;

        sge[i].addr = (uintptr_t)local_buffer + current_offset;
        sge[i].length = current_len;
        sge[i].lkey = slave_conn->snap->mr->lkey;

        memset(&wr[i], 0, sizeof(struct ibv_send_wr));
        wr[i].wr_id = i; 
        wr[i].opcode = IBV_WR_RDMA_READ;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;

        if (i == NUM_CHUNKS - 1) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
            wr[i].next = NULL; 
        } else {
            wr[i].send_flags = 0;
            wr[i].next = &wr[i+1];
        }

        wr[i].wr.rdma.remote_addr = remote_vaddr + current_offset;
        wr[i].wr.rdma.rkey = remote_rkey;
    }


    __builtin_ia32_sfence();
    int ret = ibv_post_send(slave_conn->qp, &wr[0], &bad_wr);
    if (ret != 0) {
        pdb_log_error("ibv_post_send failed: %s\n", strerror(ret));
        return;
    }
    gettimeofday(&t_post, NULL);

    struct ibv_wc wc;
    int num_comp = 0;
    while (num_comp == 0) {
        num_comp = ibv_poll_cq(slave_conn->cq, 1, &wc);
        __builtin_ia32_pause(); 
    }
    gettimeofday(&t_poll, NULL);

#if RDMA_TEST_REPORT 
    // test report
    if (wc.status != IBV_WC_SUCCESS) {
        printf("❌ [FATAL] RDMA READ Failed! Status: %s\n", ibv_wc_status_str(wc.status));
    } else {
        printf("\n--- 🔥 PATRONUS DB RDMA PERFORMANCE ---\n");
        printf("Mempool Alloc : %.3f ms\n", get_delta_ms(t_start, t_alloc));
        printf("WR Chaining   : %.3f ms\n", get_delta_ms(t_alloc, t_post));
        printf("CQ Polling    : %.3f ms (Hardware Transfer)\n", get_delta_ms(t_post, t_poll));
        printf("Total Payload : %zu bytes\n", data_size);
        printf("Effective BW  : %.2f MB/s\n", (data_size / 1024.0 / 1024.0) / (get_delta_ms(t_post, t_poll) / 1000.0));
        printf("---------------------------------------\n");
    }
#endif
}


int pdb_rdma_connect_qp(pdb_rdma_conn_ctx* conn, pdb_rdma_conn_info* remote_info) {
    if (!conn || !remote_info) return -1;
    struct ibv_qp_attr attr;
    int flags;

    // RTR 阶段
    memset(&attr, 0, sizeof(attr));
    attr.qp_state           = IBV_QPS_RTR;
    attr.path_mtu           = IBV_MTU_4096;      // 确保物理网卡 MTU 已设为 4200+
    attr.dest_qp_num        = remote_info->qpn;
    attr.rq_psn             = remote_info->psn;
    attr.max_dest_rd_atomic = RDMA_READ_DEPTH;   // 提升硬件响应深度
    attr.min_rnr_timer      = 12;
    attr.ah_attr.is_global  = 1;
    attr.ah_attr.dlid       = remote_info->lid;
    attr.ah_attr.port_num   = PDB_RDMA_PORT;
    attr.ah_attr.grh.sgid_index = PDB_RDMA_GID_IDX;
    attr.ah_attr.grh.hop_limit  = 64;
    memcpy(attr.ah_attr.grh.dgid.raw, remote_info->gid, 16);

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | 
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    ibv_modify_qp(conn->qp, &attr, flags);

    // RTS 阶段
    memset(&attr, 0, sizeof(attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.timeout       = 14;
    attr.retry_cnt     = 7;
    attr.rnr_retry     = 7;
    attr.sq_psn        = conn->local_info.psn;
    attr.max_rd_atomic = RDMA_READ_DEPTH;       // 提升硬件发起深度

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | 
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    return ibv_modify_qp(conn->qp, &attr, flags);
}

#define INTERNAL_CHUNKS 64
void execute_rdma_read_heist_chunk(pdb_rdma_conn_ctx* slave_conn, 
                                   uint64_t remote_base_vaddr, uint64_t chunk_offset, 
                                   uint32_t remote_rkey, size_t chunk_size, 
                                   void* local_buf_addr) {
    if (!slave_conn || !slave_conn->snap || chunk_size == 0 || !local_buf_addr) return;

    struct timeval t_start, t_alloc, t_post, t_poll;
    uint64_t remote_addr_start = remote_base_vaddr + chunk_offset;

    struct ibv_send_wr wr[INTERNAL_CHUNKS];
    struct ibv_sge sge[INTERNAL_CHUNKS];
    struct ibv_send_wr *bad_wr = NULL;

    size_t per_wr_size = chunk_size / INTERNAL_CHUNKS;

    for (int i = 0; i < INTERNAL_CHUNKS; i++) {
        size_t current_inner_offset = i * per_wr_size;
        size_t current_inner_len = (i == INTERNAL_CHUNKS - 1) ? 
                                   (chunk_size - current_inner_offset) : per_wr_size;

        sge[i].addr = (uintptr_t)local_buf_addr + current_inner_offset;
        sge[i].length = current_inner_len;
        sge[i].lkey = slave_conn->snap->mr->lkey;

        memset(&wr[i], 0, sizeof(struct ibv_send_wr));
        wr[i].wr_id = (uint64_t)i; 
        wr[i].opcode = IBV_WR_RDMA_READ;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;

        if (i == INTERNAL_CHUNKS - 1) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
            wr[i].next = NULL; 
        } else {
            wr[i].send_flags = 0;
            wr[i].next = &wr[i+1];
        }

        wr[i].wr.rdma.remote_addr = remote_addr_start + current_inner_offset;
        wr[i].wr.rdma.rkey = remote_rkey;
    }

    __builtin_ia32_sfence();

    int ret = ibv_post_send(slave_conn->qp, &wr[0], &bad_wr);
    if (ret != 0) {
        int err_code = (ret < 0) ? -ret : ret; 
        pdb_log_error("[THREAD HEIST] ibv_post_send failed: %s (code: %d)\n", strerror(err_code), ret);
        return;
    }

    struct ibv_wc wc;
    int num_comp = 0;
    gettimeofday(&t_post, NULL);
    while (num_comp == 0) {
        num_comp = ibv_poll_cq(slave_conn->cq, 1, &wc);
        __builtin_ia32_pause();
    }
    gettimeofday(&t_poll, NULL);
    pdb_log_info("rdma test result: %.3f\n", get_delta_ms(t_post, t_poll));
    if (wc.status != IBV_WC_SUCCESS) {
        pdb_log_error("❌ [THREAD FATAL] RDMA READ Failed! Status: %s (%d)\n", 
                      ibv_wc_status_str(wc.status), wc.status);
    }
}

void* _heist_worker(void* arg) {
    heist_thread_arg_t* t_arg = (heist_thread_arg_t*)arg;
    
    execute_rdma_read_heist_chunk(t_arg->conn, 
                                  t_arg->remote_vaddr, 
                                  t_arg->offset, 
                                  t_arg->rkey, 
                                  t_arg->size, 
                                  t_arg->local_buf);
    return NULL;
}