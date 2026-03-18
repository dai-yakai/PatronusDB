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
#include "pdb_ebpf.h"

extern void pdb_ebpf_poll();
pdb_rdma_snapshot_ctx* global_master_snapshot = NULL;

// Performance Configuration Constants
// Divide large data into 16 chunks for parallel fetching
#define NUM_CHUNKS 16        
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
    if (posix_memalign(&ptr, page_size, size) != 0) return NULL;
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
 * ************   RDMA init   ***********************
 * **************************************************
 */
pdb_rdma_snapshot_ctx* pdb_rdma_create_snapshot(const char* dev_name, size_t pool_size) {
    if (pool_size == 0) return NULL;

    pdb_rdma_snapshot_ctx* snap = (pdb_rdma_snapshot_ctx*)malloc(sizeof(pdb_rdma_snapshot_ctx));
    memset(snap, 0, sizeof(pdb_rdma_snapshot_ctx));
    snap->ref_count = 1; // master owner
    snap->is_process_down = 1;

    snap->pool.total_size = pool_size;
    snap->pool.used_offset = pdb_malloc(sizeof(int));
    *(snap->pool.used_offset) = 0;
    snap->pool.base_addr = (char*)_alloc_aligned_memory(pool_size);
    if (!snap->pool.base_addr) { free(snap); return NULL; }

    struct ibv_device **dev_list;
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

    //printf("[DEBUG-MR] Base:%p | MR_Addr:%p | Lkey:%u | Rkey:%u\n", 
        // snap->pool.base_addr, snap->mr->addr, snap->mr->lkey, snap->mr->rkey);

    // printf("🔥 [RDMA SNAPSHOT] Global Snapshot Created! Size: %zu Bytes, RefCount: %d\n", pool_size, snap->ref_count);
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
    // printf("[RDMA SNAPSHOT] RefCount dropped to %d\n", snap->ref_count);

    if (snap->ref_count <= 0) {
        if (snap->mr) ibv_dereg_mr(snap->mr);
        if (snap->pd) ibv_dealloc_pd(snap->pd);
        if (snap->ctx) ibv_close_device(snap->ctx);
        if (snap->pool.base_addr) free(snap->pool.base_addr);
        free(snap);
        // printf("🗑️ [RDMA SNAPSHOT] 0 Slaves left. 512MB Physical Memory Cleaned Up!\n");
    }
}


pdb_rdma_conn_ctx* pdb_rdma_create_conn(pdb_rdma_snapshot_ctx* snap) {
    if (!snap || !snap->ctx || !snap->pd) return NULL;

    pdb_rdma_conn_ctx* conn = (pdb_rdma_conn_ctx*)malloc(sizeof(pdb_rdma_conn_ctx));
    memset(conn, 0, sizeof(pdb_rdma_conn_ctx));
    
    conn->snap = snap;
    snap->ref_count++; 

    conn->cq = ibv_create_cq(snap->ctx, 1024, NULL, NULL, 0);
    if (!conn->cq) goto cleanup;

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = conn->cq;
    qp_attr.recv_cq = conn->cq;
    qp_attr.qp_type = IBV_QPT_RC;  
    qp_attr.cap.max_send_wr  = 256;
    qp_attr.cap.max_recv_wr  = 256;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    conn->qp = ibv_create_qp(snap->pd, &qp_attr);
    if (!conn->qp) goto cleanup_cq;

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state        = IBV_QPS_INIT;
    attr.port_num        = PDB_RDMA_PORT;
    attr.pkey_index      = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(conn->qp, &attr, flags) != 0) goto cleanup_qp;

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
        printf("❌ [SLAVE] Invalid RDMA context or empty memory pool.\n");
        return PDB_RDMA_PARAM_ERROR;
    }

    buf = (const char*)rdma->pool.base_addr;
    buf_len = *(rdma->pool.used_offset); 

    printf("📦 [SLAVE] Hardware Heist complete. Rebuilding %zu bytes to memory database...\n", buf_len);

    while (!is_eof && offset < buf_len) {
        
        if (offset + sizeof(uint8_t) > buf_len) {
            printf("❌ [FATAL] Unexpected end of stream while waiting for Opcode!\n");
            return PDB_RDMA_PARAM_ERROR;
        }
        memcpy(&opcode, buf + offset, sizeof(uint8_t));
        offset += sizeof(uint8_t);

        switch (opcode) {
            case PDB_OPCODE_HASH:
                printf(" ↳ Routing to Hash Deserializer...\n");
                ret = pdb_deserialize_hash(buf, buf_len, &offset, &global_hash);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_RBTREE:
                printf(" ↳ Routing to RBTree Deserializer...\n");
                ret = pdb_deserialize_rbtree(buf, buf_len, &offset, &global_rbtree);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_ARRAY:
                printf(" ↳ Routing to Array Deserializer...\n");
                ret = pdb_deserialize_array(buf, buf_len, &offset, &global_array);
                if (ret != PDB_RDMA_OK) return ret;
                break;

            case PDB_OPCODE_EOF:
                printf("🏁 [SLAVE] Reached EOF marker safely. Full synchronization complete!\n");
                is_eof = 1; 
                break;

            default:
                printf("❌ [FATAL] Corrupted snapshot! Unknown opcode (0x%02X) at offset: %zu\n", opcode, offset - 1);
                return PDB_RDMA_PARAM_ERROR;
        }
    }

    if (!is_eof) {
        printf("❌ [FATAL] Stream ended abruptly without EOF marker! Database state may be corrupted.\n");
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

    // 1. 内存分配 (O(1) 指针移动，极快)
    void* local_buffer = pdb_rdma_pool_alloc(slave_conn->snap, data_size);
    if (!local_buffer) {
        printf("❌ [FATAL] Slave RDMA pool out of memory!\n");
        return;
    }
    gettimeofday(&t_alloc, NULL);

    // 2. 构造 WR 链表
    // 我们准备 NUM_CHUNKS 个 WR 和 SGE，让网卡流水线作业
    struct ibv_send_wr wr[NUM_CHUNKS];
    struct ibv_sge sge[NUM_CHUNKS];
    struct ibv_send_wr *bad_wr = NULL;
    
    size_t chunk_size = data_size / NUM_CHUNKS;
    
    printf("[SLAVE RDMA] Chunking %zu bytes into %d segments...\n", data_size, NUM_CHUNKS);

    for (int i = 0; i < NUM_CHUNKS; i++) {
        size_t current_offset = i * chunk_size;
        size_t current_len = (i == NUM_CHUNKS - 1) ? (data_size - current_offset) : chunk_size;

        // 设置当前分片的本地存储位置 (SGE)
        sge[i].addr = (uintptr_t)local_buffer + current_offset;
        sge[i].length = current_len;
        sge[i].lkey = slave_conn->snap->mr->lkey;

        // 设置当前分片的远程读取位置 (WR)
        memset(&wr[i], 0, sizeof(struct ibv_send_wr));
        wr[i].wr_id = i; 
        wr[i].opcode = IBV_WR_RDMA_READ;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;

        // 只有最后一个 WR 设置 SIGNALED，这样 CQ 只会产生一个完成事件
        if (i == NUM_CHUNKS - 1) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
            wr[i].next = NULL; 
        } else {
            wr[i].send_flags = 0;
            wr[i].next = &wr[i+1]; // 串联到下一个
        }

        wr[i].wr.rdma.remote_addr = remote_vaddr + current_offset;
        wr[i].wr.rdma.rkey = remote_rkey;
    }


    __builtin_ia32_sfence();
    // 3. 一次性提交所有请求
    if (ibv_post_send(slave_conn->qp, &wr[0], &bad_wr) != 0) {
        printf("❌ [FATAL] ibv_post_send failed!\n");
        return;
    }
    gettimeofday(&t_post, NULL);

    // 4. 高效轮询 CQ
    struct ibv_wc wc;
    int num_comp = 0;
    while (num_comp == 0) {
        num_comp = ibv_poll_cq(slave_conn->cq, 1, &wc);
        // 使用 pause 指令降低 CPU 功耗并减少对内存总线的干扰，提升 DMA 效率
        __builtin_ia32_pause(); 
    }
    gettimeofday(&t_poll, NULL);

    // 5. 性能报告
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
}

extern pdb_rdma_snapshot_ctx* incre_slave_snap;
void _execute_rdma_read_heist(pdb_rdma_conn_ctx* slave_conn, uint64_t remote_vaddr, uint32_t remote_rkey, size_t data_size) {
    if (!slave_conn || !slave_conn->snap || data_size == 0) return;

    struct timeval t_start, t_alloc, t_post, t_poll;
    gettimeofday(&t_start, NULL);

    // 1. 内存分配 (O(1) 指针移动，极快)
    void* local_buffer = incre_slave_snap->pool.base_addr;
    if (!local_buffer) {
        printf("❌ [FATAL] Slave RDMA pool out of memory!\n");
        return;
    }
    gettimeofday(&t_alloc, NULL);

    // 2. 构造 WR 链表
    // 我们准备 NUM_CHUNKS 个 WR 和 SGE，让网卡流水线作业
    struct ibv_send_wr wr[NUM_CHUNKS];
    struct ibv_sge sge[NUM_CHUNKS];
    struct ibv_send_wr *bad_wr = NULL;
    
    size_t chunk_size = data_size / NUM_CHUNKS;
    
    printf("[SLAVE RDMA] Chunking %zu bytes into %d segments...\n", data_size, NUM_CHUNKS);

    for (int i = 0; i < NUM_CHUNKS; i++) {
        size_t current_offset = i * chunk_size;
        size_t current_len = (i == NUM_CHUNKS - 1) ? (data_size - current_offset) : chunk_size;

        // 设置当前分片的本地存储位置 (SGE)
        sge[i].addr = (uintptr_t)local_buffer + current_offset;
        sge[i].length = current_len;
        sge[i].lkey = slave_conn->snap->mr->lkey;

        // 设置当前分片的远程读取位置 (WR)
        memset(&wr[i], 0, sizeof(struct ibv_send_wr));
        wr[i].wr_id = i; 
        wr[i].opcode = IBV_WR_RDMA_READ;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;

        // 只有最后一个 WR 设置 SIGNALED，这样 CQ 只会产生一个完成事件
        if (i == NUM_CHUNKS - 1) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
            wr[i].next = NULL; 
        } else {
            wr[i].send_flags = 0;
            wr[i].next = &wr[i+1]; // 串联到下一个
        }

        wr[i].wr.rdma.remote_addr = remote_vaddr + current_offset;
        wr[i].wr.rdma.rkey = remote_rkey;
    }


    __builtin_ia32_sfence();
    // 3. 一次性提交所有请求
    if (ibv_post_send(slave_conn->qp, &wr[0], &bad_wr) != 0) {
        printf("❌ [FATAL] ibv_post_send failed!\n");
        return;
    }
    gettimeofday(&t_post, NULL);

    // 4. 高效轮询 CQ
    struct ibv_wc wc;
    int num_comp = 0;
    while (num_comp == 0) {
        num_comp = ibv_poll_cq(slave_conn->cq, 1, &wc);
        // 使用 pause 指令降低 CPU 功耗并减少对内存总线的干扰，提升 DMA 效率
        __builtin_ia32_pause(); 
    }
    gettimeofday(&t_poll, NULL);

    // 5. 性能报告
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
}

/**
 * 必须配合更新的连接逻辑：提升 Read 深度
 */
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


/***** increment syn ************/
extern pdb_rdma_snapshot_ctx* incre_master_snap;
extern pdb_rdma_conn_ctx* incre_slave_conn;
extern int pdb_incre_serialize(void* dataStructrue, const char* key, char* buf, size_t buf_len, size_t* offset, uint8_t opcode);

/**
 * move ebpf incre to rmda mempool
 */
int pdb_rdma_incremental_append(void* dataStructure, const char* key, uint8_t opcode) {
    // pdb_log_debug("KEY: %s\n", key);
    if (incre_master_snap == NULL)  return -1;
    // master rdma mempool para
    char* buf = incre_master_snap->pool.base_addr;
    size_t total_len = incre_master_snap->pool.total_size;
    uint64_t* offset = (uint64_t*)(incre_master_snap->pool.base_addr + (PDB_RDMA_INCRE_BUFFER_LEN - 16));
    uint64_t* flag = (uint64_t*)(incre_master_snap->pool.base_addr + (PDB_RDMA_INCRE_BUFFER_LEN - 8));
    
    int ret = pdb_incre_serialize(dataStructure, key, buf, total_len, offset, opcode);

    if (ret < 0){
        // pdb_log_debug("incre serialize(key: %s) error\n", (char*)key);
    }

    return ret;
}


void pdb_rdma_post_recv_monitor(pdb_rdma_conn_ctx* slave_conn) {
    struct ibv_recv_wr wr = {0}, *bad_wr = NULL;
    wr.wr_id = 999;
    // 不需要 SGE，因为数据已经被 RDMA Write 单边写进内存了，这里只为了接 IMM 信号
    if (ibv_post_recv(slave_conn->qp, &wr, &bad_wr) != 0) {
        printf("❌ [SLAVE] Failed to post receive monitor!\n");
    }
}


extern pdb_rdma_snapshot_ctx* incre_slave_snap;
void pdb_write_master_flag(unsigned long long master_incre_vaddr, unsigned long master_incre_rkey, uint64_t val) {
    static uint64_t zero_buffer[2] = {0, 0}; 
    
    struct ibv_sge sge = {0};
    struct ibv_send_wr wr = {0}, *bad_wr = NULL;

    if (val == 0) {
        uint64_t* val = (uint64_t*)((char*)incre_slave_snap->pool.base_addr + (4 * 1024 * 1024 - 16));
        val[0] = 0;
        val[1] = 0;
        sge.addr   = (uintptr_t)val;
        sge.length = 16; 
        sge.lkey   = incre_slave_snap->mr->lkey; 
        wr.wr.rdma.remote_addr = master_incre_vaddr + (4 * 1024 * 1024 - 16);
    } else {
        uint64_t* signal_val = incre_slave_snap->pool.base_addr + (4 * 1024 * 1024 - 32);
        *signal_val = val;
        sge.addr   = (uintptr_t)signal_val;
        sge.length = 8;
        sge.lkey   = incre_slave_snap->mr->lkey;
        wr.wr.rdma.remote_addr = master_incre_vaddr + (4 * 1024 * 1024 - 8);
    }

    wr.wr_id       = 999;
    wr.opcode      = IBV_WR_RDMA_WRITE;
    wr.sg_list     = &sge;
    wr.num_sge     = 1;
    wr.send_flags  = IBV_SEND_SIGNALED;
    wr.wr.rdma.rkey = master_incre_rkey;

    if (ibv_post_send(incre_slave_conn->qp, &wr, &bad_wr) != 0) {
        printf("--- [DEBUG EINVAL] ---\n");
        printf("Opcode: %d | WR_ID: %lu\n", wr.opcode, wr.wr_id);
        printf("Remote Addr: %llu (End: %llu)\n", 
            (unsigned long long)wr.wr.rdma.remote_addr,
            (unsigned long long)wr.wr.rdma.remote_addr + sge.length);
        printf("Local Addr:  %llu | Length: %u | Lkey: %u\n", 
            (unsigned long long)sge.addr, sge.length, sge.lkey);
        printf("QP State: %d (2=INIT, 3=RTR, 4=RTS, 5=SQE, 6=ERR)\n", incre_slave_conn->qp->state);
        
        if (wr.wr.rdma.remote_addr % 8 != 0) printf("❌ ERROR: Remote Addr NOT 8-byte aligned!\n");
    }
    
    struct ibv_wc wc;
    int n;
    while ((n = ibv_poll_cq(incre_slave_conn->cq, 1, &wc)) == 0) {
        __builtin_ia32_pause();
    }

    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        pdb_log_error("RDMA WRITE flag failed: %s\n", ibv_wc_status_str(wc.status));
    }
}

void pdb_slave_pull_cycle(unsigned long long master_incre_vaddr, unsigned long master_incre_rkey) {
    uint64_t* flag2 = (uint64_t*)(incre_slave_snap->pool.base_addr + (PDB_RDMA_INCRE_BUFFER_LEN - 24));
    char* buf = incre_slave_snap->pool.base_addr;
    size_t buf_len = incre_slave_snap->pool.total_size;
    size_t* current_offset = (uint64_t*)(incre_slave_snap->pool.base_addr + (PDB_RDMA_INCRE_BUFFER_LEN - 16));
    int offset = 0;
    do{
        _execute_rdma_read_heist(incre_slave_conn, master_incre_vaddr, master_incre_rkey, 4 * 1024 * 1024);
        pdb_write_master_flag(master_incre_vaddr, master_incre_rkey, 1);
    }while(*flag2 == 1);

    if (*current_offset != 0){
        pdb_incre_deserialize(buf, *current_offset, &offset);
    }
    
    pdb_write_master_flag(master_incre_vaddr, master_incre_rkey, 0);
    // pdb_log_debug("eeeeeeeeeeeeeeeeeeeeeeee\n");
    
    // volatile uint64_t* local_flag = (uint64_t*)(incre_slave_snap->pool.base_addr + (4 * 1024 * 1024 - 8));

    // if (*local_flag == 1) {
    //     printf("🚀 [SLAVE] Master has new incremental data!\n");
    //     char* buf = incre_slave_snap->pool.base_addr;
    //     size_t* current_offset = incre_slave_snap->pool.used_offset;
    //     size_t buf_len = incre_slave_snap->pool.total_size;
    //     pdb_incre_deserialize(buf, current_offset, buf_len);

        

    //     *local_flag = 0;
    // }
}


extern pdb_rdma_snapshot_ctx* incre_slave_snap;
extern pdb_rdma_conn_ctx* incre_slave_conn;
extern long long last_pull_time_ms;
extern int is_incre_channel_active;
extern long long get_now_ms();

void pdb_increment_syn() {
// slave node:
    if (global_conf.is_slave && incre_slave_conn && incre_slave_conn){
        long long now = get_now_ms();
        if (now - last_pull_time_ms >= 10000) { // 每 100ms 拉取一次
            pdb_slave_pull_cycle(incre_slave_conn->vaddr_str, incre_slave_conn->rkey_str);
            
            last_pull_time_ms = now;
        }
    }

// master node:
    if (incre_master_snap == NULL) return;

    pdb_ebpf_poll();
    

    // int cur_fd = global_conn_info_list_head;
    // struct conn_info* c = conn_list[cur_fd];
    // if (c && is_rdma_nic_busy(c)) {
    //     // 如果网卡还在发，不要 Poll，防止覆盖正在发送的数据
    //     return; 
    // }
    //  // 🚩 这里的 poll 会不断增加 used_offset

    // // 2. 检查发送条件
    // cur_fd = global_conn_info_list_head;
    // while(cur_fd != -1) {
    //     c = conn_list[cur_fd];
    //     if (c && c->is_syncing_incremental) {
    //         size_t current_offset = *(incre_master_snap->pool.used_offset);
            
    //         // 只有有数据时才去询问 Slave
    //         if (current_offset > 0) {
    //             int slave_busy = is_slave_processed(c); // 这里建议改成非阻塞
    //             if (!slave_busy) {
    //                 // 🚩 只有 Slave 闲了，才发
    //                 _pdb_rdma_direct_flush(c, incre_master_snap->pool.base_addr, current_offset, 0);
                    
    //                 // 🚩 发完后，我们不能立即置 0，因为网卡还没搬完
    //                 // 下一轮循环 check 时，is_rdma_nic_busy(c) 会拦截，直到搬完
    //             }
    //         } else {
    //             // 如果当前没数据积累，且网卡已发完，此时重置 offset 是安全的
    //             if (!is_rdma_nic_busy(c)) {
    //                 *(incre_master_snap->pool.used_offset) = 0;
    //             }
    //         }
    //     }
    //     cur_fd = conn_list[cur_fd]->next_fd;
    // }
}