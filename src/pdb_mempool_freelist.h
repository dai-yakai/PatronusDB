#ifndef __PDB_MEMPOOL_FREELIST_H__
#define __PDB_MEMPOOL_FREELIST_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "pdb_core.h"


#define MP_ALIGN 8
#define MP_MAX_BYTES 4096
#define MP_NFREELISTS (MP_MAX_BYTES / MP_ALIGN)     // 16个链表

// 向上取整到8的倍数
#define MP_ROUND_UP(bytes) (((bytes) + MP_ALIGN - 1) & ~(MP_ALIGN - 1))

// 根据大小计算链表索引 (0-15)
#define MP_FREELIST_INDEX(bytes) (((bytes) + MP_ALIGN - 1) / MP_ALIGN - 1)

// 头部大小，用于记录分配长度
#define MP_HEADER_SIZE (sizeof(size_t))

// Free List 头节点
union mp_obj_u {
    union mp_obj_u* free_list_link;
    char client_data[1];
};

// 用于记录向系统申请的大块内存，以便 destroy 时释放
struct mp_chunk_record_s {
    struct mp_chunk_record_s* next;
    void* ptr;
};

struct mp_pool_s {
    union mp_obj_u* free_list[MP_NFREELISTS]; // 16个自由链表
    
    char* start_free;  // 内存池当前可用起始地址
    char* end_free;    // 内存池当前可用结束地址
    size_t heap_size;  // 累计申请堆大小
    
    struct mp_chunk_record_s* chunk_head; // 记录所有从OS申请的块
};

struct mp_pool_s* pdb_mp_create_freelist_pool(size_t size);
void pdb_mp_destory_freelist_pool(struct mp_pool_s* pool);
void* pdb_mp_freelist_alloc(struct mp_pool_s* pool, size_t size);
void* pdb_mp_freelist_realloc(struct mp_pool_s* pool, void* ptr, size_t size);
void pdb_mp_freelist_free(struct mp_pool_s* pool, void* p);
void pdb_mp_reset_freelist_pool(struct mp_pool_s* pool);


#endif