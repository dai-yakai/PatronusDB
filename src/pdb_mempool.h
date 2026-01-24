#ifndef __MEMPOOL_H__
#define __MEMPOOL_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <malloc.h>

#define PRINT 1

#define MP_ALIGNMENT             8
#define MP_PAGE_SIZE             4096
#define MP_MAX_ALLOC_FROM_POOL   (MP_PAGE_SIZE - 1)

// 定义 8 种常用的规格：8, 16, 32, 64, 128, 256, 512, 1024
#define MP_FREE_LIST_NUM         8

#define mp_align_ptr(p, alignment) (void *)((((size_t)p)+(alignment-1)) & ~(alignment-1))

struct mp_pool_s;

struct mp_data_s{
    unsigned char* last;
    unsigned char* end;
    struct mp_pool_s* next;

    size_t failed;
};

struct mp_large_s {
    struct mp_large_s* next;

    void* alloc;
};


struct mp_pool_s {
    struct mp_data_s d;
    size_t max;                     // 小块内存和大块内存分配的界限

    struct mp_pool_s* current;
    struct mp_large_s* large;
};

struct mp_pool_s* kv_mp_create_pool(size_t size);
void kv_mp_destory_pool(struct mp_pool_s* pool);
void* kv_mp_alloc(struct mp_pool_s* pool, size_t size);
void kv_mp_free(struct mp_pool_s* pool, void* p);
void kv_mp_reset_pool(struct mp_pool_s* pool);


#endif