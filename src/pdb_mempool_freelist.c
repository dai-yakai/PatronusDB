#include "pdb_mempool_freelist.h"

/* 向系统申请内存并记录，用于 destroy 时清理 */
static void* _mp_malloc_tracked(struct mp_pool_s* pool, size_t size) {
    size_t total_size = size + sizeof(struct mp_chunk_record_s);
    
    char* p = (char*)malloc(total_size);
    if (!p) return NULL;

    // 头部作为记录节点
    struct mp_chunk_record_s* record = (struct mp_chunk_record_s*)p;
    record->ptr = p;
    record->next = pool->chunk_head;
    pool->chunk_head = record;

    return p + sizeof(struct mp_chunk_record_s);
}

/* 从内存池中切分内存 (SGI chunk_alloc) */
static char* _mp_chunk_alloc(struct mp_pool_s* pool, size_t size, int* nobjs) {
    char* result;
    size_t total_bytes = size * (*nobjs);
    size_t bytes_left = pool->end_free - pool->start_free;

    if (bytes_left >= total_bytes) {
        /* 内存池够用 */
        result = pool->start_free;
        pool->start_free += total_bytes;
        return result;
    } else if (bytes_left >= size) {
        /* 内存池不够 total_bytes，但至少能提供一个以上 */
        *nobjs = (int)(bytes_left / size);
        total_bytes = size * (*nobjs);
        result = pool->start_free;
        pool->start_free += total_bytes;
        return result;
    } else {
        /* 内存池连一个都不够，需要从 OS 重新申请 */
        size_t bytes_to_get = 2 * total_bytes + MP_ROUND_UP(pool->heap_size >> 4);

        /* 试着把内存池剩下的零头（碎片）配给合适的 free list */
        if (bytes_left > 0) {
            union mp_obj_u** my_free_list = \
                &(pool->free_list[MP_FREELIST_INDEX(bytes_left)]);
            ((union mp_obj_u*)pool->start_free)->free_list_link = *my_free_list;
            *my_free_list = (union mp_obj_u*)pool->start_free;
        }

        /* 向 OS 申请新内存块 (Chunk) */
        pool->start_free = (char*)_mp_malloc_tracked(pool, bytes_to_get);
        if (!pool->start_free) {
            /* 申请失败，尝试从更大的 free list 借点 */
            int i;
            for (i = size; i <= MP_MAX_BYTES; i += MP_ALIGN) {
                union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(i)]);
                union mp_obj_u* p = *my_free_list;
                if (p != 0) {
                    *my_free_list = p->free_list_link;
                    pool->start_free = (char*)p;
                    pool->end_free = pool->start_free + i;
                    return _mp_chunk_alloc(pool, size, nobjs); // 递归调用，修正 nobjs
                }
            }
            pool->end_free = 0; // 彻底没了
            return NULL;
        }
        
        pool->heap_size += bytes_to_get;
        pool->end_free = pool->start_free + bytes_to_get;
        return _mp_chunk_alloc(pool, size, nobjs); // 递归调用
    }
}

/* 填充 Free List (SGI refill) */
static void* _mp_refill(struct mp_pool_s* pool, size_t n) {
    int nobjs = 20; /* 默认一次取20个块 */
    
    /* 尝试从内存池切分 nobjs 个块，nobjs 可能会被修改为实际获取的数量 */
    char* chunk = _mp_chunk_alloc(pool, n, &nobjs);
    
    if (!chunk) return NULL;

    union mp_obj_u** my_free_list;
    union mp_obj_u* result;
    union mp_obj_u* current_obj;
    union mp_obj_u* next_obj;
    int i;

    /* 如果只获得了一个，直接返回给用户，不进 free list */
    if (1 == nobjs) return chunk;

    my_free_list = &(pool->free_list[MP_FREELIST_INDEX(n)]);

    /* 第一个块返回给用户 */
    result = (union mp_obj_u*)chunk;
    
    /* 剩下的块串起来放入 free list */
    *my_free_list = next_obj = (union mp_obj_u*)(chunk + n);
    for (i = 1; ; i++) {
        current_obj = next_obj;
        next_obj = (union mp_obj_u*)((char*)next_obj + n);
        if (nobjs - 1 == i) {
            current_obj->free_list_link = NULL;
            break;
        } else {
            current_obj->free_list_link = next_obj;
        }
    }
    return result;
}



struct mp_pool_s* kv_mp_create_freelist_pool(size_t size) {
    struct mp_pool_s* pool = (struct mp_pool_s*)malloc(sizeof(struct mp_pool_s));
    if (!pool) return NULL;

    memset(pool, 0, sizeof(struct mp_pool_s));
    return pool;
}

void kv_mp_destory_freelist_pool(struct mp_pool_s* pool) {
    if (!pool) return;

    struct mp_chunk_record_s* cur = pool->chunk_head;
    while (cur) {
        struct mp_chunk_record_s* next = cur->next;
        if (cur->ptr) free(cur->ptr);
        free(cur);
        cur = next;
    }

    free(pool);
}

void* kv_mp_freelist_alloc(struct mp_pool_s* pool, size_t size) {
    if (!pool) return NULL;
    if (size == 0) return NULL;

    /* * 需要存储 size，因为 free 接口没有 size 参数。
     * 实际申请大小 = 用户大小 + 头部大小(size_t) 
     */
    size_t real_size = size + MP_HEADER_SIZE;
    
    /* 如果是大块内存，直接 malloc，不走内存池，但依然加头部以便 free 时识别 */
    if (real_size > (size_t)MP_MAX_BYTES) {
        void* p = malloc(real_size);
        if (!p) return NULL;
        *(size_t*)p = real_size; // 写入大小
        return (char*)p + MP_HEADER_SIZE; // 返回跳过头部的指针
    }

    /* 小块内存，走 SGI 逻辑 */
    /* 寻找合适的 free list */
    size_t aligned_size = MP_ROUND_UP(real_size);
    union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(aligned_size)]);
    union mp_obj_u* result = *my_free_list;

    if (result == NULL) {
        /* 没库存了，进货 */
        void* r = _mp_refill(pool, aligned_size);
        if (!r) return NULL;
        *(size_t*)r = aligned_size; // 写入对齐后的大小（这就是这个块归还时的规格）
        return (char*)r + MP_HEADER_SIZE;
    }

    /* 有库存，弹出一个 */
    *my_free_list = result->free_list_link;
    
    *(size_t*)result = aligned_size; // 写入大小
    return (char*)result + MP_HEADER_SIZE;
}

void* kv_mp_freelist_realloc(struct mp_pool_s* pool, void* ptr, size_t size){
    // TO DO

}

void kv_mp_freelist_free(struct mp_pool_s* pool, void* p) {
    if (!pool || !p) return;

    /* 倒退指针找到头部，读取大小 */
    char* real_ptr = (char*)p - MP_HEADER_SIZE;
    size_t size = *(size_t*)real_ptr;

    /* 如果是大块内存，直接 free */
    if (size > (size_t)MP_MAX_BYTES) {
        free(real_ptr);
        return;
    }

    /* 小块内存，归还给 free list */
    union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(size)]);
    union mp_obj_u* node = (union mp_obj_u*)real_ptr;

    /* 头插法回收 */
    node->free_list_link = *my_free_list;
    *my_free_list = node;
}

void kv_mp_reset_freelist_pool(struct mp_pool_s* pool) {
    if (!pool) return;

    /* * 最彻底的 reset 是释放所有持有的 chunk 并清空状态。
     * 虽然比较耗时，但能防止内存泄露并彻底重置。
     */
    struct mp_chunk_record_s* cur = pool->chunk_head;
    while (cur) {
        struct mp_chunk_record_s* next = cur->next;
        if (cur->ptr) free(cur->ptr);
        free(cur);
        cur = next;
    }

    /* 重置结构体状态 */
    memset(pool, 0, sizeof(struct mp_pool_s));
}
