#include "pdb_mempool.h"

void mp_free(void* ptr){
    free(ptr);
}

void* mp_malloc(size_t size){
    return malloc(size);
}

void* mp_memalign(size_t alignment, size_t size){
    return memalign(alignment, size);
}

struct mp_pool_s* pdb_mp_create_pool(size_t size){
    struct mp_pool_s* p;

    p = (struct mp_pool_s*)mp_memalign(MP_ALIGNMENT, size);
    if (p == NULL){
#if PRINT
        printf("mp_create_pool: mp_memalign failed\n");
#endif
        return NULL;
    }
    memset(p, 0, size);

    p->d.last = (unsigned char*)p + sizeof(struct mp_pool_s);
    p->d.end = (unsigned char*)p + size;
    p->d.next = NULL;
    p->d.failed = 0;

    size = size - sizeof(struct mp_pool_s);

    p->current = p;
    p->max = (size > MP_MAX_ALLOC_FROM_POOL) ? MP_MAX_ALLOC_FROM_POOL : size;

    return p;
}

static void* mp_alloc_block(struct mp_pool_s* pool, size_t size){
    struct mp_pool_s* new, * p;
    unsigned char* ptr;

    size_t psize = (size_t)(pool->d.end - (unsigned char*)pool);
    ptr = mp_memalign(MP_ALIGNMENT, psize);
    if (ptr == NULL){
#if PRINT
        printf("mp_alloc_block: mp_memalign failed\n");
#endif 
    }
    memset(ptr, 0, psize);

    new = (struct mp_pool_s*)ptr;
    new->d.end = ptr + psize;
    new->d.failed = 0;
    new->d.next = NULL;

    ptr = ptr + sizeof(struct mp_data_s);
    ptr = mp_align_ptr(ptr, MP_ALIGNMENT);

    new->d.last = ptr + size;
    
    for (p = pool->current; p->d.next; p = p->d.next){
        p->d.failed++;
        if (p->d.failed > 4){
            pool->current = p->d.next;
        }
    }

    p->d.next = new;

    return (void*)ptr;
}

static void* mp_alloc_small(struct mp_pool_s* pool, size_t size, int alignment){
    struct mp_pool_s* p;
    unsigned char* ptr;

    p = pool->current;
    
    do
    {
        ptr = p->d.last;
        // 内存对齐
        if (alignment){
            ptr = mp_align_ptr(ptr, MP_ALIGNMENT);
        }

        if (p->d.end - p->d.last > size){
            p->d.last = ptr + size;

            return ptr;
        }

        p = p->d.next;
    } while (p);

    return mp_alloc_block(pool, size);
}

static void* mp_alloc_large(struct mp_pool_s* pool, size_t size){
    void* ptr; 
    int n;
    struct mp_large_s* large;

    ptr = mp_malloc(size);
    if (ptr == NULL){
#if PRINT
        printf("mp_alloc_large: malloc failed\n");
#endif       
        return NULL;
    }
    memset(ptr, 0, size);

    n = 0;   
    for (large = pool->large; large; large = large->next){
        n++;
        if (large->alloc == NULL){
            large->alloc = ptr;

            return ptr;
        }
        if (n > 3){
            break;
        }
    }

    // 头插法
    large = (struct mp_large_s*)mp_alloc_small(pool, sizeof(struct mp_large_s), 1);
    if (large == NULL){
        mp_free(ptr);
#if PRINT
        printf("mp_alloc_large: mp_alloc_small failed\n");
#endif  
        return NULL;
    }
    memset(large, 0, sizeof(struct mp_large_s));

    large->next = pool->large;
    pool->large = large;

    return ptr;
}

void* pdb_mp_alloc(struct mp_pool_s* pool, size_t size){
    if (size < pool->max){
        // 1: 进行内存对齐   0: 不进行内存对齐
        return mp_alloc_small(pool, size, 1);
    }

    return mp_alloc_large(pool, size);
}

// 仅供释放大块内存
void pdb_mp_free(struct mp_pool_s* pool, void* p){
    struct mp_large_s* large;

    for (large = pool->large; large; large = large->next){
        if (large->alloc == p){
            mp_free(p);
            large->alloc = NULL;
        }
    }
}

void pdb_mp_reset_pool(struct mp_pool_s* pool){
    struct mp_large_s* large;
    struct mp_pool_s* p;

    // 释放大块内存，遍历大块内存链表
    // 大块内存的头放在了小块内存中，不需要单独释放
    for (large = pool->large; large; large = large->next){
        if (large->alloc != NULL){
            mp_free(large->alloc);
            large->alloc = NULL;
        }
    }

    // 释放小块内存，仅做last指针的移动
    p = pool;
    p->d.last = (unsigned char*)p + sizeof(struct mp_pool_s);
    p->d.failed = 0;

    for (p = p->d.next; p; p = p->d.next){
        p->d.last = (unsigned char*)p + sizeof(struct mp_data_s);
        p->d.failed = 0;
    }

    pool->large = NULL;
    pool->current = pool;
}

void pdb_mp_destory_pool(struct mp_pool_s* pool){
    struct mp_large_s* large;
    struct mp_pool_s* p;

    for (large = pool->large; large; large = large->next){
        if (large->alloc){
            mp_free(large->alloc);
        }
    }

    for (p = pool; p; p->d.next){
        mp_free(p);
    }
}