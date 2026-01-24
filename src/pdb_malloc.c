#include "pdb_malloc.h"


#if ENABLE_MEMPOOL
static struct mp_pool_s * global_mempool = NULL;

void kvs_mem_init(size_t size){
    if (global_mempool == NULL){
        global_mempool = kv_mp_create_freelist_pool(size);
        return;
    }
    
    printf("kvs_mem_init: global_mempool has been initialized\n");
}

void kvs_mem_destroy(){
    if (global_mempool == NULL){
        printf("kvs_mem_destroy: global_mempool is NULL\n");
        return;
    }

    kv_mp_destory_freelist_pool(global_mempool);
    global_mempool = NULL;
}
#endif




void* kvs_malloc(size_t size){
    if (size <= 0){
        printf("kvs_malloc: size < 0\n");
        return NULL;
    }
    void* p;

#if ENABLE_MEMPOOL
    p = kv_mp_freelist_alloc(global_mempool, size);
#elif ENABLE_JEMALLOC
    p = malloc(size);
#else
    p = malloc(size);
#endif

    // memset(p, 0, size);

    return p;
}

void* kvs_realloc(void* ptr, size_t size){
    if (size <= 0){
        printf("kvs_malloc: size < 0\n");
        return NULL;
    }
    void* p;

#if ENABLE_MEMPOOL
    p = kv_mp_freelist_realloc(global_mempool, ptr, size);
#elif ENABLE_JEMALLOC
    p = realloc(ptr, size);
#else
    p = realloc(ptr, size);
#endif

    // memset(p, 0, size);

    return p;
}

void kvs_free(void* ptr, size_t size){
    if (ptr == NULL){
        printf("kvs_free: ptr is NULL\n");
    }
#if ENABLE_MEMPOOL
    kv_mp_freelist_free(global_mempool, ptr);
#elif ENABLE_JEMALLOC
    free(ptr);
#else
    free(ptr);
#endif
}