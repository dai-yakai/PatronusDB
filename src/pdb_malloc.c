#include "pdb_malloc.h"

size_t used_memory = 0;;


#if ENABLE_MEMPOOL
static struct mp_pool_s* global_mempool = NULL;

void pdb_mem_init(size_t size){
    if (global_mempool == NULL){
        global_mempool = pdb_mp_create_freelist_pool(size);
        return;
    }
    
    pdb_log_info("pdb_mem_init: global_mempool has been initialized\n");
}

void pdb_mem_destroy(){
    if (global_mempool == NULL){
        printf("pdb_mem_destroy: global_mempool is NULL\n");
        return;
    }

    pdb_mp_destory_freelist_pool(global_mempool);
    global_mempool = NULL;
}
#endif


void* pdb_malloc(size_t size){
    assert(size > 0);
    void* p;

#if ENABLE_MEMPOOL
    // Use memory pool to allocate memory
    p = pdb_mp_freelist_alloc(global_mempool, size);
    return p;

#elif ENABLE_JEMALLOC
    // Use jemalloc to allocate memory
    p = malloc(size);
    return p;

#else
    // Use system call--malloc to allocate memory
    if (used_memory >= global_conf.max_memory){
        pdb_log_info("used memory exceeds max_memory(: %d)\n", global_conf.max_memory);
        // return NULL; 
    }

    p = malloc(size + sizeof(size_t));
    if (p == NULL){
        return NULL;
    }
    *((size_t*)p) = size;
    used_memory += size;
    return (char*)p + sizeof(size_t);
#endif
    
}

void* pdb_realloc(void* ptr, size_t size){
    assert(ptr != NULL && size > 0);

    void* p;
#if ENABLE_MEMPOOL
    p = pdb_mp_freelist_realloc(global_mempool, ptr, size);
    return p;

#elif ENABLE_JEMALLOC
    p = realloc(ptr, size);
    return p;

#else
    p = (char*)ptr - sizeof(size_t);
    size_t old_size = *((size_t*)p);
    used_memory -= old_size;
    used_memory += size;
    p = realloc(p, size);
    return (char*)p + sizeof(size_t);
#endif
}

void pdb_free(void* ptr, size_t size){
    assert(ptr != NULL && size > 0);

#if ENABLE_MEMPOOL
    // Use memory pool to release memory
    pdb_mp_freelist_free(global_mempool, ptr);

#elif ENABLE_JEMALLOC
    // Use jemalloc to release memory
    free(ptr);

#else
    // Use free() to release memory
    void* p = (char*)ptr - sizeof(size_t);
    size_t ptr_size = *(size_t*)(p);
    used_memory -= ptr_size;
    
    free(p);
#endif
}