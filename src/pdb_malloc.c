#include "pdb_malloc.h"

size_t used_memory = 0;;
pdb_memory_manager* global_memory_manager = NULL;

#if ENABLE_MEMPOOL
struct mp_pool_s* global_mempool = NULL;

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
    if (size <= 0){
        return NULL;
    }
    void* p;


#if ENABLE_MEMPOOL
    // Use memory pool to allocate memory
    p = pdb_mp_freelist_alloc(global_mempool, size);
    global_memory_manager->used_memory += size;
    return p;

#elif ENABLE_JEMALLOC
    // Use jemalloc to allocate memory
    p = malloc(size);
    global_memory_manager->used_memory += malloc_usable_size(p);
    return p;

#else
    // Use system call--malloc to allocate memory

    p = malloc(size + sizeof(size_t));
    if (p == NULL){
        return NULL;
    }
    *((size_t*)p) = size;
    global_memory_manager->used_memory += size + sizeof(size_t);
    return (char*)p + sizeof(size_t);
#endif
    
}

void* pdb_realloc(void* ptr, size_t size){
    assert(ptr != NULL && size > 0);

    void* p;

// mempool
#if ENABLE_MEMPOOL
    char* result = (char*)p - MP_HEADER_SIZE;
    size_t old_alloc = *(size_t*)result;
    global_memory_manager->used_memory -= old_alloc;

    p = pdb_mp_freelist_realloc(global_mempool, ptr, size);
    if (p == NULL)  global_memory_manager->used_memory += size;

    return p;

// jemalloc
#elif ENABLE_JEMALLOC
    size_t old_alloc = malloc_usable_size(ptr);
    p = realloc(ptr, size);
    size_t new_alloc = malloc_usable_size(p);
    global_memory_manager->used_memory += new_alloc - old_alloc;
    return p;

// malloc
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
    assert(ptr != NULL);

#if ENABLE_MEMPOOL
    // Use memory pool to release memory
    global_memory_manager->used_memory -= size;
    pdb_mp_freelist_free(global_mempool, ptr);

#elif ENABLE_JEMALLOC
    // Use jemalloc to release memory
    global_memory_manager->used_memory -= malloc_usable_size(ptr);
    free(ptr);

#else
    // Use free() to release memory
    void* p = (char*)ptr - sizeof(size_t);
    size_t ptr_size = *(size_t*)(p);
    global_memory_manager->used_memory -= size + sizeof(size_t);
    
    free(p);
#endif
}

size_t pdb_get_current_rss(void) {
    long rss_pages = 0;
    
    FILE *fp = fopen("/proc/self/statm", "r");
    if (fp == NULL) {
        return 0;
    }
    
    if (fscanf(fp, "%*s %ld", &rss_pages) != 1) {
        rss_pages = 0; 
    }
    fclose(fp);

    return (size_t)rss_pages * sysconf(_SC_PAGESIZE);
}

void pdb_init_global_memory_manager(){
    global_memory_manager = malloc(sizeof(pdb_memory_manager));
    global_memory_manager->used_memory = sizeof(pdb_memory_manager); 
    global_memory_manager->used_memory_rss = pdb_get_current_rss();
}