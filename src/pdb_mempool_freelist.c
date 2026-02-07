#include "pdb_mempool_freelist.h"


static void* _mp_malloc_tracked(struct mp_pool_s* pool, size_t size) {
    size_t total_size = size + sizeof(struct mp_chunk_record_s);
    char* p = (char*)malloc(total_size);
    if (!p) return NULL;

    struct mp_chunk_record_s* record = (struct mp_chunk_record_s*)p;
    record->ptr = p;
    record->next = pool->chunk_head;
    pool->chunk_head = record;

    return p + sizeof(struct mp_chunk_record_s);
}


static char* _mp_chunk_alloc(struct mp_pool_s* pool, size_t size, int* nobjs) {
    char* result;
    size_t total_bytes = size * (*nobjs);
    size_t bytes_left = pool->end_free - pool->start_free;

    if (bytes_left >= total_bytes) {
        result = pool->start_free;
        pool->start_free += total_bytes;
        return result;
    } else if (bytes_left >= size) {
        *nobjs = (int)(bytes_left / size);
        total_bytes = size * (*nobjs);
        result = pool->start_free;
        pool->start_free += total_bytes;
        return result;
    } else {
        size_t bytes_to_get = 2 * total_bytes + MP_ROUND_UP(pool->heap_size >> 4);
        
        pool->start_free = (char*)_mp_malloc_tracked(pool, bytes_to_get);
        if (!pool->start_free) {
            int i;
            for (i = size; i <= MP_MAX_BYTES; i += MP_ALIGN) {
                union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(i)]);
                union mp_obj_u* p = *my_free_list;
                if (p != 0) {
                    *my_free_list = p->free_list_link;
                    pool->start_free = (char*)p;
                    pool->end_free = pool->start_free + i;
                    return _mp_chunk_alloc(pool, size, nobjs);
                }
            }
            pool->end_free = 0;
            return NULL;
        }
        
        pool->heap_size += bytes_to_get;
        pool->end_free = pool->start_free + bytes_to_get;
        return _mp_chunk_alloc(pool, size, nobjs);
    }
}


static void* _mp_refill(struct mp_pool_s* pool, size_t n) {
    int nobjs = 20;
    char* chunk = _mp_chunk_alloc(pool, n, &nobjs);
    if (!chunk) return NULL;

    if (1 == nobjs) return chunk;

    union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(n)]);
    union mp_obj_u* result = (union mp_obj_u*)chunk;
    union mp_obj_u* next_obj = (union mp_obj_u*)(chunk + n);
    union mp_obj_u* current_obj;

    *my_free_list = next_obj;
    
    int i;
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


struct mp_pool_s* pdb_mp_create_freelist_pool(size_t size) {
    struct mp_pool_s* pool = (struct mp_pool_s*)malloc(sizeof(struct mp_pool_s));
    if (!pool) return NULL;
    memset(pool, 0, sizeof(struct mp_pool_s));
    return pool;
}


void pdb_mp_destory_freelist_pool(struct mp_pool_s* pool) {
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


void* pdb_mp_freelist_alloc(struct mp_pool_s* pool, size_t size) {
    if (!pool || size == 0) return NULL;

    size_t real_size = size + MP_HEADER_SIZE;
    
    if (real_size > (size_t)MP_MAX_BYTES) {
        void* p = malloc(real_size);
        if (!p) return NULL;
        *(size_t*)p = real_size;
        return (char*)p + MP_HEADER_SIZE;
    }

    size_t aligned_size = MP_ROUND_UP(real_size);
    
    if (aligned_size < sizeof(union mp_obj_u)) {
        aligned_size = sizeof(union mp_obj_u);
    }

    union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(aligned_size)]);
    union mp_obj_u* result = *my_free_list;

    if (result == NULL) {
        void* r = _mp_refill(pool, aligned_size);
        if (!r) return NULL;
        *(size_t*)r = aligned_size; 
        return (char*)r + MP_HEADER_SIZE;
    }

    union mp_obj_u* next_node = result->free_list_link; 
    *my_free_list = next_node;
    
    *(size_t*)result = aligned_size; 
    return (char*)result + MP_HEADER_SIZE;
}


void pdb_mp_freelist_free(struct mp_pool_s* pool, void* p) {
    if (!pool || !p) return;

    char* real_ptr = (char*)p - MP_HEADER_SIZE;
    size_t size = *(size_t*)real_ptr;

    if (size > (size_t)MP_MAX_BYTES) {
        free(real_ptr);
        return;
    }

    union mp_obj_u** my_free_list = &(pool->free_list[MP_FREELIST_INDEX(size)]);
    union mp_obj_u* node = (union mp_obj_u*)real_ptr;

    node->free_list_link = *my_free_list;
    *my_free_list = node;
}


void* pdb_mp_freelist_realloc(struct mp_pool_s* pool, void* ptr, size_t size){
    if (!ptr) return pdb_mp_freelist_alloc(pool, size);
    if (size == 0) {
        pdb_mp_freelist_free(pool, ptr);
        return NULL;
    }

    char* real_ptr = (char*)ptr - MP_HEADER_SIZE;
    size_t old_aligned_size = *(size_t*)real_ptr;
    size_t old_user_size = old_aligned_size - MP_HEADER_SIZE;

    if (size <= old_user_size) return ptr;

    void* new_ptr = pdb_mp_freelist_alloc(pool, size);
    if (!new_ptr) return NULL;

    memcpy(new_ptr, ptr, old_user_size);
    pdb_mp_freelist_free(pool, ptr);
    return new_ptr;
}


void pdb_mp_reset_freelist_pool(struct mp_pool_s* pool) {
    if (!pool) return;
    struct mp_chunk_record_s* cur = pool->chunk_head;
    while (cur) {
        struct mp_chunk_record_s* next = cur->next;
        if (cur->ptr) free(cur->ptr);
        free(cur);
        cur = next;
    }
    memset(pool, 0, sizeof(struct mp_pool_s));
}