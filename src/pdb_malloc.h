#ifndef __PDB_MALLOC_H__
#define __PDB_MALLOC_H__

#include <stdlib.h>
#include <malloc.h>

#include "pdb_mempool_freelist.h"

#define ENABLE_MEMPOOL      1
#define ENABLE_JEMALLOC     0

#if ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

void* kvs_malloc(size_t size);
void* kvs_realloc(void* ptr, size_t size);
void kvs_mem_init(size_t size);
void kvs_mem_destroy();
void* kvs_malloc(size_t size);
void kvs_free(void* ptr, size_t size);

#endif