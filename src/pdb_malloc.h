#ifndef __PDB_MALLOC_H__
#define __PDB_MALLOC_H__

#include <stdlib.h>
#include <malloc.h>

#include "pdb_mempool_freelist.h"
#include "pdb_log.h"

#define ENABLE_MEMPOOL      0
#define ENABLE_JEMALLOC     0

#if ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

void* pdb_malloc(size_t size);
void* pdb_realloc(void* ptr, size_t size);
void pdb_mem_init(size_t size);
void pdb_mem_destroy();
void pdb_free(void* ptr, size_t size);

#endif