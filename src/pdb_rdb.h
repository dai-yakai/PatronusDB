#ifndef __PDB_RDB__
#define __PDB_RDB__

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "pdb_array.h"
#include "pdb_rbtree.h"
#include "pdb_hash.h"
#include "pdb_core.h"
#include "pdb_log.h"
#include "pdb_parse_protocol.h"


int pdb_rdb_load(const char* file);
int pdb_rdb_array_dump(pdb_array_t* arr, const char* file);
int pdb_rdb_hash_dump(pdb_hash_t* h, const char* file);
int pdb_rdb_rbtree_dump(pdb_rbtree_t* rbtree, const char* file);
int pdb_rdb_dump(const char* file);

#endif