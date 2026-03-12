#ifndef __PDB_BITMAP__
#define __PDB_BITMAP__

#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>

#include "pdb_sds.h"

#define BITOP_AND 0
#define BITOP_OR  1
#define BITOP_XOR 2
#define BITOP_NOT 3

#define PDB_INIT_BTIMAP_LENGTH      1024   

struct pdb_bitmap{
    char* key;
    pdb_sds data;

    char parent_key[1024];
};

struct pdb_bitmap* pdb_bitmap_create(char* key, pdb_sds s);
int pdb_bitmap_set_(struct pdb_bitmap* bitmap, uint64_t offset, int val, int* old_value);
int pdb_bitmap_set(pdb_sds* b, uint64_t offset, int val, int* old_value);
int pdb_bitmap_get(pdb_sds b, uint64_t offset);
int pdb_bitmap_count(pdb_sds b);
void pdb_bitmap_bitop(int opt, pdb_sds* dest, pdb_sds src1, pdb_sds src2);
int64_t pdb_bitmap_pos(pdb_sds b, int val, uint64_t start);
void pdb_test_bitmap();

#endif