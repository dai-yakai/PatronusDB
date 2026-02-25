#ifndef __PDB_INTSET__
#define __PDB_INTSET__

#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>

#include "pdb_malloc.h"

#define PDB_INTSET_ENCODING_INT16 (sizeof(uint16_t))
#define PDB_INTSET_ENCODING_INT32 (sizeof(uint32_t))
#define PDB_INTSET_ENCODING_INT64 (sizeof(uint64_t))

struct pdb_intset
{
    uint32_t flag;
    uint32_t len;
    int8_t data[];
};


struct pdb_intset* pdb_intset_create();
struct pdb_intset* pdb_intset_add(struct pdb_intset* intset, int64_t value, int* success);
int pdb_intset_search(struct pdb_intset* intset, int64_t value, uint32_t* pos);
struct pdb_intset* pdb_intset_delete(struct pdb_intset* intset, int64_t value, int* success);

struct pdb_intset* pdb_intset_intersect(struct pdb_intset* s1, struct pdb_intset* s2);
struct pdb_intset* pdb_intset_union(struct pdb_intset* s1, struct pdb_intset* s2);
struct pdb_intset* pdb_intset_difference(struct pdb_intset* s1, struct pdb_intset* s2);
void pdb_intset_test();
void pdb_intset_destroy(struct pdb_intset* intset);
int64_t _pdb_intset_get(struct pdb_intset* intset, uint32_t pos);
void _pdb_intset_set(struct pdb_intset* intset, uint32_t pos, int64_t value);

#endif