#ifndef __PDB_SET__
#define __PDB_SET__

#include <errno.h>

#include "pdb_intset.h"
#include "pdb_hash.h"

#define PDB_SET_ENCODING_INTSET     1
#define PDB_SET_ENCODING_HASHTABLE  2
#define PDB_MAX_INTSET_LENGTH       512

typedef struct pdb_set{
    uint8_t flag;
    void* ptr;

    long count;
} pdb_set;



struct pdb_set* pdb_set_create();
int pdb_set_add(struct pdb_set* set, const char* value);
int pdb_set_search(struct pdb_set* set, const char* value);
long pdb_set_get_count(struct pdb_set* set);
int pdb_set_delete(struct pdb_set* set, const char* value);
char* pdb_set_random_pop(struct pdb_set* set);
struct pdb_set* pdb_set_inter(struct pdb_set* set1, struct pdb_set* set2);
struct pdb_set* pdb_set_union(struct pdb_set* set1, struct pdb_set* set2);
struct pdb_set* pdb_set_differ(struct pdb_set* set1, struct pdb_set* set2);
void pdb_set_destroy(struct pdb_set* set);
void pdb_set_test();

#endif