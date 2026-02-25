#ifndef __PDB_SET__
#define __PDB_SET__

#include <errno.h>

#include "pdb_intset.h"
#include "pdb_hash.h"

#define PDB_SET_ENCODING_INTSET     1
#define PDB_SET_ENCODING_HASHTABLE  2
#define PDB_MAX_INTSET_LENGTH       512

struct pdb_set{
    uint8_t flag;
    void* ptr;
};



struct pdb_set* pdb_set_create();
int pdb_set_add(struct pdb_set* set, const char* value);
int pdb_set_search(struct pdb_set* set, const char* value);
int pdb_set_delete(struct pdb_set* set, const char* value);
void pdb_set_destroy(struct pdb_set* set);
void pdb_set_test();

#endif