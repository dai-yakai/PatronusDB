#ifndef __PDB_VALUE__
#define __PDB_VALUE__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "pdb_malloc.h"


#define PDB_VALUE_TYPE_DEFAULT      0
#define PDB_VALUE_TYPE_STRING       1
#define PDB_VALUE_TYPE_JSON         2
#define PDB_VALUE_TYPE_INT          3
#define PDB_VALUE_TYPE_BITMAP       4
#define PDB_VALUE_TYPE_NULL         5
#define PDB_VALUE_TYPE_DOUBLE       6
#define PDB_VALUE_TYPE_SET          7
#define PDB_VALUE_TYPE_SORTEDSET    8


typedef struct pdb_value{
    uint8_t type : 4;
    uint32_t lru;
    uint64_t expire_time;
    uint32_t ref_count;

    void* ptr;

} pdb_value;


void pdb_decre_value(pdb_value* value);
pdb_value* pdb_create_value(char* value, int type, ...);
char* pdb_parse_value_to_string(pdb_value* value);
void pdb_destroy_value(pdb_value* value);
void pdb_incre_value(pdb_value* value);
#endif