#ifndef __PDB_VALUE__
#define __PDB_VALUE__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pdb_malloc.h"

#define PDB_VALUE_TYPE_STRING   1
#define PDB_VALUE_TYPE_JSON     2
#define PDB_VALUE_TYPE_INT      3

/**
 * 
 * 
 */
typedef struct pdb_value{
    uint8_t type : 3;
    uint32_t lru;
    uint64_t expire_time;
    uint32_t ref_count;

    void* ptr;

} pdb_value;

int pdb_get_value_type(const char* cmd);
void pdb_decre_value(pdb_value* value);
pdb_value* pdb_create_value(char* type_cmd, char* value);
void* pdb_parse_string_to_value(char* value, int type);
int pdb_parse_value_to_string(pdb_value* value, char* buf);
void pdb_destroy_value(pdb_value* value);
void pdb_incre_value(pdb_value* value);
#endif