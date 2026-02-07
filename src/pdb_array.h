#ifndef __PDB_ARRAY_H__
#define __PDB_ARRAY_H__

#include <string.h>
#include <assert.h>

#include "pdb_malloc.h"
#include "pdb_log.h"
#include "pdb_core.h"

#define PDB_ARRAY_SIZE          1024*16
#define ENABLE_PRINT_ARRAY      0

typedef struct pdb_array_item_s pdb_array_item_t;
typedef struct pdb_array_s pdb_array_t;

extern pdb_array_t global_array;

int pdb_array_create(pdb_array_t* inst);
void pdb_array_destroy(pdb_array_t* inst);
int pdb_array_set(pdb_array_t* inst, char* key, char* value);
char* pdb_array_get(pdb_array_t* inst, char* key);
int pdb_array_del(pdb_array_t* inst, char* key);
int pdb_array_mod(pdb_array_t* inst, char* key, char* value);
int pdb_array_exist(pdb_array_t* inst, char* key);
void pdb_print_array(pdb_array_t* inst);
void pdb_array_dump(pdb_array_t *arr, const char *file);
int pdb_array_load(pdb_array_t *arr, const char *file);
int pdb_array_mset(pdb_array_t* arr, char** tokens, int count);

#endif