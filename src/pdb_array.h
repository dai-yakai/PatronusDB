#ifndef __PDB_ARRAY_H__
#define __PDB_ARRAY_H__

#include <string.h>
#include "pdb_malloc.h"

#define KVS_ARRAY_SIZE          1024*1024*16
#define ENABLE_PRINT_ARRAY      0

typedef struct kvs_array_item_s kvs_array_item_t;
typedef struct kvs_array_s kvs_array_t;

extern kvs_array_t global_array;

int kvs_array_create(kvs_array_t* inst);
void kvs_array_destroy(kvs_array_t* inst);
int kvs_array_set(kvs_array_t* inst, char* key, char* value);
char* kvs_array_get(kvs_array_t* inst, char* key);
int kvs_array_del(kvs_array_t* inst, char* key);
int kvs_array_mod(kvs_array_t* inst, char* key, char* value);
int kvs_array_exist(kvs_array_t* inst, char* key);
void kvs_print_array(kvs_array_t* inst);
void kvs_array_dump(kvs_array_t *arr, const char *file);
int kvs_array_load(kvs_array_t *arr, const char *file);
int kvs_array_mset(kvs_array_t* arr, char** tokens, int count);

#endif