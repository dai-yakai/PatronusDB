#ifndef __PDB_HASH_H__
#define __PDB_HASH_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "pdb_malloc.h"
#include "pdb_value.h"

#define MAX_KEY_LEN					128
#define MAX_VALUE_LEN				512
#define INIT_TABLE_SIZE				2000003
#define HASH_BUFFER_LENGTH 			1024*1024*32	// 32M
#define HASH_EXPAND_FACTOR			0.75
#define HASH_SHRINK_FACTOR			0.1

#define ENABLE_KEY_POINTER	1


typedef struct hashnode_s {
	char* key;
	pdb_value* value;

	struct hashnode_s* next;
} hashnode_t;


typedef struct hashtable_s {
	hashnode_t** nodes; 

	int max_slots;
	int count;
} hashtable_t;

typedef struct hashtable_s pdb_hash_t;

extern pdb_hash_t global_hash;

int pdb_hash_create(pdb_hash_t *hash);
pdb_hash_t* pdb_hash_create2();
void pdb_hash_destory(pdb_hash_t *hash);
int pdb_hash_set(hashtable_t *hash, char *key, pdb_value* value);
pdb_value* pdb_hash_get(pdb_hash_t *hash, char *key);
pdb_value* pdb_hash_get2(pdb_hash_t *hash, char *key, int* success);
int pdb_hash_mod(pdb_hash_t *hash, char *key, pdb_value* value);
int pdb_hash_del(pdb_hash_t *hash, char *key);
int pdb_hash_exist(pdb_hash_t *hash, char *key);
int pdb_hash_exist2(pdb_hash_t *hash, char *key, int* success);
int pdb_hash_mset(pdb_hash_t* arr, char** tokens, int count);

#endif