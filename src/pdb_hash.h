#ifndef __PDB_HASH_H__
#define __PDB_HASH_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "pdb_malloc.h"

#define MAX_KEY_LEN	128
#define MAX_VALUE_LEN	512
#define MAX_TABLE_SIZE	2000003

#define HASH_BUFFER_LENGTH 1024*1024*32

#define ENABLE_KEY_POINTER	1


typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
	char *key;
	char *value;
#else
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
#endif
	struct hashnode_s *next;
	
} hashnode_t;


typedef struct hashtable_s {

	hashnode_t **nodes; //* change **, 

	int max_slots;
	int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;

extern kvs_hash_t global_hash;

int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);
int kvs_hash_set(hashtable_t *hash, char *key, char *value);
char * kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);
void kvs_hash_dump(kvs_hash_t *h, const char *file);
int kvs_hash_load(kvs_hash_t *arr, const char *file);
int kvs_hash_mset(kvs_hash_t* arr, char** tokens, int count);

#endif