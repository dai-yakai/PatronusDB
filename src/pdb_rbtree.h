#ifndef __PDB_RBTREE_H__
#define __PDB_RBTREE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pdb_malloc.h"

#define RED				    1
#define BLACK 			    2

#define ENABLE_KEY_CHAR     1
#define ENABLE_PRINT_RB     0

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else 
typedef int KEY_TYPE;
#endif

#define CONSUME_CRLF(fp) { fgetc(fp); fgetc(fp); }

typedef struct _rbtree_node rbtree_node;
typedef struct _rbtree rbtree;

typedef rbtree pdb_rbtree_t;

struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;
	KEY_TYPE key;
	void *value;
} ;

struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
};

rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x);
rbtree_node *rbtree_maxi(rbtree *T, rbtree_node *x);
rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x);
void rbtree_left_rotate(rbtree *T, rbtree_node *x);
void rbtree_right_rotate(rbtree *T, rbtree_node *y);
void rbtree_insert_fixup(rbtree *T, rbtree_node *z);
void rbtree_insert(rbtree *T, rbtree_node *z);
void rbtree_delete_fixup(rbtree *T, rbtree_node *x);
rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z);
rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key);
void rbtree_traversal(rbtree *T, rbtree_node *node);


int pdb_rbtree_create(pdb_rbtree_t* inst);
void pdb_rbtree_destroy(pdb_rbtree_t* inst);
int pdb_rbtree_set(pdb_rbtree_t* inst, char* key, char* value);
int pdb_rbtree_mset(pdb_rbtree_t *arr, char** tokens, int count);
char* pdb_rbtree_get(pdb_rbtree_t* inst, char* key);
int pdb_rbtree_del(pdb_rbtree_t* inst, char* key);
int pdb_rbtree_mod(pdb_rbtree_t* inst, char* key, char* value);
int pdb_rbtree_exist(pdb_rbtree_t* inst, char* key);
void pdb_print_rbtree(pdb_rbtree_t* inst);

void pdb_rbtree_dump(pdb_rbtree_t *tree, const char *file);
int pdb_rbtree_load(pdb_rbtree_t *arr, const char *file);

extern pdb_rbtree_t global_rbtree;

#endif