#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "pdb_core.h"
#include "pdb_malloc.h"

#define MAX_LEVEL 6
#define value_type char*

typedef struct Node Node;
typedef struct SkipList SkipList;

extern struct SkipList global_skiplist;

Node* createNode(int level, value_type key, value_type value);
SkipList* createSkipTable();
int randomLevel();
int pdb_skiptable_insert(SkipList* skipList, value_type key, value_type value);
void display(SkipList* skipList);
value_type pdb_skiptable_search(SkipList* skipList, value_type key);
int pdb_skiptable_delete(SkipList* skipList, value_type key);
int pdb_skiptable_mod(SkipList* SkipList, value_type key, value_type new_value);
int pdb_skiptable_mset(SkipList* SkipList, char** tokens, int count);
