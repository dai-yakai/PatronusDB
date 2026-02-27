#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "pdb_core.h"
#include "pdb_malloc.h"
#include "pdb_value.h"

#define MAX_LEVEL 6
#define value_type pdb_value*

typedef struct Node Node;
typedef struct SkipList SkipList;

extern struct SkipList global_skiplist;

Node* createNode(int level, char* key, value_type value);
SkipList* createSkipTable();
int randomLevel();
int pdb_skiptable_insert(SkipList* skipList, char* key, value_type value);
void display(SkipList* skipList);
value_type pdb_skiptable_search(SkipList* skipList, char* key);
int pdb_skiptable_delete(SkipList* skipList, char* key);
int pdb_skiptable_mod(SkipList* SkipList, char* key, value_type new_value);
int pdb_skiptable_mset(SkipList* SkipList, char** tokens, int count);
