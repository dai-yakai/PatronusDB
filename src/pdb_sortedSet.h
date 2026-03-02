#ifndef __PDB_SORTEDSET__
#define __PDB_SORTEDSET__

#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "pdb_malloc.h"
#include "pdb_hash.h"

#define PDB_MAX_SKIPLIST_LEVEL 32
#define PDB_ZSKIPLIST_P 0.25

typedef char* KEY_TYPE;
typedef double VALUE_TYPE;

/*
    Skiplist
*/
struct pdb_skiplistNode{
    KEY_TYPE   key;
    VALUE_TYPE value;

    struct pdb_skiplistNode* backward;

    struct pdb_skiplistLevel{
        struct pdb_skiplistNode* forward;
        unsigned int span;
    } level[];
};

struct pdb_skiplist{
    struct pdb_skiplistNode* tail;
    struct pdb_skiplistNode* head;

    unsigned long count;
    int level;
};


/**
 * sorted_set
 */
struct pdb_sorted_set{
    pdb_hash_t* set;
    struct pdb_skiplist* list;
};

struct pdb_skiplist* pdb_create_skiplist();
int pdb_skiplist_add(struct pdb_skiplist* sl, KEY_TYPE key, VALUE_TYPE value);
int pdb_skiplist_delete(struct pdb_skiplist* sl, KEY_TYPE key, VALUE_TYPE value);
int pdb_skiplist_search(struct pdb_skiplist* sl, KEY_TYPE key);
VALUE_TYPE* pdb_skiplist_get(struct pdb_skiplist* sl, KEY_TYPE key);
void pdb_skiplist_destroy(struct pdb_skiplist* sl);

/*
 * sorted_set
 */

struct pdb_sorted_set* pdb_create_sortedSet();
void pdb_destroy_sortedSet(struct pdb_sorted_set*);
int pdb_sortedSet_delete(struct pdb_sorted_set* Sset, char* key);
int pdb_sortedSet_add(struct pdb_sorted_set* Sset, char* key, double d_new_value);
int pdb_sortedSet_incre(struct pdb_sorted_set* Sset, char* key, double increment);
double pdb_sortedSet_search(struct pdb_sorted_set* Sset, char* key, int* success);
unsigned long pdb_sortedSet_rank(struct pdb_sorted_set* Sset, char* key, int* success);
unsigned long pdb_sortedSet_revrank(struct pdb_sorted_set* Sset, char* key, int* success);
char** pdb_sortedSet_get_revrange(struct pdb_sorted_set* Sset, long start, long end);
char** pdb_sortedSet_topK(struct pdb_sorted_set* Sset, unsigned long k);
void test_performance();
void test_correctness();
#endif