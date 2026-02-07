#ifndef __PDB_LIST__
#define __PDB_LIST__

#include "pdb_malloc.h"
/*
    双向链表
*/

typedef struct listNode_s{
    struct listNode_s* prev;
    struct listNode_s* next;
    void* val;
} listNode;

typedef struct list_s{
    listNode* head;
    listNode* tail;
    int len;
    
} list;

void pdb_list_insert(list l, void* data);
int pdb_list_delete(list l, void* data);

#endif