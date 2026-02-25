#ifndef __PDB_LIST__
#define __PDB_LIST__

#include "pdb_malloc.h"
/*
    双向链表
*/

typedef struct pdb_listNode_s{
    struct pdb_listNode_s* prev;
    struct pdb_listNode_s* next;
    void* val;
} pdb_listNode;

typedef struct pdb_list_s{
    pdb_listNode* head;
    pdb_listNode* tail;
    
} pdb_list;

void pdb_list_insert(pdb_list* l, void* data);
int pdb_list_delete(pdb_list* l, void* data);
int pdb_list_delete_node(pdb_list* l, struct pdb_listNode_s* node);
pdb_list* pdb_get_NULL_list();
int pdb_is_list_NULL(pdb_list* l);

#endif