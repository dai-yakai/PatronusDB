#ifndef __PDB_LIST__
#define __PDB_LIST__

/*
    双向链表
*/

typedef struct listNode_s{
    struct listNode_s* prev;
    struct listNode_s* next;
    void* data;
} listNode;

typedef struct list_s{
    listNode* head;
    listNode* tail;
    int len;
    
} list;

#endif