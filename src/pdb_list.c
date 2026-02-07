#include "pdb_list.h"

void pdb_list_insert(list l, void* data){
    listNode* node = (listNode*)pdb_malloc(sizeof(listNode));
    node->val = data;

    node->prev = NULL;
    node->next = l.head;
    l.head->prev = node;
    l.head = node;
}

int pdb_list_delete(list l, void* data){
    listNode* node = l.head;
    listNode* prev = node;
    while(node != NULL){
        if (node->val == data){
            listNode* tmp = node;
            if (node == l.head){
                l.head = tmp->next;
            }
            prev->next = node->next;
            node->next->prev = prev;
            pdb_free(tmp, -1);
        }
        prev = node;
        node = node->next;
    }

    return -1;
}