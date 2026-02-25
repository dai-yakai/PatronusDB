#include "pdb_list.h"

int pdb_is_list_NULL(pdb_list* l){
    return l->head == NULL;
}

void pdb_list_insert(pdb_list* l, void* data){
    pdb_listNode* node = (pdb_listNode*)pdb_malloc(sizeof(pdb_listNode));
    node->val = data;
    node->prev = NULL;
    node->next = l->head;
    if (pdb_is_list_NULL(l)){
        l->head = node;
        l->tail = node;
        return;
    }
    l->head->prev = node;
    l->head = node;
}


int pdb_list_delete_node(pdb_list* l, struct pdb_listNode_s* node){
    if (pdb_is_list_NULL(l)){
        return PDB_OK;
    }

    if (node->next){
        node->next->prev = node->prev;        
    }else{
        l->tail = node->prev;
    }

    if (node->prev){
        node->prev->next = node->next;
    }else{
        l->head = node->next;
    }

    if (node->val){
        pdb_free(node->val, -1);
    }
    pdb_free(node, -1);
    
    return PDB_OK;
}

int pdb_list_delete(pdb_list* l, void* data){
    if (pdb_is_list_NULL(l)){
        return PDB_OK;
    }
    pdb_listNode* node = l->head;
    pdb_listNode* prev = node;
    while(node != NULL){
        if (!strcmp(node->val, data)){
            pdb_list_delete_node(l, node);
            break;
        }
        node = node->next;
    }

    return -1;
}

pdb_list* pdb_get_NULL_list(){
    pdb_list* l = (pdb_list*)pdb_malloc(sizeof(pdb_list));
    l->head = NULL;
    l->tail = NULL;

    return l;
}