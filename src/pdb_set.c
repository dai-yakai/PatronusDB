#include "pdb_set.h"

static int _pdb_set_is_numeric(const char* str, int64_t* value){
    char* endptr;
    int64_t val = strtoll(str, &endptr, 10);

    if (errno == ERANGE || *endptr != '\0' || str == endptr){
        return 0;
    }
    *value = val;
    
    return 1;
}

static void _pdb_set_convert_to_hashtable(struct pdb_set* set){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE)    return;
    
    pdb_hash_t* set_hash = (pdb_hash_t*)pdb_malloc(sizeof(pdb_hash_t));
    pdb_hash_create(set_hash);

    struct pdb_intset* intset = (struct pdb_intset*)set->ptr;
    uint32_t len = intset->len;
    uint32_t i = 0;
    for (i = 0; i < len; i++){
        char* buf = pdb_malloc(sizeof(char) * 32);
        memset(buf, 0, 32);
        int64_t value = _pdb_intset_get(intset, i);
        snprintf(buf, sizeof(buf), "%" PRId64, value);

        pdb_value* value_ = pdb_create_value(NULL, PDB_VALUE_TYPE_NULL);
        pdb_hash_set(set_hash, buf, value_);
        pdb_decre_value(value_);
    }

    pdb_free(intset, -1);
    set->ptr = (void*)(set_hash);
    set->flag = PDB_SET_ENCODING_HASHTABLE;
}

struct pdb_set* pdb_set_create(){
    struct pdb_set* set = pdb_malloc(sizeof(struct pdb_set));
    set->flag = PDB_SET_ENCODING_INTSET;
    struct pdb_intset* intset = pdb_intset_create();
    set->ptr = intset;
    set->count = 0;

    return set;
}

int pdb_set_add(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        pdb_value* value_ = pdb_create_value(NULL, PDB_VALUE_TYPE_NULL);
        int ret = pdb_hash_set(set_hash, (char*)value, value_);
        if (ret == PDB_DATASTRUCTURE_OK)    set->count++;
        pdb_decre_value(value_);
        return ret;
    }

    int64_t val;
    int success = 0;
    if (_pdb_set_is_numeric(value, &val)){
        set->ptr = pdb_intset_add((struct pdb_intset*)set->ptr, val, &success);
        if (success){
            if (((struct pdb_intset*)set->ptr)->len > PDB_MAX_INTSET_LENGTH){
                _pdb_set_convert_to_hashtable(set);
            }
            set->count++;
            return PDB_DATASTRUCTURE_OK;
        }else{
            return PDB_DATASTRUCTURE_EXIST;
        }
    }else{
        _pdb_set_convert_to_hashtable(set);
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        pdb_value* value_ = pdb_create_value(NULL, PDB_VALUE_TYPE_NULL);
        int ret = pdb_hash_set(set_hash, (char*)value, value_);
        if (ret == PDB_DATASTRUCTURE_OK)    set->count++;
        pdb_decre_value(value_);
        return ret;
    }

    return PDB_DATASTRUCTURE_OK;
}

/*
* Return 1 if found; otherwise return 0.
*/
int pdb_set_search(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        return pdb_hash_exist((pdb_hash_t*)set->ptr, (char*)value);
    }

    uint32_t pos;
    int64_t val;
    if (_pdb_set_is_numeric(value, &val)){
        return pdb_intset_search(set->ptr, val, &pos);
    }

    return PDB_DATASTRUCTURE_OK;
}


struct pdb_set* pdb_set_inter(struct pdb_set* set1, struct pdb_set* set2){
    pdb_set* res = pdb_set_create();

    if (set1->flag == PDB_SET_ENCODING_INTSET && set2->flag == PDB_SET_ENCODING_INTSET){
        pdb_intset_destroy(res->ptr);
        res->ptr = pdb_intset_intersect(set1->ptr, set2->ptr);
        res->flag = PDB_SET_ENCODING_INTSET;
        res->count = ((struct pdb_intset*)res->ptr)->len;

        return res;
    }

    pdb_set* small_set = set1->count < set2->count ? set1 : set2;
    pdb_set* large_set = set1->count >= set2->count ? set1 : set2;

    if (small_set->flag == PDB_SET_ENCODING_INTSET){
        struct pdb_intset* intset = small_set->ptr;
        uint32_t i = 0;
        for (i = 0; i < intset->len; i++){
            int64_t val = _pdb_intset_get(intset, i);
            char buf[32];
            snprintf(buf, sizeof(buf), "%" PRId64, val);

            if (pdb_set_search(large_set, buf) == PDB_DATASTRUCTURE_EXIST){
                pdb_set_add(res, buf);
            }
        }
    }else{
        hashtable_t* hash = small_set->ptr;
        hashnode_t* node;
        int i = 0;
        for (i = 0; i < hash->max_slots; i++){
            node = hash->nodes[i];
            while(node != NULL){
                if (pdb_set_search(large_set, node->key) == PDB_DATASTRUCTURE_EXIST){
                    pdb_set_add(res, node->key);
                }
                node = node->next;
            }
        }
    }

    return res;
}


struct pdb_set* pdb_set_union(struct pdb_set* set1, struct pdb_set* set2){
    pdb_set* res = pdb_set_create();

    if (set1->flag == PDB_SET_ENCODING_INTSET && set2->flag == PDB_SET_ENCODING_INTSET){
        pdb_intset_destroy(res->ptr);
        res->ptr = pdb_intset_union(set1->ptr, set2->ptr);
        res->count = ((struct pdb_intset*)res->ptr)->len;
        res->flag = PDB_SET_ENCODING_INTSET;

        return res;
    }

    if (set1->flag == PDB_SET_ENCODING_INTSET){
        uint32_t i = 0;
        struct pdb_intset* intset = set1->ptr;
        for (i = 0; i < ((struct pdb_intset*)set1->ptr)->len; i++){
            char buf[32];
            snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
            pdb_set_add(res, buf);
        }
    } else if(set1->flag == PDB_SET_ENCODING_HASHTABLE){
        int i = 0;
        hashtable_t* hash = set1->ptr;
        for (i = 0; i < hash->max_slots; i++){
            hashnode_t* node = hash->nodes[i];
            while(node != NULL){
                pdb_set_add(res, node->key);
                node = node->next;
            }

        }
    }


    if (set2->flag == PDB_SET_ENCODING_INTSET){
        uint32_t i = 0;
        struct pdb_intset* intset = set2->ptr;
        for (i = 0; i < ((struct pdb_intset*)set2->ptr)->len; i++){
            char buf[32];
            snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
            pdb_set_add(res, buf);
        }
    } else if(set2->flag == PDB_SET_ENCODING_HASHTABLE){
        int i = 0;
        hashtable_t* hash = set2->ptr;
        for (i = 0; i < hash->max_slots; i++){
            hashnode_t* node = hash->nodes[i];
            while(node != NULL){
                pdb_set_add(res, node->key);
                node = node->next;
            }
        }
    }

    return res;
}


struct pdb_set* pdb_set_differ(struct pdb_set* set1, struct pdb_set* set2){
    pdb_set* res = pdb_set_create();

    if (set1->flag == PDB_SET_ENCODING_INTSET && set2->flag == PDB_SET_ENCODING_INTSET){
        pdb_intset_destroy(res->ptr);
        res->ptr = pdb_intset_difference(set1->ptr, set2->ptr);
        res->flag = PDB_SET_ENCODING_INTSET;
        res->count = ((struct pdb_intset*)res->ptr)->len;

        return res;
    }

    if (set1->flag == PDB_SET_ENCODING_INTSET){
        struct pdb_intset* intset = set1->ptr;
        uint32_t i = 0;
        for (i = 0; i <intset->len; i++){
            char buf[64];
            snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
            if (pdb_set_search(set2, buf) == PDB_DATASTRUCTURE_NOEXIST){
                pdb_set_add(res, buf);
            }
        }
    } else if(set1->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* hash = set1->ptr;
        int i = 0;
        for (i = 0; i < hash->max_slots; i++){
            hashnode_t* node = hash->nodes[i];
            while(node != NULL){
                if (pdb_set_search(set2, node->key) == PDB_DATASTRUCTURE_NOEXIST){
                    pdb_set_add(res, node->key);
                }
                node = node->next;
            }
        }
    }

    return res;
}


long pdb_set_get_count(struct pdb_set* set){
    return set->count;
}


char* pdb_set_random_pop(struct pdb_set* set){
    if (set->count == 0)    return NULL;

    uint32_t random_idx;
    hashnode_t* node;

    if (set->flag == PDB_SET_ENCODING_INTSET){
        struct pdb_intset* intset = set->ptr;
        random_idx = rand() % intset->len;
        int64_t val = _pdb_intset_get(intset, random_idx);
        char* ret_str = pdb_malloc(32);
        snprintf(ret_str, 32, "%" PRId64, val);
        set->ptr = pdb_intset_delete(intset, val, NULL);
        set->count--;
        return ret_str;
    }

    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* hash = set->ptr;
        hashnode_t* head;
        do{
            random_idx = rand() % hash->max_slots;
            node = hash->nodes[random_idx];
        } while(node == NULL);

        head = node;
        int list_node_count = 0;
        while(node != NULL){
            list_node_count++;
            node = node->next;
        }

        random_idx = rand() % list_node_count;
        list_node_count = 0;
        node = head;
        while(list_node_count < random_idx){
            list_node_count++;
            node = node->next;
        }

        char* res = pdb_malloc(strlen(node->key) + 1);
        strcpy(res, node->key);
        pdb_hash_del(set->ptr, node->key);
        set->count--;
        return res;
    }

    return NULL;
}

/**
 * Return PDB_DATASTRUCTURE_OK or PDB_DATASTRUCTURE_NOEXIST.
 */
int pdb_set_delete(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        int ret =  pdb_hash_del(set_hash, (char*)value);
        if (ret == PDB_DATASTRUCTURE_OK)    set->count--;
        return ret;
    }

    int64_t val;
    int success = 0;
    if (_pdb_set_is_numeric(value, &val)){
        set->ptr = pdb_intset_delete(set->ptr, val, &success); 
        if (success){
            set->count--;
            return PDB_DATASTRUCTURE_OK;
        }    
    }

    return PDB_DATASTRUCTURE_NOEXIST;
}

void pdb_set_destroy(struct pdb_set* set){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        pdb_hash_destory(set_hash);
        pdb_free(set, -1);
        return;
    }

    pdb_intset_destroy(set->ptr);  
    pdb_free(set, -1); 
}


// test.......................
static void ok(void) {
    printf("OK\n");
}

void pdb_set_test(){
    printf("#############pdb_set_test#####################\n");
    srand(time(NULL));
    struct pdb_set *set;
    char buf[32];

    printf("1. Basic Intset Addition & Search: "); {
        set = pdb_set_create(); 
        assert(set->flag == PDB_SET_ENCODING_INTSET);
        
        assert(pdb_set_add(set, "123") == PDB_DATASTRUCTURE_OK); 
        assert(pdb_set_add(set, "-456") == PDB_DATASTRUCTURE_OK);
        
        assert(pdb_set_add(set, "123") == PDB_DATASTRUCTURE_EXIST); 
        
        assert(pdb_set_search(set, "123") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "-456") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "999") == PDB_DATASTRUCTURE_NOEXIST);
        
        assert(set->flag == PDB_SET_ENCODING_INTSET);
        
        pdb_set_destroy(set); 
        ok();
    }

    printf("2. Upgrade triggered by Non-Numeric String: "); {
        set = pdb_set_create();
        
        pdb_set_add(set, "10");
        pdb_set_add(set, "20");
        assert(set->flag == PDB_SET_ENCODING_INTSET); 
        
        pdb_set_add(set, "hello_world");
        
        assert(set->flag == PDB_SET_ENCODING_HASHTABLE);
        assert(pdb_set_search(set, "10") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "20") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "hello_world") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "hello") == PDB_DATASTRUCTURE_NOEXIST);
        
        pdb_set_destroy(set);
        ok();
    }

    printf("3. Upgrade triggered by Size Threshold (Max Entries): "); {
        set = pdb_set_create();
        
        for (int i = 0; i < PDB_MAX_INTSET_LENGTH; i++) {
            snprintf(buf, sizeof(buf), "%d", i);
            pdb_set_add(set, buf);
        }
        
        assert(set->flag == PDB_SET_ENCODING_INTSET);
        
        snprintf(buf, sizeof(buf), "%d", PDB_MAX_INTSET_LENGTH);
        pdb_set_add(set, buf);
        
        assert(set->flag == PDB_SET_ENCODING_HASHTABLE);
        
        assert(pdb_set_search(set, "0") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "256") == PDB_DATASTRUCTURE_EXIST);
        
        snprintf(buf, sizeof(buf), "%d", PDB_MAX_INTSET_LENGTH);
        assert(pdb_set_search(set, buf) == PDB_DATASTRUCTURE_EXIST);
        
        pdb_set_destroy(set);
        ok();
    }
    
    printf("4. Operations after converted to Hash Table: "); {
        set = pdb_set_create();
        pdb_set_add(set, "initial_string"); 
        assert(set->flag == PDB_SET_ENCODING_HASHTABLE);
        
        pdb_set_add(set, "10086");
        pdb_set_add(set, "another_string");
        
        assert(pdb_set_search(set, "10086") == PDB_DATASTRUCTURE_EXIST);
        assert(pdb_set_search(set, "another_string") == PDB_DATASTRUCTURE_EXIST);
        
        pdb_set_destroy(set);
        ok();
    }

    printf("\n\n");
}