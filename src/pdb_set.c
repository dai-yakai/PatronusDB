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

        pdb_hash_set(set_hash, buf, "1");
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

    return set;
}

int pdb_set_add(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        return pdb_hash_set(set_hash, (char*)value, "1");
    }

    int64_t val;
    int success = 0;
    if (_pdb_set_is_numeric(value, &val)){
        set->ptr = pdb_intset_add((struct pdb_intset*)set->ptr, val, &success);
        if (success){
            if (((struct pdb_intset*)set->ptr)->len > PDB_MAX_INTSET_LENGTH){
                _pdb_set_convert_to_hashtable(set);
            }
            return 1;
        }else{
            return 0;
        }
    }else{
        _pdb_set_convert_to_hashtable(set);
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        return pdb_hash_set(set_hash, (char*)value, "1");
    }

    return 0;
}

/*
* Return 1 if found; otherwise return 0.
*/
int pdb_set_search(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        if (pdb_hash_exist((pdb_hash_t*)set->ptr, (char*)value))    return 0;
        return 1;
    }

    uint32_t pos;
    int64_t val;
    if (_pdb_set_is_numeric(value, &val)){
        return pdb_intset_search(set->ptr, val, &pos);
    }

    return 0;
}

/**
 * Return 1 if found; otherwise return 0.
 */
int pdb_set_delete(struct pdb_set* set, const char* value){
    if (set->flag == PDB_SET_ENCODING_HASHTABLE){
        pdb_hash_t* set_hash = (pdb_hash_t*)set->ptr;
        return pdb_hash_del(set_hash, (char*)value);
    }

    int64_t val;
    int success = 0;
    if (_pdb_set_is_numeric(value, &val)){
        set->ptr = pdb_intset_delete(set->ptr, val, &success);
        return success;
    }

    return 0;
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
    srand(time(NULL));
    struct pdb_set *set;
    char buf[32];

    printf("1. Basic Intset Addition & Search: "); {
        set = pdb_set_create(); 
        assert(set->flag == PDB_SET_ENCODING_INTSET);
        
        assert(pdb_set_add(set, "123") == 1); 
        assert(pdb_set_add(set, "-456") == 1);
        
        assert(pdb_set_add(set, "123") == 0); 
        
        assert(pdb_set_search(set, "123") == 1);
        assert(pdb_set_search(set, "-456") == 1);
        assert(pdb_set_search(set, "999") == 0);
        
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
        assert(pdb_set_search(set, "10") == 1);
        assert(pdb_set_search(set, "20") == 1);
        assert(pdb_set_search(set, "hello_world") == 1);
        assert(pdb_set_search(set, "hello") == 0);
        
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
        
        assert(pdb_set_search(set, "0") == 1);
        assert(pdb_set_search(set, "256") == 1);
        
        snprintf(buf, sizeof(buf), "%d", PDB_MAX_INTSET_LENGTH);
        assert(pdb_set_search(set, buf) == 1);
        
        pdb_set_destroy(set);
        ok();
    }
    
    printf("4. Operations after converted to Hash Table: "); {
        set = pdb_set_create();
        pdb_set_add(set, "initial_string"); 
        assert(set->flag == PDB_SET_ENCODING_HASHTABLE);
        
        pdb_set_add(set, "10086");
        pdb_set_add(set, "another_string");
        
        assert(pdb_set_search(set, "10086") == 1);
        assert(pdb_set_search(set, "another_string") == 1);
        
        pdb_set_destroy(set);
        ok();
    }
}