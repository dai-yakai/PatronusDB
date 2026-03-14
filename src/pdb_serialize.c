#include "pdb_serialize.h"

int _pdb_append(char* buf, size_t buf_len, size_t* offset, void* data, size_t data_len) {
    if (buf == NULL || buf_len <= 0 || data == NULL || data_len <= 0) {
        pdb_log_info("buf: %p, buf_len: %d, data: %p, data_len: %d\n", buf, buf_len, data, data_len);
        return PDB_RETURN_PARAM_ERROR;
    }
    if (*offset + data_len > buf_len){
        pdb_log_info("offset: %zu, data_len: %zu, buf_len: %zu\n", *offset, data_len, buf_len);
        return PDB_RETURN_PARAM_ERROR; // 防溢出
    } 
    
    memcpy(buf + *offset, data, data_len);
    *offset += data_len;
    return PDB_RETURN_OK;
}

int _pdb_append_uint8(char* buf, size_t buf_len, size_t* offset, uint8_t val) {
    int ret = _pdb_append(buf, buf_len, offset, &val, sizeof(uint8_t));
    if (ret != PDB_RETURN_OK){
        pdb_log_info("_pdb_append_uint8, return : %d\n", ret);
    }
    return ret;
}

int _pdb_append_uint16(char* buf, size_t buf_len, size_t* offset, uint16_t val){
    int ret = _pdb_append(buf, buf_len, offset, &val, sizeof(uint16_t));
    if (ret != PDB_RETURN_OK){
        pdb_log_info("_pdb_append_uint16\n");
    }
    return ret;
}

int _pdb_append_uint32(char* buf, size_t buf_len, size_t* offset, uint32_t val){
    int ret = _pdb_append(buf, buf_len, offset, &val, sizeof(uint32_t));
    if (ret != PDB_RETURN_OK){
        pdb_log_info("_pdb_append_uint32\n");
    }
    return ret;
}

int _pdb_append_uint64(char* buf, size_t buf_len, size_t* offset, uint64_t val){
    int ret = _pdb_append(buf, buf_len, offset, &val, sizeof(uint64_t));
    if (ret != PDB_RETURN_OK){
        pdb_log_info("pdb_append_uint64\n");
    }
    return ret;
}

int _pdb_append_int(char* buf, size_t buf_len, size_t* offset, int val) {
    return _pdb_append(buf, buf_len, offset, &val, sizeof(int));
}

int _pdb_append_size_t(char* buf, size_t buf_len, size_t* offset, size_t val) {
    int ret = _pdb_append(buf, buf_len, offset, &val, sizeof(size_t));
    if (ret != PDB_RETURN_OK){
        pdb_log_info("_pdb_append_size_t ret: %d\n", ret);
    }
    return ret;
}

int _pdb_append_long(char* buf, size_t buf_len, size_t* offset, long val) {
    return _pdb_append(buf, buf_len, offset, &val, sizeof(long));
}

int _pdb_append_double(char* buf, size_t buf_len, size_t* offset, double val) {
    return _pdb_append(buf, buf_len, offset, &val, sizeof(double));
}

int _pdb_append_string(char* buf, size_t buf_len, size_t* offset, char* val) {
    size_t len = (val == NULL) ? 0 : strlen(val);
    if (_pdb_append_size_t(buf, buf_len, offset, len) < 0) return -1;
    if (len > 0) {
        if (_pdb_append(buf, buf_len, offset, val, len) < 0) return -1;
    }
    return PDB_RETURN_OK;
}

int _pdb_append_value(char* buf, size_t buf_len, size_t* offset, pdb_value* value);
int pdb_serialize_hash(char* buf, size_t buf_len, size_t* offset, pdb_hash_t* hash);

int _pdb_append_set(char* buf, size_t buf_len, size_t* offset, pdb_set* set) {
    uint8_t set_type = set->flag;
    if (_pdb_append_uint8(buf, buf_len, offset, set_type) < 0) return -1;
    long count = set->count;
    // pdb_log_debug("set count: %ld\n", count);
    if (_pdb_append_long(buf, buf_len, offset, count) < 0) return -1;

    if (set->flag == PDB_SET_ENCODING_INTSET) {
        struct pdb_intset* intset = set->ptr;
        for (int i = 0; i < intset->len; i++) {
            long val = (long)_pdb_intset_get(intset, i);
            if (_pdb_append_long(buf, buf_len, offset, val) < 0) return -1;
        }
    } else if (set->flag == PDB_SET_ENCODING_HASHTABLE) {
        pdb_hash_t* hash = set->ptr;
        for (int i = 0; i < hash->max_slots; i++) {
            hashnode_t* node = hash->nodes[i];
            while (node != NULL) {
                if (_pdb_append_string(buf, buf_len, offset, node->key) < 0) return -1;
                // pdb_log_debug("offset: %d, KEY: %s\n", *offset, node->key);
                node = node->next;
            }
        }
    }
    return PDB_RETURN_OK;
}

int _pdb_append_sset(char* buf, size_t buf_len, size_t* offset, struct pdb_sorted_set* sset) {
    if (_pdb_append_long(buf, buf_len, offset, (long)sset->list->count) < 0) return -1;
    
    struct pdb_skiplistNode* node = sset->list->head->level[0].forward;
    while (node != NULL) {
        if (_pdb_append_string(buf, buf_len, offset, node->key) < 0) return -1;
        if (_pdb_append_double(buf, buf_len, offset, node->value) < 0) return -1;
        node = node->level[0].forward;
    }
    return PDB_RETURN_OK;
}

int _pdb_append_bitmap(char* buf, size_t buf_len, size_t* offset, struct pdb_bitmap* bitmap) {
    pdb_sds s = bitmap->data;
    size_t len = pdb_get_sds_len(s);
    pdb_log_info("_pdb_append_bitmap: %d\n", len);
    if (_pdb_append_size_t(buf, buf_len, offset, len) < 0) return -1;
    if (len > 0) {
        if (_pdb_append(buf, buf_len, offset, s, len) < 0) return -1;
    }
    return PDB_RETURN_OK;
}

int _pdb_append_value(char* buf, size_t buf_len, size_t* offset, pdb_value* value) {
    int type = value->type;
    if (_pdb_append_uint8(buf, buf_len, offset, (uint8_t)type) < 0) return -1;

    switch(type) {
        case PDB_VALUE_TYPE_SET:
            return _pdb_append_set(buf, buf_len, offset, value->ptr);
        case PDB_VALUE_TYPE_SORTEDSET:
            return _pdb_append_sset(buf, buf_len, offset, value->ptr);
        case PDB_VALUE_TYPE_HASH:
            return pdb_serialize_hash(buf, buf_len, offset, value->ptr);
        case PDB_VALUE_TYPE_STRING:
        case PDB_VALUE_TYPE_NULL:
            return _pdb_append_string(buf, buf_len, offset, value->ptr);
        case PDB_VALUE_TYPE_INT:
            return _pdb_append_long(buf, buf_len, offset, (long)value->ptr);
        case PDB_VALUE_TYPE_BITMAP:
            return _pdb_append_bitmap(buf, buf_len, offset, value->ptr);
        case PDB_VALUE_TYPE_DOUBLE:
            return _pdb_append_double(buf, buf_len, offset, *((double*)value->ptr));
    }
    return -1;
}

int pdb_serialize_array(char* buf, size_t buf_len, size_t* offset, pdb_array_t* array) {
    if (array->used_count == 0) return PDB_RETURN_OK;

    if (_pdb_append_uint8(buf, buf_len, offset, PDB_OPCODE_ARRAY) < 0) return -1;
    if (_pdb_append_int(buf, buf_len, offset, array->used_count) < 0) return -1;

    for (int i = 0; i < array->total_count; i++) {
        pdb_array_item_t it = array->table[i];
        if (it.value != NULL && it.key != NULL){
            if (_pdb_append_string(buf, buf_len, offset, it.key) < 0) return -1;
            if (_pdb_append_value(buf, buf_len, offset, it.value) < 0) return -1;
        }
    }
    return PDB_RETURN_OK;
}

int pdb_serialize_hash(char* buf, size_t buf_len, size_t* offset, pdb_hash_t* hash) {
    // pdb_log_info("hash_count: %d\n", hash->count);
    if (hash->count == 0)   return PDB_RETURN_OK;
    
    if (_pdb_append_uint8(buf, buf_len, offset, PDB_OPCODE_HASH) < 0) return -1;
    if (_pdb_append_int(buf, buf_len, offset, hash->count) < 0) return -1;

    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t* node = hash->nodes[i];
        while (node != NULL) {
            if (_pdb_append_string(buf, buf_len, offset, node->key) < 0) return -1;
            if (_pdb_append_value(buf, buf_len, offset, node->value) < 0) return -1;
            node = node->next;
        }
    }
    return PDB_RETURN_OK;
}

static int _tranverse_rbtree(char* buf, size_t buf_len, size_t* offset, rbtree *T, rbtree_node *node) {
    if (node != T->nil) {
        if (_tranverse_rbtree(buf, buf_len, offset, T, node->left) < 0) return -1;
        if (_pdb_append_string(buf, buf_len, offset, node->key) < 0) return -1;
        if (_pdb_append_value(buf, buf_len, offset, node->value) < 0) return -1;
        if (_tranverse_rbtree(buf, buf_len, offset, T, node->right) < 0) return -1;
    }
    return PDB_RETURN_OK;
}

int pdb_serialize_rbtree(char* buf, size_t buf_len, size_t* offset, pdb_rbtree_t* rbtree) {
    if (rbtree->node_count == 0)    return PDB_RETURN_OK;
    
    if (_pdb_append_uint8(buf, buf_len, offset, PDB_OPCODE_RBTREE) < 0) return -1;
    if (_pdb_append_int(buf, buf_len, offset, rbtree->node_count) < 0) return -1;
    return _tranverse_rbtree(buf, buf_len, offset, rbtree, rbtree->root);
}

int _pdb_read(const char* buf, size_t buf_len, size_t* offset, void* dest, size_t len) {
    if (*offset + len > buf_len) {
        pdb_log_error("[Deserialize] Buffer overflow detected at offset %zu\n", *offset);
        return -1;
    }
    memcpy(dest, buf + *offset, len);
    *offset += len;
    return PDB_DATASTRUCTURE_OK;
}

int _pdb_read_uint8(const char* buf, size_t buf_len, size_t* offset, uint8_t* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(uint8_t));
}

int _pdb_read_uint16(const char* buf, size_t buf_len, size_t* offset, uint16_t* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(uint16_t));
}

int _pdb_read_uint32(const char* buf, size_t buf_len, size_t* offset, uint32_t* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(uint32_t));
}

int _pdb_read_uint64(const char* buf, size_t buf_len, size_t* offset, uint64_t* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(uint64_t));
}


int _pdb_read_int(const char* buf, size_t buf_len, size_t* offset, int* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(int));
}

int _pdb_read_size_t(const char* buf, size_t buf_len, size_t* offset, size_t* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(size_t));
}

int _pdb_read_long(const char* buf, size_t buf_len, size_t* offset, long* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(long));
}

int _pdb_read_double(const char* buf, size_t buf_len, size_t* offset, double* val) {
    return _pdb_read(buf, buf_len, offset, val, sizeof(double));
}

pdb_value* _pdb_deserialize_value(const char* buf, size_t buf_len, size_t* offset);

int _pdb_read_string(const char* buf, size_t buf_len, size_t* offset, char** out_str) {
    size_t len;
    if (_pdb_read_size_t(buf, buf_len, offset, &len) < 0) return -1;
    
    char* str = (char*)pdb_malloc(len + 1);
    if (!str) return PDB_MALLOC_NULL;
    
    if (len > 0) {
        if (_pdb_read(buf, buf_len, offset, str, len) < 0) {
            pdb_free(str, -1);
            return -1;
        }
    }
    str[len] = '\0';
    *out_str = str;
    return PDB_DATASTRUCTURE_OK;
}

// ============================================================================
// 2. 内部复杂结构反序列化分发器
// ============================================================================

pdb_hash_t* _pdb_deserialize_inner_hash(const char* buf, size_t buf_len, size_t* offset) {
    uint8_t dummy_opcode;
    if (_pdb_read_uint8(buf, buf_len, offset, &dummy_opcode) < 0) return NULL;

    int count;
    if (_pdb_read_int(buf, buf_len, offset, &count) < 0) return NULL;

    pdb_hash_t* hash = pdb_hash_create2();
    for (int i = 0; i < count; i++) {
        char* key;
        if (_pdb_read_string(buf, buf_len, offset, &key) < 0) goto err;

        pdb_value* value = _pdb_deserialize_value(buf, buf_len, offset);
        if (!value) { pdb_free(key, -1); goto err; }

        pdb_hash_set(hash, key, value);
        
        pdb_free(key, -1);
        pdb_decre_value(value); 
    }
    return hash;
err:
    pdb_hash_destory(hash);
    pdb_free(hash, -1);
    return NULL;
}


struct pdb_set* _pdb_deserialize_set(const char* buf, size_t buf_len, size_t* offset) {
    struct pdb_set* set = pdb_malloc(sizeof(struct pdb_set));
    if (!set) return NULL;

    uint8_t flag;
    long count;
    if (_pdb_read_uint8(buf, buf_len, offset, &flag) < 0) goto err;
    if (_pdb_read_long(buf, buf_len, offset, &count) < 0) goto err;

    set->flag = flag;
    set->count = count;

    if (flag == PDB_SET_ENCODING_INTSET) {
        struct pdb_intset* intset = pdb_intset_create();
        for (long i = 0; i < count; i++) {
            long val; // 修正序列化时的精度丢失，统使用 long 读取
            if (_pdb_read_long(buf, buf_len, offset, &val) < 0) goto err;
            intset = pdb_intset_add(intset, (int64_t)val, NULL);
        }
        set->ptr = intset;
    } else if (flag == PDB_SET_ENCODING_HASHTABLE) {
        pdb_hash_t* hash = pdb_hash_create2();
        for (long i = 0; i < count; i++) {
            char* key;
            if (_pdb_read_string(buf, buf_len, offset, &key) < 0) goto err;
            pdb_value* dummy_val = (pdb_value*)pdb_malloc(sizeof(pdb_value));
            dummy_val->type = PDB_VALUE_TYPE_NULL;
            dummy_val->ptr = NULL;
            dummy_val->ref_count = 1;
            pdb_hash_set(hash, key, dummy_val);
            pdb_free(key, -1);
        }
        set->ptr = hash;
    }
    return set;
err:
    pdb_free(set, -1);
    return NULL;
}

struct pdb_sorted_set* _pdb_deserialize_sset(const char* buf, size_t buf_len, size_t* offset) {
    struct pdb_sorted_set* sset = pdb_create_sortedSet();
    if (!sset) return NULL;

    long count;
    if (_pdb_read_long(buf, buf_len, offset, &count) < 0) return NULL; 

    for (long i = 0; i < count; i++) {
        char* key;
        double score;
        if (_pdb_read_string(buf, buf_len, offset, &key) < 0) goto err;
        if (_pdb_read_double(buf, buf_len, offset, &score) < 0) { pdb_free(key, -1); goto err; }

        pdb_sortedSet_add(sset, key, score);
        pdb_free(key, -1);
    }
    return sset;
err:
    pdb_destroy_sortedSet(sset);
    return NULL;
}


pdb_value* _pdb_deserialize_value(const char* buf, size_t buf_len, size_t* offset) {
    uint8_t type;
    if (_pdb_read_uint8(buf, buf_len, offset, &type) < 0) return NULL;

    pdb_value* value = (pdb_value*)pdb_malloc(sizeof(pdb_value));
    if (!value) return NULL;
    value->type = type;
    value->ref_count = 1; 

    switch(type) {
        case PDB_VALUE_TYPE_INT: {
            long int_val;
            if (_pdb_read_long(buf, buf_len, offset, &int_val) < 0) goto err;
            value->ptr = (void*)int_val;
            break;
        }
        case PDB_VALUE_TYPE_DOUBLE: {
            double* d_val = pdb_malloc(sizeof(double));
            if (_pdb_read_double(buf, buf_len, offset, d_val) < 0) { pdb_free(d_val, -1); goto err; }
            value->ptr = d_val;
            break;
        }
        case PDB_VALUE_TYPE_NULL:
        case PDB_VALUE_TYPE_STRING: {
            char* str;
            if (_pdb_read_string(buf, buf_len, offset, &str) < 0) goto err;
            value->ptr = str;
            break;
        }
        case PDB_VALUE_TYPE_BITMAP: {
            size_t bit_len;
            char* raw_data = NULL;
            if (_pdb_read_size_t(buf, buf_len, offset, &bit_len) < 0) goto err;

            raw_data = pdb_malloc(bit_len);
            if (bit_len > 0 && _pdb_read(buf, buf_len, offset, raw_data, bit_len) < 0) {
                pdb_free(raw_data, -1);
                goto err;
            }
            struct pdb_bitmap* bitmap = pdb_bitmap_create(NULL, pdb_get_new_sds_len(raw_data, bit_len));
            value->ptr = bitmap;
            
            pdb_free(raw_data, -1);
            break;
        }
        case PDB_VALUE_TYPE_SET: {
            value->ptr = _pdb_deserialize_set(buf, buf_len, offset);
            if (!value->ptr) goto err;
            break;
        }
        case PDB_VALUE_TYPE_SORTEDSET: {
            value->ptr = _pdb_deserialize_sset(buf, buf_len, offset);
            if (!value->ptr) goto err;
            break;
        }
        case PDB_VALUE_TYPE_HASH: {
            value->ptr = _pdb_deserialize_inner_hash(buf, buf_len, offset);
            if (!value->ptr) goto err;
            break;
        }
        default:
            goto err;
    }
    return value;
err:
    pdb_free(value, -1);
    return NULL;
}

// ============================================================================
// 3. 顶层数据库对象反序列化
// ============================================================================

int pdb_deserialize_array(const char* buf, size_t buf_len, size_t* offset, pdb_array_t* array) {
    int count;
    if (_pdb_read_int(buf, buf_len, offset, &count) < 0) return -1;

    for (int i = 0; i < count; i++) {
        char* key;
        pdb_value* value;

        if (_pdb_read_string(buf, buf_len, offset, &key) < 0) return -1;
        value = _pdb_deserialize_value(buf, buf_len, offset);
        if (!value) { pdb_free(key, -1); return -1; }

        pdb_array_set(array, key, value);

        pdb_free(key, -1);
        pdb_decre_value(value);
    }
    return PDB_DATASTRUCTURE_OK;
}

int pdb_deserialize_hash(const char* buf, size_t buf_len, size_t* offset, pdb_hash_t* hash) {
    int count;
    if (_pdb_read_int(buf, buf_len, offset, &count) < 0) 
        return -1;

    size_t target_slots = 1;
    while (target_slots <= (size_t)count) {
        target_slots <<= 1; 
    }

    // pdb_hash_resize(hash, target_slots);

    for (int i = 0; i < count; i++) {
        char* key;
        pdb_value* value;

        if (_pdb_read_string(buf, buf_len, offset, &key) < 0) 
            return -1;
        value = _pdb_deserialize_value(buf, buf_len, offset);
        if (!value) { 
            pdb_free(key, -1); 
            return -1; 
        }

        pdb_hash_set(hash, key, value);

        pdb_free(key, -1);
        pdb_decre_value(value);
    }
    return PDB_DATASTRUCTURE_OK;
}

int pdb_deserialize_rbtree(const char* buf, size_t buf_len, size_t* offset, pdb_rbtree_t* rbtree) {
    int count;
    if (_pdb_read_int(buf, buf_len, offset, &count) < 0) return -1;

    for (int i = 0; i < count; i++) {
        char* key;
        pdb_value* value;

        if (_pdb_read_string(buf, buf_len, offset, &key) < 0) return -1;
        
        value = _pdb_deserialize_value(buf, buf_len, offset);
        if (!value) { pdb_free(key, -1); return -1; }

        // pdb_rbtree_set_no_search(rbtree, key, value);
        pdb_rbtree_set(rbtree, key, value);
        
        pdb_free(key, -1);
        pdb_decre_value(value);
    }
    return PDB_DATASTRUCTURE_OK;
}