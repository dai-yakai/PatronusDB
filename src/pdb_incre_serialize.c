#include "pdb_serialize.h"
#define PDB_OPCODE_SUB_SET      0xF3
#define PDB_OPCODE_SUB_SSET     0xF4
#define PDB_OPCODE_SUB_BITMAP   0xF5
/**
 * If succeed, return serialize len; otherwise return -1;
 */
int pdb_incre_serialize(void* dataStructure, const char* key, char* buf, size_t buf_len, size_t* offset, uint8_t opcode) {
    pdb_value* value = NULL;
    buf_len = buf_len - 64;

    // 🚩 步骤 1: 身份识别逻辑
    char* parent_key = NULL;
    if (opcode == PDB_OPCODE_HASH) {
        pdb_value* value = pdb_hash_get(&global_hash, key);
        if (value != NULL && value->type == PDB_VALUE_TYPE_BITMAP){
            pdb_log_info("hash set bitmap\n");
            return -3;
        }


        pdb_hash_t* h = (pdb_hash_t*)dataStructure;
        parent_key = h->parent_key;
        if (parent_key != NULL){
            value = pdb_hash_get(&global_hash, parent_key);
            switch(value->type){
                case PDB_VALUE_TYPE_SORTEDSET:
                {
                    opcode = PDB_OPCODE_SUB_SET; 
                    break;
                }
                case PDB_VALUE_TYPE_SET:
                {
                    opcode = PDB_OPCODE_SUB_SET; 
                    break;
                }
            }
        }
    }

    // 🚩 步骤 2: 获取 Value (逻辑不变)
    switch(opcode){
        case PDB_OPCODE_ARRAY: value = pdb_array_get(dataStructure, (char*)key); break;
        case PDB_OPCODE_HASH:  value = pdb_hash_get(dataStructure, (char*)key); break;
        case PDB_OPCODE_RBTREE: value = pdb_rbtree_get(dataStructure, (char*)key); break;
        case PDB_OPCODE_SUB_SET: value = pdb_hash_get(dataStructure, (char*)key); break;
    }   

    if (value == NULL) return -1;
    if (*offset + 4096 >= buf_len - 32) return -3;

    size_t backup_offset = *offset;

    // 🚩 步骤 3: 写入字节流
    // 如果是 SUB_SET，我们需要存入 ParentKey，这样加载时才知道往哪个 Set 里塞
    if (opcode == PDB_OPCODE_SUB_SET) {
        if (_pdb_append_uint8(buf, buf_len, offset, opcode) != PDB_RETURN_OK ||
            _pdb_append_string(buf, buf_len, offset, parent_key) != PDB_RETURN_OK || // 存入 "myset"
            _pdb_append_string(buf, buf_len, offset, (char*)key) != PDB_RETURN_OK || // 存入 "member:X"
            _pdb_append_value(buf, buf_len, offset, value) != PDB_RETURN_OK) {       // 存入 Value
            goto err_clean;
        }
    } else {
        // 原有逻辑不变
        if (_pdb_append_uint8(buf, buf_len, offset, opcode) != PDB_RETURN_OK ||
            _pdb_append_string(buf, buf_len, offset, (char*)key) != PDB_RETURN_OK ||
            _pdb_append_value(buf, buf_len, offset, value) != PDB_RETURN_OK) {
            goto err_clean;
        }
    }

    return *offset - backup_offset;

err_clean:
    *offset = backup_offset;
    return -3;
}


int pdb_incre_deserialize(const char* buf, size_t buf_len, size_t* offset){
    uint8_t opcode; // 🚩 新增：路由标签
    char* key = NULL;

    // 第一步：先读 1 字节的路由标签 (必须最先读，否则整个字节流错位)
    if (_pdb_read_uint8(buf, buf_len, offset, &opcode) < 0) {
        pdb_log_error("Failed to read opcode from incremental payload\n");
        return -1;
    }


    if (opcode == PDB_OPCODE_SUB_SET) {
        char *parent_key = NULL;
        char *member_key = NULL;
        
        // 1. 读父键 (如 "myset")
        if (_pdb_read_string(buf, buf_len, offset, &parent_key) < 0) return -1;
        // 2. 读成员键 (如 "member:100")
        if (_pdb_read_string(buf, buf_len, offset, &member_key) < 0) { 
            pdb_free(parent_key, -1); return -1; 
        }
        // 3. 读值
        pdb_value* val = _pdb_deserialize_value(buf, buf_len, offset);

        // 4. 寻找并插入
        pdb_value* set_val = pdb_hash_get(&global_hash, parent_key);
        if (set_val != NULL) {
            // 3. 🚀 动态路由：根据父节点的类型，决定调用哪个 add 函数！
            if (set_val->type == PDB_VALUE_TYPE_SET) {    
                pdb_set_add((struct pdb_set*)set_val->ptr, member_key);  
            } else if (set_val->type == PDB_VALUE_TYPE_SORTEDSET) {
                double score = *((double*)val->ptr); 
                pdb_sortedSet_add((struct pdb_sorted_set*)set_val->ptr, member_key, score);
            } else if (set_val->type == PDB_VALUE_TYPE_HASH) {
                pdb_hash_set((pdb_hash_t*)set_val->ptr, member_key, val);
            } else if (set_val->type == PDB_VALUE_TYPE_BITMAP){

            }
        }

        pdb_free(parent_key, -1);
        pdb_free(member_key, -1);
        pdb_decre_value(val);
        return 0;
    }

    if (opcode == PDB_OPCODE_BITMAP){
        // pdb_log_debug("deserialize bitmap\n");
        uint32_t key_len;
        char* key = pdb_malloc(key_len + 1);
        int val;
        uint64_t bit_offset;
        int ret;
        struct pdb_bitmap* bitmap;
        if (_pdb_read_uint32(buf, buf_len, offset, &key_len) < 0 || 
            _pdb_read_string(buf, buf_len, offset, &key) < 0 || 
            _pdb_read_uint64(buf, buf_len, offset, &bit_offset) < 0 ||
            _pdb_read_int(buf, buf_len, offset, &val) < 0)
        {
            pdb_log_debug("read bitmap error\n");
        }

        pdb_log_info("key:%s\n", key);
        pdb_log_info("bit_offset: %d\n", bit_offset);
        pdb_log_info("val: %d\n", val);
        pdb_value* value = pdb_hash_get(&global_hash, key);
        if (value == NULL){
            pdb_sds sds = pdb_get_new_sds(PDB_INIT_BTIMAP_LENGTH);
            bitmap = pdb_bitmap_create(key, sds);
            value = pdb_create_value(bitmap, PDB_VALUE_TYPE_BITMAP);
            pdb_hash_set(&global_hash, key, value);
        }

        bitmap = (struct pdb_bitmap*)value->ptr;
        ret = pdb_bitmap_set_(bitmap, bit_offset, val, NULL);

        return 0;
    }

    // 第二步：读 Key
    if (_pdb_read_string(buf, buf_len, offset, &key) < 0) {
        pdb_log_error("Failed to read key from incremental payload\n");
        return -1;
    }

    // 第三步：读 Value
    pdb_value* val = _pdb_deserialize_value(buf, buf_len, offset);
    if (!val) { 
        pdb_log_error("Failed to deserialize value for key: %s\n", key);
        pdb_free(key, -1); 
        return -1;
    }

    // 第四步：🚩 路由分发，根据 Opcode 将数据写回对应的内存结构
    switch (opcode) {
        case PDB_OPCODE_HASH:
            pdb_hash_set(&global_hash, key, val);
            break;
        case PDB_OPCODE_ARRAY:
            pdb_array_set(&global_array, key, val); 
            break;
        case PDB_OPCODE_RBTREE:
            pdb_rbtree_set(&global_rbtree, key, val); 
            break;
        default:
            pdb_log_error("Unknown incremental target opcode: %d\n", opcode);
            break;
    }

    // printf("🔄 [SLAVE INCRE] Zero-Copy Replayed Key: %s to Structure: %d\n", key, opcode);

    pdb_free(key, -1);
    pdb_decre_value(val); // 归还临时对象所有权

    return 0;
}