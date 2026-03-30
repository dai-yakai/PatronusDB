#include "pdb_rdb.h"


int pdb_rdb_load(const char* file){
    int fd = open(file, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd < 0){
        pdb_log_error("open aof file error, errno: %d, reason: %s\n", errno, strerror(errno));
    }
    global_dump.dump_fd = fd;

    int time_used;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    fd = global_dump.dump_fd;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) {
        // file is empty
        return 0; 
    }
    size_t total_size = st.st_size;

    const char* mapped_buf = (const char*)mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped_buf == MAP_FAILED) {
        // close(fd);
        return -1;
    }

    size_t offset = 0;
    
    while (offset < total_size) {
        uint8_t opcode;
        if (_pdb_read_uint8(mapped_buf, total_size, &offset, &opcode) < 0) break;

        switch (opcode) {
            case PDB_OPCODE_HASH:
                if (pdb_deserialize_hash(mapped_buf, total_size, &offset, &global_hash) < 0) {
                    pdb_log_error("Failed to load HASH from RDB\n");
                    goto load_err;
                }
                break;
                
            case PDB_OPCODE_ARRAY:
                if (pdb_deserialize_array(mapped_buf, total_size, &offset, &global_array) < 0) {
                    goto load_err;
                }
                break;
                
            case PDB_OPCODE_RBTREE:
                if (pdb_deserialize_rbtree(mapped_buf, total_size, &offset, &global_rbtree) < 0) {
                    goto load_err;
                }
                break;
            case PDB_OPCODE_EOF:
            // end of file
                break;
                
            default:
                pdb_log_error("RDB File Corrupted! Unknown structure opcode: %d at offset %zu\n", opcode, offset);
                goto load_err;
        }
    }

    pdb_log_info("RDB Successfully Loaded! Processed %zu bytes.\n", offset);
    munmap((void*)mapped_buf, total_size);
    // close(fd);

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    pdb_log_info("RDB load(file size: %lld), time used: %d ms\n", (long long)st.st_size, time_used);
    
    return 0;

load_err:
    munmap((void*)mapped_buf, total_size);
    // close(fd);
    return -1;
}


int pdb_rdb_array_dump(pdb_array_t* arr, const char* file){
    assert(arr != NULL && file != NULL);
    if (arr->used_count == 0)   return PDB_OK;

    int fd = global_dump.dump_fd;
    // open(file, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        pdb_log_error("array dump fstat error\n");
        // close(fd);
        return -1;
    }
    size_t original_size = st.st_size;

    size_t max_virtual_size = 100ULL * 1024 * 1024 * 1024; // 100GB
    if (ftruncate(fd, max_virtual_size) != 0) {
        pdb_log_error("array dump ftruncate error, errno: %d, reason: %s\n", errno, strerror(errno));
        // close(fd);
        return -1;
    }

    char* mapped_buf = (char*)mmap(NULL, max_virtual_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_buf == MAP_FAILED) {
        pdb_log_error("array dump map error\n");
        // close(fd);
        return -1;
    }

    size_t offset = original_size;
    int ret = pdb_serialize_array(mapped_buf, max_virtual_size, &offset, arr);

    if (ret < 0) {
        pdb_log_error("RDB Serialization failed midway!\n");
    }

    msync(mapped_buf, offset, MS_SYNC);
    munmap(mapped_buf, max_virtual_size);

    if (ftruncate(fd, offset) != 0) {
        pdb_log_error("Failed to shrink RDB file to actual size\n");
    }

    // close(fd);

    pdb_log_info("RDB Save Success! Size: %zu bytes\n", offset);
    return ret;
}



int pdb_rdb_hash_dump(pdb_hash_t* h, const char* file){
    assert(h != NULL && file != NULL);
    if (h->count == 0)  return PDB_OK;

    int fd = global_dump.dump_fd;
    // open(file, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        // close(fd);
        return -1;
    }
    size_t original_size = st.st_size;

    size_t max_virtual_size = 100ULL * 1024 * 1024 * 1024; // 100GB
    if (ftruncate(fd, max_virtual_size) != 0) {
        // close(fd);
        return -1;
    }

    char* mapped_buf = (char*)mmap(NULL, max_virtual_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_buf == MAP_FAILED) {
        // close(fd);
        return -1;
    }

    size_t offset = original_size;
    int ret = pdb_serialize_hash(mapped_buf, max_virtual_size, &offset, h);

    if (ret < 0) {
        pdb_log_error("RDB Serialization failed midway!\n");
    }

    msync(mapped_buf, offset, MS_SYNC);
    munmap(mapped_buf, max_virtual_size);

    if (ftruncate(fd, offset) != 0) {
        pdb_log_error("Failed to shrink RDB file to actual size\n");
    }

    // close(fd);

    pdb_log_info("RDB Save Success! Size: %zu bytes\n", offset);
    return ret;
}


int pdb_rdb_rbtree_dump(pdb_rbtree_t* rbtree, const char* file){
    assert(rbtree != NULL && file != NULL);
    if (rbtree->node_count == 0)    return PDB_OK;

    int fd = global_dump.dump_fd;
    // open(file, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        // close(fd);
        return -1;
    }
    size_t original_size = st.st_size;

    size_t max_virtual_size = 100ULL * 1024 * 1024 * 1024; // 100GB
    if (ftruncate(fd, max_virtual_size) != 0) {
        // close(fd);
        return -1;
    }

    char* mapped_buf = (char*)mmap(NULL, max_virtual_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_buf == MAP_FAILED) {
        // close(fd);
        return -1;
    }

    size_t offset = original_size;
    int ret = pdb_serialize_rbtree(mapped_buf, max_virtual_size, &offset, rbtree);

    if (ret < 0) {
        pdb_log_error("RDB Serialization failed midway!\n");
    }

    msync(mapped_buf, offset, MS_SYNC);
    munmap(mapped_buf, max_virtual_size);

    if (ftruncate(fd, offset) != 0) {
        pdb_log_error("Failed to shrink RDB file to actual size\n");
    }

    // close(fd);

    pdb_log_info("RDB Save Success! Size: %zu bytes\n", offset);
    return ret;
}


int pdb_rdb_dump(const char* file){
    char tmp_file_name[1024];
    if (snprintf(tmp_file_name, 1024, "%s.tmp", file) >= 1024){
        return PDB_ERROR;
    }

    int ret = pdb_rdb_array_dump(&global_array, file);
    if (ret != PDB_OK) {
        pdb_log_error("array dump error, ret: %d\n", ret);
        return ret;
    }

    ret = pdb_rdb_rbtree_dump(&global_rbtree, file);
    if (ret != PDB_OK){
        pdb_log_error("rbtree dump error\n");
    }
    
    ret = pdb_rdb_hash_dump(&global_hash, file);
    if (ret != PDB_OK) {
        pdb_log_error("hash dump error\n");
        return ret;
    }

    int fd = global_dump.dump_fd;
    // open(tmp_file_name, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    uint8_t end_of_file = PDB_OPCODE_EOF;
    int len = write(fd, &end_of_file, 1);
    if (len < 0){
        pdb_log_error("write(fd: %d) EOF error, errno: %d, reason: %s\n", fd, errno, strerror(errno));
        ret = PDB_ERROR;
        return ret;
    }
  
    return PDB_OK;
}

int pdb_rdb_dump_raw(const char* file){
    int fd = open(file, O_TRUNC | O_CREAT | O_RDWR);
    if (fd < 0){
        pdb_log_error("open rdb dump raw failed\n");
        return PDB_ERROR;
    }

    int i = 0;
    int buffer_len = 16 * 1024;
    char* write_buffer = pdb_malloc(buffer_len); // 16k
    int len = 0;
    int write_len = 0;

    // tranverse array
    for (i = 0; i < global_array.used_count; i++){
        if (&(global_array.table[i]) == NULL)  continue;
        char* key = global_array.table[i].key;
        char* value = pdb_parse_value_to_string(global_array.table[i].value);

        size_t k_len = strlen(key);
        size_t v_len = strlen(value);
        char k_len_str[32], v_len_str[32];
        int k_num_len = snprintf(k_len_str, sizeof(k_len_str), "%zu", k_len);
        int v_num_len = snprintf(v_len_str, sizeof(v_len_str), "%zu", v_len);
        size_t required_space = 14 + (1 + k_num_len + 2 + k_len + 2) + (1 + v_num_len + 2 + v_len + 2);
        
        if (len + required_space > buffer_len){
            int write_len = write(fd, write_buffer, len);
            if (write_len < 0){
                pdb_log_error("rdb raw write failed\n");
                return PDB_ERROR;
            }
            memset(write_buffer, 0, buffer_len);
            len = 0;
        }
        
        len += sprintf(write_buffer + len, 
                        "*3\r\n$3\r\nSET\r\n$%ld\r\n%s\r\n$%ld\r\n%s\r\n",
                            k_len, key, 
                            v_len, value
                            );
    }
    // write raw array data into dump file
    if (len > 0){
        write_len = write(fd, write_buffer, len);
    }
    if (write_len < 0){
        pdb_log_error("rdb raw write failed\n");
        return PDB_ERROR;
    }
    memset(write_buffer, 0, buffer_len);
    len = 0;
    pdb_log_info("array raw dump success\n");   // success

    // tranverse hash
    hashnode_t* node;
    for (i = 0; i < global_hash.max_slots; i++){
        node = global_hash.nodes[i];
        while(node != NULL){
            char* key = node->key;
            int k_len = strlen(key);
            pdb_value* value = node->value;
            if (value->type == PDB_VALUE_TYPE_STRING){
                char* v = (char*)value->ptr;
                int v_len = strlen(v);
                char k_len_str[32], v_len_str[32];
                int k_num_len = snprintf(k_len_str, sizeof(k_len_str), "%d", k_len);
                int v_num_len = snprintf(v_len_str, sizeof(v_len_str), "%d", v_len);
                // "*3\r\n$4\r\nHSET\r\n"
                int required_space = 15 + (1 + k_num_len + 2 + k_len + 2) + (1 + v_num_len + 2 + v_len + 2);
                if (required_space + len > buffer_len){
                    int write_len = write(fd, write_buffer, len);
                    if (write_len < 0){
                        pdb_log_error("rdb raw write failed\n");
                        return PDB_ERROR;
                    }
                    memset(write_buffer, 0, buffer_len);
                    len = 0;
                }

                len += sprintf(write_buffer + len, 
                        "*3\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                            k_len, key, 
                            v_len, v
                            );

            }else if (value->type == PDB_VALUE_TYPE_INT){
                char* v = pdb_parse_value_to_string(value);
                int v_len = strlen(v);
                char k_len_str[32], v_len_str[32];
                int k_num_len = snprintf(k_len_str, sizeof(k_len_str), "%d", k_len);
                int v_num_len = snprintf(v_len_str, sizeof(v_len_str), "%d", v_len);
                int required_space = 15 + (1 + k_num_len + 2 + k_len + 2) + (1 + v_num_len + 2 + v_len + 2);
                if (required_space + len > buffer_len){
                    int write_len = write(fd, write_buffer, len);
                    if (write_len < 0){
                        pdb_log_error("rdb raw write failed\n");
                        return PDB_ERROR;
                    }
                    memset(write_buffer, 0, buffer_len);
                    len = 0;
                }

                len += sprintf(write_buffer + len, 
                        "*3\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                            k_len, key, 
                            v_len, v
                            );

            }else if (value->type == PDB_VALUE_TYPE_BITMAP){
                // bitmap
                struct pdb_bitmap* bitmap = (struct pdb_bitmap*)value->ptr;
                pdb_sds b_data = bitmap->data;
                size_t b_len = pdb_get_sds_len(b_data);

                for (size_t byte_idx = 0; byte_idx < b_len; byte_idx++) {
                    unsigned char byte = b_data[byte_idx];
                    if (byte == 0) continue;

                    for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
                        if ((byte >> (7 - bit_idx)) & 1) {
                            uint64_t offset = (uint64_t)byte_idx * 8 + bit_idx;
                            
                            char offset_str[32];
                            int o_len = snprintf(offset_str, sizeof(offset_str), "%" PRIu64, offset);
                            int k_len = strlen(key);

                            size_t required_space = 17 + (1 + 10 + 2 + k_len + 2) + (1 + 10 + 2 + o_len + 2) + 7;

                            if (len + required_space > buffer_len) {
                                write(fd, write_buffer, len);
                                len = 0;
                            }

                            len += sprintf(write_buffer + len, 
                                    "*4\r\n$6\r\nBITSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$1\r\n1\r\n",
                                    k_len, key, o_len, offset_str);
                        }
                    }
                }
            }else if (value->type == PDB_VALUE_TYPE_SET){
                // set
                pdb_set* set = (pdb_set*)value->ptr;
                if (set->flag == PDB_SET_ENCODING_HASHTABLE){
                    int j = 0;
                    pdb_hash_t* hash = (pdb_hash_t*)set->ptr;
                    for (j = 0; j < hash->max_slots; j++){
                        hashnode_t* set_node = hash->nodes[j];
                        while(set_node != NULL){
                            char* set_value = set_node->key;

                            int v_len = strlen(set_value);
                            char k_len_str[32], v_len_str[32];
                            int k_num_len = snprintf(k_len_str, sizeof(k_len_str), "%d", k_len);
                            int v_num_len = snprintf(v_len_str, sizeof(v_len_str), "%d", v_len);
                            // "*3\r\n$4\r\nSSET\r\n"
                            int required_space = 15 + (1 + k_num_len + 2 + k_len + 2) + (1 + v_num_len + 2 + v_len + 2);
                            if (required_space + len > buffer_len){
                                int write_len = write(fd, write_buffer, len);
                                if (write_len < 0){
                                    pdb_log_error("rdb raw write failed\n");
                                    return PDB_ERROR;
                                }
                                memset(write_buffer, 0, buffer_len);
                                len = 0;
                            }

                            len += sprintf(write_buffer + len, 
                                    "*3\r\n$4\r\nSSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                        k_len, key, 
                                        v_len, set_value);

                            set_node = set_node->next;
                        }
                    }
                }else if (set->flag == PDB_SET_ENCODING_INTSET){
                    struct pdb_intset* iset = (struct pdb_intset*)value->ptr;
                    uint32_t j = 0;
        
                    for (j = 0; j < iset->len; j++) {
                        int64_t set_value_int = _pdb_intset_get(iset, j);
                        
                        char set_value_str[64];
                        int v_len = snprintf(set_value_str, sizeof(set_value_str), "%" PRId64, set_value_int);
                        
                        char k_len_str[32], v_len_str[32];
                        int k_num_len = snprintf(k_len_str, sizeof(k_len_str), "%d", k_len);
                        int v_num_len = snprintf(v_len_str, sizeof(v_len_str), "%d", v_len);
                        
                        // "*3\r\n$4\r\nSSET\r\n"     14
                        // Key: "$len\r\nkey\r\n" 
                        // Value: "$len\r\nvalue\r\n"
                        int required_space = 14 + (1 + k_num_len + 2 + k_len + 2) + (1 + v_num_len + 2 + v_len + 2);
                        
                        if (required_space + len > buffer_len) {
                            int write_len = write(fd, write_buffer, len);
                            if (write_len < 0){
                                pdb_log_error("rdb raw write failed\n");
                                return PDB_ERROR;
                            }
                            memset(write_buffer, 0, buffer_len); 
                            len = 0;
                        }
                        
                        len += sprintf(write_buffer + len, 
                                "*3\r\n$4\r\nSSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                k_len, key, v_len, set_value_str);
                    }
                }
            }else if (value->type == PDB_VALUE_TYPE_SORTEDSET){
                // sorted set
                struct pdb_sorted_set* sset = (struct pdb_sorted_set*)value->ptr;
                hashtable_t* set = sset->set;
                hashnode_t* sset_node;
                int j = 0;
                for (j = 0; j < set->max_slots; j++){
                    sset_node = set->nodes[j];
                    while(sset_node != NULL){
                        char* member = sset_node->key;
                        double score = *(double*)sset_node->value->ptr;

                        int k_len = strlen(key);
                        int m_len = strlen(member);

                        char score_str[64];
                        int s_len = snprintf(score_str, sizeof(score_str), "%.6f", score);

                        size_t required_space = snprintf(NULL, 0, 
                                "*4\r\n$5\r\nSSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                k_len, key, m_len, member, s_len, score_str);

                        if (len + required_space > buffer_len){
                            int write_len = write(fd, write_buffer, len);
                            if (write_len < 0){
                                pdb_log_error("rdb raw write failed\n");
                                return PDB_ERROR;
                            }
                            memset(write_buffer, 0, buffer_len);
                            len = 0;
                        }

                        len += sprintf(write_buffer + len, 
                                "*4\r\n$5\r\nSSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                    k_len, key,
                                    m_len, member,
                                    s_len, score_str); 

                        sset_node = sset_node->next;
                    }
                }
            }
            
            node = node->next;
        }
    }
    // write raw array data into dump file
    if (len > 0){
        write_len = write(fd, write_buffer, len);
    }
    if (write_len < 0){
        pdb_log_error("rdb raw write failed\n");
        return PDB_ERROR;
    }

    pdb_log_info("hash raw dump success\n");
    close(fd);

    return PDB_OK;
}