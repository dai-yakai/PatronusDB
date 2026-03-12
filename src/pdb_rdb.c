#include "pdb_rdb.h"


int pdb_rdb_load(const char* file){

    int time_used;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    int fd = global_dump.dump_fd;
    // open(file, O_RDONLY);
    // if (fd < 0) {
    //     pdb_log_error("RDB file not found or cannot be opened: %s\n", file);
    //     return -1; 
    // }

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) {
        // file is empty
        // close(fd);
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

    // FILE* fp = fopen(file, "w");

    // for (int i = 0; i < arr->total_count; i++) {
    //     if(arr->table[i].key == NULL){
    //         continue;
    //     }
    //     const char* key = arr->table[i].key;
    //     const char* val = pdb_parse_value_to_string(arr->table[i].value);
    //     size_t klen = strlen(key);
    //     size_t vlen = strlen(val);

    //     fprintf(fp, "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
    //                 klen, key, vlen, val);
    // }
    // fflush(fp);
    // fsync(fileno(fp));
    // fclose(fp);

    

    return PDB_OK;
}



int pdb_rdb_hash_dump(pdb_hash_t* h, const char* file){
    assert(h != NULL && file != NULL);

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



// 	FILE *fp = fopen(file, "a");
// 	if (fp == NULL){
// 		printf("fopen error\n");
// 	}
//     // 遍历所有桶
//     for (int i = 0; i < h->max_slots; ++i) {
//         hashnode_t *node = h->nodes[i];
//         while (node != NULL) {
// #if ENABLE_KEY_POINTER
//             if (node->key == NULL || node->value == NULL) {
//                 node = node->next;
//                 continue;
//             }
// #endif
//             const char *key = node->key;
//             const char* val = pdb_parse_value_to_string(node->value);
//             // const char *val = node->value;
//             size_t klen = strlen(key);
//             size_t vlen = strlen(val);

//             pdb_serialize_hash();

//             fprintf(fp, "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
//                     klen, key, vlen, val);

//             node = node->next;
//         }
//     }

//     fclose(fp);

//     return PDB_OK;
}


// static void rbtree_dump_dfs(FILE *fp, rbtree_node *node, rbtree_node *nil)
// {
//     if (node == nil) return;
// 	fprintf(fp, "*3\r\n$4\r\nRSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen((char*)node->key), (char*)node->key, strlen((char*)node->value), (char*)node->value);
// 	fflush(fp);
//     rbtree_dump_dfs(fp, node->left, nil);
//     rbtree_dump_dfs(fp, node->right, nil);
// }

int pdb_rdb_rbtree_dump(pdb_rbtree_t* rbtree, const char* file){
    assert(rbtree != NULL && file != NULL);


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

    // FILE *fp = fopen(file, "a");
    // if (!fp) {
    //     perror("fopen dump");
    //     return PDB_ERROR;
    // }

    // rbtree_dump_dfs(fp, rbtree->root, rbtree->nil);
    // fclose(fp);

    // return PDB_OK;
}


int pdb_rdb_dump(const char* file){
    char tmp_file_name[1024];
    if (snprintf(tmp_file_name, 1024, "%s.tmp", file) >= 1024){
        return PDB_ERROR;
    }

    int ret = pdb_rdb_array_dump(&global_array, file);
    if (ret != PDB_OK) {
        pdb_log_error("array dump error\n");
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
        pdb_log_error("write EOF error\n");
        ret = PDB_ERROR;
        return ret;
    }

    // if (ret == PDB_OK){
    //     if (rename(tmp_file_name, file) == -1){
    //         pdb_log_error("RDB rename failed! From %s to %s. Reason: %s (errno: %d)\n", 
    //               tmp_file_name, file, strerror(errno), errno);
    //         unlink(tmp_file_name);
    //         return PDB_ERROR;
    //     }
    // }
    // close(fd);
    
    return PDB_OK;
}