#include "pdb_rdb.h"


int pdb_rdb_load(const char* file){
    assert(file != NULL);

    int time_used;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    int fd = open(file, O_RDONLY);
    if (fd < 0) {
        pdb_log_info("Failed to open rdb file: %s, Please check RDB file\n", file);
        close(fd);
        return PDB_ERROR;
    }
    struct stat t;
    if (fstat(fd, &t) == -1){
        perror("fstat failed");
        close(fd);
        return PDB_ERROR;
    }
    if (t.st_size == 0){
        pdb_log_info("No RDB file to load, RDB file is empty\n");
        close(fd);
        return PDB_ERROR;
    }

    char* curr = mmap(NULL, t.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (curr == MAP_FAILED){
        pdb_log_error("mmap failed\n");
        close(fd);
        return PDB_ERROR;
    }

    char* end = curr + t.st_size;

    while(curr != end){
        int len = check_resp_integrity(curr, t.st_size, NULL);  // len: the length of RESP msg.
        int ret = pdb_protocol(curr, len, NULL);
        if (ret != PDB_OK){
            pdb_log_error("RDB file parse failed\n");
            return ret;
        }
        curr += len;
    }

    close(fd);
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    pdb_log_info("RDB load(file size: %lld), time used: %d ms\n", (long long)t.st_size, time_used);

    return PDB_OK;
}


int pdb_rdb_array_dump(pdb_array_t* arr, const char* file){
    assert(arr != NULL && file != NULL);

    
    FILE* fp = fopen(file, "w");

    for (int i = 0; i < arr->total_count; i++) {
        if(arr->table[i].key == NULL){
            continue;
        }
        const char *key = arr->table[i].key;
        const char* val = pdb_parse_value_to_string(arr->table[i].value);
        size_t klen = strlen(key);
        size_t vlen = strlen(val);

        fprintf(fp, "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                    klen, key, vlen, val);
    }
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    

    return PDB_OK;
}



int pdb_rdb_hash_dump(pdb_hash_t* h, const char* file){
    assert(h != NULL && file != NULL);

	FILE *fp = fopen(file, "a");
	if (fp == NULL){
		printf("fopen error\n");
	}
    // 遍历所有桶
    for (int i = 0; i < h->max_slots; ++i) {
        hashnode_t *node = h->nodes[i];
        while (node != NULL) {
#if ENABLE_KEY_POINTER
            if (node->key == NULL || node->value == NULL) {
                node = node->next;
                continue;
            }
#endif
            const char *key = node->key;
            const char* val = pdb_parse_value_to_string(node->value);
            // const char *val = node->value;
            size_t klen = strlen(key);
            size_t vlen = strlen(val);

            fprintf(fp, "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                    klen, key, vlen, val);

            node = node->next;
        }
    }

    fclose(fp);

    return PDB_OK;
}


static void rbtree_dump_dfs(FILE *fp, rbtree_node *node, rbtree_node *nil)
{
    if (node == nil) return;
	fprintf(fp, "*3\r\n$4\r\nRSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen((char*)node->key), (char*)node->key, strlen((char*)node->value), (char*)node->value);
	fflush(fp);
    rbtree_dump_dfs(fp, node->left, nil);
    rbtree_dump_dfs(fp, node->right, nil);
}

int pdb_rdb_rbtree_dump(pdb_rbtree_t* rbtree, const char* file){
    assert(rbtree != NULL && file != NULL);

    FILE *fp = fopen(file, "a");
    if (!fp) {
        perror("fopen dump");
        return PDB_ERROR;
    }

    rbtree_dump_dfs(fp, rbtree->root, rbtree->nil);
    fclose(fp);

    return PDB_OK;
}


int pdb_rdb_dump(const char* file){
    char tmp_file_name[1024];
    if (snprintf(tmp_file_name, 1024, "%s.tmp", file) >= 1024){
        return PDB_ERROR;
    }

    int ret = pdb_rdb_array_dump(&global_array, tmp_file_name);
    if (ret != PDB_OK) {
        pdb_log_error("array dump error\n");
        return ret;
    }

    ret = pdb_rdb_rbtree_dump(&global_rbtree, tmp_file_name);
    if (ret != PDB_OK){
        pdb_log_error("rbtree dump error\n");
    }
    
    ret = pdb_rdb_hash_dump(&global_hash, tmp_file_name);
    if (ret != PDB_OK) {
        pdb_log_error("hash dump error\n");
        return ret;
    }

    if (ret == PDB_OK){
        if (rename(tmp_file_name, file) == -1){
            pdb_log_error("rename error\n");
            unlink(tmp_file_name);
            return PDB_ERROR;
        }
    }
    
    return ret;
}