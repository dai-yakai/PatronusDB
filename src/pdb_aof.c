#include "pdb_aof.h"

static int is_need_command(char *buf) {
    if (buf == NULL) return 0;
    
    if (strstr(buf, "SYN") != NULL) return 0; 
    
    if (strstr(buf, "SET") != NULL) return 1; // include SET, MSET, RSET, HSET
    if (strstr(buf, "DEL") != NULL) return 1; // include DEL, RDEL, HDEL
    if (strstr(buf, "MOD") != NULL) return 1; // include MOD
    
    return 0; 
}

/**
 * If get cmd(`SAVE`), the function will be call to init RDB dump.
 */
void pdb_init_aof(){
    if (!global_conf.is_aof){
        return;
    }

    pid_t pid = fork();
    if (pid == 0){
        // child process
        pdb_log_info("Child process in AOF: RDB dump begin\n");
        int ret = pdb_rdb_dump(global_conf.dump_dir);
        if (ret != PDB_OK){
            pdb_log_error("child process in AOF failed: init RDB failed\n");
        }
        pdb_log_info("Child process in AOF: RDB dump end\n");
        pdb_log_debug("sleep begin....\n");
        sleep(10);
        pdb_log_debug("sleep end.....\n");
        exit(0);
    }else if (pid > 0){
        // father process
        global_dump.is_aof_written = 1;
        global_dump.aof_pid = pid;
    }else{
        pdb_log_error("fork error\n");
    }
}


int pdb_aof_write_to_written_buffer(char* msg, size_t len){
    if (!global_dump.is_aof || !global_dump.is_aof_written){
        return PDB_OK;
    }
    
    if (global_dump.is_aof_written){
        if (strstr(msg, "SAVE")){
            return PDB_OK;
        }
        char* aof_written_buffer_data = (char*)pdb_malloc(len + 1); // '\0'
        memcpy(aof_written_buffer_data, msg, len);
        aof_written_buffer_data[len] = '\0';
        pdb_list_insert(&(global_dump.aof_rewrite_buffer), aof_written_buffer_data);
    }
    
    return PDB_OK;
}
/**
 * If child process finishes RDB dump, the new command in `global_dump->aof_rewrite_buffer` during RDB dump will be sent in this function.
 */
int pdb_is_aof_written_end(){
    if (!global_dump.is_aof){
        return PDB_OK;
    }
    if (!global_dump.is_aof_written){
        return PDB_OK;
    }

    int status;
    struct rusage rusage;
    int fd = global_dump.dump_fd;

    pid_t pid = wait4(global_dump.aof_pid, &status, WNOHANG, &rusage);
    if (pid == 0){
        // Child process does not exit
        return PDB_OK;
    }else if (pid == -1){
        if (errno != ECHILD) perror("wait4 error");
        return PDB_ERROR;
    }
    // printf("buffer: %s\n", (char*)global_dump.aof_rewrite_buffer.head->val);
    // Child process exits, and use writev to send global_dump->aof_rewrite_buffer
    if (pdb_is_list_NULL(&(global_dump.aof_rewrite_buffer))){
        pdb_log_debug("Child process exits, but `global_dump->aof_rewrite_buffer` is NULL\n");
        return PDB_OK;
    }

    pdb_log_info("aof written buffer send begin\n");
    struct iovec iov[1024];
    int i = 0;
    struct pdb_listNode_s* curr = global_dump.aof_rewrite_buffer.head;
    while(curr != NULL && i < 1024){
        iov[i].iov_base = curr->val;
        iov[i].iov_len = strlen(curr->val);  
        curr = curr->next;
        i++;
    }

    close(fd);
    global_dump.dump_fd = open(global_conf.dump_dir, O_RDWR | O_CREAT | O_APPEND, 0644);
    fd = global_dump.dump_fd;
    ssize_t nwritten = writev(fd, iov, i);
    if (nwritten < 0){
        pdb_log_error("writev error\n");
        return PDB_ERROR;
    }

    curr = global_dump.aof_rewrite_buffer.head;
    size_t sum = 0;
    while(curr != NULL){
        pdb_listNode* next = curr->next;
        size_t len = strlen(curr->val);
        size_t old_len = nwritten;
        nwritten -= len;
        if (nwritten > 0){
            pdb_list_delete_node(&(global_dump.aof_rewrite_buffer), curr);
            old_len -= len;
        }else if (nwritten == 0){
            pdb_list_delete_node(&(global_dump.aof_rewrite_buffer), curr);
            break;
        }else{
            // a part of data in this node is sent
            char* tmp = (char*)pdb_malloc(old_len + 1);
            memcpy(tmp, curr->val + old_len, len - old_len);
            tmp[old_len] = '\0';
            free(curr->val);
            curr->val = tmp;
            break;
        }
        curr = next;
    }

    global_dump.is_aof_written = 0;
    pdb_log_info("aof written buffer send successfully\n");
    return PDB_OK;
}


int pdb_aof_buffer_append(char* resp_package, size_t package_len){
    if (!global_dump.is_aof || global_dump.is_aof_written){
        return PDB_OK;
    }

    if (is_need_command(resp_package)){
        global_dump.aof_buffer = pdb_sds_cat_len(global_dump.aof_buffer, resp_package, package_len);
    }
    return PDB_OK;
}


int pdb_aof_dump(){
    if (!global_dump.is_aof || global_dump.is_aof_written){
        return PDB_OK;
    }

    // pdb_log_info("global_dump.aof_buffer: %s\n", global_dump.aof_buffer);
    ssize_t write_len = write(global_dump.dump_fd, global_dump.aof_buffer, pdb_get_sds_len(global_dump.aof_buffer));
    if (write_len != -1){
        pdb_sds_free(global_dump.aof_buffer);
        global_dump.aof_buffer = pdb_get_new_sds(AOF_BUFFER_LEN);
    }else{
        pdb_log_error("write error\n");
        return PDB_ERROR;
    }

    return PDB_OK;
}


int pdb_aof_load(const char* file){
    assert(file != NULL);

    struct timeval tv_begin, tv_end;
    int time_used;
    gettimeofday(&tv_begin, NULL);

    int fd = open(file, O_RDONLY);
    if (fd < 0) {
        pdb_log_info("Failed to open AOF file: %s, Please check RDB file\n", file);
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
        pdb_log_info("No AOF file to load, AOF file is empty\n");
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
            pdb_log_error("AOF file parse failed\n");
            return ret;
        }
        curr += len;
    }

    close(fd);
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    pdb_log_info("AOF load(file size: %lld), time used: %d ms\n", (long long)t.st_size, time_used);

    return PDB_OK;
}