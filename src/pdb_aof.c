#include "pdb_aof.h"
#define AOF_FLUSH_THRESHOLD (32 * 1024)

static inline uint64_t get_current_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static int is_need_command(char *buf) {
    if (buf == NULL) return 0;
    
    if (strstr(buf, "SYN") != NULL) return 0; 
    
    if (strstr(buf, "SET") != NULL) return 1; // include SET, MSET, RSET, HSET
    if (strstr(buf, "DEL") != NULL) return 1; // include DEL, RDEL, HDEL
    if (strstr(buf, "MOD") != NULL) return 1; // include MOD
    
    return 0; 
}


int pdb_aof_load() {
    int ret = pdb_rdb_load(global_conf.dump_dir);
    if (ret != PDB_OK) {
        pdb_log_error("Failed to load RDB dump\n");
        return ret;
    }

    size_t buf_cap = 3 * 1024 * 1024; 
    char* buf = (char*)pdb_malloc(buf_cap);
    if (buf == NULL) return PDB_MALLOC_NULL;

    size_t pos = 0; 
    int exit_code = PDB_OK;

    while (1) {
        if (buf_cap - pos < 1024 * 1024) { 
            buf_cap *= 2;
            pdb_log_info("realloc\n");
            char* new_buf = (char*)pdb_realloc(buf, buf_cap);
            if (new_buf == NULL) {
                pdb_log_error("OOM: Failed to realloc AOF buffer\n");
                exit_code = PDB_MALLOC_NULL;
                goto cleanup;
            }
            buf = new_buf;
        }

        ssize_t bytes_read = read(global_dump.dump_aof_fd, buf + pos, buf_cap - pos);
        if (bytes_read < 0) {
            pdb_log_error("Read AOF file error: %s\n", strerror(errno));
            exit_code = PDB_ERROR;
            break;
        }
        if (bytes_read == 0) break; 
        pdb_log_info("read aof: %d\n", bytes_read);

        pos += bytes_read;
        size_t curr_offset = 0;

        while (curr_offset < pos) {
            int bulk_len = 0;
            int pkg_len = check_resp_integrity(buf + curr_offset, pos - curr_offset, &bulk_len);

            if (pkg_len == PDB_HALF_PACKAGE) {
                break;
            } else if (pkg_len > 0) {
                int p_ret = pdb_protocol(-1, buf + curr_offset, pkg_len, NULL);
                if (p_ret < 0) {
                    pdb_log_info("pkg_len: %d\n", pkg_len);
                    pdb_log_error("AOF load error at offset %zu: protocol execution failed\n", curr_offset);
                    exit_code = PDB_ERROR;
                    goto cleanup;
                }
                curr_offset += pkg_len;
            }  
            else {
                pdb_log_error("AOF File corrupted at offset %zu\n", curr_offset);
                exit_code = PDB_PROTOCAL_ERROR;
                goto cleanup;
            }
        }

        size_t remaining = pos - curr_offset;
        if (remaining > 0) {
            if (curr_offset > 0) {
                memmove(buf, buf + curr_offset, remaining);
            }
        }
        pos = remaining; 
    }

    pdb_log_info("AOF file loaded successfully.\n");

cleanup:
    pdb_free(buf, -1);
    return exit_code;
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
        exit(0);
    }else if (pid > 0){
        // father process
        global_dump.is_aof_written = 1;
        // pdb_log_debug("global_dump.is_aof_written: %d\n", global_dump.is_aof_written);
        global_dump.aof_pid = pid;
    }else{
        pdb_log_error("fork error: %s\n", strerror(errno));
    }
}


ssize_t pdb_aof_write() {
    size_t len = global_dump.aof_buffer_pos;
    if (len == 0) return 0;

    uint64_t now = get_current_ms();
    uint64_t time_since_last_flush = now - global_dump.last_flush_time;

    if (len < AOF_FLUSH_THRESHOLD && time_since_last_flush < 1000) {
        return 0; 
    }

    void *snapshot = pdb_malloc(len);
    if (!snapshot) {
        pdb_log_error("OOM: Failed to allocate snapshot buffer");
        return -1;
    }

    memcpy(snapshot, global_dump.aof_buffer, len);

    global_dump.aof_buffer_pos = 0;
    pdb_set_sds_len(global_dump.aof_buffer, 0);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&global_dump.ring);
    if (!sqe) {
        pdb_log_error("io_uring sqe ring full!");
        pdb_free(snapshot, -1); 
        return -1;
    }

    io_uring_prep_write(sqe, global_dump.dump_aof_fd, snapshot, len, global_dump.aof_file_offset);
    io_uring_sqe_set_data(sqe, snapshot);
    io_uring_submit(&global_dump.ring);

    global_dump.aof_file_offset += len;
    global_dump.last_flush_time = get_current_ms();

    return len;
}

void pdb_aof_reap_uring() {
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(&global_dump.ring, head, cqe) {
        if (cqe->res < 0) {
            fprintf(stderr, "io_uring write error: %s\n", strerror(-cqe->res));
        }

        void *snapshot = io_uring_cqe_get_data(cqe);

        if (snapshot) {
            pdb_free(snapshot, -1);
        }

        count++;
    }

    if (count > 0) {
        io_uring_cq_advance(&global_dump.ring, count);
    }
}


void pdb_write_to_aof_writen_buffer(char* msg, size_t len){
    if (!global_conf.is_aof && !global_dump.is_aof){
        return;
    }

    char temp_buf[128];
    size_t check_len = len < (sizeof(temp_buf) - 1) ? len : (sizeof(temp_buf) - 1);
    
    memcpy(temp_buf, msg, check_len);
    temp_buf[check_len] = '\0';

    if (!is_need_command(temp_buf)) {
        return;
    }

    memcpy(global_dump.aof_buffer + global_dump.aof_buffer_pos, msg, len);
    global_dump.aof_buffer_pos += len;
    pdb_set_sds_len(global_dump.aof_buffer, global_dump.aof_buffer_pos);
}

/**
 * If child process finishes RDB dump, the new command in `global_dump->aof_rewrite_buffer` during RDB dump will be sent in this function.
 */
// int pdb_is_aof_written_end(){
//     // pdb_log_debug("%d, %d\n", global_conf.is_aof, global_dump.is_aof_written);
//     if (!global_conf.is_aof){
//         return PDB_OK;
//     }

//     if (!global_dump.is_aof_written){
//         return PDB_OK;
//     }

//     // pdb_log_debug("pdb_is_aof_written_end\n");
//     int status;
//     struct rusage rusage;
//     int fd = global_dump.dump_fd;

//     pid_t pid = wait4(global_dump.aof_pid, &status, WNOHANG, &rusage);
//     if (pid == 0){
//         // Child process does not exit
//         return PDB_OK;
//     }else if (pid == -1){
//         if (errno != ECHILD) perror("wait4 error");
//         return PDB_ERROR;
//     }

//     pdb_log_info("Child process exits\n");

//     if (global_dump.aof_rewrite_buffer_ebpf_offset == 0){
//         global_dump.is_aof_written = 0;
//         pdb_log_debug("Child process exits, but `global_dump->aof_rewrite_buffer` is NULL\n");
//         return PDB_OK;
//     }

//     fd = global_dump.dump_fd;
//     // open(global_conf.dump_dir, O_RDWR | O_CREAT | O_APPEND, 0644);
//     if (fd < 0) {
//         pdb_log_error("Failed to open RDB file\n");
//         return -1;
//     }

//     int len = write(fd, global_dump.aof_rewrite_buffer_ebpf, global_dump.aof_rewrite_buffer_ebpf_offset);
//     if (len < 0){
//         pdb_log_debug("write to aof file erorr\n");
//     }
//     global_dump.aof_rewrite_buffer_ebpf_offset = 0;

//     // close(fd);

//     global_dump.is_aof_written = 0;
//     pdb_log_info("aof written buffer send successfully\n");
//     return PDB_OK;
// }


// int pdb_aof_dump(){ 
//     if (!global_conf.is_aof || global_dump.is_aof_written){
//         return PDB_OK;
//     }

//     int fd = global_dump.dump_fd;
//     // open(global_conf.dump_dir, O_RDWR | O_CREAT | O_APPEND, 0644);
//     if (fd < 0) {
//         pdb_log_error("Failed to open RDB file\n");
//         return -1;
//     }

//     int is_force = 1;
//     size_t len_to_write = global_dump.aof_buffer_pos;

//     if (len_to_write == 0)  return PDB_OK;

//     struct io_uring_sqe* sqe = io_uring_get_sqe(&global_dump.ring);
//     if (!sqe){
//         io_uring_submit(&global_dump.ring);
//         sqe = io_uring_get_sqe(&global_dump.ring);
//         if (!sqe)   return -1;
//     }
//     // backup old aof_buffer and aof_buffer_pos
//     char* buffer_to_write = global_dump.aof_buffer;
//     len_to_write = global_dump.aof_buffer_pos;
//     // alloc new aof_buffer
//     global_dump.aof_buffer = pdb_get_new_sds(AOF_BUFFER_LEN);
//     global_dump.aof_buffer_pos = 0;

//     io_uring_prep_write(sqe, global_dump.dump_fd, buffer_to_write, len_to_write, -1);
//     io_uring_sqe_set_data(sqe, buffer_to_write);
//     io_uring_submit(&global_dump.ring);

//     global_dump.last_flush_time = get_current_ms();
//     return PDB_OK;
// }


// void pdb_is_aof_sqe_complete(){
//     if (!global_dump.is_aof)    return;

//     struct io_uring_cqe* cqe;
//     unsigned head;

//     io_uring_for_each_cqe(&global_dump.ring, head, cqe){
//         char* aof_buffer = io_uring_cqe_get_data(cqe);
//         if (cqe->res < 0){
//             pdb_log_error("io_uring write error\n");
//         }

//         if (aof_buffer){
//             // pdb_log_info("io_uring success\n");
//             pdb_sds_free(aof_buffer);
//         }
//         io_uring_cqe_seen(&global_dump.ring, cqe);
//     }
// }

// extern int pdb_incre_serialize(void* dataStructure, const char* key, char* buf, size_t buf_len, size_t* offset, uint8_t opcode);
// extern int pdb_incre_deserialize(const char* buf, size_t buf_len, size_t* offset);
// int pdb_aof_incrememtal_append(void* dataStructure, const char* key, uint8_t opcode){
//     int ret;
//     if (global_dump.is_aof_written){
//         char* buf = global_dump.aof_rewrite_buffer_ebpf;
//         size_t* offset = &global_dump.aof_rewrite_buffer_ebpf_offset;

//         size_t total_len = pdb_get_sds_len(buf);
//         ret = pdb_incre_serialize(dataStructure, key, buf, total_len, offset, opcode);
//         // global_dump.aof_rewrite_buffer_ebpf_offset = *offset;
//         pdb_set_sds_len(global_dump.aof_rewrite_buffer_ebpf, global_dump.aof_rewrite_buffer_ebpf_offset);
//     }else{
//         char* buf = global_dump.aof_buffer;
//         size_t* offset = &global_dump.aof_buffer_pos;

//         size_t total_len = pdb_get_sds_alloc(buf);
//         // pdb_log_info("total_len: %d\n", total_len);
//         ret = pdb_incre_serialize(dataStructure, key, buf, total_len, offset, opcode);
//         // global_dump.aof_buffer_pos = offset;
//         pdb_set_sds_len(global_dump.aof_buffer, global_dump.aof_buffer_pos);
//     }

//     pdb_is_aof_written_end();
//     pdb_aof_dump(0);

//     return ret;
// }

// int pdb_aof_buffer_append_bitmap(void* dataStructure, const char* key, uint64_t bit_offset, int val){
//     pdb_sds buf = global_dump.aof_buffer;
//     size_t* offset_ptr = &global_dump.aof_buffer_pos;
//     size_t start_offset = *offset_ptr;
//     size_t total_len = pdb_get_sds_alloc(buf);
    
//     uint8_t opcode = PDB_OPCODE_BITMAP;
//     size_t key_len = (key == NULL) ? 0 : strlen(key);

//     // printf("[AOF_DEBUG] >>> Enter Bitmap Append: Key=%s, Offset_val=%zu, Buf_ptr=%p, Alloc=%zu\n", 
//     //        key ? key : "NULL", start_offset, (void*)buf, total_len);

    
//     #define CHECK_AND_APPEND(func_call, name) do { \
//         size_t prev_off = *offset_ptr; \
//         if ((func_call) != PDB_RETURN_OK) { \
//             printf("[AOF_DEBUG] ERROR at %s: prev_off=%zu, current_off=%zu\n", name, prev_off, *offset_ptr); \
//             return -3; \
//         } \
//         if (*offset_ptr > total_len || *offset_ptr < prev_off) { \
//             printf("[AOF_DEBUG] CRITICAL: Offset corrupted after %s! %zu -> %zu (Max: %zu)\n", \
//                    name, prev_off, *offset_ptr, total_len); \
//         } \
//     } while(0)

//     CHECK_AND_APPEND(_pdb_append_uint8(buf, total_len, offset_ptr, opcode), "OPCODE");
//     CHECK_AND_APPEND(_pdb_append_uint32(buf, total_len, offset_ptr, key_len), "KEY_LEN");
//     CHECK_AND_APPEND(_pdb_append_string(buf, total_len, offset_ptr, (char*)key), "KEY_STR");
//     CHECK_AND_APPEND(_pdb_append_uint64(buf, total_len, offset_ptr, bit_offset), "BIT_OFFSET");
//     CHECK_AND_APPEND(_pdb_append_int(buf, total_len, offset_ptr, val), "VALUE");

//     #undef CHECK_AND_APPEND

//     // printf("[AOF_DEBUG] <<< Exit Bitmap Append Success: New_Offset=%zu, Written_Len=%zu\n", 
//     //        *offset_ptr, (*offset_ptr - start_offset));

//     pdb_aof_dump();
//     return 0;
// }

// static void pdb_open_aof_file(const char* file){
//     if (global_conf.is_backup){
//         pdb_log_debug("backup\n");
//         struct stat st;
//         if (stat(file, &st) != 0) {
//             return;
//         }
//         time_t rawtime;
//         time(&rawtime);
//         char backup_name[1024];
//         snprintf(backup_name, sizeof(backup_name), "%s.%ld", file, (long)rawtime);
        
//         if (rename(file, backup_name) == 0) {
//             // printf("[Backup] Success: %s -> %s\n", file, backup_name);
//         } else {
//             pdb_log_error("back up aof file error\n");
//             // perror("[Backup] Error");
//         }
//     }

//     int fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);
//     if (fd < 0){
//         pdb_log_error("open aof file error, errno: %d, reason: %s\n", errno, strerror(errno));
//     }
//     pdb_log_info("open aof file fd: %d\n", fd);
//     global_dump.dump_fd = fd;
// }

