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
    if (strstr(buf, "ADD") != NULL) return 1; // include SSADD
    
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
        // pdb_log_info("read aof: %d\n", bytes_read);

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
        int ret = pdb_rdb_dump_raw(global_conf.dump_dir);
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
