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
        // pdb_log_debug("sleep begin....\n");
        // sleep(10);
        // pdb_log_debug("sleep end.....\n");
        exit(0);
    }else if (pid > 0){
        // father process
        global_dump.is_aof_written = 1;
        pdb_log_debug("global_dump.is_aof_written: %d\n", global_dump.is_aof_written);
        global_dump.aof_pid = pid;
    }else{
        pdb_log_error("fork error\n");
    }
}


/**
 * If child process finishes RDB dump, the new command in `global_dump->aof_rewrite_buffer` during RDB dump will be sent in this function.
 */
int pdb_is_aof_written_end(){
    // pdb_log_debug("%d, %d\n", global_conf.is_aof, global_dump.is_aof_written);
    if (!global_conf.is_aof){
        return PDB_OK;
    }

    if (!global_dump.is_aof_written){
        return PDB_OK;
    }

    // pdb_log_debug("pdb_is_aof_written_end\n");
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

    pdb_log_info("Child process exits\n");

    if (global_dump.aof_rewrite_buffer_ebpf_offset == 0){
        pdb_log_debug("Child process exits, but `global_dump->aof_rewrite_buffer` is NULL\n");
        return PDB_OK;
    }

    fd = global_dump.dump_fd;
    // open(global_conf.dump_dir, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    int len = write(fd, global_dump.aof_rewrite_buffer_ebpf, global_dump.aof_rewrite_buffer_ebpf_offset);
    if (len < 0){
        pdb_log_debug("write to aof file erorr\n");
    }
    global_dump.aof_rewrite_buffer_ebpf_offset = 0;

    // close(fd);

    global_dump.is_aof_written = 0;
    pdb_log_info("aof written buffer send successfully\n");
    return PDB_OK;
}


int pdb_aof_dump(){
    
    if (!global_conf.is_aof || global_dump.is_aof_written){
        // pdb_log_debug("222222\n");
        return PDB_OK;
    }

    int fd = global_dump.dump_fd;
    // open(global_conf.dump_dir, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        pdb_log_error("Failed to open RDB file\n");
        return -1;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&global_dump.ring);
    if (!sqe){
        io_uring_submit(&global_dump.ring);
        sqe = io_uring_get_sqe(&global_dump.ring);
        if (!sqe)   return -1;
    }
    // backup old aof_buffer and aof_buffer_pos
    char* buffer_to_write = global_dump.aof_buffer;
    size_t len_to_write = global_dump.aof_buffer_pos;
    // alloc new aof_buffer
    global_dump.aof_buffer = pdb_get_new_sds(AOF_BUFFER_LEN);
    global_dump.aof_buffer_pos = 0;

    io_uring_prep_write(sqe, global_dump.dump_fd, buffer_to_write, len_to_write, -1);
    io_uring_sqe_set_data(sqe, buffer_to_write);
    io_uring_submit(&global_dump.ring);

    return PDB_OK;
}

void pdb_is_aof_sqe_complete(){
    if (!global_dump.is_aof)    return;

    struct io_uring_cqe* cqe;
    unsigned head;

    io_uring_for_each_cqe(&global_dump.ring, head, cqe){
        char* aof_buffer = io_uring_cqe_get_data(cqe);
        if (cqe->res < 0){
            pdb_log_error("io_uring write error\n");
        }

        if (aof_buffer){
            // pdb_log_info("io_uring success\n");
            pdb_sds_free(aof_buffer);
        }
        io_uring_cqe_seen(&global_dump.ring, cqe);
    }
}

extern int pdb_incre_serialize(void* dataStructure, const char* key, char* buf, size_t buf_len, size_t* offset, uint8_t opcode);
extern int pdb_incre_deserialize(const char* buf, size_t buf_len, size_t* offset);
int pdb_aof_incrememtal_append(void* dataStructure, const char* key, uint8_t opcode){
    int ret;
    if (global_dump.is_aof_written){
        char* buf = global_dump.aof_rewrite_buffer_ebpf;
        size_t* offset = &global_dump.aof_rewrite_buffer_ebpf_offset;

        size_t total_len = pdb_get_sds_len(buf);
        ret = pdb_incre_serialize(dataStructure, key, buf, total_len, offset, opcode);
        // global_dump.aof_rewrite_buffer_ebpf_offset = *offset;
        pdb_set_sds_len(global_dump.aof_rewrite_buffer_ebpf, global_dump.aof_rewrite_buffer_ebpf_offset);
    }else{
        char* buf = global_dump.aof_buffer;
        size_t* offset = &global_dump.aof_buffer_pos;

        size_t total_len = pdb_get_sds_alloc(buf);
        // pdb_log_info("total_len: %d\n", total_len);
        ret = pdb_incre_serialize(dataStructure, key, buf, total_len, offset, opcode);
        // global_dump.aof_buffer_pos = offset;
        pdb_set_sds_len(global_dump.aof_buffer, global_dump.aof_buffer_pos);
    }

    pdb_is_aof_written_end();
    pdb_aof_dump();

    return ret;
}

int pdb_aof_buffer_append_bitmap(void* dataStructure, const char* key, uint64_t bit_offset, int val){
    pdb_sds buf = global_dump.aof_buffer;
    size_t* offset_ptr = &global_dump.aof_buffer_pos;
    size_t start_offset = *offset_ptr;
    size_t total_len = pdb_get_sds_alloc(buf);
    
    uint8_t opcode = PDB_OPCODE_BITMAP;
    size_t key_len = (key == NULL) ? 0 : strlen(key);

    // printf("[AOF_DEBUG] >>> Enter Bitmap Append: Key=%s, Offset_val=%zu, Buf_ptr=%p, Alloc=%zu\n", 
    //        key ? key : "NULL", start_offset, (void*)buf, total_len);

    
    #define CHECK_AND_APPEND(func_call, name) do { \
        size_t prev_off = *offset_ptr; \
        if ((func_call) != PDB_RETURN_OK) { \
            printf("[AOF_DEBUG] ERROR at %s: prev_off=%zu, current_off=%zu\n", name, prev_off, *offset_ptr); \
            return -3; \
        } \
        if (*offset_ptr > total_len || *offset_ptr < prev_off) { \
            printf("[AOF_DEBUG] CRITICAL: Offset corrupted after %s! %zu -> %zu (Max: %zu)\n", \
                   name, prev_off, *offset_ptr, total_len); \
        } \
    } while(0)

    CHECK_AND_APPEND(_pdb_append_uint8(buf, total_len, offset_ptr, opcode), "OPCODE");
    CHECK_AND_APPEND(_pdb_append_uint32(buf, total_len, offset_ptr, key_len), "KEY_LEN");
    CHECK_AND_APPEND(_pdb_append_string(buf, total_len, offset_ptr, (char*)key), "KEY_STR");
    CHECK_AND_APPEND(_pdb_append_uint64(buf, total_len, offset_ptr, bit_offset), "BIT_OFFSET");
    CHECK_AND_APPEND(_pdb_append_int(buf, total_len, offset_ptr, val), "VALUE");

    #undef CHECK_AND_APPEND

    // printf("[AOF_DEBUG] <<< Exit Bitmap Append Success: New_Offset=%zu, Written_Len=%zu\n", 
    //        *offset_ptr, (*offset_ptr - start_offset));

    pdb_aof_dump();
    return 0;
}


int pdb_aof_load(const char* file){
    int time_used;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    int fd = global_dump.dump_fd;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) {
        // file is empty
        return 0; 
    }
    size_t total_size = st.st_size;

    const char* mapped_buf = (const char*)mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped_buf == MAP_FAILED) {
        pdb_log_error("mmap error\n");
        return -1;
    }

    size_t offset = 0;

    pdb_log_debug("aof load begin, offset: %d, total_size: %d\n", offset, total_size);
    // RDB LOAD
    while(offset < total_size){
        uint8_t opcode;
        if (_pdb_read_uint8(mapped_buf, total_size, &offset, &opcode) < 0) break;

        if (opcode == PDB_OPCODE_EOF) {
            pdb_log_info("🚧 Boundary hit at offset %zu! Switching to AOF Engine...\n", offset);
            break; 
        }

        switch (opcode) {
            case PDB_OPCODE_HASH:
                if (pdb_deserialize_hash(mapped_buf, total_size, &offset, &global_hash) < 0) goto load_err;
                break;
            case PDB_OPCODE_ARRAY:
                if (pdb_deserialize_array(mapped_buf, total_size, &offset, &global_array) < 0) goto load_err;
                break;
            case PDB_OPCODE_RBTREE:
                if (pdb_deserialize_rbtree(mapped_buf, total_size, &offset, &global_rbtree) < 0) goto load_err;
                break;
            default:
                pdb_log_error("RDB Engine Corrupted! Unknown structure opcode: 0x%02x at offset %zu\n", opcode, offset);
                goto load_err;
        }
    }

    // AOF LOAD
    while(offset < total_size){
        pdb_incre_deserialize(mapped_buf, total_size, &offset);
    }
    
    
    

    pdb_log_info("RDB Successfully Loaded! Processed %zu bytes.\n", offset);
    munmap((void*)mapped_buf, total_size);
    close(fd);

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    pdb_log_info("RDB load(file size: %lld), time used: %d ms\n", (long long)st.st_size, time_used);
    
    return 0;

load_err:
    munmap((void*)mapped_buf, total_size);
    // close(fd);
    pdb_log_error("aof load error: deserialize error\n");
    return -1;
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