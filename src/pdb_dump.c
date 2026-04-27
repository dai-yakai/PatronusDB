#include "pdb_dump.h"

#define DEPTH 4096

struct pdb_dump_s global_dump;

void pdb_init_dump(){
    // int rdb_dump_fd = open(global_conf.dump_dir, O_RDWR | O_CREAT | O_TRUNC, 0644);
    // if (rdb_dump_fd < 0){
    //     pdb_log_error("Failed to open dump file, errno: %d, reason: %s\n", errno, strerror(errno));
    // }
    // global_dump.dump_fd = rdb_dump_fd;

    if (global_conf.is_aof){
        int aof_dump_fd = open(global_conf.dump_dir_aof, O_RDWR | O_CREAT | O_APPEND, 0644);
        if (aof_dump_fd < 0){
            pdb_log_error("Failed to open aof dump file, errno: %d, reason: %s\n", errno, strerror(errno));
        }
        global_dump.dump_aof_fd = aof_dump_fd;
    }

    if (global_conf.is_aof){
        // AOF
        global_dump.aof_buffer = pdb_get_new_sds(AOF_BUFFER_LEN);
        global_dump.is_aof = 0;
        global_dump.is_aof_written = 0;
        global_dump.aof_rewrite_buffer = *(pdb_get_NULL_list());
        global_dump.aof_file_offset = 0;

        global_dump.aof_rewrite_buffer_ebpf = pdb_get_new_sds(AOF_BUFFER_LEN);
        global_dump.aof_rewrite_buffer_ebpf_offset = 0;
        global_dump.last_flush_time = 0;

        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        io_uring_queue_init_params(DEPTH, &global_dump.ring, &params);
    }
}