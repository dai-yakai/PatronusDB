#include "pdb_dump.h"

struct pdb_dump_s global_dump;

void pdb_init_dump(const char* file){
    int fd = open(file, O_RDWR | O_CREAT | O_APPEND, 0644);
    global_dump.dump_fd = fd;
    if (global_conf.is_aof){
        // AOF
        global_dump.aof_buffer = pdb_get_new_sds(AOF_BUFFER_LEN);
        global_dump.is_aof = 0;
        global_dump.is_aof_written = 0;
        global_dump.aof_rewrite_buffer = *(pdb_get_NULL_list());
    }
}