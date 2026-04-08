#ifndef __PDB_DUMP__
#define __PDB_DUMP__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <liburing.h>

#include "pdb_sds.h"
#include "pdb_list.h"

#define AOF_BUFFER_LEN                  2 * 1024*1024

struct pdb_dump_s{
    int dump_fd;
    int dump_aof_fd;

    int is_aof;                         // If receive SAVE, is_aof = 1
    pdb_sds aof_buffer;
    size_t aof_buffer_pos;
    size_t aof_file_offset;
    
    int is_aof_written;
    pdb_sds aof_rewrite_buffer_ebpf;
    size_t aof_rewrite_buffer_ebpf_offset;
    pdb_list aof_rewrite_buffer;

    pid_t aof_pid;

    struct io_uring ring;

    uint64_t last_flush_time;
};

extern struct pdb_dump_s global_dump;
void pdb_init_dump();

#endif