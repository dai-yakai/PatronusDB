#ifndef __PDB_DUMP__
#define __PDB_DUMP__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "pdb_sds.h"
#include "pdb_list.h"

#define AOF_BUFFER_LEN                  16*1024

struct pdb_dump_s{
    int dump_fd;

    int is_aof;
    pdb_sds aof_buffer;
    int aof_buffer_pos;
    
    int is_aof_written;
    pdb_list aof_rewrite_buffer;

    pid_t aof_pid;
};

extern struct pdb_dump_s global_dump;

void pdb_init_dump(const char* file);

#endif