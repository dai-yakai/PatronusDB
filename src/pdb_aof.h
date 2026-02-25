#ifndef __PDB_AOF__
#define __PDB_AOF__

#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/uio.h>

#include "pdb_rdb.h"
#include "pdb_list.h"
#include "pdb_conninfo.h"
#include "pdb_dump.h"

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define AOF_REWRITE_LIST_PACKAGE_LEN    16*1024

void pdb_init_aof();
int pdb_aof_buffer_append(char* resp_package, size_t package_len);
int pdb_aof_dump();
int pdb_aof_load(const char* file);
int pdb_aof_write_to_written_buffer(char* msg, size_t len);
int pdb_is_aof_written_end();

#endif