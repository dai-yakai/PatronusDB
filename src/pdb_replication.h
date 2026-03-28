#ifndef __PDB_REPICATION_H__
#define __PDB_REPICATION_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/syscall.h>

#include "pdb_conninfo.h"
#include "pdb_rbtree.h"
#include "pdb_array.h"
#include "pdb_hash.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"
#include "pdb_bitmap.h"
#include "pdb_value.h"
#include "pdb_rdma.h"

#define MAX_SLAVES              100
#define DIRECT_SEND_THRESHOLD   1024*1024*32
#define SYNC_BUFFER_SIZE        1024*1024*32 
#define SYNC_RDMA_MEM_SIZE      1024*1024*1024

struct conn_info;
extern int master_fd;


int connect_master(const char* ip, int port);
void pdb_init_replication();


#endif