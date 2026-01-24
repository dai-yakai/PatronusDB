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

#define MAX_SLAVES 100
#define DIRECT_SEND_THRESHOLD 1024*1024*32
#define SYNC_BUFFER_SIZE (1024*1024*32) // 64KB 缓冲区

struct conn_info;

struct replication_s{
    int is_master;          // 1: Master, 0: Slave
    int slave_to_master_fd;          // 如果是Slave，保存连接Master的socket
    char* master_ip;
    unsigned short master_port;
    
    // 如果是Master，保存所有Slave的conn_info
    struct conn_info* master_to_slaves_info[MAX_SLAVES]; 
    int slave_count;

    pid_t slave_pid[MAX_SLAVES];        // 记录正在进行全量同步的子进程的pid
} ;

extern struct replication_s global_replication;

int connect_master(const char* ip, int port);

int master_add_slave(struct conn_info* c);
void master_del_slave(struct conn_info* c);

void perform_rbtree_full_sync(int fd);
void perform_rbtree_full_sync_raw(int fd);

void perform_hash_full_sync(int fd);
void perform_hash_full_sync_raw(int fd);

#endif