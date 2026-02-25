#ifndef __PDB_CONN_INFO_H__
#define __PDB_CONN_INFO_H__

#include <stdio.h>
#include <assert.h>

#include "pdb_handler.h"
#include "pdb_sds.h"
#include "pdb_list.h"

#define REPLICATION_BUFFER_LENGTH   1024*1024
#define WRITE_BUFFER_LENGTH         16*1024*1024    // 16M
#define PDB_PROTO_IO_BUFFER_LENGTH  16*1024         // 16k     
#define CONNECTION_SIZE			    1024*1024       // 1024 * 1024

typedef int (*RCALLBACK)(int fd, msg_handler handler);

typedef struct conn_info {
    int prev_fd;
    int next_fd;

    int fd;
    int event;
    char* client_ip;
    unsigned short client_port;

    pdb_sds read_buffer;
    size_t read_pos;

    int is_big_package;
    int bulk_length;

    pdb_sds write_buffer;
    size_t write_pos;

    int is_aof_rewrite;
    int is_aof;
    pdb_sds aof_buffer;
    int aof_buffer_pos;
    pdb_list aof_rewrite_buffer;

    char* buffer;                                   // 可读 环形缓冲区
    unsigned int rtail;                             // 尾指针
    unsigned int rhead;                             // 头指针
    
    char* wbuffer;                              // 可写 环形缓冲区
    unsigned int wtail;                         // 尾指针
    unsigned int whead;                         // 头指针

    // ############## reactor ##############################
    RCALLBACK send_callback;
    RCALLBACK recv_callback;
#if 0
	union {
		RCALLBACK recv_callback;
		RCALLBACK accept_callback;
	} r_action;
#endif
    // ###################################################

    int is_slave;                               // 是否为从节点

    // 用于同步过程中，master节点的增量缓冲区
    char* master_to_slave_append_buffer;
    int master_to_slave_append_length;

    // 主从同步是否一直打开
    int is_replication;                 
    // 从节点刚上线时，进行全量同步的状态该连接正在进行主从同步
    int is_full_replication;                         // 0: 同步完成； 1: 正在同步
} conn_info_t;

extern int global_conn_info_list_head;
extern int global_conn_info_list_tail;
extern struct conn_info* conn_list[CONNECTION_SIZE];
extern int active_conn_num;

void pdb_insert_conn_list(int fd);
void pdb_delete_conn_list(int fd);

#endif