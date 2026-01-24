#ifndef __SERVER_H__
#define __SERVER_H__

#include "pdb_handler.h"
#include "pdb_replication.h"
#include "pdb_conninfo.h"

#define BUFFER_LENGTH		1024*1024*32
#define RESPONSE_LENGTH		1024*1024*32
#define BUFFER_MASK     (BUFFER_LENGTH - 1)

#define ENABLE_HTTP         0
#define ENABLE_WEBSOCKET    0
#define ENABLE_KVSTORE      1



// typedef int (*RCALLBACK)(int fd, msg_handler handler);

// struct conn_info_reactor {
// 	int fd;

// 	char* rbuffer;
// 	int rtail;
// 	int rhead;

// 	char* wbuffer;
// 	int whead;
// 	int wtail;

// 	RCALLBACK send_callback;

// 	union {
// 		RCALLBACK recv_callback;
// 		RCALLBACK accept_callback;
// 	} r_action;

// 	int status;

// 	int is_slave;                               // 是否为从节点

//     // 用于同步过程中，master节点的增量缓冲区
//     char* master_to_slave_append_buffer;
//     int master_to_slave_append_length;

//     // 主从同步是否一直打开
//     int is_replication;                 
//     // 从节点刚上线时，进行全量同步的状态该连接正在进行主从同步
//     int is_full_replication;                         // 0: 同步完成； 1: 正在同步

// #if ENABLE_WEBSOCKET // websocket
// 	char *payload;
// 	char mask[4];
// #endif
// };

#if ENABLE_HTTP
int http_request(struct conn *c);
int http_response(struct conn *c);
#endif

#if ENABLE_WEBSOCKET
int ws_request(struct conn *c);
int ws_response(struct conn *c);
#endif

#if ENABLE_KVSTORE
int kvs_request(struct conn_info *c, msg_handler handler);
int kvs_response(struct conn_info *c, msg_handler handler);

#endif

#endif