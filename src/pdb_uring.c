#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "pdb_handler.h"
#include "pdb_replication.h"
#include "pdb_conninfo.h"
#include "pdb_core.h"

#include "pdb_hash.h"
#include "pdb_rbtree.h"
#include "pdb_array.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2

#define BUFFER_LENGTH   (32 * 1024 * 1024)
#define RESPONSE_LENGTH (32 * 1024 * 1024)
#define QUEUE_DEPTH     4096
#define BUFFER_MASK     (BUFFER_LENGTH - 1)

#define CQE_BATCH       128
#define MAX_ARGS        10

static msg_handler handler;

// 计算环形缓冲区可用长度
int ringBufferLen(unsigned int tail, unsigned int head){
    int rhead_idx = head & BUFFER_MASK;
    int space_left = BUFFER_LENGTH - (head - tail);
    int linear_space = BUFFER_LENGTH - rhead_idx;
    int len = (space_left < linear_space) ? space_left : linear_space;

    return len;
}

// 计算环形缓冲区可读（buffer）的长度（如果读操作需要回绕，就分两次）
static int get_writable_length(unsigned int tail, unsigned int head) {
    int head_idx = head & BUFFER_MASK;
    int space_left = BUFFER_LENGTH - (head - tail); // 总共剩余空间
    int linear_space = BUFFER_LENGTH - head_idx;    // 物理数组尾部剩余空间
    
    // 取二者较小值
    return (space_left < linear_space) ? space_left : linear_space;
}

// 计算环形缓冲区（wbuffer）可写的长度（如果写操作需要回绕，就分两次）
static int get_readable_length(unsigned int tail, unsigned int head) {
    int tail_idx = tail & BUFFER_MASK;
    int data_len = head - tail;                  // 总共有的数据量
    int linear_len = BUFFER_LENGTH - tail_idx;   // 从 tail 到数组末尾的数据量
    
    return (data_len < linear_len) ? data_len : linear_len;
}

static int check_resp_integrity(const char *buf, int len) {
    if (buf[0] != '*') return -1; // 必须以 * 开头

    if (len < 4){
        // printf("check_resp_integrity: buf_len < 4\n");
        return 0; // 至少 *1\r\n
    }
    

    const char *curr = buf;
    const char *end = buf + len;

    // 解析数组个数
    const char *crlf = strstr(curr, "\r\n");
    if (!crlf || crlf >= end) {
        // printf("[DEBUG] Failed at Bulk Len. Hex Dump:\n");
            // for(int k=0; k<len; k++) printf("%02X ", (unsigned char)buf[k]);
             //printf("\n");
        return 0;
    }

    int count = atoi(curr + 1);
    curr = crlf + 2;

    for (int i = 0; i < count; i++) {
        if (curr >= end){
            // printf("check_resp_integrity: error\n");
            return 0;
        } 
        if (*curr != '$') return -1;

        crlf = strstr(curr, "\r\n");
        if (!crlf || crlf >= end) {
            // printf("check_resp_integrity: !crlf || crlf >= end\n");
            return 0;
        }

        int bulk_len = atoi(curr + 1);
        curr = crlf + 2;

        if (curr + bulk_len + 2 > end) {
            // printf("check_resp_integrity: curr + bulk_len + 2 > end\n");

            return 0; // 数据还没收齐
        }
        
        
        curr += bulk_len + 2; 
    }
    return (int)(curr - buf);
}

/*********************** 提交事件 *************************/

static inline void submit_accept(struct io_uring *ring, int sockfd,
                                 struct sockaddr *addr, socklen_t *len)
{
    printf("submit_accept: %d\n", sockfd);
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        // 队列满了，强制提交当前任务
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        
        if (!sqe) {
            fprintf(stderr, "Fatal: SQ Ring Full and flush faile111111d!\n");
            exit(1);
        }
    }

    conn_info_t *c = malloc(sizeof(conn_info_t));
    memset(c, 0, sizeof(conn_info_t));

    c->fd = sockfd;
    c->event = EVENT_ACCEPT;

    c->buffer = malloc(BUFFER_LENGTH);
    c->wbuffer = malloc(BUFFER_LENGTH);
    if (!c->buffer || !c->wbuffer) {
        fprintf(stderr, "Fatal: malloc buffers failed\n");
        exit(1);
    }

    c->rhead = 0;
    c->rtail = 0;
    c->whead = 0;
    c->wtail = 0;

    io_uring_prep_accept(sqe, sockfd, addr, len, 0);
    io_uring_sqe_set_data(sqe, c);
}


static inline void submit_read(struct io_uring *ring, int fd, conn_info_t *c)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        // 队列满了，强制提交任务
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        
        if (!sqe) {
            fprintf(stderr, "Fatal: SQ Ring Full and flush failed!\n");
            exit(1);
        }
    }

    // 计算环形缓冲区中能接收的长度 
    int rhead_idx = c->rhead & BUFFER_MASK;
    int len = get_writable_length(c->rtail, c->rhead);
    if (len == 0) {
        // 缓冲区满了，无法继续接收新的数据
        return; 
    }

    c->event = EVENT_READ;

    io_uring_prep_recv(sqe, fd, &c->buffer[rhead_idx], len, 0);
    io_uring_sqe_set_data(sqe, c);
}

static inline void submit_write(struct io_uring *ring, int fd,
                                conn_info_t *c)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        // 队列满了，强制提交当前任务
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        
        if (!sqe) {
            fprintf(stderr, "Fatal: SQ Ring Full and flush failed!\n");
            exit(1);
        }
    }

    int len = get_readable_length(c->wtail, c->whead);
    if (len == 0) {
        return; // 没数据发，直接返回
    }

    c->event = EVENT_WRITE;
    // memcpy(c->wbuffer, data, len);

    int whead_idx = c->wtail & BUFFER_MASK;
    io_uring_prep_send(sqe, fd, &c->wbuffer[whead_idx], len, 0);
    io_uring_sqe_set_data(sqe, c);
}

static int prepare_write_buffer(conn_info_t *c, char *data, int len) {
    unsigned int space = BUFFER_LENGTH - (c->whead - c->wtail);
    if (space < len) {
        printf("c->wbuffer: %s\n", c->wbuffer);
        fprintf(stderr, "Write buffer overflow, dropping data\n"); // 简单丢弃或扩容
        return -1;
    }

    unsigned int mask_head = c->whead & BUFFER_MASK;
    unsigned int linear_space = BUFFER_LENGTH - mask_head;

    if (len <= linear_space) {
        // 可以一次性拷进去
        memcpy(&c->wbuffer[mask_head], data, len);
    } else {
        // 需要分两段拷贝（回绕）
        memcpy(&c->wbuffer[mask_head], data, linear_space);
        memcpy(&c->wbuffer[0], data + linear_space, len - linear_space);
    }
    c->whead += len;

    return PDB_OK;
}

// 查看是否是需要的命令（get和set）
static int is_need_command(char *buf) {
    if (buf == NULL) return 0;
    
    if (strstr(buf, "SYN") != NULL) return 0; 
    
    if (strstr(buf, "SET") != NULL) return 1; // 包含 SET, MSET, RSET, HSET
    if (strstr(buf, "DEL") != NULL) return 1; // 包含 DEL, RDEL, HDEL
    if (strstr(buf, "MOD") != NULL) return 1; // 包含 MOD
    
    return PDB_OK; 
}

/**
 * @brief (buffer)中提取出指令，并丢给handler进行处理，生成响应
 * 
 * @param c 连接的上下文
 * @param response_out: 指令经过handler处理后生成的响应
 * 
 * @return 有响应，返回1；没响应，返回 <= 0
 */

static int process_read_buffer(struct io_uring* ring, conn_info_t *c, char *response_out) {
    
    unsigned int data_len = c->rhead - c->rtail;
    if (data_len == 0) return 0;

    char *temp_buf = malloc(data_len + 1);
    if (!temp_buf) {
        perror("malloc temp_buf failed");
        return 0; 
    }
    temp_buf[0] = '\0';
    // memset(temp_buf, 0, data_len + 1);

    unsigned int tail_mask = c->rtail & BUFFER_MASK;
    unsigned int linear = BUFFER_LENGTH - tail_mask;
    if (data_len <= linear) {
        memcpy(temp_buf, &c->buffer[tail_mask], data_len);
    } else {
        // 回绕
        memcpy(temp_buf, &c->buffer[tail_mask], linear);
        memcpy(temp_buf + linear, &c->buffer[0], data_len - linear);
    }
    temp_buf[data_len] = '\0'; 

    // 检查是否收齐了一个完整的RESP包
    int packet_len = check_resp_integrity(temp_buf, data_len);
    // printf("check_resp_integrity return: %d\n", packet_len);
    if (packet_len < 0) {
        // printf("#############################\n");
        // printf("temp_buf: %s\n", temp_buf);
        printf("Protocol Error: Not RESP\n");
        int len = sprintf(response_out, "Protocol Error\r\n");
        if (len > 0){
            if (!global_replication.is_master && 
                c->fd == global_replication.slave_to_master_fd) {
                    // printf("response_out: %s\n", response_out);
                    memset(response_out, 0, RESPONSE_LENGTH);
            }
        }else{
            printf("process_read_buffer: write error\n");
        }
        free(temp_buf);
        c->rtail = c->rhead; // 丢弃所有数据
        

        // 如果response发送给从节点或者主节点
        int i;
        for (i = 0; i < global_replication.slave_count; i++){
            if (c->fd == global_replication.master_to_slaves_info[i]->fd){
                printf("response_out: %s\n", response_out);
                memset(response_out, 0, strlen(response_out));
            }
        }
        return 1;
    }
    
    if (packet_len == 0) {
        // printf("process_read_buffer1: %s, %d\n", temp_buf, data_len);
        free(temp_buf);
        return PDB_OK; // 数据不完整，继续等待 io_uring 读更多数据
    }

    // master节点收到从节点同步的命令后，进行增量同步或者直接转发
    if (global_replication.is_master){
        for (int i = 0; i < global_replication.slave_count; i++){
            struct conn_info* slave = global_replication.master_to_slaves_info[i];
            if (slave->is_full_replication){
                // 正在进行全量同步时，将主节点新收到的命令放入同步buffer中
                if (slave->master_to_slave_append_length + packet_len < REPLICATION_BUFFER_LENGTH) {
                    memcpy(slave->master_to_slave_append_buffer + slave->master_to_slave_append_length, 
                           temp_buf, packet_len);
                    slave->master_to_slave_append_length += packet_len;
                } else {
                    fprintf(stderr, "Error: Slave %d replication buffer overflow!\n", slave->fd);
                }
            }else if (slave->is_replication){
                // 转发
                if(is_need_command(temp_buf)){
                    prepare_write_buffer(slave, temp_buf, packet_len);
                    submit_write(ring, slave->fd, slave);
                }
            }
        }
    }
    // printf("process_read_buffer2\n");
    int ret = handler(temp_buf, packet_len, response_out);
    if (!global_replication.is_master && 
        c->fd == global_replication.slave_to_master_fd) {
        memset(response_out, 0, RESPONSE_LENGTH); 
    }
    

    // 主节点收到syn命令
    if (ret == -9999 && global_replication.is_master){
        // printf("ret == -9999\n");
        // 从节点发送SYN，请求同步
        c->is_slave = 0;
        c->is_replication = 1;
        int idx = master_add_slave(c);    //将从节点注册进 全局从节点信息中

        pid_t pid = fork();
        if (pid == 0){
            // master子进程处理同步
            c->is_full_replication = 1;
            printf("master full syn begin\n");
            perform_rbtree_full_sync(c->fd);
            printf("master rbtree full syn success\n");
            perform_hash_full_sync(c->fd);
            printf("master hash full syn success\n");
            c->is_full_replication = 0;

            _exit(0);
        }else if (pid > 0){
            printf("child pid: %d\n", pid);
            c->master_to_slave_append_buffer = malloc(REPLICATION_BUFFER_LENGTH);
            c->master_to_slave_append_length = 0;
            global_replication.slave_pid[idx] = pid;

            // 在子进程结束之前，需要将新接收到SET命令存储到同步缓冲区中，等子进程结束再把新接收到的发送给子进程
        }else{
            perror("process_read_buffer: fork error");
            exit(-99);
        }
    }
    c->rtail += packet_len;

    free(temp_buf);

    return 1;
}


/**
 * @brief ：将响应（data指向的字符串）放入wbuffer中
 * 
 * @param data: 需要写入wbuffer的数据
 * @param len 写入数据(data)的长度
 * 
 * @return 
 */

/*********************** 启动 server *************************/

int uring_entry(unsigned short port, msg_handler request_handler,
                msg_handler response_handler)
{
    handler = request_handler;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    bind(sockfd, (struct sockaddr *)&addr, sizeof(addr));
    listen(sockfd, 128);

    printf("io_uring kvstore listening on %d ...\n", port);

    /************** 创建 io_uring 队列 **************/
    struct io_uring ring;
    io_uring_queue_init(QUEUE_DEPTH, &ring, 0);

    /************** 第一次提交 accept **************/
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    submit_accept(&ring, sockfd, (struct sockaddr *)&clientaddr, &len);

    struct io_uring_cqe* cqes[CQE_BATCH];

    struct __kernel_timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000 * 1000;

    if (global_replication.is_master == 0|| global_replication.slave_to_master_fd > 0){
        // 如果是slave节点
        struct conn_info* slave_to_master_conn_info = (struct conn_info*)malloc(sizeof(struct conn_info));
        memset(slave_to_master_conn_info, 0, sizeof(struct conn_info));

        slave_to_master_conn_info->fd = global_replication.slave_to_master_fd;
        slave_to_master_conn_info->wbuffer = malloc(BUFFER_LENGTH);
        slave_to_master_conn_info->buffer = malloc(BUFFER_LENGTH);

        submit_read(&ring, global_replication.slave_to_master_fd, slave_to_master_conn_info);
    }
    while (1)
    {
        io_uring_submit_and_wait_timeout(&ring, cqes, CQE_BATCH, &ts, NULL);
        // io_uring_submit_and_wait(&ring, 1);
#if 0
        struct io_uring_cqe *cqe;
        io_uring_wait_cqe(&ring, &cqe);
      
        conn_info_t *c = io_uring_cqe_get_data(cqe);
        int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);

        if (res < 0) {
            free(c);
            continue;
        }
#endif 
        int count = io_uring_peek_batch_cqe(&ring, cqes, CQE_BATCH);
        if (count == 0){
            continue;
        }
        int i = 0;
        for (i = 0; i < count; i++){
            struct io_uring_cqe *cqe = cqes[i];
            conn_info_t *c = io_uring_cqe_get_data(cqe);
            int res = cqe->res;

            if (c == NULL){
                printf("c==null\n");
                continue;
            }

            /************************* ACCEPT *************************/
            if (c->event == EVENT_ACCEPT) {
                int connfd = res;
                free(c);

                /* 再提交一次 accept */
                submit_accept(&ring, sockfd, (struct sockaddr *)&clientaddr, &len);

                /* 为新连接分配 conn_info */
                conn_info_t *nc = malloc(sizeof(conn_info_t));
                memset(nc, 0, sizeof(conn_info_t));
                nc->fd = connfd;
                nc->buffer = malloc(BUFFER_LENGTH);
                nc->wbuffer = malloc(BUFFER_LENGTH);
                memset(nc->buffer, 0, BUFFER_LENGTH);
                memset(nc->wbuffer, 0, BUFFER_LENGTH);

                submit_read(&ring, connfd, nc);
            }

            /************************* READ *************************/
            else if (c->event == EVENT_READ) {
                if (res <= 0) { 
                    if (res < 0) {
                        fprintf(stderr, "[Read Error] fd=%d, res=%d\n", c->fd, res);
                    }
                    close(c->fd);
                    free(c);
                    continue; 
                }
#if 0
                /* 去掉 \r\n */
                for (int i = 0; i < res; i++) {
                    if (c->buffer[i] == '\r' || c->buffer[i] == '\n')
                        c->buffer[i] = '\0';
                }
#endif
                c->rhead += res;

                char* response = malloc(RESPONSE_LENGTH);
                if (response == NULL){
                    perror("Fatal: malloc buffers failed\n");
                }
                response[0] = '\0';
                // memset(response, 0, RESPONSE_LENGTH);

                int has_response = 0;

                while (process_read_buffer(&ring, c, response) == PDB_OK) {
                    // 如果 handler 生成了响应，放入写缓冲区
                    // printf("[DEBUG] Logic Check: Handler output len = %lu\n", strlen(response));
                    if (strlen(response) > 0) {
                        // printf("response: %s\n", response);
                        int response_ret = prepare_write_buffer(c, response, strlen(response));
                        if (response_ret == -1){
                            // wbffer满了
                            submit_write(&ring, c->fd, c);
                            break;
                        }
                        has_response = 1;
                        // 清空 response 供下一次循环使用
                        memset(response, 0, sizeof(response));
                    }
                }
                free(response);

                if (has_response) {
                    submit_write(&ring, c->fd, c);
                } else {
                    submit_read(&ring, c->fd, c);
                }
            }

            /************************* WRITE *************************/
            else if (c->event == EVENT_WRITE) {
                if (res < 0) {
                    // 连接断开
                    close(c->fd);
                    free(c->buffer);
                    free(c->wbuffer);
                    free(c);

                    continue; 
                }

                c->wtail += res;

                // 检查是否还有数据没发完
                if (c->whead - c->wtail > 0) {
                    submit_write(&ring, c->fd, c);
                } else {
                    submit_read(&ring, c->fd, c);
                }
            }
        }
        io_uring_cq_advance(&ring, count);

        // 子进程完成主从同步后，处理父进程收到新的命令，将新的命令发送给从节点
        int status;
        pid_t exit_pid = waitpid(-1, &status, WNOHANG);

        if (exit_pid > 0){
            int i;
            for (i = 0; i < global_replication.slave_count; i++){
                if (exit_pid == global_replication.slave_pid[i]){
                    printf("master child thread[pid:%d] syn exit\n", exit_pid);
                    struct conn_info* c = global_replication.master_to_slaves_info[i];
                    if (c->master_to_slave_append_length > 0){
                        prepare_write_buffer(c, c->master_to_slave_append_buffer, c->master_to_slave_append_length);
                        submit_write(&ring, c->fd, c);
                    }
                }
            }
        }
    }

    return PDB_OK;
}
