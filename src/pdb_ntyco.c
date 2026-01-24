#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

#include "nty_coroutine.h"
#include "pdb_handler.h"
#include "server.h"

static msg_handler handler;


void conn_destroy(struct conn_info* c) {
    if (c) {
        if (c->fd > 0) close(c->fd);
        if (c->buffer) free(c->buffer);
        if (c->wbuffer) free(c->wbuffer);
        free(c);
    }
}


struct conn_info* conn_create(int fd) {
    struct conn_info* c = (struct conn_info*)malloc(sizeof(struct conn_info));
    if (!c) return NULL;
    memset(c, 0, sizeof(struct conn_info));
    
    c->fd = fd;
    c->buffer = (char*)malloc(BUFFER_LENGTH);
    c->wbuffer = (char*)malloc(BUFFER_LENGTH); // 如果只做读缓冲，这个可以暂时不用
    
    // 初始化头尾指针
    c->rhead = c->rtail = 0;
    c->whead = c->wtail = 0;
    
    return c;
}

// 查看是否是需要的命令（get和set）
static int is_need_command(char *buf) {
    if (buf == NULL) return 0;

    if (strstr(buf, "SYN") != NULL) return 0; 

    if (strstr(buf, "SET") != NULL) return 1;   // 包含 SET, MSET, RSET, HSET
    if (strstr(buf, "DEL") != NULL) return 1;   // 包含 DEL, RDEL, HDEL
    if (strstr(buf, "MOD") != NULL) return 1;   // 包含 MOD
    return 0; 
}

static int check_resp_integrity(const char *buf, int len) {
    if (len < 4) return -1;
    if (buf[0] != '*') return -1;

    const char *curr = buf;
    const char *end = buf + len;
    const char *crlf = strstr(curr, "\r\n");
    if (!crlf || crlf >= end) return 0;

    int count = atoi(curr + 1);
    curr = crlf + 2;

    for (int i = 0; i < count; i++) {
        if (curr >= end) return 0;
        if (*curr != '$') return -1;

        crlf = strstr(curr, "\r\n");
        if (!crlf || crlf >= end) return 0;

        int bulk_len = atoi(curr + 1);
        curr = crlf + 2;

        if (curr + bulk_len + 2 > end) return 0;
        curr += bulk_len + 2; 
    }
    return (int)(curr - buf);
}

static void prepare_write_buffer(struct conn_info *c, char *data, int len) {
    unsigned int space = BUFFER_LENGTH - (c->whead - c->wtail);
    if (space < len) {
        fprintf(stderr, "Write buffer overflow\n"); 
        return;
    }
    unsigned int mask_head = c->whead & BUFFER_MASK;
    unsigned int linear_space = BUFFER_LENGTH - mask_head;
    if (len <= linear_space) {
        memcpy(&c->wbuffer[mask_head], data, len);
    } else {
        memcpy(&c->wbuffer[mask_head], data, linear_space);
        memcpy(&c->wbuffer[0], data + linear_space, len - linear_space);
    }
    c->whead += len;
}


static int process_read_buffer(struct conn_info* c, msg_handler handler){
    unsigned int data_len = c->rhead - c->rtail;
    if (data_len == 0) return 0;

    char *temp_buf = malloc(data_len + 1);
    if (!temp_buf) return 0; 
    temp_buf[0] = '\0';
    // memset(temp_buf, 0, data_len + 1);

    unsigned int tail_mask = c->rtail & BUFFER_MASK;
    unsigned int linear = BUFFER_LENGTH - tail_mask;
    if (data_len <= linear) {
        memcpy(temp_buf, &c->buffer[tail_mask], data_len);
    } else {
        memcpy(temp_buf, &c->buffer[tail_mask], linear);
        memcpy(temp_buf + linear, &c->buffer[0], data_len - linear);
    }
    temp_buf[data_len] = '\0'; 

    // if (global_replication.is_master == 0){
    //     printf("temp_buf: %s\n", temp_buf);
    // }

    char* curr = temp_buf;
    int remaining_len = data_len;
    int total_processed_len = 0;

    char *response_out = malloc(RESPONSE_LENGTH);
    if (!response_out) { free(temp_buf); return 0; }
    response_out[0] = '\0';
    // memset(response_out, 0, RESPONSE_LENGTH);

    while(remaining_len > 0){
        int packet_len = check_resp_integrity(curr, remaining_len);
        if (packet_len == 0) break;
        if (packet_len < 0) {
            free(temp_buf);
            free(response_out);
            return packet_len;
        }

        if (global_replication.is_master){
            for (int i = 0; i < global_replication.slave_count; i++){
                struct conn_info* slave = global_replication.master_to_slaves_info[i];
                if (slave->is_full_replication){
                    if (slave->master_to_slave_append_length + packet_len < REPLICATION_BUFFER_LENGTH) {
                        memcpy(slave->master_to_slave_append_buffer + slave->master_to_slave_append_length, 
                            temp_buf, packet_len);
                        slave->master_to_slave_append_length += packet_len;
                    }
                } else if (slave->is_replication){
                    if(is_need_command(temp_buf)){
                        prepare_write_buffer(slave, temp_buf, packet_len);
                        send(slave->fd, temp_buf, packet_len, 0);
                    }
                }
            }
        }

        char backup = curr[packet_len];
        curr[packet_len] = '\0'; 
        response_out[0] = '\0';

        int ret = handler(curr, packet_len, response_out);

        curr[packet_len] = backup;

        if (!global_replication.is_master && 
            c->fd == global_replication.slave_to_master_fd) {
                response_out[0] = '\0';
        }

        if (ret == -9999 && global_replication.is_master){
            c->is_slave = 0;
            c->is_replication = 1;
            int idx = master_add_slave(c); 

            pid_t pid = fork();
            if (pid == 0){
                c->is_full_replication = 1;
                printf("master full syn begin\n");
                perform_rbtree_full_sync_raw(c->fd);
                printf("master rbtree full syn success\n");
                perform_hash_full_sync_raw(c->fd);
                printf("master hash full syn success\n");
                
                c->is_full_replication = 0;
                printf("child thread exit\n");
                _exit(0);
            } else if (pid > 0){
                printf("child pid: %d\n", pid);
                c->master_to_slave_append_buffer = malloc(REPLICATION_BUFFER_LENGTH);
                c->master_to_slave_append_length = 0;
                global_replication.slave_pid[idx] = pid;
            }
        }

        if (ret > 0) {
            prepare_write_buffer(c, response_out, strlen(response_out));
        }

        curr += packet_len;
        remaining_len -= packet_len;
        total_processed_len += packet_len;
    }

    c->rtail += total_processed_len;
    free(temp_buf);
    free(response_out);
    return total_processed_len;
}


void server_reader(void *arg) {
    struct conn_info *c = (struct conn_info *)arg;
    int fd = c->fd;
    int ret = 0;

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    while (1) {
        int writable = BUFFER_LENGTH - (c->rhead - c->rtail);
        if (writable <= 0) {
            nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
            continue; 
        }

        int rhead_idx = c->rhead & BUFFER_MASK;
        int linear_writable = BUFFER_LENGTH - rhead_idx;
        int recv_len = (writable < linear_writable) ? writable : linear_writable;

        ret = recv(fd, c->buffer + rhead_idx, recv_len, 0);

        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
                continue; 
            }
            break; 
        } else if (ret == 0) {
            break; 
        }

        c->rhead += ret;
        process_read_buffer(c, handler);
        
        // 发送回复
        int send_len = c->whead - c->wtail;
        if (send_len > 0) {
            int wtail_idx = c->wtail & BUFFER_MASK;
            int linear_send = BUFFER_LENGTH - wtail_idx;
            int real_send_len = (send_len < linear_send) ? send_len : linear_send;

            int sent = send(fd, c->wbuffer + wtail_idx, real_send_len, 0);
            if (sent > 0) {
                c->wtail += sent;
            } else if (sent < 0 && errno != EAGAIN) {
                break;
            }
        }
    }
    conn_destroy(c); 
}


void slave_routine(void *arg) {
    int fd = *(int *)arg;

    printf("[SLAVE] Coroutine taking over FD: %d\n", fd);

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    struct conn_info *c = conn_create(fd);
    if (!c) { close(fd); return; }

    while (1) {
        int writable = BUFFER_LENGTH - (c->rhead - c->rtail);
        if (writable <= 0) {
            nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
            continue;
        }

        int rhead_idx = c->rhead & BUFFER_MASK;
        int linear_writable = BUFFER_LENGTH - rhead_idx;
        int recv_len = (writable < linear_writable) ? writable : linear_writable;

        int ret = recv(fd, c->buffer + rhead_idx, recv_len, 0);

        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
                continue;
            }
            printf("[SLAVE] Disconnected from Master.\n");
            break;
        } else if (ret == 0) {
            printf("[SLAVE] Master closed connection.\n");
            break;
        }

        c->rhead += ret;
        process_read_buffer(c, handler);
        c->wtail = c->whead; // Slave 不回复，重置写指针
    }

    conn_destroy(c);
}

void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in local;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

    listen(fd, 20);
    printf("listen port : %d\n", port);

    while (1) {
        struct sockaddr_in remote;
        socklen_t len = sizeof(struct sockaddr_in);
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);

        if (cli_fd < 0) continue;
        
        struct conn_info *c = conn_create(cli_fd);
        if (!c) {
            close(cli_fd);
            continue;
        }

        nty_coroutine *read_co;
        nty_coroutine_create(&read_co, server_reader, c);
    }
}

int ntyco_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler) {
    handler = request_handler;

    nty_coroutine *co_server = NULL;
    nty_coroutine_create(&co_server, server, &port);

    if (!global_replication.is_master) {
        // [修改] 直接传递已经在 main 函数中连接好的 FD
        // 前提：main 函数必须成功连接并赋值给了 global_replication.slave_to_master_fd
        if (global_replication.slave_to_master_fd > 0) {
            nty_coroutine *co_slave = NULL;
            // 传递 FD 的地址
            nty_coroutine_create(&co_slave, slave_routine, &global_replication.slave_to_master_fd);
        } else {
            printf("[ERROR] Slave mode but no master connection FD found!\n");
        }
    }

    nty_schedule_run();
    return 0;
}