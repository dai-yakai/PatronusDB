#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <errno.h>
#include <sys/time.h>
#include <stdlib.h>
#include <sys/wait.h>



#include "server.h"


#define CONNECTION_SIZE			1024*1024 // 1024 * 1024

#define MAX_PORTS			1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int accept_cb(int fd, msg_handler handler);
int recv_cb(int fd, msg_handler handler);
int send_cb(int fd, msg_handler handler);

int epfd = 0;

struct conn_info conn_list[CONNECTION_SIZE] = {0};

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

// 查看是否是需要的命令（get和set）
static int is_need_command(char *buf) {
    if (buf == NULL) return 0;
    
    if (strstr(buf, "SYN") != NULL) return 0; 
    
    if (strstr(buf, "SET") != NULL) return 1; // 包含 SET, MSET, RSET, HSET
    if (strstr(buf, "DEL") != NULL) return 1; // 包含 DEL, RDEL, HDEL
    if (strstr(buf, "MOD") != NULL) return 1; // 包含 MOD
    
    return 0; 
}

int set_event(int fd, int event, int flag) {

	if (flag) {  // non-zero add

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

	} else {  // zero mod

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
		
	}
}


static int check_resp_integrity(const char *buf, int len) {
	if (buf[0] != '*') {
        printf("[DEBUG] Protocol Mismatch! Expected '*', got '%c' (0x%02X). Len: %d\n", 
               buf[0], (unsigned char)buf[0], len);
        return -1;
    }

    if (len < 4){
        return 0; 
    }
    if (buf[0] != '*') {
		return -1;
	} 

    const char *curr = buf;
    const char *end = buf + len;

    // 解析数组个数
    const char *crlf = strstr(curr, "\r\n");
    if (!crlf || crlf >= end) {
        return 0;
    }

    int count = atoi(curr + 1);
    curr = crlf + 2;

    for (int i = 0; i < count; i++) {
        if (curr >= end){
            return 0;
        } 
        if (*curr != '$') return -1;

        crlf = strstr(curr, "\r\n");
        if (!crlf || crlf >= end) {
            return 0;
        }

        int bulk_len = atoi(curr + 1);
        curr = crlf + 2;

        if (curr + bulk_len + 2 > end) {
            return 0; // 数据还没收齐
        }
        
        
        curr += bulk_len + 2; 
    }
    return (int)(curr - buf);
}

static void prepare_write_buffer(struct conn_info* c, char* data, int len) {
    unsigned int space = BUFFER_LENGTH - (c->whead - c->wtail);
    if (space < len) {
        fprintf(stderr, "Write buffer overflow, dropping data\n"); // 简单丢弃或扩容
        return;
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
}

static int process_read_buffer(struct conn_info* c, msg_handler handler){
	// 从环形缓冲区中把数据取出来，放入temp_buf中
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

	char* curr = temp_buf;
	int remaining_len = data_len;
    int total_processed_len = 0;

	char *response_out = malloc(RESPONSE_LENGTH);
	if (!response_out) return 0;
	response_out[0] = '\0';

	// memset(response_out, 0, RESPONSE_LENGTH);

	while(remaining_len > 0){
		int packet_len = check_resp_integrity(curr, remaining_len);

		if (packet_len == 0){
			break;
		}

		if (packet_len < 0){
		// 协议格式错误
			free(temp_buf);
			int i;
			for (i = 0; i < global_replication.slave_count; i++){
				if (c->fd == global_replication.master_to_slaves_info[i]->fd){
					printf("response_out: %s\n", response_out);
					memset(response_out, 0, strlen(response_out));
				}
			}
			return packet_len;
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
						set_event(slave->fd, EPOLLOUT, 0);
					}
				}
			}
		}

		char backup = curr[packet_len];
        curr[packet_len] = '\0'; 
        
		response_out[0] = '\0';
        // memset(response_out, 0, RESPONSE_LENGTH);

		int ret = handler(curr, packet_len, response_out);

		curr[packet_len] = backup;

		if (!global_replication.is_master && 
			c->fd == global_replication.slave_to_master_fd) {
				response_out[0] = '\0';
			// memset(response_out, 0, RESPONSE_LENGTH); 
		}
		
		// master节点收到了syn同步命令
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


int kvs_request(struct conn_info *c, msg_handler handler){
    // printf("reactor--> recv: %s, length: %d\n", c->rbuffer, c->rlength);
	int total_processed = 0;
	int res;

	while ((res = process_read_buffer(c, handler)) > 0) {
        total_processed += res;
    }

	if (res < 0) {
        printf("Protocol Error\n");
        return -1; // 协议解析失败
    }

	if (total_processed == 0){
        return 0;
	}

	return 1;

}

int kvs_response(struct conn_info *c, msg_handler handler){
	int len = get_readable_length(c->wtail, c->whead);
    if (len == 0) {
        return 0; // 没数据发，直接返回
    }
	int whead_idx = c->wtail & BUFFER_MASK;

    int ret = send(c->fd, &c->wbuffer[whead_idx], len, 0);
	if (ret < 0){
		printf("kvs_response: send error\n");
		return -1;
	}
	c->wtail += ret;

	return ret;
}


int event_register(int fd, int event) {
	if (fd < 0) return -1;

	conn_list[fd].fd = fd;
	conn_list[fd].recv_callback = recv_cb;
	conn_list[fd].send_callback = send_cb;

	conn_list[fd].buffer = (char*)malloc(BUFFER_LENGTH);
	memset(conn_list[fd].buffer, 0, BUFFER_LENGTH);
	conn_list[fd].rhead = 0;
	conn_list[fd].rtail = 0;

	conn_list[fd].wbuffer = (char*)malloc(BUFFER_LENGTH);
	memset(conn_list[fd].wbuffer, 0, BUFFER_LENGTH);
	conn_list[fd].whead = 0;
	conn_list[fd].wtail = 0;

	set_event(fd, event, 1);
}


// listenfd(sockfd) --> EPOLLIN --> accept_cb
int accept_cb(int fd, msg_handler handler) {
	struct sockaddr_in  clientaddr;
	socklen_t len = sizeof(clientaddr);

	int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
	if (clientfd < 0) {
		printf("accept errno: %d --> %s\n", errno, strerror(errno));
		return -1;
	}
	
	event_register(clientfd, EPOLLIN);  // | EPOLLET

	return 0;
}

int recv_cb(int fd, msg_handler handler){
	struct conn_info c = conn_list[fd];

	int nread;

	nread = read(fd, c.read_buffer);
}

#if 0
int recv_cb(int fd, msg_handler handler) {
	struct conn_info* c =  &conn_list[fd];

	// 计算环形缓冲区中能接收的长度 
    int rhead_idx = c->rhead & BUFFER_MASK;
    int len = get_writable_length(c->rtail, c->rhead);
    if (len == 0) {
		// 缓冲区满了，无法继续接收新的数据
		process_read_buffer(c, handler);
		printf("c->rbuffer:%s\n", c->buffer);
		printf("recv_cb: recv buffer is full\n");
        return 1; 
    }

	int count = recv(fd, &c->buffer[rhead_idx], len, 0);
	if (count == 0) { // disconnect
		printf("client disconnect: %d\n", fd);
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
		close(fd);

		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL); // unfinished

		return 0;
	} else if (count < 0) { // 
		printf("count: %d, errno: %d, %s\n", count, errno, strerror(errno));
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
		close(fd);
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);

		return -1;
	}

	c->rhead += count;

    int ret = kvs_request(&conn_list[fd], handler);
	if (ret == 0){
		return count;
	}else if (ret < 0){
		return -1;
	}
	

	if (conn_list[fd].whead > conn_list[fd].wtail) {
        set_event(fd, EPOLLOUT, 0); 
    } else {
        set_event(fd, EPOLLIN, 0); 
    }

	return count;
}


int send_cb(int fd, msg_handler handler) {
    int ret = kvs_response(&conn_list[fd], handler);
	if (ret == 99){
		return -99;
	}

	int count = 0;

	set_event(fd, EPOLLIN, 0);

	return count;
}
#endif


int init_server(unsigned short port) {

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in servaddr;
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0
	servaddr.sin_port = htons(port); // 0-1023, 

	if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
		printf("bind failed: %s\n", strerror(errno));
	}

	listen(sockfd, 1024);

	return sockfd;

}

int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler){
    epfd = epoll_create(1);

	int i = 0;

	for (i = 0;i < MAX_PORTS;i ++) {
		
		int sockfd = init_server(port + i);
		
		conn_list[sockfd].fd = sockfd;
		conn_list[sockfd].recv_callback = accept_cb;
		
		set_event(sockfd, EPOLLIN, 1);
	}

	if (global_replication.is_master == 0|| global_replication.slave_to_master_fd > 0){
        // 如果是slave节点
        struct conn_info* slave_to_master_conn_info = (struct conn_info*)malloc(sizeof(struct conn_info));
        memset(slave_to_master_conn_info, 0, sizeof(struct conn_info));

        slave_to_master_conn_info->fd = global_replication.slave_to_master_fd;
        slave_to_master_conn_info->wbuffer = malloc(BUFFER_LENGTH);
        slave_to_master_conn_info->buffer = malloc(BUFFER_LENGTH);
		slave_to_master_conn_info->recv_callback = recv_cb;
		slave_to_master_conn_info->send_callback = send_cb;

		set_event(global_replication.slave_to_master_fd, EPOLLIN, 1);
		conn_list[global_replication.slave_to_master_fd] = *slave_to_master_conn_info;
    }

	while (1) { // mainloop
		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, -1);

		int i = 0;
		for (i = 0;i < nready;i ++) {
			int connfd = events[i].data.fd;

			if (events[i].events & EPOLLIN) {
				int ret = conn_list[connfd].recv_callback(connfd, request_handler);
				if (ret < 0){
					return -1;
				}	
			} 

			if (events[i].events & EPOLLOUT) {
				int ret = conn_list[connfd].send_callback(connfd, response_handler);
				if (ret < 0){
					return -1;
				}
			}
		}

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
                        set_event(c->fd, EPOLLOUT, 0);
                    }
                }
            }
        }
	}
    
    return 0;
}
