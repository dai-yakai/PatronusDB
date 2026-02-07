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
#include "pdb_log.h"


#define CONNECTION_SIZE			1024*1024 // 1024 * 1024
#define MAX_PORTS				1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int accept_cb(int fd, msg_handler handler);
int recv_cb(int fd, msg_handler handler);
int send_cb(int fd, msg_handler handler);

int epfd = 0;
struct conn_info* conn_list[CONNECTION_SIZE] = {0};

// 查看是否是需要的命令（get和set）
// static int is_need_command(char *buf) {
//     if (buf == NULL) return 0;
    
//     if (strstr(buf, "SYN") != NULL) return 0; 
    
//     if (strstr(buf, "SET") != NULL) return 1; // include SET, MSET, RSET, HSET
//     if (strstr(buf, "DEL") != NULL) return 1; // include DEL, RDEL, HDEL
//     if (strstr(buf, "MOD") != NULL) return 1; // include MOD
    
//     return 0; 
// }

/**
 * Check for a complete package in 'buf' and return its total length.
 * 
 * Return PDB_PROTOCAL_ERROR if `buf` violates the RESP.
 * Return PDB_HALF_PACKAGE if `buf` contains a incomplete package(a half package).
 * Return total length if `buf` contains a valid and complete package.
 */
static int check_resp_integrity(const char *buf, int len, int* bulk_length) {
	if (buf[0] != '*') {
        return PDB_PROTOCAL_ERROR;
    }
    if (len < 4){
        return PDB_HALF_PACKAGE; 
    }
    if (buf[0] != '*') {
		return PDB_PROTOCAL_ERROR;
	} 

    const char *curr = buf;
    const char *end = buf + len;

    // Parsing the num of tokens
    const char *crlf = strstr(curr, "\r\n");
    if (!crlf || crlf >= end) {
		return PDB_HALF_PACKAGE;
    }

    int count = atoi(curr + 1);
    curr = crlf + 2;

    for (int i = 0; i < count; i++) {
        if (curr >= end){
            return PDB_HALF_PACKAGE;
        } 
        if (*curr != '$'){
			return PDB_PROTOCAL_ERROR;
		}

        crlf = strstr(curr, "\r\n");
        if (!crlf || crlf >= end) {
            return PDB_HALF_PACKAGE;
        }

		int bulk_len = atoi(curr + 1);
		if (bulk_length != NULL){
			*bulk_length = bulk_len > *bulk_length ? bulk_len : *bulk_length;
		}
		
        curr = crlf + 2;

        if (curr + bulk_len + 2 > end) {
            return PDB_HALF_PACKAGE; 
        }
             
        curr += bulk_len + 2; 
    }
    return (int)(curr - buf);
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


int event_register(int fd, int event) {
	assert(fd >= 0);

	conn_list[fd]->write_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
	conn_list[fd]->write_length = PDB_PROTO_IO_BUFFER_LENGTH;
	conn_list[fd]->read_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
	conn_list[fd]->read_pos = 0;
	conn_list[fd]->write_pos = 0;

	conn_list[fd]->fd = fd;
	conn_list[fd]->recv_callback = recv_cb;
	conn_list[fd]->send_callback = send_cb;

	// conn_list[fd]->buffer = (char*)malloc(BUFFER_LENGTH);
	// memset(conn_list[fd]->buffer, 0, BUFFER_LENGTH);
	// conn_list[fd]->rhead = 0;
	// conn_list[fd]->rtail = 0;

	// conn_list[fd]->wbuffer = (char*)malloc(BUFFER_LENGTH);
	// memset(conn_list[fd]->wbuffer, 0, BUFFER_LENGTH);
	// conn_list[fd]->whead = 0;
	// conn_list[fd]->wtail = 0;

	set_event(fd, event, 1);
}


// listenfd(sockfd) --> EPOLLIN --> accept_cb
int accept_cb(int fd, msg_handler handler) {
	struct sockaddr_in  clientaddr;
	socklen_t len = sizeof(clientaddr);

	int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
	if (clientfd < 0) {
		pdb_log_error("accept errno: %d --> %s\n", errno, strerror(errno));
		return -1;
	}
	conn_list[clientfd] = (struct conn_info*)pdb_malloc(sizeof(struct conn_info));
	assert(conn_list[clientfd] != NULL);
	memset(conn_list[clientfd], 0, sizeof(struct conn_info));

	conn_list[clientfd]->client_ip = (char*)malloc(16);
	assert(conn_list[clientfd]->client_ip != NULL);

	inet_ntop(AF_INET, &clientaddr.sin_addr, conn_list[clientfd]->client_ip, 16);
	conn_list[clientfd]->client_port = ntohs(clientaddr.sin_port);
	event_register(clientfd, EPOLLIN);  // | EPOLLET

	return 0;
}


static void prepare_write_buffer(struct conn_info* c, char* data, int len) {
    memcpy(c->write_buffer + c->write_pos, data, len);
}


static int process_read_buffer(int fd, msg_handler handler){
	struct conn_info* c = conn_list[fd];

	while(c->read_pos > 0){
		int bulk_length = 0;
		size_t tmp1 = c->read_pos;
		size_t tmp2 = (pdb_get_sds_len(c->read_buffer));
		size_t package_len = check_resp_integrity(c->read_buffer, c->read_pos, &bulk_length);
		
		if (bulk_length > PDB_PROTO_IO_BUFFER_LENGTH){
			c->is_big_package = 1;
			c->bulk_length = bulk_length;
		}
		
		if (package_len == PDB_HALF_PACKAGE){
			// pdb_log_debug("process_read_buffer receive half package\n");
			return PDB_HALF_PACKAGE;
		}else if (package_len == PDB_PROTOCAL_ERROR){
			pdb_log_debug("process_read_buffer receive error protocal\n");
			memcpy(c->write_buffer + c->write_pos, "protocal error\r\n", 17);
			return PDB_PROTOCAL_ERROR;
		}

		// Write response directly into `c->write_buffer` to avoid extra allocating.
		int response_len = handler(c->read_buffer, package_len, c->write_buffer + c->write_pos);
		if (response_len > 0){
			// Updata `c->write_length` and `c->write_pos` according to response_len.
			c->write_length += response_len;
			c->write_pos += response_len;
			pdb_sds_len_increment(c->write_buffer, response_len);
		}

		pdb_sds_range(c->read_buffer, package_len, -1);
		c->read_pos -= package_len;
		
		if (c->is_big_package == 1){
			c->is_big_package = -1;
		}
	}

	return PDB_OK;
}


int recv_cb(int fd, msg_handler handler){
	struct conn_info* c = conn_list[fd];
	size_t read_len = PDB_PROTO_IO_BUFFER_LENGTH;
	size_t avail_len = pdb_get_sds_avail(c->read_buffer);
	assert(avail_len >= 0);

	int nread;

	if (c->is_big_package){
		read_len = c->bulk_length;
	}

	if (avail_len < read_len + 2){
		size_t remaining_length = read_len + 2 - avail_len;		// +2:\r\n
		read_len = remaining_length;

		c->read_buffer = pdb_enlarge_sds_greedy(c->read_buffer, read_len);
	}
	
	nread = recv(fd, c->read_buffer + c->read_pos, read_len, 0);
	if (nread < 0){
		pdb_log_error("reactor recv_cb: read error\n");
		return PDB_ERROR;
	}else if (nread == 0){
		pdb_log_info("[%s:%d] disconnect\n", conn_list[fd]->client_ip, conn_list[fd]->client_port);
		return PDB_DISCONNECT;
	}
	c->read_pos += nread;
	pdb_sds_len_increment(c->read_buffer, nread);

	int ret = process_read_buffer(fd, handler);
	if (ret != PDB_OK){
		// pdb_log_debug("reactor recv_cb process_read_buffer return error\n");
		return ret;
	}

	set_event(c->fd, EPOLLOUT, 0);

	return PDB_OK;
}


int send_cb(int fd, msg_handler handler) {
	struct conn_info* c = conn_list[fd];
    int ret = send(fd, c->write_buffer, c->write_pos, 0);
	if (ret < 0){
		pdb_log_error("send error\n");
		return PDB_ERROR;
	}else if (ret > 0){
		pdb_sds_range(c->write_buffer, ret, -1);
		c->write_pos -= ret;
		c->write_length -= ret;
	}

	set_event(fd, EPOLLIN, 0);

	return PDB_OK;
}


int init_server(unsigned short port) {
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	// reusing ports in TIME_WAIT state
	int opt = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0){
		perror("setsockopt error");
		return PDB_ERROR;
	}


	struct sockaddr_in servaddr;
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0
	servaddr.sin_port = htons(port); // 0-1023, 

	if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
		pdb_log_error("bind failed: %s\n", strerror(errno));
	}

	listen(sockfd, 1024);

	return sockfd;
}

static void destroy_conn_info(int fd){
	struct conn_info* c = conn_list[fd];

	assert(c->read_buffer != NULL && c->write_buffer != NULL);
	pdb_sds_free(c->read_buffer);
	pdb_sds_free(c->write_buffer);
}

int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler){
    epfd = epoll_create(1);

	int i = 0;

	for (i = 0;i < MAX_PORTS;i ++) {
		int sockfd = init_server(port + i);
		conn_list[sockfd] = (struct conn_info*)pdb_malloc(sizeof(struct conn_info));
		memset(conn_list[sockfd], 0, sizeof(struct conn_info));
		
		conn_list[sockfd]->fd = sockfd;
		conn_list[sockfd]->recv_callback = accept_cb;
		
		set_event(sockfd, EPOLLIN, 1);
	}

	if (global_replication.is_master == 0|| global_replication.slave_to_master_fd > 0){
        // slave
        struct conn_info* slave_to_master_conn_info = (struct conn_info*)malloc(sizeof(struct conn_info));
        memset(slave_to_master_conn_info, 0, sizeof(struct conn_info));

        slave_to_master_conn_info->fd = global_replication.slave_to_master_fd;
        slave_to_master_conn_info->wbuffer = malloc(BUFFER_LENGTH);
        slave_to_master_conn_info->buffer = malloc(BUFFER_LENGTH);
		slave_to_master_conn_info->recv_callback = recv_cb;
		slave_to_master_conn_info->send_callback = send_cb;

		set_event(global_replication.slave_to_master_fd, EPOLLIN, 1);
		conn_list[global_replication.slave_to_master_fd] = slave_to_master_conn_info;
    }

	while (1) { // mainloop
		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, -1);

		int i = 0;
		for (i = 0;i < nready;i ++) {
			int connfd = events[i].data.fd;
			struct conn_info* c = conn_list[connfd];
			if (events[i].events & EPOLLIN) {
				int ret = c->recv_callback(connfd, request_handler);
				if (ret < 0){
					pdb_log_debug("EPOLLIN call_back return error\n");
				}
				if (c->write_buffer != NULL && c->read_buffer != NULL){
					pdb_log_info("read_buffer size: %d, write_buffer size: %d\n", pdb_get_sds_len(c->read_buffer), pdb_get_sds_len(c->write_buffer));	// DEBUG by DYK
				}
				
				if (ret == PDB_DISCONNECT){
					epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
				}

				if (c->write_pos != 0){
					set_event(connfd, EPOLLOUT, 0);
				}
			} 

			if (events[i].events & EPOLLOUT) {
				int ret = c->send_callback(connfd, response_handler);
				if (ret < 0){
					pdb_log_debug("EPOLLOUT call_back return error\n");
				}
				if (c->write_buffer != NULL && c->read_buffer != NULL){
					pdb_log_info("read_buffer size: %d, write_buffer size: %d\n", pdb_get_sds_alloc(c->read_buffer), pdb_get_sds_alloc(c->write_buffer));	// DEBUG by DYK
				}
				if (ret == PDB_DISCONNECT){
					destroy_conn_info(connfd);
					epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
				}
			}
		}

		// increment replication 
        int status;
        pid_t exit_pid = waitpid(-1, &status, WNOHANG);

        if (exit_pid > 0){
            int i;
            for (i = 0; i < global_replication.slave_count; i++){
                if (exit_pid == global_replication.slave_pid[i]){
                    pdb_log_debug("master child thread[pid:%d] syn exit\n", exit_pid);
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
