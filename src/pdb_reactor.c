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
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/wait.h>

#include "server.h"
#include "pdb_log.h"
#include "pdb_parse_protocol.h"
#include "pdb_aof.h"
#include "pdb_conninfo.h"


#define MAX_PORTS				1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int accept_cb(int fd, msg_handler handler);
int recv_cb(int fd, msg_handler handler);
int send_cb(int fd, msg_handler handler);
extern int pdb_ebpf_poll();

int epfd = 0;



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

	conn_list[fd]->write_buffer = pdb_get_new_sds(20 * 1024 * 1024);
	conn_list[fd]->read_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
	conn_list[fd]->read_pos = 0;
	conn_list[fd]->write_pos = 0;

	conn_list[fd]->fd = fd;
	conn_list[fd]->recv_callback = recv_cb;
	conn_list[fd]->send_callback = send_cb;
	conn_list[fd]->is_aof_rewrite = 0;
	conn_list[fd]->is_aof = 0;

	conn_list[fd]->is_slave = global_conf.is_slave;

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
	int flags = fcntl(clientfd, F_GETFL, 0);
    if (flags == -1) return -1;
    fcntl(clientfd, F_SETFL, flags | O_NONBLOCK);

	pdb_insert_conn_list(clientfd);

	conn_list[clientfd]->client_ip = (char*)malloc(16);
	assert(conn_list[clientfd]->client_ip != NULL);

	inet_ntop(AF_INET, &clientaddr.sin_addr, conn_list[clientfd]->client_ip, 16);
	conn_list[clientfd]->client_port = ntohs(clientaddr.sin_port);
	event_register(clientfd, EPOLLIN);  // | EPOLLET

	return 0;
}

void pdb_write_to_slave(int fd, char* msg, int msg_len){
	if (global_conf.is_slave){
		return;
	}

	int i;
	for (i = 0; i < global_replication->slave_num; i++){
		int slave_fd = global_replication->fd[i];
		// pdb_log_debug("slave_num: %d fd : %d\n", global_replication->slave_num, slave_fd);
		if (!conn_list[slave_fd]->is_incre_ready){
			// pdb_log_debug("fd: %d, is_incre_ready: %d\n", fd, conn_list[fd]->is_incre_ready);
			return ;
		}
		
		memcpy(conn_list[slave_fd]->write_buffer + conn_list[slave_fd]->write_pos, msg, msg_len);
		pdb_sds_len_increment(conn_list[slave_fd]->write_buffer, msg_len); 
		conn_list[slave_fd]->write_pos += msg_len;
		set_event(slave_fd, EPOLLOUT, 0);

		// pdb_log_debug("write to slave[fd %d]: %s\n", fd, msg);
	}
}


static int process_read_buffer(int fd, msg_handler handler){
	struct conn_info* c = conn_list[fd];

	while(c->read_pos > 0){
		int bulk_length = 0;
		size_t package_len = check_resp_integrity(c->read_buffer, c->read_pos, &bulk_length);
		
		if (bulk_length > PDB_PROTO_IO_BUFFER_LENGTH){
			c->is_big_package = 1;
			c->bulk_length = bulk_length;
		}
		
		if (package_len == PDB_HALF_PACKAGE){
			// pdb_log_debug("process_read_buffer receive half package\n");
			return PDB_HALF_PACKAGE;
		}else if (package_len == PDB_PROTOCAL_ERROR){
			pdb_log_debug("process_read_buffer receive error protocal\n%s\n", c->read_buffer);
			memcpy(c->write_buffer + c->write_pos, "protocal error\r\n", 17);
			return PDB_PROTOCAL_ERROR;
		}

		if (global_conf.is_replication){
			pdb_write_to_slave(fd, c->read_buffer, package_len);
		}
		
		// pdb_log_info("read_buffer:%s\n", c->read_buffer);
		// Write response directly into `c->write_buffer` to avoid extra allocating.
		int response_len = handler(fd, c->read_buffer, package_len, c->write_buffer + c->write_pos);
		if (response_len > 0){
			c->write_pos += response_len;
			pdb_sds_len_increment(c->write_buffer, response_len);
		}
		// pdb_log_info("c->write_pos: %d\n", c->write_pos);
		
		// deal with AOF
		// pdb_aof_buffer_append(c->read_buffer, package_len);
		// pdb_aof_write_to_written_buffer(c->read_buffer, package_len);

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
	// pdb_log_info("read_buffer: %d, write_buffer:%d\n", pdb_get_sds_alloc(c->read_buffer), pdb_get_sds_alloc(c->write_buffer));

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
		// pdb_log_info("read_len: %d\n", read_len);
		c->read_buffer = pdb_enlarge_sds_greedy(c->read_buffer, read_len);
	}
	
	while(1){
		nread = recv(fd, c->read_buffer + c->read_pos, read_len, 0);
		if (nread > 0){
			c->read_pos += nread;
			pdb_sds_len_increment(c->read_buffer, nread);
			int ret = process_read_buffer(fd, handler);
			if (ret != PDB_OK){
				//pdb_log_debug("reactor recv_cb process_read_buffer return error\n");
				return ret;
			}
		}else if (nread < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
			break;
		}else{
			pdb_log_info("[%s:%d] disconnect\n", conn_list[fd]->client_ip, conn_list[fd]->client_port);
			return PDB_DISCONNECT;
		}		
	}

	set_event(c->fd, EPOLLOUT, 0);
	
	return PDB_OK;
}


int send_cb(int fd, msg_handler handler) {
	// pdb_log_debug("send_cb : %d, write_buffer: %s\n", fd, conn_list[fd]->write_buffer);
	struct conn_info* c = conn_list[fd];
	if (c->write_pos > 3*1024){
		int ret = send(fd, c->write_buffer, c->write_pos, 0);
		if (ret < 0){
			pdb_log_error("send error\n");
			return PDB_ERROR;
		}else if (ret > 0){
			// pdb_log_info("send success: %d\n", ret);
			pdb_sds_range(c->write_buffer, ret, -1);
			c->write_pos -= ret;
		}
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

void init_replication_slave_to_master_conn_list(int fd){
	int flags = fcntl(fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

	pdb_insert_conn_list(fd);
	conn_list[fd]->write_buffer = pdb_get_new_sds(1100 * 1024);
	conn_list[fd]->read_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
	conn_list[fd]->read_pos = 0;
	conn_list[fd]->write_pos = 0;

	conn_list[fd]->fd = fd;
	conn_list[fd]->recv_callback = recv_cb;
	conn_list[fd]->send_callback = send_cb;
	conn_list[fd]->is_aof_rewrite = 0;
	conn_list[fd]->is_aof = 0;

	conn_list[fd]->is_slave = global_conf.is_slave;
	conn_list[fd]->event = EPOLLIN;
	set_event(fd, EPOLLIN, 1);

	conn_list[fd]->client_ip = (char*)pdb_malloc(16);
    if (conn_list[fd]->client_ip) {
        strcpy(conn_list[fd]->client_ip, global_conf.master_ip); 
    }
    conn_list[fd]->client_port = global_conf.master_port;
}

extern int is_incre_ready;
extern pdb_rdma_snapshot_ctx* incre_master_snap;
int reactor_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler){
    epfd = epoll_create(1);

	int i = 0;

	// listen fd
	for (i = 0; i < MAX_PORTS; i++) {
		int sockfd = init_server(port + i);
		conn_list[sockfd] = (struct conn_info*)pdb_malloc(sizeof(struct conn_info));
		memset(conn_list[sockfd], 0, sizeof(struct conn_info));
		
		conn_list[sockfd]->fd = sockfd;
		conn_list[sockfd]->recv_callback = accept_cb;
		
		set_event(sockfd, EPOLLIN, 1);
	}

	// initialize slave to master connection
	pdb_init_replication();
	
	while (1) { // mainloop
		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, 10);

		if (global_dump.is_aof){
			pdb_ebpf_poll();
		}
		pdb_is_aof_sqe_complete();

		if (nready == 0){
			// epoll_wait timeout
			if (active_conn_num != 0){
				int tmp_fd = global_conn_info_list_head;
				while(tmp_fd != -1){
					if (conn_list[tmp_fd]->write_pos != 0){
						int ret = send(tmp_fd, conn_list[tmp_fd]->write_buffer, conn_list[tmp_fd]->write_pos, 0);
						if (ret < 0){
							pdb_log_error("send error\n");
							return PDB_ERROR;
						}else if (ret > 0){
							// pdb_log_info("send success: %d\n", ret);
							pdb_sds_range(conn_list[tmp_fd]->write_buffer, ret, -1);
							conn_list[tmp_fd]->write_pos -= ret;
						}
					}
					tmp_fd = conn_list[tmp_fd]->next_fd;
				}
			}
			// pdb_is_aof_written_end();
			continue;
		}

		int i = 0;
		for (i = 0; i < nready; i ++) {
			int connfd = events[i].data.fd;
			struct conn_info* c = conn_list[connfd];
			if (events[i].events & EPOLLIN) {
				int ret = c->recv_callback(connfd, request_handler);
				if (ret < 0){
					pdb_log_debug("EPOLLIN call_back return error\n");
				}				
				if (ret == PDB_DISCONNECT){
					// conn disconnect
					pdb_delete_conn_list(connfd);
					if (!global_conf.is_slave){
						int i = 0;
						for (i = 0; i < REPLICATION_NUM; i++){
							if (global_replication->fd[i] == connfd){
								global_replication->fd[i] = 0;
								global_replication->slave_num--;
							}
						}
					}
					
					epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
				}
			} 

			if (events[i].events & EPOLLOUT) {
				int ret = c->send_callback(connfd, response_handler);
				if (ret == PDB_DISCONNECT){
					// destroy_conn_info(connfd);
					pdb_delete_conn_list(connfd);
					epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
				}
			}

			
		}
		
		
	}
    
    return 0;
}
