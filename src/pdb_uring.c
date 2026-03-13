#define _GNU_SOURCE

#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sched.h>

#include "pdb_handler.h"
#include "pdb_replication.h"
#include "pdb_conninfo.h"
#include "pdb_core.h"
#include "pdb_parse_protocol.h"

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

/*********************** 提交事件 *************************/
static inline void submit_accept(struct io_uring *ring, int sockfd)
{
    struct conn_info* c = conn_list[sockfd];
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            fprintf(stderr, "Fatal: SQ Ring Full and flush failed!\n");
            exit(1);
        }
    }

    io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)&c->uring_accept_addr, &c->uirng_accept_len, 0);
    io_uring_sqe_set_data(sqe, c);
}

static inline void submit_read(struct io_uring *ring, int fd)
{
    struct conn_info* c = conn_list[fd];
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        
        if (!sqe) {
            fprintf(stderr, "Fatal: SQ Ring Full and flush failed!\n");
            exit(1);
        }
    }

    // expand read buffer
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

    int len = pdb_get_sds_avail(c->read_buffer);
    c->event = EVENT_READ;
    io_uring_prep_recv(sqe, fd, c->read_buffer + c->read_pos, len, 0);
    io_uring_sqe_set_data(sqe, c);
}

int handle_accept_completion(struct io_uring *ring, struct io_uring_cqe *cqe, int listen_fd) 
{
    struct conn_info* listen_c = conn_list[listen_fd];
    int clientfd = cqe->res; 
    if (clientfd < 0) {
        pdb_log_error("io_uring accept failed: %s\n", strerror(-clientfd));
        return clientfd;
    }
    pdb_log_info("Accept returned FD: %d\n", clientfd);

    int flags = fcntl(clientfd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(clientfd, F_SETFL, flags | O_NONBLOCK);
    }

    pdb_insert_conn_list(clientfd);
    conn_list[clientfd]->client_ip = (char*)malloc(16);
    assert(conn_list[clientfd]->client_ip != NULL);

    struct sockaddr_in *clientaddr = (struct sockaddr_in *)&listen_c->uring_accept_addr;
    inet_ntop(AF_INET, &clientaddr->sin_addr, conn_list[clientfd]->client_ip, 16);
    conn_list[clientfd]->client_port = ntohs(clientaddr->sin_port);

    conn_list[clientfd]->event = EVENT_READ;

    conn_list[clientfd]->write_buffer = pdb_get_new_sds(16 * 1024);
	conn_list[clientfd]->read_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
	conn_list[clientfd]->read_pos = 0;
	conn_list[clientfd]->write_pos = 0;

	conn_list[clientfd]->fd = clientfd;
	conn_list[clientfd]->is_aof_rewrite = 0;
	conn_list[clientfd]->is_aof = 0;

	conn_list[clientfd]->is_slave = global_conf.is_slave;

    submit_read(ring, clientfd);

    return clientfd;
}




static inline void submit_write(struct io_uring *ring, int fd)
{
    struct conn_info* c = conn_list[fd];
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

    c->event = EVENT_WRITE;
    io_uring_prep_send(sqe, fd, c->write_buffer, c->write_pos, 0);
    io_uring_sqe_set_data(sqe, c);
}


static void pdb_write_to_slave(struct io_uring* ring, char* msg, int msg_len){
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
		
        submit_write(ring, slave_fd);

		// pdb_log_debug("write to slave[fd %d]: %s\n", fd, msg);
	}
}


static int process_read_buffer(struct io_uring* ring, conn_info_t *c) {
	int fd = c->fd;
    size_t processed_bytes = 0;

    while(c->read_pos > processed_bytes){
		int bulk_length = 0;
		size_t package_len = check_resp_integrity(c->read_buffer + processed_bytes, c->read_pos - processed_bytes, &bulk_length);
		
		if (bulk_length > PDB_PROTO_IO_BUFFER_LENGTH){
			c->is_big_package = 1;
			c->bulk_length = bulk_length;
		}
		
		if (package_len == PDB_HALF_PACKAGE){
            break;
			// pdb_log_debug("process_read_buffer receive half package\n");
			// return PDB_HALF_PACKAGE;
		}else if (package_len == PDB_PROTOCAL_ERROR){
			// pdb_log_debug("process_read_buffer receive error protocal\n%s\n", c->read_buffer);
			memcpy(c->write_buffer + c->write_pos, "protocal error\r\n", 17);
			return PDB_PROTOCAL_ERROR;
		}

        if (global_conf.is_replication){
            pdb_write_to_slave(ring, c->read_buffer, package_len);
        }
		
		// pdb_log_info("read_buffer:%s\n", c->read_buffer);
		// Write response directly into `c->write_buffer` to avoid extra allocating.
		int response_len = handler(fd, c->read_buffer + processed_bytes, package_len, c->write_buffer + c->write_pos);
		if (response_len > 0){
			c->write_pos += response_len;
			pdb_sds_len_increment(c->write_buffer, response_len);
		}
		// pdb_log_info("c->write_pos: %d\n", c->write_pos);
		
		// deal with AOF
		// pdb_aof_buffer_append(c->read_buffer, package_len);
		// pdb_aof_write_to_written_buffer(c->read_buffer, package_len);
        processed_bytes += package_len;

		// pdb_sds_range(c->read_buffer, package_len, -1);
		// c->read_pos -= package_len;
		
		if (c->is_big_package == 1){
			c->is_big_package = -1;
		}
	}

    if (processed_bytes > 0) {
        pdb_sds_range(c->read_buffer, processed_bytes, -1);
        c->read_pos -= processed_bytes;
    }

	return PDB_OK;
}


/*********************** 启动 server *************************/

int uring_entry(unsigned short port, msg_handler request_handler,
                msg_handler response_handler)
{
    handler = request_handler;

    int opt = 1;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0){
		perror("setsockopt error");
		return PDB_ERROR;
	}


    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    bind(sockfd, (struct sockaddr *)&addr, sizeof(addr));
    listen(sockfd, 128);
    pdb_insert_conn_list(sockfd);
    conn_list[sockfd]->event = EVENT_ACCEPT;
    conn_list[sockfd]->fd = sockfd;

    /************** 创建 io_uring 队列 **************/
    struct io_uring ring;
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_idle = 2000;
    params.sq_thread_cpu = 1;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    int ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
    if (ret < 0) {
        pdb_log_error("SQPOLL init failed! Check permissions.\n");
    }

    // io_uring_queue_init(QUEUE_DEPTH, &ring, 0);

    submit_accept(&ring, sockfd);
    

    struct io_uring_cqe* cqes[CQE_BATCH];
    struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 1000 * 1000 };

    // init replication
    pdb_init_replication();

    while (1) {
        // io_uring_submit_and_wait_timeout(&ring, cqes, CQE_BATCH, &ts, NULL);
        // if (io_uring_sqring_wait(&ring)) {
        //     io_uring_submit(&ring);
        // } else {
            
        // }

        io_uring_submit(&ring);

        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) continue;

        unsigned head;
        unsigned processed = 0;

        // int count = io_uring_peek_batch_cqe(&ring, cqes, CQE_BATCH);
        // if (count == 0) {
        //     struct io_uring_cqe *cqe_ptr;
        //     int wait_ret = io_uring_wait_cqe(&ring, &cqe_ptr);
        //     if (wait_ret < 0) continue;
            
        //     cqes[0] = cqe_ptr;
        //     count = io_uring_peek_batch_cqe(&ring, &cqes[1], CQE_BATCH - 1);
        //     count += 1; 
        // }

        // printf("DEBUG: CQ Ready before process: %u\n", io_uring_cq_ready(&ring));
        // for (int i = 0; i < count; i++) {
        io_uring_for_each_cqe(&ring, head, cqe) {
            // pdb_log_info("main loop: count: %d\n", count);
            processed++;
            // struct io_uring_cqe* cqe = cqes[i];
            conn_info_t* c = io_uring_cqe_get_data(cqe);
            int res = cqe->res;

            if (c == NULL) continue;

            /************************* ACCEPT *************************/
            if (c->event == EVENT_ACCEPT) {
                pdb_log_info("io_uring accept\n");
                if (res >= 0) {     
                    int client_fd = handle_accept_completion(&ring, cqe, c->fd);
                }
                submit_accept(&ring, c->fd);

            }

            /************************* READ *************************/
            else if (c->event == EVENT_READ) {
                if (res <= 0) { 
                    // read error
                    if (res < 0 && res != -ECONNRESET) fprintf(stderr, "[Read Error] fd=%d, res=%d\n", c->fd, res);
                    // close(c->fd);
                    // pdb_sds_free(c->read_buffer);
                    // pdb_sds_free(c->write_buffer);

                    int fd_to_close = c->fd;
                    close(fd_to_close); 
                    pdb_delete_conn_list(fd_to_close);
                    
                    continue;
                }
                // pdb_log_info("io_uring read\n");
                c->read_pos += res;
                pdb_sds_len_increment(c->read_buffer, res);
                process_read_buffer(&ring, c);

                if (c->write_pos > 0) {
                    submit_write(&ring, c->fd);
                } else {
                    submit_read(&ring, c->fd);
                }
            }

            /************************* WRITE *************************/
            else if (c->event == EVENT_WRITE) {
                if (res < 0) {
                    int fd_to_close = c->fd;
                    close(fd_to_close);
                    pdb_delete_conn_list(fd_to_close);
                    continue; 
                }

                // pdb_log_info("io_uring write\n");
                pdb_sds_range(c->write_buffer, res, -1);
                c->write_pos -= res;

                if (c->write_pos > 0) {
                    submit_write(&ring, c->fd);
                } else {
                    submit_read(&ring, c->fd);
                }
            }
        }
        if (processed > 0) {
            io_uring_cq_advance(&ring, processed);
        }
    }

    return PDB_OK;
}
