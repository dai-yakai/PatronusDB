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
#include "pdb_parse_protocol.h"
#include "pdb_conninfo.h"
#include "pdb_sds.h"

static msg_handler handler;


static void pdb_write_to_slave(int fd, char* msg, int msg_len){
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
		
        int ret = send(slave_fd, msg, msg_len, 0);
		// pdb_log_debug("write to slave[fd %d]: %s\n", fd, msg);
	}
}

static int process_read_buffer(struct conn_info* c, msg_handler handler){
    int fd = c->fd;
    size_t processed = 0;
    while(c->read_pos > processed){
        // pdb_log_info("c->read_pos: %d\n", c->read_pos);
		int bulk_length = 0;
		size_t package_len = check_resp_integrity(c->read_buffer + processed, c->read_pos - processed, &bulk_length);
		

		if (bulk_length > PDB_PROTO_IO_BUFFER_LENGTH){
			c->is_big_package = 1;
			c->bulk_length = bulk_length;
		}
		
		if (package_len == PDB_HALF_PACKAGE){
			// pdb_log_debug("process_read_buffer receive half package\n");
            break;
			return PDB_HALF_PACKAGE;
		}else if (package_len == PDB_PROTOCAL_ERROR){
			// pdb_log_debug("process_read_buffer receive error protocal\n%s\n", c->read_buffer);
			memcpy(c->write_buffer + c->write_pos, "protocal error\r\n", 17);
			return PDB_PROTOCAL_ERROR;
		}

        if (global_conf.is_replication){
            pdb_write_to_slave(fd, c->read_buffer, package_len);
        }
		

		// pdb_log_info("read_buffer:%s\n", c->read_buffer);
		// Write response directly into `c->write_buffer` to avoid extra allocating.
		// printf("LEN BEFORE: %zu\n", pdb_get_sds_len(c->read_buffer));
        int response_len = handler(fd, c->read_buffer + processed, package_len, c->write_buffer + c->write_pos);
        // printf("LEN after: %zu\n", pdb_get_sds_len(c->read_buffer));
		if (response_len > 0){
			c->write_pos += response_len;
			pdb_sds_len_increment(c->write_buffer, response_len);
		}

        processed += package_len;
		// pdb_log_info("c->write_pos: %d\n", c->write_pos);
		
		// deal with AOF
		// pdb_aof_buffer_append(c->read_buffer, package_len);
		// pdb_aof_write_to_written_buffer(c->read_buffer, package_len);
        // printf("DEBUG before range: c=%p, read_buf=%p, write_buf=%p\n", (void*)c, (void*)c->read_buffer, (void*)c->write_buffer);
		// pdb_sds_range(c->read_buffer, package_len, -1);

        // printf("--- Visualized ---\n");
        // for (size_t i = 0; i < package_len; i++) {
        //     char ch = c->read_buffer[i];
        //     if (ch == '\r') printf("\\r");
        //     else if (ch == '\n') printf("\\n\n");
        //     else if (ch >= 32 && ch <= 126) printf("%c", ch);
        //     else printf("."); // 不可打印字符用点代替
        // }
        // printf("\n\n");

		// c->read_pos -= package_len;
		
		if (c->is_big_package == 1){
			c->is_big_package = -1;
		}
	}

    if (processed > 0) {
        pdb_sds_range(c->read_buffer, processed, -1);
        c->read_pos -= processed;
    }

	return PDB_OK;
}


void server_reader(void *arg) {
    struct conn_info *c = (struct conn_info *)arg;
    int fd = c->fd;
    int ret = 0;

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    while (1) {
        // expand read buffer
        size_t avail_len = pdb_get_sds_avail(c->read_buffer);
        size_t used_len = pdb_get_sds_len(c->read_buffer);
        assert(avail_len >= 0);

        if (avail_len == 0){
            pdb_enlarge_sds_greedy(c->read_buffer, used_len * 2);
        }
        avail_len = pdb_get_sds_avail(c->read_buffer) - 1;   
        // pdb_log_info("read_len: %d\n", read_len);
        ret = recv(fd, c->read_buffer + c->read_pos, avail_len, 0);

        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
                continue; 
            }
            break; 
        } else if (ret == 0) {
            break; 
        }
        
        // pdb_log_info("recv: %d\n", ret);
        c->read_pos += ret;
        pdb_sds_len_increment(c->read_buffer, ret);
        process_read_buffer(c, handler);
        
        // if (c->write_pos < 8 * 1024){
        //     return;
        // }

        int sent = send(fd, c->write_buffer, c->write_pos, 0);
        if (sent > 0) {
            pdb_sds_range(c->write_buffer, sent, -1);
            c->write_pos -= sent;
        } else if (sent < 0 && errno != EAGAIN) {
            break;
        }
        // pdb_log_info("send: %d\n", sent);
        
    }
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
    pdb_insert_conn_list(fd);
    conn_list[fd]->fd = fd;
    // printf("listen port : %d\n", port);

    while (1) {
        struct sockaddr_in remote;
        socklen_t len = sizeof(struct sockaddr_in);
        int clientfd = accept(fd, (struct sockaddr*)&remote, &len);

        if (clientfd < 0) continue;
        
        pdb_insert_conn_list(clientfd);
        conn_list[clientfd]->client_ip = (char*)malloc(16);
        assert(conn_list[clientfd]->client_ip != NULL);

        struct conn_info* listen_c = conn_list[fd];
        struct sockaddr_in *clientaddr = (struct sockaddr_in *)&listen_c->uring_accept_addr;
        inet_ntop(AF_INET, &clientaddr->sin_addr, conn_list[clientfd]->client_ip, 16);
        conn_list[clientfd]->client_port = ntohs(clientaddr->sin_port);


        conn_list[clientfd]->write_buffer = pdb_get_new_sds(16 * 1024);
        conn_list[clientfd]->read_buffer = pdb_get_new_sds(PDB_PROTO_IO_BUFFER_LENGTH);
        conn_list[clientfd]->read_pos = 0;
        conn_list[clientfd]->write_pos = 0;

        conn_list[clientfd]->fd = clientfd;
        conn_list[clientfd]->is_aof_rewrite = 0;
        conn_list[clientfd]->is_aof = 0;

        conn_list[clientfd]->is_slave = global_conf.is_slave;

        struct conn_info* c = conn_list[clientfd];
        nty_coroutine *read_co;
        nty_coroutine_create(&read_co, server_reader, c);
    }
}

int ntyco_entry(unsigned short port, msg_handler request_handler, msg_handler response_handler) {
    handler = request_handler;

    nty_coroutine *co_server = NULL;
    nty_coroutine_create(&co_server, server, &port);

    nty_schedule_run();
    return 0;
}