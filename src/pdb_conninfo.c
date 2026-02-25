#include "pdb_conninfo.h"

int global_conn_info_list_head = -1;
int global_conn_info_list_tail = -1;
int active_conn_num = 0;

struct conn_info* conn_list[CONNECTION_SIZE] = {0};


void pdb_insert_conn_list(int fd){
    // tail insert
    conn_list[fd] = (struct conn_info*)pdb_malloc(sizeof(struct conn_info));
	assert(conn_list[fd] != NULL);
	memset(conn_list[fd], 0, sizeof(struct conn_info));
    if (global_conn_info_list_head == -1 && global_conn_info_list_tail == -1){
        // conn_list is empty
        global_conn_info_list_head = fd;
        global_conn_info_list_tail = fd;
        conn_list[fd]->prev_fd = -1;
        conn_list[fd]->next_fd = -1;
    }else{
        conn_list[global_conn_info_list_tail]->next_fd = fd;
        conn_list[fd]->next_fd = -1;
        conn_list[fd]->prev_fd = global_conn_info_list_tail;
        global_conn_info_list_tail = fd;
    }
    active_conn_num++;
}

void pdb_delete_conn_list(int fd){
    if (active_conn_num == 1){
        pdb_sds_free(conn_list[fd]->read_buffer);
        pdb_sds_free(conn_list[fd]->write_buffer);
        pdb_free(conn_list[fd], -1);
        conn_list[fd] = NULL;

        active_conn_num--;
    } else if (fd == global_conn_info_list_tail){
        global_conn_info_list_tail = conn_list[fd]->prev_fd;
        conn_list[global_conn_info_list_tail]->next_fd = -1;

        assert(conn_list[fd]->read_buffer != NULL && conn_list[fd]->write_buffer != NULL);
        pdb_sds_free(conn_list[fd]->read_buffer);
        pdb_sds_free(conn_list[fd]->write_buffer);
        pdb_free(conn_list[fd], -1);
        conn_list[fd] = NULL;

        active_conn_num--;
    } else if(fd == global_conn_info_list_head){
        global_conn_info_list_head = conn_list[fd]->next_fd;
        conn_list[global_conn_info_list_head]->prev_fd = -1;

        assert(conn_list[fd]->read_buffer != NULL && conn_list[fd]->write_buffer != NULL);
        pdb_sds_free(conn_list[fd]->read_buffer);
        pdb_sds_free(conn_list[fd]->write_buffer);
        pdb_free(conn_list[fd], -1);
        conn_list[fd] = NULL;

        active_conn_num--;
    }else{
        int prev_fd = conn_list[fd]->prev_fd;
        int next_fd = conn_list[fd]->next_fd;
        conn_list[prev_fd]->next_fd = next_fd;
        conn_list[next_fd]->prev_fd = prev_fd;

        assert(conn_list[fd]->read_buffer != NULL && conn_list[fd]->write_buffer != NULL);
        pdb_sds_free(conn_list[fd]->read_buffer);
        pdb_sds_free(conn_list[fd]->write_buffer);
        pdb_free(conn_list[fd], -1);
        conn_list[fd] = NULL;

        active_conn_num--;
    }

    if (active_conn_num == 0){
        global_conn_info_list_head = -1;
        global_conn_info_list_tail = -1;
    }
}

