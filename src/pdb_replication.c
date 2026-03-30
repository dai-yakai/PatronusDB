#include "pdb_replication.h"

int master_fd;

int connect_master(const char* ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip);

    if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        pdb_log_info("No mater node to connect\n");
        return -1;
    }
    return sockfd;
}


void pdb_init_replication(){
    // rdb replication init
    if (global_conf.is_replication){
        if (global_conf.is_slave){
            // slave node
            char* master_ip = global_conf.master_ip;
            int master_port = global_conf.master_port;
            int fd = connect_master(master_ip, master_port);

            if (fd > 0){
                extern void init_replication_slave_to_master_conn_list(int fd);
                init_replication_slave_to_master_conn_list(fd);
            }else{
                // printf("slave connect to master failed\n");
                return;
            }

            master_fd = fd;
            if (global_conf.is_rdma){
                // slave node send SYN to master node: "*1\r\n$4\r\nSYNC\r\n"
                char* msg = "*1\r\n$4\r\nZSYN\r\n";
                int ret = send(fd, msg, strlen(msg), 0);
                if (ret < 0){
                    pdb_log_info("slave send SYN failed\n");
                }
            }else{
                // master use sendfile()
                char* msg = "*1\r\n$13\r\nZSYN-SENDFILE\r\n";
                int ret = send(fd, msg, strlen(msg), 0);
                if (ret < 0){
                    pdb_log_info("slave send ZSYN-SENDFILE failed\n");
                }
            }
        }else{
            // master node
            global_replication = pdb_malloc(sizeof(struct conn_replication));
            global_replication->fd = pdb_malloc(sizeof(int) * REPLICATION_NUM);
            memset(global_replication->fd, 0, sizeof(int) * REPLICATION_NUM);
            global_replication->slave_num = 0;
        }  
    }
}