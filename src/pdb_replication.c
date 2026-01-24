#include "pdb_replication.h"


/**
 * @brief slave连接master节点
 * 
 * @return 返回slave连接master的fd
 */
int connect_master(const char* ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip);

    if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        perror("Connect master failed");
        return -1;
    }
    return sockfd;
}


/**
 * @brief 将slave节点注册进global_replication表中
 * 
 * @param c slave和master节点连接的上下文
 */
int master_add_slave(struct conn_info* c) {
    if (global_replication.slave_count < MAX_SLAVES) {
        global_replication.master_to_slaves_info[global_replication.slave_count++] = c;
        printf("Slave registered. FD: %d, Total slave: %d\n", c->fd, global_replication.slave_count);
        return global_replication.slave_count - 1;
    }
}

/**
 * @brief 将slave节点从global_replication表中删除
 */
void master_del_slave(struct conn_info* c) {
    for (int i = 0; i < global_replication.slave_count; i++) {
        if (global_replication.master_to_slaves_info[i] == c) {
            // 将最后一个移到当前位置填补空缺
            global_replication.master_to_slaves_info[i] = global_replication.master_to_slaves_info[global_replication.slave_count - 1];
            global_replication.slave_count--;
            printf("Slave removed. FD: %d\n", c->fd);
            break;
        }
    }
}

/**
 * @brief 供协程子进程使用的send函数
 *          区别：不使用hook，直接调用syscall发送
 */
static ssize_t raw_send(int fd, const void *buf, size_t len) {
    const char *ptr = (const char *)buf;
    size_t remaining = len;
    ssize_t sent;

    while (remaining > 0) {
        sent = syscall(SYS_write, fd, ptr, remaining);
        
        if (sent < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000); 
                continue;
            }
            return -1; 
        }
        ptr += sent;
        remaining -= sent;
    }
    return len;
}

/**
 * @brief 如果用户态缓冲区里旧的数据长度加上新的数据长度超过了缓冲区容量，
 *              则将旧数据先发送出去，在把新的数据放入用户态缓冲区中
 * @param big_buf   用户态同步缓冲区
 * @param pos       big_buf起始空闲位置
 * @param data      要放入的新数据
 * @param len       新数据的长度
 */
static void flush_and_write(int fd, char *big_buf, int *pos, char *data, int len) {
    if (*pos + len > SYNC_BUFFER_SIZE) {
        // 判断缓冲区里的旧数据加上新数据是否超过了缓冲区的大小
        int n = 0;
        int total_sent = 0;
        while (total_sent < *pos) {
            n = write(fd, big_buf + total_sent, *pos - total_sent);
            if (n < 0) {
                if (errno == EINTR) continue; // 被信号中断，重试
                perror("master sync write error");
                return; 
            }
            total_sent += n;
        }
        *pos = 0; 
    }
    
    memcpy(big_buf + *pos, data, len);
    *pos += len;
}

/**
 * @brief 供协程子进程使用，功能同flush_and_write
 *      区别：协程子进程发送直接调用raw_send（系统调用，不走hook）
 */
static void flush_and_write_raw(int fd, char *big_buf, int *pos, char *data, int len) {
    if (*pos + len > SYNC_BUFFER_SIZE) {
        int n = 0;
        int total_sent = 0;
        while (total_sent < *pos) {
            n = raw_send(fd, big_buf + total_sent, *pos - total_sent);
            if (n < 0) {
                if (errno == EINTR) continue; // 被信号中断，重试
                perror("master sync write error");
                return; 
            }
            total_sent += n;
        }
        *pos = 0; 
    }
    
    memcpy(big_buf + *pos, data, len);
    *pos += len;
}


//*************************************************** */
//*************************************************** */
//*************************************************** */
//************     rbtree 的全量同步  **************** */
//*************************************************** */
//*************************************************** */
//*************************************************** */
static void rbtree_sync_traverse(rbtree_node* node, rbtree_node* nil, int fd, char *big_buf, int *pos) {
    if (node == NULL || node == nil) {
        return;
    }

    rbtree_sync_traverse(node->left, nil, fd, big_buf, pos);

    if (node->key && node->value) {
        // 考虑大value情况
        int value_len = strlen(node->value);
        int key_len = strlen(node->key);
        int total_len = value_len + key_len + 128;

        char* temp_buf = malloc(total_len);
        if (temp_buf == NULL){
            printf("rbtree_sync_traverse: malloc error\n");
            return;
        }
        temp_buf[0] = '\0';
        
        int len = snprintf(temp_buf, total_len, 
            "*3\r\n"
            "$4\r\nRSET\r\n"
            "$%lu\r\n%s\r\n"
            "$%lu\r\n%s\r\n",
            strlen((char*)node->key), (char*)node->key,
            strlen((char*)node->value), (char*)node->value
        );

        if (len > 0) {
            flush_and_write(fd, big_buf, pos, temp_buf, len);
        }
        free(temp_buf);
    }

    rbtree_sync_traverse(node->right, nil, fd, big_buf, pos);
}

/**
 * big_buf: 全量同步的用户态缓冲区
 * pos: 指向big_buf中下一个可以存放新数据的位置
 */
void perform_rbtree_full_sync(int fd) {
    if (global_rbtree.root) {
        char *big_buf = malloc(SYNC_BUFFER_SIZE);
        if (!big_buf) {
            perror("malloc sync buffer failed");
            return;
        }
        
        int pos = 0; 
        
        rbtree_sync_traverse(global_rbtree.root, global_rbtree.nil, fd, big_buf, &pos);
        
        if (pos > 0) {
            int n = 0;
            int total_sent = 0;
            while (total_sent < pos) {
                n = write(fd, big_buf + total_sent, pos - total_sent);
                if (n < 0) {
                    perror("master sync final flush error");
                    break;
                }
                total_sent += n;
            }
        }
        
        free(big_buf);
        // printf("Full sync transfer complete.\n");
    }
}

/**
 * @brief 供协程子进程使用，功能同rbtree_sync_traverse
 */
void rbtree_sync_traverse_raw(rbtree_node* node, rbtree_node* nil, int fd, char *big_buf, int *pos) {
    if (node == NULL || node == nil) {
        return;
    }

    rbtree_sync_traverse_raw(node->left, nil, fd, big_buf, pos);

    if (node->key && node->value) {
        int value_len = strlen(node->value);
        int key_len = strlen(node->key);
        int total_len = value_len + key_len + 128;

        char* temp_buf = malloc(total_len);
        if (temp_buf == NULL){
            printf("rbtree_sync_traverse: malloc error\n");
            return;
        }
        temp_buf[0] = '\0';
        
        int len = snprintf(temp_buf, total_len, 
            "*3\r\n"
            "$4\r\nRSET\r\n"
            "$%lu\r\n%s\r\n"
            "$%lu\r\n%s\r\n",
            strlen((char*)node->key), (char*)node->key,
            strlen((char*)node->value), (char*)node->value
        );

        if (len > 0) {
            flush_and_write_raw(fd, big_buf, pos, temp_buf, len);
        }

        free(temp_buf);
    }

    rbtree_sync_traverse_raw(node->right, nil, fd, big_buf, pos);
}

/**
 * @brief 供协程子进程使用，功能同perform_rbtree_full_sync
 */
void perform_rbtree_full_sync_raw(int fd) {
    if (global_rbtree.root) {
        char *big_buf = malloc(SYNC_BUFFER_SIZE);
        if (!big_buf) {
            perror("malloc sync buffer failed");
            return;
        }
        
        int pos = 0; 
        
        rbtree_sync_traverse_raw(global_rbtree.root, global_rbtree.nil, fd, big_buf, &pos);
        
        if (pos > 0) {
            int n = 0;
            int total_sent = 0;
            while (total_sent < pos) {
                n = raw_send(fd, big_buf + total_sent, pos - total_sent);
                if (n < 0) {
                    perror("master sync final flush error");
                    break;
                }
                total_sent += n;
            }
        }
        
        free(big_buf);
        // printf("Full sync transfer complete.\n");
    }
}

//*************************************************** */
//*************************************************** */
//*************************************************** */
//************     hash 的全量同步    **************** */
//*************************************************** */
//*************************************************** */
//*************************************************** */
static void hash_sync_traverse(kvs_hash_t *h, int fd, char *big_buf, int *pos) {

    for (int i = 0; i < h->max_slots; i++) {
        hashnode_t *node = h->nodes[i];
        
        // 遍历链表
        while (node) {
            if (node->key && node->value) {
                int value_len = strlen(node->value);
                int key_len = strlen(node->key);
                int total_len = value_len + key_len + 128;

                char* temp_buf = malloc(total_len);
                if (temp_buf == NULL){
                    printf("hash_sync_traverse: malloc error\n");
                    return;
                }
                temp_buf[0] = '\0';
                
                // 拼装 RESP 协议
                int len = snprintf(temp_buf, total_len, 
                    "*3\r\n"
                    "$4\r\nHSET\r\n"
                    "$%lu\r\n%s\r\n"
                    "$%lu\r\n%s\r\n",
                    strlen((char*)node->key), (char*)node->key,
                    strlen((char*)node->value), (char*)node->value
                );

                if (len > 0 && len < total_len) {
                    flush_and_write(fd, big_buf, pos, temp_buf, len);
                } else {
                    fprintf(stderr, "Protocol truncation detected! Buffer too small.\n");
                    // 这种情况下不能发送，否则会导致 Slave 死锁
                }

                free(temp_buf);
            }
            node = node->next;
        }
    }
}

void perform_hash_full_sync(int fd){
    char *big_buf = malloc(SYNC_BUFFER_SIZE);
    if (!big_buf) {
        perror("malloc sync buffer failed");
        return;
    }
        
    int pos = 0; 

    hash_sync_traverse(&global_hash, fd, big_buf, &pos);

    if (pos > 0) {
        write(fd, big_buf, pos); 
    }

    return;
}

/**
 * @brief 供协程子进程使用，功能同hash_sync_traverse
 */
static void hash_sync_traverse_raw(kvs_hash_t *h, int fd, char *big_buf, int *pos) {

    for (int i = 0; i < h->max_slots; i++) {
        hashnode_t *node = h->nodes[i];
        
        // 遍历链表
        while (node) {
            if (node->key && node->value) {
                int value_len = strlen(node->value);
                int key_len = strlen(node->key);
                int total_len = value_len + key_len + 128;

                char* temp_buf = malloc(total_len);
                if (temp_buf == NULL){
                    printf("hash_sync_traverse: malloc error\n");
                    return;
                }
                temp_buf[0] = '\0';
                
                // 拼装 RESP 协议
                int len = snprintf(temp_buf, total_len, 
                    "*3\r\n"
                    "$4\r\nHSET\r\n"
                    "$%lu\r\n%s\r\n"
                    "$%lu\r\n%s\r\n",
                    strlen((char*)node->key), (char*)node->key,
                    strlen((char*)node->value), (char*)node->value
                );

                if (len > 0) {
                    // 写入大缓冲区，满则发送
                    flush_and_write_raw(fd, big_buf, pos, temp_buf, len);
                }

                free(temp_buf);
            }
            node = node->next;
        }
    }
}

/**
 * @brief 供协程子进程使用，功能同perform_hash_full_sync
 */
void perform_hash_full_sync_raw(int fd){
    char *big_buf = malloc(SYNC_BUFFER_SIZE);
    if (!big_buf) {
        perror("malloc sync buffer failed");
        return;
    }
        
    int pos = 0; 

    hash_sync_traverse_raw(&global_hash, fd, big_buf, &pos);

    return;
}

// void perform_hash_full_sync(int fd) {
//     if (global_hash) {
//         char *big_buf = malloc(SYNC_BUFFER_SIZE);
//         if (!big_buf) {
//             perror("malloc sync buffer failed");
//             return;
//         }
        
//         int pos = 0; 
        
//         hash_sync_traverse(global_rbtree.root, global_rbtree.nil, fd, big_buf, &pos);
        
//         if (pos > 0) {
//             int n = 0;
//             int total_sent = 0;
//             while (total_sent < pos) {
//                 n = write(fd, big_buf + total_sent, pos - total_sent);
//                 if (n < 0) {
//                     perror("master sync final flush error");
//                     break;
//                 }
//                 total_sent += n;
//             }
//         }
        
//         free(big_buf);
//         printf("Full sync transfer complete.\n");
//     }
// }

//***************array 的全量同步******************** */



//***************skiptable 的全量同步******************** */