#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>

#define MAX_MSG_LENGTH  1024
#define CMD_NUM         1000000

#define ARRAY           0
#define HASH            1
#define RBTREE          1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int connect_tcpserver(const char* ip, unsigned short port){
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;

    int ret = connect(connfd, (struct sockaddr*)&addr, sizeof(struct sockaddr));
    if (ret < 0){
        perror("connect error");
        return -1;
    }
    return connfd;
}

int send_msg(int connfd, char* msg, int length){
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR) continue; 
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue; 
            }
            perror("send error");
            return -1;
        }
        if (res == 0) {
            printf("Peer closed connection during send\n");
            return -1; 
        }
        
        total_sent += res;
        ptr += res;
        left -= res;
    }
    return total_sent;
}



// 接收并校验响应
// pattern: 期望的响应 (如 "OK\r\n")
void verify_responses(int fd, int expect_count, const char* pattern) {
    char buffer[65536]; // 加大接收缓冲，避免频繁 recv 系统调用
    int received_count = 0;
    int buf_len = 0;    
    int pattern_len = strlen(pattern);

    while (received_count < expect_count) {
        int n = recv(fd, buffer + buf_len, sizeof(buffer) - buf_len - 1, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv error");
            exit(1);
        } else if (n == 0) {
            printf("Server closed connection unexpectedly\n");
            exit(1);
        }
        
        buf_len += n;
        buffer[buf_len] = '\0'; 

        char *current = buffer;
        while (1) {
            char *line_end = strchr(current, '\n');
            if (line_end == NULL) {
                break; // 半包
            }
            
            if (strncmp(current, pattern, pattern_len) != 0) {
                *line_end = '\0'; 
                printf("\n[FATAL] Response Verify Failed!\n");
                printf("Expected: %s", pattern);
                printf("Received: %s\n", current);
                printf("Progress: %d / %d in this batch\n", received_count, expect_count);
                exit(-1);
            }

            received_count++;
            current = line_end + 1;

            if (received_count == expect_count) break;
        }

        int consumed = current - buffer;
        int remaining = buf_len - consumed;
        if (consumed > 0 && remaining > 0) {
            memmove(buffer, current, remaining);
        }
        buf_len = remaining;
    }
}

void testcase_100w(int connfd, int batch_size){
    int count = CMD_NUM; // 100w
    int i = 0;
    
    size_t buf_size = (size_t)batch_size * 256 + 4096;
    char *batch_buf = (char*)malloc(buf_size);
    if (!batch_buf) {
        perror("malloc batch_buf failed");
        exit(1);
    }

    int batch_len = 0;
    int pending_iters = 0; 

    struct timeval tv_begin;
    struct timeval tv_end;
    long time_used;

    char key[64];
    char val[64];
    int k_len, v_len;

    // ############## RBtree ###################### 
#if RBTREE
    printf("RBTREE test begin (Batch: %d loops, %d cmds)\n", batch_size, batch_size * 3);
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$4\r\nRSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",   
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$4\r\nRSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n$4\r\nRDEL\r\n$%d\r\n%s\r\n", 
            k_len, key);
        
        pending_iters++; 
        
        if (pending_iters >= batch_size) {
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                verify_responses(connfd, pending_iters * 3, "OK\r\n");
                
                batch_len = 0;
                pending_iters = 0;
            }
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
        verify_responses(connfd, pending_iters * 3, "OK\r\n");
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    if (time_used > 0)
        printf("RBTree testcase --> time_use: %ld ms, qps: %ld\n", time_used, (long)count * 3 * 1000 / time_used);
#endif

    batch_len = 0;
    pending_iters = 0;

#if HASH
    //############## Hash ######################
    printf("HASH test begin (Batch: %d loops, %d cmds)\n", batch_size, batch_size * 3);
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",   
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n$4\r\nHDEL\r\n$%d\r\n%s\r\n", 
            k_len, key);
        
        pending_iters++;
        
        if (pending_iters >= batch_size) {
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                verify_responses(connfd, pending_iters * 3, "OK\r\n");
                batch_len = 0;
                pending_iters = 0;
            }
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
        verify_responses(connfd, pending_iters * 3, "OK\r\n");
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    if (time_used > 0)
        printf("HASH testcase --> time_use: %ld ms, qps: %ld\n", time_used, (long)count * 3 * 1000 / time_used);
#endif

    // 清理堆内存
    free(batch_buf);
}   

// ./testcase 192.168.127.222 8888 100
int main(int argc, char* argv[]){
    if (argc < 3){
        printf("Usage: %s <ip> <port> [batch_size]\n", argv[0]);
        return -1;
    }

    char* ip = argv[1];
    int port = atoi(argv[2]);
    
    int batch_size = 1;
    if (argc >= 4) {
        batch_size = atoi(argv[3]);
        if (batch_size <= 0) batch_size = 1;
    }

    int connfd = connect_tcpserver(ip, port);
    if (connfd == -1){
        exit(-1);
    }

    testcase_100w(connfd, batch_size);

    return 0;
}