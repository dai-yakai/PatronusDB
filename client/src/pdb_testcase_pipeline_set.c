#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>

#include "pdb_cli_tool.h"

#define MAX_MSG_LENGTH  1024
#define SEND_BATCH      1024
#define CMD_NUM         1000000

#define ARRAY           0
#define HASH            1
#define RBTREE          1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int pipeline_set_send_msg(int connfd, char* msg, int length){
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR) continue; // 被信号打断，重试
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

int pipeline_set_recv_msg(int connfd, char* msg, int length){
    int res = recv(connfd, msg, length, 0);
    if (res < 0){
        perror("recv error");
        return -1;
    }
    return res;
}

void pipeline_set_verify_responses(int fd, int start_i, int expect_count, const char* pattern, const char* ds_type) {
    char buffer[16384]; 
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
                break; 
            }
            int current_resp_len = line_end - current + 1;
            
            if (strncmp(current, pattern, pattern_len) != 0) {
                *line_end = '\0'; 
                if (current_resp_len > 1 && current[current_resp_len-2] == '\r') 
                    current[current_resp_len-2] = '\0';

                // 计算出是在第几个循环 (i)，以及是该循环拼接的第几条命令
                int fail_i = start_i + (received_count / 3);
                int cmd_idx = received_count % 3; // 0代表第一条，1代表第二条，2代表第三条
                
                printf("\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Response Verify Failed!\033[0m\n");
                // 停止瞎猜，直接告诉你是循环里的第几个动作报错
                printf("Error Location : Loop Index (i) = %d\n", fail_i);
                printf("Error Command  : The No.%d command in this loop (Index: %d)\n", cmd_idx + 1, cmd_idx);
                printf("Expected Reply : %s", pattern); 
                printf("Actual Reply   : %s\n", current);
                printf("============================================\n");
                
                // 提示你怎么找真实命令
                printf("=> How to fix: Look at your for() loop at i=%d, and check the %d-th sprintf(...) command.\n", fail_i, cmd_idx + 1);
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

static void testcase_100w_set(int connfd){
    int count = 1000000;
    int i = 0;
    int response_count = 0;
    int batch_start_i = 0; 
    
    char batch_buf[8192];
    int batch_len = 0;

    struct timeval tv_begin;
    struct timeval tv_end;
    long time_used;

    char key[64];
    char val[64];
    int k_len, v_len;

    // ############## RBtree ###################### 
#if RBTREE
    printf("RBTREE is testing..........\n");
    batch_start_i = 0; 
    response_count = 0;
    batch_len = 0;

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
            "*2\r\n$4\r\nRDEL\r\n$%d\r\n%s\r\n", k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_set_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "RBTREE");
            batch_start_i += (response_count / 3); 
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        pipeline_set_send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "RBTREE");
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("RBTree testcase success --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif

#if HASH
    //############## Hash ######################
    printf("HASH is testing........\n");
    batch_start_i = 0; // 重置游标
    response_count = 0;
    batch_len = 0;

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
            "*2\r\n$4\r\nHDEL\r\n$%d\r\n%s\r\n", k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_set_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "HASH");
            batch_start_i += (response_count / 3);
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        pipeline_set_send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "HASH");
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("HASH testcase success --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif

#if ARRAY
    //############## Array ######################  
    printf("ARRAY test begin\n");
    batch_start_i = 0; // 重置游标
    response_count = 0;
    batch_len = 0;

    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",  
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_set_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "ARRAY");
            batch_start_i += (response_count / 3);
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        pipeline_set_send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "ARRAY");
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("ARRAY testcase success --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif    
}   

void testcase_100w_delete(int connfd){
    printf("#### DELETE BEGIN ###\n");
    int count = 1000000;
    int i = 0;
    int response_count = 0;
    int batch_start_i = 0; 
    
    char batch_buf[8192];
    int batch_len = 0;

    char key[64];
    char val[64];
    int k_len, v_len;

    for(i = 0; i < count; i++){
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n$4\r\nRDEL\r\n$%d\r\n%s\r\n",  
            k_len, key);
        
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$4\r\nRSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
            k_len, key, v_len, val);
        
        k_len = sprintf(key, "TAO%d", i);
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n$4\r\nRDEL\r\n$%d\r\n%s\r\n", k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_set_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "RBTREE");
            batch_start_i += (response_count / 3); 
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        pipeline_set_send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        pipeline_set_verify_responses(connfd, batch_start_i, response_count, "+OK\r\n", "RBTREE");
    }
}


// ./testcase 192.168.127.222 8888
int pdb_testcase_pipeline_set(char* ip, int port){
    int connfd = connect_tcpserver(ip, port);

    if (connfd == -1){
        printf("connect error\n");
        exit(-1);
    }

    testcase_100w_set(connfd);
    // testcase_100w_delete(connfd);

    return 0;
}