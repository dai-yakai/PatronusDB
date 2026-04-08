#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <signal.h>

#include "pdb_cli_tool.h"

#define MAX_MSG_LENGTH  1024
#define SEND_BATCH      1024
#define CMD_NUM         1000000

#define ARRAY           0
#define HASH            1
#define RBTREE          1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

extern volatile sig_atomic_t g_test_stop;
extern int g_in_test;

int pipeline_get_send_msg(int connfd, char* msg, int length){
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR) {
                if (g_test_stop) return -1;
                continue; 
            }
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


void verify_get_exist_responses(int fd, int start_i, int count, const char* expect_exist) {
    char buffer[16384]; 
    int received_count = 0;
    int buf_len = 0;    
    int expect_responses = count * 2; 

    while (received_count < expect_responses) {
        int n = recv(fd, buffer + buf_len, sizeof(buffer) - buf_len - 1, 0);
        if (n < 0) {
            if (errno == EINTR) {
                if (g_test_stop) return;
                continue;
            }
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
            if (line_end == NULL) break; 
            
            if (line_end > current && *(line_end - 1) == '\r') {
                *(line_end - 1) = '\0';
            } else {
                *line_end = '\0';
            }

            int req_index = start_i + (received_count / 2);
            int is_get_cmd = (received_count % 2) != 0; 

            char expected_str[64];
            if (!is_get_cmd) {
                strcpy(expected_str, expect_exist);
            } else {
                if (strcmp(expect_exist, "+EXIST") == 0) {
                    sprintf(expected_str, "+%d", req_index);
                } else {
                    strcpy(expected_str, "NO EXIST");
                }
            }

            if (strcmp(current, expected_str) != 0) {
                printf("\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Pipeline Verification Failed!\033[0m\n");
                printf("Key    : DAI%d\n", req_index);
                printf("Command: %s\n", is_get_cmd ? "GET" : "EXIST");
                printf("Expect : %s\n", expected_str);
                printf("Actual : %s\n", current);
                printf("============================================\n");
                exit(-1); 
            }

            received_count++;
            current = line_end + 1;

            if (received_count == expect_responses) break;
        }

        int consumed = current - buffer;
        int remaining = buf_len - consumed;
        if (consumed > 0 && remaining > 0) {
            memmove(buffer, current, remaining);
        }
        buf_len = remaining;
    }
}


void testcase_get_exist_100w(int connfd, char* expect_result){
    g_in_test = 1;
    g_test_stop = 0;
    int count = 1000000;
    int i = 0;
    
    char batch_buf[8192];
    int batch_len = 0;
    int batch_start_i = 0;        
    int current_batch_count = 0;  

    struct timeval tv_begin;
    struct timeval tv_end;
    long time_used;

    char key[64];
    int k_len;

    // ############## RBtree ###################### 
#if RBTREE
    printf("RBTREE GET/EXIST test begin...\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (g_test_stop) goto abort_test;
        k_len = sprintf(key, "DAI%d", i);  

        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$6\r\nREXIST\r\n$%d\r\n%s\r\n", k_len, key);
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$4\r\nRGET\r\n$%d\r\n%s\r\n", k_len, key);
        
        current_batch_count++;
        
        if (batch_len > 4096 || (current_batch_count * 2) >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_get_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
            if (current_batch_count > 0) {
                verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
                batch_start_i = i + 1; 
                current_batch_count = 0;
            }
        }

        print_progress("RBTREE", i + 1, count);
    }
    
    if (batch_len > 0) {
        pipeline_get_send_msg(connfd, batch_buf, batch_len);
    }
    if (current_batch_count > 0) {
        verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("RBTree GET/EXIST testcase --> time_use: %ld ms, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif

    batch_start_i = 0;
    current_batch_count = 0;
    batch_len = 0;

#if HASH
    //############## Hash ######################
    printf("HASH GET/EXIST test begin...\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        k_len = sprintf(key, "DAI%d", i);  
        
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$6\r\nHEXIST\r\n$%d\r\n%s\r\n", k_len, key);
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$4\r\nHGET\r\n$%d\r\n%s\r\n", k_len, key);
        
        current_batch_count++;
        
        if (batch_len > 4096 || (current_batch_count * 2) >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_get_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
            if (current_batch_count > 0) {
                verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
                batch_start_i = i + 1;
                current_batch_count = 0;
            }
        }

        print_progress("RBTREE", i + 1, count);
    }
    
    if (batch_len > 0) {
        pipeline_get_send_msg(connfd, batch_buf, batch_len);
    }
    if (current_batch_count > 0) {
        verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("HASH GET/EXIST testcase --> time_use: %ld ms, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif

    batch_start_i = 0;
    current_batch_count = 0;
    batch_len = 0;

#if ARRAY
    //############## Array ######################  
    printf("ARRAY GET/EXIST test begin...\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        k_len = sprintf(key, "DAI%d", i);  
        
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$5\r\nEXIST\r\n$%d\r\n%s\r\n", k_len, key);
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", k_len, key);
        
        current_batch_count++;
        
        if (batch_len > 4096 || (current_batch_count * 2) >= SEND_BATCH) { 
            if (batch_len > 0) {
                pipeline_get_send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
            if (current_batch_count > 0) {
                verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
                batch_start_i = i + 1;
                current_batch_count = 0;
            }
        }

        print_progress("RBTREE", i + 1, count);
    }
    
    if (batch_len > 0) {
        pipeline_get_send_msg(connfd, batch_buf, batch_len);
    }
    if (current_batch_count > 0) {
        verify_get_exist_responses(connfd, batch_start_i, current_batch_count, expect_result);
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("ARRAY GET/EXIST testcase --> time_use: %ld ms, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif   
    g_in_test = 0;
    return;

abort_test:
    printf("\n\n\033[1;33m[Test Aborted by User]\033[0m\n");
    g_in_test = 0;
    return;
}

int pdb_testcase_pipeline_get(char* ip, int port, char* expect_result){
    int connfd = connect_tcpserver(ip, port);

    if (connfd == -1){
        printf("connect error\n");
        return -1;
    }

    testcase_get_exist_100w(connfd, expect_result);

    return 0;
}