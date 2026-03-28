#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "pdb_cli_tool.h"

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define BUFFER_SIZE (64 * 1024)

#define ARRAY   0
#define RBTREE  1
#define HASH    1

#define BATCH          100
#define BATCH_NUM      10000

int mset_send_all(int fd, char* msg, int length) {
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        int res = send(fd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR) continue;
            perror("send error");
            return -1;
        }
        total_sent += res;
        ptr += res;
        left -= res;
    }
    return total_sent;
}


int mset_verify_response(int fd) {
    char buffer[64];
    int n = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) {
        perror("recv error or server closed");
        return -1;
    }
    buffer[n] = '\0';
    if (strncmp(buffer, "+OK", 2) == 0) {
        return 0; // 成功
    }else if (strncmp(buffer, "ERROR", 5) == 0){
        return 1;
    }else if (strncmp(buffer, "EXIST", 5) == 0){
        return 2;
    }
    printf("Failed: expected OK, got %s\n", buffer);
    return -1;
}


void testcase_mset_100w(int connfd) {
    printf("Starting MSET testcase (10 batches * 10,000 keys)...\n");

    int batches = BATCH;           
    int keys_per_batch = BATCH_NUM; 
    long long total_keys = batches * keys_per_batch;

    int buffer_size = 2 * 1024 * 1024; 
    char *send_buf = malloc(buffer_size);
    if (!send_buf) {
        perror("malloc failed");
        exit(1);
    }

    struct timeval tv_start, tv_end;
    int global_key_idx = 0;
    long time_used_ms;
    double seconds;
    long qps;
    double avg_latency_ms;

    
#if ARRAY
    // Array ---> 
    gettimeofday(&tv_start, NULL);
    memset(send_buf, 0, buffer_size);
    for (int b = 0; b < batches; b++) {
        int offset = 0;
        
        int num_args = 1 + (keys_per_batch * 2);
        offset += sprintf(send_buf + offset, "*%d\r\n$4\r\nMSET\r\n", num_args);

        for (int k = 0; k < keys_per_batch; k++) {
            char key[32], val[32];
            int klen = sprintf(key, "AK_%d", global_key_idx);
            int vlen = sprintf(val, "AV_%d", global_key_idx);
            
            // 拼接 Key: $Len\r\nKey\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", klen, key);
            // 拼接 Value: $Len\r\nValue\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", vlen, val);

            global_key_idx++;
        }

        if (mset_send_all(connfd, send_buf, offset) < 0) {
            exit(1);
        }

        int ret = mset_verify_response(connfd);
        if (ret == 0){
            printf("OK\n");
        }else if (ret == 1){
            printf("ERRPR\n");
        }else if (ret == 2){
            printf("EXIST\n");
        }else{
          exit(-1);
        }
        
    }

    gettimeofday(&tv_end, NULL);
    
    time_used_ms = TIME_SUB_MS(tv_end, tv_start);
    seconds = time_used_ms / 1000.0;
    
    qps = (long)(total_keys / seconds);
    
    avg_latency_ms = (double)time_used_ms / batches; 

    printf("---------------ARRAY---------------------------------\n");
    printf("Total Keys Set:  %lld\n", total_keys);
    printf("Total Time:      %ld ms\n", time_used_ms);
    printf("QPS (Keys/sec):  %ld\n", qps);
    printf("Latency:     %.2f ms per MSET command\n", avg_latency_ms);
    printf("------------------------------------------------\n");
#endif 
    
    
    
#if RBTREE   
    gettimeofday(&tv_start, NULL);
    global_key_idx = 0;
    memset(send_buf, 0, buffer_size);
    
    for (int b = 0; b < batches; b++) {
        int offset = 0;
        
        int num_args = 1 + (keys_per_batch * 2);
        offset += sprintf(send_buf + offset, "*%d\r\n$5\r\nRMSET\r\n", num_args);

        for (int k = 0; k < keys_per_batch; k++) {
            char key[32], val[32];
            int klen = sprintf(key, "RK_%d", global_key_idx);
            int vlen = sprintf(val, "RV_%d", global_key_idx);
            
            // 拼接 Key: $Len\r\nKey\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", klen, key);
            // 拼接 Value: $Len\r\nValue\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", vlen, val);

            global_key_idx++;
        }

        if (mset_send_all(connfd, send_buf, offset) < 0) {
            exit(1);
        }

        int ret = mset_verify_response(connfd);
        if (ret == 0){
            printf("OK\n");
        }else if (ret == 1){
            printf("ERRPR\n");
        }else if (ret == 2){
            printf("EXIST\n");
        }else{
          exit(-1);
        }
        
    }

    gettimeofday(&tv_end, NULL);
    
    time_used_ms = TIME_SUB_MS(tv_end, tv_start);
    seconds = time_used_ms / 1000.0;
    
    qps = (long)(total_keys / seconds);
    
    avg_latency_ms = (double)time_used_ms / batches; 
    

    printf("----------------------RBtree--------------------------\n");
    printf("Total Keys Set:  %lld\n", total_keys);
    printf("Total Time:      %ld ms\n", time_used_ms);
    printf("QPS (Keys/sec):  %ld\n", qps);
    printf("Latency:     %.2f ms per MSET command\n", avg_latency_ms);
    printf("------------------------------------------------\n");
#endif
    

#if HASH
    // Hash ---> 
    gettimeofday(&tv_start, NULL);
    global_key_idx = 0;
    memset(send_buf, 0, buffer_size);
    for (int b = 0; b < batches; b++) {
        int offset = 0;
        
        int num_args = 1 + (keys_per_batch * 2);
        offset += sprintf(send_buf + offset, "*%d\r\n$5\r\nHMSET\r\n", num_args);

        for (int k = 0; k < keys_per_batch; k++) {
            char key[32], val[32];
            int klen = sprintf(key, "HK_%d", global_key_idx);
            int vlen = sprintf(val, "HV_%d", global_key_idx);
            
            // 拼接 Key: $Len\r\nKey\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", klen, key);
            // 拼接 Value: $Len\r\nValue\r\n
            offset += sprintf(send_buf + offset, "$%d\r\n%s\r\n", vlen, val);

            global_key_idx++;
        }

        if (mset_send_all(connfd, send_buf, offset) < 0) {
            exit(1);
        }

        int ret = mset_verify_response(connfd);
        if (ret == 0){
            printf("OK\n");
        }else if (ret == 1){
            printf("ERRPR\n");
        }else if (ret == 2){
            printf("EXIST\n");
        }else{
          exit(-1);
        }
        
    }

    gettimeofday(&tv_end, NULL);
    time_used_ms = TIME_SUB_MS(tv_end, tv_start);
    seconds = time_used_ms / 1000.0;
    
    qps = (long)(total_keys / seconds);
    
    avg_latency_ms = (double)time_used_ms / batches; 

    printf("---------------HASH---------------------------------\n");
    printf("Total Keys Set:  %lld\n", total_keys);
    printf("Total Time:      %ld ms\n", time_used_ms);
    printf("QPS (Keys/sec):  %ld\n", qps);
    printf("Latency:     %.2f ms per MSET command\n", avg_latency_ms);
    printf("------------------------------------------------\n");
#endif

    free(send_buf);
}


void recv_and_print_raw(int fd, int expected_count) {
    char buffer[BUFFER_SIZE];
    int n;
    int items_received = 0;
    
    // 循环接收，直到收齐 expected_count 个换行符
    while (items_received < expected_count) {
        n = recv(fd, buffer, BUFFER_SIZE - 1, 0);
        
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                continue; // 暂时无数据，继续等待
            }
            perror("recv error");
            break; // 致命错误，退出
        } 
        else if (n == 0) {
            // 服务器断开
            printf("\nServer closed connection prematurely. Received %d/%d items.\n", 
                   items_received, expected_count);
            break; 
        }

        // 收到数据，进行处理
        buffer[n] = '\0'; 
        
        // 遍历收到的数据，统计有多少个 '\n'
        for (int i = 0; i < n; i++) {
            if (buffer[i] == '\n') {
                items_received++;
            }
        }
        printf("recv chunk: %s", buffer);
    }
}

int pdb_testcase_mset(char* ip, int port) {
    int connfd = connect_tcpserver(ip, port);
    if (connfd == -1) {
        exit(-1);
    }

    testcase_mset_100w(connfd);

    close(connfd);
    return 0;
}