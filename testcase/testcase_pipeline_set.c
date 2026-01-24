#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>

#define MAX_MSG_LENGTH  1024
#define SEND_BATCH      1024
#define CMD_NUM         1000000

#define ARRAY           0
#define HASH            1
#define RBTREE          1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

int send_msg(int connfd, char* msg, int length){
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        // printf("send: %.*s\n", left, ptr);
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR) continue; // 被信号打断，重试
            // 如果是阻塞模式，EAGAIN 理论上不会出现，但为了稳健可以加
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
    
    // 只有当 total_sent == length 时才算成功
    return total_sent;
}

int recv_msg(int connfd, char* msg, int length){
    int res = recv(connfd, msg, length, 0);
    if (res < 0){
        perror("recv error");
        return -1;
    }

    return res;
}


int connect_tcpserver(const char* ip, unsigned short port){
    int connfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;

    int ret = connect(connfd, (struct sockaddr*)&addr, sizeof(struct sockaddr));
    if (ret < 0){
        printf("connect_tcpserver: connect error\n");
        perror("connect");
        return -1;
    }

    return connfd;
}

void testcase(int connfd, char* msg, char* pattern, char* casename){
    if (!msg || !pattern || !casename)    return ;

    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    if (strcmp(result, pattern) == 0){
#if ENABLE_PRINT_KV
        printf("==> PASS -> %s\n", casename);
#endif
    }else {
        printf("==> FAILED -> %s, '%s' != '%s'\n", casename, result, pattern);
        exit(-1);
    }

}

void verify_responses(int fd, int expect_count, const char* pattern) {
    char buffer[16384]; 
    int received_count = 0;
    int buf_len = 0;    
    int pattern_len = strlen(pattern);

    while (received_count < expect_count) {
        // 追加接收
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
        // printf("recv: %s\n", current);
        while (1) {
            char *line_end = strchr(current, '\n');
            if (line_end == NULL) {
                break; // 半包，跳出内层循环继续 recv
            }
            int current_resp_len = line_end - current + 1;
            
            if (strncmp(current, pattern, pattern_len) != 0) {
                // 校验失败！
                // 把 \n 换成 \0 方便打印错误信息
                *line_end = '\0'; 
                // 去掉可能的 \r
                if (current_resp_len > 1 && current[current_resp_len-2] == '\r') 
                    current[current_resp_len-2] = '\0';

                printf("\n[FATAL] Response Verify Failed!\n");
                printf("Expected: %s", pattern); // pattern自带换行
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

void testcase_100w(int connfd){
    int count = 1000000;
    int i = 0;
    char cmd[4096];
    int response_count = 0;
    
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
    printf("RBTREE test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // RSET DAI{i} {i}
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$4\r\nRSET\r\n"
            "$%d\r\n%s\r\n"   
            "$%d\r\n%s\r\n",  
            k_len, key, 
            v_len, val);
        
        // RSET TAO{i} {i}
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$4\r\nRSET\r\n"
            "$%d\r\n%s\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key, 
            v_len, val);
        
        // RDEL TAO{i}
        k_len = sprintf(key, "TAO%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$4\r\nRDEL\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            verify_responses(connfd, response_count, "OK\r\n");
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        verify_responses(connfd, response_count, "OK\r\n");
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("RBTree testcase --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif

    response_count = 0;
#if HASH
    //############## Hash ######################
    printf("HASH test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // RSET DAI{i} {i}
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$4\r\nHSET\r\n"
            "$%d\r\n%s\r\n"   
            "$%d\r\n%s\r\n",  
            k_len, key, 
            v_len, val);
        
        // RSET TAO{i} {i}
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$4\r\nHSET\r\n"
            "$%d\r\n%s\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key, 
            v_len, val);
        
        // RDEL TAO{i}
        k_len = sprintf(key, "TAO%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$4\r\nHDEL\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            verify_responses(connfd, response_count, "OK\r\n");
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        verify_responses(connfd, response_count, "OK\r\n");
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("HASH testcase --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif

    response_count = 0;
#if ARRAY
    //############## Array ######################  
    printf("ARRAY test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // RSET DAI{i} {i}
        k_len = sprintf(key, "DAI%d", i);  
        v_len = sprintf(val, "%d", i);     
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$3\r\nSET\r\n"
            "$%d\r\n%s\r\n"   
            "$%d\r\n%s\r\n",  
            k_len, key, 
            v_len, val);
        
        // RSET TAO{i} {i}
        k_len = sprintf(key, "TAO%d", i);
        v_len = sprintf(val, "%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n"
            "$3\r\nSET\r\n"
            "$%d\r\n%s\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key, 
            v_len, val);
        
        // RDEL TAO{i}
        k_len = sprintf(key, "TAO%d", i);
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$3\r\nDEL\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 3;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            verify_responses(connfd, response_count, "OK\r\n");
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        verify_responses(connfd, response_count, "OK\r\n");
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("ARRAY testcase --> time_use: %ld, qps: %ld\n", time_used, 3000000L * 1000 / time_used);
#endif    
}   


// ./testcase 192.168.127.222 8888
int main(int argc, char* argv[]){
    if (argc != 3){
        printf("argc error\n");
        return -1;
    }

    char* ip = argv[1];
    int port = atoi(argv[2]);

    int connfd = connect_tcpserver(ip, port);

    if (connfd == -1){
        printf("connect error\n");
        exit(-1);
    }


    // array_testcase_10w(connfd);
    testcase_100w(connfd);

    return 0;
}