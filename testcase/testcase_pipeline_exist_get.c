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

void verify_responses(int fd, int expect_count, const char* pattern) {
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
                 if (strcmp(pattern, "OK\r\n") == 0) {
                    *line_end = '\0'; 
                    printf("\n[FATAL] Response Verify Failed!\n");
                    printf("Expected: %s", pattern);
                    printf("Received: %s\n", current);
                    exit(-1);
                 }
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

void recv_and_print_responses(int fd, int expect_count) {
    char buffer[16384]; 
    int received_count = 0;
    int buf_len = 0;    

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
            
            char saved_char = *(line_end + 1);
            *(line_end + 1) = '\0';
            
            printf("Server Response: %s", current);
            
            *(line_end + 1) = saved_char;

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


void testcase_get_exist_100w(int connfd){
    int count = 1000000;
    int i = 0;
    int response_count = 0;
    
    char batch_buf[8192];
    int batch_len = 0;

    struct timeval tv_begin;
    struct timeval tv_end;
    long time_used;

    char key[64];
    int k_len;

    // ############## RBtree ###################### 
#if RBTREE
    printf("RBTREE GET/EXIST test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // RGET DAI{i}
        k_len = sprintf(key, "DAI%d", i);  
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$4\r\nRGET\r\n"
            "$%d\r\n%s\r\n",   
            k_len, key);
        
        // REXIST DAI{i}
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$6\r\nREXIST\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 2;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            recv_and_print_responses(connfd, response_count);
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        recv_and_print_responses(connfd, response_count);
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("RBTree GET/EXIST testcase --> time_use: %ld, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif

    response_count = 0;
#if HASH
    //############## Hash ######################
    printf("HASH GET/EXIST test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // HGET DAI{i}
        k_len = sprintf(key, "DAI%d", i);  
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$4\r\nHGET\r\n"
            "$%d\r\n%s\r\n",   
            k_len, key);
        
        // HEXIST DAI{i}
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$6\r\nHEXIST\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 2;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            recv_and_print_responses(connfd, response_count);
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        recv_and_print_responses(connfd, response_count);
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("HASH GET/EXIST testcase --> time_use: %ld, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif

    response_count = 0;
#if ARRAY
    //############## Array ######################  
    printf("ARRAY GET/EXIST test begin\n");
    gettimeofday(&tv_begin, NULL);
    for(i = 0; i < count; i++){
        if (i % 10000 == 0){
            printf("i: %d\n", i);
        }
        
        // GET DAI{i}
        k_len = sprintf(key, "DAI%d", i);  
        
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$3\r\nGET\r\n"
            "$%d\r\n%s\r\n",   
            k_len, key);
        
        // EXIST DAI{i}
        batch_len += sprintf(batch_buf + batch_len, 
            "*2\r\n"
            "$5\r\nEXIST\r\n"
            "$%d\r\n%s\r\n", 
            k_len, key);
        
        response_count += 2;
        
        if (batch_len > 4096 || response_count >= SEND_BATCH) { 
            if (batch_len > 0) {
                send_msg(connfd, batch_buf, batch_len);
                batch_len = 0;
            }
        }
        
        if (response_count >= SEND_BATCH) {
            recv_and_print_responses(connfd, response_count);
            response_count = 0;
        }
    }
    
    if (batch_len > 0) {
        send_msg(connfd, batch_buf, batch_len);
    }
    if (response_count > 0) {
        recv_and_print_responses(connfd, response_count);
    }

    gettimeofday(&tv_end, NULL);

    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("ARRAY GET/EXIST testcase --> time_use: %ld, qps: %ld\n", time_used, 2000000L * 1000 / time_used);
#endif    
} 

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

    testcase_get_exist_100w(connfd);

    return 0;
}