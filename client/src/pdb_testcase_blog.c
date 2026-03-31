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

#define MAX_MSG_LENGTH  1024
#define SEND_BATCH      1024
#define BLOG_NUM        50

#define HASH            1
#define RBTREE          1
#define ARRAY           0

#define BLOG_CONTENT_SIZE (10 * 1024 * 1024)

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


static int blog_send_msg(int connfd, char* msg, int length){
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        // printf("send: %.*s\n", left, ptr);
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

static int blog_recv_msg(int connfd, char* msg, int length){
    int res = recv(connfd, msg, length, 0);
    if (res < 0){
        perror("recv error");
        return -1;
    }

    return res;
}



static void testcase_large_blog(int connfd) {
    // 准备 20MB 的博客内容数据
    char *content = (char*)malloc(BLOG_CONTENT_SIZE + 1);
    if (!content) {
        perror("malloc content failed");
        exit(1);
    }
    // 用 'a' 填充内容，模拟博客正文
    memset(content, 'a', BLOG_CONTENT_SIZE);
    content[BLOG_CONTENT_SIZE] = '\0';

    size_t send_buf_size = BLOG_CONTENT_SIZE + 1024; 
    char *send_buf = (char*)malloc(send_buf_size);
    if (!send_buf) {
        perror("malloc send_buf failed");
        free(content);
        exit(1);
    }

    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    int count = BLOG_NUM; // BLOG_NUM * 20M
    int i = 0;
    int time_used;
    // long long total_bytes;
    // double throughput;

#if RBTREE
    printf("############TEST RBTREE################\n");
    for (i = 0; i < count; i++) {
        char key[64];
        int key_len = sprintf(key, "Blog_Article_%d", i);
    
        // int header_len = sprintf(send_buf, "*3\r\n$3\r\nSET\r\n$%d\r\nBlog_Article_%d\r\n$%d\r\n", i, (int)strlen("Blog_Article_1"), BLOG_CONTENT_SIZE);
        int header_len = sprintf(send_buf, 
            "*3\r\n"
            "$4\r\nRSET\r\n"
            "$%d\r\n%s\r\n"  
            "$%d\r\n", 
            key_len, key,        
            BLOG_CONTENT_SIZE); 
        
        memcpy(send_buf + header_len, content, BLOG_CONTENT_SIZE);
        
        int total_len = header_len + BLOG_CONTENT_SIZE;
        memcpy(send_buf + total_len, "\r\n", 2);
        total_len += 2;

        if (blog_send_msg(connfd, send_buf, total_len) != total_len) {
            printf("Failed to send large blog %d\n", i);
            exit(-1);
        }

        // 接收响应
        char result[MAX_MSG_LENGTH] = {0};
        int n = blog_recv_msg(connfd, result, MAX_MSG_LENGTH); 
        if (n <= 0) {
            printf("Server disconnected or error\n");
            exit(-1);
        }
        
        if (strncmp(result, "+OK", 2) == 0) {
             printf("==> PASS -> Large Blog %d saved\n", i);
        } else {
             printf("==> FAILED -> Blog %d, Response: %s\n", i, result);
             exit(-1);
        }
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);

#endif

#if HASH
    printf("############TEST HASH################\n");
    for (i = 0; i < count; i++) {
        char key[64];
        int key_len = sprintf(key, "Blog_Article_%d", i);
    
        // int header_len = sprintf(send_buf, "*3\r\n$3\r\nSET\r\n$%d\r\nBlog_Article_%d\r\n$%d\r\n", i, (int)strlen("Blog_Article_1"), BLOG_CONTENT_SIZE);
        int header_len = sprintf(send_buf, 
            "*3\r\n"
            "$4\r\nHSET\r\n"
            "$%d\r\n%s\r\n"  
            "$%d\r\n", 
            key_len, key,        
            BLOG_CONTENT_SIZE); 
        
        memcpy(send_buf + header_len, content, BLOG_CONTENT_SIZE);
        
        int total_len = header_len + BLOG_CONTENT_SIZE;
        memcpy(send_buf + total_len, "\r\n", 2);
        total_len += 2;

        if (blog_send_msg(connfd, send_buf, total_len) != total_len) {
            printf("Failed to send large blog %d\n", i);
            exit(-1);
        }

        char result[MAX_MSG_LENGTH] = {0};
        int n = blog_recv_msg(connfd, result, MAX_MSG_LENGTH); 
        if (n <= 0) {
            printf("Server disconnected or error\n");
            exit(-1);
        }
        
        if (strncmp(result, "+OK", 2) == 0) {
             printf("==> PASS -> Large Blog %d saved\n", i);
        } else {
             printf("==> FAILED -> Blog %d, Response: %s\n", i, result);
             exit(-1);
        }
    }

    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
#endif
    free(content);
    free(send_buf);
}

int pdb_testcase_blog(char* ip, int port){
    int connfd = connect_tcpserver(ip, port);

    if (connfd == -1){
        printf("connect error\n");
        exit(-1);
    }

    testcase_large_blog(connfd);

    close(connfd);
    return 0;
}