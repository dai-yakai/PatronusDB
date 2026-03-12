#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

#include "pdb_cli_tool.h"

#define BATCH_SIZE 1000

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


enum VerifyMode {
    VERIFY_NONE = 0,
    VERIFY_BITSET,
    VERIFY_BITGET,
    VERIFY_BITCOUNT,
    VERIFY_BITOP
};



static int send_batch_msg(int connfd, char* msg, int length) {
    int total_sent = 0;
    int left = length;
    char *ptr = msg;

    while (left > 0) {
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue; 
            perror("send error");
            return -1;
        }
        if (res == 0) return -1;
        total_sent += res;
        ptr += res;
        left -= res;
    }
    return total_sent;
}


static void recv_and_verify_responses(int fd, int expect_count, int mode, int start_idx) {
    char buffer[16384];
    int received_count = 0;
    int buf_len = 0;

    while (received_count < expect_count) {
        int n = recv(fd, buffer + buf_len, sizeof(buffer) - buf_len - 1, 0);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            perror("recv error");
            exit(-1);
        } else if (n == 0) {
            printf("\nServer closed connection unexpectedly\n");
            exit(-1);
        }
        
        buf_len += n;
        buffer[buf_len] = '\0'; 

        char *current = buffer;
        while (1) {
            char *line_end = strchr(current, '\n');
            if (line_end == NULL) break; 
            
            // 兼容 \r\n 和 \n，获取纯净返回值
            if (line_end > current && *(line_end - 1) == '\r') {
                *(line_end - 1) = '\0';
            } else {
                *line_end = '\0';
            }

            int current_item_idx = start_idx + received_count;
            char expected[64] = {0};

            // 🎯 根据不同的测试模式，推导期望值
            if (mode == VERIFY_BITSET) {
                strcpy(expected, "OK");
            } else if (mode == VERIFY_BITGET) {
                // 因为写入时是 010101 交替，即 偶数位为1，奇数位为0
                expected[0] = (current_item_idx % 2 == 0) ? '1' : '0';
                expected[1] = '\0';
            } else if (mode == VERIFY_BITCOUNT) {
                // 100万次写入中，正好有一半(50万)个 1
                strcpy(expected, "500000"); 
            } else if (mode == VERIFY_BITOP) {
                strcpy(expected, "OK");
            }

            // 🛑 执行硬核比对
            if (strcmp(current, expected) != 0 && mode != VERIFY_NONE) {
                printf("\n\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Data Verification Failed!\033[0m\n");
                printf("Test Mode : %d\n", mode);
                printf("Offset Idx: %d\n", current_item_idx);
                printf("Expected  : '%s'\n", expected);
                printf("Actual    : '%s'\n", current);
                printf("============================================\n");
                exit(-1); 
            }

            received_count++;
            current = line_end + 1;
            if (received_count == expect_count) break;
        }

        // 碎片化缓冲区整理
        int consumed = current - buffer;
        int remaining = buf_len - consumed;
        if (consumed > 0 && remaining > 0) {
            memmove(buffer, current, remaining);
        }
        buf_len = remaining;
    }
}


static void testcase_100w_bitmap(int fd) {
    int count_write = 1000000; 
    int count_op = 100000;     
    
    char batch_buf[16384];
    int batch_len = 0;
    int current_batch_count = 0;

    struct timeval tv_begin, tv_end;
    long time_used;
    char offset_str[32];
    int off_len;

    printf("========================================\n");
    printf("   PDB BITMAP PIPELINE QPS TESTSUITE    \n");
    printf("========================================\n");

    // ---------------------------------------------------------
    // 1. BITSET 测试 (交替写入 100 万个 bit)
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_write; i++) {
        off_len = sprintf(offset_str, "%d", i);
        char val_str = (i % 2 == 0) ? '1' : '0'; 

        batch_len += sprintf(batch_buf + batch_len, 
            "*4\r\n$6\r\nBITSET\r\n$4\r\nbmp1\r\n$%d\r\n%s\r\n$1\r\n%c\r\n", 
            off_len, offset_str, val_str);
        
        current_batch_count++;
        if (batch_len > 8192 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);

            recv_and_verify_responses(fd, current_batch_count, VERIFY_BITSET, i + 1 - current_batch_count);
            batch_len = 0;
            current_batch_count = 0;
        }
        print_progress("BITSET", i + 1, count_write);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_BITSET, count_write - current_batch_count);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[BITSET  ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_write, time_used, (count_write * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 2. BITGET 测试 (读取并强校验 100 万个 bit)
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_write; i++) {
        off_len = sprintf(offset_str, "%d", i);

        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$6\r\nBITGET\r\n$4\r\nbmp1\r\n$%d\r\n%s\r\n", 
            off_len, offset_str);
        
        current_batch_count++;
        if (batch_len > 8192 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            // ✨ 传入 VERIFY_BITGET 进行严格校验
            recv_and_verify_responses(fd, current_batch_count, VERIFY_BITGET, i + 1 - current_batch_count);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("BITGET", i + 1, count_write);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_BITGET, count_write - current_batch_count);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[BITGET  ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_write, time_used, (count_write * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 3. BITCOUNT 测试
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_op; i++) {
        batch_len += sprintf(batch_buf + batch_len, "*2\r\n$8\r\nBITCOUNT\r\n$4\r\nbmp1\r\n");
        current_batch_count++;
        if (batch_len > 8192 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_BITCOUNT, 0);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("BITCOUNT", i + 1, count_op);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_BITCOUNT, 0);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[BITCOUNT] %d requests | Time: %ld ms | QPS: %ld\n\n", count_op, time_used, (count_op * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 4. BITOP 测试 
    // ---------------------------------------------------------
    const char* bmp2_cmd = "*4\r\n$6\r\nBITSET\r\n$4\r\nbmp2\r\n$1\r\n0\r\n$1\r\n1\r\n";
    send_batch_msg(fd, (char*)bmp2_cmd, strlen(bmp2_cmd));
    recv_and_verify_responses(fd, 1, VERIFY_NONE, 0); 

    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_op; i++) {
        batch_len += sprintf(batch_buf + batch_len, "*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$6\r\nresbmp\r\n$4\r\nbmp1\r\n$4\r\nbmp2\r\n");
        current_batch_count++;
        if (batch_len > 8192 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_BITOP, 0);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("BITOP   ", i + 1, count_op);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_BITOP, 0);
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[BITOP   ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_op, time_used, (count_op * 1000L) / (time_used ? time_used : 1));
}

void pdb_testcase_bitmap(const char* ip, unsigned short port) {
    int fd = connect_tcpserver(ip, port);
    if (fd < 0) return;
    
    testcase_100w_bitmap(fd);
    
    close(fd);
}