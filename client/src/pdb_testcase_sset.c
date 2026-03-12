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

// 校验模式枚举
enum VerifyMode {
    VERIFY_NONE = 0,
    VERIFY_SSADD,
    VERIFY_SSCORE,
    VERIFY_SSRANK,
    VERIFY_SSRANGE
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
            
            if (line_end > current && *(line_end - 1) == '\r') {
                *(line_end - 1) = '\0';
            } else {
                *line_end = '\0';
            }

            int current_item_idx = start_idx + received_count;
            char expected[64] = {0};
            int verify_failed = 0;

            if (mode == VERIFY_SSADD) {
                strcpy(expected, "OK");
                if (strcmp(current, expected) != 0) verify_failed = 1;
                
            } else if (mode == VERIFY_SSCORE) {
                sprintf(expected, "%d.000000", (current_item_idx * 37) % 100000);
                double exp_val = atof(expected);
                double act_val = atof(current);
                if (act_val < exp_val - 0.001 || act_val > exp_val + 0.001) {
                    verify_failed = 1;
                    sprintf(expected, "%.1f", exp_val); 
                }
                
            } else if (mode == VERIFY_SSRANK) {
                if (strncmp(current, "NO EXIST", 8) == 0 || atoi(current) <= 0) {
                    strcpy(expected, "A valid positive rank number");
                    verify_failed = 1;
                }
                
            } else if (mode == VERIFY_SSRANGE) {
                if (strncmp(current, "player:", 7) != 0) {
                    strcpy(expected, "String starting with 'player:'");
                    verify_failed = 1;
                }
            }

            if (verify_failed) {
                printf("\n\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Data Verification Failed!\033[0m\n");
                printf("Test Mode : %d\n", mode);
                printf("Item Index: %d\n", current_item_idx);
                printf("Expected  : '%s'\n", expected);
                printf("Actual    : '%s'\n", current);
                printf("============================================\n");
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

void testcase_100w_sortedset(int fd) {
    int count_write = 1000000; 
    int count_read = 100000;   
    
    char batch_buf[32768];
    int batch_len = 0;
    int current_batch_count = 0;

    struct timeval tv_begin, tv_end;
    long time_used;

    printf("========================================\n");
    printf("   PDB SORTEDSET PIPELINE QPS TESTSUITE \n");
    printf("========================================\n");

    // ---------------------------------------------------------
    // SSADD (100 万玩家入榜)
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_write; i++) {
        char member_str[32];
        char score_str[32];
        int mem_len = sprintf(member_str, "player:%d", i);
        int score_len = sprintf(score_str, "%d.0", (i * 37) % 100000); 

        batch_len += sprintf(batch_buf + batch_len, 
            "*4\r\n$5\r\nSSADD\r\n$11\r\nleaderboard\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
            mem_len, member_str, score_len, score_str);
        
        current_batch_count++;
        if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_SSADD, i + 1 - current_batch_count);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("SSADD", i + 1, count_write);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_SSADD, count_write - current_batch_count);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[SSADD   ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_write, time_used, (count_write * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 2. SSCORE 测试 (O(1) 精准分数值校验)
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_read; i++) {
        char member_str[32];
        int mem_len = sprintf(member_str, "player:%d", i);

        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$6\r\nSSCORE\r\n$11\r\nleaderboard\r\n$%d\r\n%s\r\n", 
            mem_len, member_str);
        
        current_batch_count++;
        if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_SSCORE, i + 1 - current_batch_count);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("SSCORE", i + 1, count_read);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_SSCORE, count_read - current_batch_count);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[SSCORE  ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_read, time_used, (count_read * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 3. SSRANK 测试
    // ---------------------------------------------------------
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < count_read; i++) {
        char member_str[32];
        int mem_len = sprintf(member_str, "player:%d", i);

        batch_len += sprintf(batch_buf + batch_len, 
            "*3\r\n$6\r\nSSRANK\r\n$11\r\nleaderboard\r\n$%d\r\n%s\r\n", 
            mem_len, member_str);
        
        current_batch_count++;
        if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_SSRANK, i + 1 - current_batch_count);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("SSRANK", i + 1, count_read);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_SSRANK, count_read - current_batch_count);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[SSRANK  ] %d requests | Time: %ld ms | QPS: %ld\n\n", count_read, time_used, (count_read * 1000L) / (time_used ? time_used : 1));

    // ---------------------------------------------------------
    // 4. SSRANGE 测试 (拉取全服前 100 名)
    // ---------------------------------------------------------
    printf("\n[SSRANGE ] Fetching Top 100 Players...\n");
    const char* range_cmd = "*4\r\n$7\r\nSSRANGE\r\n$11\r\nleaderboard\r\n$1\r\n0\r\n$2\r\n99\r\n";
    send_batch_msg(fd, (char*)range_cmd, strlen(range_cmd));
    
    recv_and_verify_responses(fd, 100, VERIFY_SSRANGE, 0); 
    printf("\n[SSRANGE ] Top 100 fetch completed and verified successfully.\n");
}

void pdb_testcase_sortedset(const char* ip, unsigned short port) {
    int fd = connect_tcpserver(ip, port);
    if (fd < 0) return;
    
    testcase_100w_sortedset(fd);
    
    close(fd);
}