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
#define TEST_COUNT 1000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

static int pipeline_send_msg(int connfd, char* msg, int length) {
    int total_sent = 0, left = length;
    char *ptr = msg;
    while (left > 0) {
        int res = send(connfd, ptr, left, 0);
        if (res < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue; 
            perror("send error"); return -1;
        }
        if (res == 0) return -1;
        total_sent += res; ptr += res; left -= res;
    }
    return total_sent;
}

static void verify_read_responses(int fd, int start_idx, int expect_count, int ds_type_id, const char* ds_type) {
    char buffer[16384];
    int received_count = 0, buf_len = 0;

    while (received_count < expect_count) {
        int n = recv(fd, buffer + buf_len, sizeof(buffer) - buf_len - 1, 0);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            printf("\nServer closed connection unexpectedly\n");
            exit(-1);
        }
        buf_len += n;
        buffer[buf_len] = '\0'; 
        char *current = buffer;
        
        while (1) {
            char *line_end = strchr(current, '\n');
            if (line_end == NULL) break; 
            if (line_end > current && *(line_end - 1) == '\r') *(line_end - 1) = '\0';
            else *line_end = '\0';

            int curr_i = start_idx + received_count;
            char expected[64] = {0};
            int verify_failed = 0;

            if (ds_type_id >= 0 && ds_type_id <= 2) { // ARRAY, RBTREE, HASH
                sprintf(expected, "%d", curr_i);
                if (strcmp(current, expected) != 0) verify_failed = 1;
            } else if (ds_type_id == 3) { // SET
                strcpy(expected, "EXIST");
                if (strcmp(current, expected) != 0) verify_failed = 1;
            } else if (ds_type_id == 4) { // SSET
                sprintf(expected, "%d.000000", curr_i % 100000);
                double exp_val = atof(expected);
                double act_val = atof(current);
                if (act_val < exp_val - 0.001 || act_val > exp_val + 0.001) verify_failed = 1;
            } else if (ds_type_id == 5) { // BITMAP
                expected[0] = (curr_i % 2 == 0) ? '1' : '0'; expected[1] = '\0';
                if (strcmp(current, expected) != 0) verify_failed = 1;
            }

            if (verify_failed) {
                printf("\n\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Read Verification Failed!\033[0m\n");
                printf("Data Structure : %s\n", ds_type);
                printf("Failed Index   : %d\n", curr_i);
                printf("Expected Reply : '%s'\n", expected);
                printf("Actual Reply   : '%s'\n", current);
                printf("============================================\n");
                exit(-1); 
            }

            received_count++;
            current = line_end + 1;
            if (received_count == expect_count) break;
        }

        int consumed = current - buffer;
        int remaining = buf_len - consumed;
        if (consumed > 0 && remaining > 0) memmove(buffer, current, remaining);
        buf_len = remaining;
    }
}

void testcase_read_100w(int fd) {
    char batch_buf[32768];
    int batch_len = 0, current_batch_count = 0, batch_start_i = 0;
    struct timeval tv_begin, tv_end;
    long time_used;
    char key[64];
    int k_len;

    printf("========================================\n");
    printf("     PDB ALL DATA READ PIPELINE TEST    \n");
    printf("========================================\n");

    const char* types[] = {"ARRAY", "RBTREE", "HASH", "SET", "SSET", "BITMAP"};
    const char* cmds[]  = {"GET",   "RGET",   "HGET", "SEXIST", "SSCORE", "BITGET"};

    for (int t = 0; t < 6; t++) {
        // 🎯 关键调整：如果是 ARRAY 类型，只跑 10w 条数据
        int current_limit = (t == 0) ? 1000 : TEST_COUNT;

        batch_len = 0; current_batch_count = 0; batch_start_i = 0;
        gettimeofday(&tv_begin, NULL);
        
        for (int i = 0; i < current_limit; i++) {
            if (t == 3) { // SET
                k_len = sprintf(key, "member:%d", i);
                batch_len += sprintf(batch_buf + batch_len, "*3\r\n$6\r\nSEXIST\r\n$5\r\nmyset\r\n$%d\r\n%s\r\n", k_len, key);
            } else if (t == 4) { // SSET
                k_len = sprintf(key, "player:%d", i);
                batch_len += sprintf(batch_buf + batch_len, "*3\r\n$6\r\nSSCORE\r\n$6\r\nmyzset\r\n$%d\r\n%s\r\n", k_len, key);
            } else if (t == 5) { // BITMAP
                k_len = sprintf(key, "%d", i);
                batch_len += sprintf(batch_buf + batch_len, "*3\r\n$6\r\nBITGET\r\n$5\r\nmybmp\r\n$%d\r\n%s\r\n", k_len, key);
            } else { // ARRAY, RBTREE, HASH
                k_len = sprintf(key, "%s_%d", types[t], i);
                int cmd_len = strlen(cmds[t]);
                batch_len += sprintf(batch_buf + batch_len, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", cmd_len, cmds[t], k_len, key);
            }

            current_batch_count++;
            if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) {
                pipeline_send_msg(fd, batch_buf, batch_len);
                verify_read_responses(fd, batch_start_i, current_batch_count, t, types[t]);
                batch_len = 0; batch_start_i = i + 1; current_batch_count = 0;
            }
            print_progress(types[t], i + 1, current_limit);
        }
        
        if (batch_len > 0) {
            pipeline_send_msg(fd, batch_buf, batch_len);
            verify_read_responses(fd, batch_start_i, current_batch_count, t, types[t]);
        }
        
        gettimeofday(&tv_end, NULL);
        time_used = TIME_SUB_MS(tv_end, tv_begin);
        printf("\n[%-8s READ ] %d Requests | %ld ms | QPS: %ld\n\n", types[t], current_limit, time_used, (current_limit * 1000L) / (time_used ? time_used : 1));
    }
}

int pdb_testcase_all_data_get(const char* ip, unsigned short port) {
    int fd = connect_tcpserver(ip, port);
    if (fd < 0) return -1;
    testcase_read_100w(fd);
    close(fd);
    return 0;
}