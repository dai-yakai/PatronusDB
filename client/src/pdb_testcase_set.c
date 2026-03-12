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
    VERIFY_EXACT = 1,       
    VERIFY_PREFIX,          
    VERIFY_NOT_ERROR        
};


static int send_batch_msg(int connfd, char* msg, int length) {
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

// 增加 capture_buf 参数，如果传了，就把收到的最后一条数据拷出来供后续查验
static void recv_and_verify_responses(int fd, int expect_count, int mode, const char* expected_val, char* capture_buf) {
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

            int verify_failed = 0;

            if (mode == VERIFY_EXACT) {
                if (strcmp(current, expected_val) != 0) verify_failed = 1;
            } else if (mode == VERIFY_PREFIX) {
                if (strncmp(current, expected_val, strlen(expected_val)) != 0) verify_failed = 1;
            }

            if (verify_failed) {
                printf("\n\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Data Verification Failed!\033[0m\n");
                printf("Expected  : '%s'\n", expected_val ? expected_val : "VALID_DATA");
                printf("Actual    : '%s'\n", current);
                printf("============================================\n");
                exit(-1); 
            }

            // 抓取数据
            if (capture_buf != NULL) {
                strcpy(capture_buf, current);
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

// ✨ 专为交并差设计的集合数学内容校验引擎
static void recv_and_verify_math_set(int fd, int expect_count, int min_val, int max_val) {
    char buffer[16384];
    int received_count = 0, buf_len = 0;
    
    int range = max_val - min_val + 1;
    char* visited = (char*)calloc(range, sizeof(char)); // 访问记录表

    while (received_count < expect_count) {
        int n = recv(fd, buffer + buf_len, sizeof(buffer) - buf_len - 1, 0);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
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

            // 提取数字并校验内容边界
            int val_id;
            if (sscanf(current, "op_mem:%d", &val_id) != 1) {
                printf("\n\033[1;31m[FATAL ERROR] Unexpected element format: %s\033[0m\n", current);
                exit(-1);
            }
            if (val_id < min_val || val_id > max_val) {
                printf("\n\033[1;31m[FATAL ERROR] Element Out of Bounds! %d not in [%d, %d]\033[0m\n", val_id, min_val, max_val);
                exit(-1);
            }
            if (visited[val_id - min_val] == 1) {
                printf("\n\033[1;31m[FATAL ERROR] Duplicate Element found: %d\033[0m\n", val_id);
                exit(-1);
            }
            
            // 打勾
            visited[val_id - min_val] = 1;

            received_count++;
            current = line_end + 1;
            if (received_count == expect_count) break;
        }

        int consumed = current - buffer;
        int remaining = buf_len - consumed;
        if (consumed > 0 && remaining > 0) memmove(buffer, current, remaining);
        buf_len = remaining;
    }

    // 严苛检验：是否真的无遗漏全覆盖
    for (int i = 0; i < range; i++) {
        if (visited[i] == 0) {
            printf("\n\033[1;31m[FATAL ERROR] Missing Element! Expected %d but never received.\033[0m\n", min_val + i);
            exit(-1);
        }
    }
    free(visited);
}


void testcase_all_set(int fd) {
    char batch_buf[32768];
    int batch_len = 0, current_batch_count = 0;
    struct timeval tv_begin, tv_end;
    long time_used;

    printf("========================================\n");
    printf("     PDB SET COMPREHENSIVE TESTSUITE    \n");
    printf("========================================\n");

    int basic_count = 100000; 

    // ==========================================
    // 基础增删改查压测
    // ==========================================
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < basic_count; i++) {
        char val_str[32];
        int val_len = sprintf(val_str, "member:%d", i);
        batch_len += sprintf(batch_buf + batch_len, "*3\r\n$4\r\nSSET\r\n$4\r\nset1\r\n$%d\r\n%s\r\n", val_len, val_str);
        
        current_batch_count++;
        if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
            batch_len = 0; current_batch_count = 0;
        }
        print_progress("SSET", i + 1, basic_count);
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
        batch_len = 0; current_batch_count = 0;
    }
    gettimeofday(&tv_end, NULL);
    time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("\n[SSET    ] %d requests | Time: %ld ms | QPS: %ld\n", basic_count, time_used, (basic_count * 1000L) / (time_used ? time_used : 1));

    // 测试 SCARD
    const char* card_cmd1 = "*2\r\n$5\r\nSCARD\r\n$4\r\nset1\r\n";
    send_batch_msg(fd, (char*)card_cmd1, strlen(card_cmd1));
    recv_and_verify_responses(fd, 1, VERIFY_EXACT, "100000", NULL); 

    // 测试 SSDEL 和 SCARD 同步校验
    printf("\n[SSDEL   ] Testing Deletions...\n");
    batch_len = 0; current_batch_count = 0;
    for (int i = 0; i < 50000; i++) { 
        char val_str[32];
        int val_len = sprintf(val_str, "member:%d", i);
        batch_len += sprintf(batch_buf + batch_len, "*3\r\n$4\r\nSDEL\r\n$4\r\nset1\r\n$%d\r\n%s\r\n", val_len, val_str);
        current_batch_count++;
        if (batch_len > 16384 || current_batch_count >= BATCH_SIZE) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
            batch_len = 0; current_batch_count = 0;
        }
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
    }
    
    send_batch_msg(fd, (char*)card_cmd1, strlen(card_cmd1));
    recv_and_verify_responses(fd, 1, VERIFY_EXACT, "50000", NULL); 
    printf("[SSDEL   ] Verified successfully (Remaining count matches exactly).\n");

    // ==========================================
    // 精准内容校验：POP 的存在性抹除校验
    // ==========================================
    printf("\n[SPOP    ] Testing SRANDOMPOP Correctness...\n");
    const char* pop_cmd = "*2\r\n$10\r\nSRANDOMPOP\r\n$4\r\nset1\r\n";
    char popped_val[64];
    
    // 弹出一个元素，抓取到 popped_val 中
    send_batch_msg(fd, (char*)pop_cmd, strlen(pop_cmd));
    recv_and_verify_responses(fd, 1, VERIFY_PREFIX, "member:", popped_val); 

    // ✨ 核心：检查刚才弹出的元素是否真的从服务端消失了！
    char exist_cmd[128];
    sprintf(exist_cmd, "*3\r\n$6\r\nSEXIST\r\n$4\r\nset1\r\n$%lu\r\n%s\r\n", strlen(popped_val), popped_val);
    send_batch_msg(fd, exist_cmd, strlen(exist_cmd));
    recv_and_verify_responses(fd, 1, VERIFY_EXACT, "NO EXIST", NULL); // 必须是 NO EXIST！

    // 检查总数是否减少了 1 变为了 49999
    send_batch_msg(fd, (char*)card_cmd1, strlen(card_cmd1));
    recv_and_verify_responses(fd, 1, VERIFY_EXACT, "49999", NULL); 
    printf("[SPOP    ] Verified: Element '%s' actually removed from DB.\n", popped_val);


    // ==========================================
    // 集合运算 (交、并、差) 
    // ==========================================
    printf("\n========================================\n");
    printf("   STAGE 2: SINTER / SUNION / SDIFFER   \n");
    printf("========================================\n");
    
    // setA: 0 ~ 9999 (10000 个)
    // setB: 5000 ~ 14999 (10000 个)
    printf("[SETUP   ] Building setA (0-9999) and setB (5000-14999)...\n");
    batch_len = 0; current_batch_count = 0;
    for (int i = 0; i < 15000; i++) {
        char val_str[32];
        int val_len = sprintf(val_str, "op_mem:%d", i);

        if (i < 10000) { 
            batch_len += sprintf(batch_buf + batch_len, "*3\r\n$4\r\nSSET\r\n$4\r\nsetA\r\n$%d\r\n%s\r\n", val_len, val_str);
            current_batch_count++;
        }
        if (i >= 5000) { 
            batch_len += sprintf(batch_buf + batch_len, "*3\r\n$4\r\nSSET\r\n$4\r\nsetB\r\n$%d\r\n%s\r\n", val_len, val_str);
            current_batch_count++;
        }

        if (batch_len > 16384) { 
            send_batch_msg(fd, batch_buf, batch_len);
            recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
            batch_len = 0; current_batch_count = 0;
        }
    }
    if (batch_len > 0) {
        send_batch_msg(fd, batch_buf, batch_len);
        recv_and_verify_responses(fd, current_batch_count, VERIFY_EXACT, "OK", NULL);
    }

    // 测试交集 (setA ∩ setB = 5000~9999，共 5000 个)
    printf("\n[SINTER  ] Testing intersection...");
    const char* inter_cmd = "*3\r\n$6\r\nSINTER\r\n$4\r\nsetA\r\n$4\r\nsetB\r\n";
    send_batch_msg(fd, (char*)inter_cmd, strlen(inter_cmd));
    recv_and_verify_math_set(fd, 5000, 5000, 9999); 
    printf(" [PASS] Exact 5000 valid elements matched.\n");

    // 测试并集 (setA ∪ setB = 0~14999，共 15000 个)
    printf("[SUNION  ] Testing union........");
    const char* union_cmd = "*3\r\n$6\r\nSUNION\r\n$4\r\nsetA\r\n$4\r\nsetB\r\n";
    send_batch_msg(fd, (char*)union_cmd, strlen(union_cmd));
    recv_and_verify_math_set(fd, 15000, 0, 14999); 
    printf(" [PASS] Exact 15000 valid elements matched.\n");

    // 测试差集 (setA - setB = 0~4999，共 5000 个)
    printf("[SDIFFER ] Testing difference...");
    const char* differ_cmd = "*3\r\n$7\r\nSDIFFER\r\n$4\r\nsetA\r\n$4\r\nsetB\r\n";
    send_batch_msg(fd, (char*)differ_cmd, strlen(differ_cmd));
    recv_and_verify_math_set(fd, 5000, 0, 4999); 
    printf(" [PASS] Exact 5000 valid elements matched.\n");

    printf("\n========================================\n");
    printf("  ALL SET COMMANDS VERIFIED PERFECTLY!  \n");
    printf("========================================\n\n");
}

void pdb_testcase_set(const char* ip, unsigned short port) {
    int fd = connect_tcpserver(ip, port);
    if (fd < 0) return;
    testcase_all_set(fd);
    close(fd);
}