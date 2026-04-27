#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>


#define BATCH_SIZE 100000
#define TEST_COUNT 1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define PIPELINE_BUF_SIZE 256 * 1024  
#define PIPELINE_BATCH 5000

int connect_tcpserver(const char* ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;
    if (connect(connfd, (struct sockaddr*)&addr, sizeof(struct sockaddr)) < 0) return -1;
    return connfd;
}


void print_progress(const char* prefix, int current, int total) {
    if (current % 10000 == 0 || current == total) {
        int percent = (int)((long long)current * 100 / total);
        printf("\r[%-8s] [", prefix);
        for (int p = 0; p < 50; p++) {
            if (p < percent / 2) printf("=");
            else if (p == percent / 2) printf(">");
            else printf(" ");
        }
        printf("] %3d%% (%d/%d)", percent, current, total);
        fflush(stdout); 
    }
}

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

static void verify_write_responses(int fd, int start_idx, int expect_count, const char* ds_type) {
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

            if (strcmp(current, "+OK") != 0) {
                int fail_idx = start_idx + received_count;
                printf("\n\n============================================\n");
                printf("\033[1;31m[FATAL ERROR] Write Verification Failed!\033[0m\n");
                printf("Data Structure : %s\n", ds_type);
                printf("Failed Index   : %d\n", fail_idx);
                printf("Expected Reply : +OK\n");
                printf("Actual Reply   : %s\n", current);
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

void testcase_write_100w(int fd) {
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    char *batch_buf = malloc(PIPELINE_BUF_SIZE);
    int batch_len = 0, current_batch_count = 0, total_sent = 0;
    struct timeval tv_begin, tv_end;
    long time_used;
    char key[64], val[64];

    const char* types[] = {"ARRAY", "RBTREE", "HASH", "SET", "SSET", "BITMAP"};
    const char* cmds[]  = {"SET",   "RSET",  "HSET", "SSET", "SSADD", "BITSET"};

    for (int t = 0; t < 6; t++) {
        int current_limit = (t == 0) ? 1000 : TEST_COUNT;
        batch_len = 0; current_batch_count = 0; total_sent = 0;
        
        gettimeofday(&tv_begin, NULL);
        
        for (int i = 0; i < current_limit; i++) {
            int n = 0;
            if (t == 3) { // SET
                n = sprintf(val, "member:%d", i);
                n = sprintf(batch_buf + batch_len, "*3\r\n$4\r\nSSET\r\n$5\r\nmyset\r\n$%d\r\n%s\r\n", n, val);
            } else if (t == 4) { // SSET
                int k_len = sprintf(key, "player:%d", i);
                int v_len = sprintf(val, "%d.0", i % 100000);
                n = sprintf(batch_buf + batch_len, "*4\r\n$5\r\nSSADD\r\n$6\r\nmyzset\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", k_len, key, v_len, val);
            } else if (t == 5) { // BITMAP
                int k_len = sprintf(key, "%d", i);
                n = sprintf(batch_buf + batch_len, "*4\r\n$6\r\nBITSET\r\n$5\r\nmybmp\r\n$%d\r\n%s\r\n$1\r\n%c\r\n", k_len, key, (i % 2 == 0) ? '1' : '0');
            } else { // ARRAY, RBTREE, HASH
                int k_len = sprintf(key, "%s_%d", types[t], i);
                int v_len = sprintf(val, "%d", i);
                n = sprintf(batch_buf + batch_len, "*3\r\n$%zu\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", strlen(cmds[t]), cmds[t], k_len, key, v_len, val);
            }
            
            batch_len += n;
            current_batch_count++;

            if (batch_len > (PIPELINE_BUF_SIZE - 2048) || current_batch_count >= PIPELINE_BATCH) {
                pipeline_send_msg(fd, batch_buf, batch_len);
                verify_write_responses(fd, total_sent, current_batch_count, types[t]);
                
                total_sent += current_batch_count;
                batch_len = 0;
                current_batch_count = 0;
            }
            
            if (i % 10000 == 0) print_progress(types[t], i + 1, current_limit);
        }
        
        if (batch_len > 0) {
            pipeline_send_msg(fd, batch_buf, batch_len);
            verify_write_responses(fd, total_sent, current_batch_count, types[t]);
        }
        
        gettimeofday(&tv_end, NULL);
        time_used = TIME_SUB_MS(tv_end, tv_begin);
        printf("\n[%-8s WRITE] %d Requests | %ld ms | QPS: %ld\n\n", 
               types[t], current_limit, time_used, (current_limit * 1000L) / (time_used ? time_used : 1));
    }
    free(batch_buf);
}


int pdb_testcase_all_data_set(const char* ip, unsigned short port) {
    int fd = connect_tcpserver(ip, port);
    if (fd < 0){
        printf("connect_tcpserver error\n");
        return -1;
    } 
    testcase_write_100w(fd);
    close(fd);
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("Usage: %s <IP> <Port>\n", argv[0]);
        return -1;
    }
    const char* ip = argv[1];
    unsigned short port = atoi(argv[2]);

    return pdb_testcase_all_data_set(ip, port);
}