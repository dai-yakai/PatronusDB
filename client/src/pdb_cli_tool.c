#include "pdb_cli_tool.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>

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

int process_response(){
    
}