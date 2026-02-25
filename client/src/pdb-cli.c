#include <readline/readline.h>
#include <readline/history.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

void handle_command(int fd, char *line);
void interactive_mode(int fd);
int tokenize(char *line, char **argv);
void send_request(int fd, int argc, char **argv);


void handle_command(int fd, char *line) {
    char *argv[10];
    int argc = tokenize(line, argv);
    if (argc == 0) return;

    send_request(fd, argc, argv);

    char response[4096];
    int n = recv(fd, response, sizeof(response) - 1, 0);
    if (n > 0) {
        response[n] = '\0';
        printf("%s", response); 
    } else if (n == 0) {
        printf("Error: Server closed connection.\n");
        exit(0);
    }
}

void interactive_mode(int fd) {
    char *line;
    while ((line = readline("pdb-cli> ")) != NULL) {
        if (*line) {
            add_history(line); 
            handle_command(fd, line);
        }
        free(line);
    }
}

int tokenize(char *line, char **argv) {
    int argc = 0;
    char *token = strtok(line, " ");
    while (token && argc < 10) {
        argv[argc++] = token;
        token = strtok(NULL, " ");
    }
    return argc;
}

void send_request(int fd, int argc, char **argv) {
    char buf[4096];
    int len = 0;
    len += sprintf(buf + len, "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        len += sprintf(buf + len, "$%lu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }
    send(fd, buf, len, 0);
}




// ./pdb-cli <ip> <port>
int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <host> <port>\n", argv[0]);
        return -1;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket error");
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        printf("Invalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection Failed");
        return -1;
    }

    printf("Connected to PatronusDB. Welcome!\n");
    printf("$pdb-cli\n");

    interactive_mode(sockfd);

    close(sockfd);

    return 0;
}