#include <readline/readline.h>
#include <readline/history.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <strings.h>
#include <signal.h>

#define CLI_CMD_NUM     42

extern int pdb_testcase_all_data_get(const char* ip, unsigned short port);
extern int pdb_testcase_all_data_set(const char* ip, unsigned short port);
extern void pdb_testcase_bitmap(const char* ip, unsigned short port);
extern int pdb_testcase_blog(char* ip, int port);
extern int pdb_testcase_mget(char* ip, int port);
extern int pdb_testcase_mset(char* ip, int port);
extern int pdb_testcase_pipeline_get(char* ip, int port, char* expect_result);
extern int pdb_testcase_pipeline_set(char* ip, int port);
extern void pdb_testcase_set(const char* ip, unsigned short port);
extern void pdb_testcase_sortedset(const char* ip, unsigned short port);

char* ip = NULL;
unsigned short port = 0;
int sockfd = -1;
volatile sig_atomic_t g_test_stop = 0; 
int g_in_test = 0;                     

const char* command[] = {
    // inner cmd(3)
    "quit", "show", "exit",

    // test cmd(10)
    "T-MSET", "T-MGET", "T-PIPELINE-SET", "T-PIPELINE-GET", "T-BLOG", "T-BITMAP", "T-SET", "T-SSET",
    "T-ALL_DATA_GET", "T-ALL_DATA_SET",

    // array(7)
    "SET", "GET", "DEL", "MOD", "EXIST", "MSET", "MGET",

    // Rbtree(7)
    "RSET", "RGET", "RDEL", "RMOD", "REXIST", "RMSET", "RMGET",

    // Hash(7)
    "HSET", "HGET", "HDEL", "HMOD", "HEXIST", "HMSET", "HMGET",

    // Bitmap(5)
    "BITSET", "BITGET", "BITCOUNT", "BITPOS", "BITOP",
    // 3
    "SAVE", "NSAVE", "SYN",
    NULL
};

void sigint_handler(int signo) {
    if (signo == SIGINT) {
        if (g_in_test) {
            g_test_stop = 1;
        } else {
            printf("\n"); 
            rl_on_new_line();    
            rl_replace_line("", 0);     
            rl_redisplay();
        }
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

void send_request(int argc, char **argv) {
    char buf[4096];
    int len = 0;
    len += sprintf(buf + len, "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        len += sprintf(buf + len, "$%lu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }
    ssize_t nsend = send(sockfd, buf, len, 0);
}

char *pdb_command_generator(const char *text, int state) {
    static int list_index, len;
    const char *name;

    if (!state) {
        list_index = 0;
        len = strlen(text);
    }

    while ((name = command[list_index++])) {
        if (strncasecmp(name, text, len) == 0) {
            return strdup(name);
        }
    }

    return NULL;
}

char **pdb_completion(const char *text, int start, int end) {
    char **matches = NULL;

    if (start == 0) {
        matches = rl_completion_matches(text, pdb_command_generator);
    }

    rl_attempted_completion_over = 1;

    return matches;
}


void pdb_cli_show_all_cmd(){
    int i;
    for (i = 0; i < CLI_CMD_NUM; i++){
        printf("%s, ", command[i]);
    }
    printf("\n");
}


int pdb_connect_to_server(){
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket error");
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        printf("Invalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        // perror("Connection Failed");
        return -1;
    }

    return sockfd;
}

int pdb_parser_cmd(int argc, char **argv) {
    char* cmd_str = argv[0];

    if (!strcmp(cmd_str, "T-MSET")) {
        pdb_testcase_mset(ip, port);
        return 0;
    }             
    if (!strcmp(cmd_str, "T-MGET")) {
        pdb_testcase_mget(ip, port);
        return 0;
    }            
    if (!strcmp(cmd_str, "T-PIPELINE-SET")) {
        pdb_testcase_pipeline_set(ip, port);
        return 0;
    }     
    if (!strcmp(cmd_str, "T-PIPELINE-GET")) {
        char* expect_result = "EXIST";
        for (int i = 1; i < argc; i++) {
            if (strncmp(argv[i], "-result=", 8) == 0) {
                expect_result = argv[i] + 8; 
                break;
            }
        }
        pdb_testcase_pipeline_get(ip, port, expect_result);
        return 0;
    }      
    if (!strcmp(cmd_str, "T-BLOG")) {
        pdb_testcase_blog(ip, port);
        return 0;
    } 
    if (!strcasecmp(cmd_str, "show")){
        pdb_cli_show_all_cmd();
        return 0;
    }       
    if (!strcasecmp(cmd_str, "T-BITMAP")){
        pdb_testcase_bitmap(ip, port);
        return 0;
    }
    if (!strcasecmp(cmd_str, "T-SET")){
        pdb_testcase_set(ip, port);
        return 0;
    }
    if (!strcasecmp(cmd_str, "T-SSET")){
        pdb_testcase_sortedset(ip, port);
        return 0;
    }
    if (!strcasecmp(cmd_str, "T-ALL_DATA_GET")){
        pdb_testcase_all_data_get(ip, port);
        return 0;
    }

    if (!strcasecmp(cmd_str, "T-ALL_DATA_SET")){
        pdb_testcase_all_data_set(ip, port);
        return 0;
    }

    

    return -1; // can not find the command
}

void handle_command(char *line) {
    char *argv[10];
    int argc = tokenize(line, argv);
    if (argc == 0) return;

    if (*line == 'q'){
        printf("\n");
        printf("========================\n");
        printf("   \033[1;32mBYE BYE ~~~\033[0m   \n"); 
        printf("========================\n");
        printf("\n");
        exit(0);
    }
    int ret = pdb_parser_cmd(argc, argv);
    if (ret == -1){
        // The cmd is not in cli-cmd.
        send_request(argc, argv);
        char response[4096];
        int n = recv(sockfd, response, sizeof(response) - 1, 0);
        if (n > 0) {
            response[n] = '\0';
            printf("%s", response); 
        } else if (n == 0) {
            // disconnect
            while(1){
                printf("Server disconnect, Reconnecting......\n");
                sockfd = pdb_connect_to_server();
                if (sockfd > 0){
                    printf("Reconnect successfully\n");
                    break;
                }
                sleep(3);
            }
        }
    }
}

void interactive_mode() {
    char* line;
    char buf[64];
    sprintf(buf, "%s:%d> ", ip, port);
    while ((line = readline(buf)) != NULL) {
        if (*line) {
            add_history(line); 
            handle_command(line);
        }
        free(line);
    }
}




// ./pdb-cli <ip> <port>
int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <host> <port>\n", argv[0]);
        return -1;
    }
    ip = argv[1];
    port = atoi(argv[2]);

    printf("\n");
    printf("========================\n");
    printf("\033[1;36mWelcome PDB-client!\033[0m\n");
    printf("========================\n");
    printf("\n");

    sockfd = pdb_connect_to_server();
    if (sockfd < 0){
        // Connect failed
        while(sockfd < 0){
            printf("Server down. Try to reconnecting......\n");
            sockfd = pdb_connect_to_server();
            sleep(5);
        }
    }
    signal(SIGINT, sigint_handler);
    rl_attempted_completion_function = pdb_completion;

    interactive_mode(sockfd);
    close(sockfd);

    return 0;
}