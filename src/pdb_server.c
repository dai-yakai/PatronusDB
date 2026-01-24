#include "pdb_server.h"


/**
 * @brief 查找下一个\r\n的位置
 * 
 * @param start 传入数组的起始指针
 * @param remaining_len 待搜索数组的长度
 * 
 * @return 找到，返回指向\r\n的指针；未找到，返回NULL
 */
static char* find_crlf(char* start, int remaining_len){
    char* p;
    p = start;
    while (remaining_len >= 2){
        if ((*p) == '\r' && *(p + 1) == '\n'){
            return p;
        }
        p++;
        remaining_len--;
    }
    
    // 未找到返回NULL
    return NULL;
}

int kvs_split_token(char* msg, int len, char* tokens[]){
    if (msg == NULL || tokens == NULL || len < 0){
        printf("kvs_split_token: parameter error\n");
        return -1;
    }

    int idx = 0;
    char* curr = msg;
    char* endptr = msg + len;

    if (*curr != '*'){
        // 不符合协议格式
        printf("kvs_split_token: protocal error\n");
        return -1;
    }

    curr++;

    // 找到共有多少个token（SET key value，count = 3）
    char* crlf = find_crlf(curr, endptr - curr);
    if (crlf == NULL){
        printf("kvs_split_token: crlf == NULL\n");
        return -1;
    }
    *crlf = '\0';
    int count = atoi(curr); // 
    *crlf = '\r';

    curr = crlf + 2;

    int i;
    int token_len;
    for (i = 0; i < count; i++){
        curr++;     //跳过$

        crlf = find_crlf(curr, endptr-curr);
        if (crlf == NULL){
            // 信息没收全
            printf("value is too long\n");
            return -1;
        }
        *crlf = '\0';
        token_len = atoi(curr);
        *crlf = '\r';

        curr = crlf + 2;
        tokens[idx++] = curr;
        curr[token_len] = '\0';

        curr += token_len + 2;
    }

    return count;
}

static int kvs_parser_cmd(const char* cmd_str) {
    if (!cmd_str) return -1;

    // 利用首字母进行第一轮 O(1) 过滤
    switch (cmd_str[0]) {
        case 'A':
            if (strcmp(cmd_str, "ASAVE") == 0) return KVS_CMD_ASAVE;
            break;

        case 'D':
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "DEL") == 0) return KVS_CMD_DEL;
#endif
            break;

        case 'E':
            if (strcmp(cmd_str, "EXIT") == 0) return KVS_CMD_EXIT;
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "EXIST") == 0) return KVS_CMD_EXIST;
#endif
            break;

        case 'G':
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "GET") == 0) return KVS_CMD_GET;
#endif
            break;

        case 'H':
            // 按照你提供的数组，HMSET/HMGET 是无条件开启的
            if (strcmp(cmd_str, "HMSET") == 0) return KVS_CMD_HMSET;
            if (strcmp(cmd_str, "HMGET") == 0) return KVS_CMD_HMGET;
#if ENABLE_HASH
            if (strcmp(cmd_str, "HSET") == 0) return KVS_CMD_HSET;
            if (strcmp(cmd_str, "HGET") == 0) return KVS_CMD_HGET;
            if (strcmp(cmd_str, "HDEL") == 0) return KVS_CMD_HDEL;
            if (strcmp(cmd_str, "HMOD") == 0) return KVS_CMD_HMOD;
            if (strcmp(cmd_str, "HEXIST") == 0) return KVS_CMD_HEXIST;
#endif
            break;

        case 'M':
            // MSET/MGET 是无条件开启的
            if (strcmp(cmd_str, "MSET") == 0) return KVS_CMD_MSET;
            if (strcmp(cmd_str, "MGET") == 0) return KVS_CMD_MGET;
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "MOD") == 0) return KVS_CMD_MOD;
#endif
            break;

        case 'R':
            // 按照你提供的数组，RMSET/RMGET 是无条件开启的
            if (strcmp(cmd_str, "RMSET") == 0) return KVS_CMD_RMSET;
            if (strcmp(cmd_str, "RMGET") == 0) return KVS_CMD_RMGET;
#if ENABLE_RBTREE
            if (strcmp(cmd_str, "RSET") == 0) return KVS_CMD_RSET;
            if (strcmp(cmd_str, "RGET") == 0) return KVS_CMD_RGET;
            if (strcmp(cmd_str, "RDEL") == 0) return KVS_CMD_RDEL;
            if (strcmp(cmd_str, "RMOD") == 0) return KVS_CMD_RMOD;
            if (strcmp(cmd_str, "REXIST") == 0) return KVS_CMD_REXIST;
#endif
            break;

        case 'S':
            if (strcmp(cmd_str, "SAVE") == 0) return KVS_CMD_SAVE;
            if (strcmp(cmd_str, "SYN") == 0) return KVS_CMD_SYN;
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "SET") == 0) return KVS_CMD_SET;
#endif
            break;
    }

    return -1; // 未找到命令
}

int kvs_filter_protocol(char** tokens, int count, char* response){
    if (tokens == NULL || count == 0 || response == NULL){
        printf("kvs_filter_protocol: parameter error\n");
        return -1;
    }

    // int cmd = KVS_CMD_START;
    // for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++){
    //     if (strcmp(tokens[0], command[cmd]) == 0){
    //         break;
    //     }
    // }

    int cmd = kvs_parser_cmd(tokens[0]);
#if ENABLE_PRINT_KV
    printf("token: %s\n, cmd: %s\n", tokens[0], command[cmd]);
#endif
    int i;
    int len = 0;
    int ret = 0;
    char* key = tokens[1];
    char* value = tokens[2];

    char* value_get = NULL;

    pid_t pid;

    switch(cmd){
#ifdef ENABLE_ARRAY
        case KVS_CMD_SET:
            ret = kvs_array_set(&global_array, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case KVS_CMD_GET:
            value_get = kvs_array_get(&global_array, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;
        case KVS_CMD_MGET:
            len = 0;
            for (i = 1; i < count; i++){
                char* key = tokens[i];
                char* val = kvs_rbtree_get(&global_rbtree, key);

                if (val == NULL) {
                    len += sprintf(response + len, "ERROR\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            break;

        case KVS_CMD_MSET:
            ret = kvs_array_mset(&global_array, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;

        case KVS_CMD_DEL:
            ret = kvs_array_del(&global_array, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_MOD:
            ret = kvs_array_mod(&global_array, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_EXIST:
            ret = kvs_array_exist(&global_array, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;
#endif
            
#ifdef ENABLE_RBTREE
        case KVS_CMD_RSET:
            ret = kvs_rbtree_set(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case KVS_CMD_RGET:
            value_get = kvs_rbtree_get(&global_rbtree, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case KVS_CMD_RMSET:
            ret = kvs_rbtree_mset(&global_rbtree, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;

        case KVS_CMD_RMGET:
            len = 0;
            for (i = 1; i < count; i++){
                char* key = tokens[i];
                char* val = kvs_rbtree_get(&global_rbtree, key);

                if (val == NULL) {
                    len += sprintf(response + len, "NO EXIST\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            
            break;

        case KVS_CMD_RDEL:
            ret = kvs_rbtree_del(&global_rbtree, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_RMOD:
            ret = kvs_rbtree_mod(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_REXIST:
            ret = kvs_rbtree_exist(&global_rbtree, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;
#endif

#if ENABLE_HASH
        case KVS_CMD_HSET:
            ret = kvs_hash_set(&global_hash, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case KVS_CMD_HGET:
            value_get = kvs_hash_get(&global_hash, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case KVS_CMD_HMGET:
            len = 0;
            for (i = 1; i < count; i++){   
                char* key = tokens[i];
                char* val = kvs_hash_get(&global_hash, key);
                // printf("key: %s\n", key);
                if (val == NULL) {
                    len += sprintf(response + len, "NO EXIST\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            break;

        case KVS_CMD_HMSET:
            ret = kvs_hash_mset(&global_hash, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;
            break;

        case KVS_CMD_HDEL:
            ret = kvs_hash_del(&global_hash, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_HMOD:
            ret = kvs_hash_mod(&global_hash, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_HEXIST:
            ret = kvs_hash_exist(&global_hash, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;   

#endif
        case KVS_CMD_SAVE:
            pid = fork();
            if (pid == 0){
                // 子进程
                printf("save\n");
                printf("child pid: %d\n", pid);
#if ENABLE_ARRAY
                kvs_array_dump(&global_array, "array.dump");
#endif
#if ENABLE_RBTREE
                kvs_rbtree_dump(&global_rbtree, "rbtree.dump");
#endif
#if ENABLE_HASH
                kvs_hash_dump(&global_hash, "hash.dump");
#endif
                len = sprintf(response, "OK\r\n");
                
                _exit(0);
            }else if (pid > 0){
                // 父进程
                len = sprintf(response, "OK\r\n");
            }else{
                perror("fork failed");
                len = sprintf(response, "SAVE failed\r\n");
            }

            break;

        case KVS_CMD_ASAVE:
        
            break;
        
#if ENABLE_SKIPTABLE
        case KVS_CMD_SKSET:
            ret = kvs_rbtree_set(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case KVS_CMD_SKGET:
            value_get = kvs_rbtree_get(&global_rbtree, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case KVS_CMD_SKDEL:
            ret = kvs_rbtree_del(&global_rbtree, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_SKMOD:
            ret = kvs_rbtree_mod(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case KVS_CMD_SKEXIST:
            ret = kvs_rbtree_exist(&global_rbtree, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;   

#endif
        case KVS_CMD_SYN:
            return -9999;

        case KVS_CMD_EXIT:
            return -99;
        default:
            ret = sprintf(response, "cmd error\r\n");
            break;
    }

    return len;

}


int kvs_protocol(char* rmsg, int length, char* out){
    if (rmsg == NULL || length < 0 || out == NULL){
        printf("kvs_protocol: parameter error\n");
        return -1;
    }

    // 获取token的数量
    if (rmsg[0] != '*') {
        printf("Protocol Error: Not a RESP Array\n");
        return -1;
    }
    char *crlf = strstr(rmsg, "\r\n");
    if (crlf == NULL) {
        printf("kvs_protocol: crlf == NULL\n");
        return -1;
    }
    int count = atoi(rmsg + 1);
    if (count <= 0) {
        printf("count <= 0\n");
        return -1;
    }
    char** tokens;
    if (count < 1024){
        char* p_tokens[1024];
        tokens = p_tokens;
    }else{
        tokens = (char**)malloc(sizeof(char*) * count);  
    }

    if (tokens == NULL) {
        perror("kvs_protocol: malloc tokens failed");
        return -1;
    }
    memset(tokens, 0, sizeof(char*) * count);
    // char* tokens[KVS_MAX_TOKENS] = {0};

    count = kvs_split_token(rmsg, length, tokens);
    if (count == -1){
        printf("kvs_protocol: kvs_split_token return -1\n");
        free(tokens);
        return -1;
    }

    int ret = kvs_filter_protocol(tokens, count, out);

    if (count >= 1024){
        free(tokens);
    }
    
    return ret;
}


int kvs_response_handler(char* rmsg, int length, char* out){
    printf("rmgs: %lu, length: %d, out: %lu", strlen(rmsg), length, strlen(out));
    memcpy(out, rmsg, length);

    return strlen(out);
}

void dest_kvengine(void){
#if ENABLE_ARRAY
    // kvs_array_dump(&global_array, "array.dump");
    kvs_array_destroy(&global_array);
    printf("array suucess\n");
#endif

#if ENABLE_RBTREE
    // kvs_rbtree_dump(&global_rbtree, "rbtree.dump");
    kvs_rbtree_destroy(&global_rbtree);
    printf("rbtree suucess\n");
#endif

#if ENABLE_HASH
    // kvs_hash_dump(&global_hash, "hash.dump");
    kvs_hash_destory(&global_hash);
#endif

#if ENABLE_THREADPOOL
    destroyPool(&g_kv_pool);
#endif

#if ENABLE_MEMPOOL
    kvs_mem_destroy();
#endif

}


int init_kvengine(void){
#if ENABLE_THREADPOOL
    createThreadPool(&g_kv_pool, KV_WORKER_NUM);
#endif

#if ENABLE_MEMPOOL
    kvs_mem_init(1024 * 512);
#endif

#if ENABLE_ARRAY
    kvs_array_create(&global_array);
    kvs_array_load(&global_array, "array.dump");
#endif

#if ENABLE_RBTREE
    kvs_rbtree_create(&global_rbtree);
    kvs_rbtree_load(&global_rbtree, "rbtree.dump");
#endif

#if ENABLE_HASH
    kvs_hash_create(&global_hash);
    kvs_hash_load(&global_hash, "hash.dump");
#endif

    return 0;
}


// port, kvs_protocal
// ./kvstore 9001 3          (Master)
// ./kvstore 9002 3 127.0.0.1 9001 (Slave)
int main(int argc, char* argv[]){
    printf("############## Hello kv store #############\n");
    if (argc != 3 && argc != 5){
        return -1;
    }

    // int port = atoi(argv[1]);
    // int mode = atoi(argv[2]);

    int port = global_conf.port;
    int mode = global_conf.network_mode;
    
    init_kvengine();
    loadServerConfig("../PatronusDB.conf");
    // printf("load dump success\n");

    if (global_conf.is_replication){
        // 为slave节点
        // char* master_ip = argv[3];
        // unsigned short master_port = atoi(argv[4]);
        char* master_ip = global_conf.master_ip;
        int master_port = global_conf.master_port;
        global_replication.master_ip = master_ip;
        global_replication.master_port = master_port;

        global_replication.is_master = 0;
        int fd = connect_master(master_ip, master_port);
        if (fd > 0){
            global_replication.slave_to_master_fd = fd;
        }else{
            printf("slave connect to master failed\n");
        }

        // 从节点主动发送SYN: "*1\r\n$4\r\nSYNC\r\n"
        char* msg = "*1\r\n$3\r\nSYN\r\n";
        int ret = send(fd, msg, strlen(msg), 0);
        if (ret < 0){
            printf("slave send SYN failed\n");
        }
        
    }else{
        // 为master节点
        global_replication.is_master = 1;
    }

    if (mode == 1){
        printf("newtork mode: %s\n", "reactor");
        reactor_entry(port, kvs_protocol, kvs_response_handler);
    }else if (mode == 2){
        printf("newtork mode: %s\n", "ntyco");
        ntyco_entry(port, kvs_protocol, kvs_response_handler);
    }else if (mode == 3){
        printf("newtork mode: %s\n", "io_uring");
        uring_entry(port, kvs_protocol, kvs_response_handler);
    }

    dest_kvengine();
    
    return 0;
}