#include "pdb_server.h"


/**
 * Locate the first occurrence of crlf(\r\n) within the given `remaining_len`.
 * Return Pointer to the start of crlf if found; NULL otherwise.
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
    
    return NULL;
}

int pdb_split_token(char* msg, int len, char* tokens[]){
    assert(msg != NULL && len > 0 && tokens != NULL);

    int idx = 0;
    char* curr = msg;
    char* endptr = msg + len;

    if (*curr != '*'){
        // validate the RESP protocal
        pdb_log_error("protocal error\n");
        return -1;
    }

    curr++;

    char* crlf = find_crlf(curr, endptr - curr);
    if (crlf == NULL){
        pdb_log_debug("crlf == NULL\n");
        return -1;
    }
    *crlf = '\0';
    int count = atoi(curr); 
    *crlf = '\r';

    curr = crlf + 2;

    int i;
    int token_len;
    for (i = 0; i < count; i++){
        curr++;     // skip $

        crlf = find_crlf(curr, endptr-curr);
        if (crlf == NULL){
            // receive half package
            pdb_log_debug("value is too long\n");
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

static int pdb_parser_cmd(const char* cmd_str) {
    if (!cmd_str) return -1;

    switch (cmd_str[0]) {
        case 'A':
            if (strcmp(cmd_str, "ASAVE") == 0) return PDB_CMD_ASAVE;
            break;

        case 'D':
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "DEL") == 0) return PDB_CMD_DEL;
#endif
            break;

        case 'E':
            if (strcmp(cmd_str, "EXIT") == 0) return PDB_CMD_EXIT;
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "EXIST") == 0) return PDB_CMD_EXIST;
#endif
            break;

        case 'G':
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "GET") == 0) return PDB_CMD_GET;
#endif
            break;

        case 'H':
            if (strcmp(cmd_str, "HMSET") == 0) return PDB_CMD_HMSET;
            if (strcmp(cmd_str, "HMGET") == 0) return PDB_CMD_HMGET;
#if ENABLE_HASH
            if (strcmp(cmd_str, "HSET") == 0) return PDB_CMD_HSET;
            if (strcmp(cmd_str, "HGET") == 0) return PDB_CMD_HGET;
            if (strcmp(cmd_str, "HDEL") == 0) return PDB_CMD_HDEL;
            if (strcmp(cmd_str, "HMOD") == 0) return PDB_CMD_HMOD;
            if (strcmp(cmd_str, "HEXIST") == 0) return PDB_CMD_HEXIST;
#endif
            break;

        case 'M':
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "MSET") == 0) return PDB_CMD_MSET;
            if (strcmp(cmd_str, "MGET") == 0) return PDB_CMD_MGET;

            if (strcmp(cmd_str, "MOD") == 0) return PDB_CMD_MOD;
#endif
            break;

        case 'R':
            if (strcmp(cmd_str, "RMSET") == 0) return PDB_CMD_RMSET;
            if (strcmp(cmd_str, "RMGET") == 0) return PDB_CMD_RMGET;
#if ENABLE_RBTREE
            if (strcmp(cmd_str, "RSET") == 0) return PDB_CMD_RSET;
            if (strcmp(cmd_str, "RGET") == 0) return PDB_CMD_RGET;
            if (strcmp(cmd_str, "RDEL") == 0) return PDB_CMD_RDEL;
            if (strcmp(cmd_str, "RMOD") == 0) return PDB_CMD_RMOD;
            if (strcmp(cmd_str, "REXIST") == 0) return PDB_CMD_REXIST;
#endif
            break;

        case 'S':
            if (strcmp(cmd_str, "SAVE") == 0) return PDB_CMD_SAVE;
            if (strcmp(cmd_str, "SYN") == 0) return PDB_CMD_SYN;
#if ENABLE_ARRAY
            if (strcmp(cmd_str, "SET") == 0) return PDB_CMD_SET;
#endif
            break;
    }

    return -1; // can not find the command
}

int pdb_filter_protocol(char** tokens, int count, char* response){
    if (tokens == NULL || count == 0 || response == NULL){
        printf("pdb_filter_protocol: parameter error\n");
        return -1;
    }

    int cmd = pdb_parser_cmd(tokens[0]);

    int i;
    int len = 0;
    int ret = 0;
    char* key = tokens[1];
    char* value = tokens[2];

    char* value_get = NULL;

    pid_t pid;

    switch(cmd){
#if ENABLE_ARRAY
        case PDB_CMD_SET:
            ret = pdb_array_set(&global_array, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case PDB_CMD_GET:
            value_get = pdb_array_get(&global_array, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;
        case PDB_CMD_MGET:
            len = 0;
            for (i = 1; i < count; i++){
                char* key = tokens[i];
                char* val = pdb_rbtree_get(&global_rbtree, key);

                if (val == NULL) {
                    len += sprintf(response + len, "ERROR\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            break;

        case PDB_CMD_MSET:
            ret = pdb_array_mset(&global_array, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;

        case PDB_CMD_DEL:
            ret = pdb_array_del(&global_array, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_MOD:
            ret = pdb_array_mod(&global_array, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_EXIST:
            ret = pdb_array_exist(&global_array, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;
#endif
            
#ifdef ENABLE_RBTREE
        case PDB_CMD_RSET:
            ret = pdb_rbtree_set(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case PDB_CMD_RGET:
            value_get = pdb_rbtree_get(&global_rbtree, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case PDB_CMD_RMSET:
            ret = pdb_rbtree_mset(&global_rbtree, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;

        case PDB_CMD_RMGET:
            len = 0;
            for (i = 1; i < count; i++){
                char* key = tokens[i];
                char* val = pdb_rbtree_get(&global_rbtree, key);

                if (val == NULL) {
                    len += sprintf(response + len, "NO EXIST\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            
            break;

        case PDB_CMD_RDEL:
            ret = pdb_rbtree_del(&global_rbtree, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_RMOD:
            ret = pdb_rbtree_mod(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_REXIST:
            ret = pdb_rbtree_exist(&global_rbtree, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;
#endif

#if ENABLE_HASH
        case PDB_CMD_HSET:
            ret = pdb_hash_set(&global_hash, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case PDB_CMD_HGET:
            value_get = pdb_hash_get(&global_hash, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case PDB_CMD_HMGET:
            len = 0;
            for (i = 1; i < count; i++){   
                char* key = tokens[i];
                char* val = pdb_hash_get(&global_hash, key);

                if (val == NULL) {
                    len += sprintf(response + len, "NO EXIST\r\n");
                } else {
                    len += sprintf(response + len, "%s\r\n", val);
                }
            }
            break;

        case PDB_CMD_HMSET:
            ret = pdb_hash_mset(&global_hash, tokens, count - 1);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            }
            break;
            break;

        case PDB_CMD_HDEL:
            ret = pdb_hash_del(&global_hash, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_HMOD:
            ret = pdb_hash_mod(&global_hash, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_HEXIST:
            ret = pdb_hash_exist(&global_hash, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;   

#endif
        case PDB_CMD_SAVE:
            pid = fork();
            if (pid == 0){
                // child thread
                printf("save\n");
                printf("child pid: %d\n", pid);
#if ENABLE_ARRAY
                pdb_array_dump(&global_array, "array.dump");
#endif
#if ENABLE_RBTREE
                pdb_rbtree_dump(&global_rbtree, "rbtree.dump");
#endif
#if ENABLE_HASH
                pdb_hash_dump(&global_hash, "hash.dump");
#endif
                len = sprintf(response, "OK\r\n");
                
                _exit(0);
            }else if (pid > 0){
                // father thread
                len = sprintf(response, "OK\r\n");
            }else{
                perror("fork failed");
                len = sprintf(response, "SAVE failed\r\n");
            }

            break;

        case PDB_CMD_ASAVE:
        
            break;
        
#if ENABLE_SKIPTABLE
        case PDB_CMD_SKSET:
            ret = pdb_rbtree_set(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = sprintf(response, "OK\r\n");
            } else if (ret > 0){
                len = sprintf(response, "EXIST\r\n");
            }
            break;

        case PDB_CMD_SKGET:
            value_get = pdb_rbtree_get(&global_rbtree, key);
            if (value_get == NULL){
                len = sprintf(response, "NO EXIST\r\n");
            }else {
                len = sprintf(response, "%s\r\n", value_get);
            }
            break;

        case PDB_CMD_SKDEL:
            ret = pdb_rbtree_del(&global_rbtree, key);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_SKMOD:
            ret = pdb_rbtree_mod(&global_rbtree, key, value);
            if (ret < 0){
                len = sprintf(response, "ERROR\r\n");
            } else if (ret == 0){
                len = len = sprintf(response, "OK\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;

        case PDB_CMD_SKEXIST:
            ret = pdb_rbtree_exist(&global_rbtree, key);
            if (ret == 0){
                len = sprintf(response, "EXIST\r\n");
            } else{
                len = sprintf(response, "NO EXIST\r\n");
            }
            break;   

#endif
        case PDB_CMD_SYN:
            return -9999;

        case PDB_CMD_EXIT:
            return -99;
        default:
            ret = sprintf(response, "cmd error\r\n");
            break;
    }

    return len;

}


int pdb_protocol(char* msg, int length, char* out){
    assert(msg != NULL && length > 0 && out != NULL);

    char* rmsg = (char*)malloc(length);
    if (rmsg == NULL){
        pdb_log_error("malloc error\n");
    }
    memcpy(rmsg, msg, length);

    // Geting the num of tokens
    if (rmsg[0] != '*') {
        free(rmsg);
        pdb_log_debug("Protocol Error: Valilate RESP protocal\n");
        return -1;
    }
    char *crlf = strstr(rmsg, "\r\n");
    if (crlf == NULL) {
        pdb_log_debug("crlf == NULL\n");
        free(rmsg);
        return -1;
    }
    int count = atoi(rmsg + 1);
    if (count <= 0) {
        pdb_log_debug("count <= 0\n");
        free(rmsg);
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
        perror("malloc tokens failed");
        return -1;
    }
    memset(tokens, 0, sizeof(char*) * count);

    count = pdb_split_token(rmsg, length, tokens);
    if (count == -1){
        pdb_log_debug("pdb_split_token return -1\n");
        free(tokens);
        return -1;
    }

    int ret = pdb_filter_protocol(tokens, count, out);

    if (count >= 1024){
        free(tokens);
    }

    free(rmsg);
    return ret;
}


int pdb_response_handler(char* rmsg, int length, char* out){
    printf("rmgs: %lu, length: %d, out: %lu", strlen(rmsg), length, strlen(out));
    memcpy(out, rmsg, length);

    return strlen(out);
}

void dest_pdb_engine(void){
#if ENABLE_ARRAY
    // pdb_array_dump(&global_array, "array.dump");
    pdb_array_destroy(&global_array);
    printf("array suucess\n");
#endif

#if ENABLE_RBTREE
    // pdb_rbtree_dump(&global_rbtree, "rbtree.dump");
    pdb_rbtree_destroy(&global_rbtree);
    printf("rbtree suucess\n");
#endif

#if ENABLE_HASH
    // pdb_hash_dump(&global_hash, "hash.dump");
    pdb_hash_destory(&global_hash);
#endif

#if ENABLE_MEMPOOL
    pdb_mem_destroy();
#endif

}


int init_pdb_engine(){
#if ENABLE_MEMPOOL
    pdb_mem_init(1024 * 512);
#endif

#if ENABLE_ARRAY
    pdb_array_create(&global_array);
    pdb_array_load(&global_array, global_conf.array_dump_dir);
#endif

#if ENABLE_RBTREE
    pdb_rbtree_create(&global_rbtree);
    pdb_rbtree_load(&global_rbtree, global_conf.rbtree_dump_dir);
#endif

#if ENABLE_HASH
    pdb_hash_create(&global_hash);
    pdb_hash_load(&global_hash, global_conf.hash_dump_dir);
#endif

    return 0;
}


int main(int argc, char* argv[]){
    printf("############## Hello PDB #############\n");

    loadServerConfig("/home/dai/PatronusDB/PatronusDB.conf");
    init_pdb_engine();

    int port = global_conf.port;
    int mode = global_conf.network_mode;

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
        int ret = reactor_entry(port, pdb_protocol, pdb_response_handler);
        printf("ret: %d\n", ret);
    }else if (mode == 2){
        printf("newtork mode: %s\n", "ntyco");
        ntyco_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 3){
        printf("newtork mode: %s\n", "io_uring");
        uring_entry(port, pdb_protocol, pdb_response_handler);
    }

    dest_pdb_engine();
    
    return 0;
}