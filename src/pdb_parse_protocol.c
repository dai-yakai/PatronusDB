#include "pdb_parse_protocol.h"

pdb_rdma_conn_ctx* slave_conn;
uint64_t remote_vaddr;
uint32_t remote_rkey;
size_t   pull_size;

pdb_rdma_snapshot_ctx* incre_slave_snap;
pdb_rdma_snapshot_ctx* incre_master_snap;
pdb_rdma_conn_ctx* incre_slave_conn;
pdb_rdma_conn_ctx* incre_master_conn;

int is_incre_ready = 0;

long long get_now_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
long long last_pull_time_ms = 0;
int is_incre_channel_active = 0;

const char* command[] = {
    // array
    "SET", "GET", "DEL", "MOD", "EXIST", "MSET", "MGET", 
    // rbtree
    "RSET", "RGET", "RDEL", "RMOD", "REXIST", "RMSET", "RMGET", 
    // hash
    "HSET", "HGET", "HDEL", "HMOD", "HEXIST", "HMSET", "HMGET",
    // bitmap
    "BITSET", "BITGET", "BITCOUNT", "BITPOS", "BITOP",
    // set
    "SSET", "SDEL", "SEXIST", "SCARD", "SRANDOMPOP", "SNRANDOMPOP", "SINTER", "SUNION", "SDIFFER"
    // sortedSet
    "SSADD", "SSCORE", "SSINCRBY", "SSRANK", "SSRANGE"
    
    "EXIT", "SAVE", "NSAVE", 
    // syn
    "ZSYN", "ZRDMA_READY", "ZSYN_OOB", "ZOOB_ACK",
    // mem
    "GETUSEDMEM", "GETUSEDMEMRSS",
    "PING"
};


enum{
    PDB_CMD_START = 0,

    // array
    PDB_CMD_SET = PDB_CMD_START,    //0
    PDB_CMD_GET,                    //1
    PDB_CMD_DEL,                    //2
    PDB_CMD_MOD,                    //3
    PDB_CMD_EXIST,                  //4
    PDB_CMD_MSET,
    PDB_CMD_MGET,

    // rbtree
    PDB_CMD_RSET,                   //5
    PDB_CMD_RGET,                   //6
    PDB_CMD_RDEL,                   //7
    PDB_CMD_RMOD,                   //8
    PDB_CMD_REXIST,                 //9
    PDB_CMD_RMSET,
    PDB_CMD_RMGET,

    // hash
    PDB_CMD_HSET,                   //10
    PDB_CMD_HGET,                   //11
    PDB_CMD_HDEL,                   //12
    PDB_CMD_HMOD,                   //13
    PDB_CMD_HEXIST,                 //14
    PDB_CMD_HMSET,
    PDB_CMD_HMGET,

    // skiptable
    PDB_CMD_SKSET,
    PDB_CMD_SKGET,
    PDB_CMD_SKDEL,
    PDB_CMD_SKMOD,
    PDB_CMD_SKEXIST,
    PDB_CMD_SKMSET,
    PDB_CMD_SKMGET,

    // bitmap
    PDB_CMD_BITMAP_GET,
    PDB_CMD_BITMAP_SET,
    PDB_CMD_BITMAP_COUNT,
    PDB_CMD_BITMAP_POS,
    PDB_CMD_BITMAP_OP,

    // set
    PDB_CMD_SET_SET,
    PDB_CMD_SET_DEL,
    PDB_CMD_SET_CARD,
    PDB_CMD_SET_EXIST,
    PDB_CMD_SET_RANDOMPOP,
    PDB_CMD_SET_NRANDOMPOP,
    PDB_CMD_SET_INTER,
    PDB_CMD_SET_UNION,
    PDB_CMD_SET_DIFFER,

    // sorted set
    PDB_CMD_SSET_ADD,
    PDB_CMD_SSET_SCORE,
    PDB_CMD_SSET_INCRBY,
    PDB_CMD_SSET_RANK,
    PDB_CMD_SSET_RANGE,

    PDB_CMD_EXIT,                   
    PDB_CMD_SAVE,
    PDB_CMD_NSAVE,

    PDB_CMD_SYN,
    PDB_CMD_SYNC_ACK,
    PDB_CMD_SYNC_OOB,
    PDB_CMD_SLAVE_REQUIRE,
    PDB_CMD_SLAVE_OOB,
    PDB_CMD_ZRDB_OK,

    PDB_CMD_INCRE_SYN,
    PDB_CMD_INCRE_ACK,

    // mem
    PDB_CMD_MEM_USED,
    PDB_CMD_MEM_RSS,

    PDB_CMD_PING,



    PDB_CMD_COUNT
};


static void _gid_to_str(uint8_t *gid, char *str) {
    for(int i = 0; i < 16; i++) sprintf(str + i * 2, "%02x", gid[i]);
    str[32] = '\0';
}

static void _str_to_gid(const char *str, uint8_t *gid) {
    for(int i = 0; i < 16; i++) sscanf(str + i * 2, "%2hhx", &gid[i]);
}


/**
 * Locate the first occurrence of crlf(\r\n) within the given `remaining_len`.
 * Return Pointer to the start of crlf if found; NULL otherwise.
 */
char* find_crlf(char* start, int remaining_len){
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

/**
 * Parse msg and return the num of tokens.
 */
int pdb_split_token(char* msg, int len, char* tokens[]){
    assert(msg != NULL && len > 0 && tokens != NULL);

    int idx = 0;
    char* curr = msg;
    char* endptr = curr + len;

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
            pdb_log_debug("receive half package\n");
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

int pdb_parser_cmd(const char* cmd_str) {
    if (!cmd_str) return -1;

    switch (cmd_str[0]) {
        case 'D':
            if (strcmp(cmd_str, "DEL") == 0)    return PDB_CMD_DEL;
            break;

        case 'E':
            if (strcmp(cmd_str, "EXIT") == 0)   return PDB_CMD_EXIT;
            if (strcmp(cmd_str, "EXIST") == 0)  return PDB_CMD_EXIST;
            break;

        case 'G':
            if (strcmp(cmd_str, "GET") == 0)    return PDB_CMD_GET;
            if (strcmp(cmd_str, "GETUSEDMEM") == 0)         return PDB_CMD_MEM_USED;
            if (strcmp(cmd_str, "GETUSEDMEMRSS") == 0)      return PDB_CMD_MEM_RSS;

            break;

        case 'H':
            if (strcmp(cmd_str, "HMSET") == 0)  return PDB_CMD_HMSET;
            if (strcmp(cmd_str, "HMGET") == 0)  return PDB_CMD_HMGET;
            if (strcmp(cmd_str, "HSET") == 0)   return PDB_CMD_HSET;
            if (strcmp(cmd_str, "HGET") == 0)   return PDB_CMD_HGET;
            if (strcmp(cmd_str, "HDEL") == 0)   return PDB_CMD_HDEL;
            if (strcmp(cmd_str, "HMOD") == 0)   return PDB_CMD_HMOD;
            if (strcmp(cmd_str, "HEXIST") == 0) return PDB_CMD_HEXIST;
            break;

        case 'M':
            if (strcmp(cmd_str, "MSET") == 0)   return PDB_CMD_MSET;
            if (strcmp(cmd_str, "MGET") == 0)   return PDB_CMD_MGET;
            if (strcmp(cmd_str, "MOD") == 0)    return PDB_CMD_MOD;
            break;

        case 'R':
            if (strcmp(cmd_str, "RMSET") == 0)  return PDB_CMD_RMSET;
            if (strcmp(cmd_str, "RMGET") == 0)  return PDB_CMD_RMGET;
            if (strcmp(cmd_str, "RSET") == 0)   return PDB_CMD_RSET;
            if (strcmp(cmd_str, "RGET") == 0)   return PDB_CMD_RGET;
            if (strcmp(cmd_str, "RDEL") == 0)   return PDB_CMD_RDEL;
            if (strcmp(cmd_str, "RMOD") == 0)   return PDB_CMD_RMOD;
            if (strcmp(cmd_str, "REXIST") == 0) return PDB_CMD_REXIST;
            break;

        case 'S':
            if (strcmp(cmd_str, "SAVE") == 0)       return PDB_CMD_SAVE;
            if (strcmp(cmd_str, "SET") == 0)        return PDB_CMD_SET;

            if (strcmp(cmd_str, "SSET") == 0)        return PDB_CMD_SET_SET;
            if (strcmp(cmd_str, "SDEL") == 0)        return PDB_CMD_SET_DEL;
            if (strcmp(cmd_str, "SEXIST") == 0)      return PDB_CMD_SET_EXIST;
            if (strcmp(cmd_str, "SCARD") == 0)       return PDB_CMD_SET_CARD;
            if (strcmp(cmd_str, "SRANDOMPOP") == 0)  return PDB_CMD_SET_RANDOMPOP;
            if (strcmp(cmd_str, "SNRANDOMPOP") == 0) return PDB_CMD_SET_NRANDOMPOP;
            if (strcmp(cmd_str, "SINTER") == 0)      return PDB_CMD_SET_INTER;
            if (strcmp(cmd_str, "SUNION") == 0)      return PDB_CMD_SET_UNION;
            if (strcmp(cmd_str, "SDIFFER") == 0)     return PDB_CMD_SET_DIFFER;

            if (strcmp(cmd_str, "SSADD") == 0)       return PDB_CMD_SSET_ADD;
            if (strcmp(cmd_str, "SSCORE") == 0)      return PDB_CMD_SSET_SCORE;
            if (strcmp(cmd_str, "SSINCRBY") == 0)    return PDB_CMD_SSET_INCRBY;
            if (strcmp(cmd_str, "SSRANK") == 0)      return PDB_CMD_SSET_RANK;
            if (strcmp(cmd_str, "SSRANGE") == 0)     return PDB_CMD_SSET_RANGE;

            break;

        case 'N':
            if (strcmp(cmd_str, "NSAVE") == 0)  return PDB_CMD_NSAVE;

        case 'B':
            if (strcmp(cmd_str, "BITSET") == 0)      return PDB_CMD_BITMAP_SET;
            if (strcmp(cmd_str, "BITGET") == 0)      return PDB_CMD_BITMAP_GET;
            if (strcmp(cmd_str, "BITCOUNT") == 0)    return PDB_CMD_BITMAP_COUNT;
            if (strcmp(cmd_str, "BITOP") == 0)       return PDB_CMD_BITMAP_OP;
            if (strcmp(cmd_str, "BITPOS") == 0)      return PDB_CMD_BITMAP_POS;
            break;

        case 'P':
            if (strcmp(cmd_str, "PING") == 0)        return PDB_CMD_PING;
            break;

        case 'Z':
            if (strcmp(cmd_str, "ZSYN") == 0)           return PDB_CMD_SYN;
            if (strcmp(cmd_str, "ZSYN_OOB") == 0)       return PDB_CMD_SYNC_OOB;
            if (strcmp(cmd_str, "ZSYN_ACK") == 0)       return PDB_CMD_SYNC_ACK;
            if (strcmp(cmd_str, "ZOOB_ACK") == 0)       return PDB_CMD_SLAVE_REQUIRE;
            if (strcmp(cmd_str, "ZRDMA_READY") == 0)    return PDB_CMD_SLAVE_OOB;
            if (strcmp(cmd_str, "ZRDB_OK") == 0)        return PDB_CMD_ZRDB_OK;
            if (strcmp(cmd_str, "ZINCRE_SYN") == 0)     return PDB_CMD_INCRE_SYN;
            if (strcmp(cmd_str, "ZINCRE_ACK") == 0)     return PDB_CMD_INCRE_ACK; 
    }
    return PDB_ERROR; // can not find the command
}

static inline int parse_resp_int(const char **p, const char *end) {
    int val = 0;
    const char *curr = *p;
    
    while (curr < end && *curr >= '0' && *curr <= '9') {
        val = val * 10 + (*curr - '0');
        curr++;
    }
    
    *p = curr;
    return val;
}

/**
 * Check for a complete package in 'buf' and return its total length.
 * 
 * Return PDB_PROTOCAL_ERROR if `buf` violates the RESP.
 * Return PDB_HALF_PACKAGE if `buf` contains a incomplete package(a half package).
 * Return total length if `buf` contains a valid and complete package.
 */
int check_resp_integrity(const char *buf, int len, int* bulk_length) {
	if (len < 4) return PDB_HALF_PACKAGE; 
    if (buf[0] != '*') return PDB_PROTOCAL_ERROR;

    const char *curr = buf + 1; 
    const char *end = buf + len;

    int count = parse_resp_int(&curr, end);

    if (curr + 1 >= end) return PDB_HALF_PACKAGE;
    if (*curr != '\r' || *(curr + 1) != '\n') return PDB_PROTOCAL_ERROR;
    curr += 2; 

    int max_bulk = bulk_length ? *bulk_length : 0;

    for (int i = 0; i < count; i++) {
        if (curr >= end) return PDB_HALF_PACKAGE;
        if (*curr != '$') return PDB_PROTOCAL_ERROR;
        curr++; 

        int bulk_len = parse_resp_int(&curr, end);

        if (curr + 1 >= end) return PDB_HALF_PACKAGE;
        if (*curr != '\r' || *(curr + 1) != '\n') return PDB_PROTOCAL_ERROR;
        curr += 2; 

        if (bulk_len > max_bulk) {
            max_bulk = bulk_len;
        }

        if (curr + bulk_len + 2 > end) {
            return PDB_HALF_PACKAGE; 
        }
        
        // if (curr[bulk_len] != '\r' || curr[bulk_len + 1] != '\n') return PDB_PROTOCAL_ERROR;
             
        curr += bulk_len + 2; 
    }

    if (bulk_length) {
        *bulk_length = max_bulk;
    }
    
    return (int)(curr - buf);
}


int is_slave_to_master_response(int fd){
    if (global_conf.is_slave){
        return fd == master_fd;
    }

    return 0;
}

/**
 * Call the corresponding operator function() based on tokens.
 * If response != NULL, operator result will be written in response.
 * count: the num of tokens
 * Return the length of response
 */
extern int pdb_ebpf_init();
int pdb_filter_protocol(int fd, char** tokens, int count, char* response){
    assert(tokens != NULL && count != 0);

    int len = 0;
    int cmd = pdb_parser_cmd(tokens[0]);
    if (cmd == PDB_ERROR){
        // Receive error cmd
        pdb_log_info("unkown command: %s\n", tokens[0]);
        len = sprintf(response, "-ERR unknown command: %s\r\n", tokens[0]);
        return len;
    }

    int i;
    
    int ret = 0;
    char* key = tokens[1];
    char* raw_value;
    pdb_value* value;
    char* value_type_cmd = NULL;

    if (count > 2){
        raw_value = tokens[2];
    }
    char* value_get = NULL;

    pid_t pid;

    /*******bit map */
    char* endptr;
    uint64_t offset;
    int val;
    pdb_sds sds;
    int bitmap_value;
    int bitmap_count;
    int bitmap_pos_value;
    uint64_t start;
    uint64_t pos;
    int is_new = 0;
    struct pdb_bitmap* bitmap = NULL;
    /********* *****/

    /*******set***** */
    pdb_set* set;
    long set_el_count;
    int pop_count;
    char* key1;
    char* key2;
    pdb_value* value1;
    pdb_value* value2;
    int set_size;

    /***sorted set */
    struct pdb_sorted_set* sset;
    char* member;
    double score;
    double increment;
    int success;
    unsigned long rank;
    int sset_start;
    int sset_stop;
    char** res_range;

    switch(cmd){
        case PDB_CMD_PING:
            if (response != NULL){
                len = sprintf(response, "+PONG\r\n");
            }
            break;


        // array
        case PDB_CMD_SET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_array_set(&global_array, key, value);         
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST AND MOD\r\n");
                }
            }

            pdb_decre_value(value);
            break;

        case PDB_CMD_GET:
            value = pdb_array_get(&global_array, key);
            pdb_incre_value(value);
            value_get = pdb_parse_value_to_string(value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (value_get == NULL){
                    len = sprintf(response, "+NO EXIST\r\n");
                }else {
                    len = sprintf(response, "+%s\r\n", value_get);
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_MGET:
            len = 0;
            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response + len, "*%d\r\n", count - 1);
            }
            for (i = 1; i < count; i++){
                key = tokens[i];
                value = pdb_array_get(&global_array, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL && !is_slave_to_master_response(fd)){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "$-1\r\n");
                    } else {
                        len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(value_get), value_get);
                    }
                }
            }
            break;

        case PDB_CMD_MSET:
            ret = pdb_array_mset(&global_array, tokens, count - 1);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "-ERROR: mset error\r\n");
                } else if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "-ERROR: exist key\r\n");  
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                }
            }
            break;

        case PDB_CMD_DEL:
            ret = pdb_array_del(&global_array, key);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_MOD:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_array_mod(&global_array, key, value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_EXIST:
            ret = pdb_array_exist(&global_array, key);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST\r\n");
                } else{
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;

        // RBTREE
        case PDB_CMD_RSET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_rbtree_set(&global_rbtree, key, value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_RGET:
            value = pdb_rbtree_get(&global_rbtree, key);
            value_get = pdb_parse_value_to_string(value);

            if (response != NULL && !is_slave_to_master_response(fd)){
                if (value_get == NULL){
                    len = sprintf(response, "+NO EXIST\r\n");
                }else {
                    len = sprintf(response, "+%s\r\n", value_get);
                }
            }
            // pdb_decre_value(value);
            break;

        case PDB_CMD_RMSET:
            ret = pdb_rbtree_mset(&global_rbtree, tokens, count - 1);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                }
            }
            break;

        case PDB_CMD_RMGET:
            len = 0;
            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response + len, "*%d\r\n", count - 1);
            }
            for (i = 1; i < count; i++){
                key = tokens[i];
                value = pdb_rbtree_get(&global_rbtree, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL && !is_slave_to_master_response(fd)){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "$-1\r\n");
                    } else {
                        len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(value_get), value_get);
                    }
                }
            }
            break;

        case PDB_CMD_RDEL:
            ret = pdb_rbtree_del(&global_rbtree, key);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_RMOD:
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_rbtree_mod(&global_rbtree, key, value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_REXIST:
            ret = pdb_rbtree_exist(&global_rbtree, key);
            if(response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST\r\n");
                } else{
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;

        // HASH
        case PDB_CMD_HSET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_hash_set(&global_hash, key, value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_HGET:
            value = pdb_hash_get(&global_hash, key);
            value_get = pdb_parse_value_to_string(value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (value_get == NULL){
                    len = sprintf(response, "+NO EXIST\r\n");
                }else {
                    len = sprintf(response, "+%s\r\n", value_get);
                }
            }
            break;

        case PDB_CMD_HMGET:
            len = 0;
            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response + len, "*%d\r\n", count - 1);
            }
            for (i = 1; i < count; i++){   
                key = tokens[i];
                value = pdb_hash_get(&global_hash, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL && !is_slave_to_master_response(fd)){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "$-1\r\n");
                    } else {
                        len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(value_get), value_get);
                    }
                }
            }
            break;

        case PDB_CMD_HMSET:
            ret = pdb_hash_mset(&global_hash, tokens, count - 1);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "+OK\r\n");
                }
            }
            break;

        case PDB_CMD_HDEL:
            ret = pdb_hash_del(&global_hash, key);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_ERROR){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == PDB_DATASTRUCTURE_OK){
                    len = sprintf(response, "+OK\r\n");
                } else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }

            break;

        case PDB_CMD_HMOD:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_hash_mod(&global_hash, key, value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "+MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "+ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "+OK\r\n");
                } else{
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_HEXIST:
            ret = pdb_hash_exist(&global_hash, key);
            if(response != NULL && !is_slave_to_master_response(fd)){
                if (ret == PDB_DATASTRUCTURE_EXIST){
                    len = sprintf(response, "+EXIST\r\n");
                } else{
                    len = sprintf(response, "+NO EXIST\r\n");
                }
            }
            break;  




        /********************************************** */  
        /********************************************** */  
        /******************* bitmap ******************* */
        /********************************************** */  
        /********************************************** */  
        case PDB_CMD_BITMAP_SET:
        // BITSET key offset value
            // pdb_log_info("BITSET\n");
            if (count != 4){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+Protocol Error: Valilate RESP protocal\r\n");
                    break;
                }
                pdb_log_info("BITMAPSET: receive error protocol\n");
                break;
            }

            offset = strtoull(tokens[2], &endptr, 10);
            val = atoi(tokens[3]);
            value = pdb_hash_get(&global_hash, key);

            if (value != NULL && value->type != PDB_VALUE_TYPE_BITMAP){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    switch(value->type){
                        case PDB_VALUE_TYPE_SET:
                        {
                            len = sprintf(response, "+EXIST key in set\r\n");
                            break;
                        }

                        case PDB_VALUE_TYPE_SORTEDSET:
                        {
                            len = sprintf(response, "+EXIST key in sortedSet\r\n");
                            break;
                        }
                    }
                    
                }
                // pdb_log_info("value11111\n");
                break;
            }

            if (value == NULL){
                sds = pdb_get_new_sds(PDB_INIT_BTIMAP_LENGTH);
                bitmap = pdb_bitmap_create(key, sds);

                strcpy(bitmap->parent_key, key);
                value = pdb_create_value(bitmap, PDB_VALUE_TYPE_BITMAP);
                pdb_hash_set(&global_hash, key, value);
                is_new = 1;    
            }

            bitmap = (struct pdb_bitmap*)value->ptr;
            ret = pdb_bitmap_set_(bitmap, offset, val, NULL);
            if(is_new){
                pdb_decre_value(value);
            }

            if (ret == PDB_OK){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+OK\r\n");
                }
            }
            break;
            
        case PDB_CMD_BITMAP_GET:
            // BITGET key offset
            if (count != 3){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+Protocol Error: Valilate RESP protocal\r\n");
                    break;
                }
                pdb_log_info("BITMAPSET: receive error protocol\n");
                break;
            }
            // key = tokens[1];
            offset = strtoull(tokens[2], &endptr, 10);
            value = pdb_hash_get(&global_hash, key);
            
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+Unavailable key\r\n");
                }
                break;
            }
            bitmap = (struct pdb_bitmap*)value->ptr;
            sds = bitmap->data;
            
            bitmap_value = pdb_bitmap_get(sds, offset);
            // pdb_log_debug("bitmap_value: %d\n", bitmap_value);
            if (response != NULL && !is_slave_to_master_response(fd)){
                len = sprintf(response, "+%d\r\n", bitmap_value);
            }

            break;

        case PDB_CMD_BITMAP_COUNT:
        {
            // BITCOUNT key 
            value = pdb_hash_get(&global_hash, key);
            struct pdb_bitmap* bitmap = (struct pdb_bitmap*)pdb_parse_value_to_string(value);
            sds = bitmap->data;
            if (sds == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+unavailable key\r\n");
                    break;
                }
            }
            bitmap_count = pdb_bitmap_count(sds);
            if (response != NULL && !is_slave_to_master_response(fd)){
                len = sprintf(response, "+%d\r\n", bitmap_count);
            }
            break;
        }

        case PDB_CMD_BITMAP_POS:
        {
            // BITPOS key value start
            value = pdb_hash_get(&global_hash, key);
            struct pdb_bitmap* bitmap = (struct pdb_bitmap*)pdb_parse_value_to_string(value);
            sds = bitmap->data;
            if (sds == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+unavailable key\r\n");
                    break;
                }
            }
            
            bitmap_pos_value = atoi(tokens[2]);

            start = strtoull(tokens[3], &endptr, 10);
            pos = pdb_bitmap_pos(sds, bitmap_pos_value, start);
            if (response != NULL && !is_slave_to_master_response(fd)){
                len = sprintf(response, "+%ld\r\n", pos);
            }
            break;
        }

        case PDB_CMD_BITMAP_OP:
            // BITOP option[AND, OR, XOR, NOT] result_key key1 key2
        {
            char* option_token = tokens[1];
            char* result_key = tokens[2];
            char* key1 = tokens[3];
            char* key2;
            if (count > 4){
                key2 = tokens[4];
            }
            
            int option = 0;
            
            if (!strcmp(option_token, "AND")) option = BITOP_AND;
            else if (!strcmp(option_token, "OR")) option = BITOP_OR;
            else if (!strcmp(option_token, "XOR")) option = BITOP_XOR;
            else if (!strcmp(option_token, "NOT")) option = BITOP_NOT;
            else {
                if (response != NULL && !is_slave_to_master_response(fd)) len = sprintf(response, "-ERR: error option\r\n");
                break;
            }

            pdb_value* res_val = pdb_hash_get(&global_hash, result_key);
            struct pdb_bitmap* result_bitmap;
            pdb_sds result_sds;
            
            if (res_val == NULL) {
                result_sds = pdb_get_new_sds(PDB_INIT_BTIMAP_LENGTH);
                bitmap = pdb_bitmap_create(result_key, result_sds);

                strcpy(bitmap->parent_key, result_key);
                res_val = pdb_create_value(bitmap, PDB_VALUE_TYPE_BITMAP);

                pdb_hash_set(&global_hash, result_key, res_val);     
                pdb_decre_value(res_val);
            } else {
                struct pdb_bitmap* bitmap = (struct pdb_bitmap*)pdb_parse_value_to_string(res_val);
                result_sds = bitmap->data;
            }

            // bitmap1
            pdb_value* val1_obj = pdb_hash_get(&global_hash, key1);
            if (val1_obj == NULL) {
                if (response != NULL && !is_slave_to_master_response(fd)) len = sprintf(response, "-ERR: unavailable key1\r\n");
                break;
            }
            struct pdb_bitmap* bitmap1 = (struct pdb_bitmap*)pdb_parse_value_to_string(val1_obj);
            pdb_sds value1 = bitmap1->data;
            
            // NOT
            if (option == BITOP_NOT){
                pdb_bitmap_bitop(option, &result_sds, value1, NULL);
                ((struct pdb_bitmap*)(res_val->ptr))->data = result_sds; 
            
                if (response != NULL && !is_slave_to_master_response(fd)) {
                    len = sprintf(response, "+OK\r\n");
                }
                break;
            }

            // bitmap2
            pdb_value* val2_obj = pdb_hash_get(&global_hash, key2);
            if (val2_obj == NULL) {
                if (response != NULL && !is_slave_to_master_response(fd)) len = sprintf(response, "-ERR: unavailable key2\r\n");
                break;
            }
            struct pdb_bitmap* bitmap2 = (struct pdb_bitmap*)pdb_parse_value_to_string(val2_obj);
            pdb_sds value2 = bitmap2->data;
            
            pdb_bitmap_bitop(option, &result_sds, value1, value2);
            
            ((struct pdb_bitmap*)(res_val->ptr))->data = result_sds; 
            
            if (response != NULL && !is_slave_to_master_response(fd)) {
                len = sprintf(response, "+OK\r\n");
            }
            break;
        }

        


        /********************************************** */  
        /********************************************** */  
        /***************** sorted set ***************** */
        /********************************************** */  
        /********************************************** */
        case PDB_CMD_SSET_ADD:
            // SSADD key member score
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                sset = pdb_create_sortedSet_with_key(key);
                value = pdb_create_value((char*)sset, PDB_VALUE_TYPE_SORTEDSET);
                pdb_hash_set(&global_hash, key, value);
            }
            sset = value->ptr;
            sset->set->parent_key = key;

            member = tokens[2];
            score = atof(tokens[3]);
            ret = pdb_sortedSet_add(sset, member, score);
            if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+OK\r\n");
            break;

        case PDB_CMD_SSET_SCORE:
            // SSCORE key member
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EIXST\r\n");
                break;
            }
            sset = value->ptr;
            member = tokens[2];
            score = pdb_sortedSet_search(sset, member, &success);
            if (success == PDB_DATASTRUCTURE_NOEXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
            }else {
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+%f\r\n", score);
            }
            break;

        case PDB_CMD_SSET_INCRBY:
            // SSINCRBY key member increment
            key = tokens[1];
            increment = atof(tokens[3]);
            member = tokens[2];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            sset = value->ptr;
            ret = pdb_sortedSet_incre(sset, member, increment);
            if (ret == PDB_DATASTRUCTURE_NOEXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
            }else if (ret == PDB_DATASTRUCTURE_OK){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+OK\r\n");
            }

            break;

        case PDB_CMD_SSET_RANK:
            // SSRANK key member
            key = tokens[1];
            member = tokens[2];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            sset = value->ptr;
            rank = pdb_sortedSet_rank(sset, member, &success);
            if (success == PDB_DATASTRUCTURE_NOEXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
            }else if (success == PDB_DATASTRUCTURE_EXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+%lu\r\n", rank);
            }

            break;

        case PDB_CMD_SSET_RANGE:
            // ZRANGE key start stop
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                printf("no exist\n");
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            sset = value->ptr;
            sset_start = atoi(tokens[2]);
            sset_stop = atoi(tokens[3]);

            res_range = pdb_sortedSet_get_revrange(sset, sset_start, sset_stop);
            if (response != NULL && !is_slave_to_master_response(fd)){
                int count = sset_stop - sset_start + 1;
                len += sprintf(response + len, "*%d\r\n", count);
            }

            for (i = 0; i < sset_stop - sset_start + 1; i++){
                if (res_range[i] != NULL){
                    if (response != NULL && !is_slave_to_master_response(fd)){
                        len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(res_range[i]), res_range[i]);
                    }
                }else{
                    if (response != NULL && !is_slave_to_master_response(fd)){
                        len += sprintf(response + len, "$-1\r\n");
                    }
                } 
            }

            break;




        /********************************************** */  
        /********************************************** */  
        /***************** set ************************ */
        /********************************************** */  
        /********************************************** */
        case PDB_CMD_SET_SET:
            // SSET key value1 value2 value3....
            value = NULL;
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                set = pdb_set_create_with_key(key);
                value = pdb_create_value((char*)set, PDB_VALUE_TYPE_SET);
                pdb_hash_set(&global_hash, key, value);
            }
            set = (pdb_set*)value->ptr;
            if (set->flag == PDB_SET_ENCODING_HASHTABLE){
                size_t key_len = strlen(key) + 1;
                char* parent = pdb_malloc(key_len);
                strcpy(parent, key);
                parent[key_len - 1] = '\0';
                ((pdb_hash_t*)set->ptr)->parent_key = parent;
            }

            for (i = 2; i < count; i++){
                ret = pdb_set_add(set, tokens[i]);
            }
            if (ret != PDB_DATASTRUCTURE_ERROR){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len = sprintf(response, "+OK\r\n");
                }
            }
            break;

        case PDB_CMD_SET_DEL:
            //SDEL key member1 member2 ...
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            set = (pdb_set*)value->ptr;
            for (i = 2; i < count; i++){
                ret = pdb_set_delete(set, tokens[i]);
                if (ret == PDB_DATASTRUCTURE_NOEXIST){
                    if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                    break;
                }
            }
            if (response != NULL && !is_slave_to_master_response(fd)){
                len = sprintf(response, "+OK\r\n");
            }
            
            break;

        case PDB_CMD_SET_CARD:
            // SCARD key
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            set = (struct pdb_set*)value->ptr;
            set_el_count = pdb_set_get_count(set);
            if (response != NULL && !is_slave_to_master_response(fd)){
                len = sprintf(response, "+%ld\r\n", set_el_count);
            }
            break;

        case PDB_CMD_SET_EXIST:
            // SEXIST key value
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            raw_value = tokens[2];
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            set = (struct pdb_set*)value->ptr;
            ret = pdb_set_search(set, raw_value);
            if (ret == PDB_DATASTRUCTURE_EXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+EXIST\r\n");
            }else if (ret == PDB_DATASTRUCTURE_NOEXIST){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
            }else {
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+ERROR\r\n");
            }
            break;

        case PDB_CMD_SET_RANDOMPOP:
            // SRANDOMPOP
            key = tokens[1];
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            set = (struct pdb_set*)value->ptr;
            value_get = pdb_set_random_pop(set);
            if (value_get == NULL)  {
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+EMPTY\r\n");
            }else{
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+%s\r\n", value_get);
            } 
            break;

        case PDB_CMD_SET_NRANDOMPOP:
            // SNRANDOMPOP key count
            key = tokens[1];
            pop_count = atoi(tokens[2]);
            value = pdb_hash_get(&global_hash, key);
            if (value == NULL){
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "+NO EXIST\r\n");
                break;
            }
            set = (struct pdb_set*)value->ptr;
            if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response, "*%d\r\n", pop_count);
            for (i = 0; i < pop_count; i++){
                value_get = pdb_set_random_pop(set);
                len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(value_get), value_get);
            }
            break;

        case PDB_CMD_SET_INTER:
            // SINTER key1 key2
            key1 = tokens[1];
            key2 = tokens[2];
            value1 = pdb_hash_get(&global_hash, key1);
            value2 = pdb_hash_get(&global_hash, key2);
            if (value1 == NULL || value2 == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len += sprintf(response, "-ERROR: unknown key: %s and %s\r\n", key1, key2);
                }
                break;
            }

            set = pdb_set_inter(value1->ptr, value2->ptr);
            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response, "*%ld\r\n", set->count);
            }
            
            if (set->flag == PDB_SET_ENCODING_INTSET){
                struct pdb_intset* intset = set->ptr;
                for (i = 0; i < intset->len; i++){
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
                    if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(buf), buf);
                }
            } else if (set->flag == PDB_SET_ENCODING_HASHTABLE){
                hashtable_t* hash = set->ptr;
                for (i = 0; i < hash->max_slots; i++){
                    hashnode_t* node = hash->nodes[i];
                    while(node != NULL){
                        if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(node->key), node->key);
                        node = node->next;
                    }
                }
            }
            pdb_set_destroy(set);
            
            break;

        case PDB_CMD_SET_UNION:
            // SUNION key1 key2
            key1 = tokens[1];
            key2 = tokens[2];
            value1 = pdb_hash_get(&global_hash, key1);
            value2 = pdb_hash_get(&global_hash, key2);
            if (value1 == NULL || value2 == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len += sprintf(response, "-ERROR: unknown key: %s and %s\r\n", key1, key2);
                }
                break;
            }
            set = pdb_set_union(value1->ptr, value2->ptr);

            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response, "*%ld\r\n", set->count);
            }
            
            if (set->flag == PDB_SET_ENCODING_INTSET){
                struct pdb_intset* intset = set->ptr;
                for (i = 0; i < intset->len; i++){
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
                    if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(buf), buf);
                }
            } else if (set->flag == PDB_SET_ENCODING_HASHTABLE){
                hashtable_t* hash = set->ptr;
                for (i = 0; i < hash->max_slots; i++){
                    hashnode_t* node = hash->nodes[i];
                    while(node != NULL){
                        if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(node->key), node->key);
                        node = node->next;
                    }
                }
            }
            pdb_set_destroy(set);

            break;

        case PDB_CMD_SET_DIFFER:
            // SDIFFER key1 key2
            key1 = tokens[1];
            key2 = tokens[2];
            value1 = pdb_hash_get(&global_hash, key1);
            value2 = pdb_hash_get(&global_hash, key2);
            if (value1 == NULL || value2 == NULL){
                if (response != NULL && !is_slave_to_master_response(fd)){
                    len += sprintf(response, "-ERROR: unknown key: %s and %s\r\n", key1, key2);
                }
                break;
            }
            set = pdb_set_differ(value1->ptr, value2->ptr);
            
            if (response != NULL && !is_slave_to_master_response(fd)){
                len += sprintf(response, "*%ld\r\n", set->count);
            }

            if (set->flag == PDB_SET_ENCODING_INTSET){
                struct pdb_intset* intset = set->ptr;
                for (i = 0; i < intset->len; i++){
                    char buf[64] = {0};
                    snprintf(buf, sizeof(buf), "%" PRId64, _pdb_intset_get(intset, i));
                    if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(buf), buf);
                }
            } else if (set->flag == PDB_SET_ENCODING_HASHTABLE){
                hashtable_t* hash = set->ptr;
                for (i = 0; i < hash->max_slots; i++){
                    hashnode_t* node = hash->nodes[i];
                    while(node != NULL){
                        if (response != NULL && !is_slave_to_master_response(fd))   len += sprintf(response + len, "$%zu\r\n%s\r\n", strlen(node->key), node->key);
                        node = node->next;
                    }
                }
            }
            pdb_set_destroy(set);
            break;

        case PDB_CMD_NSAVE:
            global_dump.is_aof = 0;
            break;

        case PDB_CMD_SAVE:
            if (global_conf.is_aof){
                // AOF dump
                global_dump.is_aof = 1;
                pdb_init_aof();
                pdb_ebpf_init();
                if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "OK\r\n");
                break;
            }else{
                // RDB dump
                pid = fork();
                if (pid == 0){
                    // child thread
                    pdb_log_info("RDB(child pid: %d) is saving.......\n", pid);
                    int ret = pdb_rdb_dump(global_conf.dump_dir);
                    if (ret == PDB_OK){
                        pdb_log_info("RDB saves success\n");
                        // if (response != NULL)   len = sprintf(response, "OK\r\n");
                    }else{
                        pdb_log_info("RDB saves failed\n");
                        // if (response != NULL)   len = sprintf(response, "SAVE FAILED\r\n");
                    }
                
                    _exit(0);
                }else if (pid > 0){
                    // father thread
                    if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "OK\r\n");
                }else{
                    perror("fork failed");
                    if (response != NULL && !is_slave_to_master_response(fd))   len = sprintf(response, "SAVE FAILED\r\n");
                }
                break;
            }


        /********************************************** */  
        /********************************************** */  
        /***********  SLAVE to MASTER SYNC  *********** */
        /********************************************** */  
        /********************************************** */

        case PDB_CMD_SYN:  
        {
            /****************************************************************** */
            /***********************    master node   ************************* */
            /***********************      SETP 1      ************************* */
            /* master node receive "ZSYN", and reply "RDMA_READY" to slave node */
            /****************************************************************** */
            if (global_master_snapshot == NULL) {
                // 512M
                global_master_snapshot = pdb_rdma_create_snapshot("rxe0", PDB_RDMA_MEMPOOL_SIZE);
                if (!global_master_snapshot) {
                    // pdb_log_info("master send +RDMA_READY\n");
                    if (response != NULL) len = sprintf(response, "-ERR RDMA Snapshot init failed\r\n");
                    break;
                }
                
                if (pdb_rdma_serialize(global_master_snapshot) != PDB_RDMA_OK) {
                    pdb_rdma_release_snapshot(global_master_snapshot);
                    global_master_snapshot = NULL;
                    if (response != NULL) len = sprintf(response, "-ERR DB Serialization failed\r\n");
                    break;
                }
            } else {
                pdb_log_info("[MASTER] Snapshot already exists (RefCount: %d). Skipping serialization!\n", 
                       global_master_snapshot->ref_count);
            }

            pdb_rdma_conn_ctx* conn = pdb_rdma_create_conn(global_master_snapshot);
            if (!conn) {
                if (response != NULL) len = sprintf(response, "-ERR RDMA Conn create failed\r\n");
                break;
            }
            conn_list[fd]->rdma_conn = conn;

            char gid_str[33];
            _gid_to_str(conn->local_info.gid, gid_str);

            
            // master reply RDMA_READY
            // RDMA_READY <vaddr> <rkey> <size> <qpn> <psn> <lid> <gid_str>\r\n
            if (response != NULL) {
                char vaddr_str[64], rkey_str[32], size_str[64], qpn_str[32], psn_str[32], lid_str[32];  
                int vaddr_len = sprintf(vaddr_str, "%llu", (unsigned long long)global_master_snapshot->mr->addr);
                int rkey_len  = sprintf(rkey_str, "%u", global_master_snapshot->mr->rkey);
                int size_len  = sprintf(size_str, "%zu", *(global_master_snapshot->pool.used_offset));
                int qpn_len   = sprintf(qpn_str, "%u", conn->local_info.qpn);
                int psn_len   = sprintf(psn_str, "%u", conn->local_info.psn);
                int lid_len   = sprintf(lid_str, "%hu", conn->local_info.lid);
                int gid_len   = strlen(gid_str);

                if (response != NULL) {
                    len = sprintf(response, 
                        "*8\r\n"
                        "$11\r\nZRDMA_READY\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n"
                        "$%d\r\n%s\r\n",
                        vaddr_len, vaddr_str,
                        rkey_len,  rkey_str,
                        size_len,  size_str,
                        qpn_len,   qpn_str,
                        psn_len,   psn_str,
                        lid_len,   lid_str,
                        gid_len,   gid_str
                    );
                }
            }
            break;
        }

        case PDB_CMD_SLAVE_OOB:
        {
            /*********************************************************************** */
            /***********************     slave node   ****************************** */
            /***********************       SETP 2     ****************************** */
            /* slave node receive "RDMA_READY", and reply "ZSYN_OOB" to master node  */
            /*********************************************************************** */
            if (global_conf.is_slave){
                if (!tokens[1] || !tokens[2] || !tokens[3] || !tokens[4] || !tokens[5] || !tokens[6] || !tokens[7]) break;

                remote_vaddr = strtoull(tokens[1], NULL, 10);
                remote_rkey  = (uint32_t)strtoul(tokens[2], NULL, 10);
                pull_size    = (size_t)strtoull(tokens[3], NULL, 10);

                pdb_rdma_conn_info master_info;
                memset(&master_info, 0, sizeof(master_info));
                master_info.qpn = (uint32_t)strtoul(tokens[4], NULL, 10);
                master_info.psn = (uint32_t)strtoul(tokens[5], NULL, 10);
                master_info.lid = (uint16_t)strtoul(tokens[6], NULL, 10);
                _str_to_gid(tokens[7], master_info.gid);

                pdb_rdma_snapshot_ctx* slave_recv_snapshot = pdb_rdma_create_snapshot("rxe0", pull_size + 4096);
                if (!slave_recv_snapshot) break;

                slave_conn = pdb_rdma_create_conn(slave_recv_snapshot);
                if (!slave_conn) break;

                conn_list[fd]->rdma_conn = slave_conn;

                if (pdb_rdma_connect_qp(slave_conn, &master_info) != 0) break;

                char my_gid_str[33];
                _gid_to_str(slave_conn->local_info.gid, my_gid_str);
                
                char qpn_str[32], psn_str[32], lid_str[32];
                int qpn_len = sprintf(qpn_str, "%u", slave_conn->local_info.qpn);
                int psn_len = sprintf(psn_str, "%u", slave_conn->local_info.psn);
                int lid_len = sprintf(lid_str, "%hu", slave_conn->local_info.lid);
                int gid_len = strlen(my_gid_str);

                // reply "ZSYN_OOB" to master node
                char oob_buf[512];
                int oob_len = sprintf(oob_buf, 
                    "*5\r\n"
                    "$8\r\nZSYN_OOB\r\n"
                    "$%d\r\n%s\r\n"
                    "$%d\r\n%s\r\n"
                    "$%d\r\n%s\r\n"
                    "$%d\r\n%s\r\n",
                    qpn_len, qpn_str, psn_len, psn_str, lid_len, lid_str, gid_len, my_gid_str
                );

                write(fd, oob_buf, oob_len);
                if (response != NULL) len = 0; 
                
                // pdb_log_info("[SLAVE REPL] Sent ZSYN_OOB to Master. Waiting for ZOOB_ACK...\n");
            }
            break;
        }

        case PDB_CMD_SYNC_OOB:
        {
            /******************************************************************** */
            /***********************    master node   *************************** */
            /***********************      SETP 3      *************************** */
            /* master node receive "ZSYN_OOB", and reply "ZOOB_ACK" to slave node */
            /******************************************************************** */
            pdb_rdma_conn_ctx* conn = conn_list[fd]->rdma_conn;
            if (!conn) break; 

            pdb_rdma_conn_info slave_info;
            memset(&slave_info, 0, sizeof(slave_info));
            slave_info.qpn = atoi(tokens[1]);
            slave_info.psn = atoi(tokens[2]);
            slave_info.lid = atoi(tokens[3]);
            _str_to_gid(tokens[4], slave_info.gid);

            if (pdb_rdma_connect_qp(conn, &slave_info) != 0) break;

            // reply ZOOB_ACK to slave node
            char ack_buf[64];
            int ack_len = sprintf(ack_buf, "*1\r\n$8\r\nZOOB_ACK\r\n");
            write(fd, ack_buf, ack_len);
            
            if (response != NULL) len = 0; 
            break;
        }

        case PDB_CMD_SLAVE_REQUIRE:
        {
            /******************************************************************** */
            /***********************    slave node    *************************** */
            /***********************      SETP 4      *************************** */
            /* slave node receive "ZOOB_ACK", heist data and reply "ZSYN_ACK" to master node */
            /******************************************************************** */
            if (global_conf.is_slave) {
                // pdb_log_info("[SLAVE REPL] Received OOB_ACK. Executing RDMA HEIST!\n");
                
                struct conn_info* current_client = conn_list[fd];
                if (!current_client || !current_client->rdma_conn) break;

                pdb_rdma_conn_ctx* slave_conn = current_client->rdma_conn;

                execute_rdma_read_heist(slave_conn, remote_vaddr, remote_rkey, pull_size);
                // pdb_log_info("[SLAVE REPL] Hardware Heist complete. Rebuilding memory database...\n");

                *(slave_conn->snap->pool.used_offset) = pull_size;

                if (pdb_rdma_deserialize(slave_conn->snap) != PDB_RDMA_OK) {
                    pdb_log_error("Memory Database Reconstruction Failed!\n");
                    pdb_rdma_destroy_conn(slave_conn);
                    current_client->rdma_conn = NULL;
                    break; 
                }

                pdb_rdma_destroy_conn(slave_conn);
                current_client->rdma_conn = NULL; 

                // salve heist and deserialize data successfully.
                // slave reply ZSYN_ACK to master node
                char sync_ack_buf[64];
                int sync_ack_len = sprintf(sync_ack_buf, "*1\r\n$8\r\nZSYN_ACK\r\n");
                write(fd, sync_ack_buf, sync_ack_len);
                
                if (response != NULL) len = 0;
            }
            break;
        }


        case PDB_CMD_SYNC_ACK:
        {
            /******************************************************************** */
            /***********************    master node    ************************** */
            /***********************      SETP 5      *************************** */
            /* master node release snapshot and destroy rdma conn**************** */
            /******************************************************************** */
            if (conn_list[fd]->rdma_conn) {
                pdb_rdma_destroy_conn(conn_list[fd]->rdma_conn);
                conn_list[fd]->rdma_conn = NULL;
                
                if (global_master_snapshot && global_master_snapshot->ref_count == 1) {
                    pdb_rdma_release_snapshot(global_master_snapshot);
                    global_master_snapshot = NULL;
                }
            }

            for (i = 0; i < REPLICATION_NUM; i++){
                if (global_replication->fd[i] == 0){
                    global_replication->fd[i] = fd;
                    global_replication->slave_num++;
                }
                break;
            }

            char ok_buf[64];
            int ok_len = sprintf(ok_buf, "*1\r\n$7\r\nZRDB_OK\r\n");
            write(fd, ok_buf, ok_len);
            
            if (response != NULL) len = 0;

            conn_list[fd]->is_incre_ready = 1;
            break;
        }

        
        // increment syn slave
        case PDB_CMD_ZRDB_OK:
        {
            pdb_log_info("slave node receive ZRDB_OK\n");
            // slave node
            if (global_conf.is_slave) {
                conn_list[fd]->is_incre_ready = 1;
            }

            break;
        }

        // MEM
        case PDB_CMD_MEM_USED:
        {
            if (response != NULL){
                len = sprintf(response, "%ld\r\n", global_memory_manager->used_memory);
                uint64_t epoch = 1;
                size_t sz = sizeof(epoch);
                mallctl("epoch", &epoch, &sz, &epoch, sz);

                size_t jemalloc_allocated = 0;
                size_t len = sizeof(jemalloc_allocated);
                mallctl("stats.allocated", &jemalloc_allocated, &len, NULL, 0);

                printf("PDB used_memory: %zu, Jemalloc allocated: %zu\n", 
                        global_memory_manager->used_memory, jemalloc_allocated);
            }
            break;
        }

        case PDB_CMD_MEM_RSS:
        {
            if (response != NULL){
                len = sprintf(response, "%ld\r\n", global_memory_manager->used_memory_rss);
            }
            break;
        }

        case PDB_CMD_EXIT:
            return -99;

        default:
            if (response != NULL){
                ret = sprintf(response, "-ERR unknown command\r\n");
            }
            pdb_log_info("Receive error cmd\n");
            break;
    }

    return len;

}

/**
 * Get the num of tokens, allocate `tokens` based on the num of tokens, and call underlying data structure API based on `tokens`.
 * If `out` == NULL, the function only completes the operation of conrresponding data structure(set/get/mod...), without response for user.
 * Return the length of `out`.
 */
int pdb_protocol(int fd, char* msg, int length, char* out){
    assert(msg != NULL && length > 0);

    char* rmsg = (char*)malloc(length + 1);
    if (rmsg == NULL){
        pdb_log_error("malloc error\n");
    }
    memcpy(rmsg, msg, length);
    rmsg[length] = '\0';
    
    // Get the num of tokens.
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
    char* p_tokens[3];
    // allocate `tokens` based on `count`(the num of tokens)
    char** tokens;
    if (count < 3){
        tokens = p_tokens;
    }else{
        tokens = (char**)pdb_malloc(sizeof(char*) * (count + 2));  
    }
    if (tokens == NULL) {
        perror("malloc tokens failed");
        return -1;
    }
    memset(tokens, 0, sizeof(char*) * count);

    // split `rmsg` and 
    count = pdb_split_token(rmsg, length, tokens);
    if (count == -1){
        pdb_log_debug("pdb_split_token return -1\n");
        free(tokens);
        return -1;
    }


    // complete the opreation of data structure.
    int ret = pdb_filter_protocol(fd, tokens, count, out);
    
    if (count >= 3){
        pdb_free(tokens, -1);
    }

    free(rmsg);
    return ret;
}

