#include "pdb_parse_protocol.h"


const char* command[] = {
    // array
    "SET", "GET", "DEL", "MOD", "EXIST", "MSET", "MGET", 
    // rbtree
    "RSET", "RGET", "RDEL", "RMOD", "REXIST", "RMSET", "RMGET", 
    // hash
    "HSET", "HGET", "HDEL", "HMOD", "HEXIST", "HMSET", "HMGET",
    // skiptable
    "SKSET", "SKGET", "SKDEL", "SKMOD", "SKEXIST", "SKMSET", "SKMGET",
    // bitmap
    "BITSET", "BITGET", "BITCOUNT", "BITPOS", "BITOP",
    // 
    "EXIT", "SAVE", "NSAVE", "SYN"
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

    PDB_CMD_EXIT,                   
    PDB_CMD_SAVE,
    PDB_CMD_NSAVE,

    PDB_CMD_SYN,
    PDB_CMD_COUNT
};


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
            if (strcmp(cmd_str, "SYN") == 0)        return PDB_CMD_SYN;
            if (strcmp(cmd_str, "SET") == 0)        return PDB_CMD_SET;

            if (strcmp(cmd_str, "SKGET") == 0)      return PDB_CMD_SKGET;
            if (strcmp(cmd_str, "SKSET") == 0)      return PDB_CMD_SKSET;
            if (strcmp(cmd_str, "SKEXIST") == 0)    return PDB_CMD_SKEXIST;
            if (strcmp(cmd_str, "SKMGET") == 0)     return PDB_CMD_SKMGET;
            if (strcmp(cmd_str, "SKMSET") == 0)     return PDB_CMD_SKMSET;
            if (strcmp(cmd_str, "SKMOD") == 0)      return PDB_CMD_SKMOD;
            if (strcmp(cmd_str, "SKDEL") == 0)      return PDB_CMD_SKDEL;

            break;

        case 'N':
            if (strcmp(cmd_str, "NSAVE") == 0)  return PDB_CMD_NSAVE;

        case 'B':
            if (strcmp(cmd_str, "BITSET"))      return PDB_CMD_BITMAP_SET;
            if (strcmp(cmd_str, "BITGET"))      return PDB_CMD_BITMAP_GET;
            if (strcmp(cmd_str, "BITCOUNT"))    return PDB_CMD_BITMAP_COUNT;
            if (strcmp(cmd_str, "BITOP"))       return PDB_CMD_BITMAP_OP;
            if (strcmp(cmd_str, "BITPOS"))      return PDB_CMD_BITMAP_POS;
            break;
    }

    return PDB_ERROR; // can not find the command
}

/**
 * Check for a complete package in 'buf' and return its total length.
 * 
 * Return PDB_PROTOCAL_ERROR if `buf` violates the RESP.
 * Return PDB_HALF_PACKAGE if `buf` contains a incomplete package(a half package).
 * Return total length if `buf` contains a valid and complete package.
 */
int check_resp_integrity(const char *buf, int len, int* bulk_length) {
	if (buf[0] != '*') {
        return PDB_PROTOCAL_ERROR;
    }
    if (len < 4){
        return PDB_HALF_PACKAGE; 
    }
    if (buf[0] != '*') {
		return PDB_PROTOCAL_ERROR;
	} 

    const char *curr = buf;
    const char *end = buf + len;

    // Parsing the num of tokens
    const char *crlf = strstr(curr, "\r\n");
    if (!crlf || crlf >= end) {
		return PDB_HALF_PACKAGE;
    }

    int count = atoi(curr + 1);
    curr = crlf + 2;

    for (int i = 0; i < count; i++) {
        if (curr >= end){
            return PDB_HALF_PACKAGE;
        } 
        if (*curr != '$'){
			return PDB_PROTOCAL_ERROR;
		}

        crlf = strstr(curr, "\r\n");
        if (!crlf || crlf >= end) {
            return PDB_HALF_PACKAGE;
        }

		int bulk_len = atoi(curr + 1);
		if (bulk_length != NULL){
			*bulk_length = bulk_len > *bulk_length ? bulk_len : *bulk_length;
		}
		
        curr = crlf + 2;

        if (curr + bulk_len + 2 > end) {
            return PDB_HALF_PACKAGE; 
        }
             
        curr += bulk_len + 2; 
    }
    return (int)(curr - buf);
}


/**
 * Call the corresponding operator function() based on tokens.
 * If response != NULL, operator result will be written in response.
 * count: the num of tokens
 * Return the length of response
 */
int pdb_filter_protocol(char** tokens, int count, char* response){
    assert(tokens != NULL && count != 0);

    int len = 0;
    int cmd = pdb_parser_cmd(tokens[0]);
    if (cmd == PDB_ERROR){
        // Receive error cmd
        len = sprintf(response, "Receive error cmd\r\n");
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
    /********* *****/


    switch(cmd){
        // array
        case PDB_CMD_SET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_array_set(&global_array, key, value);         
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else if (ret > 0){
                    len = sprintf(response, "EXIST\r\n");
                }
            }

            pdb_decre_value(value);
            break;

        case PDB_CMD_GET:
            value = pdb_array_get(&global_array, key);
            pdb_incre_value(value);
            value_get = pdb_parse_value_to_string(value);
            if (response != NULL){
                if (value_get == NULL){
                    len = sprintf(response, "NO EXIST\r\n");
                }else {
                    len = sprintf(response, "%s\r\n", value_get);
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_MGET:
            len = 0;
            for (i = 1; i < count; i++){
                key = tokens[i];
                value = pdb_array_get(&global_array, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL){
                    if (value == NULL) {
                        len += sprintf(response + len, "ERROR\r\n");
                    } else {
                        len += sprintf(response + len, "%s\r\n", value_get);
                    }
                }
            }
            break;

        case PDB_CMD_MSET:
            ret = pdb_array_mset(&global_array, tokens, count - 1);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                }
            }
            break;

        case PDB_CMD_DEL:
            ret = pdb_array_del(&global_array, key);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_MOD:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_array_mod(&global_array, key, value);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_EXIST:
            ret = pdb_array_exist(&global_array, key);
            if (response != NULL){
                if (ret == 0){
                    len = sprintf(response, "EXIST\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        // RBTREE
        case PDB_CMD_RSET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_rbtree_set(&global_rbtree, key, value);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else if (ret > 0){
                    len = sprintf(response, "EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_RGET:
            value = pdb_rbtree_get(&global_rbtree, key);
            value_get = pdb_parse_value_to_string(value);

            if (response != NULL){
                if (value_get == NULL){
                    len = sprintf(response, "NO EXIST\r\n");
                }else {
                    len = sprintf(response, "%s\r\n", value_get);
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_RMSET:
            ret = pdb_rbtree_mset(&global_rbtree, tokens, count - 1);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                }
            }
            break;

        case PDB_CMD_RMGET:
            len = 0;
            for (i = 1; i < count; i++){
                key = tokens[i];
                value = pdb_array_get(&global_array, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "NO EXIST\r\n");
                    } else {
                        len += sprintf(response + len, "%s\r\n", value_get);
                    }
                }
            }
            break;

        case PDB_CMD_RDEL:
            ret = pdb_rbtree_del(&global_rbtree, key);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_RMOD:
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_rbtree_mod(&global_rbtree, key, value);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_REXIST:
            ret = pdb_rbtree_exist(&global_rbtree, key);
            if(response != NULL){
                if (ret == 0){
                    len = sprintf(response, "EXIST\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        // BITMAP
        case PDB_CMD_BITMAP_SET:
        // BITSET key offset value
            if (count != 4){
                if (response != NULL){
                    len = sprintf(response, "Protocol Error: Valilate RESP protocal\n");
                    break;
                }
                pdb_log_info("BITMAPSET: receive error protocol\n");
                break;
            }

            offset = strtoull(tokens[2], &endptr, 10);
            val = atoi(tokens[3]);
            value = pdb_hash_get(&global_hash, key);

            if (value == NULL){
                sds = pdb_get_new_sds(PDB_INIT_BTIMAP_LENGTH);
                value = pdb_create_value(sds, PDB_VALUE_TYPE_BITMAP);
                ret = pdb_hash_set(&global_hash, key, value);
                pdb_decre_value(value);     
            }
            sds = pdb_parse_value_to_string(value);
            ret = pdb_bitmap_set(&sds, offset, val, NULL);
            if (ret == PDB_OK){
                if (response != NULL){
                    len = sprintf(response, "OK\n");
                }
            }
            break;
            
        case PDB_CMD_BITMAP_GET:
            // BITGET key offset
            if (count != 3){
                if (response != NULL){
                    len = sprintf(response, "Protocol Error: Valilate RESP protocal\n");
                    break;
                }
                pdb_log_info("BITMAPSET: receive error protocol\n");
                break;
            }
            offset = strtoull(tokens[2], &endptr, 10);
            value = pdb_hash_get(&global_hash, key);
            sds = pdb_parse_value_to_string(value);
            if (sds == NULL){
                if (response != NULL){
                    len = sprintf(response, "Unavailable key\n");
                }
                break;
            }
            
            bitmap_value = pdb_bitmap_get(sds, offset);
            if (response != NULL){
                len = sprintf(response, "%d\r\n", bitmap_value);
            }

            break;

        case PDB_CMD_BITMAP_COUNT:
            // BITCOUNT key 
            value = pdb_hash_get(&global_hash, key);
            sds = pdb_parse_value_to_string(value);
            if (sds == NULL){
                if (response != NULL){
                    len = sprintf(response, "unavailable key\n");
                    break;
                }
            }
            bitmap_count = pdb_bitmap_count(sds);
            if (response != NULL){
                len = sprintf(response, "%d\r\n", bitmap_count);
            }
            break;

        case PDB_CMD_BITMAP_POS:
            // BITPOS key value start
            value = pdb_hash_get(&global_hash, key);
            sds = pdb_parse_value_to_string(value);
            if (sds == NULL){
                if (response != NULL){
                    len = sprintf(response, "unavailable key\n");
                    break;
                }
            }
            
            bitmap_pos_value = atoi(tokens[2]);

            start = strtoull(tokens[3], &endptr, 10);
            pos = pdb_bitmap_pos(sds, bitmap_pos_value, start);
            if (response != NULL){
                len = sprintf(response, "%ld\r\n", pos);
            }
            break;

        case PDB_CMD_BITMAP_OP:
            // BITOP option[AND, OR, XOR, NOT] result_key key1 key2
        {
            char* option_token = tokens[1];
            char* result_key = tokens[2];
            char* key1 = tokens[3];
            char* key2 = tokens[4];
            int option;
            if (!strcmp(option_token, "AND")){
                option = BITOP_AND;
            }
            if (!strcmp(option_token, "OR")){
                option = BITOP_OR;
            }
            if (!strcmp(option_token, "XOR")){
                option = BITOP_XOR;
            }

            value = pdb_hash_get(&global_hash, result_key);
            pdb_sds result_sds = pdb_parse_value_to_string(value);
            if (sds == NULL){
                result_sds = pdb_get_new_sds(PDB_INIT_BTIMAP_LENGTH);
                ret = pdb_hash_set(&global_hash, result_key, value);     
            }
            value = pdb_hash_get(&global_hash, key1);
            pdb_sds value1 = pdb_parse_value_to_string(value);
            if (value1 == NULL){
                if (response != NULL){
                    len = sprintf(response, "unavailable key1\n");
                    break;
                }
            }
            value = pdb_hash_get(&global_hash, key2);
            pdb_sds value2 = pdb_parse_value_to_string(value);
            if (value2 == NULL){
                if (response != NULL){
                    len = sprintf(response, "unavailable key2\n");
                    break;
                }
            }
            
            pdb_bitmap_bitop(option, &result_sds, value1, value2);
            len = sprintf(response, "OK\r\n");
            break;
        }

        // HASH
        case PDB_CMD_HSET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

            ret = pdb_hash_set(&global_hash, key, value);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else if (ret > 0){
                    len = sprintf(response, "EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_HGET:
            value = pdb_hash_get(&global_hash, key);
            value_get = pdb_parse_value_to_string(value);
            if (response != NULL){
                if (value_get == NULL){
                    len = sprintf(response, "NO EXIST\r\n");
                }else {
                    len = sprintf(response, "%s\r\n", value_get);
                }
            }
            break;

        case PDB_CMD_HMGET:
            len = 0;
            for (i = 1; i < count; i++){   
                key = tokens[i];
                value = pdb_hash_get(&global_hash, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "NO EXIST\r\n");
                    } else {
                        len += sprintf(response + len, "%s\r\n", value_get);
                    }
                }
            }
            break;

        case PDB_CMD_HMSET:
            ret = pdb_hash_mset(&global_hash, tokens, count - 1);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                }
            }
            break;

        case PDB_CMD_HDEL:
            ret = pdb_hash_del(&global_hash, key);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;

        case PDB_CMD_HMOD:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_hash_mod(&global_hash, key, value);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_HEXIST:
            ret = pdb_hash_exist(&global_hash, key);
            if (response != NULL){
                if (ret == 0){
                    len = sprintf(response, "EXIST\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            break;  


        /************** SKIPTABLE *********************/
        case PDB_CMD_SKSET:
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_skiptable_insert(&global_skiplist, key, value);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "EXIST\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_SKGET:
            value = pdb_skiptable_search(&global_skiplist, key);
            value_get = pdb_parse_value_to_string(value);
            if (response != NULL){
                if (value_get == NULL){
                    len = sprintf(response, "NO EXIST\r\n");
                }else {
                    len = sprintf(response, "%s\r\n", value_get);
                }
            }
            break;

        case PDB_CMD_SKDEL:
            ret = pdb_skiptable_delete(&global_skiplist, key);
            if (response != NULL){
                if (ret < 0){
                    len = sprintf(response, "NO EXIST\r\n");
                } else if (ret == 0){
                    len = sprintf(response, "OK\r\n");
                }
            }
            break;

        case PDB_CMD_SKMOD:
            // SKMOD key value
            raw_value = tokens[2];
            value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
            ret = pdb_skiptable_mod(&global_skiplist, key, value);
            if (response != NULL){
                if (ret == PDB_OK){
                    len = sprintf(response, "OK\r\n");
                }else if (ret == PDB_ERROR){
                    len = sprintf(response, "NO EXIST\r\n");
                }
            }
            pdb_decre_value(value);
            break;

        case PDB_CMD_SKEXIST:
            ret = pdb_rbtree_exist(&global_rbtree, key);
            if (response != NULL){
                if (ret == 0){
                    len = sprintf(response, "EXIST\r\n");
                } else{
                    len = sprintf(response, "NO EXIST\r\n");
                }
            } 
            break;   

        case PDB_CMD_SKMGET:
            len = 0;
            for (i = 1; i < count; i++){   
                char* key = tokens[i];
                value = pdb_skiptable_search(&global_skiplist, key);
                value_get = pdb_parse_value_to_string(value);
                if (response != NULL){
                    if (value_get == NULL) {
                        len += sprintf(response + len, "NO EXIST\r\n");
                    } else {
                        len += sprintf(response + len, "%s\r\n", value_get);
                    }
                }
            }
            break;
            
        case PDB_CMD_SKMSET:
            ret = pdb_skiptable_mset(&global_skiplist, tokens, count - 1);
            if (response != NULL){
                if (ret == PDB_MALLOC_NULL){
                    len = sprintf(response, "MEMORY EXCEEDS MAX_MEMORY\r\n");
                } else if (ret < 0){
                    len = sprintf(response, "ERROR\r\n");
                } else if (ret == PDB_OK){
                    len = sprintf(response, "OK\r\n");
                }
            }
            break;
       
            
        case PDB_CMD_NSAVE:
            global_dump.is_aof = 0;
            break;

        case PDB_CMD_SAVE:
            if (global_conf.is_aof){
                // AOF dump
                global_dump.is_aof = 1;
                pdb_init_aof();
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
                    if (response != NULL)   len = sprintf(response, "OK\r\n");
                }else{
                    perror("fork failed");
                    if (response != NULL)   len = sprintf(response, "SAVE FAILED\r\n");
                }
                break;
            }

        case PDB_CMD_SYN:
            return -9999;

        case PDB_CMD_EXIT:
            return -99;

        default:
            if (response != NULL){
                ret = sprintf(response, "cmd error\r\n");
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
int pdb_protocol(char* msg, int length, char* out){
    assert(msg != NULL && length > 0);

    char* rmsg = (char*)malloc(length);
    if (rmsg == NULL){
        pdb_log_error("malloc error\n");
    }
    memcpy(rmsg, msg, length);

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

    // allocate `tokens` based on `count`(the num of tokens)
    char** tokens;
    if (count < 3){
        char* p_tokens[3];
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
    int ret = pdb_filter_protocol(tokens, count, out);
    
    if (count >= 3){
        pdb_free(tokens, -1);
    }

    free(rmsg);
    return ret;
}

