#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define ARRAY_NUM       100
#define BATCH_NUM       1
#define BATCH_SIZE      100
#define MSET_NUM        100

// ./pdb_hiredis server_ip server_port
int main(int argc, char* argv[]){
    if (argc != 3){
        printf("error param\n");
        return -1;
    }
    unsigned short port = (unsigned short)atoi(argv[2]);
    const char* ip = argv[1];

    redisContext *c = redisConnect(ip, port);
    if (c == NULL || c->err) {
        if (c) {
            printf("connect error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("cannot alloc redisContext\n");
        }
        return 1;
    }

    printf("connect pdb successfully\n");
    
    int i = 0;
    int j = 0;
    srand(time(NULL));

// array test  
    // set and get
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "SET key_%d value_%d", j, j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    // get
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "GET key_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "value_%d", j);
                if (strcmp(reply->str, buf) != 0){
                    printf("set and get: %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // mod and set
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "MOD key_%d value_%d", j, (i + 1) * j * 2);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "OK");
                if (strcmp(reply->str, buf) != 0){
                    printf("mod and set, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "GET key_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "value_%d", (i + 1) * j * 2);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "EXIST key_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("EXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // del and exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "DEL key_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "EXIST key_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "NO EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("DEL AND EXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    // mset
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "MSET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "amk_%d amv_%d ", (i + 1) * j, (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply *reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            char buf[64];
            sprintf(buf, "OK");
            if (strcmp(reply->str, buf) != 0){
                printf("MSET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                printf("expected: value_%d\n", j);
                printf("receive: %s\n", reply->str);
                exit(-1);
            }
            freeReplyObject(reply);
        }
    }
    // mget
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "MGET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "amk_%d ", (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply* reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            size_t k = 0;
            for (k = 0; k < reply->elements; k++){
                redisReply* sub_reply = reply->element[k];
                char buf[64];
                sprintf(buf, "amv_%d", (int)((i + 1)*k));
                if (strcmp(sub_reply->str, buf) != 0){
                    printf("MGET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: amv_%d\n", (int)((i + 1)*k));
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
            }
            freeReplyObject(reply);
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "DEL amk_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    printf("array test successfully\n");
// array end


/********************************************************************/
/*********************** rbtree test ********************************/
/********************************************************************/
    i = 0;
    j = 0;
    // set and get
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RSET rkey_%d rvalue_%d", j, j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RGET rkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "rvalue_%d", j);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // mod and set
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RMOD rkey_%d rvalue_%d", j, (i + 1) * j * 2);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "OK");
                if (strcmp(reply->str, buf) != 0){
                    printf("rmod and rset, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RGET rkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "rvalue_%d", (i + 1) * j * 2);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "REXIST rkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("REXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // del and exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RDEL rkey_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "REXIST rkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "NO EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("RDEL AND REXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    // mset
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "RMSET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "ramk_%d ramv_%d ", (i + 1) * j, (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply *reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            char buf[64];
            sprintf(buf, "OK");
            if (strcmp(reply->str, buf) != 0){
                printf("RMSET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                printf("expected: value_%d\n", j);
                printf("receive: %s\n", reply->str);
                exit(-1);
            }
            freeReplyObject(reply);
        }
    }
    // mget
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "RMGET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "ramk_%d ", (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply* reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            size_t k = 0;
            for (k = 0; k < reply->elements; k++){
                redisReply* sub_reply = reply->element[k];
                char buf[64];
                sprintf(buf, "ramv_%d", (int)((i + 1)*k));
                if (strcmp(sub_reply->str, buf) != 0){
                    printf("RMGET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: amv_%d\n", (int)((i + 1)*k));
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
            }
            freeReplyObject(reply);
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "RDEL ramk_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    printf("rbtree test successfully\n");

/********************************************************************/
/**************************** hash test *****************************/
/********************************************************************/
    i = 0;
    j = 0;
    // HSET and HGET
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HSET hkey_%d hvalue_%d", j, j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HGET hkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "hvalue_%d", j);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // mod and set
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HMOD hkey_%d hvalue_%d", j, (i + 1) * j * 2);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "OK");
                if (strcmp(reply->str, buf) != 0){
                    printf("hmod and hset, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: hvalue_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HGET hkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "hvalue_%d", (i + 1) * j * 2);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: hvalue_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HEXIST hkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("HEXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: EXIST\n");
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // del and exist
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HDEL hkey_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HEXIST hkey_%d", j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[64];
                sprintf(buf, "NO EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("HDEL AND HEXIST, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: NO EXIST\n");
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    // mset
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "HMSET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "hamk_%d hamv_%d ", (i + 1) * j, (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply *reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            char buf[64];
            sprintf(buf, "OK");
            if (strcmp(reply->str, buf) != 0){
                printf("HMSET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                printf("expected: OK");
                printf("receive: %s\n", reply->str);
                exit(-1);
            }
            freeReplyObject(reply);
        }
    }
    // mget
    for (i = 0; i < BATCH_NUM; i++){
        char mset_buf[4096] = {0};
        int mset_buf_write_len = 0;
        mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "HMGET ");
        for (j = 0; j < BATCH_SIZE; j++){
            mset_buf_write_len += sprintf(mset_buf + mset_buf_write_len, "hamk_%d ", (i + 1) * j);
        }
        redisAppendCommand(c, mset_buf);

        redisReply* reply;
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            size_t k = 0;
            for (k = 0; k < reply->elements; k++){
                redisReply* sub_reply = reply->element[k];
                char buf[64];
                sprintf(buf, "hamv_%d", (int)((i + 1)*k));
                if (strcmp(sub_reply->str, buf) != 0){
                    printf("RMGET, %d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: hamv_%d\n", (int)((i + 1)*k));
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
            }
            freeReplyObject(reply);
        }
    }
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "HDEL hamk_%d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    printf("hash test successfully\n");

/********************************************************************/
/**************************** bitmap test ***************************/
/********************************************************************/
    i = 0;
    j = 0;
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "BITSET bitset %d 1", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "BITGET bitset %d", (i + 1) * j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[16];
                sprintf(buf, "%d", 1);
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: 1\n");
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // bit count 
    redisAppendCommand(c, "BITCOUNT bitset");
    redisReply* reply;
    if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
        char buf[16];
        sprintf(buf, "%d", BATCH_NUM * BATCH_SIZE);
        if (strcmp(reply->str, buf) != 0){
            printf("############ error ##############\n");
            printf("BITCOUNT\n");
            printf("expected: %d\n", BATCH_NUM * BATCH_SIZE);
            printf("receive: %s\n", reply->str);
            exit(-1);
        }
        freeReplyObject(reply);
    }
    // bitop
    int bitop_bits = 1000;
    
    int *shadow_k1  = (int*)malloc(bitop_bits * sizeof(int));
    int *shadow_k2  = (int*)malloc(bitop_bits * sizeof(int));
    int *shadow_and = (int*)malloc(bitop_bits * sizeof(int));
    int *shadow_or  = (int*)malloc(bitop_bits * sizeof(int));
    int *shadow_xor = (int*)malloc(bitop_bits * sizeof(int));
    int *shadow_not = (int*)malloc(bitop_bits * sizeof(int));

    for (int k = 0; k < bitop_bits; k++) {
        shadow_k1[k] = rand() % 2;
        shadow_k2[k] = rand() % 2;
        
        shadow_and[k] = shadow_k1[k] & shadow_k2[k];
        shadow_or[k]  = shadow_k1[k] | shadow_k2[k];
        shadow_xor[k] = shadow_k1[k] ^ shadow_k2[k];
        shadow_not[k] = (!shadow_k1[k]) & 1; 
        
        redisAppendCommand(c, "BITSET bitop_k1 %d %d", k, shadow_k1[k]);
        redisAppendCommand(c, "BITSET bitop_k2 %d %d", k, shadow_k2[k]);
    }
    
    for (int k = 0; k < bitop_bits * 2; k++) {
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) freeReplyObject(reply);
    }

    const char *ops[] = {"AND", "OR", "XOR", "NOT"};
    const char *res_keys[] = {"res_and", "res_or", "res_xor", "res_not"};
    
    redisAppendCommand(c, "BITOP AND res_and bitop_k1 bitop_k2");
    redisAppendCommand(c, "BITOP OR res_or bitop_k1 bitop_k2");
    redisAppendCommand(c, "BITOP XOR res_xor bitop_k1 bitop_k2");
    redisAppendCommand(c, "BITOP NOT res_not bitop_k1");

    for (int i = 0; i < 4; i++) {
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            if (strcmp(reply->str, "OK") != 0) {
                printf("BITOP %s error: %s\n", ops[i], reply->str ? reply->str : "unknown");
                exit(-1);
            }
            freeReplyObject(reply);
        }
    }
    int *shadows[] = {shadow_and, shadow_or, shadow_xor, shadow_not};
    
    for (int i = 0; i < 4; i++) {
        // bitget
        for (int k = 0; k < bitop_bits; k++) {
            redisAppendCommand(c, "BITGET %s %d", res_keys[i], k);
        }
        // verify result
        for (int k = 0; k < bitop_bits; k++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[16];
                sprintf(buf, "%d", shadows[i][k]);
                if (strcmp(reply->str, buf) != 0) {
                    printf("############ BITOP %s verify error ##############\n", ops[i]);
                    printf("Offset: %d\n", k);
                    printf("k1: %d, (k2: %d)\n", shadow_k1[k], shadow_k2[k]);
                    printf("expected (Local %s): %d\n", ops[i], shadows[i][k]);
                    printf("receive (Server): %lld\n", reply->integer);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    free(shadow_k1); free(shadow_k2);
    free(shadow_and); free(shadow_or);
    free(shadow_xor); free(shadow_not);

    printf("bitset test successfully\n");

    


    // set test
    i = 0;
    j = 0;
    // SSET
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "SSET set %d", (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }
    // SEXIST
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "SEXIST set %d", (i + 1) * j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[16];
                sprintf(buf, "EXIST");
                if (strcmp(reply->str, buf) != 0){
                    printf("#######################\n");
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: value_%d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }
    // SDEL
    redisAppendCommand(c, "SDEL set %d", 23);
    redisGetReply(c, (void**)&reply);
    redisAppendCommand(c, "SEXIST set 23");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        if (strcmp("NO EXIST", reply->str) != 0){
            printf("#######################\n");
            printf("SDEL ERROR\n");
            printf("expected: NO EXIST");
            printf("receive: %s\n", reply->str);
        }
        freeReplyObject(reply);
    }

    // SCARD
    redisAppendCommand(c, "SCARD set");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        char buf[64] = {0};
        sprintf(buf, "%d", 99);
        if (strcmp(buf, reply->str) != 0){
            printf("#######################\n");
            printf("SCARD ERROR\n");
            printf("expected: 99\n");
            printf("receive: %s\n", reply->str);
        }
        freeReplyObject(reply);
    }
    

    // SRANDOMPOP
    redisAppendCommand(c, "SRANDOMPOP set");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        printf("SET POP: %s\n", reply->str);
    }
    char* pop = malloc(strlen(reply->str));
    strcpy(pop, reply->str);
    freeReplyObject(reply);

    redisAppendCommand(c, "SCARD set");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        char buf[64] = {0};
        sprintf(buf, "%d", 98);
        if (strcmp(buf, reply->str) != 0){
            printf("#######################\n");
            printf("SPOP ERROR\n");
            printf("expected: 98\n");
            printf("receive: %s\n", reply->str);
        }
        freeReplyObject(reply);
    }
    redisAppendCommand(c, "SEXIST set %s", pop);
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        if (strcmp("NO EXIST", reply->str) != 0){
            printf("#######################\n");
            printf("SPOP ERROR\n");
            printf("expected: NO EXIST\n");
            printf("receive: %s\n", reply->str);
        }
        freeReplyObject(reply);
    }
    free(pop);


    // SNRANDOMPOP
    redisAppendCommand(c, "SNRANDOMPOP set %d", 20);
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        size_t k = 0;
        printf("npop result: \n");
        for (k = 0; k < reply->elements; k++){
            if (reply->element[k]->type == REDIS_REPLY_STRING){
                printf("%s ", reply->element[k]->str);
            }
        }
        printf("\n");
    }

    // set operation
    for (int k = 1; k <= 5; k++) {
        redisAppendCommand(c, "SSET set_a %d", k);
    }
    printf("set_a: 1, 2, 3, 4, 5\n");
    for (int k = 4; k <= 8; k++) {
        redisAppendCommand(c, "SSET set_b %d", k);
    }
    printf("set_b: 4, 5, 6, 7, 8\n");

    for (int k = 0; k < 10; k++) {
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) freeReplyObject(reply);
    }
    const char *set_ops[] = {"SINTER", "SUNION", "SDIFFER"};
    int expected_counts[] = {2, 8, 3}; 

    for (int i = 0; i < 3; i++) {        
        redisAppendCommand(c, "%s set_a set_b", set_ops[i]);
        if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
            
            if (reply->type == REDIS_REPLY_ARRAY) {
                if (reply->elements != expected_counts[i]) {
                    printf("############ %s Count Mismatch ##############\n", set_ops[i]);
                    printf("Expected elements: %d\n", expected_counts[i]);
                    printf("Received elements: %zu\n", reply->elements);
                    exit(-1);
                }

                printf("  Result correctly contains %zu elements: { ", reply->elements);
                for (size_t k = 0; k < reply->elements; k++) {
                    if (reply->element[k]->type == REDIS_REPLY_STRING) {
                        printf("%s ", reply->element[k]->str);
                    }
                }
                printf("}\n");
            }else{
                printf("#############################\n");
                printf("%s error\n", set_ops[i]);
                printf("receive: %s\n", reply->str);
            }
            freeReplyObject(reply);
        }
    }
    

    printf("set test successfully\n");
    
    // sorted set test
    i = 0;
    j = 0;
    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "SSADD sset sset:%d %d", (i + 1) * j, (i + 1) * j);
        }

        redisReply *reply;
        for (j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                freeReplyObject(reply);
            }
        }
    }

    for (i = 0; i < BATCH_NUM; i++){
        for (j = 0; j < BATCH_SIZE; j++){
            redisAppendCommand(c, "SSCORE sset sset:%d", (i + 1) * j);
        }

        redisReply *reply;
        for (int j = 0; j < BATCH_SIZE; j++) {
            if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
                char buf[16];
                sprintf(buf, "%f", (double)((i + 1) * j));
                if (strcmp(reply->str, buf) != 0){
                    printf("%d/%d(BATCH: %d)\n", j, BATCH_SIZE, i);
                    printf("expected: %d\n", j);
                    printf("receive: %s\n", reply->str);
                    exit(-1);
                }
                freeReplyObject(reply);
            }
        }
    }

    // SSINCRBY
    double incr_val = 10.5;
    redisAppendCommand(c, "SSINCRBY sset sset:1 %f", incr_val);
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        freeReplyObject(reply);
    }

    redisAppendCommand(c, "SSCORE sset sset:1");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        char buf[64];
        sprintf(buf, "%f", 11.5);
        if (strcmp(buf, reply->str) != 0){
            printf("sorted set SSINCRBY error\n");
            printf("expected: %f\n", 11.5);
            printf("receive: %s\n", reply->str);   
        }
        freeReplyObject(reply);
    }

    // SSRANK
    redisAppendCommand(c, "SSRANK sset sset:1");
    if (redisGetReply(c, (void**)&reply) == REDIS_OK){
        char buf[64];
        sprintf(buf, "%d", 12);
        if (strcmp(reply->str, buf) != 0){
            printf("sorted set SSRANK error\n");
            printf("expected: 12\n");
            printf("receive: %s\n", reply->str);
        }
    }

    // ZRANGE
    int start_idx = 0;
    int stop_idx = 5;
    redisAppendCommand(c, "SSRANGE sset %d %d", start_idx, stop_idx);

    if (redisGetReply(c, (void**)&reply) == REDIS_OK) {
        size_t k = 0;
        if (reply->type == REDIS_REPLY_ERROR){
            printf("error command\n");
        }
        int rankd = 99;
        for (k = 0; k < reply->elements; k++){
            if (reply->element[k]->type == REDIS_REPLY_STRING) {
                char buf[64] = {0};
                sprintf(buf, "sset:%d", rankd);
                rankd--;
                if(strcmp(buf, reply->element[k]->str) != 0){
                    printf("ssrange error\n");
                    printf("expected: %s\n", buf);
                    printf("receive: %s\n", reply->element[k]->str);
                }
            }
        }
    }

    printf("sorted set test successfully\n");
    return 0;
}
