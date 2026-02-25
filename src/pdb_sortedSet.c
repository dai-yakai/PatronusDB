#include "pdb_sortedSet.h"

/**
 * Deep string copy
 */
static char* _pdb_strdup(char* str){
    size_t len = strlen(str);
    char* new_str = pdb_malloc(len + 1);
    strcpy(new_str, str);
    new_str[len] = '\0';

    return new_str;
}


/**
 * SKIPLIST
 */
static int _pdb_random_level(){
    int level = 1;
    while ((rand() & 0xFFFF) < (PDB_ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < PDB_MAX_SKIPLIST_LEVEL) ? level : PDB_MAX_SKIPLIST_LEVEL;
}

static struct pdb_skiplistNode* _pdb_create_node(int level, KEY_TYPE key, VALUE_TYPE value){
    struct pdb_skiplistNode* node = (struct pdb_skiplistNode*)pdb_malloc(sizeof(struct pdb_skiplistNode) + level * sizeof(struct pdb_skiplistLevel));
    node->value = value;
    node->key = _pdb_strdup(key);

    return node;
}

static void _pdb_destroy_node(struct pdb_skiplistNode* node){
    pdb_free(node->key, -1);
    pdb_free(node, -1);
}

struct pdb_skiplist* pdb_create_skiplist(){
    struct pdb_skiplist* sl = (struct pdb_skiplist*)pdb_malloc(sizeof(struct pdb_skiplist));
    assert(sl != NULL);
    // dummyNode
    sl->head = _pdb_create_node(PDB_MAX_SKIPLIST_LEVEL, "\0", 0);
    sl->tail = NULL;
    sl->level = 1;
    sl->count = 0;
    int i;
    for (i = 0; i < PDB_MAX_SKIPLIST_LEVEL; i++) {
        sl->head->level[i].forward = NULL;
        sl->head->level[i].span = 0;
    }
    sl->head->backward = NULL;

    return sl;
}


int pdb_skiplist_add(struct pdb_skiplist* sl, KEY_TYPE key, VALUE_TYPE value){
    struct pdb_skiplistNode* update[PDB_MAX_SKIPLIST_LEVEL];
    unsigned int rank[PDB_MAX_SKIPLIST_LEVEL];

    struct pdb_skiplistNode* node = sl->head;
    int i;

    for (i = sl->level - 1; i >= 0; i--){
        rank[i] = i == sl->level - 1 ? 0 : rank[i + 1];

        while(node->level[i].forward != NULL && 
             (node->level[i].forward->value < value || 
             (node->level[i].forward->value == value && strcmp(node->level[i].forward->key, key) < 0)))
        {
            rank[i] += node->level[i].span;
            node = node->level[i].forward;
        }

        update[i] = node;
    }

    int new_level = _pdb_random_level();
    if (new_level > sl->level){
        for (i = sl->level; i < new_level; i++){
            update[i] = sl->head;
            rank[i] = 0;
            update[i]->level[i].span = sl->count;
        }
        sl->level = new_level;
    }

    node = _pdb_create_node(new_level, key, value);
    for (i = 0; i < new_level; i++){
        node->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = node;

        node->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    for (i = new_level; i < sl->level; i++){
        update[i]->level[i].span++;
    }

    node->backward = (update[0] == sl->head) ? NULL : update[0];
    
    if (node->level[0].forward) {
        node->level[0].forward->backward = node;
    } else {
        sl->tail = node;
    }

    sl->count++;
    
    return 1; 
}


static struct pdb_skiplistNode* _pdb_search_skiplist_node(struct pdb_skiplist* sl, KEY_TYPE key, VALUE_TYPE value){
    if (sl->count == 0) return 0;

    struct pdb_skiplistNode* node = sl->head;
    int i;
    for (i = sl->level - 1; i >= 0; i--){
        while(node->level[i].forward != NULL && 
             (node->level[i].forward->value < value || 
             (node->level[i].forward->value == value && strcmp(node->level[i].forward->key, key) < 0)))
        {
            node = node->level[i].forward;
        }
    }

    node = node->level[0].forward;
    if (strcmp(node->key, key) == 0 && value == node->value){
        return node;
    }

    return NULL;
}


int pdb_skiplist_delete(struct pdb_skiplist* sl, KEY_TYPE key, VALUE_TYPE value){
    if (sl->count == 0) return 0;

    struct pdb_skiplistNode* node = NULL;
    struct pdb_skiplistNode* pre_node = NULL;
    int i;

    struct pdb_skiplistNode* update[PDB_MAX_SKIPLIST_LEVEL];

    node = sl->head;
    for (i = sl->level - 1; i >= 0; i--){
        while(node->level[i].forward != NULL && 
             (node->level[i].forward->value < value || 
             (node->level[i].forward->value == value && strcmp(node->level[i].forward->key, key) < 0)))
        {
            node = node->level[i].forward;
        }

        update[i] = node;
    }

    node = node->level[0].forward;
    if (node != NULL && strcmp(node->key, key) == 0 && node->value == value){
        for (i = 0; i < sl->level; i++){
            if (update[i]->level[i].forward == node){
                update[i]->level[i].forward = node->level[i].forward;
                update[i]->level[i].span = update[i]->level[i].span + node->level[i].span - 1;
            }else{
                update[i]->level[i].span -= 1;
            }
        }

        if (node->level[0].forward != NULL) {
            node->level[0].forward->backward = node->backward;
        } else {
            sl->tail = node->backward;
        }

        while (sl->level > 1 && sl->head->level[sl->level - 1].forward == NULL) {
            sl->level--;
        }

        sl->count--;
        _pdb_destroy_node(node);

        return 1;
    }

    return 0;
}


void pdb_skiplist_destroy(struct pdb_skiplist* sl){
    struct pdb_skiplistNode* node;
    struct pdb_skiplistNode* next;

    node = sl->head;
    while(node != NULL){
        next = node->level[0].forward;
        _pdb_destroy_node(node);
        node = next;
    }

    pdb_free(sl, -1);
}

/**
 * sorted set
 */

struct pdb_sorted_set* pdb_create_sortedSet(){
    struct pdb_sorted_set* Sset = (struct pdb_sorted_set*)pdb_malloc(sizeof(struct pdb_sorted_set));
    Sset->set = pdb_hash_create2();
    Sset->list = pdb_create_skiplist();

    return Sset;
}

void pdb_destroy_sortedSet(struct pdb_sorted_set* Sset){
    pdb_hash_destory(Sset->set);
    pdb_skiplist_destroy(Sset->list);
    pdb_free(Sset, -1);
}

/**
 * Return len of buf
 */
static int _pdb_double_to_str(double value, char* buf){
    int len = snprintf(buf, 64, "%.6f", value);
    return len;
}

int pdb_sortedSet_add(struct pdb_sorted_set* Sset, char* key, double value){
    char value_buf[64] = {0};
    _pdb_double_to_str(value, value_buf);

    char* value_hash = pdb_hash_get(Sset->set, key);
    if (value_hash != NULL){
        // exist
        if (strcmp(value_hash, value_buf) == 0){
            return 1;
        }else{
            double old_value = atof(value_hash);

            pdb_hash_mod(Sset->set, key, value_buf);
            pdb_skiplist_delete(Sset->list, key, old_value);
            pdb_skiplist_add(Sset->list, key, value);
        }
    }else{
        pdb_hash_set(Sset->set, key, value_buf);
        pdb_skiplist_add(Sset->list, key, value);
    }

    return 1;
}

int pdb_sortedSet_delete(struct pdb_sorted_set* Sset, char* key){
    char* value = pdb_hash_get(Sset->set, key);
    if (value == NULL)  return 0;

    double skiplist_value = atof(value);
    pdb_hash_del(Sset->set, key);
    pdb_skiplist_delete(Sset->list, key, skiplist_value);

    return 1;
}

double pdb_sortedSet_search(struct pdb_sorted_set* Sset, char* key, int* success){
    char* value = pdb_hash_get(Sset->set, key);
    if (value == NULL){
        if (success != NULL)    *success = 0;
        return 0;
    } 

    if (success != NULL){
        *success = 1;
    }
    double skiplist_value = atof(value);
    return skiplist_value;
}

unsigned long pdb_sortedSet_rank(struct pdb_sorted_set* Sset, char* key, int* success){
    unsigned long rank = 0;
    double value = pdb_sortedSet_search(Sset, key, success);
    if (*success == 0)   return 0;

    struct pdb_skiplistNode* node = Sset->list->head;
    int i;
    for (i = Sset->list->level - 1; i >= 0; i--){
        while(node->level[i].forward != NULL && 
             (node->level[i].forward->value < value || 
             (node->level[i].forward->value == value && strcmp(node->level[i].forward->key, key) < 0)))
        {
            rank += node->level[i].span;
            node = node->level[i].forward;
        }
    }

    node = node->level[0].forward;
    rank++;
    if (node != NULL && strcmp(node->key, key) == 0 && node->value == value){
        return rank;
    }

    if (success != NULL)    *success = 0;
    return 0;
}

unsigned long pdb_sortedSet_revrank(struct pdb_sorted_set* Sset, char* key, int* success){
    unsigned long res = pdb_sortedSet_rank(Sset, key, success);
    if (success != NULL && *success == 0)   return 0;

    return Sset->list->count - res + 1;
}

static struct pdb_skiplistNode* _pdb_sortedSet_get_node_by_rank(struct pdb_sorted_set* Sset, unsigned long rank){
    unsigned long cross = 0;
    int i;
    struct pdb_skiplistNode* node = Sset->list->head;

    for (i = Sset->list->level - 1; i >= 0; i--){
        while(node->level[i].forward != NULL && (cross + node->level[0].span) <= rank){
            cross += node->level[i].span;
            node = node->level[i].forward;
        }
    }
    
    if (cross == rank){
        return node;
    }

    return NULL;
}


/**
 * Return [start, end]
 * If start/end < 0
 */
char** pdb_sortedSet_get_revrange(struct pdb_sorted_set* Sset, long start, long end){
    int count = Sset->list->count;
    
    if (start < 0)  start += count;
    if (end < 0)    end += count;
    if (start > end || start >= count || end >= count)    return NULL;

    long range = end - start + 1;
    long start_ = count - start;
    struct pdb_skiplistNode* node = _pdb_sortedSet_get_node_by_rank(Sset, start_);

    char** res = (char**)pdb_malloc(range * sizeof(char*));
    int i;
    for (i = 0; i < range; i++){
        res[i] = node->key;
        node = node->backward;
    }

    return res;
}

char** pdb_sortedSet_topK(struct pdb_sorted_set* Sset, unsigned long k){
    return pdb_sortedSet_get_revrange(Sset, 0, k - 1);
}

void test_correctness() {
    struct pdb_sorted_set* zs = pdb_create_sortedSet();
    int success = 0;
    
    pdb_sortedSet_add(zs, "Alice", 100.5);
    pdb_sortedSet_add(zs, "Bob", 300.0);
    pdb_sortedSet_add(zs, "Charlie", 200.0);
    pdb_sortedSet_add(zs, "David", 300.0); 
    
    // Alice(100.5) -> Charlie(200.0) -> Bob(300.0) -> David(300.0)
    
    double score = pdb_sortedSet_search(zs, "Charlie", &success);
    if (success && score == 200.0) printf("[OK] 基础查分正常.\n");
    else printf("[FAIL] 基础查分错误! %f\n", score);

    unsigned long rank = pdb_sortedSet_rank(zs, "Charlie", &success);
    if (success && rank == 2) printf("[OK] 排名计算正常 (Charlie 第 2 名).\n");
    else printf("[FAIL] 排名计算错误! 期望 2，实际 %lu\n", rank);

    rank = pdb_sortedSet_rank(zs, "David", &success);
    if (success && rank == 4) printf("[OK] 同分排名正常 (David 第 4 名).\n");
    else printf("[FAIL] 同分排名错误! 期望 4，实际 %lu\n", rank);

    pdb_sortedSet_add(zs, "Alice", 500.0); 
    score = pdb_sortedSet_search(zs, "Alice", &success);
    printf("%f\n", score);
    rank = pdb_sortedSet_rank(zs, "Alice", &success);
    if (success && rank == 4) printf("[OK] 分数更新并重排正常 (Alice 升至第 4 名).\n");
    else printf("[FAIL] 分数更新错误! 期望 4，实际 %lu\n", rank);

    pdb_sortedSet_delete(zs, "Bob");
    pdb_sortedSet_search(zs, "Bob", &success);
    if (success == 0) printf("[OK] 节点删除正常.\n");
    else printf("[FAIL] 节点删除错误!\n");

    char** tops = pdb_sortedSet_topK(zs, 2); // top 2
    if (tops != NULL && strcmp(tops[0], "Alice") == 0 && strcmp(tops[1], "David") == 0) {
        printf("[OK] pdb_sortedSet_topK 逆序获取正常.\n");
    } else {
        printf("[FAIL] TopK 获取错误!\n");
    }
    
    if (tops) pdb_free(tops, -1);
    pdb_destroy_sortedSet(zs);
    printf("阶段 1 测试完毕。\n\n");
}

#define STRESS_COUNT 100000

/*
    return (us)
*/
static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

void test_performance() {
    struct pdb_sorted_set* zs = pdb_create_sortedSet();
    
    char** keys = malloc(STRESS_COUNT * sizeof(char*));
    double* scores = malloc(STRESS_COUNT * sizeof(double));
    for (int i = 0; i < STRESS_COUNT; i++) {
        keys[i] = malloc(32);
        sprintf(keys[i], "player_%d", i);
        scores[i] = (double)(rand() % 1000000) / 100.0;
    }

    long long start;
    long long end;
    int success = 0;

    // add performence
    start = usec();
    for (int i = 0; i < STRESS_COUNT; i++) {
        pdb_sortedSet_add(zs, keys[i], scores[i]);
    }
    end = usec();
    long long time_used = end - start;
    printf("[压测] 插入 %d 条数据耗时: %lld us (QPS: %lld)\n", STRESS_COUNT, time_used, (long long)STRESS_COUNT * 1000 * 1000 / time_used);

    // search performence
    start = usec();
    for (int i = 0; i < STRESS_COUNT; i++) {
        pdb_sortedSet_search(zs, keys[i], &success);
    }
    end = usec();
    time_used = end - start;
    printf("[压测] 查询 %d 次分数耗时: %lld us (QPS: %lld)\n", STRESS_COUNT, time_used, (long long)STRESS_COUNT * 1000 * 1000 / time_used);

    // rank performence
    start = usec();
    for (int i = 0; i < STRESS_COUNT; i++) {
        pdb_sortedSet_rank(zs, keys[i], &success);
    }
    end = usec();
    time_used = end - start;
    printf("[压测] 计算 %d 次排名耗时: %lld us (QPS: %lld)\n", STRESS_COUNT, time_used, (long long)STRESS_COUNT * 1000 * 1000 / time_used);

    // topK performence
    start = usec();
    char** tops = pdb_sortedSet_topK(zs, 100);
    end = usec();
    time_used = end - start;
    printf("[压测] 获取全服 Top 100 耗时: %lld us\n", time_used);

    if (tops) pdb_free(tops, -1);
    for (int i = 0; i < STRESS_COUNT; i++) free(keys[i]);
    free(keys);
    free(scores);
    pdb_destroy_sortedSet(zs);
}