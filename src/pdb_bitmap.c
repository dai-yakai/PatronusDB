#include "pdb_bitmap.h"

/**
 * `old_value` can be NULL
 * offset >> a          <==>    offset / 2^a
 * offset & (2^n-1)     <==>    offset % 2^n 
 * e.g. if offset = 26, byte_idx = 3, bit_idx = 2
 */
int pdb_bitmap_set(pdb_sds* b, uint64_t offset, int val, int* old_value){
    size_t byte_idx = offset >> 3;
    size_t bit_idx = 7 - (offset & 7);
    size_t old_len = pdb_get_sds_len(*b);

    if (byte_idx >= pdb_get_sds_len(*b)){
        // enlarge
        if (val == 0){
            /**
             * If `val` == 0, we do not enlarge to avoid OOM atack
             * e.g. If user calls pdb_bitmap_set(b, 4294967295, 0) without detecing OOM atack, the code will allocate 4G memory to storage.
             */
            if (old_value != NULL)    *old_value = 0;
            return 0;
        }   
        /**
         * +1: If byte_idx = 0 and pdb_get_sds_len(b) = 0, add_size will equal 0.
         * We must add 1 to avoid add_size = 0
         */
        size_t add_size = byte_idx - pdb_get_sds_len(*b) + 1;
        *b = pdb_enlarge_sds_greedy(*b, add_size);
        memset(*b + old_len, 0, pdb_get_sds_alloc(*b) - old_len);
        pdb_set_sds_len(*b, byte_idx + 1);
    }

    if (old_value != NULL){
        *old_value = ((*b)[byte_idx] >> bit_idx) & 1;
    }
    
    if (val){
        (*b)[byte_idx] |= (1 << bit_idx);
    }else{
        (*b)[byte_idx] &= ~(1 << bit_idx);
    }

    return PDB_OK;
}

int pdb_bitmap_get(pdb_sds b, uint64_t offset){
    size_t byte_idx = offset >> 3;
    size_t bit_idx = 7 - (offset & 7);

    if (byte_idx >= pdb_get_sds_len(b))  return 0;

    return (b[byte_idx] >> bit_idx) & 1;
}

int pdb_bitmap_count(pdb_sds b){
    uint64_t count = 0;
    size_t len = pdb_get_sds_len(b);
    unsigned char* p = (unsigned char*)b;

    while(len >= 8){
        uint64_t val;
        memcpy(&val, p, sizeof(uint64_t));
        count += __builtin_popcountll(val);
        p += 8;
        len -= 8;
    }

    while(len > 0){
        count += __builtin_popcount(*p);
        p++;
        len--;
    }

    return count;
}

/**
 * According to opt, the function will operate src1 & src2、
 *     src1 | src2、src1 ^ src2
 * 
 */
void pdb_bitmap_bitop(int opt, pdb_sds* dest, pdb_sds src1, pdb_sds src2){
    size_t len1 = pdb_get_sds_len(src1);
    size_t len2 = pdb_get_sds_len(src2);
    size_t max_len = len1 > len2 ? len1 : len2;

    size_t cur_len = pdb_get_sds_len(*dest);
    if (cur_len < max_len){
        *dest = pdb_enlarge_sds_greedy(*dest, max_len - cur_len);
    }
    // *dest = pdb_get_new_sds(max_len);
    pdb_set_sds_len(*dest, max_len);

    size_t i;
    if (opt == BITOP_AND){
        for (i = 0; i < max_len; i++){
            unsigned char b1 = i < len1 ? src1[i] : 0;
            unsigned char b2 = i < len2 ? src2[i] : 0;
            (*dest)[i] = b1 & b2;
        }
    } else if (opt == BITOP_OR){
        for (i = 0; i < max_len; i++){
            unsigned char b1 = i < len1 ? src1[i] : 0;
            unsigned char b2 = i < len2 ? src2[i] : 0;
            (*dest)[i] = b1 | b2;
        }
    } else if (opt == BITOP_XOR){
        for (i = 0; i < max_len; i++){
            unsigned char b1 = i < len1 ? src1[i] : 0;
            unsigned char b2 = i < len2 ? src2[i] : 0;
            (*dest)[i] = b1 ^ b2;
        }
    }

    (*dest)[max_len] = '\0';
}


int64_t pdb_bitmap_pos(pdb_sds b, int val, uint64_t start){
    size_t len = pdb_get_sds_len(b);
    unsigned char* p = (unsigned char*)b;
    size_t skip_bytes = start >> 3;
    
    if (skip_bytes >= len) return -1;

    unsigned char* curr = p + skip_bytes;
    size_t remaining = len - skip_bytes;
    int skip_bits = start & 7;
    if (skip_bits > 0 && remaining > 0) {
        unsigned char byte = *curr;
        unsigned char mask = (1 << (8 - skip_bits)) - 1; 
        
        unsigned char target = (val == 1) ? (byte & mask) : ((~byte) & mask);

        if (target != 0) { 
            int pos = __builtin_clz((unsigned int)target) - 24; 
            return (int64_t)((curr - p) * 8 + pos);
        }
        curr++;
        remaining--;
    }

    while (remaining >= 8) {
        uint64_t value;
        memcpy(&value, curr, sizeof(uint64_t));
        value = __builtin_bswap64(value);

        uint64_t target = (val == 1) ? value : ~value;

        if (target != 0) {
            int pos = __builtin_clzll(target);
            return (int64_t)((curr - p) * 8 + pos);
        }
        curr += 8;
        remaining -= 8;
    }

    while (remaining > 0) {
        unsigned char byte = *curr;
        unsigned char target = (val == 1) ? byte : (unsigned char)~byte;

        if (target != 0) {
            int pos = __builtin_clz((unsigned int)target) - 24;
            return (int64_t)((curr - p) * 8 + pos);
        }
        curr++;
        remaining--;
    }

    return -1; 
}


static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

void pdb_test_bitmap(){
    printf("========== PDB Bitmap test ==========\n");
    
    pdb_sds b = pdb_get_new_sds(0);
    int old_val = 0;

    pdb_bitmap_set(&b, 10, 1, &old_val);
    printf("old_value: %d\n", old_val);
    assert(old_val == 0);
    assert(pdb_bitmap_get(b, 10) == 1);
    assert(pdb_bitmap_get(b, 11) == 0); 

    pdb_bitmap_set(&b, 1000000, 1, NULL); 
    assert(pdb_bitmap_get(b, 1000000) == 1);
    
    assert(pdb_bitmap_count(b) == 2); 
    assert(pdb_bitmap_pos(b, 1, 0) == 10);
    assert(pdb_bitmap_pos(b, 1, 11) == 1000000); 
    
    printf("[OK] 功能正确性、掩码计算、自动扩容校验全部通过！\n\n");

    int STRESS_COUNT = 10000000;
    long long start, end;

    printf("--- 开始 %d 次级别极限性能压测 ---\n", STRESS_COUNT);
    
    start = usec();
    for(int i = 0; i < STRESS_COUNT; i++) {
        pdb_bitmap_set(&b, i * 2, 1, NULL); 
    }
    end = usec();
    long long set_time = end - start;
    printf("SetBit 耗时: %lld us \t(QPS: %lld/秒)\n", set_time, (long long)STRESS_COUNT * 1000000 / set_time);

    start = usec();
    int dummy = 0;
    for(int i = 0; i < STRESS_COUNT; i++) {
        dummy += pdb_bitmap_get(b, i * 2);
    }
    end = usec();
    long long get_time = end - start;
    printf("GetBit 耗时: %lld us \t(QPS: %lld/秒)\n", get_time, (long long)STRESS_COUNT * 1000000 / get_time);

    start = usec();
    int total_ones = pdb_bitmap_count(b);
    end = usec();
    printf("Count  耗时: %lld us \t(统计 %d 万总在线人数极速响应)\n", end - start, total_ones / 10000);

    pdb_sds_free(b);
    printf("========== 测试结束 ==========\n\n");
}