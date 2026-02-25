#include "pdb_intset.h"

static void pdb_print_intset(struct pdb_intset* intset){
    int len = intset->len;
    int i = 0;
    for (i = 0; i < len; i++){
        if (intset->flag == PDB_INTSET_ENCODING_INT16){
            printf("%" PRId16 " ", ((int16_t*)intset->data)[i]);
        } else if (intset->flag == PDB_INTSET_ENCODING_INT32){
            printf("%" PRId32 " ", ((int32_t*)intset->data)[i]);
        } else if (intset->flag == PDB_INTSET_ENCODING_INT64){
            printf("%" PRId64 " ", ((int64_t*)intset->data)[i]);
        }
    }
    printf("\n");
}


int64_t _pdb_intset_get(struct pdb_intset* intset, uint32_t pos){
    if (intset->flag == PDB_INTSET_ENCODING_INT16)
        return ((int16_t*)intset->data)[pos];
    if (intset->flag == PDB_INTSET_ENCODING_INT32)
        return ((int32_t*)intset->data)[pos];
    if (intset->flag == PDB_INTSET_ENCODING_INT64)
        return ((int64_t*)intset->data)[pos];
}

void _pdb_intset_set(struct pdb_intset* intset, uint32_t pos, int64_t value){
    if (intset->flag == PDB_INTSET_ENCODING_INT16)
        ((int16_t*)intset->data)[pos] = value;
    else if (intset->flag == PDB_INTSET_ENCODING_INT32)
        ((int32_t*)intset->data)[pos] = value;
    else if (intset->flag == PDB_INTSET_ENCODING_INT64)
        ((int64_t*)intset->data)[pos] = value;
}

static uint8_t _pdb_intset_val_encoding(int64_t value){
    if (value < INT32_MIN || value > INT32_MAX) return PDB_INTSET_ENCODING_INT64;
    if (value < INT16_MIN || value > INT16_MAX) return PDB_INTSET_ENCODING_INT32;
    
    return PDB_INTSET_ENCODING_INT16;
}

/**
 * Binary serach
 * If the target is found, `pos` stores its index; otherwise, it stores the insertion point.
 * Return 1 if the target is found; otherwise return 0.
 */
static uint8_t _pdb_intset_search(struct pdb_intset* intset, uint32_t* pos, int64_t value){
    int min = 0, max = intset->len - 1, mid = -1;
    int64_t cur = -1;

    if (intset->len == 0) {
        if (pos) *pos = 0;
        return 0;
    }

    if (value > _pdb_intset_get(intset, max)) {
        if (pos) *pos = intset->len;
        return 0;
    } else if (value < _pdb_intset_get(intset, 0)) {
        if (pos) *pos = 0;
        return 0;
    }

    while (max >= min) {
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
        cur = _pdb_intset_get(intset, mid);
        if (value > cur) {
            min = mid + 1;
        } else if (value < cur) {
            max = mid - 1;
        } else {
            break; 
        }
    }

    if (value == cur) {
        if (pos) *pos = mid;
        return 1;
    } else {
        if (pos) *pos = min; 
        return 0;
    }
}

static struct pdb_intset* _pdb_intset_upgrade_and_add(struct pdb_intset* intset, int64_t value){
    uint8_t cur_encoding = intset->flag;
    uint8_t new_encoding = _pdb_intset_val_encoding(value);
    int len = intset->len;

    struct pdb_intset* new_intset = pdb_malloc(sizeof(struct pdb_intset) + (len + 1) * new_encoding);
    new_intset->flag = new_encoding;
    new_intset->len = len + 1;

    // value < 0, flag = 1; value > 0, flag = 0
    int flag;
    flag = value < 0 ? 1 : 0;
    int new_len = new_intset->len;
    while(len > 0){
        int64_t old_value;
        if (cur_encoding == PDB_INTSET_ENCODING_INT16){
            old_value = ((int16_t*)(intset->data))[len - 1];
        }else if (cur_encoding == PDB_INTSET_ENCODING_INT32){
            old_value = ((int32_t*)(intset->data))[len - 1];
        }
        if (flag){
            // front
            _pdb_intset_set(new_intset, len, old_value);
            len--;
        }else{
            // end
            len--;
            _pdb_intset_set(new_intset, len, old_value);
        }
    }
    if (flag){
        // front
        _pdb_intset_set(new_intset, 0, value);
    }else{
        _pdb_intset_set(new_intset, new_intset->len - 1, value);
    }

    pdb_free(intset, -1);

    return new_intset;
}

struct pdb_intset* pdb_intset_create(){
    struct pdb_intset* intset = pdb_malloc(sizeof(struct pdb_intset));
    assert(intset != NULL);

    intset->flag = PDB_INTSET_ENCODING_INT16;
    intset->len = 0;

    return intset;
}


struct pdb_intset* pdb_intset_add(struct pdb_intset* intset, int64_t value, int* success){
    // pdb_print_intset(intset);
    uint8_t required_encoding = _pdb_intset_val_encoding(value);
    if (required_encoding > intset->flag){
        return _pdb_intset_upgrade_and_add(intset, value);
    }

    uint32_t pos = 0;
    uint8_t ret = _pdb_intset_search(intset, &pos, value);
    // exist
    if (ret) {
        if (success != NULL)    *success = 0;
        return intset;
    }   
    // enlarge
    struct pdb_intset* new_intset = (struct pdb_intset*)pdb_malloc(sizeof(struct pdb_intset) + intset->flag * (intset->len + 1));
    assert(new_intset != NULL);
    new_intset->flag = intset->flag;
    new_intset->len = intset->len + 1;
    // memmove
    memcpy(new_intset->data, intset->data, pos * new_intset->flag);
    // set new_value
    _pdb_intset_set(new_intset, pos, value);
    // memmove remaining elements
    memcpy((char*)new_intset->data + (pos + 1) * new_intset->flag, (char*)intset->data + pos * new_intset->flag, (intset->len - pos) * new_intset->flag);

    pdb_free(intset, -1);
    if (success != NULL)    *success = 1;

    return new_intset;
}

/**
 * Return 1 if found; otherwise return 0
 */
int pdb_intset_search(struct pdb_intset* intset, int64_t value, uint32_t* pos){
    if (_pdb_intset_search(intset, pos, value)){
        return 1;
    }

    return 0;
}

struct pdb_intset* pdb_intset_delete(struct pdb_intset* intset, int64_t value, int* success){
    uint32_t pos;
    if (!pdb_intset_search(intset, value, &pos)){
        if (success != NULL)    *success = 0;
        return intset;
    }
    // memmove
    int i = pos;
    struct pdb_intset* new_intset = (struct pdb_intset*)pdb_malloc(sizeof(struct pdb_intset) + intset->flag * (intset->len - 1));
    assert(new_intset != NULL);
    new_intset->len = intset->len - 1;
    new_intset->flag = intset->flag;

    memcpy(new_intset->data, intset->data, new_intset->flag * pos);
    memcpy((char*)new_intset->data + new_intset->flag * pos, (char*)intset->data + intset->flag * (pos + 1), (intset->len - 1 - pos) * intset->flag);

    pdb_free(intset, -1);
    if (success != NULL)    *success = 1;

    return new_intset;
}

void pdb_intset_destroy(struct pdb_intset* intset){
    pdb_free(intset, -1);
}


struct pdb_intset* pdb_intset_intersect(struct pdb_intset* s1, struct pdb_intset* s2){
    uint32_t len = s1->len < s2->len ? s1->len : s2->len;
    uint32_t encoding = s1->flag > s2->flag ? s1->flag : s2->flag;
    struct pdb_intset* intset = pdb_malloc(sizeof(struct pdb_intset) + len * encoding);
    intset->flag = encoding;

    uint32_t idx1 = 0;
    uint32_t idx2 = 0;
    uint32_t idx = 0;

    while(idx1 < s1->len && idx2 < s2->len){
        int64_t value1 = _pdb_intset_get(s1, idx1);
        int64_t value2 = _pdb_intset_get(s2, idx2);
        if (value1 == value2){
            _pdb_intset_set(intset, idx, value1);
            idx++;
            idx1++;
            idx2++;
        }else if (value1 < value2){
            idx1++;
        }else{
            idx2++;
        }
    }

    intset->len = idx;
    if (idx < len){
        struct pdb_intset* intset2;
        intset2 = pdb_malloc(sizeof(struct pdb_intset) + intset->len * intset->flag);
        memcpy(intset2->data, intset->data, intset->len * intset->flag);
        intset2->len = idx;
        intset2->flag = intset->flag;
        pdb_free(intset, -1);

        return intset2;
    }
    
    return intset;
}


struct pdb_intset* pdb_intset_union(struct pdb_intset* s1, struct pdb_intset* s2){
    uint32_t encoding = s1->flag > s2->flag ? s1->flag : s2->flag;
    uint32_t len = s1->len + s2->len;
    struct pdb_intset* intset = pdb_malloc(sizeof(struct pdb_intset) + len * encoding);
    assert(intset != NULL);
    intset->flag = encoding;

    uint32_t idx1 = 0;
    uint32_t idx2 = 0;
    uint32_t idx = 0;
    while(idx1 < s1->len && idx2 < s2->len){
        int64_t value1 = _pdb_intset_get(s1, idx1);
        int64_t value2 = _pdb_intset_get(s2, idx2);
        if (value1 == value2){
            _pdb_intset_set(intset, idx, value1);
            idx1++;
            idx2++;
            idx++;
        }else if (value1 < value2){
            _pdb_intset_set(intset, idx, value1);
            idx++;
            idx1++;
        }else {
            _pdb_intset_set(intset, idx, value2);
            idx++;
            idx2++;
        }
    }
    if (idx1 == s1->len){
        while(idx2 < s2->len){
            int64_t value2 = _pdb_intset_get(s2, idx2);
            _pdb_intset_set(intset, idx, value2);
            idx2++;
            idx++;
        }
    }
    if (idx2 == s2->len){
        while(idx1 < s1->len){
            int64_t value1 = _pdb_intset_get(s1, 1);
            _pdb_intset_set(intset, idx, value1);
            idx1++;
            idx++;
        }
    }

    intset->len = idx;
    if (idx < len){
        struct pdb_intset* intset2;
        intset2 = pdb_malloc(sizeof(struct pdb_intset) + intset->len * intset->flag);
        memcpy(intset2->data, intset->data, intset->len * intset->flag);
        intset2->len = idx;
        intset2->flag = intset->flag;
        pdb_free(intset, -1);

        return intset2;
    }

    return intset;
}

/**
 * s1 - s2
 */
struct pdb_intset* pdb_intset_difference(struct pdb_intset* s1, struct pdb_intset* s2){
    struct pdb_intset* intset = pdb_malloc(sizeof(struct pdb_intset) + s1->len * s1->flag);
    assert(intset != NULL);

    uint32_t len = s1->len;

    uint32_t idx1 = 0;
    uint32_t idx2 = 0;
    uint32_t idx = 0;
    while(idx1 < s1->len){
        int64_t value1 = _pdb_intset_get(s1, idx1);
        int64_t value2 = _pdb_intset_get(s2, idx2);
        if (value1 < value2){
            _pdb_intset_set(intset, idx, value1);
            idx++;
            idx1++;
        } else if (value1 == value2){
            idx1++;
            idx2++;
        } else{
            idx2++;
        }
    }

    intset->len = idx;

    if (idx < len){
        struct pdb_intset* intset2;
        intset2 = pdb_malloc(sizeof(struct pdb_intset) + intset->len * intset->flag);
        memcpy(intset2->data, intset->data, intset->len * intset->flag);
        intset2->len = idx;
        intset2->flag = intset->flag;
        pdb_free(intset, -1);

        return intset2;
    }

    return intset;
}

//test.................................
static void ok(void) {
    printf("OK\n");
}

static void check(struct pdb_intset* intset){
    uint32_t len = intset->len;
    uint32_t i = 0;
    int64_t value;
    for (i = 0; i < len - 1; i++){
        if (intset->flag == PDB_INTSET_ENCODING_INT16){
            int16_t* i16 = ((int16_t*)intset->data);
            assert(i16[i] < i16[i + 1]);
        } else if (intset->flag == PDB_INTSET_ENCODING_INT32){
            int32_t* i32 = ((int32_t*)intset->data);
            assert(i32[i] < i32[i + 1]);
        } else if (intset->flag == PDB_INTSET_ENCODING_INT64){
            int64_t* i64 = ((int64_t*)intset->data);
            assert(i64[i] < i64[i + 1]);
        }
    }
}

static struct pdb_intset* create_set(int bits, int size){
    uint64_t mask = (1 << bits) - 1;
    int64_t value;
    struct pdb_intset* intset;
    int i = 0;
    intset = pdb_intset_create();
    for (i = 0; i < size; i++){   
        if (bits > 32){
            int64_t value = (rand() * rand()) & mask;  
            intset = pdb_intset_add(intset, value, NULL);
            // pdb_print_intset(intset);
        }else{
            int64_t value = rand() & mask;
            // pdb_print_intset(intset);
            intset = pdb_intset_add(intset, value, NULL);
        }
    }

    return intset;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

void pdb_intset_test(){
    uint8_t success;
    int i;
    struct pdb_intset *is;
    struct pdb_intset *is2;
    srand(time(NULL));

    printf("Value encodings: "); {
        assert(_pdb_intset_val_encoding(-32768) == PDB_INTSET_ENCODING_INT16);
        assert(_pdb_intset_val_encoding(+32767) == PDB_INTSET_ENCODING_INT16);
        assert(_pdb_intset_val_encoding(-32769) == PDB_INTSET_ENCODING_INT32);
        assert(_pdb_intset_val_encoding(+32768) == PDB_INTSET_ENCODING_INT32);
        assert(_pdb_intset_val_encoding(-2147483648) == PDB_INTSET_ENCODING_INT32);
        assert(_pdb_intset_val_encoding(+2147483647) == PDB_INTSET_ENCODING_INT32);
        assert(_pdb_intset_val_encoding(-2147483649) == PDB_INTSET_ENCODING_INT64);
        assert(_pdb_intset_val_encoding(+2147483648) == PDB_INTSET_ENCODING_INT64);
        assert(_pdb_intset_val_encoding(-9223372036854775808ull) ==
                    PDB_INTSET_ENCODING_INT64);
        assert(_pdb_intset_val_encoding(+9223372036854775807ull) ==
                    PDB_INTSET_ENCODING_INT64);
        ok();
    }

    printf("Basic adding: "); {
        is = pdb_intset_create();
        is = pdb_intset_add(is, 5, NULL); 
        is = pdb_intset_add(is, 6, NULL);
        is = pdb_intset_add(is, 4, NULL);
        is = pdb_intset_add(is, 4, NULL);
        assert(is->len == 3);
        // pdb_print_intset(is);
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = pdb_intset_create();
        int success = 0;
        for (i = 0; i < 1024; i++) {
            is = pdb_intset_add(is, rand() % 0x800, &success);
            if (success) inserts++;
        }
        assert(is->len == inserts);
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("Upgrade from int16 to int32: "); {
        uint32_t pos;
        is = pdb_intset_create();
        is = pdb_intset_add(is, 32, NULL);
        // pdb_print_intset(is);
        assert(is->flag == PDB_INTSET_ENCODING_INT16);
        is = pdb_intset_add(is, 65535, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT32);
        // pdb_print_intset(is);
        assert(pdb_intset_search(is, 32, &pos) == 1);
        assert(pdb_intset_search(is, 65535, &pos) == 1);
        check(is);
        pdb_free(is, -1);

        is = pdb_intset_create();
        is = pdb_intset_add(is, 32, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT16);
        is = pdb_intset_add(is, -65535, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT32);
        assert(pdb_intset_search(is, 32, &pos) == 1);
        assert(pdb_intset_search(is, -65535, &pos) == 1);
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("Upgrade from int16 to int64: "); {
        uint32_t pos;
        is = pdb_intset_create();
        is = pdb_intset_add(is, 32, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT16);
        is = pdb_intset_add(is, 4294967295, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT64);
        assert(pdb_intset_search(is, 32, &pos) == 1);
        assert(pdb_intset_search(is, 4294967295, &pos) == 1);
        check(is);
        pdb_free(is, -1);

        is = pdb_intset_create();
        is = pdb_intset_add(is, 32, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT16);
        is = pdb_intset_add(is, -4294967295, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT64);
        assert(pdb_intset_search(is, 32, &pos) == 1);
        assert(pdb_intset_search(is, -4294967295, &pos) == 1);
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("Upgrade from int32 to int64: "); {
        uint32_t pos;
        is = pdb_intset_create();
        is = pdb_intset_add(is, 65535, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT32);
        is = pdb_intset_add(is, 4294967295, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT64);
        assert(pdb_intset_search(is, 65535, &pos));
        assert(pdb_intset_search(is, 4294967295, &pos));
        check(is);
        pdb_free(is, -1);

        is = pdb_intset_create();
        is = pdb_intset_add(is, 65535, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT32);
        is = pdb_intset_add(is, -4294967295, NULL);
        assert(is->flag == PDB_INTSET_ENCODING_INT64);
        assert(pdb_intset_search(is, 65535, &pos));
        assert(pdb_intset_search(is, -4294967295, &pos));
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = create_set(bits, size);
        // pdb_print_intset(is);
        check(is);

        start = usec();
        uint32_t pos;
        for (i = 0; i < num; i++) pdb_intset_search(is, rand() % ((1<<bits)-1), &pos);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
        pdb_free(is, -1);
    }

    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = pdb_intset_create();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = pdb_intset_add(is, v1, NULL);
            uint32_t pos;
            assert(pdb_intset_search(is, v1, &pos) == 1);

            v2 = rand() % 0xfff;
            while(v2 == v1) v2 = rand() % 0xfff;
            is = pdb_intset_delete(is, v2, NULL);
            assert(pdb_intset_search(is, v2, &pos) == 0);
        }
        check(is);
        ok();
        pdb_free(is, -1);
    }

    printf("intersect/union/difference:\n"); {
        is = pdb_intset_create();
        is = pdb_intset_add(is, 5, NULL); 
        is = pdb_intset_add(is, 6, NULL);
        is = pdb_intset_add(is, 4, NULL);
        is = pdb_intset_add(is, 4, NULL);
        pdb_print_intset(is);

        is2 = pdb_intset_create();
        is2 = pdb_intset_add(is2, 5, NULL); 
        is2 = pdb_intset_add(is2, 7, NULL);
        is2 = pdb_intset_add(is2, 8, NULL);
        is2 = pdb_intset_add(is2, 8, NULL);
        pdb_print_intset(is2);

        printf("intersect: ");
        struct pdb_intset* is3;
        is3 = pdb_intset_intersect(is, is2);
        pdb_print_intset(is3);

        printf("difference: ");
        is3 = pdb_intset_difference(is, is2);
        pdb_print_intset(is3);

        printf("union: ");
        is3 = pdb_intset_union(is, is2);
        pdb_print_intset(is3);
    }

    return;
}
