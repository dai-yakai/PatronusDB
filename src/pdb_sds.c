#include "pdb_sds.h"

char pdb_req_sds_type(size_t len){
    if (len < (1ULL << 5)) return PDB_SDS_TYPE_5;
    if (len < (1ULL << 8)) return PDB_SDS_TYPE_8;
    if (len < (1ULL << 16))    return PDB_SDS_TYPE_16;
    if (len < (1ULL << 32))    return PDB_SDS_TYPE_32;
}

static pdb_sds _pdb_enlarge_sds(pdb_sds s, size_t add_size, int greedy){
    size_t len = pdb_get_sds_len(s);
    size_t avail = pdb_get_sds_avail(s);
    char old_type = pdb_get_sds_type(s);
    char* sh = s - pdb_get_head_size(old_type);
    
    // Skip enlargement if current capacity is sufficient
    if (avail >= add_size){
        return s;
    }

    size_t new_len = len + add_size;
    // pdb_log_info("sds enlarge, old_len: %d, new_len: %d\n", len, new_len); // debug by DYK
    
    if (greedy == 1){
        if (new_len < PDB_MAX_PREALLOC){
            new_len = new_len * 2;
        }else{
            new_len = new_len + PDB_MAX_PREALLOC;
        }    
    }

    char new_type = pdb_req_sds_type(new_len);
    if (new_type == PDB_SDS_TYPE_5){
        new_type = PDB_SDS_TYPE_8;
    }

    size_t hdr_len = pdb_get_head_size(new_type);
    size_t total_len = hdr_len + new_len;
    void* ptr;

    if (old_type == new_type){
        ptr = pdb_realloc(sh, total_len + 1);       // +1: '\0'
        assert(ptr != NULL);
    }else{ 
        ptr = pdb_malloc(total_len + 1);        // +1: '\0'
        assert(ptr != NULL);

        memcpy((char*)ptr + hdr_len, s, len + 1);
        pdb_free(sh, -1);
    }
    
    s = (char*)ptr + hdr_len;
    pdb_set_sds_type(s, new_type);

    assert((new_len > 0) || (len > 0));
    pdb_set_sds_alloc(s, new_len);
    pdb_set_sds_len(s, len);

    return s;
}

pdb_sds pdb_enlarge_sds_greedy(pdb_sds s, size_t add_size){
    return _pdb_enlarge_sds(s, add_size, 1);
}

pdb_sds pdb_enlarge_sds_no_greedy(pdb_sds s, size_t add_size){
    return _pdb_enlarge_sds(s, add_size, 0);
}

pdb_sds pdb_get_new_sds(size_t init_len){
    void* sh;
    char type = pdb_req_sds_type(init_len);

    if (type == PDB_SDS_TYPE_5 && init_len == 0) type = PDB_SDS_TYPE_8;

    size_t total_len = init_len + pdb_get_head_size(type) + 1;  // +1:\0

    sh = pdb_malloc(total_len);
    if (sh == NULL){
        return NULL;
    }
    // ((char*)sh)[total_len - 1] = '\0';

    pdb_sds new_sds = (char*)sh + pdb_get_head_size(type);
    pdb_set_sds_alloc(new_sds, init_len);
    pdb_set_sds_type(new_sds, type);
    pdb_set_sds_len(new_sds, 0);
    new_sds[0] = '\0';

    return new_sds;
}

void pdb_sds_sub_str(pdb_sds s, ssize_t start, ssize_t len){
    size_t old_len = pdb_get_sds_len(s);
    if (start >= old_len){
        return;
    }
    if (len > old_len - start){
        len = old_len - start;
    }
    assert(len > 0);
    memmove(s, s + start, len);

    pdb_set_sds_len(s, len);
    s[len] = '\0';
}

// -1:最后一个字符，-2:倒数第二个字符
void pdb_sds_range(pdb_sds s, ssize_t start, ssize_t end){
    size_t len = pdb_get_sds_len(s);

    if (start < 0){
        start = start + len;
    }
    if (end < 0){
        end = end + len;
    }

    if (start > end)    
        return;

    size_t new_len;
    new_len = end - start + 1;                  // 闭区间(0,2)：0,1,2
    pdb_sds_sub_str(s, start, new_len);

    new_len = pdb_get_sds_len(s);
    // pdb_log_info("old_len: %d, new_len: %d\n", len, new_len);   // DEBUG by DYK
    return;
}

/**
 * If 'increment' < 0, the length of 's' decreases;
 * If 'increment' > 0, the length of 's' increases.
 */
void pdb_sds_len_increment(pdb_sds s, ssize_t increment){
    unsigned char type = pdb_get_sds_type(s);
    if (*s == '\177'){
        printf(" ");
    }
    size_t new_len = pdb_get_sds_len(s) + increment;
    assert(new_len >= 0);
    pdb_set_sds_len(s, new_len);

    s[new_len] = '\0';
}

void pdb_sds_free(pdb_sds s){
    unsigned char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:   
            pdb_free((char*)PDB_HDR_PTR(5, s), -1);
            return;
        case PDB_SDS_TYPE_8:
            pdb_free((char*)PDB_HDR_PTR(8, s), -1);
            return;
        case PDB_SDS_TYPE_16:
            pdb_free((char*)PDB_HDR_PTR(16, s), -1);
            return;
        case PDB_SDS_TYPE_32:
            pdb_free((char*)PDB_HDR_PTR(32, s), -1);
            return;
        case PDB_SDS_TYPE_64:
            pdb_free((char*)PDB_HDR_PTR(64, s), -1);
            return;
    }
}

size_t pdb_get_sds_alloc(pdb_sds s){
    unsigned char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:   
            return PDB_HDR_PTR(5, s)->flags & PDB_SDS_TYPE_LEN_5_MASK;
            break;
        case PDB_SDS_TYPE_8:
            return PDB_HDR_PTR(8, s)->alloc;
            break;
        case PDB_SDS_TYPE_16:
            return PDB_HDR_PTR(16, s)->alloc;
            break;
        case PDB_SDS_TYPE_32:
            return PDB_HDR_PTR(32, s)->alloc;
            break;
        case PDB_SDS_TYPE_64:
            return PDB_HDR_PTR(64, s)->alloc;
            break;
    }
}