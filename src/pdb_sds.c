#include "pdb_sds.h"

/**
 * A collection of functions to get, set, determine SDS type.
 * `pdb_req_sds_type`: select the appropriate SDS type based on the given length.
 * WARNING: The SDS type must be initialized first, as all other functions rely on it.
 */
int pdb_get_sds_type(pdb_sds s){
    int type = s[-1];
    return type & PDB_SDS_TYPE_MASK;
}

void pdb_set_sds_type(pdb_sds s, char type){
    s[-1] = type;
}

int pdb_req_sds_type(size_t len){
    if (len < (1ULL << 5))      return PDB_SDS_TYPE_5;
    if (len < (1ULL << 8))      return PDB_SDS_TYPE_8;
    if (len < (1ULL << 16))     return PDB_SDS_TYPE_16;
    if (len < (1ULL << 32))     return PDB_SDS_TYPE_32;

    return PDB_SDS_TYPE_64;
}


size_t pdb_get_sds_hdrlen(int type){
    switch (type)
    {
        case PDB_SDS_TYPE_5:
            return sizeof(struct pdb_sds_5_s);
            break;

        case PDB_SDS_TYPE_8:
            return sizeof(struct pdb_sds_8_s);
            break;

        case PDB_SDS_TYPE_16:
            return sizeof(struct pdb_sds_16_s);
            break;

        case PDB_SDS_TYPE_32:
            return sizeof(struct pdb_sds_32_s);
            break;
        
        case PDB_SDS_TYPE_64:
            return sizeof(struct pdb_sds_64_s);
            break;
    }
    return 0;
}


size_t pdb_get_sds_len(pdb_sds s){
    unsigned char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:   
            return (PDB_HDR_PTR(5, s)->flags & PDB_SDS_TYPE_LEN_5_MASK) >> 3;
            break;
        case PDB_SDS_TYPE_8:
            return PDB_HDR_PTR(8, s)->len;
            break;
        case PDB_SDS_TYPE_16:
            return PDB_HDR_PTR(16, s)->len;
            break;
        case PDB_SDS_TYPE_32:
            return PDB_HDR_PTR(32, s)->len;
            break;
        case PDB_SDS_TYPE_64:
            return PDB_HDR_PTR(64, s)->len;
            break;
    }

    return PDB_OK;
}

void pdb_set_sds_len(pdb_sds s, size_t len){
    assert(len >= 0);
    int type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:
        {
            struct pdb_sds_5_s* sh5 = (struct pdb_sds_5_s*)((char*)s - sizeof(struct pdb_sds_5_s));
            sh5->flags = type | (len << 3);
            break;
        }
        case PDB_SDS_TYPE_8:
        {
            struct pdb_sds_8_s* sh8 = (struct pdb_sds_8_s*)((char*)s - sizeof(struct pdb_sds_8_s));
            sh8->len = len;
            break;
        }
        case PDB_SDS_TYPE_16:
        {
            struct pdb_sds_16_s* sh16 = (struct pdb_sds_16_s*)((char*)s - sizeof(struct pdb_sds_16_s));
            sh16->len = len;
            break;
        }
        case PDB_SDS_TYPE_32:
        {
            struct pdb_sds_32_s* sh32 = (struct pdb_sds_32_s*)((char*)s - sizeof(struct pdb_sds_32_s));
            sh32->len = len;
            break;
        }
        case PDB_SDS_TYPE_64:
        {
            struct pdb_sds_64_s* sh64 = (struct pdb_sds_64_s*)((char*)s - sizeof(struct pdb_sds_64_s));
            sh64->len = len;
            break;
        }
    }
}


size_t pdb_get_sds_avail(pdb_sds s){
    char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:
            return 0;
            break;
        case PDB_SDS_TYPE_8:
        {
            struct pdb_sds_8_s* sh = PDB_HDR_PTR(8, s);

            assert(sh->alloc >= sh->len);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_16:
        {
            struct pdb_sds_16_s* sh = PDB_HDR_PTR(16, s);

            assert(sh->alloc >= sh->len);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_32:
        {
            struct pdb_sds_32_s* sh = PDB_HDR_PTR(32, s);

            assert(sh->alloc >= sh->len);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_64:
        {
            struct pdb_sds_64_s* sh = PDB_HDR_PTR(64, s);

            assert(sh->alloc >= sh->len);
            return sh->alloc - sh->len;
        }
    }

    return PDB_OK;
}


size_t pdb_get_head_size(char type){
    switch(type){
        case PDB_SDS_TYPE_5:
            return sizeof(struct pdb_sds_5_s);
            break;
        case PDB_SDS_TYPE_8:
            return sizeof(struct pdb_sds_8_s);
            break;
        case PDB_SDS_TYPE_16:
            return sizeof(struct pdb_sds_16_s);
            break;
        case PDB_SDS_TYPE_32:
            return sizeof(struct pdb_sds_32_s);
            break;
        case PDB_SDS_TYPE_64:
            return sizeof(struct pdb_sds_64_s);
            break;
    }

    return 0;
}


void pdb_set_sds_alloc(pdb_sds s, size_t alloc){
    char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_8:
        {
            struct pdb_sds_8_s* sh8 = (struct pdb_sds_8_s*)((char*)s - sizeof(struct pdb_sds_8_s));
            sh8->alloc = alloc;
            break;
        }
        case PDB_SDS_TYPE_16:
        {
            struct pdb_sds_16_s* sh16 = (struct pdb_sds_16_s*)((char*)s - sizeof(struct pdb_sds_16_s));
            sh16->alloc = alloc;
            break;
        }
        case PDB_SDS_TYPE_32:
        {
            struct pdb_sds_32_s* sh32 = (struct pdb_sds_32_s*)((char*)s - sizeof(struct pdb_sds_32_s));
            sh32->alloc = alloc;
            break;
        }
        case PDB_SDS_TYPE_64:
        {
            struct pdb_sds_64_s* sh64 = (struct pdb_sds_64_s*)((char*)s - sizeof(struct pdb_sds_64_s));
            sh64->alloc = alloc;
            break;
        }
    }
}

size_t pdb_get_sds_alloc(pdb_sds s){
    unsigned char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:   
            return (PDB_HDR_PTR(5, s)->flags & PDB_SDS_TYPE_LEN_5_MASK) >> 3;
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

    int new_type = pdb_req_sds_type(new_len);
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


/**
 * Return NULL sds with `init_len`
 */
pdb_sds pdb_get_new_sds(size_t init_len){
    void* sh;
    int type = pdb_req_sds_type(init_len);
    if (type == PDB_SDS_TYPE_5 || init_len == 0) type = PDB_SDS_TYPE_8;

    size_t total_len = init_len + pdb_get_head_size(type) + 1;  // +1:\0

    sh = pdb_malloc(total_len);
    if (sh == NULL){
        return NULL;
    }

    pdb_sds new_sds = (char*)sh + pdb_get_head_size(type);
    pdb_set_sds_type(new_sds, type);

    pdb_set_sds_alloc(new_sds, init_len);
    pdb_set_sds_len(new_sds, 0);
    new_sds[0] = '\0';

    return new_sds;
}


/**
 * Return new sds witi c string
 */

pdb_sds pdb_get_new_sds2(const char* str){
    if (str == NULL)    return NULL;

    int type;
    size_t len;
    size_t hdrlen;
    size_t total_len;

    len = strlen(str);
    type = pdb_req_sds_type(len);
    if (type == PDB_SDS_TYPE_5) type = PDB_SDS_TYPE_8;
    hdrlen = pdb_get_sds_hdrlen(type);
    total_len = len + hdrlen;
    pdb_sds s = pdb_malloc(total_len + 1);
    if (s == NULL)  return NULL;

    pdb_sds new_sds = s + hdrlen;
    pdb_set_sds_type(new_sds, type);
    pdb_set_sds_alloc(new_sds, len);
    new_sds[len] = '\0';

    memcpy(new_sds, str, len);
    pdb_set_sds_len(new_sds, len);
    return new_sds;
}

/**
 * Changes `s` in-place to be a substring starting from `start` with `len`.
 * It does not shrink the buffer. The unuse capacity remains.
 */
void pdb_sds_sub_str(pdb_sds s, size_t start, size_t len){
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


/**
 * Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 * 
 * The substring includes `start` and `end`.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 * 
 * The string is modified in-place.
 */
void pdb_sds_range(pdb_sds s, ssize_t start, ssize_t end){
    size_t len = pdb_get_sds_len(s);
    if (len == 0)       return;
    if (start < 0){
        start = start + len;
        if (start < 0)  start = 0;
    }
    if (end < 0){
        end = end + len;
        if (end < 0)    end = 0;
    }
    if (end >= len)     end = len - 1;
    if (start > end)    return;

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

pdb_sds pdb_sds_cat_len(pdb_sds s, pdb_sds cat_s, size_t len){
    size_t curr_len = pdb_get_sds_len(s);
    size_t new_len = curr_len + len;

    s = pdb_enlarge_sds_greedy(s, len);
    if (s == NULL)  return NULL;
    memcpy(s + curr_len, cat_s, len);
    pdb_set_sds_len(s, curr_len + len);
    s[new_len] = '\0';

    return s;

    // size_t cat_len = pdb_get_sds_len(cat_s);
    // assert(len <= cat_len);

    // size_t new_len = curr_len + len;

    // pdb_sds new_s = pdb_get_new_sds(new_len + 1);
    // pdb_set_sds_len(new_s, new_len);

    // memcpy(new_s, s, curr_len);
    // memcpy(new_s + curr_len, cat_s, len);
    // new_s[new_len] = '\0';

    // pdb_sds_free(s);

    // return new_s;
}

void pdb_sds_clear(pdb_sds s){
    pdb_set_sds_len(s, 0);
    s[0] = '\0';
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

