#include "pdb_sds.h"

char pdb_req_sds_type(size_t len){
    if (len < (1 << 5)) return PDB_SDS_TYPE_5;
    if (len < (1 << 8)) return PDB_SDS_TYPE_8;
    if (len < (1 << 16))    return PDB_SDS_TYPE_16;
    if (len < (1 << 32))    return PDB_SDS_TYPE_32;
}

static pdb_sds _pdb_enlarge_sds(pdb_sds s, size_t add_size, int greedy){
    size_t len = pdb_get_sds_len(s);
    size_t avail = pdb_get_sds_avail(s);
    char old_type = pdb_get_sds_type(s);
    char* sh = s - pdb_get_head_size(old_type);
    
    // 如果剩余的空间够用，就不进行扩容
    if (avail >= add_size){
        return s;
    }

    size_t new_len = len + add_size;
    
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
        ptr = kvs_realloc(sh, total_len + 1);       // +1: '\0'
    }else{ 
        ptr = kvs_malloc(total_len + 1);        // +1: '\0'
        memcpy((char*)ptr + hdr_len, s, len + 1);
        kvs_free(sh, len + pdb_get_head_size(old_type) + 1);
    }
    
    s = (char*)ptr + hdr_len;
    pdb_set_sds_type(s, new_type);
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

