#ifndef __PDB_SDS__
#define __PDB_SDS__

#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>
#include <assert.h>

#include "pdb_core.h"
#include "pdb_malloc.h"
#include "pdb_log.h"

#define PDB_SDS_TYPE_MASK       7
#define PDB_SDS_TYPE_LEN_5_MASK 248

#define PDB_PROTO_MAX_SDS_LEN   512*1024*1024   // 512M
#define PDB_MAX_PREALLOC        1024*1024

#define PDB_SDS_TYPE_5          0
#define PDB_SDS_TYPE_8          1
#define PDB_SDS_TYPE_16         2
#define PDB_SDS_TYPE_32         3
#define PDB_SDS_TYPE_64         4

typedef char* pdb_sds;

struct __attribute__ ((__packed__)) pdb_sds_5_s{
    unsigned char flags;        // 低3位表示类型，高5位表示len
    char buf[];
};

// 小于32字节使用
struct __attribute__ ((__packed__)) pdb_sds_8_s{
    uint8_t len;                // 已经使用的长度（0-31）
    uint8_t alloc;              // 分配的空间（0-31）
    unsigned char flags;        // 低三位表示类型
    char buf[];
};

// 小于64K使用
struct __attribute__ ((__packed__)) pdb_sds_16_s{
    uint16_t len;
    uint16_t alloc;
    unsigned char flags;
    char buf[];
};

// 小于4G使用
struct __attribute__ ((__packed__)) pdb_sds_32_s{
    uint32_t len;
    uint32_t alloc;
    unsigned char flags;
    char buf[];
};

// 大于4G使用
struct __attribute__ ((__packed__)) pdb_sds_64_s{
    uint64_t len;
    uint64_t alloc;
    unsigned char flags;
    char buf[];
};

#define PDB_HDR_PTR(T, s)     ((struct pdb_sds_##T##_s*)(((char*)(s)) - (sizeof(struct pdb_sds_##T##_s))))


static inline unsigned char pdb_get_sds_type(pdb_sds s){
    unsigned char type = s[-1];
    return type & PDB_SDS_TYPE_MASK;
}

/**
 * @brief len，已经使用的长度
 * 
 */
static inline size_t pdb_get_sds_len(pdb_sds s){
    unsigned char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:   
            return PDB_HDR_PTR(5, s)->flags & PDB_SDS_TYPE_LEN_5_MASK;
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

/**
 * @brief 获取sds剩余可用的长度
 */
static inline size_t pdb_get_sds_avail(pdb_sds s){
    char type = pdb_get_sds_type(s);
    switch(type){
        case PDB_SDS_TYPE_5:
            return 0;
            break;
        case PDB_SDS_TYPE_8:
        {
            struct pdb_sds_8_s* sh = PDB_HDR_PTR(8, s);

            assert((sh->alloc - sh->len) >= 0);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_16:
        {
            struct pdb_sds_16_s* sh = PDB_HDR_PTR(16, s);

            assert((sh->alloc - sh->len) >= 0);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_32:
        {
            struct pdb_sds_32_s* sh = PDB_HDR_PTR(32, s);

            assert((sh->alloc - sh->len) >= 0);
            return sh->alloc - sh->len;
        }
        case PDB_SDS_TYPE_64:
        {
            struct pdb_sds_64_s* sh = PDB_HDR_PTR(64, s);

            assert((sh->alloc - sh->len) >= 0);
            return sh->alloc - sh->len;
        }
    }

    return PDB_OK;
}

static inline size_t pdb_get_head_size(char type){
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

static inline void pdb_set_sds_type(pdb_sds s, char type){
    s[-1] = type;
}

static inline void pdb_set_sds_len(pdb_sds s, size_t len){
    assert(len >= 0);
    char type = pdb_get_sds_type(s);
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

static inline void pdb_set_sds_alloc(pdb_sds s, size_t alloc){
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

char pdb_req_sds_type(size_t len);
pdb_sds pdb_enlarge_sds_greedy(pdb_sds s, size_t add_size);
pdb_sds pdb_enlarge_sds_no_greedy(pdb_sds s, size_t add_size);
pdb_sds pdb_get_new_sds(size_t init_len);
void pdb_sds_range(pdb_sds s, ssize_t start, ssize_t end);
void pdb_sds_sub_str(pdb_sds s, ssize_t start, ssize_t len);
void pdb_sds_len_increment(pdb_sds s, ssize_t increment);
void pdb_sds_free(pdb_sds s);
size_t pdb_get_sds_alloc(pdb_sds s);

#endif