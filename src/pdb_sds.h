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



int pdb_get_sds_type(pdb_sds s);
size_t pdb_get_sds_len(pdb_sds s);
size_t pdb_get_sds_avail(pdb_sds s);
size_t pdb_get_head_size(char type);
void pdb_set_sds_type(pdb_sds s, char type);
void pdb_set_sds_len(pdb_sds s, size_t len);
size_t pdb_get_sds_hdrlen(int type);
void pdb_set_sds_alloc(pdb_sds s, size_t alloc);
int pdb_req_sds_type(size_t len);
pdb_sds pdb_enlarge_sds_greedy(pdb_sds s, size_t add_size);
pdb_sds pdb_enlarge_sds_no_greedy(pdb_sds s, size_t add_size);
pdb_sds pdb_get_new_sds(size_t init_len);
void pdb_sds_range(pdb_sds s, ssize_t start, ssize_t end);
void pdb_sds_sub_str(pdb_sds s, size_t start, size_t len);
void pdb_sds_len_increment(pdb_sds s, ssize_t increment);
void pdb_sds_free(pdb_sds s);
size_t pdb_get_sds_alloc(pdb_sds s);
pdb_sds pdb_sds_cat_len(pdb_sds s, pdb_sds cat_s, size_t len);

#endif