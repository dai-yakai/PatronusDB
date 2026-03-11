#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define PDB_MAX_KEY_LEN 64

char LICENSE[] SEC("license") = "GPL";

typedef struct pdb_hash {
    void **nodes;       // 偏移 0: 8 字节指针
             
    int max_slots;      // 偏移 8: 4 字节
    int count;          // 偏移 12: 4 字节
    char parent_key[PDB_MAX_KEY_LEN];   // 偏移 16: 8 字节指针 (这就是我们要读的字段！)
} pdb_hash_t;

struct dirty_key_event {
    uint32_t opcode;
    char key[60];

    void* dataStructure;
    char parent_key[PDB_MAX_KEY_LEN];
    int is_sub_element;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024 * 1024);
} rb SEC(".maps");


/**
 * int pdb_hash_set(pdb_hash_t* hash, char* key, pdb_value* value);
 *                          RDI,         RSI(si),        RDX...
 */
SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_hash_set")
int pdb_hash_set_entry(struct pt_regs *ctx) {
    void* dataStructure = (void*)ctx->di;


    char *key_ptr = (char *)ctx->si; 
    // bpf_printk("BPF_TRACE: pdb_hash_set matched!\n");
    if (!key_ptr) return 0;

    struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
     if (!e) {
        bpf_printk("BPF_WARN: Ringbuf FULL! Dropping event for key ptr: %p\n", key_ptr);
        return 0;
    }

    char *parent_ptr = NULL;
    bpf_probe_read_user(&parent_ptr, sizeof(parent_ptr), (char*)dataStructure + offsetof(pdb_hash_t, parent_key));
    if (parent_ptr) {
        bpf_probe_read_user_str(e->parent_key, sizeof(e->parent_key), parent_ptr);
        e->is_sub_element = 1; 
    }

    e->opcode = 0xFA; 
    bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);
    e->dataStructure = dataStructure;
    bpf_ringbuf_submit(e, 0);

    return 0;
}

/**
 * int pdb_array_set(pdb_array_t* inst, char* key, pdb_value* value)
 */
SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_array_set")
int pdb_array_set_entry(struct pt_regs *ctx) {
    void* dataStructure = (void*)ctx->di;

    char *key_ptr = (char *)ctx->si; 
    // bpf_printk("BPF_TRACE: pdb_array_set matched!\n");
    if (!key_ptr) return 0;

    struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
     if (!e) {
        bpf_printk("BPF_WARN: Ringbuf FULL! Dropping event for key ptr: %p\n", key_ptr);
        return 0;
    }

    e->opcode = 0xFC; 
    e->dataStructure = dataStructure;
    bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);
    bpf_ringbuf_submit(e, 0);
    return 0;
}


SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_rbtree_set")
int pdb_rbtree_set_entry(struct pt_regs *ctx) {
    void* dataStructure = (void*)ctx->di;

    char *key_ptr = (char *)ctx->si; 
    // bpf_printk("BPF_TRACE: pdb_rbtree_set matched!\n");
    if (!key_ptr) return 0;

    struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (!e) {
        bpf_printk("BPF_WARN: Ringbuf FULL! Dropping event for key ptr: %p\n", key_ptr);
        return 0;
    }

    e->opcode = 0xFB; 
    e->dataStructure = dataStructure;
    bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);
    bpf_ringbuf_submit(e, 0);
    return 0;
}

// set
// SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_set_add")
// int pdb_set_add_entry(struct pt_regs *ctx) {
//     // 1. 获取第一个参数：struct pdb_set* set
//     // 在 x86_64 架构下，第一个参数存放在 di 寄存器中
//     void *set_ptr = (void *)ctx->di; 
//     if (!set_ptr) return 0;

//     // 2. 🚩 第一次探针读取：提取 char* key 指针
//     // 因为 key 是 pdb_set 结构体的第一个成员，所以偏移量为 0。
//     char *key_ptr = NULL;
//     long ret = bpf_probe_read_user(&key_ptr, sizeof(key_ptr), set_ptr);
    
//     // 如果读取失败，或者这个 Set 是没有顶级 Key 的临时计算集合 (key_ptr == NULL)，则直接放过
//     if (ret != 0 || !key_ptr) return 0;

//     struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
//     if (!e) return 0;

//     e->opcode = 0xFA;

//     // 4. 🚩 第二次探针读取：顺着指针提取真实的字符串内容
//     bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);

//     bpf_ringbuf_submit(e, 0);
//     return 0;
// }

// // sset
// SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_sortedSet_add")
// int pdb_sortedSet_add_entry(struct pt_regs *ctx) {
//     // 1. 获取第一个参数：struct pdb_set* set
//     // 在 x86_64 架构下，第一个参数存放在 di 寄存器中
//     void *set_ptr = (void *)ctx->di; 
//     if (!set_ptr) return 0;

//     // 2. 🚩 第一次探针读取：提取 char* key 指针
//     // 因为 key 是 pdb_set 结构体的第一个成员，所以偏移量为 0。
//     char *key_ptr = NULL;
//     long ret = bpf_probe_read_user(&key_ptr, sizeof(key_ptr), set_ptr);
    
//     // 如果读取失败，或者这个 Set 是没有顶级 Key 的临时计算集合 (key_ptr == NULL)，则直接放过
//     if (ret != 0 || !key_ptr) return 0;

//     struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
//     if (!e) return 0;

//     e->opcode = 0xFA;

//     // 4. 🚩 第二次探针读取：顺着指针提取真实的字符串内容
//     bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);

//     bpf_ringbuf_submit(e, 0);
//     return 0;
// }

// bitmap
// SEC("uprobe//home/dai/PatronusDB/pdb_server:pdb_bitmap_set_")
// int pdb_bitmap_add_entry(struct pt_regs *ctx) {
//     // 1. 获取第一个参数：struct pdb_set* set
//     // 在 x86_64 架构下，第一个参数存放在 di 寄存器中
//     void *set_ptr = (void *)ctx->di; 
//     if (!set_ptr) return 0;

//     // 2. 🚩 第一次探针读取：提取 char* key 指针
//     // 因为 key 是 pdb_set 结构体的第一个成员，所以偏移量为 0。
//     char *key_ptr = NULL;
//     long ret = bpf_probe_read_user(&key_ptr, sizeof(key_ptr), set_ptr);
    
//     // 如果读取失败，或者这个 Set 是没有顶级 Key 的临时计算集合 (key_ptr == NULL)，则直接放过
//     if (ret != 0 || !key_ptr) return 0;

//     struct dirty_key_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
//     if (!e) return 0;

//     e->opcode = 0xFA;

//     // 4. 🚩 第二次探针读取：顺着指针提取真实的字符串内容
//     bpf_probe_read_user_str(&e->key, sizeof(e->key), key_ptr);

//     bpf_ringbuf_submit(e, 0);
//     return 0;
// }