#ifndef __PDB_EBPF__
#define __PDB_EBPF__

#include <stdint.h>
#include "pdb_aof.h"

#define EBPF_RING_CAPACITY 1024 * 1024
#define PDB_MAX_KEY_LEN 64

// ebpf ring buffer
typedef struct { 
    char key[256];
    uint8_t opcode;
    volatile int ready; // 状态位：0=空闲, 1=正在写, 2=已就绪可读
} pdb_ebpf_event_t;

typedef struct {
    pdb_ebpf_event_t events[EBPF_RING_CAPACITY];
    volatile uint32_t write_idx; // eBPF 追加位
    volatile uint32_t read_idx;  // RDMA 消费位
} pdb_ebpf_ring_t;

extern pdb_ebpf_ring_t g_ebpf_ring;


#endif