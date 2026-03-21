/**
 * We utilize eBPF uprobes to intercept database mutation in real-time.
 * Upon each increment, the `handle_event` callback is triggered to
 * asynchronously append the incremental data into the `global_dump.aof_buffer`. 
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <bpf/libbpf.h>
#include <stdint.h>

#include "pdb_aof.h"
#include "pdb_delta.skel.h"
#include "pdb_log.h"
#include "pdb_sds.h"
#include "pdb_rdma.h"
#include "pdb_conninfo.h"

#define PDB_MAX_KEY_LEN 64

static struct pdb_delta_bpf* skel;
static struct ring_buffer* rb = NULL;

extern int pdb_array_set();
extern int pdb_rbtree_set();
extern int pdb_set_add();
extern int pdb_sortedSet_add();
extern int pdb_bitmap_set_();

struct dirty_key_event {
    uint32_t opcode;   
    char key[60];   
    
    void* dataStructure;
    char parent_key[PDB_MAX_KEY_LEN];
    int is_sub_element;

    // bitmap
    uint64_t offset;
    int val;
};



////////////////////////////////////////////////////
static int handle_event(void *ctx, void *data, size_t data_sz) {
    // pdb_log_debug("append\n");
    struct dirty_key_event *e = (struct dirty_key_event *)data;
    fflush(stdout);

    int ret;
    if (e->opcode == PDB_OPCODE_BITMAP){
        // pdb_log_info("handle: %p\n", e->key);
        ret = pdb_aof_buffer_append_bitmap(e->dataStructure, e->key, e->offset, e->val);
        return 0;
    }
    ret = pdb_aof_incrememtal_append(e->dataStructure, e->key, e->opcode);
    if (ret < 0){
        // full
        // pdb_log_debug("key: %s\n", e->key);
        return 0;
    }

    return 0;
}

int pdb_ebpf_init() {
    skel = pdb_delta_bpf__open_and_load();
    if (!skel) return -1;

    /**************************************************** */
    /********************    HASH    ******************** */
    /**************************************************** */
    // calculate func address
    Dl_info hash_info;
    if (dladdr((void *)pdb_hash_set, &hash_info) == 0) {
        fprintf(stderr, "Error: dladdr failed\n");
        return -1;
    }
    
    size_t dynamic_offset = (size_t)pdb_hash_set - (size_t)hash_info.dli_fbase;
    skel->links.pdb_hash_set_entry = bpf_program__attach_uprobe(
        skel->progs.pdb_hash_set_entry,
        false,
        -1,
        "/proc/self/exe",
        dynamic_offset
    );
    if (!skel->links.pdb_hash_set_entry) {
        fprintf(stderr, "Error: Failed to attach uprobe manually\n");
        return -1;
    }


    /**************************************************** */
    /********************    ARRAY    ******************** */
    /**************************************************** */
    // calculate func address
    Dl_info array_info;
    if (dladdr((void *)pdb_array_set, &array_info) == 0) {
        fprintf(stderr, "Error: dladdr failed\n");
        return -1;
    }
    
    dynamic_offset = (size_t)pdb_array_set- (size_t)array_info.dli_fbase;
    skel->links.pdb_array_set_entry = bpf_program__attach_uprobe(
        skel->progs.pdb_array_set_entry,
        false,
        -1,
        "/proc/self/exe",
        dynamic_offset
    );
    if (!skel->links.pdb_array_set_entry) {
        fprintf(stderr, "Error: Failed to attach uprobe manually\n");
        return -1;
    }

    /**************************************************** */
    /********************    RBTREE    ****************** */
    /**************************************************** */
    // calculate func address
    Dl_info rbtree_info;
    if (dladdr((void *)pdb_rbtree_set, &rbtree_info) == 0) {
        fprintf(stderr, "Error: dladdr failed\n");
        return -1;
    }
    
    dynamic_offset = (size_t)pdb_rbtree_set - (size_t)rbtree_info.dli_fbase;
    skel->links.pdb_rbtree_set_entry = bpf_program__attach_uprobe(
        skel->progs.pdb_rbtree_set_entry,
        false,
        -1,
        "/proc/self/exe",
        dynamic_offset
    );
    if (!skel->links.pdb_rbtree_set_entry) {
        fprintf(stderr, "Error: Failed to attach uprobe manually\n");
        return -1;
    }

    /**************************************************** */
    /********************    BITMAP    ****************** */
    /**************************************************** */
    Dl_info bitmap_info;
    if (dladdr((void *)pdb_bitmap_set_, &bitmap_info) == 0) {
        fprintf(stderr, "Error: dladdr failed\n");
        return -1;
    }
    
    dynamic_offset = (size_t)pdb_bitmap_set_ - (size_t)bitmap_info.dli_fbase;
    skel->links.pdb_bitmap_add_entry = bpf_program__attach_uprobe(
        skel->progs.pdb_bitmap_add_entry,
        false,
        -1,
        "/proc/self/exe",
        dynamic_offset
    );
    if (!skel->links.pdb_bitmap_add_entry) {
        fprintf(stderr, "Error: Failed to attach uprobe manually\n");
        return -1;
    }

    rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
    if (!rb) return -3;

    return 0;
}

void pdb_ebpf_poll() {
    int ret = ring_buffer__poll(rb, 0);
    return;
}