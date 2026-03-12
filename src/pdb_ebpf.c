// src/pdb_ebpf.c

/**
 * We utilize eBPF uprobes to intercept database mutation in real-time.
 * Upon each increment, the `handle_event` callback is triggered to
 * asynchronously append the incremental data into the `global_dump.aof_buffer`. 
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <bpf/libbpf.h>
#include "pdb_delta.skel.h"
#include "pdb_log.h"
#include "pdb_sds.h"
#include "pdb_ebpf.h"
#include "pdb_rdma.h"
#include "pdb_conninfo.h"

static struct pdb_delta_bpf* skel;
static struct ring_buffer* rb = NULL;
extern pdb_rdma_snapshot_ctx* incre_master_snap;
pdb_ebpf_ring_t g_ebpf_ring = {0};

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

/**
 * queue
 */
typedef struct pdb_retry_node {
    void *dataStructure;
    char key[256];      // store key
    uint8_t opcode;
    struct pdb_retry_node *next;
} pdb_retry_node_t;

typedef struct {
    pdb_retry_node_t *head;
    pdb_retry_node_t *tail;
    size_t count;
    pthread_mutex_t lock;
} pdb_retry_queue_t;

static pdb_retry_queue_t g_retry_queue = {NULL, NULL, 0, PTHREAD_MUTEX_INITIALIZER};

void pdb_push_to_retry_queue(void *ds, const char *key, uint8_t opcode) {
    // pdb_log_debug("key: %s\n", key);
    pdb_retry_node_t *new_node = malloc(sizeof(pdb_retry_node_t));
    if (!new_node) {
        pdb_log_error("Retry queue: malloc failed\n");
        return;
    }

    new_node->dataStructure = ds;
    strncpy(new_node->key, key, sizeof(new_node->key) - 1);
    new_node->opcode = opcode;
    new_node->next = NULL;

    pthread_mutex_lock(&g_retry_queue.lock);
    if (g_retry_queue.tail == NULL) {
        g_retry_queue.head = g_retry_queue.tail = new_node;
    } else {
        g_retry_queue.tail->next = new_node;
        g_retry_queue.tail = new_node;
    }
    g_retry_queue.count++;
    pthread_mutex_unlock(&g_retry_queue.lock);
}

static int process_single_retry() {
    pthread_mutex_lock(&g_retry_queue.lock);
    if (g_retry_queue.head == NULL) {
        pthread_mutex_unlock(&g_retry_queue.lock);
        return 0; 
    }

    pdb_retry_node_t *node = g_retry_queue.head;
    int ret = pdb_rdma_incremental_append(node->dataStructure, node->key, node->opcode);
    
    if (ret == 0) {
        g_retry_queue.head = node->next;
        if (g_retry_queue.head == NULL) g_retry_queue.tail = NULL;
        g_retry_queue.count--;
        pthread_mutex_unlock(&g_retry_queue.lock);
        free(node);
        return 1; 
    } else if (ret == -3) {
        pthread_mutex_unlock(&g_retry_queue.lock);
        return -1; 
    } else {
        g_retry_queue.head = node->next;
        if (g_retry_queue.head == NULL) g_retry_queue.tail = NULL;
        g_retry_queue.count--;
        pthread_mutex_unlock(&g_retry_queue.lock);
        // pdb_log_debug("Dropped poison key: %s\n", (char*)node->key);
        free(node);
        return 1; 
    }
}

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
    printf("DEBUG: Dynamic Offset Calculated: 0x%zx\n", dynamic_offset);
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
    printf("DEBUG: Dynamic Offset Calculated: 0x%zx\n", dynamic_offset);
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
    printf("DEBUG: Dynamic Offset Calculated: 0x%zx\n", dynamic_offset);
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
    printf("DEBUG: Dynamic Offset Calculated: 0x%zx\n", dynamic_offset);
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
    // pdb_log_debug("pdb_ebpf_poll\n");
    int ret = ring_buffer__poll(rb, 0);
    return;
}