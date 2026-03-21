#include "pdb_server.h"

extern int pdb_ebpf_init();

int pdb_response_handler(int fd, char* rmsg, int length, char* out){
    // printf("rmgs: %lu, length: %d, out: %lu", strlen(rmsg), length, strlen(out));
    // memcpy(out, rmsg, length);
    // return strlen(out);
}

void dest_pdb_engine(void){
    pdb_rbtree_destroy(&global_rbtree);
    pdb_hash_destory(&global_hash);

#if ENABLE_MEMPOOL
    pdb_mem_destroy();
#endif

}

void init_pdb_engine(){
    loadServerConfig("./PatronusDB.conf");

#if ENABLE_MEMPOOL
    pdb_mem_init(1024 * 512);
#endif
    pdb_log_init();
    pdb_init_global_memory_manager();

    pdb_array_create(&global_array);
    pdb_rbtree_create(&global_rbtree);
    pdb_hash_create(&global_hash);

    // Init persistence setting.
    pdb_init_dump(global_conf.dump_dir);

    // Load persistence file
    int ret;
    if (global_conf.is_aof == 0){
        pdb_log_info("RDB file is loading...\n");
        ret = pdb_rdb_load(global_conf.dump_dir);
    }else{
        pdb_log_info("AOF file is loading...\n");     
        ret = pdb_aof_load(global_conf.dump_dir);
    }
    if (ret == PDB_OK){
        if (global_conf.is_aof == 0){
            pdb_log_info("RDB file loading success...\n");
        }else{
            pdb_log_info("AOF file loading success...\n");
        }   
    }
}


int main(int argc, char* argv[]){
    // print logo
    const char* logo = 
        "  _____      _                                _____  ____  \n"
        " |  __ \\    | |                              |  __ \\|  _ \\ \n"
        " | |__) |_ _| |_ _ __ ___  _ __  _   _ ___  | |  | | |_) |\n"
        " |  ___/ _` | __| '__/ _ \\| '_ \\| | | / __| | |  | |  _ < \n"
        " | |  | (_| | |_| | | (_) | | | | |_| \\__ \\ | |__| | |_) |\n"
        " |_|   \\__,_|\\__|_|  \\___/|_| |_|\\__,_|___/ |_____/|____/ \n";
    printf("\033[36m%s\033[0m\n", logo);

    init_pdb_engine();

    // test all datastructure
#if DATA_STRUCTURE_TEST
    pdb_intset_test();
    pdb_set_test();
    test_performance();
    test_correctness();
    pdb_test_bitmap();
#endif

    int port = global_conf.port;
    int mode = global_conf.network_mode;

    if (mode == 1){
        pdb_log_info("newtork mode, %s\n", "reactor");
        int ret = reactor_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 2){
        pdb_log_info("newtork mode: %s\n", "ntyco");
        int ret = ntyco_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 3){
        pdb_log_info("newtork mode: %s\n", "io_uring");
        int ret = uring_entry(port, pdb_protocol, pdb_response_handler);
    }

    dest_pdb_engine();

    return 0;
}