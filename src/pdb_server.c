#include "pdb_server.h"




int pdb_response_handler(char* rmsg, int length, char* out){
    printf("rmgs: %lu, length: %d, out: %lu", strlen(rmsg), length, strlen(out));
    memcpy(out, rmsg, length);

    return strlen(out);
}

void dest_pdb_engine(void){
#if 0
    // pdb_array_dump(&global_array, "array.dump");
    pdb_array_destroy(&global_array);
    printf("array suucess\n");
#endif

    // pdb_rbtree_dump(&global_rbtree, "rbtree.dump");
    pdb_rbtree_destroy(&global_rbtree);
    printf("rbtree suucess\n");

    // pdb_hash_dump(&global_hash, "hash.dump");
    pdb_hash_destory(&global_hash);

#if ENABLE_MEMPOOL
    pdb_mem_destroy();
#endif

}


void init_pdb_engine(){
#if ENABLE_MEMPOOL
    pdb_mem_init(1024 * 512);
#endif

    pdb_array_create(&global_array);
    pdb_rbtree_create(&global_rbtree);
    pdb_hash_create(&global_hash);

    // Load persistence file
    int ret;
    if (global_conf.is_aof == 0){
        pdb_log_info("RDB file is loading...\n");
        ret = pdb_rdb_load(global_conf.dump_dir);
    }else{
        pdb_log_info("AOF file is loading...\n");
        // pdb_init_aof(global_conf.dump_dir);
        ret = pdb_aof_load(global_conf.dump_dir);
    }
    if (ret == PDB_OK){
        if (global_conf.is_aof == 0){
            pdb_log_info("RDB file loading success...\n");
        }else{
            pdb_log_info("AOF file loading success...\n");
        }
        
    }

    // Init persistence setting.
    pdb_init_dump(global_conf.dump_dir);
}


int main(int argc, char* argv[]){
    const char* logo = 
        "  _____      _                                _____  ____  \n"
        " |  __ \\    | |                              |  __ \\|  _ \\ \n"
        " | |__) |_ _| |_ _ __ ___  _ __  _   _ ___  | |  | | |_) |\n"
        " |  ___/ _` | __| '__/ _ \\| '_ \\| | | / __| | |  | |  _ < \n"
        " | |  | (_| | |_| | | (_) | | | | |_| \\__ \\ | |__| | |_) |\n"
        " |_|   \\__,_|\\__|_|  \\___/|_| |_|\\__,_|___/ |_____/|____/ \n";

    printf("\033[36m%s\033[0m\n", logo);

    loadServerConfig("/home/dai/PatronusDB/PatronusDB.conf");
    init_pdb_engine();

    // pdb_intset_test();
    // pdb_set_test();
    test_performance();
    test_correctness();
    pdb_test_bitmap();
    
    int port = global_conf.port;
    int mode = global_conf.network_mode;

    if (global_conf.is_replication){
        // 为slave节点
        // char* master_ip = argv[3];
        // unsigned short master_port = atoi(argv[4]);
        char* master_ip = global_conf.master_ip;
        int master_port = global_conf.master_port;
        global_replication.master_ip = master_ip;
        global_replication.master_port = master_port;

        global_replication.is_master = 0;
        int fd = connect_master(master_ip, master_port);
        if (fd > 0){
            global_replication.slave_to_master_fd = fd;
        }else{
            printf("slave connect to master failed\n");
        }

        // 从节点主动发送SYN: "*1\r\n$4\r\nSYNC\r\n"
        char* msg = "*1\r\n$3\r\nSYN\r\n";
        int ret = send(fd, msg, strlen(msg), 0);
        if (ret < 0){
            printf("slave send SYN failed\n");
        }
        
    }else{
        // 为master节点
        global_replication.is_master = 1;
    }

    if (mode == 1){
        pdb_log_info("newtork mode, %s\n", "reactor");
        int ret = reactor_entry(port, pdb_protocol, pdb_response_handler);
       //  printf("ret: %d\n", ret);
    }else if (mode == 2){
        pdb_log_info("newtork mode: %s\n", "ntyco");
        ntyco_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 3){
        pdb_log_info("newtork mode: %s\n", "io_uring");
        uring_entry(port, pdb_protocol, pdb_response_handler);
    }

    dest_pdb_engine();
    
    return 0;
}