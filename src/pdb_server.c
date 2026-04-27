#include "pdb_server.h"

int pdb_response_handler(int fd, char* rmsg, int length, char* out){

}

void dest_pdb_engine(void){
    pdb_rbtree_destroy(&global_rbtree);
    pdb_hash_destory(&global_hash);

#if ENABLE_MEMPOOL
    pdb_mem_destroy();
#endif

}

#ifdef ENABLE_DPDK
int pdb_init_dpdk(){
    pdb_log_debug("init dpdk env\n");
    int fake_argc = 3;
    char *fake_argv[3];
    int ret_dpdk = 0;

    fake_argv[0] = strdup("pdb_server");
    fake_argv[1] = strdup("--conf");
    fake_argv[2] = strdup(global_conf.init_dpdk);
    
    int saved_stdout = dup(STDOUT_FILENO);
    int saved_stderr = dup(STDERR_FILENO);
    int log_fd = open(global_conf.dpdk_log, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (log_fd < 0){
        pdb_log_error("open/create dpdk log error\n");
    }else{
        dup2(log_fd, STDOUT_FILENO);
        dup2(log_fd, STDERR_FILENO);
    }

    // timestap
    time_t t = time(NULL);
    struct tm *tm_info = localtime(&t);
    char time_buf[64];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
    char header_buf[256];
    int header_len = snprintf(header_buf, sizeof(header_buf), 
        "\n========================================\n"
        "PatronusDB DPDK Engine Startup Log\n"
        "Time: %s\n"
        "========================================\n\n", 
        time_buf);
    syscall(SYS_write, log_fd, header_buf, header_len);

    ret_dpdk = ff_init(fake_argc, fake_argv);

    if (log_fd >= 0){
        dup2(saved_stdout, STDOUT_FILENO);
        dup2(saved_stderr, STDERR_FILENO);
        close(log_fd);
        close(saved_stdout);
        close(saved_stderr);
    }

    free(fake_argv[0]);
    free(fake_argv[1]);
    free(fake_argv[2]);
    
    return ret_dpdk;
}
#endif

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
        ret = pdb_aof_load();
    }
    if (ret == PDB_OK){
        if (global_conf.is_aof == 0){
            pdb_log_info("RDB file loading success...\n");
        }else{
            pdb_log_info("AOF file loading success...\n");
        }   
    }

    // init dpdk env
#ifdef ENABLE_DPDK
    int dpdk_ret = pdb_init_dpdk();
#endif
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
    int ret = 0;
    if (mode == 1){
        pdb_log_info("newtork mode, %s\n", "reactor");
        ret = reactor_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 2){
        pdb_log_info("newtork mode: %s\n", "ntyco");
        ret = ntyco_entry(port, pdb_protocol, pdb_response_handler);
    }else if (mode == 3){
        pdb_log_info("newtork mode: %s\n", "io_uring");
        ret = uring_entry(port, pdb_protocol, pdb_response_handler);
    }

    dest_pdb_engine();
    pdb_log_info("pdb exit: %d", ret);

    return 0;
}