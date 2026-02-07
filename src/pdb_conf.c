#include "pdb_conf.h"

server_config global_conf;

config_entry conf_table[] = {
    {"port", CONF_TYPE_INT, &global_conf.port},
    {"network_mode", CONF_TYPE_INT, &global_conf.network_mode},
    {"master_port", CONF_TYPE_INT, &global_conf.master_port},
    {"master_ip", CONF_TYPE_STRING, &global_conf.master_ip},
    {"is_slave", CONF_TYPE_INT, &global_conf.is_slave},
    {"is_replication", CONF_TYPE_BOOL, &global_conf.is_replication},
    {"array_dump_dir", CONF_TYPE_STRING, &global_conf.array_dump_dir},
    {"rbtree_dump_dir", CONF_TYPE_STRING, &global_conf.rbtree_dump_dir},
    {"hash_dump_dir", CONF_TYPE_STRING, &global_conf.hash_dump_dir},
    {NULL, 0, NULL}
};

static bool stringToBool(char* val){
    if (!strcasecmp(val, "yes")) return true;
    if (!strcasecmp(val, "no"))  return false;

    return false;
}

int setConfigValue(config_entry* entry, char* value) {
    switch (entry->type) {
        case CONF_TYPE_INT:
            *(int*)(entry->data_ptr) = atoi(value);
            break;

        case CONF_TYPE_BOOL:
        {
            bool val = stringToBool(value);
            *(bool*)entry->data_ptr = val;
            break;
        }
        case CONF_TYPE_STRING:
            if (*(char**)(entry->data_ptr)) {
                free(*(char**)(entry->data_ptr));
            }
            *(char**)entry->data_ptr = strdup(value);
            break;
            
        default:
            return PDB_ERROR;
    }
    return PDB_OK;
}

void loadServerConfig(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) return;

    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        // 过滤注释和空行
        char *p = strchr(line, '#');
        if (p) *p = '\0';
        
        char *key = strtok(line, " \t\r\n");
        char *val = strtok(NULL, " \t\r\n");
        
        if (!key || !val) continue;

        int found = 0;
        for (int i = 0; conf_table[i].name != NULL; i++) {
            if (strcasecmp(key, conf_table[i].name) == 0) {
                if (setConfigValue(&conf_table[i], val) == PDB_OK) {
                    // printf("[Config] %s set to %s\n", key, val);
                } else {
                    fprintf(stderr, "Invalid value for %s\n", key);
                }
                found = 1;
                break;
            }
        }
        
        if (!found) {
            printf("[Warning] Unknown directive: %s\n", key);
        }
    }
    fclose(fp);
}
