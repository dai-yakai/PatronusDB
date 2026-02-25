#ifndef __PDB_CONF_H__
#define __PDB_CONF_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "pdb_core.h"

typedef struct server_config_s{
    int port;
    int network_mode;

    bool is_replication;
    int master_port;
    char* master_ip;
    int is_slave;

    char* dump_dir;
    char* persistence;
    bool is_aof;

    int max_memory;
}server_config;

extern struct server_config_s global_conf;

typedef enum conf_type_e{
    CONF_TYPE_INT,
    CONF_TYPE_STRING,
    CONF_TYPE_BOOL,
    CONF_TYPE_LONG
} conf_type;


typedef struct config_entry_s{
    const char* name;
    conf_type type;
    void* data_ptr;
} config_entry;

int setConfigValue(config_entry* entry, char* value);
void loadServerConfig(const char *filename);

#endif