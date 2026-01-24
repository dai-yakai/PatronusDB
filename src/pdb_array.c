#include "pdb_array.h"
// 单例模式

struct kvs_array_item_s{
    char* key;
    char* value;
};

struct kvs_array_s{
    kvs_array_item_t* table;
    int idx;

    int total_count;
};

kvs_array_t global_array;

int kvs_array_create(kvs_array_t* inst){
    if (!inst)  return -1;

    if (inst->table){
        printf("table has alloc\n");
        return -1;
    }

    inst->table = kvs_malloc(sizeof(kvs_array_item_t) * KVS_ARRAY_SIZE);
    if (!inst->table){
        printf("kvs_malloc error\n");
        return -1;
    }
    memset(inst->table, 0, sizeof(kvs_array_item_t) * KVS_ARRAY_SIZE);

    inst->idx = 0;
    inst->total_count = 0;

    return 0;
}

void kvs_array_destroy(kvs_array_t* inst){
    if (!inst)  return ;

    if (inst->table){
        kvs_free(inst->table, sizeof(kvs_array_item_t));
        inst->table = NULL;
    }
}

int kvs_array_set(kvs_array_t* inst, char* key, char* value){

    if (inst == NULL || key == NULL || value == NULL){
        printf("kvs_array_set: parameter error\n");
        return -1;
    }

    if (inst->total_count == KVS_ARRAY_SIZE){
        printf("out of kvs_array size\n");
        return -1;
    }

    char* str = kvs_array_get(inst, key);
    if (str){
        printf("total_num: %d\n", inst->total_count);
#if ENABLE_PRINT_ARRAY
        kvs_print_array(inst);
#endif
        printf("kvs_array_set: key exist\n");
        return 1;
    }

    char* kcopy = kvs_malloc(strlen(key) + 1);
    if (kcopy == NULL){
        printf("kvs_malloc failed\n");
        return -1;
    }
    memset(kcopy, 0, strlen(key) + 1);
    strncpy(kcopy, key, strlen(key) + 1);
    
    char* kvalue = kvs_malloc(strlen(value) + 1);
    if (kvalue == NULL) {
        printf("kvs_malloc failed\n");
        return -2;
    }
    memset(kvalue, 0, strlen(value) + 1);
    strncpy(kvalue, value, strlen(value) + 1);

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            inst->table[i].key = kcopy;
            inst->table[i].value = kvalue;
            inst->total_count ++;
#if ENABLE_PRINT_ARRAY
            kvs_print_array(inst);
#endif
            return 0;
        }
    }

    if (i == inst->total_count && i < KVS_ARRAY_SIZE){
        inst->table[i].key = kcopy;
        inst->table[i].value = kvalue;
        inst->total_count ++;
    }   
#if ENABLE_PRINT_ARRAY
    kvs_print_array(inst);
#endif
    return 0;
}

// no exist, return NULL
char* kvs_array_get(kvs_array_t* inst, char* key){
    if (inst == NULL){
        return NULL;
    }

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            continue;
        }

        if (strcmp(inst->table[i].key, key) == 0){
            return inst->table[i].value;
        }
    }

    return NULL;

}

/**
 * @return:
 * < 0: error
 * = 0: success
 * > 0: no exist
 */
int kvs_array_del(kvs_array_t* inst, char* key){

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (strcmp(inst->table[i].key, key) == 0){
            kvs_free(inst->table[i].key, sizeof(char));
            inst->table[i].key = NULL;

            kvs_free(inst->table[i].value, sizeof(char));
            inst->table[i].value = NULL;
#if ENABLE_PRINT_ARRAY
            kvs_print_array(inst);
#endif
            return 0;
        }
    }
#if ENABLE_PRINT_ARRAY
    kvs_print_array(inst);
#endif
    return i;

}

int kvs_array_mod(kvs_array_t* inst, char* key, char* value){
    if (inst == NULL || key == NULL || value == NULL){
        printf("kvs_array_mod: parameter error\n");
        return -1;
    }

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            continue;
        }
        if (strcmp(inst->table[i].key, key) == 0){
            kvs_free(inst->table[i].value, sizeof(char));

            char* kvalue = kvs_malloc(strlen(value) + 1);
            if (kvalue == NULL){
                printf("kvs_malloc() failed\n");
                return -1;
            }
            memset(kvalue, 0, sizeof(value));

            strncpy(kvalue, value, strlen(value));
            inst->table[i].value = kvalue;
#if ENABLE_PRINT_ARRAY
            kvs_print_array(inst);
#endif

            return 0;
        }
    }
#if ENABLE_PRINT_ARRAY
    kvs_print_array(inst);
#endif
    return i;
}

/**
 * 0: exist
 * 1: no exist
 */
int kvs_array_exist(kvs_array_t* inst, char* key){
    char* str = kvs_array_get(inst, key);
    if (!str){
        return 1;
    }
    return 0;
}

void kvs_print_array(kvs_array_t* inst){
#if ENABLE_PRINT_ARRAY
    printf("########### print kv store ############\n");
    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        printf("key: %s, value: %s\n", inst->table[i].key, inst->table[i].value);
    }
#endif
    return ;
}

void kvs_array_dump(kvs_array_t *arr, const char *file){
    if (arr == NULL || file == NULL)
    {
        return;
    }
    
    FILE *fp = fopen(file, "w");
    for (int i = 0; i < arr->total_count; i++) {
        if(arr->table[i].key == NULL){
            continue;
        }
        int ret = fprintf(fp, "SET %s %s\n",
                arr->table[i].key, arr->table[i].value);
        // printf("kvs_array_dump--> ret: %d\n", ret);
    }

    fclose(fp);
}

int kvs_array_load(kvs_array_t *arr, const char *file){
    if (!arr || !file) return -1;

    FILE *fp = fopen(file, "r");
    if (!fp) {
        perror("fopen load");
        return -2;
    }

    char cmd[16];
    char key[1024];
    char value[1024];

    while (fscanf(fp, "%15s %127s %511s", cmd, key, value) == 3) {
        if (strcmp(cmd, "SET") == 0) {
            kvs_array_set(arr, key, value);
        }
    }

    fclose(fp);
    return 0;
}

int kvs_array_mset(kvs_array_t* arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* value = tokens[i + 1];

		int ret = kvs_array_set(arr, key, value);
		if (ret != 0){
			return ret;
		}
	}

	return 0;

}
