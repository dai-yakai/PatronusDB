#include "pdb_array.h"
// 单例模式

struct pdb_array_item_s{
    char* key;
    char* value;
};

struct pdb_array_s{
    pdb_array_item_t* table;
    int idx;

    int total_count;
};

pdb_array_t global_array;

int pdb_array_create(pdb_array_t* inst){
    if (!inst)  return -1;

    if (inst->table){
        printf("table has alloc\n");
        return -1;
    }

    inst->table = pdb_malloc(sizeof(pdb_array_item_t) * PDB_ARRAY_SIZE);
    if (!inst->table){
        printf("pdb_malloc error\n");
        return -1;
    }
    memset(inst->table, 0, sizeof(pdb_array_item_t) * PDB_ARRAY_SIZE);

    inst->idx = 0;
    inst->total_count = 0;

    return 0;
}

void pdb_array_destroy(pdb_array_t* inst){
    if (!inst)  return ;

    if (inst->table){
        pdb_free(inst->table, sizeof(pdb_array_item_t));
        inst->table = NULL;
    }
}

int pdb_array_set(pdb_array_t* inst, char* key, char* value){
    assert(inst != NULL && key != NULL && value != NULL);

    if (inst->total_count == PDB_ARRAY_SIZE){
        pdb_log_error("out of pdb_array size\n");
        return -1;
    }

    char* str = pdb_array_get(inst, key);
    if (str){
        // pdb_log_info("total_num: %d\n", inst->total_count);
#if ENABLE_PRINT_ARRAY
        pdb_print_array(inst);
#endif
        pdb_log_info("pdb_array_set: key exist\n");
        return 1;
    }

    char* kcopy = pdb_malloc(strlen(key) + 1);
    assert(kcopy != NULL);

    memset(kcopy, 0, strlen(key) + 1);
    strncpy(kcopy, key, strlen(key) + 1);
    
    char* kvalue = pdb_malloc(strlen(value) + 1);
    assert(kvalue != NULL);

    memset(kvalue, 0, strlen(value) + 1);
    strncpy(kvalue, value, strlen(value) + 1);

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            inst->table[i].key = kcopy;
            inst->table[i].value = kvalue;
            inst->total_count ++;
#if ENABLE_PRINT_ARRAY
            pdb_print_array(inst);
#endif
            return 0;
        }
    }

    if (i == inst->total_count && i < PDB_ARRAY_SIZE){
        inst->table[i].key = kcopy;
        inst->table[i].value = kvalue;
        inst->total_count ++;
    }   
#if ENABLE_PRINT_ARRAY
    pdb_print_array(inst);
#endif
    return 0;
}

// no exist, return NULL
char* pdb_array_get(pdb_array_t* inst, char* key){
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
int pdb_array_del(pdb_array_t* inst, char* key){

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (strcmp(inst->table[i].key, key) == 0){
            pdb_free(inst->table[i].key, sizeof(char));
            inst->table[i].key = NULL;

            pdb_free(inst->table[i].value, sizeof(char));
            inst->table[i].value = NULL;
#if ENABLE_PRINT_ARRAY
            pdb_print_array(inst);
#endif
            return 0;
        }
    }
#if ENABLE_PRINT_ARRAY
    pdb_print_array(inst);
#endif
    return i;

}

int pdb_array_mod(pdb_array_t* inst, char* key, char* value){
    if (inst == NULL || key == NULL || value == NULL){
        printf("pdb_array_mod: parameter error\n");
        return -1;
    }

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            continue;
        }
        if (strcmp(inst->table[i].key, key) == 0){
            pdb_free(inst->table[i].value, sizeof(char));

            char* kvalue = pdb_malloc(strlen(value) + 1);
            if (kvalue == NULL){
                printf("pdb_malloc() failed\n");
                return -1;
            }
            memset(kvalue, 0, sizeof(value));

            strncpy(kvalue, value, strlen(value));
            inst->table[i].value = kvalue;
#if ENABLE_PRINT_ARRAY
            pdb_print_array(inst);
#endif

            return 0;
        }
    }
#if ENABLE_PRINT_ARRAY
    pdb_print_array(inst);
#endif
    return i;
}

/**
 * 0: exist
 * 1: no exist
 */
int pdb_array_exist(pdb_array_t* inst, char* key){
    char* str = pdb_array_get(inst, key);
    if (!str){
        return 1;
    }
    return 0;
}

void pdb_print_array(pdb_array_t* inst){
#if ENABLE_PRINT_ARRAY
    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        printf("key: %s, value: %s\n", inst->table[i].key, inst->table[i].value);
    }
#endif
    return ;
}

void pdb_array_dump(pdb_array_t *arr, const char *file){
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
        // printf("pdb_array_dump--> ret: %d\n", ret);
    }

    fclose(fp);
}

int pdb_array_load(pdb_array_t *arr, const char *file){
    assert(arr != NULL && file != NULL);

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
            pdb_array_set(arr, key, value);
        }
    }

    fclose(fp);
    return 0;
}

int pdb_array_mset(pdb_array_t* arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* value = tokens[i + 1];

		int ret = pdb_array_set(arr, key, value);
		if (ret != 0){
			return ret;
		}
	}

	return 0;
}
