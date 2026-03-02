#include "pdb_array.h"
// 单例模式

pdb_array_t global_array;

int pdb_array_create(pdb_array_t* inst){
    if (!inst)  return -1;

    if (inst->table){
        printf("table has alloc\n");
        return -1;
    }

    inst->table = pdb_malloc(sizeof(pdb_array_item_t) * PDB_ARRAY_SIZE);
    if (inst->table == NULL){
        return PDB_MALLOC_NULL;
    }

    memset(inst->table, 0, sizeof(pdb_array_item_t) * PDB_ARRAY_SIZE);

    inst->idx = 0;
    inst->total_count = 0;

    return PDB_DATASTRUCTURE_OK;
}

void pdb_array_destroy(pdb_array_t* inst){
    if (!inst)  return ;

    if (inst->table){
        int i;
        for (i = 0; i < PDB_ARRAY_SIZE; i++){
            if ((inst->table)[i].key != NULL){
                pdb_free(inst->table[i].key, -1);
                pdb_free(inst->table[i].value, -1);
            }
        }
        pdb_free(inst->table, -1);
        inst->table = NULL;
    }
}

int pdb_array_set(pdb_array_t* inst, char* key, pdb_value* value){
    assert(inst != NULL && key != NULL && value != NULL);
    pdb_value* old_value;
    if (inst->total_count == PDB_ARRAY_SIZE){
        pdb_log_error("out of pdb_array size\n");
        return -1;
    }

    old_value = pdb_array_get(inst, key);
    if (old_value){
        pdb_log_info("pdb_array_set: key exist\n");
        return PDB_DATASTRUCTURE_EXIST;
    }

    // copy key
    char* kcopy = pdb_malloc(strlen(key) + 1);
    if (kcopy == NULL){
        return PDB_MALLOC_NULL;
    }
    memset(kcopy, 0, strlen(key) + 1);
    strncpy(kcopy, key, strlen(key) + 1);
    
    pdb_incre_value(value);

    int i = 0;
    for (i = 0; i < inst->total_count; i++){
        if (inst->table[i].key == NULL){
            inst->table[i].key = kcopy;
            inst->table[i].value = value;
            inst->total_count++;
            return PDB_DATASTRUCTURE_OK;
        }
    }

    if (i == inst->total_count && i < PDB_ARRAY_SIZE){
        inst->table[i].key = kcopy;
        inst->table[i].value = value;
        inst->total_count++;
    }   

    return PDB_DATASTRUCTURE_OK;
}

// no exist, return NULL
pdb_value* pdb_array_get(pdb_array_t* inst, char* key){
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
            pdb_free(inst->table[i].key, -1);
            inst->table[i].key = NULL;

            pdb_decre_value(inst->table[i].value);
            inst->table[i].value = NULL;

            return PDB_DATASTRUCTURE_OK;
        }
    }

    return PDB_DATASTRUCTURE_NOEXIST;
}

int pdb_array_mod(pdb_array_t* inst, char* key, pdb_value* value){
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
            pdb_decre_value(inst->table[i].value);

            inst->table[i].value = value;
            pdb_incre_value(value);

            return PDB_DATASTRUCTURE_OK;
        }
    }
    return PDB_DATASTRUCTURE_NOEXIST;
}


int pdb_array_exist(pdb_array_t* inst, char* key){
    pdb_value* value;
    value = pdb_array_get(inst, key);
    if (value == NULL){
        return PDB_DATASTRUCTURE_NOEXIST;
    }
    return PDB_DATASTRUCTURE_EXIST;
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



int pdb_array_mset(pdb_array_t* arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* raw_value = tokens[i + 1];
        pdb_value* value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

		int ret = pdb_array_set(arr, key, value);
        pdb_decre_value(value);
		if (ret != 0){
			return ret;
		}
	}

	return PDB_DATASTRUCTURE_OK;
}
