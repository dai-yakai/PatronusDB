#include "pdb_value.h"


static int _pdb_get_value_type(char* cmd){
    if (cmd == NULL)    return PDB_VALUE_TYPE_STRING;   // NULL <-> default: STRING
    if (strcmp(cmd, "JSON") == 0){
        return PDB_VALUE_TYPE_JSON;
    }
    if (strcmp(cmd, "INT") == 0){
        return PDB_VALUE_TYPE_INT;
    }

    return PDB_VALUE_TYPE_STRING;
}

void* pdb_parse_string_to_value(char* value, int type){
    if (type == PDB_VALUE_TYPE_INT){

    }
    if (type == PDB_VALUE_TYPE_JSON){

    }
    if (type == PDB_VALUE_TYPE_STRING){
        size_t len = strlen(value);
        char* v = pdb_malloc(len + 1);
        memcpy(v, value, strlen(value));
        return v;
    }

    return value;
}

int pdb_parse_value_to_string(pdb_value* value, char* buf){
    if (value->type == PDB_VALUE_TYPE_STRING){
        size_t len = strlen((char*)value->ptr);
        strcpy(buf, value->ptr);
        return len;
    }
    if (value->type == PDB_VALUE_TYPE_INT){
        // TODO
    }
    if (value->type == PDB_VALUE_TYPE_JSON){
        // TODO
    }
}

pdb_value* pdb_create_value(char* type_cmd, char* value){
    int type = _pdb_get_value_type(type_cmd);
    void* value_ptr = pdb_parse_string_to_value(value, type);

    pdb_value* v = (pdb_value*)pdb_malloc(sizeof(pdb_value));
    v->expire_time = 0;
    v->lru = 0;
    v->ref_count = 1;
    v->type = type;
    v->ptr = value_ptr;

    return v;
}

void pdb_destroy_value(pdb_value* value){
    pdb_free(value->ptr, -1);
    pdb_free(value, -1);
}

void pdb_decre_value(pdb_value* value){
    if (value == NULL)  return;
    if (value->ref_count <= 0)   return;
    value->ref_count--;

    if (value->ref_count == 0){
        pdb_destroy_value(value);
    }
}

void pdb_incre_value(pdb_value* value){
    if (value == NULL)  return;
    value->ref_count++;
}