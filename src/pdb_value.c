#include "pdb_value.h"
#include "pdb_set.h"
#include "pdb_sds.h"
#include "pdb_sortedSet.h"
/**
 * Return 1 if succeeding; otherwise return 0
 */
static int _pdb_string_to_long(char* s, long* out_value){
    if (s == NULL || *s == '\0') {
        return 0; 
    }

    char *endptr;
    errno = 0; 
    
    long val = strtol(s, &endptr, 10);

    if (errno == ERANGE) {
        return 0; 
    }

    if (*endptr != '\0') {
        return 0; 
    }
    *out_value = val;
    return 1;
}

static int _pdb_double_to_str(double value, char* buf){
    int len = snprintf(buf, 64, "%.6f", value);
    return len;
}


pdb_sds pdb_parse_value_to_string(pdb_value* value){
    if (value == NULL)  return NULL;

    switch(value->type){
        case PDB_VALUE_TYPE_STRING:
        {
            size_t len = strlen((char*)value->ptr);
            return (char*)value->ptr;
        }

        case PDB_VALUE_TYPE_DOUBLE:
        {
            char buf[64];
            snprintf(buf, 64, "%lf", *((double*)(value->ptr)));
            pdb_sds s = pdb_get_new_sds2(buf);
            return s;
        }

        case PDB_VALUE_TYPE_INT:
        {
            // TODO
            char buf[64];
            snprintf(buf, 64, "%d", (int)(value->ptr));
            pdb_sds s = pdb_get_new_sds2(buf);
            return s;
        }

        case PDB_VALUE_TYPE_JSON:
        {
            // TODO
        }

        case PDB_VALUE_TYPE_BITMAP:
        {
            return (pdb_sds)value->ptr;
        }

        case PDB_VALUE_TYPE_NULL:
        {
            return "1";
        }
    }
}

pdb_value* pdb_create_value(char* value, int type, ...){
    va_list args;
    va_start(args, type);

    pdb_value* v = (pdb_value*)pdb_malloc(sizeof(pdb_value));

    switch(type){
        case PDB_VALUE_TYPE_SET:
        {
            v->ptr = (pdb_set*)value;
            v->type = PDB_VALUE_TYPE_SET;

            break;
        }

        case PDB_VALUE_TYPE_SORTEDSET:
        {
            v->ptr = (struct pdb_sorted_set*)value;
            v->type = PDB_VALUE_TYPE_SORTEDSET;

            break;
        }
            
        case PDB_VALUE_TYPE_DEFAULT:
        {
            // default
            long value_long;
            if (_pdb_string_to_long(value, &value_long)){
                v->type = PDB_VALUE_TYPE_INT;
                v->ptr = (void*)(intptr_t)value_long;
            } else{
                // string
                v->type = PDB_VALUE_TYPE_STRING;
                size_t len = strlen(value);
                char* cvalue = pdb_malloc(len + 1);
                cvalue[len] = '\0';
                v->ptr = cvalue;
            }
            break;
        }

        case PDB_VALUE_TYPE_STRING:
        {
            v->type = PDB_VALUE_TYPE_STRING;
            size_t len = strlen(value);
            char* cvalue = pdb_malloc(len + 1);
            strcpy(cvalue, value);
            cvalue[len] = '\0';
            v->ptr = cvalue;

            break;
        }

        case PDB_VALUE_TYPE_JSON:
        {
            // TODO
            break;
        }

        case PDB_VALUE_TYPE_INT:
        {
            long value_long;
            if (_pdb_string_to_long(value, &value_long)){
                v->type = PDB_VALUE_TYPE_INT;
                v->ptr = (void*)(intptr_t)value_long;
            } else{
                pdb_log_error("Value can not be parse to long\n");
                pdb_free(v, -1);
                return NULL;
            }

            break;
        }

        case PDB_VALUE_TYPE_BITMAP:
        {
            v->ptr = value;
            v->type = PDB_VALUE_TYPE_BITMAP;
            break;
        }

        case PDB_VALUE_TYPE_NULL:
        {
            char* ptr = pdb_malloc(strlen("1") + 1);
            strcpy(ptr, "1");
            v->ptr = ptr;
            v->type = PDB_VALUE_TYPE_NULL;
            break;
        }

        case PDB_VALUE_TYPE_DOUBLE:
        {
            double d_val = va_arg(args, double);
            double* pd = pdb_malloc(sizeof(double));
            *pd = d_val;
            v->ptr = pd;
            v->type = PDB_VALUE_TYPE_DOUBLE;
        }
            
    }
 
    v->expire_time = 0;
    v->lru = 0;
    v->ref_count = 1;

    va_end(args);

    return v;
}

void pdb_destroy_value(pdb_value* value){
    if (value->type == PDB_VALUE_TYPE_INT){
        pdb_free(value, -1);
        return;
    }
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