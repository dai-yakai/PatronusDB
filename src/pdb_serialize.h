/**
 * RDB 
 * 
 */

#ifndef __PDB_SERIALIZE__
#define __PDB_SERIALIZE__

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "pdb_array.h"
#include "pdb_hash.h"
#include "pdb_rbtree.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"
#include "pdb_bitmap.h"
#include "pdb_value.h"
#include "pdb_sds.h"

#define PDB_OPCODE_HASH      0xFA  // HASH
#define PDB_OPCODE_RBTREE    0xFB  // RBTREE
#define PDB_OPCODE_ARRAY     0xFC  // ARRAY
#define PDB_OPCODE_SET       0xFD  // SET
#define PDB_OPCODE_SSET      0xFE  // SSET
#define PDB_OPCODE_BITMAP    0xF0  // BITMAP
#define PDB_OPCODE_NULL      0xF1   // NULL
#define PDB_OPCODE_INT       0xF2   
#define PDB_OPCODE_EOF       0xFF  // End Of File

#define PDB_RETURN_PARAM_ERROR      -1
#define PDB_RETURN_OK               0

int _pdb_append(char* buf, size_t buf_len, size_t* offset, void* data, size_t data_len);
int _pdb_append_uint8(char* buf, size_t buf_len, size_t* offset, uint8_t val);
int _pdb_append_int(char* buf, size_t buf_len, size_t* offset, int val);
int _pdb_append_size_t(char* buf, size_t buf_len, size_t* offset, size_t val);
int _pdb_append_long(char* buf, size_t buf_len, size_t* offset, long val);
int _pdb_append_double(char* buf, size_t buf_len, size_t* offset, double val);
int _pdb_append_string(char* buf, size_t buf_len, size_t* offset, char* val);

int _pdb_append_value(char* buf, size_t buf_len, size_t* offset, pdb_value* value);
int _pdb_append_set(char* buf, size_t buf_len, size_t* offset, pdb_set* set);
int _pdb_append_sset(char* buf, size_t buf_len, size_t* offset, struct pdb_sorted_set* sset);
int _pdb_append_bitmap(char* buf, size_t buf_len, size_t* offset, pdb_sds s);
int pdb_serialize_hash(char* buf, size_t buf_len, size_t* offset, pdb_hash_t* hash);
int pdb_serialize_array(char* buf, size_t buf_len, size_t* offset, pdb_array_t* array);
int pdb_serialize_rbtree(char* buf, size_t buf_len, size_t* offset, pdb_rbtree_t* rbtree);

int _pdb_read(const char* buf, size_t buf_len, size_t* offset, void* dest, size_t len);
int _pdb_read_uint8(const char* buf, size_t buf_len, size_t* offset, uint8_t* val);
int _pdb_read_int(const char* buf, size_t buf_len, size_t* offset, int* val);
int _pdb_read_size_t(const char* buf, size_t buf_len, size_t* offset, size_t* val);
int _pdb_read_long(const char* buf, size_t buf_len, size_t* offset, long* val);
int _pdb_read_double(const char* buf, size_t buf_len, size_t* offset, double* val);
int _pdb_read_string(const char* buf, size_t buf_len, size_t* offset, char** out_str);

pdb_value* _pdb_deserialize_value(const char* buf, size_t buf_len, size_t* offset);
pdb_hash_t* _pdb_deserialize_inner_hash(const char* buf, size_t buf_len, size_t* offset);
struct pdb_set* _pdb_deserialize_set(const char* buf, size_t buf_len, size_t* offset);
struct pdb_sorted_set* _pdb_deserialize_sset(const char* buf, size_t buf_len, size_t* offset);

int pdb_deserialize_hash(const char* buf, size_t buf_len, size_t* offset, pdb_hash_t* hash);
int pdb_deserialize_array(const char* buf, size_t buf_len, size_t* offset, pdb_array_t* array);
int pdb_deserialize_rbtree(const char* buf, size_t buf_len, size_t* offset, pdb_rbtree_t* rbtree);

#endif