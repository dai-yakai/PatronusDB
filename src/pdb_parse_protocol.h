#ifndef __PDB_PARSE_PROTOCOL_H__
#define __PDB_PARSE_PROTOCOL_H__

#include <stdlib.h>
#include <stdio.h>

#include "pdb_array.h"
#include "pdb_hash.h"
#include "pdb_rbtree.h"
#include "pdb_skiptable.h"
#include "pdb_rdb.h"
#include "pdb_dump.h"
#include "pdb_aof.h"
#include "pdb_bitmap.h"
#include "pdb_value.h"
#include "pdb_set.h"
#include "pdb_sortedSet.h"


char* find_crlf(char* start, int remaining_len);
int pdb_split_token(char* msg, int len, char* tokens[]);
int pdb_parser_cmd(const char* cmd_str);
int check_resp_integrity(const char *buf, int len, int* bulk_length);
int pdb_filter_protocol(char** tokens, int count, char* response);
int pdb_protocol(char* msg, int length, char* out);

#endif