#ifndef __PDB_SERVER_H__
#define __PDB_SERVER_H__

#include "pdb_handler.h"
#include "pdb_replication.h"
#include "pdb_conninfo.h"
#include "pdb_sds.h"

#define BUFFER_LENGTH		1024*1024*32
#define RESPONSE_LENGTH		1024*1024*32
#define BUFFER_MASK     (BUFFER_LENGTH - 1)

int pdb_request(struct conn_info *c, msg_handler handler);
int pdb_response(struct conn_info *c, msg_handler handler);

#endif