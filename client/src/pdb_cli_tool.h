#ifndef __PDB_CLI_TOOL__
#define __PDB_CLI_TOOL__

int connect_tcpserver(const char* ip, unsigned short port);
void print_progress(const char* prefix, int current, int total);

#endif