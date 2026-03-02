#ifndef __PDB_CORE__
#define __PDB_CORE__

#define PDB_OK                  0
#define PDB_ERROR               -1
#define PDB_BUFFER_ERROR        -2
#define PDB_PROTOCAL_ERROR      -3
#define PDB_MALLOC_NULL         -4

#define PDB_ARRAY_OK            1
#define PDB_ARRAY_EXIST         2
#define PDB_ARRAY_NO_EXIST      3
#define PDB_ARRAY_ERROR         4

#define PDB_DATASTRUCTURE_OK            0
#define PDB_DATASTRUCTURE_ERROR         1
#define PDB_DATASTRUCTURE_EXIST         2
#define PDB_DATASTRUCTURE_NOEXIST       3

// 业务返回代码
#define PDB_HALF_PACKAGE        4 
#define PDB_DISCONNECT          5
#endif