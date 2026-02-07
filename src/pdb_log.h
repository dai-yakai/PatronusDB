#ifndef __PDB_LOG_H__
#define __PDB_LOG_H__

#define PRINT 1

#include <stdio.h>
#include <stdarg.h>

void _pdb_log_debug_impl(const char* file, int line, const char* func, const char* fmt, ...);
void _pdb_log_info_impl(const char* file, int line, const char* func, const char* fmt, ...);
void _pdb_log_error_impl(const char* file, int line, const char* func, const char* fmt, ...);


#define pdb_log_debug(fmt, ...) \
    _pdb_log_debug_impl(__FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)


#define pdb_log_info(fmt, ...) \
    _pdb_log_info_impl(__FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)


#define pdb_log_error(fmt, ...) \
    _pdb_log_error_impl(__FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)




#endif