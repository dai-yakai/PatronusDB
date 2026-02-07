#include "pdb_log.h"

void _pdb_log_debug_impl(const char* file, int line, const char* func, const char* fmt, ...) {
#if PRINT
    va_list args;
    va_start(args, fmt);

    fprintf(stdout, "[DEBUG] <%s> [%s:%d]: ", file, func, line);
    vfprintf(stdout, fmt, args); 
    // fprintf(stdout, "\n");       

    va_end(args);
#endif
}

void _pdb_log_info_impl(const char* file, int line, const char* func, const char* fmt, ...) {
#if PRINT
    va_list args;
    va_start(args, fmt);

    fprintf(stdout, "[INFO] <%s> [%s:%d]: ", file, func, line);
    vfprintf(stdout, fmt, args); 
    // fprintf(stdout, "\n");       

    va_end(args);
#endif
}

void _pdb_log_error_impl(const char* file, int line, const char* func, const char* fmt, ...) {
#if PRINT
    va_list args;
    va_start(args, fmt);

    fprintf(stdout, "[ERROR] <%s> [%s:%d]: ", file, func, line);
    vfprintf(stdout, fmt, args); 
    // fprintf(stdout, "\n");       

    va_end(args);
#endif
}