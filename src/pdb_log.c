#include "pdb_log.h"

int log_info = 0;
int log_debug = 0;
int log_error = 0;

void pdb_log_init(){
    int log_level = global_conf.log_level;
    switch (log_level)
    {
        case 0:
        {
            log_info = 0;
            log_debug = 0;
            log_error = 0;
            break;
        }
        case 1:
        {
            log_info = 1;
            log_debug = 1;
            log_error = 1;
            break;
        }
        case 2:
        {
            log_info = 0;
            log_debug = 1;
            log_error = 1;
            break;
        }
        case 3:
        {
            log_info = 0;
            log_debug = 0;
            log_error = 1;
            break;
        }
    }
}

void _pdb_log_debug_impl(const char* file, int line, const char* func, const char* fmt, ...) {
    if (log_debug){
        va_list args;
        va_start(args, fmt);

        fprintf(stdout, "[DEBUG] <%s> [%s:%d]: ", file, func, line);
        vfprintf(stdout, fmt, args);       

        va_end(args);
    }
}

void _pdb_log_info_impl(const char* file, int line, const char* func, const char* fmt, ...) {
    if (log_info){
        va_list args;
        va_start(args, fmt);

        fprintf(stdout, "[INFO] <%s> [%s:%d]: ", file, func, line);
        vfprintf(stdout, fmt, args);      

        va_end(args);
    }
}

void _pdb_log_error_impl(const char* file, int line, const char* func, const char* fmt, ...) {
    if (log_error){
        va_list args;
        va_start(args, fmt);

        fprintf(stdout, "[ERROR] <%s> [%s:%d]: ", file, func, line);
        vfprintf(stdout, fmt, args);      

        va_end(args);
    }
    
}