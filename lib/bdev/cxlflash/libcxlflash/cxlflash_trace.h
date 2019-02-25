#ifndef LIBCXLFLASH_CXLFLASH_TRACE_H
#define LIBCXLFLASH_CXLFLASH_TRACE_H

#include <stdio.h>

#ifdef NDEBUG
#define debug(fmt, ...) ((void)0)
#else
#define debug(fmt, ...) fprintf(stderr, "%s(%d):" fmt, __func__, __LINE__, ##__VA_ARGS__)
#endif

#endif //LIBCXLFLASH_CXLFLASH_TRACE_H
