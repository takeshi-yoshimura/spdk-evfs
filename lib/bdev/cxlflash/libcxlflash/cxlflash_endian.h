#ifndef LIBCXLFLASH_ENDIAN_H
#define LIBCXLFLASH_ENDIAN_H

#include "cxlflash_types.h"

static inline void out_be64(volatile __u64 *addr, __u64 val) {
    *addr = __builtin_bswap64(val);
}

static inline void out_be32(volatile __u32 *addr, __u32 val) {
    *addr = __builtin_bswap32(val);
}

static inline __u64 in_be64(volatile __u64 *addr) {
    return __builtin_bswap64(*addr);
}

#endif //LIBCXLFLASH_ENDIAN_H
