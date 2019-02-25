#ifndef LIBCXLFLASH_TLDP_H
#define LIBCXLFLASH_TLDP_H

#include "cxlflash_types.h"

typedef struct cxlflash_mempool {
    void * data;
    void ** list;
    uint32_t list_size, nr_data, data_len;
} cxlflash_mempool_t;

cxlflash_mempool_t * cxlflash_mempool_alloc(uint32_t data_len, uint32_t nr_data);
void cxlflash_mempool_free(cxlflash_mempool_t * pool);
void * cxlflash_mempool_get(cxlflash_mempool_t * pool);
void cxlflash_mempool_put(cxlflash_mempool_t * pool, void *data);
int cxlflash_mempool_index(cxlflash_mempool_t * pool, void *data);
int cxlflash_mempool_empty(cxlflash_mempool_t * pool);

#endif //LIBCXLFLASH_TLDP_H
