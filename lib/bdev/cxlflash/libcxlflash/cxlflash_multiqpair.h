#ifndef LIBCXLFLASH_CXLFLASH_MULTIQPAIR_H
#define LIBCXLFLASH_CXLFLASH_MULTIQPAIR_H

#include "cxlflash.h"

typedef struct cxlflash_waiting_qpair_list {
    cxlflash_qpair_t * qpair;
    struct cxlflash_waiting_qpair_list * next, * prev;
} cxlflash_waiting_qpair_list_t;

typedef struct cxlflash_multiqpair {
    cxlflash_qpair_t ** qpairs;
    int nr_qpairs, prod_index;
    cxlflash_mempool_t * mempool;
    cxlflash_waiting_qpair_list_t head;
} cxlflash_multiqpair_t;

cxlflash_multiqpair_t * cxlflash_multiqpair_open(char * path, int queue_depth, int nr_qpairs);
void cxlflash_multiqpair_close(cxlflash_multiqpair_t * qpairs);
int cxlflash_multiqpair_aread(cxlflash_multiqpair_t * qpairs, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_multiqpair_awrite(cxlflash_multiqpair_t * qpairs, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_multiqpair_aunmap(cxlflash_multiqpair_t * qpairs, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_multiqpair_io_completion(cxlflash_multiqpair_t * qpairs, int * ret);
uint64_t cxlflash_multiqpair_get_last_lba(cxlflash_multiqpair_t * qpairs);
int cxlflash_multiqpair_is_mmapable(cxlflash_multiqpair_t * qpairs);

#endif //LIBCXLFLASH_CXLFLASH_MULTIQPAIR_H
