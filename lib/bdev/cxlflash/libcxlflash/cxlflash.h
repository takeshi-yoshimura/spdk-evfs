#ifndef LIBCXLFLASH_CXLFLASH_H
#define LIBCXLFLASH_CXLFLASH_H

#include "cxlflash_types.h"
#include "cxlflash_qpair.h"

typedef struct cxlflash_qpair {
    sisl_qpair_t * qpair;
    int fd;
} cxlflash_qpair_t;

cxlflash_qpair_t * cxlflash_open(char *path, int queue_depth);
void cxlflash_close(cxlflash_qpair_t * qpair);
int cxlflash_aread(cxlflash_qpair_t * qpair, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_awrite(cxlflash_qpair_t * qpair, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_aunmap(cxlflash_qpair_t * qpair, void * buf, uint64_t lba, uint64_t nblocks);
int cxlflash_io_completion(cxlflash_qpair_t * qpair, int * ret); // TODO: fix return in param
uint64_t cxlflash_get_last_lba(cxlflash_qpair_t * qpair);
int cxlflash_is_mmapable(cxlflash_qpair_t * qpair);

#endif //LIBCXLFLASH_CXLFLASH_H
