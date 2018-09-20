#ifndef LIBCXLFLASH_CXLFLASH_QPAIR_H
#define LIBCXLFLASH_CXLFLASH_QPAIR_H

#include "cxlflash_types.h"
#include "cxlflash_mmio.h"
#include "cxlflash_mempool.h"

/**
 * queue pair should be allocated for each I/O thread
 */

typedef struct sisl_qpair {
    sisl_ioarcb_t ** sq;
    sisl_ioarcb_t ** sq_end;
    sisl_ioarcb_t ** sq_prod, ** sq_cons;
    sisl_ioasa_t ** rrq;
    sisl_ioasa_t ** rrq_end;
    volatile sisl_ioasa_t ** rrq_cons;
    cxlflash_mempool_t * cmd_pool;
    struct sisl_host_map * reg;
    struct dk_cxlflash_attach * attached;
    struct dk_cxlflash_udirect * udirect;
    int unmappable;
    uint32_t toggle;
} sisl_qpair_t;

sisl_qpair_t * cxlflash_qpair_alloc(struct dk_cxlflash_attach * attached, struct dk_cxlflash_udirect * udirect, uint64_t queue_depth);
void cxlflash_qpair_free(sisl_qpair_t * qpair);
int cxlflash_qpair_try_get_cmd_room(sisl_qpair_t * qpair);
int cxlflash_qpair_submit_cmd_nosq(sisl_qpair_t * qpair, void * buf, uint64_t lba, uint32_t nblocks, int op_code);
int cxlflash_qpair_submit_cmd_sq(sisl_qpair_t * qpair, void * buf, uint64_t lba, uint32_t nblocks, int op_code);
int cxlflash_qpair_submit_cmd(sisl_qpair_t * qpair, void * buf, uint64_t lba, uint32_t nblocks, int op_code);
int cxlflash_qpair_get_complete_cmd(sisl_qpair_t * qpair, int *ret); // TODO: fix return in param

#endif //LIBCXLFLASH_CXLFLASH_QPAIR_H
