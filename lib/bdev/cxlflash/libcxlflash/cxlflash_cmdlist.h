#ifndef LIBCXLFLASH_CXLFLASH_CMDLIST_H
#define LIBCXLFLASH_CXLFLASH_CMDLIST_H

#include "cxlflash_mempool.h"

typedef struct cxlflash_cmd {
    int cmd;
    uint64_t data;
    struct cxlflash_cmd * next, * prev;
} cxlflash_cmd_t;

typedef struct cxlflash_cmdlist {
    cxlflash_mempool_t * mempool;
    cxlflash_cmd_t cmd_head;
    uint32_t size;
} cxlflash_cmdlist_t;


cxlflash_cmdlist_t * cxlflash_cmdlist_alloc(uint32_t max_size);
void cxlflash_cmdlist_free(cxlflash_cmdlist_t * list);
int cxlflash_cmdlist_reserve(cxlflash_cmdlist_t * list);
void cxlflash_cmdlist_revoke_reserve(cxlflash_cmdlist_t * list);
void cxlflash_cmdlist_setlast(cxlflash_cmdlist_t * list, int cmd, uint64_t data);
int cxlflash_cmdlist_remove(cxlflash_cmdlist_t * list, int cmd, uint64_t * data);  // TODO: fix return in param
int cxlflash_cmdlist_size(cxlflash_cmdlist_t * list);
int cxlflash_cmdlist_full(cxlflash_cmdlist_t * list);

#endif //LIBCXLFLASH_CXLFLASH_CMDLIST_H
