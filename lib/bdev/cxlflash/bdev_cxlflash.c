/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "bdev_cxlflash.h"
#include "spdk/bdev.h"
#include "spdk/conf.h"
#include "spdk/endian.h"
#include "spdk/env.h"
#include "spdk/copy_engine.h"
#include "spdk/json.h"
#include "spdk/thread.h"
#include "spdk/queue.h"
#include "spdk/string.h"
#include "spdk/likely.h"

#include "spdk/bdev_module.h"
#include "spdk_internal/log.h"

#include "cxlflash.h"
#include "cxlflash_qpair.h"
#include "cxlflash_ioctl.h"
#include "cxlflash_cmdlist.h"


#define BLK_SIZE (4 * 1024)
#define MAX_SGDEVS 8


static int bdev_cxlflash_initialize(void);

static void bdev_cxlflash_finish(void);

static void bdev_cxlflash_get_spdk_running_config(FILE *fp);

static int bdev_cxlflash_get_ctx_size(void);

static struct spdk_bdev_module cxlflash_if = {
        .name = "cxlflash",
        .module_init = bdev_cxlflash_initialize,
        .module_fini = bdev_cxlflash_finish,
        .config_text = bdev_cxlflash_get_spdk_running_config,
        .get_ctx_size = bdev_cxlflash_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(&cxlflash_if)

struct cxlflash_bdev {
    struct spdk_bdev disk;
    char *devStr;
    int queue_depth;
    bool unmap_supported;
    TAILQ_ENTRY(cxlflash_bdev) link;
};

static TAILQ_HEAD(, cxlflash_bdev) g_cxlflash_bdev_head = TAILQ_HEAD_INITIALIZER(g_cxlflash_bdev_head);
static int cxlflash_bdev_count = 0;

struct cxlflash_bdev_io {
    struct spdk_bdev_io * bio;
    int nr_wait_cmds;
    int nr_completed;
    int nr_failed;
    int failed_at_request;
};

struct cmd_dummy {
    int cmd;
};

struct cxlflash_io_channel {
    struct spdk_poller * poller;
    cxlflash_qpair_t * qpair;
    cxlflash_cmdlist_t * cmdlist;
};

static int bdev_cxlflash_destruct(void *ctx) {
    struct cxlflash_bdev *bdev = ctx;

    TAILQ_REMOVE(&g_cxlflash_bdev_head, bdev, link);
    free(bdev->disk.name);
    spdk_dma_free(bdev);
    return 0;
}

static int
bdev_cxlflash_readv(struct cxlflash_io_channel *ch, struct spdk_bdev_io *bdev_io, struct iovec *iov, int iovcnt,
                    uint64_t lba_count, uint64_t lba) {
    int i;
    uint64_t src_lba;
    uint64_t remaining_count;
    struct cxlflash_bdev_io * bio = (struct cxlflash_bdev_io *)bdev_io->driver_ctx;

    src_lba = lba;
    remaining_count = lba_count;
    bio->nr_wait_cmds = bio->nr_completed = bio->nr_failed = 0;

    for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
        uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
        if (nblocks > 0) {
            int rc;
            rc = cxlflash_cmdlist_reserve(ch->cmdlist);
            if (rc) {
                SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "cmdlist is full\n");
                ++bio->failed_at_request;
                return -1;
            }
            rc = cxlflash_aread(ch->qpair, iov[i].iov_base, src_lba, nblocks);
            SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "cxlflash_aread(%p, %p, %ld, %ld): %d, %p\n", ch->qpair,
                          iov[i].iov_base, src_lba, nblocks, rc, bdev_io);
            if (spdk_unlikely(rc < 0)) {
                ++bio->failed_at_request;
                return -1;
            }
            cxlflash_cmdlist_setlast(ch->cmdlist, rc, (uint64_t) bdev_io);
            ++bio->nr_wait_cmds;
            src_lba += nblocks * BLK_SIZE;
            remaining_count -= nblocks;
        }
    }
    return 0;
}

static void bdev_cxlflash_get_buf_cb(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io) {
    struct cxlflash_io_channel *ch = spdk_io_channel_get_ctx(_ch);
    int ret;

    ret = bdev_cxlflash_readv(ch,
                              bdev_io,
                              bdev_io->u.bdev.iovs,
                              bdev_io->u.bdev.iovcnt,
                              bdev_io->u.bdev.num_blocks,
                              bdev_io->u.bdev.offset_blocks * bdev_io->bdev->blocklen);

    if (spdk_likely(ret == 0)) {
        return;
    } else {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
    }
}

static int bdev_cxlflash_writev(struct cxlflash_io_channel *ch, struct spdk_bdev_io *bdev_io,
                                struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t lba) {
    int i;
    uint64_t dst_lba;
    uint64_t remaining_count;
    struct cxlflash_bdev_io * bio = (struct cxlflash_bdev_io *)bdev_io->driver_ctx;

    dst_lba = lba;
    remaining_count = lba_count;
    bio->nr_wait_cmds = bio->nr_completed = bio->nr_failed = 0;

    for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
        uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
        if (nblocks > 0) {
            int rc;
            rc = cxlflash_cmdlist_reserve(ch->cmdlist);
            if (rc) {
                SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "cmdlist is full\n");
                ++bio->failed_at_request;
                return -1;
            }
            rc = cxlflash_awrite(ch->qpair, iov[i].iov_base, dst_lba, nblocks);
            SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "cxlflash_awrite(%p, %p, %ld, %ld): %d, %p\n", ch->qpair,
                          iov[i].iov_base, dst_lba, nblocks, rc, bdev_io);
            if (spdk_unlikely(rc < 0)) {
                /**
                 * TODO: we cannot revoke the writen data on disk. how can we fix this?
                 * */
                ++bio->failed_at_request;
                return -1;
            }
            cxlflash_cmdlist_setlast(ch->cmdlist, rc, (uint64_t) bdev_io);
            ++bio->nr_wait_cmds;
            dst_lba += nblocks * BLK_SIZE;
            remaining_count -= nblocks;
        }
    }

    return 0;
}

static void *g_zero_buffer; // allocated at bdev_cxlflash_initialize

static int bdev_cxlflash_unmap(struct cxlflash_io_channel *ch, struct spdk_bdev_io *bdev_io,
                               uint64_t lba_count, uint64_t lba) {
    struct cxlflash_bdev_io * bio = (struct cxlflash_bdev_io *)bdev_io->driver_ctx;
    int rc;
    rc = cxlflash_cmdlist_reserve(ch->cmdlist);
    if (rc) {
        SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "cmdlist is full\n");
        ++bio->failed_at_request;
        return -1;
    }
    rc = cxlflash_aunmap(ch->qpair, g_zero_buffer, lba, lba_count);
    if (rc >= 0) {
        cxlflash_cmdlist_setlast(ch->cmdlist, rc, (uint64_t) bdev_io);
        ++bio->nr_wait_cmds;
        return 0;
    }
    return rc;
}

static bool bdev_cxlflash_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type) {
    switch (io_type) {
        case SPDK_BDEV_IO_TYPE_READ:
        case SPDK_BDEV_IO_TYPE_WRITE:
        case SPDK_BDEV_IO_TYPE_FLUSH:
        case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
            return true;

        case SPDK_BDEV_IO_TYPE_UNMAP:
            return ((struct cxlflash_bdev *) ctx)->unmap_supported;

        default:
            return false;
    }
}

static int _bdev_cxlflash_submit_request(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io) {
    struct cxlflash_bdev *bdev = (struct cxlflash_bdev *) bdev_io->bdev->ctxt;
    struct cxlflash_io_channel *ch = spdk_io_channel_get_ctx(_ch);
    uint32_t block_size = bdev_io->bdev->blocklen;

    switch (bdev_io->type) {
        case SPDK_BDEV_IO_TYPE_READ:
            // handle a case where iov[0].iov_base == NULL
            spdk_bdev_io_get_buf(bdev_io, bdev_cxlflash_get_buf_cb, bdev_io->u.bdev.num_blocks * block_size);
            return 0;

        case SPDK_BDEV_IO_TYPE_WRITE:
            return bdev_cxlflash_writev(ch,
                                        bdev_io,
                                        bdev_io->u.bdev.iovs,
                                        bdev_io->u.bdev.iovcnt,
                                        bdev_io->u.bdev.num_blocks * block_size,
                                        bdev_io->u.bdev.offset_blocks * block_size);

        case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
            if (!bdev->unmap_supported) {
                return bdev_cxlflash_writev(ch,
                                            bdev_io,
                                            bdev_io->u.bdev.iovs,
                                            bdev_io->u.bdev.iovcnt,
                                            bdev_io->u.bdev.num_blocks * block_size,
                                            bdev_io->u.bdev.offset_blocks * block_size);
            }
            // fall through

        case SPDK_BDEV_IO_TYPE_UNMAP:
            return bdev_cxlflash_unmap(ch, bdev_io, bdev_io->u.bdev.num_blocks * block_size,
                                       bdev_io->u.bdev.offset_blocks * block_size);

        case SPDK_BDEV_IO_TYPE_FLUSH:
            return 0;
        default:
            return -1;
    }
}

static void bdev_cxlflash_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io) {
    int rc = _bdev_cxlflash_submit_request(ch, bdev_io);

    if (spdk_unlikely(rc != 0)) {
        if (rc == -ENOMEM) {
            spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
        } else {
            spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        }
    }
}

static struct spdk_io_channel *bdev_cxlflash_get_io_channel(void *ctx) {
    return spdk_get_io_channel(&g_cxlflash_bdev_head);
}

static void bdev_cxlflash_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w) {
    struct cxlflash_bdev *cxlflash_bdev = (struct cxlflash_bdev *) bdev->ctxt;
    char uuid_str[SPDK_UUID_STRING_LEN];

    spdk_json_write_object_begin(w);

    spdk_json_write_named_string(w, "method", "construct_cxlflash_bdev");

    spdk_json_write_named_object_begin(w, "params");
    spdk_json_write_named_string(w, "name", bdev->name);
    spdk_json_write_named_string(w, "devStr", cxlflash_bdev->devStr);
    spdk_json_write_named_int32(w, "queueDepth", cxlflash_bdev->queue_depth);
    spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
    spdk_json_write_named_string(w, "uuid", uuid_str);

    spdk_json_write_object_end(w);

    spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table cxlflash_fn_table = {
        .destruct        = bdev_cxlflash_destruct,
        .submit_request        = bdev_cxlflash_submit_request,
        .io_type_supported    = bdev_cxlflash_io_type_supported,
        .get_io_channel        = bdev_cxlflash_get_io_channel,
        .write_config_json    = bdev_cxlflash_write_json_config,
};


static int cxlflash_io_poll(void *arg) {
    struct cxlflash_io_channel *ch = arg;
    int c = 0;

    while (cxlflash_cmdlist_size(ch->cmdlist) > 0) {
        int cmd;
        uint64_t data;
        int rc = cxlflash_io_completion(ch->qpair, &cmd);
        if (rc > 0) {
            rc = cxlflash_cmdlist_remove(ch->cmdlist, cmd, &data);
            SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "complete: %d, %p, %d\n", cmd, (void *) data, rc);
            if (rc == 0) {
                struct spdk_bdev_io *bio = (struct spdk_bdev_io *) data;
                struct cxlflash_bdev_io *cbio = (struct cxlflash_bdev_io *) bio->driver_ctx;
                ++cbio->nr_completed;

                if (cbio->failed_at_request) {
                    // we already informed an error to user
                    continue;
                }
                if (cbio->nr_wait_cmds == cbio->nr_completed) {
                    c++;
                    spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_SUCCESS);
                } else if (cbio->nr_wait_cmds == cbio->nr_completed + cbio->nr_failed) {
                    spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
                }
            }
        } else if (rc < 0) {
            SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "failed: %d, %p, %d\n", cmd, (void *) data, rc);
            rc = cxlflash_cmdlist_remove(ch->cmdlist, cmd, &data);
            if (rc == 0) {
                struct spdk_bdev_io *bio = (struct spdk_bdev_io *) data;
                struct cxlflash_bdev_io *cbio = (struct cxlflash_bdev_io *) bio->driver_ctx;
                ++cbio->nr_failed;

                if (cbio->failed_at_request) {
                    // we already informed an error to user
                    continue;
                }
                if (cbio->nr_wait_cmds == cbio->nr_completed + cbio->nr_failed) {
                    spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
                }
            }
            abort();
        } else {
            break;
        }
    }

    return c;
}

static char *g_devStr;
static uint64_t g_queue_depth;

static int cxlflash_bdev_create_cb(void *io_device, void *ctx_buf) {
    struct cxlflash_io_channel *ch = ctx_buf;

    ch->cmdlist = cxlflash_cmdlist_alloc(g_queue_depth);
    if (!ch->cmdlist) {
        spdk_poller_unregister(&ch->poller);
        SPDK_ERRLOG("failed to create cxlflash_cmdlist_alloc(%lu)\n", g_queue_depth);
        return -1;
    }

    ch->qpair = cxlflash_open(g_devStr, g_queue_depth);
    if (!ch->qpair) {
        cxlflash_cmdlist_free(ch->cmdlist);
        spdk_poller_unregister(&ch->poller);
        SPDK_ERRLOG("failed to create cxlflash_open(%s, %lu)\n", g_devStr, g_queue_depth);
        return -1;
    }
    ch->poller = spdk_poller_register(cxlflash_io_poll, ch, 0);

    SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "create qpair: %p\n", ch->qpair);
    return 0;
}

static void cxlflash_bdev_destroy_cb(void *io_device, void *ctx_buf) {
    struct cxlflash_io_channel *ch = ctx_buf;
    spdk_poller_unregister(&ch->poller);
    cxlflash_close(ch->qpair);
    cxlflash_cmdlist_free(ch->cmdlist);
}

struct spdk_bdev *create_cxlflash_bdev(char *name, struct spdk_uuid *uuid, char *devStr, int queue_depth) {
    struct cxlflash_bdev *bdev;
    int rc;
    size_t lun_size;
    cxlflash_qpair_t *qpair; /* tmp qpair */

    if (queue_depth < 0) {
        SPDK_ERRLOG("queue_depth must be >= 0");
        return NULL;
    }

    bdev = spdk_dma_zmalloc(sizeof(*bdev), 0, NULL);
    if (!bdev) {
        SPDK_ERRLOG("bdev spdk_dma_zmalloc() failed\n");
        return NULL;
    }

    qpair = cxlflash_open(devStr, O_RDWR);
    if (!qpair) {
        SPDK_ERRLOG("cxlflash_open failed\n");
        goto free_bdev;
    }
    g_devStr = devStr;
    g_queue_depth = queue_depth;

    if (name) {
        bdev->disk.name = strdup(name);
    } else {
        bdev->disk.name = spdk_sprintf_alloc("CXLFLASH%d", cxlflash_bdev_count++);
    }
    if (!bdev->disk.name) {
        goto close_cblk;
    }

    lun_size = cxlflash_get_last_lba(qpair);

    bdev->devStr = devStr;
    bdev->disk.product_name = "cxlflash";
    bdev->disk.write_cache = 0;
    bdev->disk.blocklen = BLK_SIZE;
    bdev->disk.blockcnt = lun_size;
    if (uuid) {
        bdev->disk.uuid = *uuid;
    } else {
        spdk_uuid_generate(&bdev->disk.uuid);
    }
    bdev->disk.ctxt = bdev;
    bdev->disk.fn_table = &cxlflash_fn_table;
    bdev->disk.module = &cxlflash_if;
    bdev->queue_depth = queue_depth;

    SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "spdk_bdev_register\n");
    rc = spdk_bdev_register(&bdev->disk);
    if (rc) {
        goto free_disk_name;
    }

    TAILQ_INSERT_TAIL(&g_cxlflash_bdev_head, bdev, link);

    SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "open: %s queue_depth: %d lun_size:%ld map: %d\n",
                  devStr, queue_depth, lun_size, bdev->unmap_supported);

    cxlflash_close(qpair);

    return &bdev->disk;

    free_disk_name:
    free(bdev->disk.name);
    close_cblk:
    cxlflash_close(qpair);
    free_bdev:
    spdk_dma_free(bdev);
    return NULL;
}

void delete_bdev_cxlflash(struct spdk_bdev *bdev, spdk_delete_cxlflash_complete cb_fn, void *cb_arg) {
    if (!bdev || bdev->module != &cxlflash_if) {
        cb_fn(cb_arg, -ENODEV);
        return;
    }

    spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static int bdev_cxlflash_initialize(void) {
    struct spdk_conf_section *sp;
    int i, rc = 0;

    sp = spdk_conf_find_section(NULL, "CXLFLASH");
    if (sp == NULL) {
        return 0;
    }

    g_zero_buffer = spdk_dma_zmalloc(BLK_SIZE, BLK_SIZE, NULL);
    if (!g_zero_buffer) {
        SPDK_ERRLOG("spdk_dma_zmalloc() failed\n");
        return -ENOMEM;
    }

    SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "spdk_io_device_register\n");
    spdk_io_device_register(&g_cxlflash_bdev_head, cxlflash_bdev_create_cb, cxlflash_bdev_destroy_cb,
                            sizeof(struct cxlflash_io_channel));

    i = 0;
    while (true) {
        char *devStr, *qdStr;
        int queue_depth;
        struct spdk_bdev *bdev;

        devStr = spdk_conf_section_get_nmval(sp, "devConf", i, 0);
        if (devStr == NULL) {
            break;
        }
        qdStr = spdk_conf_section_get_nmval(sp, "devConf", i, 1);
        queue_depth = (int) strtol(qdStr, NULL, 10);
        SPDK_DEBUGLOG(SPDK_LOG_BDEV_CXLFLASH, "devStr=%s, queueDepth=%d\n", devStr, queue_depth);

        bdev = create_cxlflash_bdev(NULL, NULL, devStr, queue_depth);
        if (bdev == NULL) {
            SPDK_ERRLOG("Could not create cxlflash disk for %s, queue_depth=%d\n", devStr, queue_depth);
            rc = -EINVAL;
            goto free_buffer;
        }
        if (++i >= MAX_SGDEVS) {
            SPDK_ERRLOG("too many devs\n");
            break;
        }
    }
    return 0;

    free_buffer:
    spdk_dma_free(g_zero_buffer);
    return rc;
}

static void _bdev_cxlflash_finish_cb(void *arg) {
    spdk_dma_free(g_zero_buffer);
}

static void bdev_cxlflash_finish(void) {
    if (!TAILQ_EMPTY(&g_cxlflash_bdev_head)) {
        spdk_io_device_unregister(&g_cxlflash_bdev_head, _bdev_cxlflash_finish_cb);
    }
}

static void bdev_cxlflash_get_spdk_running_config(FILE *fp) {
    struct cxlflash_bdev *bdev;
    int idx = 0;

    TAILQ_FOREACH(bdev, &g_cxlflash_bdev_head, link) {
        if (idx++ == 0) {
            fprintf(fp, "\n[CXLFLASH]\n");
        }
        fprintf(fp, "  devConf %s %d\n", bdev->devStr, bdev->queue_depth);
    }
}

static int bdev_cxlflash_get_ctx_size(void) {
    return sizeof(struct cxlflash_bdev_io);
}

SPDK_LOG_REGISTER_COMPONENT("bdev_cxlflash", SPDK_LOG_BDEV_CXLFLASH)
