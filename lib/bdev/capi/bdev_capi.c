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

#include "bdev_capi.h"
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

#include "capiflash/src/include/capiblock.h"
#include "capiflash/src/include/pbuf.h"


#define BLK_SIZE (4 * 1024)
#define MAX_SGDEVS 8


static int bdev_capi_initialize(void);
static void bdev_capi_get_spdk_running_config(FILE *fp);
static int bdev_capi_get_ctx_size(void);

static struct spdk_bdev_module capi_if = {
		.name = "capi",
		.module_init = bdev_capi_initialize,
		.config_text = bdev_capi_get_spdk_running_config,
		.get_ctx_size = bdev_capi_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(&capi_if)

struct capi_bdev {
	struct spdk_bdev disk;
	chunk_id_t chunk_id;
	size_t lba;
	size_t last_lba;
	uint32_t queue_depth;
	bool intrp_thds;
	bool seq;
	bool plun;
	int vlunsize;
	bool unmap_supported;
	TAILQ_ENTRY(capi_bdev) link;
};

static TAILQ_HEAD(, capi_bdev) g_capi_bdev_head = TAILQ_HEAD_INITIALIZER(g_capi_bdev_head);
static int capi_bdev_count = 0;


struct capi_io_channel {
	struct spdk_poller	*poller;
	int *tags;
	TAILQ_HEAD(, spdk_bdev_io)	io; // TODO: enhancing bdev_io to have a tag
};

static void
capi_disk_free(struct capi_bdev *capi_disk)
{
	if (!capi_disk) {
		return;
	}

	free(capi_disk->disk.name);
	spdk_dma_free(capi_disk);
	cblk_term(NULL, 0);
}

static int
bdev_capi_destruct(void *ctx)
{
	struct capi_bdev *capi_disk = ctx;

	TAILQ_REMOVE(&g_capi_bdev_head, capi_disk, link);
	capi_disk_free(capi_disk);
	return 0;
}

static int
bdev_capi_readv(struct capi_bdev *bdev, struct capi_io_channel *ch,
		  struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t lba)
{
	int i, rc = 0;
	uint64_t src_lba;
	uint64_t remaining_count;

	src_lba = lba;
	remaining_count = lba_count;
	for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
		uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
		if (nblocks > 0) {
            rc = cblk_aread(bdev->chunk_id, iov[i].iov_base, src_lba, nblocks, &ch->tag, 0, 0);
            if (rc < 0) {
                return errno;
            }
            src_lba += nblocks * BLK_SIZE;
            remaining_count -= nblocks;
        }
	}
	return 0;
}

static void
bdev_capi_get_buf_cb(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
    struct capi_io_channel *ch = spdk_io_channel_get_ctx(_ch);
    int ret;

    ret = bdev_capi_readv((struct capi_bdev *)bdev_io->bdev->ctxt,
                          ch,
                          bdev_io->u.bdev.iovs,
                          bdev_io->u.bdev.iovcnt,
                          bdev_io->u.bdev.num_blocks,
                          bdev_io->u.bdev.offset_blocks * bdev_io->bdev->blocklen);

    if (spdk_likely(ret == 0)) {
        TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
        return;
    } else if (ret == -ENOMEM) {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
    } else {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
    }
}

static int
bdev_capi_writev(struct capi_bdev *bdev, struct capi_io_channel *ch, struct spdk_bdev_io *bdev_io,
		   struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t lba)
{
    int i, rc, tag;
    uint64_t dst_lba;
    uint64_t remaining_count;

    dst_lba = lba;
    remaining_count = lba_count;
    for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
        uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
        if (nblocks > 0) {
            rc = cblk_awrite(bdev->chunk_id, iov[i].iov_base, dst_lba, nblocks, &ch->tag, 0, 0);
            if (spdk_unlikely(rc < 0)) {
                return errno;
            }
            dst_lba += nblocks * BLK_SIZE;
            remaining_count -= nblocks;
        }
    }

    TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
    return 0;
}

static void * g_zero_buffer; // allocated at bdev_capi_initialize

static int
bdev_capi_unmap(struct capi_bdev *bdev, struct capi_io_channel *ch, struct spdk_bdev_io *bdev_io,
        uint64_t lba_count, uint64_t lba)
{
    int tag;
    int rc = cblk_aunmap(bdev->chunk_id, g_zero_buffer, lba, lba_count, &ch->tag, 0, 0);
    if (rc == 0) {
        TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
        return 0;
    }
    return errno;
}

static int
bdev_capi_reset(struct capi_bdev *mdisk)
{
    /* First, delete all NVMe I/O queue pairs. */
    spdk_for_each_channel(nbdev->nvme_ctrlr->ctrlr,
                          _bdev_nvme_reset_destroy_qpair,
                          bio,
                          _bdev_nvme_reset);

	return 0;
}

static bool
bdev_capi_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
    switch (io_type) {
        case SPDK_BDEV_IO_TYPE_READ:
        case SPDK_BDEV_IO_TYPE_WRITE:
        case SPDK_BDEV_IO_TYPE_FLUSH:
        case SPDK_BDEV_IO_TYPE_RESET:
        case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
            return true;

        case SPDK_BDEV_IO_TYPE_UNMAP:
            return ((struct capi_bdev *) ctx)->unmap_supported;

        default:
            return false;
    }
}

static int _bdev_capi_submit_request(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
    struct capi_bdev * bdev = (struct capi_bdev *)bdev_io->bdev->ctxt;
    struct capi_io_channel *ch = spdk_io_channel_get_ctx(_ch);
	uint32_t block_size = bdev_io->bdev->blocklen;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
        // handle a case where iov[0].iov_base == NULL
        spdk_bdev_io_get_buf(bdev_io, bdev_capi_get_buf_cb, bdev_io->u.bdev.num_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		return bdev_capi_writev(bdev,
				   ch,
				   bdev_io,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks * block_size,
				   bdev_io->u.bdev.offset_blocks * block_size);

    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
        if (!bdev->unmap_supported) {
            return bdev_capi_writev(bdev,
                    ch,
                    bdev_io,
                    bdev_io->u.bdev.iovs,
                    bdev_io->u.bdev.iovcnt,
                    bdev_io->u.bdev.num_blocks * block_size,
                    bdev_io->u.bdev.offset_blocks * block_size);
        }
        // fall through

    case SPDK_BDEV_IO_TYPE_UNMAP:
        return bdev_capi_unmap(bdev, ch, bdev_io, bdev_io->u.bdev.num_blocks * block_size,
                                bdev_io->u.bdev.offset_blocks * block_size);

    case SPDK_BDEV_IO_TYPE_RESET:
		return bdev_capi_reset(bdev);

	case SPDK_BDEV_IO_TYPE_FLUSH:
        return 0;
	default:
		return -1;
	}
	return 0;
}

static void bdev_capi_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	int rc = _bdev_capi_submit_request(ch, bdev_io);

    if (spdk_unlikely(rc != 0)) {
        if (rc == -ENOMEM) {
            spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
        } else {
            spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        }
    }
}

static struct spdk_io_channel *
bdev_capi_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(&g_capi_bdev_head);
}

static void
bdev_capi_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	char uuid_str[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "construct_capi_bdev");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_json_write_named_uint64(w, "num_blocks", bdev->blockcnt);
	spdk_json_write_named_uint32(w, "block_size", bdev->blocklen);
	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
	spdk_json_write_named_string(w, "uuid", uuid_str);

	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table capi_fn_table = {
	.destruct		= bdev_capi_destruct,
	.submit_request		= bdev_capi_submit_request,
	.io_type_supported	= bdev_capi_io_type_supported,
	.get_io_channel		= bdev_capi_get_io_channel,
	.write_config_json	= bdev_capi_write_json_config,
};




static int
capi_io_poll(void *arg)
{
	struct capi_io_channel *ch = arg;
    TAILQ_HEAD(, spdk_bdev_io)	io;
    struct spdk_bdev_io		*bdev_io;
    int status, rc;

	TAILQ_INIT(&io);
	TAILQ_SWAP(&ch->io, &io, spdk_bdev_io, module_link);

	if (TAILQ_EMPTY(&io)) {
		return 0;
	}

    if (ch->tag == -1) {
        return 0;
    }

	while (!TAILQ_EMPTY(&io)) {
        struct capi_bdev * bdev;
		bdev_io = TAILQ_FIRST(&io);
        bdev = (struct capi_bdev *)bdev_io->bdev->ctxt;
        if (bdev->intrp_thds) {
            rc = cblk_aresult(bdev->chunk_id, &ch->tag, &status, CBLK_ARESULT_BLOCKING);
        } else {
            rc = cblk_aresult(bdev->chunk_id, &ch->tag, &status, 0);
        }
		// TODO: check result
		TAILQ_REMOVE(&io, bdev_io, module_link);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
	}

	return 1;
}

static int
capi_bdev_create_cb(void *io_device, void *ctx_buf)
{
	struct capi_io_channel *ch = ctx_buf;

	TAILQ_INIT(&ch->io);
	ch->poller = spdk_poller_register(capi_io_poll, ch, 0);
	ch->tag = -1;

	return 0;
}

static void
capi_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
	struct capi_io_channel *ch = ctx_buf;
	spdk_poller_unregister(&ch->poller);
}

struct spdk_bdev *create_capi_bdev(const char *devStr, uint32_t queue_depth,
								   bool intrp_thds, bool seq, bool plun, uint32_t vlunsize,
								   uint64_t num_blocks)
{
	struct capi_bdev	*mdisk;
	int			rc;
	int flags = 0;
	size_t lun_size;
    chunk_attrs_t attrs;

	if (num_blocks == 0) {
		SPDK_ERRLOG("Disk must be more than 0 blocks\n");
		return NULL;
	}

	mdisk = spdk_dma_zmalloc(sizeof(*mdisk), 0, NULL);
	if (!mdisk) {
		SPDK_ERRLOG("mdisk spdk_dma_zmalloc() failed\n");
		return NULL;
	}

	rc = cblk_init(NULL, 0);
	if (rc) {
		SPDK_ERRLOG("cblk_init failed with rc = %d and errno = %d\n",
				rc, errno);
        spdk_dma_free(mdisk);
		return NULL;
	}

    spdk_mem_all_zero(&attrs, sizeof(attrs));
    cblk_get_attrs(mdisk->chunk_id, &attrs, 0);
    mdisk->unmap_supported = (attrs.flags1 & CFLSH_ATTR_UNMAP) != 0;

	if (!plun)
		flags  = CBLK_OPN_VIRT_LUN;
	if (!intrp_thds)
		flags |= CBLK_OPN_NO_INTRP_THREADS;

	mdisk->chunk_id = cblk_open(devStr, queue_depth, O_RDWR, 0, flags);

	if (mdisk->chunk_id == NULL_CHUNK_ID) {
		SPDK_ERRLOG("cblk_open: errno:%d\n", errno);
		goto free_disk;
	}

	rc = cblk_get_lun_size(mdisk->chunk_id, &lun_size, 0);
	if (rc) {
		SPDK_ERRLOG("cblk_get_lun_size failed: errno: %d\n", errno);
		goto free_disk;
	}

	if (plun) {
		mdisk->last_lba = lun_size - 1;
	} else {
		mdisk->last_lba = vlunsize > lun_size ? lun_size - 1 : vlunsize - 1;
		rc = cblk_set_size(mdisk->chunk_id, mdisk->last_lba + 1, 0);
		if (rc) {
			SPDK_ERRLOG("cblk_set_size failed, errno: %d\n", errno);
			goto free_disk;
		}
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "open: %s dev:%p id:%d nblks:%ld\n",
		  devStrs[i],dev, dev->id, dev->last_lba+1);

	// bump lba, reset lba if it wraps
	if (seq) {
        mdisk->lba += num_blocks;
        if (mdisk->lba + num_blocks >= mdisk->last_lba) {
        	mdisk->lba = 0;
        }
    } else {
		mdisk->lba = lrand48() % mdisk->last_lba;
	}

	mdisk->disk.name = spdk_sprintf_alloc("CAPI%d", capi_bdev_count);
	capi_bdev_count++;
	if (!mdisk->disk.name) {
		goto free_disk;
	}
	mdisk->disk.product_name = "CAPI Flash";
	mdisk->disk.write_cache = 1;
	mdisk->disk.blocklen = BLK_SIZE;
	mdisk->disk.blockcnt = num_blocks;
	spdk_uuid_generate(&mdisk->disk.uuid);
	mdisk->disk.ctxt = mdisk;
	mdisk->disk.fn_table = &capi_fn_table;
	mdisk->disk.module = &capi_if;
	mdisk->queue_depth = queue_depth;
	mdisk->intrp_thds = intrp_thds;
	mdisk->seq = seq;
	mdisk->plun = plun;
	mdisk->vlunsize = vlunsize;

	rc = spdk_bdev_register(&mdisk->disk);
	if (rc) {
		goto free_disk;
	}

	TAILQ_INSERT_TAIL(&g_capi_bdev_head, mdisk, link);

	return &mdisk->disk;

free_disk:
	capi_disk_free(mdisk);
	return NULL;
}

void
delete_bdev_capi(struct spdk_bdev *bdev, spdk_delete_capi_complete cb_fn, void *cb_arg)
{
	if (!bdev || bdev->module != &capi_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static int bdev_capi_initialize(void)
{
	struct spdk_conf_section *sp;
	int i, rc = 0, devN = 0;
	char *devStr, *pstr;
	char devStrs[MAX_SGDEVS][64];
	int queue_depth;
	bool intrp_thds;
	bool seq;
	bool plun;
	int vlunsize,
	struct spdk_bdev *bdev;

	sp = spdk_conf_find_section(NULL, "CAPI");
	if (sp == NULL) {
		return 0;
	}

	g_zero_buffer = spdk_dma_zmalloc(BLK_SIZE, BLK_SIZE, NULL);
	if (g_zero_buffer) {
        SPDK_ERRLOG("spdk_dma_zmalloc() failed\n");
        return -ENOMEM;
	}

	spdk_io_device_register(&g_capi_bdev_head, capi_bdev_create_cb, capi_bdev_destroy_cb,
			sizeof(struct capi_io_channel));

	devStr = spdk_conf_section_get_nmval(sp, "devStr", i, 1);

	while (( pstr = strsep(&devStr, ":"))) {
		if (devN == MAX_SGDEVS) {
			SPDK_ERRLOG("too many devs\n");
			break;
		}
		SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "%s\n", pstr);
		sprintf(devStrs[devN++], "%s", pstr);
	}
	queue_depth = spdk_conf_section_get_intval(sp, "queueDepth");
	intrp_thds = spdk_conf_section_get_boolval(sp, "interruptMode", false);
	seq = spdk_conf_section_get_boolval(sp, "sequentialLunAlloc", false);
	plun = spdk_conf_section_get_boolval(sp, "physicalLunMode", false);
	vlunsize = spdk_conf_section_get_intval(sp, "vlunSizeInGB");
	if (queue_depth < 1) {
		queue_depth = 100;
	}
	if (plun) {
		vlunsize = -1;
	} else {
		if (vlunsize < 0) {
			vlunsize = 1;
		}
		vlunsize *= 256 * 1024;
	}

	for (i = 0; i < devN; i++) {
		bdev = create_capi_bdev(devStr[i], queue_depth, intrp_thds, seq, plun, vlunsize, num_blocks);
		if (bdev == NULL) {
			SPDK_ERRLOG("Could not create capi disk\n");
			rc = EINVAL;
			goto end;
		}
	}

end:
	return rc;
}

static void
bdev_capi_get_spdk_running_config(FILE *fp)
{
	int num_capi_luns = 0;
	int queue_depth = 0;
	bool intrp_thds = false;
	bool seq = false;
	bool plun = false;
	int vlunsize = 0;
	struct capi_bdev *mdisk;

	/* count number of malloc LUNs, get LUN size */
	TAILQ_FOREACH(mdisk, &g_capi_bdev_head, link) {
		if (0 == num_capi_luns) {
			/* assume all malloc luns the same size */
			queue_depth = mdisk->queue_depth;
			intrp_thds = mdisk->intrp_thds;
			seq = mdisk->seq;
			plun = mdisk->plun;
			vlunsize = mdisk->vlunsize / 256 / 1024;
		}
		num_capi_luns++;
	}

	if (num_capi_luns > 0) {
		fprintf(fp,
			"\n"
			"# Users may change this section to create a different number or size of\n"
			"# malloc LUNs.\n"
			"# This will generate %d LUNs with a malloc-allocated backend. Each LUN\n"
			"# will be %" PRIu64 "MB in size and these will be named CAPI0 through CAPI%d.\n"
			"# Not all LUNs defined here are necessarily used below.\n"
			"[CAPI]\n"
			"  queueDepth %d\n"
			"  interruptMode %d\n"
			"  sequentialLunAlloc %d\n"
			"  physicalLunMode %d\n"
			"  vlunSizeInGB %d\n",
			queue_depth, intrp_thds, seq, plun, vlunsize);
	}
}

static int
bdev_capi_get_ctx_size(void)
{
	return sizeof(struct capi_bdev);
}

SPDK_LOG_REGISTER_COMPONENT("bdev_capi", SPDK_LOG_BDEV_CAPI)
