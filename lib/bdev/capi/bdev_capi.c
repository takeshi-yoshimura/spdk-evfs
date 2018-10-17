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

#include "capiblock.h"


#define BLK_SIZE (4 * 1024)
#define MAX_SGDEVS 8


static int bdev_capi_initialize(void);
static void bdev_capi_finish(void);
static void bdev_capi_get_spdk_running_config(FILE *fp);
static int bdev_capi_get_ctx_size(void);

static struct spdk_bdev_module capi_if = {
		.name = "capi",
		.module_init = bdev_capi_initialize,
		.module_fini = bdev_capi_finish,
		.config_text = bdev_capi_get_spdk_running_config,
		.get_ctx_size = bdev_capi_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(&capi_if)

struct capi_bdev {
	struct spdk_bdev disk;
	char *devStr;
	chunk_id_t chunk_id;
	int queue_depth;
	bool unmap_supported;
	TAILQ_ENTRY(capi_bdev) link;
};

static TAILQ_HEAD(, capi_bdev) g_capi_bdev_head = TAILQ_HEAD_INITIALIZER(g_capi_bdev_head);
static int capi_bdev_count = 0;


struct capi_bdev_io {
	int tag;
};

struct capi_io_channel {
	struct spdk_poller *poller;
	TAILQ_HEAD(, spdk_bdev_io) io;
};

static int bdev_capi_destruct(void *ctx)
{
	struct capi_bdev *bdev = ctx;

	TAILQ_REMOVE(&g_capi_bdev_head, bdev, link);
	free(bdev->disk.name);
	spdk_dma_free(bdev);
	return 0;
}

static int bdev_capi_readv(struct capi_bdev *bdev, struct capi_bdev_io *bio,
		  struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t lba)
{
	int i, rc;
	uint64_t src_lba;
	uint64_t remaining_count;

	src_lba = lba;
	remaining_count = lba_count;
	for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
		uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
		if (nblocks > 0) {
			rc = cblk_aread(bdev->chunk_id, iov[i].iov_base, src_lba, nblocks, &bio->tag, 0, 0);
			SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_aread(%d, %p, %ld, %ld, %d, 0, 0)\n", bdev->chunk_id, iov[i].iov_base, src_lba, nblocks, bio->tag);
			if (rc == -EAGAIN || rc == -ENOMEM) {
				return -ENOMEM;
			}
			src_lba += nblocks;
			remaining_count -= nblocks;
		}
	}
	return 0;
}

static void bdev_capi_get_buf_cb(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
	struct capi_io_channel *ch = spdk_io_channel_get_ctx(_ch);
	struct capi_bdev_io *bio = (struct capi_bdev_io *)bdev_io->driver_ctx;
	int ret;

	ret = bdev_capi_readv((struct capi_bdev *)bdev_io->bdev->ctxt,
			  bio,
			  bdev_io->u.bdev.iovs,
			  bdev_io->u.bdev.iovcnt,
			  bdev_io->u.bdev.num_blocks,
			  bdev_io->u.bdev.offset_blocks);

	if (spdk_likely(ret == 0)) {
		TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
		return;
	} else if (ret == -ENOMEM) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
	} else {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static int bdev_capi_writev(struct capi_bdev *bdev, struct capi_io_channel *ch, struct spdk_bdev_io *bdev_io,
		   struct capi_bdev_io *bio,
		   struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t lba)
{
	int i, rc;
	uint64_t dst_lba;
	uint64_t remaining_count;

	dst_lba = lba;
	remaining_count = lba_count;
	for (i = 0; i < iovcnt && 0 < remaining_count; i++) {
		uint64_t nblocks = spdk_min(iov[i].iov_len / BLK_SIZE, remaining_count);
		if (nblocks > 0) {
			rc = cblk_awrite(bdev->chunk_id, iov[i].iov_base, dst_lba, nblocks, &bio->tag, 0, 0);
			SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_awrite(%d, %p, %ld, %ld, %d, 0, 0)\n", bdev->chunk_id, iov[i].iov_base, dst_lba, nblocks, bio->tag);
			if (spdk_unlikely(rc == -EAGAIN || rc == -ENOMEM)) {
				return -ENOMEM;
			}
			dst_lba += nblocks;
			remaining_count -= nblocks;
		}
	}

	TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
	return 0;
}

static void * g_zero_buffer; // allocated at bdev_capi_initialize

static int bdev_capi_unmap(struct capi_bdev *bdev, struct capi_io_channel *ch, struct spdk_bdev_io *bdev_io,
        struct capi_bdev_io *bio,
        uint64_t lba_count, uint64_t lba)
{
	int rc = cblk_aunmap(bdev->chunk_id, g_zero_buffer, lba, lba_count, &bio->tag, 0, 0);
	if (rc == 0) {
		TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
		return 0;
	}
	return errno;
}

static bool bdev_capi_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
    switch (io_type) {
        case SPDK_BDEV_IO_TYPE_READ:
        case SPDK_BDEV_IO_TYPE_WRITE:
        case SPDK_BDEV_IO_TYPE_FLUSH:
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
    struct capi_bdev_io * bio = (struct capi_bdev_io *)bdev_io->driver_ctx;
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
				   bio,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks,
				   bdev_io->u.bdev.offset_blocks);

    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
        if (!bdev->unmap_supported) {
            return bdev_capi_writev(bdev,
                    ch,
                    bdev_io,
                    bio,
                    bdev_io->u.bdev.iovs,
                    bdev_io->u.bdev.iovcnt,
                    bdev_io->u.bdev.num_blocks,
                    bdev_io->u.bdev.offset_blocks);
        }
        // fall through

    case SPDK_BDEV_IO_TYPE_UNMAP:
        return bdev_capi_unmap(bdev, ch, bdev_io, bio, bdev_io->u.bdev.num_blocks,
                                bdev_io->u.bdev.offset_blocks);

	case SPDK_BDEV_IO_TYPE_FLUSH:
        return 0;
	default:
		return -1;
	}
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

static struct spdk_io_channel * bdev_capi_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(&g_capi_bdev_head);
}

static void bdev_capi_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	struct capi_bdev * capi_bdev = (struct capi_bdev *)bdev->ctxt;
	char uuid_str[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "construct_capi_bdev");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_json_write_named_string(w, "devStr", capi_bdev->devStr);
	spdk_json_write_named_int32(w, "queueDepth", capi_bdev->queue_depth);
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


static int capi_io_poll(void *arg)
{
	struct capi_io_channel *ch = arg;
	int rc, c = 0;
	uint64_t status;
	struct spdk_bdev_io *bdev_io;
	int pflag = 0;
	TAILQ_HEAD(, spdk_bdev_io)	io;

	TAILQ_INIT(&io);
	TAILQ_FOREACH(bdev_io, &ch->io, module_link) {
		struct capi_bdev * bdev = (struct capi_bdev *)bdev_io->bdev->ctxt;
		struct capi_bdev_io *bio = (struct capi_bdev_io *)bdev_io->driver_ctx;

		rc = cblk_aresult(bdev->chunk_id, &bio->tag, &status, pflag);
		if (rc > 0) {
			c++;
			SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_aresult(%d, %d, status, %d)=SUCCESS\n", bdev->chunk_id, bio->tag, pflag);
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
			TAILQ_INSERT_TAIL(&io, bdev_io, module_link);
		} else if (rc < 0) {
			SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_aresult(%d, %d, status, %d)=FAIL\n", bdev->chunk_id, bio->tag, pflag);
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			TAILQ_INSERT_TAIL(&io, bdev_io, module_link);
		}
		if (pflag == 0) {
		    pflag = CBLK_ARESULT_NO_HARVEST;
		}
	}

	TAILQ_FOREACH(bdev_io, &io, module_link) {
		TAILQ_REMOVE(&ch->io, bdev_io, module_link);
	}
	return c;
}

static int capi_bdev_create_cb(void *io_device, void *ctx_buf)
{
	struct capi_io_channel *ch = ctx_buf;

	TAILQ_INIT(&ch->io);
	ch->poller = spdk_poller_register(capi_io_poll, ch, 0);

	return 0;
}

static void capi_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
	struct capi_io_channel *ch = ctx_buf;
	spdk_poller_unregister(&ch->poller);
}

struct spdk_bdev *create_capi_bdev(char * name, struct spdk_uuid * uuid, char * devStr, int queue_depth)
{
	struct capi_bdev *bdev;
	int rc;
	chunk_attrs_t attrs;
	size_t lun_size;

	if (queue_depth < 0) {
		SPDK_ERRLOG("queue_depth must be >= 0");
		return NULL;
	}

	bdev = spdk_dma_zmalloc(sizeof(*bdev), 0, NULL);
	if (!bdev) {
		SPDK_ERRLOG("bdev spdk_dma_zmalloc() failed\n");
		return NULL;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_open\n");
	bdev->chunk_id = cblk_open(devStr, queue_depth, O_RDWR, 0, CBLK_OPN_NO_INTRP_THREADS);
	if (bdev->chunk_id == NULL_CHUNK_ID) {
		SPDK_ERRLOG("cblk_open: errno:%d\n", errno);
		goto free_bdev;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_get_attrs\n");
	spdk_mem_all_zero(&attrs, sizeof(attrs));
	cblk_get_attrs(bdev->chunk_id, &attrs, 0);
	bdev->unmap_supported = (attrs.flags1 & CFLSH_ATTR_UNMAP) != 0;

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_get_lun_size\n");
	rc = cblk_get_lun_size(bdev->chunk_id, &lun_size, 0);
	if (rc < 0) {
		SPDK_ERRLOG("cblk_get_lun_size failed: errno: %d\n", errno);
		goto close_cblk;
	}

	if (name) {
		bdev->disk.name = strdup(name);
	} else {
		bdev->disk.name = spdk_sprintf_alloc("CAPI%d", capi_bdev_count++);
	}
	if (!bdev->disk.name) {
		goto close_cblk;
	}

	bdev->devStr = devStr;
	bdev->disk.product_name = "CAPI Flash";
	bdev->disk.write_cache = 0;
	bdev->disk.blocklen = BLK_SIZE;
	bdev->disk.blockcnt = lun_size / BLK_SIZE;
	if (uuid) {
		bdev->disk.uuid = *uuid;
	} else {
		spdk_uuid_generate(&bdev->disk.uuid);
	}
	bdev->disk.ctxt = bdev;
	bdev->disk.fn_table = &capi_fn_table;
	bdev->disk.module = &capi_if;
	bdev->queue_depth = queue_depth;

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "spdk_bdev_register\n");
	rc = spdk_bdev_register(&bdev->disk);
	if (rc) {
		goto free_disk_name;
	}

	TAILQ_INSERT_TAIL(&g_capi_bdev_head, bdev, link);

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "open: %s queue_depth: %d chunk_id:%d lun_size:%ld map: %d\n",
				  devStr, queue_depth, bdev->chunk_id, lun_size, bdev->unmap_supported);

	return &bdev->disk;

free_disk_name:
	free(bdev->disk.name);
close_cblk:
	cblk_close(bdev->chunk_id, 0);
free_bdev:
	spdk_dma_free(bdev);
	return NULL;
}

void delete_bdev_capi(struct spdk_bdev *bdev, spdk_delete_capi_complete cb_fn, void *cb_arg)
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
	int i, rc = 0;

	sp = spdk_conf_find_section(NULL, "CAPI");
	if (sp == NULL) {
		return 0;
	}

	g_zero_buffer = spdk_dma_zmalloc(BLK_SIZE, BLK_SIZE, NULL);
	if (!g_zero_buffer) {
		SPDK_ERRLOG("spdk_dma_zmalloc() failed\n");
		return -ENOMEM;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "cblk_init\n");
	rc = cblk_init(NULL, 0);
	if (rc) {
		SPDK_ERRLOG("cblk_init failed with rc = %d and errno = %d\n",
					rc, errno);
		goto free_buffer;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "spdk_io_device_register\n");
	spdk_io_device_register(&g_capi_bdev_head, capi_bdev_create_cb, capi_bdev_destroy_cb,
			sizeof(struct capi_io_channel));

	i = 0;
	while (true) {
		char * devStr, * qdStr;
		int queue_depth;
		struct spdk_bdev *bdev;

		devStr = spdk_conf_section_get_nmval(sp, "devConf", i, 0);
		if (devStr == NULL) {
			break;
		}
		qdStr = spdk_conf_section_get_nmval(sp, "devConf", i, 1);
		queue_depth = (int)strtol(qdStr, NULL, 10);
		SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "devStr=%s, queueDepth=%d\n", devStr, queue_depth);

		bdev = create_capi_bdev(NULL, NULL, devStr, queue_depth);
		if (bdev == NULL) {
			SPDK_ERRLOG("Could not create capi disk for %s, queue_depth=%d\n", devStr, queue_depth);
			rc = -EINVAL;
			goto term_cblk;
		}
		if (++i >= MAX_SGDEVS) {
			SPDK_ERRLOG("too many devs\n");
			break;
		}
	}
	return 0;

term_cblk:
	cblk_term(NULL, 0);
free_buffer:
	spdk_dma_free(g_zero_buffer);
	return rc;
}

static void _bdev_capi_finish_cb(void *arg)
{
    struct capi_bdev *bdev;

	spdk_dma_free(g_zero_buffer);
    TAILQ_FOREACH(bdev, &g_capi_bdev_head, link) {
        cblk_close(bdev->chunk_id, 0);
    }
	cblk_term(NULL, 0);
}

static void bdev_capi_finish(void)
{
	if (!TAILQ_EMPTY(&g_capi_bdev_head)) {
		spdk_io_device_unregister(&g_capi_bdev_head, _bdev_capi_finish_cb);
	}
}

static void bdev_capi_get_spdk_running_config(FILE *fp)
{
	struct capi_bdev *bdev;
	int idx = 0;

	TAILQ_FOREACH(bdev, &g_capi_bdev_head, link) {
		if (idx++ == 0) {
			fprintf(fp, "\n[CAPI]\n");
		}
		fprintf(fp, "  devConf %s %d\n", bdev->devStr, bdev->queue_depth);
	}
}

static int bdev_capi_get_ctx_size(void)
{
	return sizeof(struct capi_bdev_io);
}

SPDK_LOG_REGISTER_COMPONENT("bdev_capi", SPDK_LOG_BDEV_CAPI)
