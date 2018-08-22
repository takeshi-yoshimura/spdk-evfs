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

#include "spdk/bdev_module.h"
#include "spdk_internal/log.h"

#include "capiflash/src/include/capiblock.h"
#include "capiflash/src/include/pbuf.h"


#define MAX_SGDEVS 8

struct capi_disk {
	struct spdk_bdev		disk;
	void				*capi_buf;
	pbuf_t					*pbuf;
	chunk_id_t              chunk_id;
	size_t                  lba;
	size_t   				last_lba;
	uint32_t                queue_depth;
	bool intrp_thds;
	bool seq;
	bool plun;
	int vlunsize;
	TAILQ_ENTRY(capi_disk)	link;
};

struct capi_task {
	int				num_outstanding;
	enum spdk_bdev_io_status	status;
};

static struct capi_task *
__capi_task_from_copy_task(struct spdk_copy_task *ct)
{
	return (struct capi_task *)((uintptr_t)ct - sizeof(struct capi_task));
}

static struct spdk_copy_task *
__copy_task_from_capi_task(struct capi_task *mt)
{
	return (struct spdk_copy_task *)((uintptr_t)mt + sizeof(struct capi_task));
}

static void
capi_done(void *ref, int status)
{
	struct capi_task *task = __capi_task_from_copy_task(ref);

	if (status != 0) {
		if (status == -ENOMEM) {
			task->status = SPDK_BDEV_IO_STATUS_NOMEM;
		} else {
			task->status = SPDK_BDEV_IO_STATUS_FAILED;
		}
	}

	if (--task->num_outstanding == 0) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), task->status);
	}
}

static TAILQ_HEAD(, capi_disk) g_capi_disks = TAILQ_HEAD_INITIALIZER(g_capi_disks);

int capi_disk_count = 0;

static int bdev_capi_initialize(void);
static void bdev_capi_get_spdk_running_config(FILE *fp);

static int
bdev_capi_get_ctx_size(void)
{
	return sizeof(struct capi_task) + spdk_copy_task_size();
}

static struct spdk_bdev_module capi_if = {
	.name = "capi",
	.module_init = bdev_capi_initialize,
	.config_text = bdev_capi_get_spdk_running_config,
	.get_ctx_size = bdev_capi_get_ctx_size,

};

SPDK_BDEV_MODULE_REGISTER(&capi_if)

static void
capi_disk_free(struct capi_disk *capi_disk)
{
	if (!capi_disk) {
		return;
	}

	free(capi_disk->disk.name);
	pbuf_put(capi_disk->pbuf, capi_disk->capi_buf);
	pbuf_free(capi_disk->pbuf);
	cblk_term(NULL, 0);
}

static int
bdev_capi_destruct(void *ctx)
{
	struct capi_disk *capi_disk = ctx;

	TAILQ_REMOVE(&g_capi_disks, capi_disk, link);
	capi_disk_free(capi_disk);
	return 0;
}

static int
bdev_capi_check_iov_len(struct iovec *iovs, int iovcnt, size_t nbytes)
{
	int i;

	for (i = 0; i < iovcnt; i++) {
		if (nbytes < iovs[i].iov_len) {
			return 0;
		}

		nbytes -= iovs[i].iov_len;
	}

	return nbytes != 0;
}

static void
bdev_capi_readv(struct capi_disk *mdisk, struct spdk_io_channel *ch,
		  struct capi_task *task,
		  struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	int64_t res = 0;
	void *src = mdisk->capi_buf + offset;
	int i;

	if (bdev_capi_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "read %lu bytes from offset %#lx\n",
		      len, offset);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = iovcnt;

	for (i = 0; i < iovcnt; i++) {
		res = spdk_copy_submit(__copy_task_from_capi_task(task),
				       ch, iov[i].iov_base,
				       src, iov[i].iov_len, capi_done);

		if (res != 0) {
			capi_done(__copy_task_from_capi_task(task), res);
		}

		src += iov[i].iov_len;
		len -= iov[i].iov_len;
	}
}

static void
bdev_capi_writev(struct capi_disk *mdisk, struct spdk_io_channel *ch,
		   struct capi_task *task,
		   struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	int64_t res = 0;
	void *dst = mdisk->capi_buf + offset;
	int i;

	if (bdev_capi_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "wrote %lu bytes to offset %#lx\n",
		      len, offset);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = iovcnt;

	for (i = 0; i < iovcnt; i++) {
		res = spdk_copy_submit(__copy_task_from_capi_task(task),
				       ch, dst, iov[i].iov_base,
				       iov[i].iov_len, capi_done);

		if (res != 0) {
			capi_done(__copy_task_from_capi_task(task), res);
		}

		dst += iov[i].iov_len;
	}
}

static int
bdev_capi_unmap(struct capi_disk *mdisk,
		  struct spdk_io_channel *ch,
		  struct capi_task *task,
		  uint64_t offset,
		  uint64_t byte_count)
{
	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 1;

	return spdk_copy_submit_fill(__copy_task_from_capi_task(task), ch,
				     mdisk->capi_buf + offset, 0, byte_count, capi_done);
}

static int64_t
bdev_capi_flush(struct capi_disk *mdisk, struct capi_task *task,
		  uint64_t offset, uint64_t nbytes)
{
	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), SPDK_BDEV_IO_STATUS_SUCCESS);

	return 0;
}

static int
bdev_capi_reset(struct capi_disk *mdisk, struct capi_task *task)
{
	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), SPDK_BDEV_IO_STATUS_SUCCESS);

	return 0;
}

static int _bdev_capi_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
			assert(bdev_io->u.bdev.iovcnt == 1);
			bdev_io->u.bdev.iovs[0].iov_base =
				((struct capi_disk *)bdev_io->bdev->ctxt)->capi_buf +
				bdev_io->u.bdev.offset_blocks * block_size;
			bdev_io->u.bdev.iovs[0].iov_len = bdev_io->u.bdev.num_blocks * block_size;
			spdk_bdev_io_complete(spdk_bdev_io_from_ctx(bdev_io->driver_ctx),
					      SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		}

		bdev_capi_readv((struct capi_disk *)bdev_io->bdev->ctxt,
				  ch,
				  (struct capi_task *)bdev_io->driver_ctx,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks * block_size,
				  bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		bdev_capi_writev((struct capi_disk *)bdev_io->bdev->ctxt,
				   ch,
				   (struct capi_task *)bdev_io->driver_ctx,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks * block_size,
				   bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_RESET:
		return bdev_capi_reset((struct capi_disk *)bdev_io->bdev->ctxt,
					 (struct capi_task *)bdev_io->driver_ctx);

	case SPDK_BDEV_IO_TYPE_FLUSH:
		return bdev_capi_flush((struct capi_disk *)bdev_io->bdev->ctxt,
					 (struct capi_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_UNMAP:
		return bdev_capi_unmap((struct capi_disk *)bdev_io->bdev->ctxt,
					 ch,
					 (struct capi_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		/* bdev_capi_unmap is implemented with a call to mem_cpy_fill which zeroes out all of the requested bytes. */
		return bdev_capi_unmap((struct capi_disk *)bdev_io->bdev->ctxt,
					 ch,
					 (struct capi_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	default:
		return -1;
	}
	return 0;
}

static void bdev_capi_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	if (_bdev_capi_submit_request(ch, bdev_io) != 0) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
bdev_capi_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		return true;

	default:
		return false;
	}
}

static struct spdk_io_channel *
bdev_capi_get_io_channel(void *ctx)
{
	return spdk_copy_engine_get_io_channel();
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

struct spdk_bdev *create_capi_disk(const char *devStr, uint32_t queue_depth,
					bool intrp_thds, bool seq, bool plun, uint32_t vlunsize,
				     uint64_t num_blocks)
{
	struct capi_disk	*mdisk;
	int			rc;
	int flags = 0;
	size_t lun_size;

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

	mdisk->pbuf = pbuf_new(queue_depth, num_blocks * 4 * 1024);
	mdisk->capi_buf = pbuf_get(mdisk->pbuf);
	if (!mdisk->capi_buf) {
		SPDK_ERRLOG("capi_buf pbuf_get() failed\n");
		goto free_disk;
	}

	mdisk->disk.name = spdk_sprintf_alloc("CAPI%d", capi_disk_count);
	capi_disk_count++;
	if (!mdisk->disk.name) {
		goto free_disk;
	}
	mdisk->disk.product_name = "CAPI Flash";
	mdisk->disk.write_cache = 1;
	mdisk->disk.blocklen = 4 * 1024;
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

	TAILQ_INSERT_TAIL(&g_capi_disks, mdisk, link);

	return &mdisk->disk;

free_disk:
	capi_disk_free(mdisk);
	return NULL;
}

void
delete_capi_disk(struct spdk_bdev *bdev, spdk_delete_capi_complete cb_fn, void *cb_arg)
{
	if (!bdev || bdev->module != &capi_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static int bdev_capi_initialize(void)
{
	struct spdk_conf_section *sp = spdk_conf_find_section(NULL, "CAPI");
	int i, rc = 0, devN = 0;
	char *devStr, *pstr;
	char devStrs[MAX_SGDEVS][64];
	int queue_depth;
	bool intrp_thds;
	bool seq;
	bool plun;
	int vlunsize,
	struct spdk_bdev *bdev;

	if (sp == NULL) {
		return rc;
	}

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
		bdev = create_capi_disk(devStr[i], queue_depth, intrp_thds, seq, plun, vlunsize, num_blocks);
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
	struct capi_disk *mdisk;

	/* count number of malloc LUNs, get LUN size */
	TAILQ_FOREACH(mdisk, &g_capi_disks, link) {
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

SPDK_LOG_REGISTER_COMPONENT("bdev_capi", SPDK_LOG_BDEV_CAPI)
