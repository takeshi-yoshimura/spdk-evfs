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

#include "spdk/blobfs.h"
#include "spdk/conf.h"
#include "blobfs_internal.h"

#include "spdk/queue.h"
#include "spdk/thread.h"
#include "spdk/assert.h"
#include "spdk/env.h"
#include "spdk/util.h"
#include "spdk_internal/log.h"

#define BLOBFS_TRACE(file, str, args...) \
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s " str, file->name, ##args)

#define BLOBFS_TRACE_RW(file, str, args...) \
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS_RW, "file=%s " str, file->name, ##args)

#define BLOBFS_DEFAULT_CACHE_SIZE (4ULL * 1024 * 1024 * 1024)
#define SPDK_BLOBFS_DEFAULT_OPTS_CLUSTER_SZ (1024 * 1024)

static uint64_t g_fs_cache_size = BLOBFS_DEFAULT_CACHE_SIZE;
static struct spdk_mempool *g_cache_pool;
static TAILQ_HEAD(, spdk_file) g_caches;
static int g_fs_count = 0;
static pthread_mutex_t g_cache_init_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_spinlock_t g_caches_lock;
static bool g_fs_sync = 0;

void
spdk_cache_buffer_free(struct cache_buffer *cache_buffer)
{
	spdk_mempool_put(g_cache_pool, cache_buffer->buf);
	free(cache_buffer);
}

#define CACHE_READAHEAD_THRESHOLD	(128 * 1024)

struct spdk_file {
	struct spdk_filesystem	*fs;
	struct spdk_blob	*blob;
	char			*name;
	uint64_t		length;
	bool                    is_deleted;
	bool			open_for_writing;
	bool            resizing;
	uint64_t		length_flushed;
	uint64_t		append_pos;
	uint64_t		seq_byte_count;
	uint64_t		next_seq_offset;
	uint32_t		priority;
	TAILQ_ENTRY(spdk_file)	tailq;
	spdk_blob_id		blobid;
	uint32_t		ref_count;
	pthread_spinlock_t	lock;
    pthread_spinlock_t	writeorder_lock;
    pthread_spinlock_t	buffer_lock;
	struct cache_buffer	*last;
	struct cache_tree	*tree;
	TAILQ_HEAD(open_requests_head, spdk_fs_request) open_requests;
    TAILQ_HEAD(sync_requests_head, spdk_fs_request) sync_requests;
    TAILQ_HEAD(dirty_buffer_head, cache_buffer) dirty_buffers;
    TAILQ_HEAD(resize_waiter_head, spdk_fs_request) resize_waiter;
	TAILQ_ENTRY(spdk_file)	cache_tailq;
};

struct spdk_deleted_file {
	spdk_blob_id	id;
	TAILQ_ENTRY(spdk_deleted_file)	tailq;
};

struct spdk_filesystem {
	struct spdk_blob_store	*bs;
	TAILQ_HEAD(, spdk_file)	files;
	struct spdk_bs_opts	bs_opts;
	struct spdk_bs_dev	*bdev;
	fs_send_request_fn	send_request;

	struct {
		uint32_t		max_ops;
		struct spdk_io_channel	*sync_io_channel;
		struct spdk_fs_channel	*sync_fs_channel;
	} sync_target;

	struct {
		uint32_t		max_ops;
		struct spdk_io_channel	*md_io_channel;
		struct spdk_fs_channel	*md_fs_channel;
	} md_target;

	struct {
		uint32_t		max_ops;
	} io_target;
};

struct spdk_fs_cb_args {
	union {
		spdk_fs_op_with_handle_complete		fs_op_with_handle;
		spdk_fs_op_complete			fs_op;
		spdk_file_op_with_handle_complete	file_op_with_handle;
		spdk_file_op_complete			file_op;
		spdk_file_stat_op_complete		stat_op;
		fs_request_fn rw_op;
		spdk_file_op_complete resize_op;
	} fn;
	void *arg;
	sem_t *sem;
	struct spdk_filesystem *fs;
	struct spdk_file *file;
	int rc;
	bool from_request;
	union {
		struct {
			TAILQ_HEAD(, spdk_deleted_file)	deleted_files;
		} fs_load;
		struct {
			uint64_t	length;
		} truncate;
		struct {
			struct spdk_io_channel	*channel;
			void		*user_buf;
			void		*pin_buf;
			int		is_read;
			off_t		offset;
			size_t		length;
			uint64_t	start_lba;
			uint64_t	num_lba;
			uint32_t	blocklen;
		} rw;
		struct {
			const char	*old_name;
			const char	*new_name;
		} rename;
		struct {
			struct cache_buffer	*cache_buffer;
			uint64_t		length;
		} flush;
		struct {
			struct cache_buffer	*cache_buffer;
			uint64_t		length;
			uint64_t		offset;
		} readahead;
		struct {
			uint64_t			offset;
			TAILQ_ENTRY(spdk_fs_request)	tailq;
			bool				xattr_in_progress;
		} sync;
		struct {
			uint32_t			num_clusters;
		} resize;
		struct {
			const char	*name;
			uint32_t	flags;
			TAILQ_ENTRY(spdk_fs_request)	tailq;
		} open;
		struct {
			const char		*name;
			struct spdk_blob	*blob;
		} create;
		struct {
			const char	*name;
		} delete;
		struct {
			const char	*name;
		} stat;
		struct {
            void		*user_buf;
            void		*pin_buf;
            uint64_t		offset;
            uint64_t		length;
			int oflag;
            bool is_read;
            bool needrc;
            bool delayed;
            struct cache_buffer * buffer;
            TAILQ_ENTRY(spdk_fs_request)	sync_tailq;
            TAILQ_ENTRY(spdk_fs_request)	resize_tailq;
            TAILQ_ENTRY(spdk_fs_request)	write_tailq;
		} blobfs2_rw;
	} op;
};

static void cache_free_buffers(struct spdk_file *file);
static void init_blobfs2(void);

void
spdk_fs_opts_init(struct spdk_blobfs_opts *opts)
{
	opts->cluster_sz = SPDK_BLOBFS_DEFAULT_OPTS_CLUSTER_SZ;
}

static void
__initialize_cache(void)
{
	assert(g_cache_pool == NULL);

	g_cache_pool = spdk_mempool_create("spdk_fs_cache",
					   g_fs_cache_size / CACHE_BUFFER_SIZE,
					   CACHE_BUFFER_SIZE,
					   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
					   SPDK_ENV_SOCKET_ID_ANY);
	if (!g_cache_pool) {
		SPDK_ERRLOG("Create mempool failed, you may "
			    "increase the memory and try again\n");
		assert(false);
	}
	TAILQ_INIT(&g_caches);
	init_blobfs2();
	pthread_spin_init(&g_caches_lock, 0);
}

static void
__free_cache(void)
{
	assert(g_cache_pool != NULL);

	spdk_mempool_free(g_cache_pool);
	g_cache_pool = NULL;
}

static uint64_t
__file_get_blob_size(struct spdk_file *file)
{
	uint64_t cluster_sz;

	cluster_sz = file->fs->bs_opts.cluster_sz;
	return cluster_sz * spdk_blob_get_num_clusters(file->blob);
}

struct spdk_fs_request {
	struct spdk_fs_cb_args		args;
	TAILQ_ENTRY(spdk_fs_request)	link;
	struct spdk_fs_channel		*channel;
};

struct spdk_fs_channel {
	struct spdk_fs_request		*req_mem;
	TAILQ_HEAD(, spdk_fs_request)	reqs;
	sem_t				sem;
	struct spdk_filesystem		*fs;
	struct spdk_io_channel		*bs_channel;
	fs_send_request_fn		send_request;
	bool				sync;
	pthread_spinlock_t		lock;
};

static struct spdk_fs_request *
alloc_fs_request(struct spdk_fs_channel *channel)
{
	struct spdk_fs_request *req;

	if (channel->sync) {
		pthread_spin_lock(&channel->lock);
	}

	req = TAILQ_FIRST(&channel->reqs);
	if (req) {
		TAILQ_REMOVE(&channel->reqs, req, link);
	}

	if (channel->sync) {
		pthread_spin_unlock(&channel->lock);
	}

	if (req == NULL) {
		return NULL;
	}
	memset(req, 0, sizeof(*req));
	req->channel = channel;
	req->args.from_request = true;

	return req;
}

static void
free_fs_request(struct spdk_fs_request *req)
{
	struct spdk_fs_channel *channel = req->channel;

	if (channel->sync) {
		pthread_spin_lock(&channel->lock);
	}

	TAILQ_INSERT_HEAD(&req->channel->reqs, req, link);

	if (channel->sync) {
		pthread_spin_unlock(&channel->lock);
	}
}

static int
_spdk_fs_channel_create(struct spdk_filesystem *fs, struct spdk_fs_channel *channel,
			uint32_t max_ops)
{
	uint32_t i;

	channel->req_mem = calloc(max_ops, sizeof(struct spdk_fs_request));
	if (!channel->req_mem) {
		return -1;
	}

	TAILQ_INIT(&channel->reqs);
	sem_init(&channel->sem, 0, 0);

	for (i = 0; i < max_ops; i++) {
		TAILQ_INSERT_TAIL(&channel->reqs, &channel->req_mem[i], link);
	}

	channel->fs = fs;

	return 0;
}

static int
_spdk_fs_md_channel_create(void *io_device, void *ctx_buf)
{
	struct spdk_filesystem		*fs;
	struct spdk_fs_channel		*channel = ctx_buf;

	fs = SPDK_CONTAINEROF(io_device, struct spdk_filesystem, md_target);

	return _spdk_fs_channel_create(fs, channel, fs->md_target.max_ops);
}

static int
_spdk_fs_sync_channel_create(void *io_device, void *ctx_buf)
{
	struct spdk_filesystem		*fs;
	struct spdk_fs_channel		*channel = ctx_buf;

	fs = SPDK_CONTAINEROF(io_device, struct spdk_filesystem, sync_target);

	return _spdk_fs_channel_create(fs, channel, fs->sync_target.max_ops);
}

static int
_spdk_fs_io_channel_create(void *io_device, void *ctx_buf)
{
	struct spdk_filesystem		*fs;
	struct spdk_fs_channel		*channel = ctx_buf;

	fs = SPDK_CONTAINEROF(io_device, struct spdk_filesystem, io_target);

	return _spdk_fs_channel_create(fs, channel, fs->io_target.max_ops);
}

static void
_spdk_fs_channel_destroy(void *io_device, void *ctx_buf)
{
	struct spdk_fs_channel *channel = ctx_buf;

	free(channel->req_mem);
	if (channel->bs_channel != NULL) {
		spdk_bs_free_io_channel(channel->bs_channel);
	}
}

static void
__send_request_direct(fs_request_fn fn, void *arg)
{
	fn(arg);
}

static void
common_fs_bs_init(struct spdk_filesystem *fs, struct spdk_blob_store *bs)
{
	fs->bs = bs;
	fs->bs_opts.cluster_sz = spdk_bs_get_cluster_size(bs);
	fs->md_target.md_fs_channel->bs_channel = spdk_bs_alloc_io_channel(fs->bs);
	fs->md_target.md_fs_channel->send_request = __send_request_direct;
	fs->sync_target.sync_fs_channel->bs_channel = spdk_bs_alloc_io_channel(fs->bs);
	fs->sync_target.sync_fs_channel->send_request = __send_request_direct;

	pthread_mutex_lock(&g_cache_init_lock);
	if (g_fs_count == 0) {
		__initialize_cache();
	}
	g_fs_count++;
	pthread_mutex_unlock(&g_cache_init_lock);
}

static void
init_cb(void *ctx, struct spdk_blob_store *bs, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;

	if (bserrno == 0) {
		common_fs_bs_init(fs, bs);
	} else {
		free(fs);
		fs = NULL;
	}

	args->fn.fs_op_with_handle(args->arg, fs, bserrno);
	free_fs_request(req);
}

static void
fs_conf_parse(void)
{
	struct spdk_conf_section *sp;

	sp = spdk_conf_find_section(NULL, "Blobfs");
	if (sp == NULL) {
		g_fs_cache_buffer_shift = CACHE_BUFFER_SHIFT_DEFAULT;
		return;
	}

	g_fs_cache_buffer_shift = spdk_conf_section_get_intval(sp, "CacheBufferShift");
	if (g_fs_cache_buffer_shift <= 0) {
		g_fs_cache_buffer_shift = CACHE_BUFFER_SHIFT_DEFAULT;
	}

	g_fs_sync = spdk_conf_section_get_boolval(sp, "Sync", false);
}

static struct spdk_filesystem *
fs_alloc(struct spdk_bs_dev *dev, fs_send_request_fn send_request_fn)
{
	struct spdk_filesystem *fs;

	fs = calloc(1, sizeof(*fs));
	if (fs == NULL) {
		return NULL;
	}

	fs->bdev = dev;
	fs->send_request = send_request_fn;
	TAILQ_INIT(&fs->files);

	fs->md_target.max_ops = 512;
	spdk_io_device_register(&fs->md_target, _spdk_fs_md_channel_create, _spdk_fs_channel_destroy,
				sizeof(struct spdk_fs_channel), "blobfs_md");
	fs->md_target.md_io_channel = spdk_get_io_channel(&fs->md_target);
	fs->md_target.md_fs_channel = spdk_io_channel_get_ctx(fs->md_target.md_io_channel);

	fs->sync_target.max_ops = 512;
	spdk_io_device_register(&fs->sync_target, _spdk_fs_sync_channel_create, _spdk_fs_channel_destroy,
				sizeof(struct spdk_fs_channel), "blobfs_sync");
	fs->sync_target.sync_io_channel = spdk_get_io_channel(&fs->sync_target);
	fs->sync_target.sync_fs_channel = spdk_io_channel_get_ctx(fs->sync_target.sync_io_channel);

	fs->io_target.max_ops = 512;
	spdk_io_device_register(&fs->io_target, _spdk_fs_io_channel_create, _spdk_fs_channel_destroy,
				sizeof(struct spdk_fs_channel), "blobfs_io");

	return fs;
}

static void
__wake_caller(void *arg, int fserrno)
{
	struct spdk_fs_cb_args *args = arg;

	args->rc = fserrno;
	sem_post(args->sem);
}

void
spdk_fs_init(struct spdk_bs_dev *dev, struct spdk_blobfs_opts *opt,
	     fs_send_request_fn send_request_fn,
	     spdk_fs_op_with_handle_complete cb_fn, void *cb_arg)
{
	struct spdk_filesystem *fs;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	struct spdk_bs_opts opts = {};

	fs = fs_alloc(dev, send_request_fn);
	if (fs == NULL) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	fs_conf_parse();

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		spdk_put_io_channel(fs->md_target.md_io_channel);
		spdk_io_device_unregister(&fs->md_target, NULL);
		spdk_put_io_channel(fs->sync_target.sync_io_channel);
		spdk_io_device_unregister(&fs->sync_target, NULL);
		spdk_io_device_unregister(&fs->io_target, NULL);
		free(fs);
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.fs_op_with_handle = cb_fn;
	args->arg = cb_arg;
	args->fs = fs;

	spdk_bs_opts_init(&opts);
	snprintf(opts.bstype.bstype, sizeof(opts.bstype.bstype), "BLOBFS");
	if (opt) {
		opts.cluster_sz = opt->cluster_sz;
	}
    opts.num_md_pages = 4096;
	spdk_bs_init(dev, &opts, init_cb, req);
}

static struct spdk_file *
file_alloc(struct spdk_filesystem *fs)
{
	struct spdk_file *file;

	file = calloc(1, sizeof(*file));
	if (file == NULL) {
		return NULL;
	}

	file->tree = calloc(1, sizeof(*file->tree));
	if (file->tree == NULL) {
		free(file);
		return NULL;
	}

	file->fs = fs;
	TAILQ_INIT(&file->open_requests);
	TAILQ_INIT(&file->sync_requests);
    TAILQ_INIT(&file->dirty_buffers);
    TAILQ_INIT(&file->resize_waiter);
	pthread_spin_init(&file->lock, 0);
    pthread_spin_init(&file->writeorder_lock, 0);
    pthread_spin_init(&file->buffer_lock, 0);
	TAILQ_INSERT_TAIL(&fs->files, file, tailq);
	file->priority = SPDK_FILE_PRIORITY_LOW;
	return file;
}

static void fs_load_done(void *ctx, int bserrno);

static int
_handle_deleted_files(struct spdk_fs_request *req)
{
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;

	if (!TAILQ_EMPTY(&args->op.fs_load.deleted_files)) {
		struct spdk_deleted_file *deleted_file;

		deleted_file = TAILQ_FIRST(&args->op.fs_load.deleted_files);
		TAILQ_REMOVE(&args->op.fs_load.deleted_files, deleted_file, tailq);
		spdk_bs_delete_blob(fs->bs, deleted_file->id, fs_load_done, req);
		free(deleted_file);
		return 0;
	}

	return 1;
}

static void
fs_load_done(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;

	/* The filesystem has been loaded.  Now check if there are any files that
	 *  were marked for deletion before last unload.  Do not complete the
	 *  fs_load callback until all of them have been deleted on disk.
	 */
	if (_handle_deleted_files(req) == 0) {
		/* We found a file that's been marked for deleting but not actually
		 *  deleted yet.  This function will get called again once the delete
		 *  operation is completed.
		 */
		return;
	}

	args->fn.fs_op_with_handle(args->arg, fs, 0);
	free_fs_request(req);

}

static void
iter_cb(void *ctx, struct spdk_blob *blob, int rc)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;
	uint64_t *length;
	const char *name;
	uint32_t *is_deleted;
	size_t value_len;

	if (rc < 0) {
		args->fn.fs_op_with_handle(args->arg, fs, rc);
		free_fs_request(req);
		return;
	}

	rc = spdk_blob_get_xattr_value(blob, "name", (const void **)&name, &value_len);
	if (rc < 0) {
		args->fn.fs_op_with_handle(args->arg, fs, rc);
//		free_fs_request(req);
		return;
	}

	rc = spdk_blob_get_xattr_value(blob, "length", (const void **)&length, &value_len);
	if (rc < 0) {
		args->fn.fs_op_with_handle(args->arg, fs, rc);
		free_fs_request(req);
		return;
	}

	assert(value_len == 8);

	/* This file could be deleted last time without close it, then app crashed, so we delete it now */
	rc = spdk_blob_get_xattr_value(blob, "is_deleted", (const void **)&is_deleted, &value_len);
	if (rc < 0) {
		struct spdk_file *f;

		f = file_alloc(fs);
		if (f == NULL) {
			args->fn.fs_op_with_handle(args->arg, fs, -ENOMEM);
			free_fs_request(req);
			return;
		}

		f->name = strdup(name);
		f->blobid = spdk_blob_get_id(blob);
		f->length = *length;
		f->length_flushed = *length;
		f->append_pos = *length;
		f->resizing = false;
		SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "added file %s length=%ju\n", f->name, f->length);
	} else {
		struct spdk_deleted_file *deleted_file;

		deleted_file = calloc(1, sizeof(*deleted_file));
		if (deleted_file == NULL) {
			args->fn.fs_op_with_handle(args->arg, fs, -ENOMEM);
			free_fs_request(req);
			return;
		}
		deleted_file->id = spdk_blob_get_id(blob);
		TAILQ_INSERT_TAIL(&args->op.fs_load.deleted_files, deleted_file, tailq);
	}
}

static void
load_cb(void *ctx, struct spdk_blob_store *bs, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;
	struct spdk_bs_type bstype;
	static const struct spdk_bs_type blobfs_type = {"BLOBFS"};
	static const struct spdk_bs_type zeros;

	if (bserrno != 0) {
		args->fn.fs_op_with_handle(args->arg, NULL, bserrno);
		free_fs_request(req);
		free(fs);
		return;
	}

	bstype = spdk_bs_get_bstype(bs);

	if (!memcmp(&bstype, &zeros, sizeof(bstype))) {
		SPDK_DEBUGLOG(SPDK_LOG_BLOB, "assigning bstype\n");
		spdk_bs_set_bstype(bs, blobfs_type);
	} else if (memcmp(&bstype, &blobfs_type, sizeof(bstype))) {
		SPDK_DEBUGLOG(SPDK_LOG_BLOB, "not blobfs\n");
		SPDK_TRACEDUMP(SPDK_LOG_BLOB, "bstype", &bstype, sizeof(bstype));
		args->fn.fs_op_with_handle(args->arg, NULL, bserrno);
		free_fs_request(req);
		free(fs);
		return;
	}

	common_fs_bs_init(fs, bs);
	fs_load_done(req, 0);
}

static void
spdk_fs_io_device_unregister(struct spdk_filesystem *fs)
{
	assert(fs != NULL);
	spdk_io_device_unregister(&fs->md_target, NULL);
	spdk_io_device_unregister(&fs->sync_target, NULL);
	spdk_io_device_unregister(&fs->io_target, NULL);
	free(fs);
}

static void
spdk_fs_free_io_channels(struct spdk_filesystem *fs)
{
	assert(fs != NULL);
	spdk_fs_free_io_channel(fs->md_target.md_io_channel);
	spdk_fs_free_io_channel(fs->sync_target.sync_io_channel);
}

void
spdk_fs_load(struct spdk_bs_dev *dev, fs_send_request_fn send_request_fn,
	     spdk_fs_op_with_handle_complete cb_fn, void *cb_arg)
{
	struct spdk_filesystem *fs;
	struct spdk_fs_cb_args *args;
	struct spdk_fs_request *req;
	struct spdk_bs_opts	bs_opts;

	fs = fs_alloc(dev, send_request_fn);
	if (fs == NULL) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	fs_conf_parse();

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		spdk_fs_free_io_channels(fs);
		spdk_fs_io_device_unregister(fs);
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.fs_op_with_handle = cb_fn;
	args->arg = cb_arg;
	args->fs = fs;
	TAILQ_INIT(&args->op.fs_load.deleted_files);
	spdk_bs_opts_init(&bs_opts);
	bs_opts.iter_cb_fn = iter_cb;
	bs_opts.iter_cb_arg = req;
	spdk_bs_load(dev, &bs_opts, load_cb, req);
}

static void
unload_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_filesystem *fs = args->fs;
	struct spdk_file *file, *tmp;

	TAILQ_FOREACH_SAFE(file, &fs->files, tailq, tmp) {
		TAILQ_REMOVE(&fs->files, file, tailq);
		cache_free_buffers(file);
		free(file->name);
		free(file->tree);
		free(file);
	}

	pthread_mutex_lock(&g_cache_init_lock);
	g_fs_count--;
	if (g_fs_count == 0) {
		__free_cache();
	}
	pthread_mutex_unlock(&g_cache_init_lock);

	args->fn.fs_op(args->arg, bserrno);
	free(req);

	spdk_fs_io_device_unregister(fs);
}

void
spdk_fs_unload(struct spdk_filesystem *fs, spdk_fs_op_complete cb_fn, void *cb_arg)
{
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	/*
	 * We must free the md_channel before unloading the blobstore, so just
	 *  allocate this request from the general heap.
	 */
	req = calloc(1, sizeof(*req));
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.fs_op = cb_fn;
	args->arg = cb_arg;
	args->fs = fs;

	spdk_fs_free_io_channels(fs);
	spdk_bs_unload(fs->bs, unload_cb, req);
}

static struct spdk_file *
fs_find_file(struct spdk_filesystem *fs, const char *name)
{
	struct spdk_file *file;

	TAILQ_FOREACH(file, &fs->files, tailq) {
		if (!strncmp(name, file->name, SPDK_FILE_NAME_MAX)) {
			return file;
		}
	}

	return NULL;
}

void
spdk_fs_file_stat_async(struct spdk_filesystem *fs, const char *name,
			spdk_file_stat_op_complete cb_fn, void *cb_arg)
{
	struct spdk_file_stat stat;
	struct spdk_file *f = NULL;

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		cb_fn(cb_arg, NULL, -ENAMETOOLONG);
		return;
	}

	f = fs_find_file(fs, name);
	if (f != NULL) {
		stat.blobid = f->blobid;
		stat.size = f->append_pos >= f->length ? f->append_pos : f->length;
		cb_fn(cb_arg, &stat, 0);
		return;
	}

	cb_fn(cb_arg, NULL, -ENOENT);
}

static void
__copy_stat(void *arg, struct spdk_file_stat *stat, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	args->rc = fserrno;
	if (fserrno == 0) {
		memcpy(args->arg, stat, sizeof(*stat));
	}
	sem_post(args->sem);
}

static void
__file_stat(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	spdk_fs_file_stat_async(args->fs, args->op.stat.name,
				args->fn.stat_op, req);
}

int
spdk_fs_file_stat(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
		  const char *name, struct spdk_file_stat *stat)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	int rc;

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	req->args.fs = fs;
	req->args.op.stat.name = name;
	req->args.fn.stat_op = __copy_stat;
	req->args.arg = stat;
	req->args.sem = &channel->sem;
	channel->send_request(__file_stat, req);
	sem_wait(&channel->sem);

	rc = req->args.rc;
	free_fs_request(req);

	return rc;
}

static void
fs_create_blob_close_cb(void *ctx, int bserrno)
{
	int rc;
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	rc = args->rc ? args->rc : bserrno;
	args->fn.file_op(args->arg, rc);
	free_fs_request(req);
}

static void
fs_create_blob_resize_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *f = args->file;
	struct spdk_blob *blob = args->op.create.blob;
	uint64_t length = 0;

	args->rc = bserrno;
	if (bserrno) {
		spdk_blob_close(blob, fs_create_blob_close_cb, args);
		return;
	}

	spdk_blob_set_xattr(blob, "name", f->name, strlen(f->name) + 1);
	spdk_blob_set_xattr(blob, "length", &length, sizeof(length));

	spdk_blob_close(blob, fs_create_blob_close_cb, args);
}

static void
fs_create_blob_open_cb(void *ctx, struct spdk_blob *blob, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	if (bserrno) {
		args->fn.file_op(args->arg, bserrno);
		free_fs_request(req);
		return;
	}

	args->op.create.blob = blob;
	spdk_blob_resize(blob, 1, fs_create_blob_resize_cb, req);
}

static void
fs_create_blob_create_cb(void *ctx, spdk_blob_id blobid, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *f = args->file;

	if (bserrno) {
		args->fn.file_op(args->arg, bserrno);
		free_fs_request(req);
		return;
	}

	f->blobid = blobid;
	spdk_bs_open_blob(f->fs->bs, blobid, fs_create_blob_open_cb, req);
}

void
spdk_fs_create_file_async(struct spdk_filesystem *fs, const char *name,
			  spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_file *file;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		cb_fn(cb_arg, -ENAMETOOLONG);
		return;
	}

	file = fs_find_file(fs, name);
	if (file != NULL) {
		cb_fn(cb_arg, -EEXIST);
		return;
	}

	file = file_alloc(fs);
	if (file == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->file = file;
	args->fn.file_op = cb_fn;
	args->arg = cb_arg;

	file->name = strdup(name);
	spdk_bs_create_blob(fs->bs, fs_create_blob_create_cb, args);
}

static void
__fs_create_file_done(void *arg, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	args->rc = fserrno;
	sem_post(args->sem);
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.create.name);
}

static void
__fs_create_file(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.create.name);
	spdk_fs_create_file_async(args->fs, args->op.create.name, __fs_create_file_done, req);
}

int
spdk_fs_create_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel, const char *name)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", name);

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;
	args->fs = fs;
	args->op.create.name = name;
	args->sem = &channel->sem;
	fs->send_request(__fs_create_file, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	free_fs_request(req);

	return rc;
}

static void
fs_open_blob_done(void *ctx, struct spdk_blob *blob, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *f = args->file;

	f->blob = blob;
	while (!TAILQ_EMPTY(&f->open_requests)) {
		req = TAILQ_FIRST(&f->open_requests);
		args = &req->args;
		TAILQ_REMOVE(&f->open_requests, req, args.op.open.tailq);
		args->fn.file_op_with_handle(args->arg, f, bserrno);
		free_fs_request(req);
	}
}

static void
fs_open_blob_create_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;
	struct spdk_filesystem *fs = args->fs;

	if (file == NULL) {
		/*
		 * This is from an open with CREATE flag - the file
		 *  is now created so look it up in the file list for this
		 *  filesystem.
		 */
		file = fs_find_file(fs, args->op.open.name);
		assert(file != NULL);
		args->file = file;
	}

	file->ref_count++;
	TAILQ_INSERT_TAIL(&file->open_requests, req, args.op.open.tailq);
	if (file->ref_count == 1) {
		assert(file->blob == NULL);
		spdk_bs_open_blob(fs->bs, file->blobid, fs_open_blob_done, req);
	} else if (file->blob != NULL) {
		fs_open_blob_done(req, file->blob, 0);
	} else {
		/*
		 * The blob open for this file is in progress due to a previous
		 *  open request.  When that open completes, it will invoke the
		 *  open callback for this request.
		 */
	}
}

void
spdk_fs_open_file_async(struct spdk_filesystem *fs, const char *name, uint32_t flags,
			spdk_file_op_with_handle_complete cb_fn, void *cb_arg)
{
	struct spdk_file *f = NULL;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		cb_fn(cb_arg, NULL, -ENAMETOOLONG);
		return;
	}

	f = fs_find_file(fs, name);
	if (f == NULL && !(flags & SPDK_BLOBFS_OPEN_CREATE)) {
		cb_fn(cb_arg, NULL, -ENOENT);
		return;
	}

	if (f != NULL && f->is_deleted == true) {
		cb_fn(cb_arg, NULL, -ENOENT);
		return;
	}

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.file_op_with_handle = cb_fn;
	args->arg = cb_arg;
	args->file = f;
	args->fs = fs;
	args->op.open.name = name;

	if (f == NULL) {
		spdk_fs_create_file_async(fs, name, fs_open_blob_create_cb, req);
	} else {
		fs_open_blob_create_cb(req, 0);
	}
}

static void
__fs_open_file_done(void *arg, struct spdk_file *file, int bserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	args->file = file;
	__wake_caller(args, bserrno);
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.open.name);
}

static void
__fs_open_file(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.open.name);
	spdk_fs_open_file_async(args->fs, args->op.open.name, args->op.open.flags,
				__fs_open_file_done, req);
}

int
spdk_fs_open_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
		  const char *name, uint32_t flags, struct spdk_file **file)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", name);

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;
	args->fs = fs;
	args->op.open.name = name;
	args->op.open.flags = flags;
	args->sem = &channel->sem;
	fs->send_request(__fs_open_file, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	if (rc == 0) {
		*file = args->file;
	} else {
		*file = NULL;
	}
	free_fs_request(req);

	return rc;
}

static void
fs_rename_blob_close_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	args->fn.fs_op(args->arg, bserrno);
	free_fs_request(req);
}

static void
fs_rename_blob_open_cb(void *ctx, struct spdk_blob *blob, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	const char *new_name = args->op.rename.new_name;

	spdk_blob_set_xattr(blob, "name", new_name, strlen(new_name) + 1);
	spdk_blob_close(blob, fs_rename_blob_close_cb, req);
}

static void
__spdk_fs_md_rename_file(struct spdk_fs_request *req)
{
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *f;

	f = fs_find_file(args->fs, args->op.rename.old_name);
	if (f == NULL) {
		args->fn.fs_op(args->arg, -ENOENT);
		free_fs_request(req);
		return;
	}

	free(f->name);
	f->name = strdup(args->op.rename.new_name);
	args->file = f;
	spdk_bs_open_blob(args->fs->bs, f->blobid, fs_rename_blob_open_cb, req);
}

static void
fs_rename_delete_done(void *arg, int fserrno)
{
	__spdk_fs_md_rename_file(arg);
}

void
spdk_fs_rename_file_async(struct spdk_filesystem *fs,
			  const char *old_name, const char *new_name,
			  spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_file *f;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "old=%s new=%s\n", old_name, new_name);
	if (strnlen(new_name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		cb_fn(cb_arg, -ENAMETOOLONG);
		return;
	}

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.fs_op = cb_fn;
	args->fs = fs;
	args->arg = cb_arg;
	args->op.rename.old_name = old_name;
	args->op.rename.new_name = new_name;

	f = fs_find_file(fs, new_name);
	if (f == NULL) {
		__spdk_fs_md_rename_file(req);
		return;
	}

	/*
	 * The rename overwrites an existing file.  So delete the existing file, then
	 *  do the actual rename.
	 */
	spdk_fs_delete_file_async(fs, new_name, fs_rename_delete_done, req);
}

static void
__fs_rename_file_done(void *arg, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	__wake_caller(args, fserrno);
}

static void
__fs_rename_file(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	spdk_fs_rename_file_async(args->fs, args->op.rename.old_name, args->op.rename.new_name,
				  __fs_rename_file_done, req);
}

int
spdk_fs_rename_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
		    const char *old_name, const char *new_name)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;

	args->fs = fs;
	args->op.rename.old_name = old_name;
	args->op.rename.new_name = new_name;
	args->sem = &channel->sem;
	fs->send_request(__fs_rename_file, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	free_fs_request(req);
	return rc;
}

static void
blob_delete_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	args->fn.file_op(args->arg, bserrno);
	free_fs_request(req);
}

void
spdk_fs_delete_file_async(struct spdk_filesystem *fs, const char *name,
			  spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_file *f;
	spdk_blob_id blobid;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", name);

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		cb_fn(cb_arg, -ENAMETOOLONG);
		return;
	}

	f = fs_find_file(fs, name);
	if (f == NULL) {
		cb_fn(cb_arg, -ENOENT);
		return;
	}

	req = alloc_fs_request(fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.file_op = cb_fn;
	args->arg = cb_arg;

	if (f->ref_count > 0) {
		/* If the ref > 0, we mark the file as deleted and delete it when we close it. */
		f->is_deleted = true;
		spdk_blob_set_xattr(f->blob, "is_deleted", &f->is_deleted, sizeof(bool));
		spdk_blob_sync_md(f->blob, blob_delete_cb, args);
		return;
	}

	TAILQ_REMOVE(&fs->files, f, tailq);

	cache_free_buffers(f);

	blobid = f->blobid;

	free(f->name);
	free(f->tree);
	free(f);

	spdk_bs_delete_blob(fs->bs, blobid, blob_delete_cb, req);
}

static void
__fs_delete_file_done(void *arg, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	__wake_caller(args, fserrno);
}

static void
__fs_delete_file(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	spdk_fs_delete_file_async(args->fs, args->op.delete.name, __fs_delete_file_done, req);
}

int
spdk_fs_delete_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
		    const char *name)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;
	args->fs = fs;
	args->op.delete.name = name;
	args->sem = &channel->sem;
	fs->send_request(__fs_delete_file, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	free_fs_request(req);

	return rc;
}

spdk_fs_iter
spdk_fs_iter_first(struct spdk_filesystem *fs)
{
	struct spdk_file *f;

	f = TAILQ_FIRST(&fs->files);
	return f;
}

spdk_fs_iter
spdk_fs_iter_next(spdk_fs_iter iter)
{
	struct spdk_file *f = iter;

	if (f == NULL) {
		return NULL;
	}

	f = TAILQ_NEXT(f, tailq);
	return f;
}

const char *
spdk_file_get_name(struct spdk_file *file)
{
	return file->name;
}

uint64_t
spdk_file_get_length(struct spdk_file *file)
{
	assert(file != NULL);
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s length=0x%jx\n", file->name, file->length);
	return file->length;
}

static void
fs_truncate_complete_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	args->fn.file_op(args->arg, bserrno);
	free_fs_request(req);
}

static void
fs_truncate_resize_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;
	uint64_t *length = &args->op.truncate.length;

	if (bserrno) {
		args->fn.file_op(args->arg, bserrno);
		free_fs_request(req);
		return;
	}

	spdk_blob_set_xattr(file->blob, "length", length, sizeof(*length));

	file->length = *length;
	if (file->append_pos > file->length) {
		file->append_pos = file->length;
	}

	spdk_blob_sync_md(file->blob, fs_truncate_complete_cb, args);
}

static uint64_t
__bytes_to_clusters(uint64_t length, uint64_t cluster_sz)
{
	return (length + cluster_sz - 1) / cluster_sz;
}

void
spdk_file_truncate_async(struct spdk_file *file, uint64_t length,
			 spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_filesystem *fs;
	size_t num_clusters;
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s old=0x%jx new=0x%jx\n", file->name, file->length, length);
	if (length == file->length) {
		cb_fn(cb_arg, 0);
		return;
	}

	req = alloc_fs_request(file->fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->fn.file_op = cb_fn;
	args->arg = cb_arg;
	args->file = file;
	args->op.truncate.length = length;
	fs = file->fs;

	num_clusters = __bytes_to_clusters(length, fs->bs_opts.cluster_sz);

	spdk_blob_resize(file->blob, num_clusters, fs_truncate_resize_cb, req);
}

static void
__truncate(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	spdk_file_truncate_async(args->file, args->op.truncate.length,
				 args->fn.file_op, args);
}

int
spdk_file_truncate(struct spdk_file *file, struct spdk_io_channel *_channel,
		   uint64_t length)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;

	args->file = file;
	args->op.truncate.length = length;
	args->fn.file_op = __wake_caller;
	args->sem = &channel->sem;

	channel->send_request(__truncate, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	free_fs_request(req);

	return rc;
}

static void
__rw_done(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	spdk_dma_free(args->op.rw.pin_buf);
	args->fn.file_op(args->arg, bserrno);
	free_fs_request(req);
}

static void
__read_done(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	assert(req != NULL);
	if (args->op.rw.is_read) {
		memcpy(args->op.rw.user_buf,
		       args->op.rw.pin_buf + (args->op.rw.offset & (args->op.rw.blocklen - 1)),
		       args->op.rw.length);
		__rw_done(req, 0);
	} else {
		memcpy(args->op.rw.pin_buf + (args->op.rw.offset & (args->op.rw.blocklen - 1)),
		       args->op.rw.user_buf,
		       args->op.rw.length);
		spdk_blob_io_write(args->file->blob, args->op.rw.channel,
				   args->op.rw.pin_buf,
				   args->op.rw.start_lba, args->op.rw.num_lba,
				   __rw_done, req);
	}
}

static void
__do_blob_read(void *ctx, int fserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	if (fserrno) {
		__rw_done(req, fserrno);
		return;
	}
	spdk_blob_io_read(args->file->blob, args->op.rw.channel,
			  args->op.rw.pin_buf,
			  args->op.rw.start_lba, args->op.rw.num_lba,
			  __read_done, req);
}

static void
__get_page_parameters(struct spdk_file *file, uint64_t offset, uint64_t length,
		      uint64_t *start_lba, uint32_t *lba_size, uint64_t *num_lba)
{
	uint64_t end_lba;

	*lba_size = spdk_bs_get_io_unit_size(file->fs->bs);
	*start_lba = offset / *lba_size;
	end_lba = (offset + length - 1) / *lba_size;
	*num_lba = (end_lba - *start_lba + 1);
}

static void
__readwrite(struct spdk_file *file, struct spdk_io_channel *_channel,
	    void *payload, uint64_t offset, uint64_t length,
	    spdk_file_op_complete cb_fn, void *cb_arg, int is_read)
{
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	uint64_t start_lba, num_lba, pin_buf_length;
	uint32_t lba_size;

	if (is_read && offset + length > file->length) {
		cb_fn(cb_arg, -EINVAL);
		return;
	}

	req = alloc_fs_request(channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);

	args = &req->args;
	args->fn.file_op = cb_fn;
	args->arg = cb_arg;
	args->file = file;
	args->op.rw.channel = channel->bs_channel;
	args->op.rw.user_buf = payload;
	args->op.rw.is_read = is_read;
	args->op.rw.offset = offset;
	args->op.rw.length = length;
	args->op.rw.blocklen = lba_size;

	pin_buf_length = num_lba * lba_size;
	args->op.rw.pin_buf = spdk_dma_malloc(pin_buf_length, lba_size, NULL);
	if (args->op.rw.pin_buf == NULL) {
		SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "Failed to allocate buf for: file=%s offset=%jx length=%jx\n",
			      file->name, offset, length);
		free_fs_request(req);
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args->op.rw.start_lba = start_lba;
	args->op.rw.num_lba = num_lba;

	if (!is_read && file->length < offset + length) {
		spdk_file_truncate_async(file, offset + length, __do_blob_read, req);
	} else {
		__do_blob_read(req, 0);
	}
}

void
spdk_file_write_async(struct spdk_file *file, struct spdk_io_channel *channel,
		      void *payload, uint64_t offset, uint64_t length,
		      spdk_file_op_complete cb_fn, void *cb_arg)
{
	__readwrite(file, channel, payload, offset, length, cb_fn, cb_arg, 0);
}

void
spdk_file_read_async(struct spdk_file *file, struct spdk_io_channel *channel,
		     void *payload, uint64_t offset, uint64_t length,
		     spdk_file_op_complete cb_fn, void *cb_arg)
{
	SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s offset=%jx length=%jx\n",
		      file->name, offset, length);
	__readwrite(file, channel, payload, offset, length, cb_fn, cb_arg, 1);
}

struct spdk_io_channel *
spdk_fs_alloc_io_channel(struct spdk_filesystem *fs)
{
	struct spdk_io_channel *io_channel;
	struct spdk_fs_channel *fs_channel;

	io_channel = spdk_get_io_channel(&fs->io_target);
	fs_channel = spdk_io_channel_get_ctx(io_channel);
	fs_channel->bs_channel = spdk_bs_alloc_io_channel(fs->bs);
	fs_channel->send_request = __send_request_direct;

	return io_channel;
}

struct spdk_io_channel *
spdk_fs_alloc_io_channel_sync(struct spdk_filesystem *fs)
{
	struct spdk_io_channel *io_channel;
	struct spdk_fs_channel *fs_channel;

	io_channel = spdk_get_io_channel(&fs->io_target);
	fs_channel = spdk_io_channel_get_ctx(io_channel);
	fs_channel->send_request = fs->send_request;
	fs_channel->sync = 1;
	pthread_spin_init(&fs_channel->lock, 0);

	return io_channel;
}

void
spdk_fs_free_io_channel(struct spdk_io_channel *channel)
{
	spdk_put_io_channel(channel);
}

void
spdk_fs_set_cache_size(uint64_t size_in_mb)
{
	g_fs_cache_size = size_in_mb * 1024 * 1024;
}

uint64_t
spdk_fs_get_cache_size(void)
{
	return g_fs_cache_size / (1024 * 1024);
}

static void __file_flush(void *_args);

static void *
alloc_cache_memory_buffer(struct spdk_file *context)
{
	struct spdk_file *file;
	void *buf;

	buf = spdk_mempool_get(g_cache_pool);
	if (buf != NULL) {
		return buf;
	}

	pthread_spin_lock(&g_caches_lock);
	TAILQ_FOREACH(file, &g_caches, cache_tailq) {
		if (!file->open_for_writing &&
		    file->priority == SPDK_FILE_PRIORITY_LOW &&
		    file != context) {
			break;
		}
	}
	pthread_spin_unlock(&g_caches_lock);
	if (file != NULL) {
		cache_free_buffers(file);
		buf = spdk_mempool_get(g_cache_pool);
		if (buf != NULL) {
			return buf;
		}
	}

	pthread_spin_lock(&g_caches_lock);
	TAILQ_FOREACH(file, &g_caches, cache_tailq) {
		if (!file->open_for_writing && file != context) {
			break;
		}
	}
	pthread_spin_unlock(&g_caches_lock);
	if (file != NULL) {
		cache_free_buffers(file);
		buf = spdk_mempool_get(g_cache_pool);
		if (buf != NULL) {
			return buf;
		}
	}

	pthread_spin_lock(&g_caches_lock);
	TAILQ_FOREACH(file, &g_caches, cache_tailq) {
		if (file != context) {
			break;
		}
	}
	pthread_spin_unlock(&g_caches_lock);
	if (file != NULL) {
		cache_free_buffers(file);
		buf = spdk_mempool_get(g_cache_pool);
		if (buf != NULL) {
			return buf;
		}
	}

    int released = 0;
	for (uint64_t off = 0; off < context->length_flushed; off += CACHE_BUFFER_SIZE) {
		struct cache_buffer * c = spdk_tree_find_buffer(context->tree, off);
		if (c != NULL) {
            pthread_spin_lock(&g_caches_lock);
            spdk_tree_remove_buffer(context->tree, c);
            if (context->tree->present_mask == 0) {
                TAILQ_REMOVE(&g_caches, context, cache_tailq);
            }
            pthread_spin_unlock(&g_caches_lock);
            released += 1;
			break;
		}
	}
	if (released > 0) {
		buf = spdk_mempool_get(g_cache_pool);
		if (buf != NULL) {
			return buf;
		}
	}
	return NULL;
}

static struct cache_buffer *
cache_insert_buffer(struct spdk_file *file, uint64_t offset)
{
	struct cache_buffer *buf;
	int count = 0;

	buf = calloc(1, sizeof(*buf));
	if (buf == NULL) {
		SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "calloc failed\n");
		return NULL;
	}

	buf->buf = alloc_cache_memory_buffer(file);
	while (buf->buf == NULL) {
		/*
		 * TODO: alloc_cache_memory_buffer() should eventually free
		 *  some buffers.  Need a more sophisticated check here, instead
		 *  of just bailing if 100 tries does not result in getting a
		 *  free buffer.  This will involve using the sync channel's
		 *  semaphore to block until a buffer becomes available.
		 */
		if (count++ == 100) {
			SPDK_ERRLOG("could not allocate cache buffer\n");
			assert(false);
			free(buf);
			return NULL;
		}
		buf->buf = alloc_cache_memory_buffer(file);
	}

	buf->buf_size = CACHE_BUFFER_SIZE;
	buf->offset = offset;

	pthread_spin_lock(&g_caches_lock);
	if (file->tree->present_mask == 0) {
		TAILQ_INSERT_TAIL(&g_caches, file, cache_tailq);
	}
	file->tree = spdk_tree_insert_buffer(file->tree, buf);
	pthread_spin_unlock(&g_caches_lock);

	return buf;
}

static struct cache_buffer *
cache_append_buffer(struct spdk_file *file)
{
	struct cache_buffer *last;

	assert(file->last == NULL || file->last->bytes_filled == file->last->buf_size);
	assert((file->append_pos % CACHE_BUFFER_SIZE) == 0);

	last = cache_insert_buffer(file, file->append_pos);
	if (last == NULL) {
		SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "cache_insert_buffer failed\n");
		return NULL;
	}

	file->last = last;

	return last;
}

static void __check_sync_reqs(struct spdk_file *file);

static void
__file_cache_finish_sync(void *ctx, int bserrno)
{
	struct spdk_file *file = ctx;
	struct spdk_fs_request *sync_req;
	struct spdk_fs_cb_args *sync_args;

	pthread_spin_lock(&file->lock);
	sync_req = TAILQ_FIRST(&file->sync_requests);
	sync_args = &sync_req->args;
	assert(sync_args->op.sync.offset <= file->length_flushed);
	BLOBFS_TRACE(file, "sync done offset=%jx\n", sync_args->op.sync.offset);
	TAILQ_REMOVE(&file->sync_requests, sync_req, args.op.sync.tailq);
	pthread_spin_unlock(&file->lock);

	sync_args->fn.file_op(sync_args->arg, bserrno);
	__check_sync_reqs(file);

	pthread_spin_lock(&file->lock);
	free_fs_request(sync_req);
	pthread_spin_unlock(&file->lock);
}

static void
__free_args(struct spdk_fs_cb_args *args)
{
	struct spdk_fs_request *req;

	if (!args->from_request) {
		free(args);
	} else {
		/* Depends on args being at the start of the spdk_fs_request structure. */
		req = (struct spdk_fs_request *)args;
		free_fs_request(req);
	}
}

static void
__check_sync_reqs(struct spdk_file *file)
{
	struct spdk_fs_request *sync_req;

	pthread_spin_lock(&file->lock);

	TAILQ_FOREACH(sync_req, &file->sync_requests, args.op.sync.tailq) {
		if (sync_req->args.op.sync.offset <= file->length_flushed) {
			break;
		}
	}

	if (sync_req != NULL && !sync_req->args.op.sync.xattr_in_progress) {
		BLOBFS_TRACE(file, "set xattr length 0x%jx\n", file->length_flushed);
		sync_req->args.op.sync.xattr_in_progress = true;
		spdk_blob_set_xattr(file->blob, "length", &file->length_flushed,
				    sizeof(file->length_flushed));

		pthread_spin_unlock(&file->lock);
		spdk_blob_sync_md(file->blob, __file_cache_finish_sync, file);
	} else {
		pthread_spin_unlock(&file->lock);
	}
}

static void
__file_flush_done(void *arg, int bserrno)
{
	struct spdk_fs_cb_args *args = arg;
	struct spdk_file *file = args->file;
	struct cache_buffer *next = args->op.flush.cache_buffer;

	BLOBFS_TRACE(file, "length=%jx\n", args->op.flush.length);

	pthread_spin_lock(&file->lock);
	next->in_progress = false;
	next->bytes_flushed += args->op.flush.length;
	file->length_flushed += args->op.flush.length;
	if (file->length_flushed > file->length) {
		file->length = file->length_flushed;
	}
	if (next->bytes_flushed == next->buf_size) {
		BLOBFS_TRACE(file, "write buffer fully flushed 0x%jx\n", file->length_flushed);
		next = spdk_tree_find_buffer(file->tree, file->length_flushed);
	}

	/*
	 * Assert that there is no cached data that extends past the end of the underlying
	 *  blob.
	 */
	assert(next == NULL || next->offset < __file_get_blob_size(file) ||
	       next->bytes_filled == 0);

	pthread_spin_unlock(&file->lock);

	__check_sync_reqs(file);

	__file_flush(args);
}

static void
__file_flush(void *_args)
{
	struct spdk_fs_cb_args *args = _args;
	struct spdk_file *file = args->file;
	struct cache_buffer *next;
	uint64_t offset, length, start_lba, num_lba;
	uint32_t lba_size;

	pthread_spin_lock(&file->lock);
	next = spdk_tree_find_buffer(file->tree, file->length_flushed);
	if (next == NULL || next->in_progress) {
		/*
		 * There is either no data to flush, or a flush I/O is already in
		 *  progress.  So return immediately - if a flush I/O is in
		 *  progress we will flush more data after that is completed.
		 */
		__free_args(args);
		if (next == NULL) {
			/*
			 * For cases where a file's cache was evicted, and then the
			 *  file was later appended, we will write the data directly
			 *  to disk and bypass cache.  So just update length_flushed
			 *  here to reflect that all data was already written to disk.
			 */
			file->length_flushed = file->append_pos;
		}
		pthread_spin_unlock(&file->lock);
		if (next == NULL) {
			/*
			 * There is no data to flush, but we still need to check for any
			 *  outstanding sync requests to make sure metadata gets updated.
			 */
			__check_sync_reqs(file);
		}
		return;
	}

	offset = next->offset + next->bytes_flushed;
	length = next->bytes_filled - next->bytes_flushed;
	if (length == 0) {
		__free_args(args);
		pthread_spin_unlock(&file->lock);
		return;
	}
	args->op.flush.length = length;
	args->op.flush.cache_buffer = next;

	__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);

	next->in_progress = true;
	BLOBFS_TRACE(file, "offset=%jx length=%jx page start=%jx num=%jx\n",
		     offset, length, start_lba, num_lba);
	pthread_spin_unlock(&file->lock);
	spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
			   next->buf + (start_lba * lba_size) - next->offset,
			   start_lba, num_lba, __file_flush_done, args);
}

static void
__file_extend_done(void *arg, int bserrno)
{
	struct spdk_fs_cb_args *args = arg;

	__wake_caller(args, bserrno);
}

static void
__file_extend_resize_cb(void *_args, int bserrno)
{
	struct spdk_fs_cb_args *args = _args;
	struct spdk_file *file = args->file;

	if (bserrno) {
		__wake_caller(args, bserrno);
		return;
	}

	spdk_blob_sync_md(file->blob, __file_extend_done, args);
}

static void
__file_extend_blob(void *_args)
{
	struct spdk_fs_cb_args *args = _args;
	struct spdk_file *file = args->file;

	spdk_blob_resize(file->blob, args->op.resize.num_clusters, __file_extend_resize_cb, args);
}

static void
__rw_from_file_done(void *arg, int bserrno)
{
	struct spdk_fs_cb_args *args = arg;

	__wake_caller(args, bserrno);
	__free_args(args);
}

static void
__rw_from_file(void *_args)
{
	struct spdk_fs_cb_args *args = _args;
	struct spdk_file *file = args->file;

	if (args->op.rw.is_read) {
		spdk_file_read_async(file, file->fs->sync_target.sync_io_channel, args->op.rw.user_buf,
				     args->op.rw.offset, args->op.rw.length,
				     __rw_from_file_done, args);
	} else {
		spdk_file_write_async(file, file->fs->sync_target.sync_io_channel, args->op.rw.user_buf,
				      args->op.rw.offset, args->op.rw.length,
				      __rw_from_file_done, args);
	}
}

static int
__send_rw_from_file(struct spdk_file *file, sem_t *sem, void *payload,
		    uint64_t offset, uint64_t length, bool is_read)
{
	struct spdk_fs_cb_args *args;

	args = calloc(1, sizeof(*args));
	if (args == NULL) {
		sem_post(sem);
		return -ENOMEM;
	}

	args->file = file;
	args->sem = sem;
	args->op.rw.user_buf = payload;
	args->op.rw.offset = offset;
	args->op.rw.length = length;
	args->op.rw.is_read = is_read;
	file->fs->send_request(__rw_from_file, args);
	return 0;
}

int
spdk_file_write(struct spdk_file *file, struct spdk_io_channel *_channel,
		void *payload, uint64_t offset, uint64_t length)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_cb_args *args;
	uint64_t rem_length, copy, blob_size, cluster_sz;
	uint32_t cache_buffers_filled = 0;
	uint8_t *cur_payload;
	struct cache_buffer *last;

	BLOBFS_TRACE_RW(file, "offset=%jx length=%jx\n", offset, length);

	if (length == 0) {
		return 0;
	}

	if (offset != file->append_pos) {
		BLOBFS_TRACE(file, " error offset=%jx append_pos=%jx\n", offset, file->append_pos);
		return -EINVAL;
	}

	pthread_spin_lock(&file->lock);
	file->open_for_writing = true;

	if (file->last == NULL) {
		if (file->append_pos % CACHE_BUFFER_SIZE == 0) {
			cache_append_buffer(file);
		} else {
			int rc;

			file->append_pos += length;
			pthread_spin_unlock(&file->lock);
			rc = __send_rw_from_file(file, &channel->sem, payload,
						 offset, length, false);
			sem_wait(&channel->sem);
			return rc;
		}
	}

	blob_size = __file_get_blob_size(file);

	if ((offset + length) > blob_size) {
		struct spdk_fs_cb_args extend_args = {};

		cluster_sz = file->fs->bs_opts.cluster_sz;
		extend_args.sem = &channel->sem;
		extend_args.op.resize.num_clusters = __bytes_to_clusters((offset + length), cluster_sz);
		extend_args.file = file;
		BLOBFS_TRACE(file, "start resize to %u clusters\n", extend_args.op.resize.num_clusters);
		pthread_spin_unlock(&file->lock);
		file->fs->send_request(__file_extend_blob, &extend_args);
		sem_wait(&channel->sem);
		if (extend_args.rc) {
			return extend_args.rc;
		}
	}

	last = file->last;
	rem_length = length;
	cur_payload = payload;
	while (rem_length > 0) {
		copy = last->buf_size - last->bytes_filled;
		if (copy > rem_length) {
			copy = rem_length;
		}
		BLOBFS_TRACE_RW(file, "  fill offset=%jx length=%jx\n", file->append_pos, copy);
		memcpy(&last->buf[last->bytes_filled], cur_payload, copy);
		file->append_pos += copy;
		if (file->length < file->append_pos) {
			file->length = file->append_pos;
		}
		cur_payload += copy;
		last->bytes_filled += copy;
		rem_length -= copy;
		if (last->bytes_filled == last->buf_size) {
			cache_buffers_filled++;
			last = cache_append_buffer(file);
			if (last == NULL) {
				BLOBFS_TRACE(file, "nomem\n");
				pthread_spin_unlock(&file->lock);
				return -ENOMEM;
			}
		}
	}

	pthread_spin_unlock(&file->lock);

	if (cache_buffers_filled == 0) {
		return 0;
	}

	args = calloc(1, sizeof(*args));
	if (args == NULL) {
		return -ENOMEM;
	}

	args->file = file;
	file->fs->send_request(__file_flush, args);
	return 0;
}

static void
__readahead_done(void *arg, int bserrno)
{
	struct spdk_fs_cb_args *args = arg;
	struct cache_buffer *cache_buffer = args->op.readahead.cache_buffer;
	struct spdk_file *file = args->file;

	BLOBFS_TRACE(file, "offset=%jx\n", cache_buffer->offset);

	pthread_spin_lock(&file->lock);
	cache_buffer->bytes_filled = args->op.readahead.length;
	cache_buffer->bytes_flushed = args->op.readahead.length;
	cache_buffer->in_progress = false;
	pthread_spin_unlock(&file->lock);

	__free_args(args);
}

static void
__readahead(void *_args)
{
	struct spdk_fs_cb_args *args = _args;
	struct spdk_file *file = args->file;
	uint64_t offset, length, start_lba, num_lba;
	uint32_t lba_size;

	offset = args->op.readahead.offset;
	length = args->op.readahead.length;
	assert(length > 0);

	__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);

	BLOBFS_TRACE(file, "offset=%jx length=%jx page start=%jx num=%jx\n",
		     offset, length, start_lba, num_lba);
	spdk_blob_io_read(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
			  args->op.readahead.cache_buffer->buf,
			  start_lba, num_lba, __readahead_done, args);
}

static uint64_t
__next_cache_buffer_offset(uint64_t offset)
{
	return (offset + CACHE_BUFFER_SIZE) & ~(CACHE_TREE_LEVEL_MASK(0));
}

static void
check_readahead(struct spdk_file *file, uint64_t offset)
{
	struct spdk_fs_cb_args *args;

	offset = __next_cache_buffer_offset(offset);
	if (spdk_tree_find_buffer(file->tree, offset) != NULL || file->length <= offset) {
		return;
	}

	args = calloc(1, sizeof(*args));
	if (args == NULL) {
		return;
	}

	BLOBFS_TRACE(file, "offset=%jx\n", offset);

	args->file = file;
	args->op.readahead.offset = offset;
	args->op.readahead.cache_buffer = cache_insert_buffer(file, offset);
	if (!args->op.readahead.cache_buffer) {
		BLOBFS_TRACE(file, "Cannot allocate buf for offset=%jx\n", offset);
		free(args);
		return;
	}

	args->op.readahead.cache_buffer->in_progress = true;
	if (file->length < (offset + CACHE_BUFFER_SIZE)) {
		args->op.readahead.length = file->length & (CACHE_BUFFER_SIZE - 1);
	} else {
		args->op.readahead.length = CACHE_BUFFER_SIZE;
	}
	file->fs->send_request(__readahead, args);
}

static int
__file_read(struct spdk_file *file, void *payload, uint64_t offset, uint64_t length, sem_t *sem)
{
	struct cache_buffer *buf;
	int rc;

	buf = spdk_tree_find_filled_buffer(file->tree, offset);
	if (buf == NULL) {
		pthread_spin_unlock(&file->lock);
		rc = __send_rw_from_file(file, sem, payload, offset, length, true);
		pthread_spin_lock(&file->lock);
		return rc;
	}

	if ((offset + length) > (buf->offset + buf->bytes_filled)) {
		length = buf->offset + buf->bytes_filled - offset;
	}
	BLOBFS_TRACE(file, "read %p offset=%ju length=%ju\n", payload, offset, length);
	memcpy(payload, &buf->buf[offset - buf->offset], length);
	if ((offset + length) % CACHE_BUFFER_SIZE == 0) {
		pthread_spin_lock(&g_caches_lock);
		spdk_tree_remove_buffer(file->tree, buf);
		if (file->tree->present_mask == 0) {
			TAILQ_REMOVE(&g_caches, file, cache_tailq);
		}
		pthread_spin_unlock(&g_caches_lock);
	}

	sem_post(sem);
	return 0;
}

int64_t
spdk_file_read(struct spdk_file *file, struct spdk_io_channel *_channel,
	       void *payload, uint64_t offset, uint64_t length)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	uint64_t final_offset, final_length;
	uint32_t sub_reads = 0;
	int rc = 0;

	pthread_spin_lock(&file->lock);

	BLOBFS_TRACE_RW(file, "offset=%ju length=%ju\n", offset, length);

	file->open_for_writing = false;

	if (length == 0 || offset >= file->append_pos) {
		pthread_spin_unlock(&file->lock);
		return 0;
	}

	if (offset + length > file->append_pos) {
		length = file->append_pos - offset;
	}

	if (offset != file->next_seq_offset) {
		file->seq_byte_count = 0;
	}
	file->seq_byte_count += length;
	file->next_seq_offset = offset + length;
	if (file->seq_byte_count >= CACHE_READAHEAD_THRESHOLD) {
		check_readahead(file, offset);
		check_readahead(file, offset + CACHE_BUFFER_SIZE);
	}

	final_length = 0;
	final_offset = offset + length;
	while (offset < final_offset) {
		length = NEXT_CACHE_BUFFER_OFFSET(offset) - offset;
		if (length > (final_offset - offset)) {
			length = final_offset - offset;
		}
		rc = __file_read(file, payload, offset, length, &channel->sem);
		if (rc == 0) {
			final_length += length;
		} else {
			break;
		}
		payload += length;
		offset += length;
		sub_reads++;
	}
	pthread_spin_unlock(&file->lock);
	while (sub_reads-- > 0) {
		sem_wait(&channel->sem);
	}
	if (rc == 0) {
		return final_length;
	} else {
		return rc;
	}
}

static void
_file_sync(struct spdk_file *file, struct spdk_fs_channel *channel,
	   spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_fs_request *sync_req;
	struct spdk_fs_request *flush_req;
	struct spdk_fs_cb_args *sync_args;
	struct spdk_fs_cb_args *flush_args;

	BLOBFS_TRACE(file, "offset=%jx\n", file->append_pos);

	pthread_spin_lock(&file->lock);
	if (file->append_pos <= file->length_flushed) {
		BLOBFS_TRACE(file, "done - no data to flush\n");
		pthread_spin_unlock(&file->lock);
		cb_fn(cb_arg, 0);
		return;
	}

	sync_req = alloc_fs_request(channel);
	if (!sync_req) {
		pthread_spin_unlock(&file->lock);
		cb_fn(cb_arg, -ENOMEM);
		return;
	}
	sync_args = &sync_req->args;

	flush_req = alloc_fs_request(channel);
	if (!flush_req) {
		pthread_spin_unlock(&file->lock);
		cb_fn(cb_arg, -ENOMEM);
		return;
	}
	flush_args = &flush_req->args;

	sync_args->file = file;
	sync_args->fn.file_op = cb_fn;
	sync_args->arg = cb_arg;
	sync_args->op.sync.offset = file->append_pos;
	sync_args->op.sync.xattr_in_progress = false;
	TAILQ_INSERT_TAIL(&file->sync_requests, sync_req, args.op.sync.tailq);
	pthread_spin_unlock(&file->lock);

	flush_args->file = file;
	channel->send_request(__file_flush, flush_args);
}

int
spdk_file_sync(struct spdk_file *file, struct spdk_io_channel *_channel)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_cb_args args = {};

	args.sem = &channel->sem;
	_file_sync(file, channel, __wake_caller, &args);
	sem_wait(&channel->sem);

	return args.rc;
}

void
spdk_file_sync_async(struct spdk_file *file, struct spdk_io_channel *_channel,
		     spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);

	_file_sync(file, channel, cb_fn, cb_arg);
}

void
spdk_file_set_priority(struct spdk_file *file, uint32_t priority)
{
	BLOBFS_TRACE(file, "priority=%u\n", priority);
	file->priority = priority;

}

/*
 * Close routines
 */

static void
__file_close_async_done(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;

	if (file->is_deleted) {
		spdk_fs_delete_file_async(file->fs, file->name, blob_delete_cb, ctx);
		return;
	}

	args->fn.file_op(args->arg, bserrno);
	free_fs_request(req);
}

static void
__file_close_async(struct spdk_file *file, struct spdk_fs_request *req)
{
	struct spdk_blob *blob;

	pthread_spin_lock(&file->lock);
	if (file->ref_count == 0) {
		pthread_spin_unlock(&file->lock);
		__file_close_async_done(req, -EBADF);
		return;
	}

	file->ref_count--;
	if (file->ref_count > 0) {
		pthread_spin_unlock(&file->lock);
		req->args.fn.file_op(req->args.arg, 0);
		free_fs_request(req);
		return;
	}

	pthread_spin_unlock(&file->lock);

	blob = file->blob;
	file->blob = NULL;
	spdk_blob_close(blob, __file_close_async_done, req);
}

static void
__file_close_async__sync_done(void *arg, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	__file_close_async(args->file, req);
}

void
spdk_file_close_async(struct spdk_file *file, spdk_file_op_complete cb_fn, void *cb_arg)
{
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	req = alloc_fs_request(file->fs->md_target.md_fs_channel);
	if (req == NULL) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	args = &req->args;
	args->file = file;
	args->fn.file_op = cb_fn;
	args->arg = cb_arg;

	spdk_file_sync_async(file, file->fs->md_target.md_io_channel, __file_close_async__sync_done, req);
}

static void
__file_close(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;

	__file_close_async(file, req);
}

int
spdk_file_close(struct spdk_file *file, struct spdk_io_channel *_channel)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;

	req = alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;

	spdk_file_sync(file, _channel);
	BLOBFS_TRACE(file, "name=%s\n", file->name);
	args->file = file;
	args->sem = &channel->sem;
	args->fn.file_op = __wake_caller;
	args->arg = req;
	channel->send_request(__file_close, req);
	sem_wait(&channel->sem);

	return args->rc;
}

int
spdk_file_get_id(struct spdk_file *file, void *id, size_t size)
{
	if (size < sizeof(spdk_blob_id)) {
		return -EINVAL;
	}

	memcpy(id, &file->blobid, sizeof(spdk_blob_id));

	return sizeof(spdk_blob_id);
}

static void
cache_free_buffers(struct spdk_file *file)
{
	BLOBFS_TRACE(file, "free=%s\n", file->name);
	pthread_spin_lock(&file->lock);
	pthread_spin_lock(&g_caches_lock);
	if (file->tree->present_mask == 0) {
		pthread_spin_unlock(&g_caches_lock);
		pthread_spin_unlock(&file->lock);
		return;
	}
	spdk_tree_free_buffers(file->tree);

	TAILQ_REMOVE(&g_caches, file, cache_tailq);
	/* If not freed, put it in the end of the queue */
	if (file->tree->present_mask != 0) {
		TAILQ_INSERT_TAIL(&g_caches, file, cache_tailq);
	}
	file->last = NULL;
	pthread_spin_unlock(&g_caches_lock);
	pthread_spin_unlock(&file->lock);
}








/* new interfaces for supporting random access and direct I/O in BlobFS */
#include "spdk/barrier.h"
#include <fcntl.h>

#ifndef __linux__
#define O_DIRECT 1
#endif

static TAILQ_HEAD(, cache_buffer) g_zeroref_caches;
static uint64_t g_dmasize = 0;

static void init_blobfs2(void) {
	TAILQ_INIT(&g_zeroref_caches);
	if (CACHE_BUFFER_SIZE < sysconf(_SC_PAGESIZE)) {
		SPDK_WARNLOG("CacheBufferShift should be larger than page shift for this platform. This hurts Blobfs2 performance\n");
	}
}

static void blobfs2_free_buffer(struct cache_buffer * buf) {
    if (buf != NULL) {
        spdk_dma_free(buf->buf);
        free(buf);
        g_dmasize -= CACHE_BUFFER_SIZE;
    }
}

static struct cache_buffer * blobfs2_alloc_buffer(struct spdk_blob_store * bs)
{
	struct cache_buffer *buf;
	void * dma = NULL;

	if (g_dmasize < g_fs_cache_size) {
		dma = spdk_dma_malloc(CACHE_BUFFER_SIZE, spdk_bs_get_io_unit_size(bs), NULL);
		if (dma) {
			g_dmasize += CACHE_BUFFER_SIZE;
		}
	}

	if (!dma) {
		buf = TAILQ_FIRST(&g_zeroref_caches);
		if (!buf) {
			return NULL;
		}
		TAILQ_REMOVE(&g_zeroref_caches, buf, zeroref_tailq);
	} else {
		buf = calloc(1, sizeof(*buf));
		if (!buf) {
			spdk_dma_free(dma);
			return NULL;
		}
		buf->buf = dma;
	}

	buf->buf_size = CACHE_BUFFER_SIZE;
	buf->ref = 1;
	memset(buf->buf, 0, CACHE_BUFFER_SIZE);

	return buf;
}

static void blobfs2_insert_buffer(struct spdk_file *file, struct cache_buffer * buf, uint64_t offset)
{
	buf->offset = offset;
	if (file->tree->present_mask == 0) {
		TAILQ_INSERT_TAIL(&g_caches, file, cache_tailq);
	}

    file->tree = spdk_tree_insert_buffer(file->tree, buf);
}

static void blobfs2_buffer_up(struct cache_buffer * buffer)
{
    if (buffer->ref++ == 0) {
        if (TAILQ_NEXT(buffer, zeroref_tailq)) {
            TAILQ_REMOVE(&g_zeroref_caches, buffer, zeroref_tailq);
        }
    }
}

static struct cache_buffer * blobfs2_get_buffer(struct spdk_file * file, uint64_t offset)
{
    struct cache_buffer * buffer;

    buffer = spdk_tree_find_buffer(file->tree, offset);
	if (buffer) {
        blobfs2_buffer_up(buffer);
	}
    return buffer;
}

static void blobfs2_put_buffer(struct cache_buffer * buffer)
{
    if (--buffer->ref == 0 && !buffer->in_progress && !buffer->dirty) {
		TAILQ_INSERT_TAIL(&g_zeroref_caches, buffer, zeroref_tailq);
    }
}

static struct spdk_fs_request * blobfs2_alloc_fs_request(struct spdk_fs_channel * channel)
{
	struct spdk_fs_request * req = alloc_fs_request(channel);
	if (req) {
		req->args.from_request = true;
		return req;
	}
	req = calloc(1, sizeof(*req));
	if (req) {
		req->args.from_request = false;
		return req;
	}
	return NULL;
}

static void blobfs2_free_fs_request(struct spdk_fs_request * req)
{
	if (!req) {
		return;
	}
	if (req->args.from_request) {
		free_fs_request(req);
	} else {
		free(req);
	}
}

static void __blobfs2_rw_cb(void * _args);

static void __blobfs2_rw_last(struct cache_buffer *buffer, struct spdk_fs_request *req, int rc)
{
    struct spdk_file * file = req->args.file;
	struct spdk_fs_cb_args * args = &req->args;
    struct spdk_fs_request * head, * dreq;

    if (!args->op.blobfs2_rw.is_read) {
		free(args->op.blobfs2_rw.user_buf);
		args->op.blobfs2_rw.user_buf = NULL;
    }

	// for write barrier/sync
	TAILQ_REMOVE(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
	head = TAILQ_FIRST(&file->sync_requests);
    if (head && head->args.fn.file_op) {
		head->args.fn.file_op(req, rc);
    }

    if (buffer) {
        buffer->in_progress = false;
        blobfs2_put_buffer(buffer);
        // resubmit delayed reads/writes
        TAILQ_FOREACH(dreq, &buffer->write_waiter, args.op.blobfs2_rw.write_tailq) {
            dreq->args.op.blobfs2_rw.delayed = true;
            req->channel->send_request(__blobfs2_rw_cb, dreq);
        }
    }

    if (args->sem) {
		args->rc = rc;
		sem_post(args->sem);
	}
	blobfs2_free_fs_request(req);
}

static void __blobfs2_buffer_flush_done(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
	struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
	struct spdk_file * file = args->file;

	if (bserrno == 0) {
        buffer->dirty = false;
        TAILQ_REMOVE(&file->dirty_buffers, buffer, dirty_tailq);
    }
	__blobfs2_rw_last(buffer, req, bserrno);
}

static void __blobfs2_flush_buffer(void * _args)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file *file = args->file;
	struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
	uint64_t offset = args->op.blobfs2_rw.offset;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
	uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;

	if (!buffer || !buffer->dirty) {
		// no need to flush
		__blobfs2_rw_last(buffer, req, 0);
	    return;
	}

    __get_page_parameters(file, buffer_offset, CACHE_BUFFER_SIZE, &start_lba, &lba_size, &num_lba);
    spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
                       buffer->buf + (start_lba * lba_size) - buffer_offset,
                       start_lba, num_lba, __blobfs2_buffer_flush_done, req);
}

static void __blobfs2_rw_copy_buffer(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
    void * payload = args->op.blobfs2_rw.user_buf;
    uint64_t offset = args->op.blobfs2_rw.offset;
    uint64_t length = args->op.blobfs2_rw.length;
    uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
	int64_t copylen;

    if (bserrno) {
        SPDK_ERRLOG("failed to fetch on-disk data\n");
		__blobfs2_rw_last(buffer, req, -EIO);
        return;
    }

	copylen = (offset + length < buffer_offset + CACHE_BUFFER_SIZE ? length: CACHE_BUFFER_SIZE - (offset - buffer_offset));

    if (args->op.blobfs2_rw.is_read) {
		memcpy(payload, buffer->buf + offset - buffer_offset, copylen);
    } else {
		memcpy(buffer->buf + offset - buffer_offset, payload, copylen);
		buffer->dirty = true;
		TAILQ_INSERT_TAIL(&file->dirty_buffers, buffer, dirty_tailq);
		if (args->op.blobfs2_rw.oflag & (O_SYNC | O_DSYNC)) {
			args->rc64 = copylen;
			__blobfs2_flush_buffer(req);
			return;
		}
    }
	__blobfs2_rw_last(buffer, req, 0);
}

static void __blobfs2_rw_buffered(struct spdk_fs_request * req, struct spdk_file * file, uint64_t offset, uint64_t length)
{
	struct spdk_fs_cb_args * args = &req->args;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
	uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
	struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;

	if (!args->op.blobfs2_rw.is_read && offset % CACHE_BUFFER_SIZE == 0 && length % CACHE_BUFFER_SIZE == 0) {
		// aligned write. we don't need fetch before write. copy and return immediately.
		__blobfs2_rw_copy_buffer(req, 0);
	} else {
		// fetch on-disk data
		__get_page_parameters(file, buffer_offset, CACHE_BUFFER_SIZE, &start_lba, &lba_size, &num_lba);
		spdk_blob_io_read(args->file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						  buffer->buf, start_lba, num_lba, __blobfs2_rw_copy_buffer, req);
	}
}

static void __blobfs2_rw_direct_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	uint64_t length = args->op.blobfs2_rw.length;

    if (args->op.blobfs2_rw.is_read) {
		memcpy(args->op.blobfs2_rw.user_buf, args->op.blobfs2_rw.pin_buf, length);
    }
    spdk_dma_free(req->args.op.blobfs2_rw.pin_buf);
	__blobfs2_rw_last(NULL, (struct spdk_fs_request *) _args, bserrno);
}

static void __blobfs2_rw_direct(struct spdk_fs_request * req, struct spdk_file * file, uint64_t offset, uint64_t length)
{
	struct spdk_fs_cb_args * args = &req->args;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
	uint64_t align = spdk_bs_get_io_unit_size(file->fs->bs);
	void * dma;

	// we need to alloc temporal buffer to issue DMA
	dma = spdk_dma_malloc(length, align, NULL);
	if (!dma) {
		SPDK_ERRLOG("failed to alloc buffer\n");
		__blobfs2_rw_last(NULL, req, -ENOMEM);
		return;
	}

	__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);
	args->op.blobfs2_rw.pin_buf = dma;
	if (args->op.blobfs2_rw.is_read) {
		spdk_blob_io_read(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
				dma + (start_lba * lba_size) - offset,
				start_lba, num_lba, __blobfs2_rw_direct_done, req);
	} else {
		memcpy(dma, args->op.blobfs2_rw.user_buf, length);
		spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
				dma + (start_lba * lba_size) - offset,
				start_lba, num_lba, __blobfs2_rw_direct_done, req);
	}
}

static void __blobfs2_rw_cachemiss(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    uint64_t offset = args->op.blobfs2_rw.offset;
    uint64_t length = args->op.blobfs2_rw.length;

    if (bserrno) {
		__blobfs2_rw_last(NULL, req, bserrno);
    	return;
    }

    if (!args->op.blobfs2_rw.is_read && (args->op.blobfs2_rw.oflag & O_DSYNC) && args->sem) {
    	sem_post(args->sem);
    	args->sem = NULL;
    }

    if (args->op.blobfs2_rw.oflag & O_DIRECT) {
		__blobfs2_rw_direct(req, file, offset, length);
    } else {
    	__blobfs2_rw_buffered(req, file, offset, length);
    }
}

static void __blobfs2_resubmit_rw_after_resize(struct spdk_fs_request * req)
{
	struct spdk_fs_request * dreq;
	struct spdk_file * file = req->args.file;

	TAILQ_FOREACH(dreq, &file->resize_waiter, args.op.blobfs2_rw.resize_tailq) {
		dreq->args.op.blobfs2_rw.delayed = true;
		req->channel->send_request(__blobfs2_rw_cb, dreq);
	}
}

static void __blobfs2_resize_done(void * _args, int bserrno) {
    struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;

	file->resizing = false;
	__blobfs2_resubmit_rw_after_resize(req);

    if (bserrno) {
		args->fn.resize_op(_args, bserrno);
        return;
    }

	file->length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
    args->fn.resize_op(_args, bserrno);
}

static void __blobfs2_write_resize_cb(void * _args, int bserrno) {
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;

    if (bserrno) {
        file->resizing = false;
		__blobfs2_resubmit_rw_after_resize(req);
		args->fn.resize_op(_args, bserrno);
        return;
    }

    uint64_t length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
    spdk_blob_set_xattr(file->blob, "length", &length, sizeof(length));
    spdk_blob_sync_md(args->file->blob, __blobfs2_resize_done, req);
}

static void __blobfs2_rw_cb(void * _args)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    struct cache_buffer * buffer;
    uint64_t offset = args->op.blobfs2_rw.offset;
    uint64_t length = args->op.blobfs2_rw.length;
    uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;

    if (!args->op.blobfs2_rw.delayed) {
        // we will run blocking ops in the next callback. record write ordering
        TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
    } else {
        args->op.blobfs2_rw.delayed = false;
    }

    if (file->resizing) {
        // racy resize exists. wait for its completion
        TAILQ_INSERT_TAIL(&file->resize_waiter, req, args.op.blobfs2_rw.resize_tailq);
        return;
    }

	if (args->op.blobfs2_rw.is_read && offset + length > file->length) {
		length = file->length - offset;
		args->op.blobfs2_rw.length = length;
	}

    buffer = blobfs2_get_buffer(file, buffer_offset);
    if (buffer) {
        if (args->op.blobfs2_rw.oflag & O_DIRECT) {
			__blobfs2_rw_last(buffer, req, -EIO);
            return;
        }
		if (buffer->in_progress) {
			// racy write exists. wait for its completion.
			TAILQ_INSERT_TAIL(&buffer->write_waiter, req, args.op.blobfs2_rw.write_tailq);
			return;
		}
        // cache hit. copy and return immediately.
        args->op.blobfs2_rw.buffer = buffer;
        __blobfs2_rw_copy_buffer(req, 0);
        return;
    }
    buffer = blobfs2_alloc_buffer(file->fs->bs);
    if (!buffer) {
        __blobfs2_rw_last(buffer, req, -ENOMEM);
        return;
    }
    buffer->in_progress = true;
    blobfs2_insert_buffer(file, buffer, buffer_offset);
    args->op.blobfs2_rw.buffer = buffer;

	if (!args->op.blobfs2_rw.is_read && offset + length > file->length) {
		uint64_t sz = __bytes_to_clusters(offset + length, file->fs->bs_opts.cluster_sz);
		file->resizing = true;
		args->fn.resize_op = __blobfs2_rw_cachemiss;
		spdk_blob_resize(file->blob, sz, __blobfs2_write_resize_cb, req);
	} else {
		__blobfs2_rw_cachemiss(req, 0);
	}
}

static int64_t blobfs2_rw(struct spdk_file *file, struct spdk_io_channel * _channel, void * payload, uint64_t offset, uint64_t length, int oflag, bool is_read)
{
	struct spdk_fs_channel * channel;
	struct spdk_fs_request * req;
	uint64_t align = spdk_bs_get_io_unit_size(file->fs->bs);
	void * user_buf = NULL;
	bool waitrc = (is_read || (oflag & (O_SYNC | O_DSYNC)));

	if (length == 0) {
		return 0;
	}

	if ((oflag & O_DIRECT) && ((uint64_t)payload % align != 0 || offset % align != 0 || length % align != 0)) {
		return -EINVAL;
	}

	channel = spdk_io_channel_get_ctx(_channel);
	if (!channel) {
		return -EINVAL;
	}

	if (!is_read) {
		// TODO: use copy-on-write for large length
		user_buf = malloc(length);
		if (!user_buf) {
			return -ENOMEM;
		}
		memcpy(user_buf, payload, length);
	}

	req = blobfs2_alloc_fs_request(channel);
	if (!req) {
		return -ENOMEM;
	}

	// setup a non-blocking call to avoid blocking at racy buffer cache update
	req->args.file = file;
	req->args.op.blobfs2_rw.offset = offset;
	req->args.op.blobfs2_rw.length = length;
	req->args.op.blobfs2_rw.oflag = oflag;
	req->args.op.blobfs2_rw.is_read = is_read;
	req->args.op.blobfs2_rw.needrc = false;
	req->args.op.blobfs2_rw.delayed = false;
	req->args.fn.file_op = NULL;
	req->args.op.blobfs2_rw.user_buf = is_read ? payload: user_buf;
	req->args.sem = waitrc ? &channel->sem: NULL;

	channel->send_request(__blobfs2_rw_cb, req);
	if (waitrc) {
		sem_wait(&channel->sem);
		return length;
	}

	return is_read ? length: 0; // terrible. original blobfs does this
}

int64_t blobfs2_write(struct spdk_file *file, struct spdk_io_channel * _channel, void * payload, uint64_t offset, uint64_t length, int oflag)
{
	return blobfs2_rw(file, _channel, payload, offset, length, oflag, false);
}

int64_t blobfs2_read(struct spdk_file *file, struct spdk_io_channel * _channel, void *payload, uint64_t offset, uint64_t length, int oflag)
{
	return blobfs2_rw(file, _channel, payload, offset, length, oflag, true);
}

static void __blobfs2_sync_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	req->args.rc = bserrno;
	sem_post(req->args.sem);
	blobfs2_free_fs_request(req);
}

static void __blobfs2_sync_cb(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	struct cache_buffer * buffer;

	if (TAILQ_EMPTY(&file->dirty_buffers)) {
		args->rc = 0;
		sem_post(args->sem);
        blobfs2_free_fs_request(req);
		return;
	}

	TAILQ_FOREACH(buffer, &file->dirty_buffers, dirty_tailq) {
		struct spdk_fs_request * subreq = blobfs2_alloc_fs_request(req->channel);

		if (!subreq) {
			args->rc = -ENOMEM;
			sem_post(args->sem);
            blobfs2_free_fs_request(req);
			return;
		}

		blobfs2_buffer_up(buffer);
		subreq->args.file = file;
		subreq->args.op.blobfs2_rw.offset = buffer->offset;
		subreq->args.op.blobfs2_rw.length = CACHE_BUFFER_SIZE;
		subreq->args.fn.file_op = NULL;
		subreq->args.sem = NULL;
		TAILQ_INSERT_TAIL(&file->sync_requests, subreq, args.op.blobfs2_rw.sync_tailq);
		if (TAILQ_NEXT(buffer, dirty_tailq) == NULL) {
			args->fn.file_op = __blobfs2_sync_done;
			TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
		}
		__blobfs2_flush_buffer(subreq);
	}
}

static void __blobfs2_barrier_cb(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;

	if (!TAILQ_EMPTY(&file->sync_requests)) {
		TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
	} else {
		args->fn.file_op(_args, 0);
	}
}

int blobfs2_sync(struct spdk_file * file, struct spdk_io_channel * _channel)
{
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
    struct spdk_fs_request * req;

    req = blobfs2_alloc_fs_request(channel);
    if (!req) {
    	return -ENOMEM;
    }

    req->args.file = file;
    req->args.sem = &channel->sem;
    req->args.fn.file_op = __blobfs2_sync_cb;
    channel->send_request(__blobfs2_barrier_cb, req);
	sem_wait(&channel->sem);

    return req->args.rc;
}

int blobfs2_barrier(struct spdk_file * file, struct spdk_io_channel * _channel) {
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request * req;

	req = blobfs2_alloc_fs_request(channel);
	if (!req) {
		return -ENOMEM;
	}

	req->args.file = file;
	req->args.sem = &channel->sem;
	req->args.fn.file_op = __wake_caller;
	channel->send_request(__blobfs2_barrier_cb, req);
	sem_wait(&channel->sem);

	return req->args.rc;
}

/*static void __blobfs2_close_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	uint64_t off;

	for (off = 0; off < file->length; off += CACHE_BUFFER_SIZE) {
		blobfs2_free_buffer(spdk_tree_find_buffer(file->tree, off));
	}

	args->rc = bserrno;
	sem_post(args->sem);
}*/

int blobfs2_close(struct spdk_file * file, struct spdk_io_channel * _channel)
{
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request * req;
	struct spdk_fs_cb_args * args;

	req = blobfs2_alloc_fs_request(channel);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;

	blobfs2_sync(file, _channel);
	BLOBFS_TRACE(file, "name=%s\n", file->name);
	args->file = file;
	args->sem = &channel->sem;
	args->fn.file_op = __wake_caller;
	args->arg = req;
	channel->send_request(__file_close, req);
	sem_wait(&channel->sem);

	return args->rc;
}

SPDK_LOG_REGISTER_COMPONENT("blobfs", SPDK_LOG_BLOBFS)
SPDK_LOG_REGISTER_COMPONENT("blobfs_rw", SPDK_LOG_BLOBFS_RW)
