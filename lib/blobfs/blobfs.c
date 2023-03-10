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

#define BLOBFS_DEFAULT_CACHE_SIZE_IN_GB (40)
#define SPDK_BLOBFS_DEFAULT_OPTS_CLUSTER_SZ (1024 * 1024)

static uint64_t g_page_size = 0;
static int g_fs_cache_size_in_gb = BLOBFS_DEFAULT_CACHE_SIZE_IN_GB;
static int g_channel_heap_in_gb = BLOBFS_DEFAULT_CACHE_SIZE_IN_GB;
//static int g_queue_depth = 128;
static struct spdk_mempool *g_cache_pool;
static TAILQ_HEAD(, spdk_file) g_caches;
static int g_fs_count = 0;
static pthread_mutex_t g_cache_init_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_spinlock_t g_caches_lock;
static int g_dirty_ratio = 20;

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
	uint64_t		length_flushed;
	uint64_t		append_pos;
	uint64_t		seq_byte_count;
	uint64_t		next_seq_offset;
	uint32_t		priority;
	TAILQ_ENTRY(spdk_file)	tailq;
	spdk_blob_id		blobid;
	uint32_t		ref_count;
	pthread_spinlock_t	lock;
	struct cache_buffer	*last;
	struct cache_tree	*tree;
	TAILQ_HEAD(open_requests_head, spdk_fs_request) open_requests;
    TAILQ_HEAD(sync_requests_head, spdk_fs_request) sync_requests;
    TAILQ_HEAD(dirty_buffer_head, cache_buffer) dirty_buffers;
	TAILQ_HEAD(resize_waiter_head, spdk_fs_request) resize_waiter;
	TAILQ_HEAD(evict_waiter_head, spdk_fs_request) evict_waiter;
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
	bool need_dma;

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
	union {
        spdk_file_op_complete			file_op;
		fs_request_fn  resize_op;
		fs_request_fn  write_op;
	} delayed_fn;
	void *arg;
	sem_t *sem;
	int cond;
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
            struct channel_heap * user_buf;
            void * pin_buf;
            uint64_t offset;
            uint64_t length;
			int oflag;
            bool is_read;
            bool delayed;
            int ubuf_heap_index;
            struct cache_buffer * buffer;
            TAILQ_ENTRY(spdk_fs_request) sync_tailq;
            TAILQ_ENTRY(spdk_fs_request) resize_tailq;
			TAILQ_ENTRY(spdk_fs_request) write_tailq;
			TAILQ_ENTRY(spdk_fs_request) evict_tailq;
			TAILQ_ENTRY(spdk_fs_request) fetch_tailq;
            spdk_file_op_complete sync_op;
            int64_t nr_evict;
		} blobfs2_rw;
		struct {
			const char * name;
			int mode;
		} blobfs2_access;
	} op;
};

static void cache_free_buffers(struct spdk_file *file);

void
spdk_fs_opts_init(struct spdk_blobfs_opts *opts)
{
	opts->cluster_sz = SPDK_BLOBFS_DEFAULT_OPTS_CLUSTER_SZ;
}
#if 0
static void
__initialize_cache(void)
{
	assert(g_cache_pool == NULL);

	g_cache_pool = spdk_mempool_create("spdk_fs_cache",
					   g_fs_cache_size_in_gb / CACHE_BUFFER_SIZE,
					   CACHE_BUFFER_SIZE,
					   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
					   SPDK_ENV_SOCKET_ID_ANY);
	if (!g_cache_pool) {
		SPDK_ERRLOG("Create mempool failed, you may "
			    "increase the memory and try again\n");
		assert(false);
	}
	TAILQ_INIT(&g_caches);
	pthread_spin_init(&g_caches_lock, 0);
}
#endif

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

struct channel_heap {
    void * page;
    TAILQ_ENTRY(channel_heap) link;
};

struct blobfs2_mmap_area {
    void * page;
    uint64_t nr_pages;
    TAILQ_ENTRY(blobfs2_mmap_area) link;
};

struct spdk_fs_channel {
	struct spdk_fs_request		*req_mem;
    TAILQ_HEAD(, blobfs2_mmap_area) mmap_pages;
    uint64_t nr_allocated_pages;
//	int cur_queue_depth;
	TAILQ_HEAD(, spdk_fs_request)	reqs;
    TAILQ_HEAD(, channel_heap) heaps[4]; // 128B, 1024B, 4KB, 64KB
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

static void blobfs2_expand_reqs(struct spdk_fs_channel * channel);
static void blobfs2_release_heap(struct spdk_fs_channel * channel);

static int
_spdk_fs_channel_create(struct spdk_filesystem *fs, struct spdk_fs_channel *channel,
			uint32_t max_ops)
{
    if (g_page_size == 0) {
        g_page_size = sysconf(_SC_PAGESIZE);
    }
	TAILQ_INIT(&channel->reqs);
    TAILQ_INIT(&channel->heaps[0]);
    TAILQ_INIT(&channel->heaps[1]);
    TAILQ_INIT(&channel->heaps[2]);
    TAILQ_INIT(&channel->heaps[3]);
	sem_init(&channel->sem, 0, 0);

	channel->sync = 0;
	channel->nr_allocated_pages = 0;
    TAILQ_INIT(&channel->mmap_pages);
    blobfs2_expand_reqs(channel);

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

    blobfs2_release_heap(channel);
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
/*	if (g_fs_count == 0) {
		__initialize_cache();
	}*/
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
	int fs_cache_buffer_shift; // we use local variable in case of parse errors

	sp = spdk_conf_find_section(NULL, "Blobfs");
	if (sp == NULL) {
		g_fs_cache_buffer_shift = CACHE_BUFFER_SHIFT_DEFAULT;
		return;
	}

    fs_cache_buffer_shift = spdk_conf_section_get_intval(sp, "CacheBufferShift");
	if (fs_cache_buffer_shift < 12) {
        SPDK_WARNLOG("We cannot set cache size under 4KB. Set CacheBufferShift=%d now.\n", CACHE_BUFFER_SHIFT_DEFAULT);
		fs_cache_buffer_shift = CACHE_BUFFER_SHIFT_DEFAULT;
	}
    g_fs_cache_buffer_shift = (uint32_t) fs_cache_buffer_shift;

	g_dirty_ratio = spdk_conf_section_get_intval(sp, "DirtyRatio");
	if (g_dirty_ratio <= 0 || g_dirty_ratio >= 100) {
		SPDK_WARNLOG("DirtyRatio must be > 1 and < 99\n");
		g_dirty_ratio = 20;
	}

	g_fs_cache_size_in_gb = spdk_conf_section_get_intval(sp, "CacheSizeInGB");
	if (g_fs_cache_size_in_gb <= 0) {
		SPDK_WARNLOG("CacheSizeInGB must be > 0\n");
        g_fs_cache_size_in_gb = BLOBFS_DEFAULT_CACHE_SIZE_IN_GB;
	}

/*	g_queue_depth = spdk_conf_section_get_intval(sp, "QueueDepth");
	if (g_queue_depth <= 0) {
		SPDK_WARNLOG("QueueDepth must be > 0\n");
		g_queue_depth = 128;
	}*/

    g_channel_heap_in_gb = spdk_conf_section_get_intval(sp, "ChannelHeapSizeInGB");
    if (g_channel_heap_in_gb <= 0) {
        SPDK_WARNLOG("ChannelHeapSizeInGB must be > 0\n");
        g_channel_heap_in_gb = BLOBFS_DEFAULT_CACHE_SIZE_IN_GB;
    }
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
	opts.max_channel_ops = 65536;
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
	TAILQ_INIT(&file->evict_waiter);
	pthread_spin_init(&file->lock, 0);
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

void spdk_fs_set_need_dma(struct spdk_filesystem * fs, bool need_dma)
{
    fs->need_dma = need_dma;
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
	g_fs_cache_size_in_gb = size_in_mb / 1024;
}

uint64_t
spdk_fs_get_cache_size(void)
{
	return g_fs_cache_size_in_gb * 1024 * 1024 * 1024;
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
#include "../blob/blobstore.h"

#ifndef __linux__
#define O_DIRECT 1
#endif

static int64_t g_nr_buffers;
static int64_t g_nr_dirties;
static TAILQ_HEAD(, cache_buffer) g_zeroref_caches;
static TAILQ_HEAD(, spdk_fs_request) g_evict_waiter;
static TAILQ_HEAD(, spdk_fs_request) g_fetch_waiter;
static TAILQ_HEAD(, cache_buffer) g_blank_buffer;
static void ** dma_head;

static void blobfs2_expand_reqs(struct spdk_fs_channel * channel)
{
    uint8_t * page;
    uint64_t off;
    struct spdk_fs_request * req;
    struct blobfs2_mmap_area * a;

    // should we strictly check allocated pages with locking?
    if ((int)((channel->nr_allocated_pages + 1) * g_page_size / 1024 / 1024 / 1024) >= g_channel_heap_in_gb) {
        return;
    }

    page = mmap(NULL, g_page_size, PROT_WRITE | PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (page == MAP_FAILED) {
        return;
    }

    a = (struct blobfs2_mmap_area *) page;
    a->page = page;
    a->nr_pages = 1;

    if (channel->sync) {
        pthread_spin_lock(&channel->lock);
    }
    ++channel->nr_allocated_pages;
    TAILQ_INSERT_TAIL(&channel->mmap_pages, a, link);

    for (off = sizeof(*a); off + sizeof(*req) < g_page_size; off += sizeof(*req)) {
        req = (struct spdk_fs_request *)(page + off);
        TAILQ_INSERT_TAIL(&channel->reqs, req, link);
    }
    if (channel->sync) {
        pthread_spin_unlock(&channel->lock);
    }
}

static void blobfs2_expand_heap(struct spdk_fs_channel * channel, int objsize)
{
    uint8_t * pages;
    uint64_t nr_pages;
    struct channel_heap * h;
    struct blobfs2_mmap_area * a;
    int heap_index, i;

    nr_pages = 1 + (((g_page_size - sizeof(struct blobfs2_mmap_area)) / sizeof(struct channel_heap)) * objsize + g_page_size - 1) / g_page_size;

    // should we strictly check allocated pages with locking?
    if ((int)((channel->nr_allocated_pages + nr_pages) * g_page_size / 1024 / 1024 / 1024) >= g_channel_heap_in_gb) {
        return;
    }

    // TODO: heap_index = log16(logsize & 0x1ffff) - 1
    if (objsize == 128) {
        heap_index = 1;
    } else if (objsize == 1024) {
        heap_index = 2;
    } else if (objsize == 4096) {
        heap_index = 3;
    } else {
        heap_index = 4;
    }

    pages = mmap(NULL, g_page_size * nr_pages, PROT_WRITE | PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (pages == MAP_FAILED) {
        return;
    }

    a = (struct blobfs2_mmap_area *) pages;
    a->page = pages;
    a->nr_pages = nr_pages;

    if (channel->sync) {
        pthread_spin_lock(&channel->lock);
    }
    channel->nr_allocated_pages += nr_pages;
    TAILQ_INSERT_TAIL(&channel->mmap_pages, a, link);

    for (i = 0; sizeof(*a) + (i + 1) * sizeof(*h) < g_page_size; i++) {
        h = (struct channel_heap *)(pages + sizeof(*a) + i * sizeof(*h));
        h->page = pages + g_page_size + i * objsize;
        TAILQ_INSERT_TAIL(&channel->heaps[heap_index - 1], h, link);
    }
    if (channel->sync) {
        pthread_spin_unlock(&channel->lock);
    }
}

static void blobfs2_release_heap(struct spdk_fs_channel * channel)
{
    if (channel->sync) {
        pthread_spin_lock(&channel->lock);
    }
    while (!TAILQ_EMPTY(&channel->mmap_pages)) {
        struct blobfs2_mmap_area * a = TAILQ_FIRST(&channel->mmap_pages);
        channel->nr_allocated_pages -= a->nr_pages;
        munmap(a->page, a->nr_pages * g_page_size);
        TAILQ_REMOVE(&channel->mmap_pages, a, link);
    }
    if (channel->sync) {
        pthread_spin_unlock(&channel->lock);
    }
}

int blobfs2_init(void)
{
	int i;

	g_nr_buffers = 0;
	g_nr_dirties = 0;
	TAILQ_INIT(&g_caches);
	TAILQ_INIT(&g_zeroref_caches);
	TAILQ_INIT(&g_evict_waiter);
	TAILQ_INIT(&g_fetch_waiter);

    // DMA malloc is too heavy to execute during event loop. pool them at the beginning
	dma_head = malloc(g_fs_cache_size_in_gb * sizeof(void *));
	if (!dma_head) {
		return -ENOMEM;
	}

	TAILQ_INIT(&g_blank_buffer);
	for (i = 0; i < g_fs_cache_size_in_gb; i++) {
		uint64_t dmasize = 0;
		uint8_t * dma = spdk_dma_malloc(1024 * 1024 * 1024, g_page_size, NULL);
		if (!dma) {
            SPDK_ERRLOG("failed to spdk_dma_malloc(%lu, %lu, NULL) after allocated %d GB memory\n", 1024UL * 1024 * 1024, g_page_size, i);
			return -ENOMEM;
		}
		for (dmasize = 0; dmasize < 1024 * 1024 * 1024; dmasize += CACHE_BUFFER_SIZE) {
			struct cache_buffer * buf = malloc(sizeof(struct cache_buffer));
			if (!buf) {
				SPDK_WARNLOG("malloc failed during Blobfs initialization\n");
				break;
			}
			buf->buf = dma + dmasize;
			TAILQ_INSERT_TAIL(&g_blank_buffer, buf, zeroref_tailq);
		}
		dma_head[i] = dma;
	}
	return 0;
}

void blobfs2_shutdown(void)
{
	int i;
	for (i = 0; i < g_fs_cache_size_in_gb; i++) {
		spdk_dma_free(dma_head[i]);
	}
	free(dma_head);
}

static void __blobfs2_wake_caller(void * _args, int rc)
{
	struct spdk_fs_request * req = _args;

	req->args.rc = rc;
	sem_post(req->args.sem);
}

static long t_alloc_buffer = 0;
static struct cache_buffer * blobfs2_alloc_buffer(struct spdk_file * file)
{
	struct cache_buffer * buf;
	struct timespec t, t2;
	long n;
	clock_gettime(CLOCK_MONOTONIC, &t);

	buf = TAILQ_FIRST(&g_blank_buffer);
	if (!buf) {
		buf = TAILQ_FIRST(&g_zeroref_caches);
		if (buf) {
			TAILQ_REMOVE(&g_zeroref_caches, buf, zeroref_tailq);
		} else {
			return NULL;
		}
	} else {
		TAILQ_REMOVE(&g_blank_buffer, buf, zeroref_tailq);
	}

	buf->buf_size = 0;
	buf->ref = 1;
//	memset(buf->buf, 0, CACHE_BUFFER_SIZE); // > 20usec. too slow
    TAILQ_INIT(&buf->write_waiter);

    ++g_nr_buffers;
	clock_gettime(CLOCK_MONOTONIC, &t2);
	n = (t2.tv_sec - t.tv_sec) * 1000 * 1000 * 1000 + (t2.tv_nsec - t.tv_nsec);
	if (n > t_alloc_buffer) {
		t_alloc_buffer = n;
	}
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

static void blobfs2_buffer_up(struct spdk_file * file, struct cache_buffer * buffer)
{
    if (buffer->ref++ == 0 && !buffer->dirty) {
		TAILQ_REMOVE(&g_zeroref_caches, buffer, zeroref_tailq);
    }
}

static void blobfs2_put_buffer(struct spdk_file * file, struct cache_buffer * buffer)
{
	assert(!buffer->in_progress);
    if (--buffer->ref == 0 && !buffer->dirty) {
		TAILQ_INSERT_TAIL(&g_zeroref_caches, buffer, zeroref_tailq);
    }
}

static struct spdk_fs_request * blobfs2_alloc_fs_request_nonblock(struct spdk_fs_channel * channel)
{
	struct spdk_fs_request * req = calloc(1, sizeof(struct spdk_fs_request));
	if (!req) {
		return NULL;
	}
	req->channel = channel;
	req->args.from_request = false;
	return req;
}

static struct spdk_fs_request * blobfs2_alloc_fs_request(struct spdk_fs_channel * channel, int ubuf_len)
{
    struct spdk_fs_request * req;
    struct channel_heap * h = NULL;
    int heap_index, heap_size;

    // TODO: heap_index = log16(logsize & 0x1ffff) - 1
    if (ubuf_len <= 0) {
        heap_index = 0;
        heap_size = 0;
    } else if (ubuf_len <= 128) {
        heap_index = 1;
        heap_size = 128;
    } else if (ubuf_len <= 1024) {
        heap_index = 2;
        heap_size = 1024;
    } else if (ubuf_len <= 4096) {
        heap_index = 3;
        heap_size = 4096;
    } else {
        heap_index = 4;
        heap_size = 65536;
    }

    if (channel->sync) {
        pthread_spin_lock(&channel->lock);
    }

/*	while (channel->cur_queue_depth >= g_queue_depth) {
		if (channel->sync) {
			pthread_spin_unlock(&channel->lock);
		}
		usleep(1);
		if (channel->sync) {
			pthread_spin_lock(&channel->lock);
		}
	}

	++channel->cur_queue_depth;*/

    req = TAILQ_FIRST(&channel->reqs);
    if (req) {
        TAILQ_REMOVE(&channel->reqs, req, link);
    }

    if (heap_index > 0) {
        h = TAILQ_FIRST(&channel->heaps[heap_index - 1]);
        if (h) {
            TAILQ_REMOVE(&channel->heaps[heap_index - 1], h, link);
        }
    }

    if (channel->sync) {
        pthread_spin_unlock(&channel->lock);
    }

    if (!req) {
        blobfs2_expand_reqs(channel);
        while (!req) {
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
            if (!req) {
                // reached memory limit
                usleep(1);
            }
        }
    }
	req->channel = channel;
    req->args.from_request = true;

    if (heap_index > 0) {
        if (!h) {
            blobfs2_expand_heap(channel, heap_size);
            while (!h) {
                if (channel->sync) {
                    pthread_spin_lock(&channel->lock);
                }
                h = TAILQ_FIRST(&channel->heaps[heap_index - 1]);
                if (h) {
                    TAILQ_REMOVE(&channel->heaps[heap_index - 1], h, link);
                }
                if (channel->sync) {
                    pthread_spin_unlock(&channel->lock);
                }
                if (!h) {
                    // reached memory limit
                    usleep(1);
                }
            }
        }
        req->args.op.blobfs2_rw.user_buf = h;
    }
    req->args.op.blobfs2_rw.ubuf_heap_index = heap_index;
	return req;
}

static void blobfs2_free_fs_request(struct spdk_fs_request * req)
{
    struct spdk_fs_channel *channel = req->channel;
    int heap_index = req->args.op.blobfs2_rw.ubuf_heap_index;
    bool from_req = req->args.from_request;
    struct channel_heap * ubuf = req->args.op.blobfs2_rw.user_buf;

    if (from_req) {
        memset(req, 0, sizeof(*req));
        if (heap_index > 0) {
            int heap_size = 0;
            switch(heap_index) {
                case 1: heap_size = 128; break;
                case 2: heap_size = 1024; break;
                case 3: heap_size = 4096; break;
                default: heap_size = 65536;
            }
            memset(ubuf->page, 0, heap_size);
        }
    }

	if (channel->sync) {
		pthread_spin_lock(&channel->lock);
	}
	// TODO: currently, we pool all the allocated reqs/heaps. this might become memory pressure
	if (heap_index > 0) {
		TAILQ_INSERT_TAIL(&channel->heaps[heap_index - 1], ubuf, link);
	}
	if (from_req) {
//		--channel->cur_queue_depth;
		TAILQ_INSERT_HEAD(&channel->reqs, req, link);
	}
	if (channel->sync) {
		pthread_spin_unlock(&channel->lock);
	}
	if (!from_req) {
		free(req);
	}
}

static void __blobfs2_copy_stat(void *arg, struct spdk_file_stat *stat, int fserrno)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;

	args->rc = fserrno;
	if (fserrno == 0) {
		memcpy(args->arg, stat, sizeof(*stat));
	}
	sem_post(args->sem);
}

static void __blobfs2_stat(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file_stat stat;
	struct spdk_file *f = NULL;

	if (strnlen(args->op.stat.name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		__blobfs2_copy_stat(req, NULL, -ENAMETOOLONG);
		return;
	}

	f = fs_find_file(args->fs, args->op.stat.name);
	if (f != NULL) {
		stat.blobid = f->blobid;
		stat.size = f->length;
		__blobfs2_copy_stat(req, &stat, 0);
	} else {
		__blobfs2_copy_stat(req, &stat, -ENOENT);
	}
}

int blobfs2_stat(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
				  const char *name, struct spdk_file_stat *stat)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	int rc;

	req = blobfs2_alloc_fs_request(channel, 0);
	if (req == NULL) {
		return -ENOMEM;
	}

	req->args.fs = fs;
	req->args.op.stat.name = name;
	req->args.arg = stat;
	req->args.sem = &channel->sem;
	channel->send_request(__blobfs2_stat, req);
	sem_wait(&channel->sem);

	rc = req->args.rc;
	blobfs2_free_fs_request(req);

	return rc;
}

uint64_t blobfs2_fstat_unsafe(struct spdk_file * file)
{
	return file->length;
}

static void __blobfs2_resubmit_op_after_resize(struct spdk_fs_request *req)
{
    if (!TAILQ_EMPTY(&req->args.file->resize_waiter)) {
		struct spdk_fs_request * dreq = TAILQ_FIRST(&req->args.file->resize_waiter);
		TAILQ_REMOVE(&req->args.file->resize_waiter, dreq, args.op.blobfs2_rw.resize_tailq);
		dreq->args.delayed_fn.resize_op(dreq);
    }
}

static bool __blobfs2_blob_is_resizing(struct spdk_file * file)
{
	return file->blob->resize_in_progress;
}

static uint64_t __blobfs2_blob_md_size(struct spdk_blob * blob)
{
	uint64_t * plength;
	size_t len;

	spdk_blob_get_xattr_value(blob, "length", (const void **)&plength, &len);
	if (len != sizeof(uint64_t)) {
		return UINT64_MAX;
	}
	return *plength;
}

static void __blobfs2_resize_done(void *_args, int bserrno) {
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;

	if (bserrno == 0) {
		args->file->length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
	}
	args->rc = 0;
	sem_post(args->sem);
	__blobfs2_resubmit_op_after_resize(req);
}

static void __blobfs2_resize_cb(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file *file = args->file;
	uint64_t nr_clusters = spdk_blob_get_num_clusters(file->blob);
	uint64_t new_length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
	uint64_t new_nr_clusters = __bytes_to_clusters(new_length, file->fs->bs_opts.cluster_sz);

	if (__blobfs2_blob_is_resizing(file)) {
		// resize is in-processing. postpone this request after the resize to avoid -EBUSY
		args->delayed_fn.resize_op = __blobfs2_resize_cb;
		TAILQ_INSERT_TAIL(&file->resize_waiter, req, args.op.blobfs2_rw.resize_tailq);
		return;
	}

	if (new_nr_clusters > nr_clusters) {
		spdk_blob_resize(file->blob, new_nr_clusters, __blobfs2_resize_done, req);
	} else {
		// no need to expand blob. update in-memory metadata only
        __blobfs2_resize_done(_args, 0);
	}
}
// TODO: should barrier in case of racy write
int blobfs2_truncate(struct spdk_file * file, struct spdk_io_channel * _channel, uint64_t length)
{
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request * req;
	int rc;

	req = blobfs2_alloc_fs_request(channel, 0);
	if (!req) {
		return -ENOMEM;
	}

	req->args.file = file;
	req->args.sem = &channel->sem;
	req->args.op.blobfs2_rw.offset = 0;
	req->args.op.blobfs2_rw.length = length;
	channel->send_request(__blobfs2_resize_cb, req);
	sem_wait(&channel->sem);
	rc = req->args.rc;
	blobfs2_free_fs_request(req);

	return rc;
}

static void __blobfs2_rw_last(struct cache_buffer *buffer, struct spdk_fs_request *req, int rc)
{
    struct spdk_file * file = req->args.file;
	struct spdk_fs_cb_args * args = &req->args;
    struct spdk_fs_request * head;

    if (buffer) {
    	buffer->in_progress = false;
        blobfs2_put_buffer(file, buffer);
    }

	// for write barrier/sync
    TAILQ_REMOVE(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
    do {
        head = TAILQ_FIRST(&file->sync_requests);
        if (head && head->args.fn.file_op) {
            head->args.fn.file_op(head, rc);
        } else {
            break;
        }
    } while (true);

    // resubmit delayed reads/writes
    if (buffer && !TAILQ_EMPTY(&buffer->write_waiter)) {
        struct spdk_fs_request * dreq = TAILQ_FIRST(&buffer->write_waiter);
        TAILQ_REMOVE(&buffer->write_waiter, dreq, args.op.blobfs2_rw.write_tailq);
        dreq->args.delayed_fn.write_op(dreq);
    }

	while (!TAILQ_EMPTY(&g_zeroref_caches) && !TAILQ_EMPTY(&g_evict_waiter)) {
		struct spdk_fs_request * dreq = TAILQ_FIRST(&g_evict_waiter);
		TAILQ_REMOVE(&g_evict_waiter, dreq, args.op.blobfs2_rw.evict_tailq);
		dreq->args.delayed_fn.write_op(dreq);
	}

	if (args->cond > 0) {
		args->cond = 0;
		spdk_smp_rmb();
	} else if (args->sem) {
		args->rc = rc;
		sem_post(args->sem);
	} else {
		blobfs2_free_fs_request(req);
	}
}

static void __blobfs2_buffer_flush_done(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
	struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
	struct spdk_file * file = args->file;

	if (bserrno) {
		buffer->dirty = true;
		TAILQ_INSERT_TAIL(&file->dirty_buffers, buffer, dirty_tailq);
		++g_nr_dirties;
	}
	__blobfs2_rw_last(buffer, req, bserrno);
}

static void __blobfs2_flush_buffer_blob(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file *file = args->file;
	struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
	uint64_t offset = args->op.blobfs2_rw.offset;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
	uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;

	blobfs2_buffer_up(file, buffer);

	if (bserrno) {
		__blobfs2_buffer_flush_done(req, bserrno);
        return;
	}

	buffer->in_progress = true;
	buffer->dirty = false;

    __get_page_parameters(file, buffer_offset, buffer->buf_size, &start_lba, &lba_size, &num_lba);
    spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
                       buffer->buf + (start_lba * lba_size) - buffer_offset,
                       start_lba, num_lba, __blobfs2_buffer_flush_done, req);
}

static void __blobfs2_flush_buffer_blob_evict_cache(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    int64_t nr_remaining_evict = args->op.blobfs2_rw.nr_evict;
    TAILQ_HEAD(, cache_buffer) dirty_buffers;
    int rc;

    if (bserrno) {
    	SPDK_WARNLOG("failed to resize to file->length=%lu: %d\n", file->length, bserrno);
		g_nr_dirties += nr_remaining_evict;
        __blobfs2_rw_last(NULL, req, bserrno);
        return;
    }

    if (TAILQ_EMPTY(&file->dirty_buffers)) {
        args->rc = 0;
		g_nr_dirties += nr_remaining_evict;
        __blobfs2_rw_last(NULL, req, 0);
        return;
    }

    rc = 0;
    TAILQ_INIT(&dirty_buffers);
    TAILQ_SWAP(&file->dirty_buffers, &dirty_buffers, cache_buffer, dirty_tailq);
    while (nr_remaining_evict > 0 && !TAILQ_EMPTY(&dirty_buffers)) {
        struct cache_buffer * buffer = TAILQ_FIRST(&dirty_buffers);
        struct spdk_fs_request * subreq;

        if (buffer->in_progress) {
            continue;
        }

        subreq = blobfs2_alloc_fs_request_nonblock(req->channel);

        if (!subreq) {
            rc = -ENOMEM;
            break;
        }

        TAILQ_REMOVE(&dirty_buffers, buffer, dirty_tailq);

        subreq->args.file = file;
        subreq->args.op.blobfs2_rw.buffer = buffer;
        subreq->args.op.blobfs2_rw.offset = buffer->offset;
        subreq->args.op.blobfs2_rw.length = buffer->buf_size;
        subreq->args.fn.file_op = NULL;
        subreq->args.sem = NULL;
        TAILQ_INSERT_TAIL(&file->sync_requests, subreq, args.op.blobfs2_rw.sync_tailq);
        __blobfs2_flush_buffer_blob(subreq, 0);
        nr_remaining_evict--;
    }
    g_nr_dirties += nr_remaining_evict;
    TAILQ_CONCAT(&file->dirty_buffers, &dirty_buffers, dirty_tailq);
    __blobfs2_rw_last(NULL, req, rc);
}

static void __blobfs2_flush_buffer_check_resize(void * _args);

long g_nr_resize = 0;
static void __blobfs2_buffered_blob_resize_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;

	if (bserrno == 0) {
	    struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
		if (buffer) {
		    buffer->in_progress = false;
		}
	}
    args->op.blobfs2_rw.sync_op(req, bserrno);
    if (!TAILQ_EMPTY(&req->args.file->resize_waiter)) {
        struct spdk_fs_request * dreq = TAILQ_FIRST(&req->args.file->resize_waiter);
        TAILQ_REMOVE(&req->args.file->resize_waiter, dreq, args.op.blobfs2_rw.resize_tailq);
//        req->channel->send_request(__blobfs2_flush_buffer_check_resize, dreq);
		__blobfs2_flush_buffer_check_resize(dreq);
    }
}

static void __blobfs2_flush_buffer_check_resize(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file *file = args->file;
	uint64_t nr_clusters = spdk_blob_get_num_clusters(file->blob);
	uint64_t new_length = file->length;
	uint64_t new_nr_clusters = __bytes_to_clusters(new_length, file->fs->bs_opts.cluster_sz);

	if (__blobfs2_blob_is_resizing(file)) {
		// resize is in-processing. postpone this request after the resize to avoid -EBUSY
		args->delayed_fn.resize_op = __blobfs2_flush_buffer_check_resize;
		TAILQ_INSERT_TAIL(&file->resize_waiter, req, args.op.blobfs2_rw.resize_tailq);
		return;
	}

	if (new_nr_clusters > nr_clusters) {
		spdk_blob_resize(file->blob, new_nr_clusters, __blobfs2_buffered_blob_resize_done, req);
		++g_nr_resize;
	} else {
		// no need to expand blob. update in-memory metadata only
        __blobfs2_buffered_blob_resize_done(_args, 0);
	}
}

static long t_memcpy = 0;
static void __blobfs2_rw_copy_buffer(void * _args)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    struct cache_buffer * buffer = args->op.blobfs2_rw.buffer;
    void * payload = args->op.blobfs2_rw.ubuf_heap_index > 0 ? args->op.blobfs2_rw.user_buf->page: args->op.blobfs2_rw.user_buf;
    uint64_t offset = args->op.blobfs2_rw.offset;
    uint64_t length = args->op.blobfs2_rw.length;
    uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
	struct timespec t, t2;
	long n;

	clock_gettime(CLOCK_MONOTONIC, &t);

    if (args->op.blobfs2_rw.is_read) {
		memcpy(payload, buffer->buf + offset - buffer_offset, length);
    } else {
		memcpy(buffer->buf + offset - buffer_offset, payload, length);
		if (offset + length > file->length) {
			file->length = offset + length;
		}
		if (offset + length > buffer->offset + buffer->buf_size) {
		    buffer->buf_size = offset + length - buffer->offset;
		}
		if (args->op.blobfs2_rw.oflag & (O_SYNC | O_DSYNC)) {
            buffer->in_progress = true;
            args->op.blobfs2_rw.sync_op = __blobfs2_flush_buffer_blob;
            __blobfs2_flush_buffer_check_resize(req);
			return;
		}
		if (!buffer->dirty) {
			buffer->dirty = true;
			++g_nr_dirties;
			TAILQ_INSERT_TAIL(&file->dirty_buffers, buffer, dirty_tailq);
		}
    }
	clock_gettime(CLOCK_MONOTONIC, &t2);
	n = (t2.tv_sec - t.tv_sec) * 1000 * 1000 * 1000 + (t2.tv_nsec - t.tv_nsec);
	if (n > t_memcpy) {
		t_memcpy = n;
	}
	__blobfs2_rw_last(buffer, req, 0);
}

static long t_evict_cache = 0;
static int blobfs2_evict_cache(void * _args)
{
	struct spdk_fs_request * req = _args, * subreq;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	long n;
	struct timespec t, t2;
	int64_t nr_evict;

	clock_gettime(CLOCK_MONOTONIC, &t);

	if (g_nr_buffers < 100) {
		return 0;
	}

	nr_evict = (g_nr_dirties - g_nr_buffers * g_dirty_ratio / 100) * 2;
	if (nr_evict <= 0) {
		return 0;
	}

	subreq = blobfs2_alloc_fs_request_nonblock(req->channel);
	if (!subreq) {
	    return -ENOMEM;
	}

    if (nr_evict > g_nr_dirties) {
        nr_evict = g_nr_dirties;
        g_nr_dirties = 0;
    } else {
        g_nr_dirties -= nr_evict;
    }

	subreq->args.file = file;
    subreq->args.sem = NULL;
	subreq->channel = req->channel;
	subreq->args.op.blobfs2_rw.buffer = NULL;
	subreq->args.op.blobfs2_rw.sync_op = __blobfs2_flush_buffer_blob_evict_cache;
	subreq->args.op.blobfs2_rw.nr_evict = nr_evict;
    TAILQ_INSERT_TAIL(&file->sync_requests, subreq, args.op.blobfs2_rw.sync_tailq);
//    req->channel->send_request(__blobfs2_flush_buffer_check_resize, subreq);
    __blobfs2_flush_buffer_check_resize(subreq);

	clock_gettime(CLOCK_MONOTONIC, &t2);
	n = (t2.tv_sec - t.tv_sec) * 1000 * 1000 * 1000 + (t2.tv_nsec - t.tv_nsec);
	if (n > t_evict_cache) {
		t_evict_cache = n;
	}
	return 0;
}

static void __blobfs2_rw_fetch_done(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;

    if (bserrno == -ENOMEM) {
        // reached the hard limit of the number of reads. resubmit this request after another in-progress read.
        TAILQ_INSERT_TAIL(&g_fetch_waiter, req, args.op.blobfs2_rw.fetch_tailq);
        return;
    } else if (bserrno) {
        SPDK_ERRLOG("failed to fetch on-disk data : %d\n", bserrno);
        __blobfs2_rw_last(req->args.op.blobfs2_rw.buffer, req, -EIO);
        return;
    }
    __blobfs2_rw_copy_buffer(_args);

    if (!TAILQ_EMPTY(&g_fetch_waiter)) {
        struct spdk_fs_request * dreq = TAILQ_FIRST(&g_fetch_waiter);
        struct spdk_fs_cb_args * dargs = &dreq->args;
        struct spdk_file * file = dargs->file;
        struct cache_buffer * buffer = dargs->op.blobfs2_rw.buffer;
        uint64_t offset = dargs->op.blobfs2_rw.offset;
        uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
        uint64_t blob_size = __file_get_blob_size(file);
        uint64_t start_lba, num_lba;
        uint32_t lba_size;

        TAILQ_REMOVE(&g_fetch_waiter, dreq, args.op.blobfs2_rw.fetch_tailq);
        if (file->length < blob_size) {
            blob_size = file->length;
        }
        uint64_t len = blob_size - buffer_offset;
        if (len > CACHE_BUFFER_SIZE) {
            len = CACHE_BUFFER_SIZE;
        }
        __get_page_parameters(file, buffer_offset, len, &start_lba, &lba_size, &num_lba);
        spdk_blob_io_read(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
                          buffer->buf + (start_lba * lba_size) - buffer_offset, start_lba, num_lba, __blobfs2_rw_fetch_done, dreq);
    }
}

static void __blobfs2_rw_resubmit(void * _args);

static void __blobfs2_rw_buffered(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	struct cache_buffer * buffer;
	uint64_t offset = args->op.blobfs2_rw.offset;
	uint64_t length = args->op.blobfs2_rw.length;
	uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
	uint64_t blob_size;

	if (blobfs2_evict_cache(req)) {
		__blobfs2_rw_last(NULL, req, -ENOMEM);
		return;
	}

	if (offset + length >= buffer_offset + CACHE_BUFFER_SIZE) {
		length = CACHE_BUFFER_SIZE - (offset - buffer_offset);
		args->op.blobfs2_rw.length = length;
	}

	if (args->op.blobfs2_rw.is_read) {
		if (offset >= file->length) {
			args->op.blobfs2_rw.length = 0;
			__blobfs2_rw_last(NULL, req, 0);
			return;
		} else if (offset + length > file->length) {
			length = file->length - offset;
			args->op.blobfs2_rw.length = length;
		}
	}

	buffer = spdk_tree_find_buffer(file->tree, offset);
	if (buffer) {
		if (buffer->in_progress) {
			// buffer is synchronizing to on-disk data. postpone this request after the write.
			req->args.delayed_fn.write_op = __blobfs2_rw_resubmit;
			TAILQ_INSERT_TAIL(&buffer->write_waiter, req, args.op.blobfs2_rw.write_tailq);
			return;
		}
		blobfs2_buffer_up(file, buffer);
		// cache hit. copy and return immediately.
		args->op.blobfs2_rw.buffer = buffer;
		__blobfs2_rw_copy_buffer(req);
		return;
	}

	// cache miss.
	buffer = blobfs2_alloc_buffer(file);
	if (!buffer) {
		// should evict dirtied buffers. postpone this request after the eviction.
		args->delayed_fn.write_op = __blobfs2_rw_resubmit;
		TAILQ_INSERT_TAIL(&g_evict_waiter, req, args.op.blobfs2_rw.evict_tailq);
		return;
	}
	buffer->in_progress = true;
	blobfs2_insert_buffer(file, buffer, buffer_offset);
	args->op.blobfs2_rw.buffer = buffer;

	blob_size = __file_get_blob_size(file);
	if (file->length < blob_size) {
		blob_size = file->length;
	}

	if (!args->op.blobfs2_rw.is_read && offset % CACHE_BUFFER_SIZE == 0 && length % CACHE_BUFFER_SIZE == 0) {
		__blobfs2_rw_copy_buffer(req);
		return;
	}
	if (buffer_offset < blob_size) {
		// fetch on-disk data
		uint64_t len = blob_size - buffer_offset;
		if (len > CACHE_BUFFER_SIZE) {
			len = CACHE_BUFFER_SIZE;
		}
		__get_page_parameters(file, buffer_offset, len, &start_lba, &lba_size, &num_lba);
		spdk_blob_io_read(args->file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						buffer->buf + (start_lba * lba_size) - buffer_offset, start_lba, num_lba, __blobfs2_rw_fetch_done, req);
	} else {
		// no need to fetch on-disk data before write. copy and return immediately.
		__blobfs2_rw_copy_buffer(req);
	}
}


static void __blobfs2_rw_direct_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	uint64_t length = args->op.blobfs2_rw.length;
	struct spdk_file * file = req->args.file;

	if (bserrno) {
	    SPDK_ERRLOG("failed to write: offset=%lu, length=%lu, bserrno=%d\n", args->op.blobfs2_rw.offset, length, bserrno);
	}
	if (file->fs->need_dma) {
		if (args->op.blobfs2_rw.is_read) {
			memcpy(args->op.blobfs2_rw.user_buf, args->op.blobfs2_rw.buffer->buf, length);
		}

		TAILQ_INSERT_TAIL(&g_blank_buffer, args->op.blobfs2_rw.buffer, zeroref_tailq);
	}
	__blobfs2_rw_last(NULL, (struct spdk_fs_request *) _args, bserrno);
}

static void __blobfs2_write_direct_blob(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	struct cache_buffer * buffer;
	uint64_t offset = args->op.blobfs2_rw.offset;
	uint64_t length = args->op.blobfs2_rw.length;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;
//	uint64_t align = spdk_bs_get_io_unit_size(file->fs->bs);

	if (file->fs->need_dma) {
		// we need to alloc temporal buffer to issue DMA
		buffer = TAILQ_FIRST(&g_blank_buffer);
		if (!buffer) {
			SPDK_ERRLOG("failed to alloc buffer\n");
			__blobfs2_rw_last(NULL, req, -ENOMEM);
			return;
		}
		args->op.blobfs2_rw.buffer = buffer;

		__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);
		memcpy(buffer->buf, args->op.blobfs2_rw.user_buf, length);
		spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						   buffer->buf + (start_lba * lba_size) - offset, start_lba, num_lba, __blobfs2_rw_direct_done, req);
	} else {
		__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);
		spdk_blob_io_write(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						   args->op.blobfs2_rw.user_buf + (start_lba * lba_size) - offset, start_lba, num_lba, __blobfs2_rw_direct_done, req);
	}
}

static void __blobfs2_write_direct(void * _args);

static void __blobfs2_write_direct_blob_resize_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;

	if (bserrno) {
		__blobfs2_rw_last(NULL, _args, bserrno);
	} else {
		args->file->length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
		__blobfs2_write_direct_blob(_args);
	}
    if (!TAILQ_EMPTY(&req->args.file->resize_waiter)) {
        struct spdk_fs_request * dreq = TAILQ_FIRST(&req->args.file->resize_waiter);
        TAILQ_REMOVE(&req->args.file->resize_waiter, dreq, args.op.blobfs2_rw.resize_tailq);
        __blobfs2_write_direct(dreq);
    }
}

static void __blobfs2_write_direct(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file *file = args->file;
	uint64_t nr_clusters = spdk_blob_get_num_clusters(file->blob);
	uint64_t new_length = args->op.blobfs2_rw.offset + args->op.blobfs2_rw.length;
	uint64_t new_nr_clusters = __bytes_to_clusters(new_length, file->fs->bs_opts.cluster_sz);

	if (__blobfs2_blob_is_resizing(file)) {
		// resize is in-processing. postpone this request after the resize to avoid -EBUSY
		args->delayed_fn.resize_op = __blobfs2_write_direct;
		TAILQ_INSERT_TAIL(&file->resize_waiter, req, args.op.blobfs2_rw.resize_tailq);
		return;
	}

	if (new_nr_clusters > nr_clusters) {
		spdk_blob_resize(file->blob, new_nr_clusters, __blobfs2_write_direct_blob_resize_done, req);
	} else {
		// no need to expand blob. update in-memory metadata only
        __blobfs2_write_direct_blob_resize_done(_args, 0);
	}
}

static void __blobfs2_read_direct(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	uint64_t offset = args->op.blobfs2_rw.offset;
	uint64_t length = args->op.blobfs2_rw.length;
	struct cache_buffer * buffer;
	uint64_t start_lba, num_lba;
	uint32_t lba_size;

	if (offset >= file->length) {
		args->op.blobfs2_rw.length = 0;
		__blobfs2_rw_last(NULL, req, 0);
		return;
	} else if (offset + length > file->length) {
		length = file->length - offset;
		args->op.blobfs2_rw.length = length;
	}

	if (file->fs->need_dma) {
		// we need to alloc temporal buffer to issue DMA
		buffer = TAILQ_FIRST(&g_blank_buffer);
		if (!buffer) {
			SPDK_ERRLOG("failed to alloc buffer\n");
			__blobfs2_rw_last(NULL, req, -ENOMEM);
			return;
		}

		__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);
		args->op.blobfs2_rw.buffer = buffer;
		spdk_blob_io_read(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						  buffer->buf + (start_lba * lba_size) - offset, start_lba, num_lba, __blobfs2_rw_direct_done, req);
	} else {
		__get_page_parameters(file, offset, length, &start_lba, &lba_size, &num_lba);
		spdk_blob_io_read(file->blob, file->fs->sync_target.sync_fs_channel->bs_channel,
						  args->op.blobfs2_rw.user_buf + (start_lba * lba_size) - offset, start_lba, num_lba, __blobfs2_rw_direct_done, req);
	}
}

static void __blobfs2_rw_cb(void * _args)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;

    if (!args->op.blobfs2_rw.delayed) {
        // record this request for consistent sync
        TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
    } else {
        args->op.blobfs2_rw.delayed = false;
    }

    if (args->op.blobfs2_rw.oflag & O_DIRECT) {
    	if (args->op.blobfs2_rw.is_read) {
    		__blobfs2_read_direct(_args);
    	} else {
    		__blobfs2_write_direct(_args);
    	}
    } else {
    	__blobfs2_rw_buffered(_args);
    }
}

static void __blobfs2_rw_resubmit(void * _args)
{
	((struct spdk_fs_request *)_args)->args.op.blobfs2_rw.delayed = true;
	__blobfs2_rw_cb(_args);
}

//TODO: support >64KB writes
static int64_t blobfs2_rw(struct spdk_file *file, struct spdk_io_channel * _channel, void * payload, uint64_t offset, uint64_t length, int oflag, bool is_read)
{
	struct spdk_fs_channel * channel;
	struct spdk_fs_request * req;
	uint64_t align = spdk_bs_get_io_unit_size(file->fs->bs);
	uint64_t buffer_offset = offset - offset % CACHE_BUFFER_SIZE;
	struct channel_heap * user_buf = NULL;
	bool waitrc = (is_read || (oflag & (O_SYNC | O_DSYNC | O_DIRECT)));
	int64_t rc;

	if (length == 0) {
		return 0;
	}

	if (offset - buffer_offset + length > CACHE_BUFFER_SIZE) {
	    length = CACHE_BUFFER_SIZE - (offset - buffer_offset);
	}

	if ((oflag & O_DIRECT) && ((uint64_t)payload % align != 0 || offset % align != 0 || length % align != 0)) {
		return -EINVAL;
	}

	channel = spdk_io_channel_get_ctx(_channel);
	if (!channel) {
		return -EINVAL;
	}

	req = blobfs2_alloc_fs_request(channel, waitrc ? 0: length);
	if (!req) {
		return -ENOMEM;
	}

    if (!waitrc) {
        user_buf = req->args.op.blobfs2_rw.user_buf;
        memcpy(user_buf->page, payload, length);
    } else {
        req->args.op.blobfs2_rw.user_buf = payload;
    }

	// setup a non-blocking call to avoid blocking at racy buffer cache update
	req->args.file = file;
	req->args.op.blobfs2_rw.offset = offset;
	req->args.op.blobfs2_rw.length = length;
	req->args.op.blobfs2_rw.oflag = oflag;
	req->args.op.blobfs2_rw.is_read = is_read;
	req->args.op.blobfs2_rw.delayed = false;
	req->args.fn.file_op = NULL;
	req->args.sem = waitrc ? &channel->sem: NULL;
	req->args.cond = 0; // waitrc ? 1: 0;

	channel->send_request(__blobfs2_rw_cb, req);
	if (waitrc) {
		/*while (req->args.cond > 0) {
			usleep(1);
		}*/
		sem_wait(&channel->sem);
		rc = req->args.op.blobfs2_rw.length;
		blobfs2_free_fs_request(req);
	} else {
		rc = length;
	}

	return rc;
}

int64_t blobfs2_write(struct spdk_file *file, struct spdk_io_channel * _channel, void * payload, uint64_t offset, uint64_t length, int oflag)
{
	return blobfs2_rw(file, _channel, payload, offset, length, oflag, false);
}

int64_t blobfs2_read(struct spdk_file *file, struct spdk_io_channel * _channel, void *payload, uint64_t offset, uint64_t length, int oflag)
{
	return blobfs2_rw(file, _channel, payload, offset, length, oflag, true);
}

static void __blobfs2_sync_md(struct spdk_fs_request * req, struct spdk_file * file)
{
    uint64_t old_length;
    if (!file->blob) {
    	sem_post(req->args.sem);
    	return;
    }
    old_length = __blobfs2_blob_md_size(file->blob);
    if (old_length != file->length) {
        spdk_blob_set_xattr(file->blob, "length", &file->length, sizeof(file->length));
        spdk_blob_sync_md(file->blob, __blobfs2_wake_caller, req);
    } else {
        sem_post(req->args.sem);
    }
}

static void __blobfs2_sync_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_file * file = req->args.file;
	req->args.rc = bserrno;
    req->args.fn.file_op = NULL;
	TAILQ_REMOVE(&req->args.file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
	__blobfs2_sync_md(req, file);
}

static void __blobfs2_sync_cb(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	TAILQ_HEAD(, cache_buffer) dirty_buffers;

	if (bserrno) {
	    args->rc = bserrno;
	    __blobfs2_sync_md(req, file);
	    return;
	}

    if (TAILQ_EMPTY(&file->dirty_buffers)) {
        args->rc = 0;
        __blobfs2_sync_md(req, file);
        return;
    }

    TAILQ_INIT(&dirty_buffers);
    TAILQ_SWAP(&file->dirty_buffers, &dirty_buffers, cache_buffer, dirty_tailq);
    while (!TAILQ_EMPTY(&dirty_buffers)) {
        struct cache_buffer * buffer = TAILQ_FIRST(&dirty_buffers);
		struct spdk_fs_request * subreq = blobfs2_alloc_fs_request_nonblock(req->channel);

		if (!subreq) {
			args->rc = -ENOMEM;
            __blobfs2_sync_md(req, file);
			return;
		}

        TAILQ_REMOVE(&dirty_buffers, buffer, dirty_tailq);
		--g_nr_dirties;

		subreq->args.file = file;
        subreq->args.sem = NULL;
        subreq->args.op.blobfs2_rw.buffer = buffer;
		subreq->args.op.blobfs2_rw.offset = buffer->offset;
		subreq->args.op.blobfs2_rw.length = buffer->buf_size;
		subreq->args.fn.file_op = NULL;
		TAILQ_INSERT_TAIL(&file->sync_requests, subreq, args.op.blobfs2_rw.sync_tailq);
        if (TAILQ_EMPTY(&dirty_buffers)) {
            // append a notifier request
            args->fn.file_op = __blobfs2_sync_done;
            TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
        }
		__blobfs2_flush_buffer_blob(subreq, 0);
	}
}

static void __blobfs2_sync_check_resize(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;

    if (TAILQ_EMPTY(&file->dirty_buffers)) {
        args->rc = 0;
        __blobfs2_sync_md(req, file);
        return;
    }

    req->args.op.blobfs2_rw.offset = 0;
    req->args.op.blobfs2_rw.length = file->length;
    args->op.blobfs2_rw.sync_op = __blobfs2_sync_cb;
    __blobfs2_flush_buffer_check_resize(req);
}

static void __blobfs2_delayed_barrier_cb(void * _args, int bserrno)
{
    struct spdk_fs_request * req = _args;
    struct spdk_fs_cb_args * args = &req->args;
    struct spdk_file * file = args->file;
    TAILQ_REMOVE(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
    args->fn.file_op = args->delayed_fn.file_op;
    args->delayed_fn.file_op = NULL;
    args->fn.file_op(_args, 0);
}

static void __blobfs2_barrier_cb(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;

	if (!TAILQ_EMPTY(&file->sync_requests)) {
	    req->args.delayed_fn.file_op = req->args.fn.file_op;
	    req->args.fn.file_op = __blobfs2_delayed_barrier_cb;
		TAILQ_INSERT_TAIL(&file->sync_requests, req, args.op.blobfs2_rw.sync_tailq);
	} else {
		args->fn.file_op(_args, 0);
	}
}

int blobfs2_sync(struct spdk_file * file, struct spdk_io_channel * _channel)
{
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
    struct spdk_fs_request * req;
    int rc;

    req = blobfs2_alloc_fs_request(channel, 0);
    if (!req) {
    	return -ENOMEM;
    }

    req->args.file = file;
    req->args.sem = &channel->sem;
    req->args.fn.file_op = __blobfs2_sync_check_resize;
    channel->send_request(__blobfs2_barrier_cb, req);
	sem_wait(&channel->sem);
	rc = req->args.rc;
	blobfs2_free_fs_request(req);

	return rc;
}

int blobfs2_barrier(struct spdk_file * file, struct spdk_io_channel * _channel) {
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request * req;
	int rc;

	req = blobfs2_alloc_fs_request(channel, 0);
	if (!req) {
		return -ENOMEM;
	}

	req->args.file = file;
	req->args.sem = &channel->sem;
	req->args.fn.file_op = __blobfs2_wake_caller;
	channel->send_request(__blobfs2_barrier_cb, req);
	sem_wait(&channel->sem);
	rc = req->args.rc;
	blobfs2_free_fs_request(req);

	return rc;
}

static void blobfs2_create_blob_close_cb(void *ctx, int bserrno)
{
    int rc;
    struct spdk_fs_request *req = ctx;
    struct spdk_fs_cb_args *args = &req->args;

    rc = args->rc ? args->rc : bserrno;
    args->fn.file_op(args->arg, rc);
    blobfs2_free_fs_request(req);
}

static void blobfs2_create_blob_resize_cb(void *ctx, int bserrno)
{
    struct spdk_fs_request *req = ctx;
    struct spdk_fs_cb_args *args = &req->args;
    struct spdk_file *f = args->file;
    struct spdk_blob *blob = args->op.create.blob;
    uint64_t length = 0;

    args->rc = bserrno;
    if (bserrno) {
        spdk_blob_close(blob, blobfs2_create_blob_close_cb, args);
        return;
    }

    spdk_blob_set_xattr(blob, "name", f->name, strlen(f->name) + 1);
    spdk_blob_set_xattr(blob, "length", &length, sizeof(length));

    spdk_blob_close(blob, blobfs2_create_blob_close_cb, args);
}

static void blobfs2_create_blob_open_cb(void *ctx, struct spdk_blob *blob, int bserrno)
{
    struct spdk_fs_request *req = ctx;
    struct spdk_fs_cb_args *args = &req->args;

    if (bserrno) {
        args->fn.file_op(args->arg, bserrno);
        blobfs2_free_fs_request(req);
        return;
    }

    args->op.create.blob = blob;
    spdk_blob_resize(blob, 1, blobfs2_create_blob_resize_cb, req);
}

static void blobfs2_create_blob_create_cb(void *ctx, spdk_blob_id blobid, int bserrno)
{
    struct spdk_fs_request *req = ctx;
    struct spdk_fs_cb_args *args = &req->args;
    struct spdk_file *f = args->file;

    if (bserrno) {
        args->fn.file_op(args->arg, bserrno);
        blobfs2_free_fs_request(req);
        return;
    }

    f->blobid = blobid;
    spdk_bs_open_blob(f->fs->bs, blobid, blobfs2_create_blob_open_cb, req);
}

static void blobfs2_create_file_async(struct spdk_fs_channel * channel, struct spdk_filesystem *fs, const char *name,
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

    req = blobfs2_alloc_fs_request_nonblock(channel);
    if (req == NULL) {
        cb_fn(cb_arg, -ENOMEM);
        return;
    }

    args = &req->args;
    args->file = file;
    args->fn.file_op = cb_fn;
    args->arg = cb_arg;

    file->name = strdup(name);
    spdk_bs_create_blob(fs->bs, blobfs2_create_blob_create_cb, args);
}

static void
__blobfs2_create_file_done(void *arg, int fserrno)
{
    struct spdk_fs_request *req = arg;
    struct spdk_fs_cb_args *args = &req->args;

    args->rc = fserrno;
    sem_post(args->sem);
    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.create.name);
}

static void __blobfs2_create_file_cb(void *arg)
{
    struct spdk_fs_request *req = arg;
    struct spdk_fs_cb_args *args = &req->args;

    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.create.name);
    blobfs2_create_file_async(req->channel, args->fs, args->op.create.name, __blobfs2_create_file_done, req);
}

int blobfs2_create_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel, const char *name)
{
    struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
    struct spdk_fs_request *req;
    struct spdk_fs_cb_args *args;
    int rc;

    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", name);

    req = blobfs2_alloc_fs_request(channel, 0);
    if (req == NULL) {
        return -ENOMEM;
    }

    args = &req->args;
    args->fs = fs;
    args->op.create.name = name;
    args->sem = &channel->sem;
    fs->send_request(__blobfs2_create_file_cb, req);
    sem_wait(&channel->sem);
    rc = args->rc;
    blobfs2_free_fs_request(req);

    return rc;
}


static void blobfs2_open_blob_done(void *ctx, struct spdk_blob *blob, int bserrno)
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
        blobfs2_free_fs_request(req);
    }
}

static void blobfs2_open_blob_create_cb(void *ctx, int bserrno)
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
        spdk_bs_open_blob(fs->bs, file->blobid, blobfs2_open_blob_done, req);
    } else if (file->blob != NULL) {
        blobfs2_open_blob_done(req, file->blob, 0);
    } else {
        /*
         * The blob open for this file is in progress due to a previous
         *  open request.  When that open completes, it will invoke the
         *  open callback for this request.
         */
    }
}

static void blobfs2_open_async(struct spdk_fs_channel * channel, struct spdk_filesystem *fs, const char *name, uint32_t flags,
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
    if (f == NULL && !(flags & O_CREAT)) {
        cb_fn(cb_arg, NULL, -ENOENT);
        return;
    }

    if (f != NULL && f->is_deleted == true && (flags & O_CREAT)) {
        cb_fn(cb_arg, NULL, -ENOENT);
        return;
    }

    req = blobfs2_alloc_fs_request_nonblock(channel);
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

    if (f == NULL && (flags & O_CREAT)) {
        blobfs2_create_file_async(channel, fs, name, blobfs2_open_blob_create_cb, req);
    } else {
        blobfs2_open_blob_create_cb(req, 0);
    }
}

static void
__blobfs2_open_done(void *arg, struct spdk_file *file, int bserrno)
{
    struct spdk_fs_request *req = arg;
    struct spdk_fs_cb_args *args = &req->args;

    args->file = file;
    __wake_caller(args, bserrno);
    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.open.name);
}

static void __blobfs2_open_cb(void *arg)
{
    struct spdk_fs_request *req = arg;
    struct spdk_fs_cb_args *args = &req->args;

    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", args->op.open.name);
    blobfs2_open_async(req->channel, args->fs, args->op.open.name, args->op.open.flags,
                            __blobfs2_open_done, req);
}

int blobfs2_open(struct spdk_filesystem *fs, struct spdk_io_channel *_channel,
                  const char *name, uint32_t flags, struct spdk_file **file)
{
    struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
    struct spdk_fs_request *req;
    struct spdk_fs_cb_args *args;
    int rc;

    SPDK_DEBUGLOG(SPDK_LOG_BLOBFS, "file=%s\n", name);

    req = blobfs2_alloc_fs_request(channel, 0);
    if (req == NULL) {
        return -ENOMEM;
    }

    args = &req->args;
    args->fs = fs;
    args->op.open.name = name;
    args->op.open.flags = flags;
    args->sem = &channel->sem;
    fs->send_request(__blobfs2_open_cb, req);
    sem_wait(&channel->sem);
    rc = args->rc;
    if (rc == 0) {
        *file = args->file;
    } else {
        *file = NULL;
    }
    blobfs2_free_fs_request(req);

    return rc;
}

static void blobfs2_blob_delete_cb(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;

	args->fn.file_op(args->arg, bserrno);
	blobfs2_free_fs_request(req);
}

static void blobfs2_delete_file_async(struct spdk_fs_channel * channel, struct spdk_filesystem *fs, const char *name,
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

	req = blobfs2_alloc_fs_request_nonblock(channel);
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
		spdk_blob_sync_md(f->blob, blobfs2_blob_delete_cb, args);
		return;
	}

	TAILQ_REMOVE(&fs->files, f, tailq);

	cache_free_buffers(f);

	blobid = f->blobid;

	free(f->name);
	free(f->tree);
	free(f);

	spdk_bs_delete_blob(fs->bs, blobid, blobfs2_blob_delete_cb, req);
}

/*
 * Close routines
 */

static void __blobfs2_close_async_done(void *ctx, int bserrno)
{
	struct spdk_fs_request *req = ctx;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;

	if (file->is_deleted) {
		blobfs2_delete_file_async(req->channel, file->fs, file->name, blob_delete_cb, ctx);
		return;
	}

	args->fn.file_op(args->arg, bserrno);
	blobfs2_free_fs_request(req);
}

static void __blobfs2_close_async(struct spdk_file *file, struct spdk_fs_request *req)
{
	struct spdk_blob *blob;

	pthread_spin_lock(&file->lock);
	if (file->ref_count == 0) {
		pthread_spin_unlock(&file->lock);
        __blobfs2_close_async_done(req, -EBADF);
		return;
	}

	file->ref_count--;
	if (file->ref_count > 0) {
		pthread_spin_unlock(&file->lock);
		req->args.fn.file_op(req->args.arg, 0);
		blobfs2_free_fs_request(req);
		return;
	}

	pthread_spin_unlock(&file->lock);

	blob = file->blob;
	file->blob = NULL;
	spdk_blob_close(blob, __blobfs2_close_async_done, req);
}

static void __blobfs2_close_cb(void *arg)
{
	struct spdk_fs_request *req = arg;
	struct spdk_fs_cb_args *args = &req->args;
	struct spdk_file *file = args->file;

	__blobfs2_close_async(file, req);
}

int blobfs2_close(struct spdk_file * file, struct spdk_io_channel * _channel)
{
	struct spdk_fs_channel * channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request * req;
	struct spdk_fs_cb_args * args;

	req = blobfs2_alloc_fs_request(channel, 0);
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
	channel->send_request(__blobfs2_close_cb, req);
	sem_wait(&channel->sem);
	//__file_close releases the req

	return args->rc;
}

static void __blobfs2_drop_cache(struct spdk_file * file)
{
	uint64_t off;
	if (file->tree->present_mask == 0) {
		return;
	}
	for (off = 0; off < file->length; off += CACHE_BUFFER_SIZE) {
		struct cache_buffer * buffer = spdk_tree_find_buffer(file->tree, off);
		if (buffer) {
			TAILQ_INSERT_TAIL(&g_blank_buffer, buffer, zeroref_tailq);
		}
	}
	spdk_tree_free_buffers(file->tree);
	TAILQ_REMOVE(&g_caches, file, cache_tailq);
	if (file->tree->present_mask != 0) {
		TAILQ_INSERT_TAIL(&g_caches, file, cache_tailq);
	}
	file->last = NULL;
}

static void __blobfs2_delete_file_done(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;
	args->rc = bserrno;

	if (bserrno == 0) {
		__blobfs2_drop_cache(file);
		free(file->name);
		free(file->tree);
		free(file);
		TAILQ_REMOVE(&file->fs->files, file, tailq);
	}
	sem_post(args->sem);
}

static void __blobfs2_delete_file_barrier_cb(void * _args, int bserrno)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	struct spdk_file * file = args->file;

	if (file->ref_count > 0) {
		/* If the ref > 0, we mark the file as deleted and delete it when we close it. */
		file->is_deleted = true;
		spdk_blob_set_xattr(file->blob, "is_deleted", &file->is_deleted, sizeof(bool));
		spdk_blob_sync_md(file->blob, __blobfs2_delete_file_done, args);
	} else {
		args->file = file;
		spdk_bs_delete_blob(file->fs->bs, file->blobid, __blobfs2_delete_file_done, req);
	}
}

static void __blobfs2_delete_file_cb(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	const char * name = args->op.delete.name;
	struct spdk_file * file;

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		__blobfs2_delete_file_done(req, -ENAMETOOLONG);
		return;
	}

	file = fs_find_file(args->fs, name);
	if (file == NULL) {
		__blobfs2_delete_file_done(req, -ENOENT);
		return;
	}

	args->file = file;
	args->fn.file_op = __blobfs2_delete_file_barrier_cb;
	req->channel->send_request(__blobfs2_barrier_cb, req);
}

int blobfs2_delete_file(struct spdk_filesystem *fs, struct spdk_io_channel *_channel, const char *name)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	req = blobfs2_alloc_fs_request(channel, 0);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;
	args->fs = fs;
	args->op.delete.name = name;
	args->sem = &channel->sem;
	fs->send_request(__blobfs2_delete_file_cb, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	blobfs2_free_fs_request(req);

	return rc;
}

static void __blobfs2_access_cb(void * _args)
{
	struct spdk_fs_request * req = _args;
	struct spdk_fs_cb_args * args = &req->args;
	const char * name = args->op.blobfs2_access.name;
	int mode = args->op.blobfs2_access.mode;
	struct spdk_file * file;

	if (strnlen(name, SPDK_FILE_NAME_MAX + 1) == SPDK_FILE_NAME_MAX + 1) {
		__blobfs2_delete_file_done(req, -ENAMETOOLONG);
		return;
	}

	file = fs_find_file(args->fs, name);
	if (mode & F_OK && file && !file->is_deleted) {
		args->rc = 0;
	} else {
		args->rc = -1;
	}
	sem_post(args->sem);
}

int blobfs2_access(struct spdk_filesystem * fs, struct spdk_io_channel * _channel, const char * path, int mode)
{
	struct spdk_fs_channel *channel = spdk_io_channel_get_ctx(_channel);
	struct spdk_fs_request *req;
	struct spdk_fs_cb_args *args;
	int rc;

	req = blobfs2_alloc_fs_request(channel, 0);
	if (req == NULL) {
		return -ENOMEM;
	}

	args = &req->args;
	args->fs = fs;
	args->op.blobfs2_access.name = path;
	args->op.blobfs2_access.mode = mode;
	args->sem = &channel->sem;
	fs->send_request(__blobfs2_access_cb, req);
	sem_wait(&channel->sem);
	rc = args->rc;
	blobfs2_free_fs_request(req);

	return rc;
}

void blobfs2_dump_request(void)
{
//	SPDK_ERRLOG("evict = %ld, alloc = %ld, memcpy = %ld\n", t_evict_cache, t_alloc_buffer, t_memcpy);
}

SPDK_LOG_REGISTER_COMPONENT("blobfs", SPDK_LOG_BLOBFS)
SPDK_LOG_REGISTER_COMPONENT("blobfs_rw", SPDK_LOG_BLOBFS_RW)
