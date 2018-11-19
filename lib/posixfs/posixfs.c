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
#include "spdk/bdev.h"
#include "spdk/event.h"
#include "spdk/thread.h"
#include "spdk/blob_bdev.h"
#include "spdk/log.h"
#include "spdk/barrier.h"

#define HOOKFS_SPDK_CONF_ENV "HOOKFS_SPDK_CONF"
#define HOOKFS_SPDK_BDEV_ENV "HOOKFS_BDEV"
#define HOOKFS_MOUNT_POINT_ENV "HOOKFS_MOUNT_POINT"

/* Defines are needed for dlfcn.h (RTLD_NEXT) */
#define __USE_GNU
#define _GNU_SOURCE

#include <errno.h>

extern char *program_invocation_short_name;

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>

static struct spdk_io_channel * g_channel;
static struct spdk_filesystem * g_fs;

static char * g_bdev_name;
static char * g_mountpoint;
static struct spdk_bs_dev * g_bs_dev;
static struct spdk_file * conf_file;
static struct spdk_file ** files;
static uint64_t * offsets;
static long max_open;

static void
start_hookfs_fn(void *arg1, void *arg2)
{
#if 0
    struct fuse_args args = FUSE_ARGS_INIT(g_fuse_argc, g_fuse_argv);
    int rc;
    struct fuse_cmdline_opts opts = {};

    g_hookfs_thread = pthread_self();
    rc = fuse_parse_cmdline(&args, &opts);
    if (rc != 0) {
        spdk_app_stop(-1);
        fuse_opt_free_args(&args);
        return;
    }
    g_fuse = fuse_new(&args, &spdk_fuse_oper, sizeof(spdk_fuse_oper), NULL);
    fuse_opt_free_args(&args);

    rc = fuse_mount(g_fuse, g_mountpoint);
    if (rc != 0) {
        spdk_app_stop(-1);
        return;
    }

    fuse_daemonize(true /* true = run in foreground */);

    fuse_loop(g_fuse);

    fuse_unmount(g_fuse);
    fuse_destroy(g_fuse);
#endif
}

static void
init_cb(void *ctx, struct spdk_filesystem *fs, int fserrno)
{
    struct spdk_event *event;

    g_fs = fs;
    g_channel = spdk_fs_alloc_io_channel_sync(g_fs);
    event = spdk_event_allocate(1, start_hookfs_fn, NULL, NULL);
    spdk_event_call(event);
}

static void
__call_fn(void *arg1, void *arg2)
{
    fs_request_fn fn;

    fn = (fs_request_fn)arg1;
    fn(arg2);
}

static void
__send_request(fs_request_fn fn, void *arg)
{
    struct spdk_event *event;

    event = spdk_event_allocate(0, __call_fn, (void *)fn, arg);
    spdk_event_call(event);
}

static void
hookfs_run(void *arg1, void *arg2)
{
    struct spdk_bdev *bdev;

    bdev = spdk_bdev_get_by_name(g_bdev_name);
    if (bdev == NULL) {
        SPDK_ERRLOG("bdev %s not found\n", g_bdev_name);
        exit(1);
    }

    g_bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);

    printf("Mounting BlobFS on bdev %s\n", spdk_bdev_get_name(bdev));
    spdk_fs_load(g_bs_dev, __send_request, init_cb, NULL);
}

static void
shutdown_cb(void *ctx, int fserrno)
{
    spdk_fs_free_io_channel(g_channel);
    spdk_app_stop(0);
}

static void
hookfs_shutdown(void)
{
    spdk_fs_unload(g_fs, shutdown_cb, NULL);
}

static struct real_fsiface {
    int initialized;
    int (*open)(char const *, int, ...);
    ssize_t (*read)(int, void *, size_t);
    ssize_t (*write)(int, const void *, size_t);
    ssize_t (*pread)(int, void *, size_t, off_t);
    ssize_t (*pwrite)(int, const void *, size_t, off_t);
    off_t (*lseek)(int, off_t, int);
    int (*close)(int);
    int (*stat)(const char *, struct stat *);
    int (*fstat)(int, struct stat *);
    int (*posix_fadvise)(int, off_t, off_t, int);
    DIR * (*opendir)(const char *);
    int (*closedir)(DIR *);
    struct dirent * (*readdir)(DIR *);
} realfs = {.initialized = 0};

static void * load_symbol(const char * symbol) {
    char * msg;
    void * ret;
    ret = dlsym(RTLD_NEXT, symbol);
    if ( (msg = dlerror()) ) {
        fprintf(stderr, "dlsym error: %s\n", msg);
        exit(1);
    }
    return ret;
}

static void init_fsiface(void) {
    if (!realfs.initialized) {
        struct spdk_app_opts opts = {};
        int rc = 0;

        realfs.open = load_symbol("open64");
        realfs.read = load_symbol("read");
        realfs.write = load_symbol("write");
        realfs.pread = load_symbol("pread");
        realfs.pwrite = load_symbol("pwrite");
        realfs.lseek = load_symbol("lseek");
        realfs.close = load_symbol("close");
        realfs.stat = load_symbol("stat");
        realfs.fstat = load_symbol("fstat");
        realfs.posix_fadvise = load_symbol("posix_fadvise");
        realfs.opendir = load_symbol("opendir");
        realfs.closedir = load_symbol("closedir");
        realfs.readdir = load_symbol("readdir");

        spdk_app_opts_init(&opts);
        opts.name = program_invocation_short_name;
        opts.config_file = getenv(HOOKFS_SPDK_CONF_ENV);
        opts.reactor_mask = "0x3";
        opts.mem_size = 6144;
        opts.shutdown_cb = hookfs_shutdown;

        g_bdev_name = getenv(HOOKFS_SPDK_BDEV_ENV);
        g_mountpoint = getenv(HOOKFS_MOUNT_POINT_ENV);

        if (!opts.config_file || !g_bdev_name || !g_mountpoint) {
            fprintf(stderr, "set environment variables: %s, %s, %s\n", HOOKFS_SPDK_CONF_ENV, HOOKFS_SPDK_BDEV_ENV, HOOKFS_MOUNT_POINT_ENV);
            exit(1);
        }

        if (!access(g_mountpoint, 0)) {
            fprintf(stderr, "%s already exists.\n", g_mountpoint);
            exit(1);
        }

        max_open = sysconf(_SC_OPEN_MAX);
        files = malloc(sizeof(struct spdk_file *) * max_open);
        if (!files) {
            fprintf(stderr, "malloc(%ld) failed\n", sizeof(struct spdk_file *) * max_open);
            exit(1);
        }
        spdk_mem_all_zero(files, sizeof(struct spdk_file *) * max_open);

        offsets = malloc(sizeof(uint64_t) * max_open);
        if (!offsets) {
            fprintf(stderr, "malloc(%ld) failed\n", sizeof(uint64_t) * max_open);
            exit(1);
        }
        spdk_mem_all_zero(offsets, sizeof(uint64_t) * max_open);

        rc = spdk_app_start(&opts, hookfs_run, NULL, NULL);
        if (!rc) {
            spdk_app_fini();
        }
    }
}

static bool hookfs_is_under_mountpoint(const char * abspath) {
    return strncmp(abspath, g_mountpoint, strlen(g_mountpoint)) == 0;
}

static int hookfs_open(const char * abspath, int oflag) {
    struct spdk_file * file;
    int rc, fd;

    fd = realfs.open("/dev/null", oflag);
    if (fd < 0) {
        fprintf(stderr, "obtaining a FD failed: %d (%s)\n", errno, spdk_strerror(errno));
        return fd;
    }

    rc = spdk_fs_open_file(g_fs, g_channel, abspath, 0, &file);
    if (rc != 0) {
        errno = ENOENT;
        return -rc;
    }
    files[fd] = file;
    offsets[fd] = 0;

    return fd;
}

int open(char const * path, int oflag, ...) {
    va_list args;
    int mflag;
    char abspath[PATH_MAX];

    init_fsiface();
    if (!realpath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_open(abspath, oflag);
    }

    fprintf(stderr, ">>>>> in open <<<<<<\n");
    va_start(args, oflag);
    mflag = va_arg(args, int);
    return realfs.open(path, oflag, mflag);
}

static int hookfs_release(int fd) {
    int rc = spdk_file_close(files[fd], g_channel);
    if (rc == 0) {
        files[fd] = NULL;
    }
    realfs.close(fd);
    return rc;
}

int close(int fd) {
    init_fsiface();
    if (files[fd]) {
        return hookfs_release(fd);
    }
    fprintf(stderr, ">>>>> in close <<<<<<\n");
    return realfs.close(fd);
}

static int hookfs_read(int fd, char * buf, size_t len, uint64_t offset)
{
    return (int) spdk_file_read(files[fd], g_channel, buf, offset, len);
}

ssize_t read(int fd, void * buf, size_t count) {
    init_fsiface();
    if (files[fd]) {
        int len = hookfs_read(fd, buf, count, offsets[fd]);
        if (len > 0) {
            offsets[fd] += len;
        }
        return len;
    }
    fprintf(stderr, ">>>>> in read <<<<<\n");
    return realfs.read(fd, buf, count);
}

ssize_t pread(int fd, void * buf, size_t count, off_t offset) {
    init_fsiface();
    if (files[fd]) {
        return hookfs_read(fd, buf, count, offset);
    }
    return realfs.pread(fd, buf, count, offset);
}

static int hookfs_write(int fd, const char * buf, size_t len, uint64_t offset) {
    int rc;

    rc = spdk_file_write(files[fd], g_channel, (void *)buf, offset, len);
    if (rc == 0) {
        return (int)len;
    } else {
        return rc;
    }
}

ssize_t write(int fd, const void * buf, size_t count) {
    init_fsiface();
    if (files[fd]) {
        int len = hookfs_write(fd, buf, count, offsets[fd]);
        if (len > 0) {
            offsets[fd] += len;
        }
        return len;
    }
    fprintf(stderr, ">>>>> in write <<<<<\n");
    return realfs.write(fd, buf, count);
}

ssize_t pwrite(int fd, const void * buf, size_t count, off_t offset) {
    init_fsiface();
    if (files[fd]) {
        return hookfs_write(fd, buf, count, offset);
    }
    return realfs.pwrite(fd, buf, count, offset);
}

static int hookfs_stat(const char * abspath, struct stat * stbuf)
{
    struct spdk_file_stat stat;
    int rc;

    rc = spdk_fs_file_stat(g_fs, g_channel, abspath, &stat);
    if (rc == 0) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = stat.size;
    }

    return rc;
}

int stat(const char * path, struct stat * stbuf) {
    char abspath[PATH_MAX];

    init_fsiface();
    if (!realpath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_stat(abspath, stbuf);
    }

    return realfs.stat(path, stbuf);
}

int fstat(int fd, struct stat * stbuf) {
    init_fsiface();
    if (files[fd]) {
        return stat(spdk_file_get_name(files[fd]), stbuf);
    }
    return realfs.fstat(fd, stbuf);
}

off_t lseek(int fd, off_t offset, int whence) {
    struct stat stbuf;
    init_fsiface();
    if (files[fd]) {
        off_t newoffset = offsets[fd];
        if (fstat(fd, &stbuf) != 0) {
            errno = EBADF;
            return -1;
        }
        switch (whence) {
            case SEEK_SET:
                newoffset = (uint64_t) offset;
                break;
            case SEEK_CUR:
                newoffset += offset;
                break;
            case SEEK_END:
                newoffset = (uint64_t)(stbuf.st_size + offset);
                break;
            default:
                errno = EINVAL;
                return -1;
        }
        if (newoffset < 0 || newoffset > stbuf.st_size) {
            errno = EINVAL;
            return -1;
        }
        offsets[fd] = (uint64_t) newoffset;
        return newoffset;
    }
    return realfs.lseek(fd, offset, whence);
}

int posix_fadvise(int fd, off_t offset, off_t len, int advice) {
    init_fsiface();
    if (files[fd]) {
        return 0;
    }
    return realfs.posix_fadvise(fd, offset, len, advice);
}

static DIR * hookfs_opendir(const char * name) {
    struct stat stbuf;
    DIR * ret;

    ret = malloc(sizeof(DIR));
    if (!ret) {
        errno = ENOMEM;
        return NULL;
    }

    if (hookfs_stat(name, &stbuf) != 0) {
        errno = ENOENT;
        goto freedir;
    }
    ret->__dd_size = stbuf.st_size;
    ret->__dd_flags = stbuf.st_mode;

    ret->__dd_len = PAGE_SIZE;
    ret->__dd_buf = malloc(ret->__dd_len);
    if (!ret->__dd_buf) {
        errno = ENOMEM;
        goto freedir;
    }

    ret->__dd_fd = open(name, O_RDWR);
    if (ret->__dd_fd < 0) {
        goto freebuf;
    }

    ret->__dd_seek = 0;

    return ret;

freebuf:
    free(ret->__dd_buf);
freedir:
    free(ret);
    return NULL;
}

DIR * opendir(const char * name) {
    char abspath[PATH_MAX];

    init_fsiface();
    if (!realpath(name, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_opendir(abspath);
    }

    return realfs.opendir(name);
}

static struct dirent * hookfs_readdir(DIR * dirp) {
    struct dirent * ret = malloc(sizeof(struct dirent));
    if (!ret) {
        errno = ENOMEM;
        return NULL;
    }

    if (dirp->__dd_seek >= dirp->__dd_loc) {
        if ((dirp->__dd_loc += pread(dirp->__dd_fd, dirp->__dd_buf, dirp->__dd_len, dirp->__dd_seek)) < 0) {
            goto freedir;
        }
    }
    ret->d_name = strdup(dirp->__dd_buf + dirp->__dd_seek);
    ret->d_namlen = strlen(ret->d_name);
    dirp->__dd_seek += ret->d_namlen + 1;

freedir:
    free(ret);
    return NULL;
}

struct dirent * readdir(DIR * dirp) {
    init_fsiface();
    if (files[dirp->__dd_fd]) {
        return hookfs_readdir(dirp);
    }
    return realfs.readdir(dirp);
}

struct fuse *g_fuse;

int g_fserrno;
int g_fuse_argc = 0;
char **g_fuse_argv = NULL;



static int
spdk_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		  off_t offset, struct fuse_file_info *fi,
		  enum fuse_readdir_flags flags)
{
	struct spdk_file *file;
	const char *filename;
	spdk_fs_iter iter;

	filler(buf, ".", NULL, 0, 0);
	filler(buf, "..", NULL, 0, 0);

	iter = spdk_fs_iter_first(g_fs);
	while (iter != NULL) {
		file = spdk_fs_iter_get_file(iter);
		iter = spdk_fs_iter_next(iter);
		filename = spdk_file_get_name(file);
		filler(buf, &filename[1], NULL, 0, 0);
	}

	return 0;
}

static int
spdk_fuse_mknod(const char *path, mode_t mode, dev_t rdev)
{
	return spdk_fs_create_file(g_fs, g_channel, path);
}

static int
spdk_fuse_unlink(const char *path)
{
	return spdk_fs_delete_file(g_fs, g_channel, path);
}

static int
spdk_fuse_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
	struct spdk_file *file;
	int rc;

	rc = spdk_fs_open_file(g_fs, g_channel, path, 0, &file);
	if (rc != 0) {
		return -rc;
	}

	rc = spdk_file_truncate(file, g_channel, size);
	if (rc != 0) {
		return -rc;
	}

	spdk_file_close(file, g_channel);

	return 0;
}

static int
spdk_fuse_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
	return 0;
}


static int
spdk_fuse_flush(const char *path, struct fuse_file_info *info)
{
	return 0;
}

static int
spdk_fuse_fsync(const char *path, int datasync, struct fuse_file_info *info)
{
	return 0;
}

static int
spdk_fuse_rename(const char *old_path, const char *new_path, unsigned int flags)
{
	return spdk_fs_rename_file(g_fs, g_channel, old_path, new_path);
}
