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
#include "spdk/string.h"

#define HOOKFS_SPDK_CONF_ENV "HOOKFS_SPDK_CONF"
#define HOOKFS_SPDK_BDEV_ENV "HOOKFS_BDEV"
#define HOOKFS_MOUNT_POINT_ENV "HOOKFS_MOUNT_POINT"
#define HOOKFS_LOG_ENV "HOOKFS_LOG_ARG"

#define HOOKFS_PAGE_SIZE (4096)

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
#include <pthread.h>
#include <libgen.h>
#include <bits/pthreadtypes.h>

static __thread struct spdk_io_channel * g_channel;
static struct spdk_filesystem * g_fs;

static char * g_bdev_name;
static char g_mountpoint[PATH_MAX];
static int g_mountpoint_strlen;
static char * g_logstr;
static struct spdk_bs_dev * g_bs_dev;
static long max_open;

typedef struct hookfs_fd {
    int fd;
    int oflag;
    struct spdk_file * file;
    struct spdk_file_stat stat;
    int updated;
    int offset;
    pthread_rwlock_t lock;
} hookfs_fd_t;

static hookfs_fd_t * fds;

static int normalizepath(const char * path, char * ret) {
    int i, j, last;
    char abspath[PATH_MAX];
    char * c;

    memset(abspath, 0, PATH_MAX);
    if (path[0] != '/') {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)) == NULL) {
            return 1;
        }
        sprintf(abspath, "%s/%s", cwd, path);
        i = strlen(cwd) + 1;
    } else {
        strcpy(abspath, path);
        i = 0;
    }
    last = strlen(abspath);

    c = ret;
    *c = '/';
    for (i = 0; i < last; i = j) {
        j = i + 1;
        for (; j < last && abspath[j] != '/'; j++);
        if (i + 1 == j) {
            continue;
        }
        if (i + 2 == j && abspath[i + 1] == '.') {
            continue;
        }
        if (i + 3 == j && abspath[i + 1] == '.' && abspath[i + 2] == '.') {
            if (c == ret) {
                continue;
            }
            *(c--) = 0;
            while (c != ret && *c != '/') {
                *(c--) = 0;
            }
            continue;
        }

        memcpy(c + 1, &abspath[i + 1], j - i);
        c += (j - i);
    }
    if (c != ret && *c == '/') {
        *c = 0;
    }
    return 0;
}

static pthread_t hookfs_thread, hookfs_thread2, hookfs_thread3;

static struct real_fsiface {
    int initialized;
    int (*open)(char const *, int, ...);
    int (*open64)(char const *, int, ...);
    int (*creat)(const char *, mode_t);
    ssize_t (*read)(int, void *, size_t);
    ssize_t (*write)(int, const void *, size_t);
    ssize_t (*pread)(int, void *, size_t, off_t);
    ssize_t (*pwrite)(int, const void *, size_t, off_t);
    ssize_t (*pread64)(int, void *, size_t, off_t);
    ssize_t (*pwrite64)(int, const void *, size_t, off_t);
    off_t (*lseek)(int, off_t, int);
    off64_t (*lseek64)(int, off64_t, int);
    int (*close)(int);
    int (*__xstat)(int ver, const char *, struct stat *);
    int (*__lxstat)(int ver, const char *, struct stat *);
    int (*__fxstat)(int ver, int fd, struct stat *);
    int (*__xstat64)(int ver, const char *, struct stat64 *);
    int (*__lxstat64)(int ver, const char *, struct stat64 *);
    int (*__fxstat64)(int ver, int fd, struct stat64 *);
//    int (*fstat)(int, struct stat *);
    int (*posix_fadvise)(int, off_t, off_t, int);
    int (*fsync)(int);
    int (*unlink)(const char *);
    int (*unlinkat)(int ,const char *, int);
    DIR * (*opendir)(const char *);
    int (*closedir)(DIR *);
    struct dirent * (*readdir)(DIR *);
    int (*mkdir)(const char *, mode_t);
    int (*rmdir)(const char *);
    int (*truncate)(const char *, off_t);
    int (*ftruncate)(int, off_t);
    int (*truncate64)(const char *, off_t);
    int (*ftruncate64)(int, off_t);
    int (*pthread_create)(pthread_t * thread, const pthread_attr_t * attr, void * (*start)(void *), void * arg);
    int (*access)(const char *, int);
} realfs = {.initialized = 0};


static int hookfs_mkdir(const char * abspath, mode_t m);

static void
start_hookfs_fn(void *arg1, void *arg2)
{
    int rc;
    char dummy[PATH_MAX];
    snprintf(dummy, PATH_MAX - 1, "%s/", g_mountpoint);
    hookfs_thread2 = pthread_self();
    g_channel = spdk_fs_alloc_io_channel_sync(g_fs);

    rc = blobfs2_init();
    if (rc) {
        SPDK_ERRLOG("failed to initialize blobfs2\n");
        realfs.initialized = -1;
        spdk_smp_rmb();
        return;
    }
    realfs.initialized = 1;
    spdk_smp_rmb();
    rc = hookfs_mkdir(dummy, 0755);
    if (rc != 0 && errno != EEXIST) {
        SPDK_ERRLOG("FS init failed: failed to create %s\n", g_mountpoint);
        realfs.initialized = -1;
        spdk_smp_rmb();
    }
}

static bool g_need_dma = false;

static void
init_cb(void *ctx, struct spdk_filesystem *fs, int fserrno)
{
    struct spdk_event *event;

    g_fs = fs;
    spdk_fs_set_need_dma(fs, g_need_dma);
    spdk_smp_rmb();
    g_channel = spdk_fs_alloc_io_channel_sync(g_fs);
    event = spdk_event_allocate(1, start_hookfs_fn, NULL, NULL);
    spdk_event_call(event);
}

#include <time.h>
#include <include/spdk/bdev_module.h>

static long longest = 0;
static void * func = NULL;
static void * arg = NULL;

static void
__call_fn(void *arg1, void *arg2)
{
    fs_request_fn fn;
    struct timespec t, t2;
    long nano;

    fn = (fs_request_fn)arg1;
    clock_gettime(CLOCK_MONOTONIC, &t);
    fn(arg2);
    clock_gettime(CLOCK_MONOTONIC, &t2);
    nano = (t2.tv_sec - t.tv_sec) * 1000 * 1000 * 1000 + (t2.tv_nsec - t.tv_nsec);
    if (nano > 1000 * 1000) {
        blobfs2_dump_request();
    }
    if (nano > longest) {
        longest = nano;
        func = fn;
        arg = arg2;
    }
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

    if (strcmp(bdev->product_name, "pmemblk disk") == 0) {
        g_need_dma = false;
    } else {
        g_need_dma = true;
    }
    g_bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);

//  printf("Mounting BlobFS on bdev %s\n", spdk_bdev_get_name(bdev));
    spdk_fs_load(g_bs_dev, __send_request, init_cb, NULL);
}

static void
shutdown_cb(void *ctx, int fserrno)
{
    pthread_kill(hookfs_thread2, SIGINT);
    pthread_kill(hookfs_thread3, SIGINT);
    spdk_fs_free_io_channel(g_channel);
    spdk_app_stop(0);
}

static void
hookfs_shutdown(void)
{
    blobfs2_shutdown();
    spdk_fs_unload(g_fs, shutdown_cb, NULL);
}

static void * load_symbol(const char * symbol) {
    char * msg;
    void * ret;
//    SPDK_ERRLOG("load: %s\n", symbol);
    ret = dlsym(RTLD_NEXT, symbol);
    if ( (msg = dlerror()) ) {
        SPDK_ERRLOG("dlsym error: %s\n", msg);
        exit(1);
    }
 //   SPDK_ERRLOG("loaded: %p\n", ret);
    return ret;
}

static void * start_app(void * args) {
    struct spdk_app_opts opts = {};
    int rc;

    hookfs_thread3 = pthread_self();
    spdk_app_opts_init(&opts);
    opts.name = program_invocation_short_name;
    opts.config_file = getenv(HOOKFS_SPDK_CONF_ENV);
    opts.reactor_mask = "0x3";
    opts.mem_size = 100 * 1024;
    opts.shutdown_cb = hookfs_shutdown;

    if (g_logstr) {
        rc = spdk_log_set_trace_flag(g_logstr);
        if (rc < 0) {
            fprintf(stderr, "unknown flag\n");
        }
        opts.print_level = SPDK_LOG_DEBUG;
    }

    rc = spdk_app_start(&opts, hookfs_run, NULL, NULL);
    if (!rc) {
        spdk_app_fini();
    }
    pthread_exit(0);
    return NULL;
}

static void init_fsiface(void) {
    char * mp;
    int i;

    g_bdev_name = getenv(HOOKFS_SPDK_BDEV_ENV);
    mp = getenv(HOOKFS_MOUNT_POINT_ENV);
    g_logstr = getenv(HOOKFS_LOG_ENV);

    if (!getenv(HOOKFS_SPDK_CONF_ENV) || !g_bdev_name || !mp) {
        SPDK_ERRLOG("set environment variables: %s, %s, %s\n", HOOKFS_SPDK_CONF_ENV, HOOKFS_SPDK_BDEV_ENV, HOOKFS_MOUNT_POINT_ENV);
        exit(1);
    }

    if (normalizepath(mp, g_mountpoint)) {
        SPDK_ERRLOG("%s set in %s is not a valid path", mp, HOOKFS_MOUNT_POINT_ENV);
        exit(1);
    }
    g_mountpoint_strlen = strlen(g_mountpoint);
    if (g_mountpoint_strlen == 0 || g_mountpoint[0] != '/') {
        SPDK_ERRLOG("%s (normalized path: %s) is an invalid path for mount point\n", mp, g_mountpoint);
        exit(1);
    }
    if (g_mountpoint_strlen == 1 && g_mountpoint_strlen == '/') {
        *(g_mountpoint + g_mountpoint_strlen) = '/';
        g_mountpoint_strlen += 1;
        *(g_mountpoint + g_mountpoint_strlen) = '\0';
    }

    if (!access(g_mountpoint, 0)) {
        SPDK_ERRLOG("%s set in %s already exists.\n", g_mountpoint, HOOKFS_MOUNT_POINT_ENV);
        exit(1);
    }
    g_mountpoint_strlen = strlen(g_mountpoint);

    max_open = sysconf(_SC_OPEN_MAX);
    fds = malloc(sizeof(hookfs_fd_t) * max_open);
    if (!fds) {
        SPDK_ERRLOG("malloc(%ld) failed\n", sizeof(hookfs_fd_t) * max_open);
        exit(1);
    }
    memset(fds, 0, sizeof(hookfs_fd_t) * max_open);
    for (i = 0; i < max_open; i++) {
        if (pthread_rwlock_init(&fds[i].lock, NULL)) {
            SPDK_ERRLOG("failed to initialize rwlock: %d\n", errno);
            exit(1);
        }
    }

    spdk_smp_rmb();

    pthread_create(&hookfs_thread, NULL, start_app, NULL);
}

__attribute__((constructor, used)) void hookfs_init(void);

void hookfs_init(void) {
    if (!realfs.initialized) {
        init_fsiface();
        while (realfs.initialized == 0) {
            sleep(1);
        }
        if (realfs.initialized < 0) {
            SPDK_ERRLOG("error!!!\n");
        }
//        SPDK_ERRLOG("init!!!!!\n");
    }
}

__attribute__((destructor, used)) void hookfs_fini(void);
void hookfs_fini(void) {
}

int __libc_start_main(int argc, char **argv, char **env, int (*init)(int, char **, char **),
    void (*fini)(void), void * stinfo, void * stack_end)
{
    struct spdk_thread * thread = spdk_allocate_thread(NULL, NULL, NULL, NULL, NULL);
	if (!thread) {
		SPDK_ERRLOG("failed to allocate spdk thread\n");
		return -1;
	}
    g_channel = spdk_fs_alloc_io_channel_sync(g_fs);
    int (*orig)(int, char **, char **, int (*init)(int, char **, char **), void (*fini)(void), void *, void *)
		   = dlsym(RTLD_NEXT, "__libc_start_main");
    return orig(argc, argv, env, init, fini, stinfo, stack_end);
}

static void * spdk_thread_runner(void * args) {
    void * (*realstart)(void *) = ((void **)args)[0];
    void * realarg = ((void **)args)[1];
    void * ret;
    struct spdk_thread * thread = spdk_allocate_thread(NULL, NULL, NULL, NULL, NULL);
    if (!thread) {
        SPDK_ERRLOG("failed to register thread\n");
        return NULL;
    }
    g_channel = spdk_fs_alloc_io_channel_sync(g_fs);
    ret = realstart(realarg);
    free(args);
    spdk_fs_free_io_channel(g_channel);
    return ret;
}

int pthread_create(pthread_t * thread, const pthread_attr_t * attr, void * (*start)(void *), void * arg)
{
    if (!realfs.pthread_create) {
        realfs.pthread_create = load_symbol("pthread_create");
    }
    if (realfs.initialized > 0) {
        void ** args = malloc(sizeof(void *) * 2);
        args[0] = start;
        args[1] = arg;
        return realfs.pthread_create(thread, attr, spdk_thread_runner, args);
    }
    return realfs.pthread_create(thread, attr, start, arg);
}


static bool hookfs_is_under_mountpoint(const char * abspath) {
    return strncmp(abspath, g_mountpoint, g_mountpoint_strlen) == 0;
}


static int __hookfs_deletefile(const char * blobfspath) {
    struct spdk_file *file;
    int rc;
    char * buf;
    struct spdk_file_stat stat;
    char * duppath, * duppath2, * parent, * base;

    if (strlen(blobfspath) == 0) {
        errno = EINVAL;
        return -1;
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
    if (rc) {
        errno = -rc;
        return -1;
    }

    duppath = strdup(blobfspath);
    duppath2 = strdup(blobfspath);
    if (!duppath || !duppath2) {
        errno = ENOMEM;
        rc = -1;
        goto free_duppath;
    }

    parent = dirname(duppath);
    base = basename(duppath2);
    rc = blobfs2_stat(g_fs, g_channel, parent, &stat);
    if (rc) {
        errno = ENOTDIR;
        goto free_duppath;
    }

    buf = malloc(stat.size);
    if (!buf) {
        errno = ENOMEM;
        rc = -1;
        goto free_duppath;
    }

    rc = blobfs2_open(g_fs, g_channel, parent, 0, &file);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

    uint64_t off = 0;
    while (off < stat.size) {
        int64_t n = blobfs2_read(file, g_channel, buf, off, stat.size - off, false);
        if (n > 0) {
            off += n;
        } else {
            break;
        }
    }
    if (off < stat.size) {
        errno = EIO;
        rc = -1;
        goto close_file;
    }
    blobfs2_close(file, g_channel); // ensure finishing the above file read here

    off = 0;
    while (off < stat.size) {
        char * name = buf + off + 1;
        if (strcmp(name, base) == 0) {
            break;
        }
        off += 1 + strlen(name) + 1;
    }
    if (off >= stat.size) {
        errno = ENOENT;
        rc = -1;
        goto close_file;
    }

// truncate does not behave as expected. so recreate the file. TODO: fix this
    rc = blobfs2_delete_file(g_fs, g_channel, parent);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

    rc = blobfs2_create_file(g_fs, g_channel, parent);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

/*    rc = blobfs2_open(g_fs, g_channel, parent, 0, &file);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

    rc = blobfs2_truncate(file, g_channel, 0);
    if (rc) {
        errno = -rc;
        goto close_file;
    }

    blobfs2_close(file, g_channel);*/

    rc = blobfs2_open(g_fs, g_channel, parent, 0, &file);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

    if (off > 0) {
        rc = blobfs2_write(file, g_channel, buf, 0, off, false);
        if (rc < 0) {
            errno = EIO;
            rc = -1;
            goto close_file;
        }
    }

    uint64_t o2 = off + 1 + strlen(base) + 1;
    if (o2 < stat.size) {
        rc = blobfs2_write(file, g_channel, buf + o2, off, stat.size - o2, false);
        if (rc < 0) {
            errno = EIO;
            rc = -1;
            goto close_file;
        }
    }
    rc = 0;

close_file:
    blobfs2_close(file, g_channel);
free_buf:
    free(buf);
free_duppath:
    free(duppath);
    free(duppath2);
    return rc;
}

static int hookfs_is_dir(const char * blobfspath);

static int __hookfs_isemptydir(const char * blobfspath) {
    struct spdk_file *file;
    int rc;
    char * buf;
    struct spdk_file_stat stat;

    if (strlen(blobfspath) == 0) {
        SPDK_ERRLOG("cannot delete file %s\n", blobfspath);
        errno = EFAULT;
        return -1;
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
    if (rc) {
        errno = -rc;
        return -1;
    }

    rc = hookfs_is_dir(blobfspath);
    if (rc != 1) {
        errno = ENOTDIR;
        return -1;
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
    if (rc) {
        errno = ENOTDIR;
        return -1;
    }

    buf = malloc(stat.size);
    if (!buf) {
        errno = ENOMEM;
        return -1;
    }

    rc = blobfs2_open(g_fs, g_channel, blobfspath, 0, &file);
    if (rc) {
        errno = -rc;
        goto free_buf;
    }

    uint64_t off = 0;
    while (off < stat.size) {
        int64_t n = blobfs2_read(file, g_channel, buf, off, stat.size - off, false);
        if (n > 0) {
            off += n;
        } else {
            break;
        }
    }
    if (off < stat.size) {
        errno = EIO;
        rc = -1;
        goto close_file;
    }
    blobfs2_close(file, g_channel);

    off = 0;
    while (off < stat.size) {
        char * name = buf + off + 1;
        if (!(strcmp(name, ".") == 0 || strcmp(name, "..") == 0)) {
            rc = 0;
            errno = ENOTEMPTY;
            goto free_buf;
        }
        off += 1 + strlen(name) + 1;
    }
    rc = 1;
    goto free_buf;

close_file:
    blobfs2_close(file, g_channel);
free_buf:
    free(buf);
    return rc;
}


static int __hookfs_addfile(const char * blobfspath, const char *filename, unsigned char d_type) {
    struct spdk_file *file;
    int rc;
    char buf[PATH_MAX + 2];
    struct spdk_file_stat stat;

    if (strlen(filename) == 0 || strlen(blobfspath) == 0 || !(d_type == DT_DIR || d_type == DT_REG)) {
        SPDK_ERRLOG("cannot add file %s, d_type=%u in %s\n", filename, d_type, blobfspath);
        return -1;
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
    if (rc == -ENOENT) {
        rc = blobfs2_create_file(g_fs, g_channel, blobfspath);
        if (rc != 0) {
            SPDK_ERRLOG("failed to create file: %s, %d\n", blobfspath, rc);
            return -1;
        }
        stat.size = 0;
    }

    rc = blobfs2_open(g_fs, g_channel, blobfspath, SPDK_BLOBFS_OPEN_CREATE, &file);
    if (rc != 0) {
        return -rc;
    }

    snprintf(buf, PATH_MAX - 1, "%c%s%c", (char)d_type, filename, 0);
//    SPDK_ERRLOG("addfile: %s, %u, %lu, %lu in %s\n", filename, d_type, stat.size, strlen(buf) + 1, blobfspath);
    rc = blobfs2_write(file, g_channel, (void *)buf, stat.size, strlen(buf) + 1, false);
    if (rc < 0) {
        blobfs2_close(file, g_channel);
        return -rc;
    }

    rc = blobfs2_close(file, g_channel);
    if (rc != 0) {
        return -rc;
    }
    return 0;
}

static int __hookfs_open(const char * blobfspath, int oflag, int mflag)
{
    struct spdk_file * file;
    int rc, fd;
    int creating = 0;

    if ((oflag & O_TRUNC) && !((oflag & O_RDWR ) || (oflag & O_WRONLY))) {
        SPDK_ERRLOG("attempt to truncate on unreadable flag\n");
        errno = EINVAL;
        return -1;
    }

    fd = realfs.open("/dev/null", O_RDWR, mflag);
    if (fd < 0) {
        SPDK_ERRLOG("obtaining a FD failed: %d (%s)\n", errno, spdk_strerror(errno));
        return fd;
    }

    if (oflag & O_CREAT) {
        rc = blobfs2_create_file(g_fs, g_channel, blobfspath);
        if ((oflag & O_EXCL) && rc == -EEXIST) {
            SPDK_ERRLOG("failed to create file: %s, %d\n", blobfspath, rc);
            goto realfs_close;
        }
        if (rc == 0) {
            char * duppath = strdup(blobfspath);
            char * parent = dirname(duppath);
            char * duppath2 = strdup(blobfspath);
            char * base = basename(duppath2);
            rc = __hookfs_addfile(parent, base, DT_REG);
            free(duppath);
            free(duppath2);
            creating = 1;
            if (rc != 0) {
                SPDK_ERRLOG("failed to add '%s' in %s\n", base, parent);
                goto realfs_close;
            }
        }
    }

    rc = blobfs2_open(g_fs, g_channel, blobfspath, oflag, &file);
//    SPDK_ERRLOG("open: %s, %d, fd = %d, file = %p\n", blobfspath, rc, fd, file);
    if (rc != 0) {
        SPDK_ERRLOG("failed to open %s, %d\n", blobfspath, rc);
        goto realfs_close;
    }
    if (oflag & O_TRUNC) {
        rc = blobfs2_truncate(file, g_channel, 0);
        if (rc != 0) {
            SPDK_ERRLOG("failed to truncate: %s, %d\n", blobfspath, rc);
            goto spdk_close;
        }
    }

    pthread_rwlock_wrlock(&fds[fd].lock);
    fds[fd].fd = fd;
    fds[fd].file = file;
    fds[fd].offset = 0;
    fds[fd].updated = 1;
    fds[fd].oflag = oflag;
    pthread_rwlock_unlock(&fds[fd].lock);

    return fd;

spdk_close:
    blobfs2_close(file, g_channel);
realfs_close:
    realfs.close(fd);

    if (creating) {
        blobfs2_delete_file(g_fs, g_channel, blobfspath);
    }

    errno = -rc;
    return -1;
}

static int hookfs_open(const char * abspath, int oflag, int mflag) {
    const char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    return __hookfs_open(path, oflag, mflag);
}

int open(char const * path, int oflag, ...) {
    va_list args;
    int mflag;
    char abspath[PATH_MAX];

    va_start(args, oflag);
    mflag = va_arg(args, int);

    if (!realfs.open) {
        realfs.open = load_symbol("open");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_open(abspath, oflag, mflag);
    }

//    SPDK_ERRLOG(">>>>> in open: %s <<<<<<\n", path);
    return realfs.open(path, oflag, mflag);
}

int open64(char const * path, int oflag, ...) {
    va_list args;
    int mflag;
    char abspath[PATH_MAX];

    va_start(args, oflag);
    mflag = va_arg(args, int);

    if (!realfs.open64) {
        realfs.open64 = load_symbol("open64");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_open(abspath, oflag, mflag);
    }

//    SPDK_ERRLOG(">>>>> in open: %s <<<<<<\n", path);
    return realfs.open64(path, oflag, mflag);
}

int creat(const char * path, mode_t mode) {
    char abspath[PATH_MAX];
    if (!realfs.creat) {
        realfs.creat = load_symbol("creat");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_open(abspath, O_CREAT|O_WRONLY|O_TRUNC, mode);
    }

//    SPDK_ERRLOG(">>>>> in creat: %s <<<<<<\n", path);
    return realfs.creat(path, mode);
}

static int hookfs_release(hookfs_fd_t * fd) {
    int rc;
    struct spdk_file * file;
    int realfd;

    pthread_rwlock_wrlock(&fd->lock);
    if (!fd->file) { // racy case
        pthread_rwlock_unlock(&fd->lock);
        errno = EBADF;
        return -1;
    }
    file = fd->file;
    fd->file = NULL;
    realfd = fd->fd;
    fd->fd = -1;
    pthread_rwlock_unlock(&fd->lock);

    rc = blobfs2_close(file, g_channel);
//    SPDK_ERRLOG("close: %p, fd = %d, %d\n", fds[fd].file, fd, rc);
    if (rc == 0) {
        realfs.close(realfd);
        return 0;
    }

    // error condition
    pthread_rwlock_wrlock(&fd->lock);
    fd->file = file;
    fd->fd = realfd;
    pthread_rwlock_unlock(&fd->lock);
    if (rc == -EEXIST) {
        errno = EBADF;
    } else {
        errno = -rc;
    }
    return -1;
}

int close(int fd) {
    if (!realfs.close) {
        realfs.close = load_symbol("close");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_release(&fds[fd]);
    }
//    SPDK_ERRLOG(">>>>> in close <<<<<<\n");
    return realfs.close(fd);
}

static int hookfs_pread(hookfs_fd_t *fd, char * buf, size_t len, uint64_t offset)
{
    int oflag;
    struct spdk_file * file;
    int64_t rc;

    if (len == 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&fd->lock);
    oflag = fd->oflag;
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }

    rc = blobfs2_read(file, g_channel, buf, offset, len, oflag & O_DIRECT);
    if (rc >= 0) {
        return (int) rc;
    }
    errno = (int) -rc;
    return -1;
}

static int hookfs_read(hookfs_fd_t *fd, char * buf, size_t len)
{
    int oflag;
    struct spdk_file * file;
    uint64_t off;
    int64_t rc;

    if (len == 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&fd->lock);
    oflag = fd->oflag;
    file = fd->file;
    off = fd->offset;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }

    rc = blobfs2_read(file, g_channel, buf, off, len, oflag & O_DIRECT);
    if (rc == 0) {
        return 0;
    } else if (rc > 0) {
        pthread_rwlock_wrlock(&fd->lock);
        fd->offset += rc;
        pthread_rwlock_unlock(&fd->lock);
        return (int) rc;
    }
    errno = (int) -rc;
    return -1;
}

ssize_t read(int fd, void * buf, size_t count) {
    if (!realfs.read) {
        realfs.read = load_symbol("read");
    }

    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_read(&fds[fd], buf, count);
    }
//    SPDK_ERRLOG(">>>>> in read <<<<<\n");
    return realfs.read(fd, buf, count);
}

ssize_t pread(int fd, void * buf, size_t count, off_t offset) {
    if (!realfs.pread) {
        realfs.pread = load_symbol("pread");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_pread(&fds[fd], buf, count, offset);
    }
//    SPDK_ERRLOG(">>>>> in pread <<<<<\n");
    return realfs.pread(fd, buf, count, offset);
}

ssize_t pread64(int fd, void * buf, size_t count, off_t offset) {
    if (!realfs.pread64) {
        realfs.pread64 = load_symbol("pread64");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_pread(&fds[fd], buf, count, offset);
    }
//    SPDK_ERRLOG(">>>>> in pread <<<<<\n");
    return realfs.pread64(fd, buf, count, offset);
}

static int hookfs_pwrite(hookfs_fd_t * fd, const char * buf, size_t len, uint64_t offset) {
    int rc;
    struct spdk_file * file;
    int oflag;

    if (len == 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&fd->lock);
    oflag = fd->oflag;
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }

    rc = blobfs2_write(file, g_channel, (void *)buf, offset, len, oflag & O_DIRECT);
    if (rc >= 0) {
        return rc;
    } else {
        errno = -rc;
        return -1;
    }
}

static int hookfs_write(hookfs_fd_t * fd, const char * buf, size_t len) {
    int rc;
    struct spdk_file * file;
    uint64_t off;
    int oflag;

    if (len == 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&fd->lock);
    oflag = fd->oflag;
    file = fd->file;
    off = fd->offset;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }

    rc = blobfs2_write(file, g_channel, (void *)buf, off, len, oflag & O_DIRECT);
    if (rc >= 0) {
        pthread_rwlock_wrlock(&fd->lock);
        fd->offset += len;
        pthread_rwlock_unlock(&fd->lock);
        return rc;
    } else {
        errno = -rc;
        return -1;
    }
}

ssize_t write(int fd, const void * buf, size_t count) {
    if (!realfs.write) {
        realfs.write = load_symbol("write");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_write(&fds[fd], buf, count);
    }
    return realfs.write(fd, buf, count);
}

ssize_t pwrite(int fd, const void * buf, size_t count, off_t offset) {
    if (!realfs.pwrite) {
        realfs.pwrite = load_symbol("pwrite");
    }

    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_pwrite(&fds[fd], buf, count, offset);
    }
    return realfs.pwrite(fd, buf, count, offset);
}

ssize_t pwrite64(int fd, const void * buf, size_t count, off_t offset) {
    if (!realfs.pwrite64) {
        realfs.pwrite64 = load_symbol("pwrite64");
    }

    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_pwrite(&fds[fd], buf, count, offset);
    }
    return realfs.pwrite64(fd, buf, count, offset);
}


static DIR * __hookfs_opendir(const char * blobfspath);
static struct dirent * hookfs_readdir(DIR * _dirp);
static int hookfs_closedir(DIR * _dirp);

static int hookfs_is_dir(const char * blobfspath)
{
    char * duppath = strdup(blobfspath);
    char * parent = dirname(duppath);
    char * duppath2 = strdup(blobfspath);
    char * base = basename(duppath2);
    int rc;
    DIR * dir;
    struct dirent * d;

    if (strlen(blobfspath) == 1 && *blobfspath == '/') {
        free(duppath);
        free(duppath2);
        return 1;
    }

    dir = __hookfs_opendir(parent);
    if (!dir) {
        SPDK_ERRLOG("not found %s in %s, %s, %s, %s\n", blobfspath, parent, duppath, duppath2, base);
        free(duppath);
        free(duppath2);
        return -1;
    }

    rc = 0;
    while ((d = hookfs_readdir(dir)) != NULL) {
        if (strcmp(d->d_name, base) == 0) {
            rc = d->d_type == DT_DIR;
            break;
        }
    }
    hookfs_closedir(dir);
    free(duppath);
    free(duppath2);
    return rc;
}

static int __hookfs_stat(const char * blobfspath, struct stat * stbuf)
{
    struct spdk_file_stat stat;
    int rc;

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
//    SPDK_ERRLOG(">>>>> in stat:%s, %d <<<<<\n", blobfspath, rc);
    if (rc == 0) {
        stbuf->st_mode = (!hookfs_is_dir(blobfspath) ? S_IFREG : S_IFDIR) | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = stat.size;
    }

    return rc;
}

static int hookfs_stat(const char * abspath, struct stat * stbuf)
{
    const char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    return __hookfs_stat(path, stbuf);
}

static int hookfs_fstat(hookfs_fd_t * fd, struct stat * stbuf)
{
    struct spdk_file * file;

    pthread_rwlock_rdlock(&fd->lock);
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }
    return __hookfs_stat(spdk_file_get_name(file), stbuf);
}

int __xstat(int ver, const char * path, struct stat * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__xstat) {
        realfs.__xstat = load_symbol("__xstat");
    }

//  SPDK_ERRLOG(">>>>> in __xstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    if (realfs.initialized > 0 && !normalizepath(path, abspath)) {
        if (hookfs_is_under_mountpoint(abspath)) {
            return hookfs_stat(abspath, stbuf);
        } else if (strcmp(abspath, "/") == 0) {
            return realfs.__xstat(ver, abspath, stbuf);
        }
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__xstat(ver, path, stbuf);
}

int __lxstat(int ver, const char * path, struct stat * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__lxstat) {
        realfs.__lxstat = load_symbol("__lxstat");
    }

 //   SPDK_ERRLOG(">>>>> in __lxstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    if (realfs.initialized > 0 && !normalizepath(path, abspath)) {
        if (hookfs_is_under_mountpoint(abspath)) {
            return hookfs_stat(abspath, stbuf);
        } else if (strcmp(abspath, "/") == 0) {
            return realfs.__lxstat(ver, abspath, stbuf);
        }
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__lxstat(ver, path, stbuf);

}

int __fxstat(int ver, int fd, struct stat * stbuf) {
    if (!realfs.__fxstat) {
        realfs.__fxstat = load_symbol("__fxstat");
    }
//    SPDK_ERRLOG(">>>>> in __fxstat:%p, fd = %d <<<<<\n", fds[fd].file, fd);
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_fstat(&fds[fd], stbuf);
    }
    return realfs.__fxstat(ver, fd, stbuf);
}

static int __hookfs_stat64(const char * blobfspath, struct stat64 * stbuf)
{
    struct spdk_file_stat stat;
    int rc;

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
//    SPDK_ERRLOG(">>>>> in stat:%s, %d <<<<<\n", blobfspath, rc);
    if (rc == 0) {
        stbuf->st_mode = (!hookfs_is_dir(blobfspath) ? S_IFREG : S_IFDIR) | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = stat.size;
    }

    return rc;
}

static int hookfs_stat64(const char * abspath, struct stat64 * stbuf)
{
    const char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    return __hookfs_stat64(path, stbuf);
}

static int hookfs_fstat64(hookfs_fd_t * fd, struct stat64 * stbuf)
{
    struct spdk_file * file;

    pthread_rwlock_rdlock(&fd->lock);
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }
    return __hookfs_stat64(spdk_file_get_name(file), stbuf);
}

int __xstat64(int ver, const char * path, struct stat64 * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__xstat64) {
        realfs.__xstat64 = load_symbol("__xstat64");
    }

//  SPDK_ERRLOG(">>>>> in __xstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    if (realfs.initialized > 0 && !normalizepath(path, abspath)) {
        if (hookfs_is_under_mountpoint(abspath)) {
            return hookfs_stat64(abspath, stbuf);
        } else if (strcmp(abspath, "/") == 0) {
            return realfs.__xstat64(ver, abspath, stbuf);
        }
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__xstat64(ver, path, stbuf);
}

int __lxstat64(int ver, const char * path, struct stat64 * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__lxstat64) {
        realfs.__lxstat64 = load_symbol("__lxstat64");
    }

 //   SPDK_ERRLOG(">>>>> in __lxstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    if (realfs.initialized > 0 && !normalizepath(path, abspath)) {
        if (hookfs_is_under_mountpoint(abspath)) {
            return hookfs_stat64(abspath, stbuf);
        } else if (strcmp(abspath, "/") == 0) {
            return realfs.__lxstat64(ver, abspath, stbuf);
        }
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__lxstat64(ver, path, stbuf);

}

int __fxstat64(int ver, int fd, struct stat64 * stbuf) {
    if (!realfs.__fxstat64) {
        realfs.__fxstat64 = load_symbol("__fxstat64");
    }
//    SPDK_ERRLOG(">>>>> in __fxstat:%p, fd = %d <<<<<\n", fds[fd].file, fd);
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_fstat64(&fds[fd], stbuf);
    }
    return realfs.__fxstat64(ver, fd, stbuf);
}

static int64_t hookfs_lseek(hookfs_fd_t * fd, uint64_t offset, int whence) {
    struct spdk_file * file;
    struct spdk_file_stat stat;
    int64_t newoffset;

    pthread_rwlock_rdlock(&fd->lock);
    file = fd->file;
    newoffset = fd->offset;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }

    int rc = blobfs2_stat(g_fs, g_channel, spdk_file_get_name(file), &stat);
    if (rc != 0) {
        errno = EBADF;
        return -1;
    }
    switch (whence) {
        case SEEK_SET:
            newoffset = offset;
            break;
        case SEEK_CUR:
            newoffset += offset;
            break;
        case SEEK_END:
            newoffset = stat.size + offset;
            break;
        default:
            errno = EINVAL;
            return -1;
    }
    if (newoffset < 0) {
        errno = EINVAL;
        return -1;
    }
    if ((whence == SEEK_DATA || whence == SEEK_HOLE) && newoffset > (int64_t) stat.size) {
        errno = ENXIO;
        return -1;
    }

    pthread_rwlock_wrlock(&fd->lock);
    fd->offset = newoffset;
    pthread_rwlock_unlock(&fd->lock);

    return newoffset;
}

off_t lseek(int fd, off_t offset, int whence) {
    if (!realfs.lseek) {
        realfs.lseek = load_symbol("lseek");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return (off_t) hookfs_lseek(&fds[fd], offset, whence);
    }
//    SPDK_ERRLOG(">>>>> in lseek <<<<<\n");
    return realfs.lseek(fd, offset, whence);
}

off64_t lseek64(int fd, off64_t offset, int whence) {
    if (!realfs.lseek64) {
        realfs.lseek64 = load_symbol("lseek64");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_lseek(&fds[fd], offset, whence);
    }
//    SPDK_ERRLOG(">>>>> in lseek <<<<<\n");
    return realfs.lseek64(fd, offset, whence);
}

int posix_fadvise(int fd, off_t offset, off_t len, int advice) {
    if (!realfs.posix_fadvise) {
        realfs.posix_fadvise = load_symbol("posix_fadvise");
    }
 
    if (realfs.initialized > 0 && fds[fd].file) {
        return 0;
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.posix_fadvise(fd, offset, len, advice);
}

static int hookfs_fsync(hookfs_fd_t * fd) {
    struct spdk_file * file;

    pthread_rwlock_rdlock(&fd->lock);
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);
    if (!file) {
        errno = EBADF;
        return -1;
    }
    return blobfs2_sync(file, g_channel);
}

int fsync(int fd) {
    if (!realfs.fsync) {
        realfs.fsync = load_symbol("fsync");
    }
 
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_fsync(&fds[fd]);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.fsync(fd);
}

static int hookfs_unlink(const char *abspath) {
    const char * blobfspath = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    if(hookfs_is_dir(blobfspath)) {
        errno = EISDIR;
        return -1;
    }
    int rc = __hookfs_deletefile(blobfspath);
    if (rc == 0) {
        rc = blobfs2_delete_file(g_fs, g_channel, blobfspath);
        if (rc) {
            errno = -rc;
            return -1;
        }
        return 0;
    }
    return -1;
}

int unlink(const char * path) {
    char abspath[PATH_MAX];
    if (!realfs.unlink) {
        realfs.unlink = load_symbol("unlink");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_unlink(abspath);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.unlink(path);
}

static int hookfs_rmdir(const char *abspath) {
    const char * blobfspath = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    if(__hookfs_isemptydir(blobfspath) > 0) {
        int rc = __hookfs_deletefile(blobfspath);
        if (rc == 0) {
            int rc = blobfs2_delete_file(g_fs, g_channel, blobfspath);
            if (rc) {
                errno = -rc;
                return -1;
            }
            return 0;
        }
        return -1;
    }
    return -1;
}

int rmdir(const char * path) {
    char abspath[PATH_MAX];
    if (!realfs.rmdir) {
        realfs.rmdir = load_symbol("rmdir");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_rmdir(abspath);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.rmdir(path);
}

int unlinkat(int dirfd, const char * path, int flags) {
    char abspath[PATH_MAX];
    if (!realfs.unlinkat) {
        realfs.unlinkat = load_symbol("unlinkat");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        if (dirfd == AT_FDCWD || strcmp(path, abspath) == 0) {
            if ((flags & AT_REMOVEDIR)) {
                return rmdir(path);
            }
            return unlink(path);
        }
        /// TODO: add here
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.unlinkat(dirfd, path, flags);
}

typedef struct hookfs_dir {
    int __dd_fd;    /* file descriptor associated with directory */
    long    __dd_loc;   /* offset in current buffer */
    long    __dd_size;  /* amount of data returned */
    char    *__dd_buf;  /* data buffer */
    int __dd_len;   /* size of data buffer */
    long    __dd_seek;  /* magic cookie returned */
    int __dd_flags; /* flags for readdir */
    int __dd_count;
} hookfs_dir_t;


#define NR_DIRENTBUF (4096)

static DIR * __hookfs_opendir(const char * blobfspath)
{
    struct spdk_file_stat stat;
    hookfs_dir_t * ret;
    int rc;

    ret = malloc(sizeof(hookfs_dir_t));
    if (!ret) {
        errno = ENOMEM;
        return NULL;
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
//    SPDK_ERRLOG("opendir: %s, %d\n", blobfspath, rc);
    if (rc != 0) {
        errno = ENOENT;
        goto freedir;
    }
    ret->__dd_size = stat.size - 1;
    ret->__dd_flags = 0777;

    ret->__dd_len = HOOKFS_PAGE_SIZE + NR_DIRENTBUF * sizeof(struct dirent);
    ret->__dd_buf = malloc(ret->__dd_len);
    if (!ret->__dd_buf) {
        errno = ENOMEM;
        goto freedir;
    }

    ret->__dd_fd = __hookfs_open(blobfspath, O_RDWR, 0);
    if (ret->__dd_fd < 0) {
        goto freebuf;
    }

    ret->__dd_loc = 0;
    ret->__dd_count = -1;

    return (DIR *)ret;

freebuf:
    free(ret->__dd_buf);
freedir:
    free(ret);
    return NULL;
}

static DIR * hookfs_opendir(const char * name) {
    const char * path = (*(name + g_mountpoint_strlen) == 0) ? "/": name + g_mountpoint_strlen;
    return __hookfs_opendir(path);
}

DIR * opendir(const char * name) {
    char abspath[PATH_MAX];

    if (!realfs.opendir) {
        realfs.opendir = load_symbol("opendir");
    }
 
//    SPDK_ERRLOG(">>>>> in opendir:%s <<<<<\n", name);
    if (realfs.initialized > 0 && !normalizepath(name, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_opendir(abspath);
    }

//    SPDK_ERRLOG(">>>>> in opendir:%s <<<<<\n", name);
    return realfs.opendir(name);
}

static struct dirent * hookfs_readdir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    // readdir is not thread-safe, so no locks
    if (dirp->__dd_count <= 0) {
        if (dirp->__dd_loc < dirp->__dd_size) {
            char * src = dirp->__dd_buf;
            struct dirent * d = (struct dirent *)(src + HOOKFS_PAGE_SIZE);
            long rc = blobfs2_read(fds[dirp->__dd_fd].file, g_channel, src, dirp->__dd_loc, HOOKFS_PAGE_SIZE, false);
            if (rc < 0) {
                return NULL;
            }
            memset(d, 0, HOOKFS_PAGE_SIZE);
            dirp->__dd_count = 0;
            while (src < dirp->__dd_buf + rc - 1) {
                d->d_type = *(unsigned char *)src;
                src += 1;
                strcpy(d->d_name, src);
                src += strlen(d->d_name) + 1;
//                SPDK_ERRLOG("readdir: %s, %u\n", d->d_name, d->d_type);
                d++;
                dirp->__dd_count++;
            }
            dirp->__dd_loc += rc;
        } else {
            return NULL;
        }
    }

    return (struct dirent *)(dirp->__dd_buf + HOOKFS_PAGE_SIZE + sizeof(struct dirent) * --dirp->__dd_count);
}

struct dirent * readdir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    if (!realfs.readdir) {
        realfs.readdir = load_symbol("readdir");
    }
 
    if (realfs.initialized > 0 && fds[dirp->__dd_fd].file) {
        return hookfs_readdir(_dirp);
    }
//    SPDK_ERRLOG(">>>>> in readdir <<<<<\n");
    return realfs.readdir(_dirp);
}

static int hookfs_mkdir(const char * abspath, mode_t m) {
    struct spdk_file_stat stat;
    const char * blobfspath = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    char * duppath = strdup(blobfspath);
    char * parent = dirname(duppath);
    char * duppath2 = strdup(blobfspath);
    char * base = basename(duppath2);
    int rc;

//    SPDK_ERRLOG("mkdir: %s (%s) under %s\n", blobfspath, abspath, parent);
    if (strcmp(blobfspath, parent) != 0) {
        rc = hookfs_is_dir(parent);
        if (rc == 0) {
            errno = EACCES;
            rc = -1;
            goto fin;
        } else if (rc < 0) {
            errno = -rc;
            rc = -1;
            goto fin;
        }
    }

    rc = blobfs2_stat(g_fs, g_channel, blobfspath, &stat);
    if (rc == -ENOENT) {
//        SPDK_ERRLOG("mkdir: %s\n", blobfspath);

        if (__hookfs_addfile(blobfspath, ".", DT_DIR)) {
            SPDK_ERRLOG("failed to create '.' in %s\n", blobfspath);
            rc = -1;
            goto fin;
        }
        if (__hookfs_addfile(blobfspath, "..", DT_DIR)) {
            SPDK_ERRLOG("failed to create '..' in %s\n", blobfspath);
            rc = -1;
            goto fin;
        }

        if (strcmp(blobfspath, parent) == 0) {
           rc = 0;
           goto fin;
        }

        if (__hookfs_addfile(parent, base, DT_DIR)) {
            SPDK_ERRLOG("failed to create '%s' in %s\n", base, parent);
            errno = ENOENT;
            rc = -1;
        } else {
            rc = 0;
        }
    } else {
        rc = -1;
        errno = EEXIST;
    }
fin:
    free(duppath);
    free(duppath2);
    return rc;
}

int mkdir(const char * name, mode_t m) {
    char abspath[PATH_MAX];

    if (!realfs.mkdir) {
        realfs.mkdir = load_symbol("mkdir");
    }

    if (realfs.initialized > 0 && !normalizepath(name, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_mkdir(abspath, m);
    }

    return realfs.mkdir(name, m);
}

static int hookfs_closedir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    int rc = close(dirp->__dd_fd);
    free(dirp->__dd_buf);
    free(dirp);
    return rc;
}

int closedir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    if (!realfs.closedir) {
        realfs.closedir = load_symbol("closedir");
    }

    if (realfs.initialized > 0 && fds[dirp->__dd_fd].file) {
        return hookfs_closedir(_dirp);
    }
//    SPDK_ERRLOG(">>>>> in closedir <<<<<\n");
    return realfs.closedir(_dirp);
}

static int hookfs_truncate(const char * abspath, off_t length) {
    const char * blobfspath = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    struct spdk_file * file;
    int rc = blobfs2_open(g_fs, g_channel, blobfspath, 0, &file);
    if (rc != 0) {
        errno = -rc;
        return -1;
    }
    rc = blobfs2_truncate(file, g_channel, length);
    blobfs2_close(file, g_channel);
    if (rc != 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

static int hookfs_ftruncate(hookfs_fd_t * fd, off_t length) {
    int rc;
    struct spdk_file * file;

    pthread_rwlock_rdlock(&fd->lock);
    file = fd->file;
    pthread_rwlock_unlock(&fd->lock);

    if (!file) {
        errno = EBADF;
        return -1;
    }
    rc = blobfs2_truncate(file, g_channel, length);
    if (rc != 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

int truncate(const char * path, off_t length) {
    char abspath[PATH_MAX];
    if (!realfs.truncate) {
        realfs.truncate = load_symbol("truncate");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_truncate(abspath, length);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.truncate(path, length);
}

int truncate64(const char * path, off_t length) {
     char abspath[PATH_MAX];
    if (!realfs.truncate64) {
        realfs.truncate64 = load_symbol("truncate64");
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_truncate(abspath, length);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.truncate64(path, length);
}

int ftruncate(int fd, off_t length) {
    if (!realfs.ftruncate) {
        realfs.ftruncate = load_symbol("ftruncate");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_ftruncate(&fds[fd], length);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.ftruncate(fd, length);
}

int ftruncate64(int fd, off_t length) {
    if (!realfs.ftruncate64) {
        realfs.ftruncate64 = load_symbol("ftruncate64");
    }
    if (realfs.initialized > 0 && fds[fd].file) {
        return hookfs_ftruncate(&fds[fd], length);
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.ftruncate64(fd, length);
}

int access(const char * path, int mode) {
    char abspath[PATH_MAX];
    if (!realfs.access) {
        realfs.access = load_symbol(("access"));
    }
    if (realfs.initialized > 0 && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return blobfs2_access(g_fs, g_channel, abspath, mode);
    }
    return realfs.access(path, mode);
}
