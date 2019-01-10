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

static struct spdk_io_channel * g_channel;
static struct spdk_filesystem * g_fs;

static char * g_bdev_name;
static char g_mountpoint[PATH_MAX];
static int g_mountpoint_strlen;
static char * g_logstr;
static struct spdk_bs_dev * g_bs_dev;
static struct spdk_file ** files;
static uint64_t * offsets;
static long max_open;

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
    ssize_t (*read)(int, void *, size_t);
    ssize_t (*write)(int, const void *, size_t);
    ssize_t (*pread)(int, void *, size_t, off_t);
    ssize_t (*pwrite)(int, const void *, size_t, off_t);
    off_t (*lseek)(int, off_t, int);
    int (*close)(int);
    int (*__xstat)(int ver, const char *, struct stat *);
    int (*__lxstat)(int ver, const char *, struct stat *);
    int (*__fxstat)(int ver, int fd, struct stat *);
//    int (*fstat)(int, struct stat *);
    int (*posix_fadvise)(int, off_t, off_t, int);
    DIR * (*opendir)(const char *);
    int (*closedir)(DIR *);
    struct dirent * (*readdir)(DIR *);
    int (*mkdir)(const char *, mode_t);
} realfs = {.initialized = 0};


static int hookfs_mkdir(const char * abspath, mode_t m);

static void
start_hookfs_fn(void *arg1, void *arg2)
{
    int rc;
    char dummy[PATH_MAX];
    snprintf(dummy, PATH_MAX - 1, "%s/", g_mountpoint);
    hookfs_thread2 = pthread_self();

    realfs.initialized = 1;
    spdk_smp_rmb();
    rc = hookfs_mkdir(dummy, 0755);
    if (rc != 0 && errno != EEXIST) {
        SPDK_ERRLOG("FS init failed: failed to create %s\n", g_mountpoint);
        realfs.initialized = -1;
    }
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
    pthread_kill(hookfs_thread2, SIGINT);
    pthread_kill(hookfs_thread3, SIGINT);
    spdk_fs_free_io_channel(g_channel);
    spdk_app_stop(0);
}

static void
hookfs_shutdown(void)
{
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
    opts.mem_size = 6144;
    opts.shutdown_cb = hookfs_shutdown;
    opts.hugepage_single_segments = 1;

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
    files = malloc(sizeof(struct spdk_file *) * max_open);
    if (!files) {
        SPDK_ERRLOG("malloc(%ld) failed\n", sizeof(struct spdk_file *) * max_open);
        exit(1);
    }
    spdk_mem_all_zero(files, sizeof(struct spdk_file *) * max_open);

    offsets = malloc(sizeof(uint64_t) * max_open);
    if (!offsets) {
        SPDK_ERRLOG("malloc(%ld) failed\n", sizeof(uint64_t) * max_open);
        exit(1);
    }
    spdk_mem_all_zero(offsets, sizeof(uint64_t) * max_open);

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
        SPDK_ERRLOG("init!!!!!\n");
    }
}

__attribute__((destructor, used)) void hookfs_fini(void);
void hookfs_fini(void) {
}

static bool hookfs_is_under_mountpoint(const char * abspath) {
    return strncmp(abspath, g_mountpoint, g_mountpoint_strlen) == 0;
}

static int hookfs_open(const char * abspath, int oflag, int mflag) {
    struct spdk_file * file;
    int rc, fd;
    const char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;


    fd = realfs.open("/dev/null", oflag, mflag);
    if (fd < 0) {
        SPDK_ERRLOG("obtaining a FD failed: %d (%s)\n", errno, spdk_strerror(errno));
        return fd;
    }

    rc = spdk_fs_open_file(g_fs, g_channel, path, oflag, &file);
    SPDK_ERRLOG("open: %s (%s), %d\n", abspath, path, rc);
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

    va_start(args, oflag);
    mflag = va_arg(args, int);

    if (!realfs.open) {
        realfs.open = load_symbol("open");
    }
    if (realfs.initialized && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_open(abspath, oflag, mflag);
    }

//    SPDK_ERRLOG(">>>>> in open: %s <<<<<<\n", path);
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
    if (!realfs.close) {
        realfs.close = load_symbol("close");
    }
    if (realfs.initialized && files[fd]) {
        return hookfs_release(fd);
    }
//    SPDK_ERRLOG(">>>>> in close <<<<<<\n");
    return realfs.close(fd);
}

static int hookfs_read(int fd, char * buf, size_t len, uint64_t offset)
{
    return (int) spdk_file_read(files[fd], g_channel, buf, offset, len);
}

ssize_t read(int fd, void * buf, size_t count) {
    if (!realfs.read) {
        realfs.read = load_symbol("read");
    }

    if (realfs.initialized && files[fd]) {
        int len = hookfs_read(fd, buf, count, offsets[fd]);
        if (len > 0) {
            offsets[fd] += len;
        }
        return len;
    }
//    SPDK_ERRLOG(">>>>> in read <<<<<\n");
    return realfs.read(fd, buf, count);
}

ssize_t pread(int fd, void * buf, size_t count, off_t offset) {
    if (!realfs.pread) {
        realfs.pread = load_symbol("pread");
    }
    if (realfs.initialized && files[fd]) {
        return hookfs_read(fd, buf, count, offset);
    }
//    SPDK_ERRLOG(">>>>> in pread <<<<<\n");
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
    if (!realfs.write) {
        realfs.write = load_symbol("write");
    }
    if (realfs.initialized && files[fd]) {
        int len = hookfs_write(fd, buf, count, offsets[fd]);
        if (len > 0) {
            offsets[fd] += len;
        }
        return len;
    }
//    SPDK_ERRLOG(">>>>> in write <<<<<\n");
    return realfs.write(fd, buf, count);
}

ssize_t pwrite(int fd, const void * buf, size_t count, off_t offset) {
    if (!realfs.pwrite) {
        realfs.pwrite = load_symbol("pwrite");
    }

    if (realfs.initialized && files[fd]) {
        return hookfs_write(fd, buf, count, offset);
    }
//    SPDK_ERRLOG(">>>>> in pwrite <<<<<\n");
    return realfs.pwrite(fd, buf, count, offset);
}

static DIR * hookfs_opendir(const char * name);
static struct dirent * hookfs_readdir(DIR * _dirp);
static int hookfs_closedir(DIR * _dirp);

static int hookfs_is_dir(const char * abspath)
{
    char * duppath = strdup(abspath);
    char * parent = dirname(duppath);
    char * duppath2 = strdup(abspath);
    char * base = basename(duppath2);
    int rc;
    DIR * dir;
    struct dirent * d;

    if (strlen(abspath) == 1 && *abspath == '/') {
        free(duppath);
        free(duppath2);
        return 1;
    }

    dir = hookfs_opendir(parent);
    if (!dir) {
        free(duppath);
        free(duppath2);
        SPDK_ERRLOG("orphan file?: %s\n", abspath);
        return 0;
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

static int hookfs_stat(const char * abspath, struct stat * stbuf)
{
    struct spdk_file_stat stat;
    int rc;
    const char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;

    rc = spdk_fs_file_stat(g_fs, g_channel, path, &stat);
    SPDK_ERRLOG(">>>>> in stat:%s (%s), %d <<<<<\n", abspath, path, rc);
    if (rc == 0) {
        stbuf->st_mode = (!hookfs_is_dir(abspath) ? S_IFREG : S_IFDIR) | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = stat.size;
    }

    return rc;
}

int __xstat(int ver, const char * path, struct stat * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__xstat) {
        realfs.__xstat = load_symbol("__xstat");
    }

//  SPDK_ERRLOG(">>>>> in __xstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    normalizepath(path, abspath);
    if (realfs.initialized && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_stat(abspath, stbuf);
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__xstat(ver, path, stbuf);
}

int __lxstat(int ver, const char * path, struct stat * stbuf) {
    char abspath[PATH_MAX];
    if (!realfs.__lxstat) {
        realfs.__lxstat = load_symbol("__lxstat");
    }

    normalizepath(path, abspath);
 //   SPDK_ERRLOG(">>>>> in __lxstat:%s,%s,%d <<<<<\n", path, abspath, hookfs_is_under_mountpoint(abspath));
    if (realfs.initialized && !normalizepath(path, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_stat(abspath, stbuf);
    }

//    SPDK_ERRLOG(">>>>> in stat <<<<<\n");
    return realfs.__lxstat(ver, path, stbuf);

}

int __fxstat(int ver, int fd, struct stat * stbuf) {
    if (!realfs.__fxstat) {
        realfs.__fxstat = load_symbol("__fxstat");
    }
/*    if (realfs.initialized && files[fd]) {
        return stat(spdk_file_get_name(files[fd]), stbuf);
    }*/
//    SPDK_ERRLOG(">>>>> in __fxstat <<<<<\n");
    return realfs.__fxstat(ver, fd, stbuf);
}

off_t lseek(int fd, off_t offset, int whence) {
    struct stat stbuf;
    if (!realfs.lseek) {
        realfs.lseek = load_symbol("lseek");
    }
    if (realfs.initialized && files[fd]) {
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
//    SPDK_ERRLOG(">>>>> in lseek <<<<<\n");
    return realfs.lseek(fd, offset, whence);
}

int posix_fadvise(int fd, off_t offset, off_t len, int advice) {
    if (!realfs.posix_fadvise) {
        realfs.posix_fadvise = load_symbol("posix_fadvise");
    }
 
    if (realfs.initialized && files[fd]) {
        return 0;
    }
//    SPDK_ERRLOG(">>>>> in posix_fadvise <<<<<\n");
    return realfs.posix_fadvise(fd, offset, len, advice);
}

typedef struct hookfs_dir {
    int __dd_fd;    /* file descriptor associated with directory */
    long    __dd_loc;   /* offset in current buffer */
    long    __dd_size;  /* amount of data returned */
    char    *__dd_buf;  /* data buffer */
    int __dd_len;   /* size of data buffer */
    long    __dd_seek;  /* magic cookie returned */
    int __dd_flags; /* flags for readdir */
} hookfs_dir_t;


static DIR * hookfs_opendir(const char * name) {
    struct spdk_file_stat stat;
    hookfs_dir_t * ret;
    const char * path = (*(name + g_mountpoint_strlen) == 0) ? "/": name + g_mountpoint_strlen;
    int rc;

    ret = malloc(sizeof(hookfs_dir_t));
    if (!ret) {
        errno = ENOMEM;
        return NULL;
    }

    rc = spdk_fs_file_stat(g_fs, g_channel, path, &stat);
    SPDK_ERRLOG("opendir: %s (%s), %d\n", name, path, rc);
    if (rc != 0) {
        errno = ENOENT;
        goto freedir;
    }
    ret->__dd_size = stat.size - 1;
    ret->__dd_flags = 0777;

    ret->__dd_len = HOOKFS_PAGE_SIZE * 2;
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

    return (DIR *)ret;

freebuf:
    free(ret->__dd_buf);
freedir:
    free(ret);
    return NULL;
}

DIR * opendir(const char * name) {
    char abspath[PATH_MAX];

    if (!realfs.opendir) {
        realfs.opendir = load_symbol("opendir");
    }
 
//    SPDK_ERRLOG(">>>>> in opendir:%s <<<<<\n", name);
    if (realfs.initialized && !normalizepath(name, abspath) && hookfs_is_under_mountpoint(abspath)) {
        return hookfs_opendir(abspath);
    }

//    SPDK_ERRLOG(">>>>> in opendir:%s <<<<<\n", name);
    return realfs.opendir(name);
}

static struct dirent * hookfs_readdir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    struct dirent * ret = (struct dirent *)(dirp->__dd_buf + HOOKFS_PAGE_SIZE);

    if (dirp->__dd_seek >= dirp->__dd_size) {
        return NULL;
    }
    if (dirp->__dd_seek >= dirp->__dd_loc) {
        long rc = spdk_file_read(files[dirp->__dd_fd], g_channel, dirp->__dd_buf, dirp->__dd_loc, HOOKFS_PAGE_SIZE);
        if (rc < 0) {
            goto freedir;
        }
        dirp->__dd_loc += rc;
    }
    memset(ret, 0, sizeof(struct dirent));
    ret->d_type = *(unsigned char *)(dirp->__dd_buf + dirp->__dd_seek);
    strcpy(ret->d_name, dirp->__dd_buf + dirp->__dd_seek + sizeof(unsigned char));
    SPDK_ERRLOG("readdir: %s, %d, %d\n", ret->d_name, ret->d_type, dirp->__dd_seek);
    dirp->__dd_seek += strlen(ret->d_name) + sizeof(unsigned char) + 1;

    return ret;

freedir:
    free(ret);
    return NULL;
}

struct dirent * readdir(DIR * _dirp) {
    hookfs_dir_t * dirp = (hookfs_dir_t *)_dirp;
    if (!realfs.readdir) {
        realfs.readdir = load_symbol("readdir");
    }
 
    if (realfs.initialized && files[dirp->__dd_fd]) {
        return hookfs_readdir(_dirp);
    }
//    SPDK_ERRLOG(">>>>> in readdir <<<<<\n");
    return realfs.readdir(_dirp);
}

static int __hookfs_addfile(const char * path, const char *filename, unsigned char d_type) {
    struct spdk_file *file;
    int rc;
    char buf[PATH_MAX + 2];
    struct spdk_file_stat stat;

    if (strlen(filename) == 0 || strlen(path) == 0 || !(d_type == DT_DIR || d_type == DT_REG)) {
        SPDK_ERRLOG("cannot add file %s, d_type=%u in %s\n", filename, d_type, path);
        return -1;
    }

    rc = spdk_fs_file_stat(g_fs, g_channel, path, &stat);
    if (rc == -ENOENT) {
        rc = spdk_fs_create_file(g_fs, g_channel, path);
        if (rc != 0) {
            SPDK_ERRLOG("failed to create file: %s, %d\n", path, rc);
            return -1;
        }
        stat.size = 0;
    }

    rc = spdk_fs_open_file(g_fs, g_channel, path, 0, &file);
    if (rc != 0) {
        return -rc;
    }

    snprintf(buf, PATH_MAX - 1, "%c%s%c", (char)d_type, filename, 0);
    SPDK_ERRLOG("addfile: %s, %d, %d, %d\n", filename, d_type, stat.size, strlen(buf) + 1);
    rc = spdk_file_write(file, g_channel, (void *)buf, stat.size, strlen(buf) + 1);
    if (rc != 0) {
        spdk_file_close(file, g_channel);
        return -rc;
    }

    rc = spdk_file_close(file, g_channel);
    if (rc != 0) {
        return -rc;
    }
    return 0;
}

static int hookfs_mkdir(const char * abspath, mode_t m) {
    struct spdk_file_stat stat, pstat;
    char * path = (*(abspath + g_mountpoint_strlen) == 0) ? "/": abspath + g_mountpoint_strlen;
    char * duppath = strdup(path);
    char * parent = dirname(duppath);
    int rc;

    SPDK_ERRLOG("mkdir: %s (%s) under %s\n", path, abspath, parent);
    if (strcmp(path, parent) != 0) {
        rc = spdk_fs_file_stat(g_fs, g_channel, parent, &pstat);
        SPDK_ERRLOG("check: %s, %d\n", parent, rc);
        if (rc != 0) {
            errno = EACCES;
            free(duppath);
            return -1;
        }
    }

    rc = spdk_fs_file_stat(g_fs, g_channel, path, &stat);
    if (rc == -ENOENT) {
        char * duppath2 = strdup(path);
        char * base = basename(duppath2);

        SPDK_ERRLOG("mkdir: %s\n", path);

        if (__hookfs_addfile(path, ".", DT_DIR)) {
            SPDK_ERRLOG("failed to create '.' in %s\n", path);
            free(duppath);
            free(duppath2);
            return -1;
        }
        if (__hookfs_addfile(path, "..", DT_DIR)) {
            SPDK_ERRLOG("failed to create '..' in %s\n", path);
            free(duppath);
            free(duppath2);
            return -1;
        }

        if (strcmp(path, parent) == 0) {
            free(duppath);
            free(duppath2);
            return 0;
        }

        if (__hookfs_addfile(parent, base, DT_DIR)) {
            SPDK_ERRLOG("failed to create '%s' in %s\n", base, path);
            free(duppath);
            free(duppath2);
            return -1;
        }
        free(duppath);
        free(duppath2);
        return 0;
    }
    errno = EEXIST;
    free(duppath);
    return -1;
}

int mkdir(const char * name, mode_t m) {
    char abspath[PATH_MAX];

    if (!realfs.mkdir) {
        realfs.mkdir = load_symbol("mkdir");
    }

    if (realfs.initialized && !normalizepath(name, abspath) && hookfs_is_under_mountpoint(abspath)) {
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

    if (realfs.initialized && files[dirp->__dd_fd]) {
        return hookfs_closedir(_dirp);
    }
//    SPDK_ERRLOG(">>>>> in closedir <<<<<\n");
    return realfs.closedir(_dirp);
}

#if 0
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

#endif

