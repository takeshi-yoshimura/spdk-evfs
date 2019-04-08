#!/bin/bash
POSIXFSLIB=/home/tyos/data/src/spdk/build/lib/libposixfs.so

CONF=/home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/pmem_spdk.conf
BDEV=Pmem0
MOUNT_POINT=/spdk
#export HOOKFS_LOG_ARG="thread"
#export HOOKFS_LOG_ARG="all"
#export LD_DEBUG=all
#gdb --args env LD_PRELOAD=/home/tyos/data/src/pmdk/src/debug/libpmem.so:/home/tyos/data/src/pmdk/src/debug/libpmemblk.so:$POSIXFSLIB HOOKFS_SPDK_CONF=$CONF HOOKFS_MOUNT_POINT=$MOUNT_POINT HOOKFS_BDEV=$BDEV $@
gdb --args env LD_PRELOAD=$POSIXFSLIB HOOKFS_SPDK_CONF=$CONF HOOKFS_MOUNT_POINT=$MOUNT_POINT HOOKFS_BDEV=$BDEV $@
