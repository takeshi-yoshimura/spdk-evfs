#!/bin/bash
POSIXFSLIB=/home/tyos/data/src/spdk/build/lib/libposixfs.so

CONF=/home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/nvme_spdk.conf
BDEV=Nvme2n1
MOUNT_POINT=/spdk
#export HOOKFS_LOG_ARG="thread"
#export HOOKFS_LOG_ARG="all"
#export LD_DEBUG=all
gdb --args env LD_PRELOAD=$POSIXFSLIB HOOKFS_SPDK_CONF=$CONF HOOKFS_MOUNT_POINT=$MOUNT_POINT HOOKFS_BDEV=$BDEV $@
