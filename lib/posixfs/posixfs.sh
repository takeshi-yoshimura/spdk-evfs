#!/bin/bash
POSIXFSLIB=/home/tyos/data/src/spdk/build/lib/libposixfs.so

export HOOKFS_SPDK_CONF=/home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/nvme_spdk.conf
export HOOKFS_BDEV=Nvme2n1
export HOOKFS_MOUNT_POINT=/spdk
#export HOOKFS_LOG_ARG="thread"
export LD_PRELOAD=$POSIXFSLIB
$@
