#!/bin/bash
POSIXFSLIB=/home/tyos/data/src/spdk/build/lib/libposixfs.so

if [ "$HOSTNAME" = "pcloud23" ]; then
export HOOKFS_SPDK_CONF=/home/tyos/data/src/mybench/capispdk/spdkconf/pcloud23/capi_spdk.conf
export HOOKFS_BDEV=CAPI0
else
export HOOKFS_SPDK_CONF=/home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/nvme_spdk.conf
export HOOKFS_BDEV=Nvme2n1
fi
export HOOKFS_MOUNT_POINT=/spdk
#export HOOKFS_LOG_ARG="thread"
#export HOOKFS_LOG_ARG="all"
#export LD_DEBUG=all
export LD_PRELOAD=$POSIXFSLIB
$@
