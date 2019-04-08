#!/bin/bash
if [ "$HOSTNAME" = "pcloud23" ]; then
/home/tyos/data/src/spdk/test/blobfs/mkfs/mkfs /home/tyos/data/src/mybench/capispdk/spdkconf/pcloud23/capi_spdk.conf CAPI0
else
gdb --args env LD_LIBRARY_PATH=/home/tyos/data/src/pmdk/src/nondebug /home/tyos/data/src/spdk/test/blobfs/mkfs/mkfs /home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/mkfs_pmem.conf Pmem0
fi
