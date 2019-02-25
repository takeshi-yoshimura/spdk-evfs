#!/bin/bash
if [ "$HOSTNAME" = "pcloud23" ]; then
/home/tyos/data/src/spdk/test/blobfs/mkfs/mkfs /home/tyos/data/src/mybench/capispdk/spdkconf/pcloud23/capi_spdk.conf CAPI0
else
/home/tyos/data/src/spdk/test/blobfs/mkfs/mkfs /home/tyos/data/src/mybench/capispdk/spdkconf/s7fp1tyos/nvme_spdk.conf Nvme2n1
fi
