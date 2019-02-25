#!/bin/bash
cur=$(realpath `dirname $0`)
SPDKDIR=/home/tyos/data/src/spdk/build/lib
DPDKDIR=/home/tyos/data/src/dpdk/build/lib
NUMADIR=/home/tyos/data/src/numactl/.libs  # ar r libnuma.a libnuma.o syscall.o distance.o affinity.o sysfs.o rtnetlink.o
OUTDIR=/home/tyos/data/src/spdk/build/lib
if [ "$HOSTNAME" = "pcloud23" ]; then
CXLDIR=/home/tyos/data/bin/libcxlflash
DEPS_CAPI=-lspdk_bdev_capi -lspdk_bdev_cxlflash
DEPS_CAPI2=-L$CXLDIR/lib -lcxlflash -lcxlflash_compat
fi
DEPS_SPDK="-lspdk_log -lspdk_trace -lspdk_util -lspdk_json -lspdk_rpc -lspdk_jsonrpc -lspdk_conf -lspdk_env_dpdk -lspdk_thread -lspdk_event -lspdk_lvol -lspdk_bdev -lspdk_copy -lspdk_event_copy -lspdk_event_bdev -lspdk_blob -lspdk_blob_bdev -lspdk_bdev_null -lspdk_bdev_error -lspdk_bdev_raid -lspdk_bdev_rpc -lspdk_vbdev_lvol -lspdk_nvme -lspdk_vbdev_gpt"
#DEPS_SPDK=`find $SPDKDIR -name '*.a' | grep -v vbdev | grep -v posixfs | grep -v bdev_nvme`
DEPS_DPDK="-Wl,--version-script,$cur/versions.ldscript -Wl,--whole-archive -Wl,-Bstatic -L$NUMADIR -lnuma -Wl,-Bdynamic -Wl,--no-whole-archive -Wl,--start-group -Wl,--whole-archive -L$DPDKDIR -lrte_eal -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_pci -lrte_bus_pci -Wl,--end-group -Wl,--no-whole-archive"

#cmd="gcc -fPIC -Wl,'--no-as-needed' -shared -o $OUTDIR/libposixfs.so $DEPS_DPDK -Wl,--whole-archive -L$SPDKDIR $DEPS_SPDK -lspdk_bdev_nvme -lspdk_blobfs -lspdk_posixfs -Wl,--no-whole-archive -luuid -lrt -lpthread -ldl"
cmd="gcc -fPIC -shared -o $OUTDIR/libposixfs.so $DEPS_DPDK -Wl,--whole-archive -L$SPDKDIR $DEPS_SPDK $DEPS_CAPI -lspdk_bdev_nvme -lspdk_blobfs -lspdk_posixfs -Wl,--no-whole-archive $DEPS_CAPI2 -luuid -lrt -lpthread -ldl"
echo $cmd
$cmd
