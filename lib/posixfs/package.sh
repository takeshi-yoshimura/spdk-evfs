#!/bin/bash
cur=$(realpath `dirname $0`)
SPDKDIR=/home/tyos/data/src/spdk/build/lib
DPDKDIR=/home/tyos/data/src/dpdk/build/lib
NUMADIR=/home/tyos/data/src/numactl/.libs  # ar r libnuma.a libnuma.o syscall.o distance.o affinity.o sysfs.o rtnetlink.o
OUTDIR=/home/tyos/data/src/spdk/build/lib
DEPS="-lspdk_trace -lspdk_rpc -lspdk_conf -lspdk_env_dpdk -lspdk_thread -lspdk_event -lspdk_bdev -lspdk_bdev_lvol -lspdk_bdev_gpt -lspdk_nvme"
DEPS_SPDK=`find $SPDKDIR -name '*.a' | grep -v vbdev | grep -v posixfs | grep -v bdev_nvme`
DEPS_DPDK=`find $DPDKDIR -name '*.a' | grep -v mempool | grep -v vhost`
DEPS_MEMPOOL="`find $DPDKDIR -name '*.a' | grep mempool` -lrte_eal"
DEPS_DPDK2="-Wl,--version-script,$cur/versions.ldscript -Wl,--whole-archive -Wl,-Bstatic -L$NUMADIR -lnuma -Wl,-Bdynamic -Wl,--no-whole-archive -Wl,--start-group -Wl,--whole-archive -L$DPDKDIR -lrte_eal -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_pci -lrte_bus_pci -Wl,--end-group -Wl,--no-whole-archive"
cmd="gcc -shared -o $OUTDIR/libposixfs.so -fPIC $DEPS_DPDK2 -Wl,--whole-archive -L$SPDKDIR $DEPS_SPDK -lspdk_bdev_nvme -lspdk_posixfs -Wl,--no-whole-archive -luuid -lrt -lpthread -ldl"
echo $cmd
$cmd
