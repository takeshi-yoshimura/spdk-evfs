#!/bin/bash
SPDKDIR=/home/tyos/data/src/spdk/build/lib
DPDKDIR=/home/tyos/data/src/dpdk/build/lib
OUTDIR=/home/tyos/data/src/spdk
cmd="gcc -shared -o $OUTDIR/libposixfs.so -L$DPDKDIR -Wl,--whole-archive -L$SPDKDIR $SPDKDIR/libspdk_posixfs.a -ldl -Wl,--no-whole-archive `find $DPDKDIR -name '*.a' | grep -v vhost` `find $SPDKDIR -name '*.a' | grep -v posixfs`"
echo $cmd
$cmd
