#!/bin/bash

rm -rf tmp; mkdir -p tmp
BUILD=`pwd`/tmp

# we need leveldb and rocksdb shared lib

cd ./leveldb; make clean
make -j4 && cd .. || exit 1

cp -r leveldb/include/leveldb ${BUILD}
cp -f leveldb/libleveldb.* ${BUILD}

cd ./rocksdb; make clean
make -j4 shared_lib && cd .. || exit 1

cp -rf rocksdb/include/rocksdb ${BUILD}
cp -f rocksdb/librocksdb.* ${BUILD}

cd ./levigo
CGO_CFLAGS="-I${BUILD}" CGO_LDFLAGS="-L${BUILD} -lleveldb -lsnappy" go install ./
cd .. || exit 1

cd ./gorocks
CGO_CFLAGS="-I${BUILD}" CGO_LDFLAGS="-L${BUILD} -lrocksdb -lsnappy -llz4 -lbz2 -lz -lm" go install ./
cd .. || exit 1
