#!/bin/bash

echo "setup start!"

rm -rf tmp; mkdir -p tmp
BUILD=`pwd`/tmp

cd ./leveldb; make clean
make -j4 && cd .. || exit 1

cp -r leveldb/include/leveldb ${BUILD}
cp leveldb/libleveldb.a ${BUILD}
cp leveldb/libleveldb.so.1.18 ${BUILD}

cd ./rocksdb; make clean
make -j4 static_lib shared_lib && cd .. || exit 1

cp -rf rocksdb/include/rocksdb ${BUILD}
cp rocksdb/librocksdb.a ${BUILD}
cp rocksdb/librocksdb.so ${BUILD}

cd ./tmp
ln -s libleveldb.so.1.18 libleveldb.so.1
ln -s libleveldb.so.1.18 libleveldb.so
cd .. || exit 1

cd ./levigo
CGO_CFLAGS="-I${BUILD}" CGO_LDFLAGS="-L${BUILD} -lleveldb -lsnappy -lstdc++" go install ./
cd .. || exit 1

cd ./gorocks
CGO_CFLAGS="-I${BUILD}" CGO_LDFLAGS="-L${BUILD} -lrocksdb -lsnappy -llz4 -lbz2 -lz -lm -lstdc++" go install ./
cd .. || exit 1

echo "setup success!"
