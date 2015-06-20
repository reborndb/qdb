#!/bin/bash

# print every running process 
set -x

BUILD_PATH=`pwd`

# We will install rocksdb and leveldb in standard path
INSTALL_PATH=/usr/local

CXX=gcc

# Test whether Snappy library is installed
# We must install Snappy
# http://code.google.com/p/snappy/
    $CXX -x c++ - -o /dev/null 2>/dev/null  <<EOF
      #include <snappy.h>
      int main() {}
EOF
    if [ "$?" -ne 0 ]; then
       echo "You must install Snappy library"
       exit 1
    fi

# Test whether LevelDB library is installed
    $CXX -x c++ - -o /dev/null -lleveldb 2>/dev/null  <<EOF
      #include <leveldb/c.h>
      int main() {
        leveldb_major_version();  
      }
EOF
    if [ "$?" -ne 0 ]; then
        cd $BUILD_PATH/leveldb && make -j4 
        cp -rf $BUILD_PATH/leveldb/include/leveldb $INSTALL_PATH/include 
        cp -f $BUILD_PATH/leveldb/libleveldb.* $INSTALL_PATH/lib
    fi

# Test whether LevelDB library is installed
    $CXX -x c++ - -o /dev/null -lrocksdb 2>/dev/null  <<EOF
      #include <rocksdb/c.h>
      int main() {
        rocksdb_options_create();
      }
EOF
    if [ "$?" -ne 0 ]; then
        cd $BUILD_PATH/rocksdb && make -j4 shared_lib static_lib
        cp -rf $BUILD_PATH/rocksdb/include/rocksdb $INSTALL_PATH/include 
        cp -f $BUILD_PATH/rocksdb/librocksdb.* $INSTALL_PATH/lib
    fi

cd $BUILD_PATH/levigo && go clean -i ./ && CGO_LDFLAGS="-lsnappy" go install ./

cd $BUILD_PATH/gorocks && go clean -i ./ && CGO_LDFLAGS="-lsnappy" go install ./

cd $BUILD_PATH