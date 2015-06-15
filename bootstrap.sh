#!/bin/sh

FLAG=""

if [ "$1ok" = "forceok" ]; then
    FLAG="-u"
fi

echo "installing extern leveldb and rocksdb"
cd ./extern
./setup.sh || exit $?
cd ..

make clean

echo "downloading dependcies, it may take a few minutes..."

go get ${FLAG} github.com/BurntSushi/toml
go get ${FLAG} github.com/docopt/docopt-go
go get ${FLAG} github.com/syndtr/goleveldb/leveldb
go get ${FLAG} github.com/syndtr/goleveldb/leveldb/filter
go get ${FLAG} github.com/syndtr/goleveldb/leveldb/iterator
go get ${FLAG} github.com/syndtr/goleveldb/leveldb/opt
go get ${FLAG} github.com/syndtr/goleveldb/leveldb/storage
go get ${FLAG} github.com/syndtr/goleveldb/leveldb/util
go get ${FLAG} gopkg.in/check.v1

go get ${FLAG} github.com/reborndb/go/atomic2
go get ${FLAG} github.com/reborndb/go/bytesize
go get ${FLAG} github.com/reborndb/go/errors
go get ${FLAG} github.com/reborndb/go/io/ioutils
go get ${FLAG} github.com/reborndb/go/io/pipe
go get ${FLAG} github.com/reborndb/go/log
go get ${FLAG} github.com/reborndb/go/pools
go get ${FLAG} github.com/reborndb/go/redis/handler
go get ${FLAG} github.com/reborndb/go/redis/rdb
go get ${FLAG} github.com/reborndb/go/redis/resp
go get ${FLAG} github.com/reborndb/go/ring
go get ${FLAG} github.com/reborndb/go/sync2

make || exit $?
make gotest
