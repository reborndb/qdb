# qdb

A fast, high availability, fully Redis compatible store.

[![Build Status](https://travis-ci.org/reborndb/qdb.svg?branch=master)](https://travis-ci.org/reborndb/qdb)

## Redis Command Support

See [commands](https://github.com/reborndb/qdb/wiki/Redis-Commands-Support) for more.

## Install 

### Install godep

```
go get github.com/tools/godep
```

We will use godep to build our application, the godep will be installed in `$GOPATH/bin`, you may append it to your system path, e.g, `$PATH=$PATH:$GOPATH/bin`.

### Install backend engine support

+ You must first install [Snappy](https://github.com/google/snappy) library.
+ Install Rocksdb and LevelDB
```
   cd extern
   ./engine_install.sh 
```

    engine_install.sh will check whether rocksdb/leveldb is installed first, if not, it will install rocksdb/leveldb in standard path `/usr/local/lib`.

    You may use `sudo` to run this script for permisson.

+ Install go wrapper library for Rocksdb and LevelDB.
```
    cd extern
    ./golib_install.sh
```

## Install qdb and run

+ `make`, it will install qdb-server in `./bin`.
+ run `qdb-server` with specifed config file
```
    $ qdb-server -c conf/config.toml -n 4 --create
```

## Benchmark
```
OS:   Ubuntu SMP x86_64 GNU/Linux
CPU:  Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz(8 cores)
Mem:  16G
Disk: 256G SSD
```

```
redis-benchmark against qdb-server (default config, see conf/config.toml)

$ redis-benchmark -q -t set,get,incr,lpush,lpop,sadd,spop,lpush,lrange -c 100 -p 6380 -r 1000 -n 100000
SET: 19516.00 requests per second
GET: 37979.49 requests per second
INCR: 18875.05 requests per second
LPUSH: 26730.82 requests per second
LPOP: 22862.37 requests per second
SADD: 27012.43 requests per second
SPOP: 21547.08 requests per second
LPUSH (needed to benchmark LRANGE): 24906.60 requests per second
LRANGE_100 (first 100 elements): 1492.94 requests per second
LRANGE_300 (first 300 elements): 552.51 requests per second
LRANGE_500 (first 450 elements): 414.38 requests per second
LRANGE_600 (first 600 elements): 319.44 requests per second
```

