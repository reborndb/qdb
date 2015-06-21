# qdb

A fast, high availability, fully Redis compatible store.

[![Build Status](https://travis-ci.org/reborndb/qdb.svg?branch=master)](https://travis-ci.org/reborndb/qdb)

## Redis Command Support

See [commands](./doc/commands.md) for more.

## Install 

### Install backend engine support

+ You must first install Snappy library.
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

    redis-benchmark against qdb-server (default config, see conf/config.toml)

    $ ./redis-benchmark -q -t set,get,incr,lpush,lpop,sadd,spop,lpush,lrange -c 100 -p 6380 -r 1000 -n 100000
    SET: 31094.53 requests per second
    GET: 53361.79 requests per second
    INCR: 36049.03 requests per second
    LPUSH: 43346.34 requests per second
    LPOP: 37965.07 requests per second
    SADD: 40899.80 requests per second
    SPOP: 53333.33 requests per second
    LPUSH (needed to benchmark LRANGE): 40567.95 requests per second
    LRANGE_100 (first 100 elements): 3395.12 requests per second
    LRANGE_300 (first 300 elements): 1202.59 requests per second
    LRANGE_500 (first 450 elements): 849.21 requests per second
    LRANGE_600 (first 600 elements): 640.11 requests per second
