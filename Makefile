PWD=$(shell pwd)
export C_INCLUDE_PATH=${PWD}/extern/tmp
export LIBRARY_PATH=${PWD}/extern/tmp
export LD_LIBRARY_PATH=${PWD}/extern/tmp

all: build

build:
	go build -tags 'all' -o bin/qdb-server ./cmd/qdb-server 

build_leveldb:
	go build -tags 'leveldb' -o bin/qdb-server ./cmd/qdb-server  

build_rocksdb:
	go build -tags 'rocksdb' -o bin/qdb-server ./cmd/qdb-server 

build_goleveldb:
	go build -o bin/qdb-server ./cmd/qdb-server  

clean:
	rm -rf bin/* var/*

run:
	go build -tags 'all' -o bin/qdb-server ./cmd/qdb-server  
	./bin/qdb-server -c conf/config.toml -n 4

gotest:
	go test -tags 'all' ./pkg/... ./cmd/... -race -cover
