all: build

build:
	go build -tags 'all' -o bin/redis-binlog ./cmd  

build_leveldb:
	go build -tags 'leveldb' -o bin/redis-binlog ./cmd  

build_rocksdb:
	go build -tags 'rocksdb' -o bin/redis-binlog ./cmd  

build_goleveldb:
	go build -o bin/redis-binlog ./cmd  

clean:
	rm -rf bin/* var/*

run:
	./bin/redis-binlog -c conf/config.toml -n 4

gotest:
	go test -tags 'all' -cover -v ./pkg/... ./cmd/...
