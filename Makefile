# if we install godep, we will use godep to build our application.
GODEP_PATH:=$(shell godep path 2>/dev/null)

GO=go 
ifdef GODEP_PATH
GO=godep go
endif

all: build

build:
	$(GO) build -tags 'all' -o bin/qdb-server ./cmd/qdb-server 

build_leveldb:
	$(GO) build -tags 'leveldb' -o bin/qdb-server ./cmd/qdb-server  

build_rocksdb:
	$(GO) build -tags 'rocksdb' -o bin/qdb-server ./cmd/qdb-server 

build_goleveldb:
	$(GO) build -o bin/qdb-server ./cmd/qdb-server  

clean:
	$(GO) clean -i ./...
	rm -rf bin/* var/*

run:
	$(GO) build -tags 'all' -o bin/qdb-server ./cmd/qdb-server  
	./bin/qdb-server -c conf/config.toml -n 4

gotest:
	$(GO) test --race -tags 'all' -cover -v ./pkg/... ./cmd/...
