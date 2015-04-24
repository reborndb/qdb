all: build

build:
	@mkdir -p bin
	go build -o bin/redis-binlog ./cmd && ./bin/redis-binlog -c conf/config.toml -n 4 

clean:
	rm -rf bin/* var/*

gotest:
	go test -cover -v ./pkg/... ./cmd/...
