all: build

build: build-elasticsearch

build-elasticsearch:
	go build -o bin/go-mysql-sync ./cmd/go-mysql-sync

test:
	go test -timeout 1m --race ./...

clean:
	go clean -i ./...
	@rm -rf bin