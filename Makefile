all: build 

container_build: build
	podman build -t api-server:latest .

build: builddir
	GOOS=linux GOARCH=amd64 go build -o build/api-server cmd/api-server/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
