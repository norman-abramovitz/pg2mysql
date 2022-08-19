CGO_ENABLED=0

all: build

linux:
	GOOS=linux GOARCH=amd64 go build -o pg2mysql_linux cmd/pg2mysql/main.go

build:
	go build -o pg2mysql cmd/pg2mysql/main.go

test: container
	docker run -v "$(shell pwd):/src" pg2mysql:test go test

container:
	docker build -t pg2mysql:test .

clean:
	rm -f pg2mysql_linux pg2mysql
