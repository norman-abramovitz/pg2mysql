CGO_ENABLED=0

all: build

linux: test
	GOOS=linux GOARCH=amd64 go build -o pg2mysql_linux cmd/pg2mysql/main.go

build: test
	go build -o pg2mysql cmd/pg2mysql/main.go

test:
	go test

clean:
	rm pg2mysql_linux
