MODULE := github.com/franciscovmn/middlewarePubSub
PROTOS := $(wildcard proto/*.proto)

.PHONY: proto test vet build

proto:
	protoc --proto_path=proto --go_out=. --go_opt=module=$(MODULE) $(PROTOS)

test:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -o bin/broker ./cmd/broker
