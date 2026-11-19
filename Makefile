MODULE := github.com/franciscovmn/middlewarePubSub
PROTOS := $(wildcard proto/*.proto)

.PHONY: proto test vet

proto:
	protoc --proto_path=proto --go_out=. --go_opt=module=$(MODULE) $(PROTOS)

test:
	go test -race ./...

vet:
	go vet ./...
