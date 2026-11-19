MODULE := github.com/franciscovmn/middlewarePubSub
PROTOS := $(wildcard proto/*.proto)

.PHONY: proto test vet build imagem ensaio

proto:
	protoc --proto_path=proto --go_out=. --go_opt=module=$(MODULE) $(PROTOS)

test:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -o bin/broker ./cmd/broker
	go build -o bin/sonda ./cmd/sonda

imagem:
	scripts/ensaio.sh construir

# Exemplo: make ensaio CENARIO="3 explicito"
ensaio:
	scripts/ensaio.sh cenario $(CENARIO)
