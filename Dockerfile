# Imagem única dos ensaios: broker, sonda e as ferramentas de injeção de
# falhas de rede (iptables e tc).
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/broker ./cmd/broker \
 && CGO_ENABLED=0 go build -trimpath -o /out/sonda ./cmd/sonda

FROM alpine:3.20
RUN apk add --no-cache iptables iproute2
COPY --from=build /out/broker /out/sonda /usr/local/bin/
COPY scripts/rede.sh /usr/local/bin/rede
RUN chmod +x /usr/local/bin/rede
EXPOSE 7000/tcp 7001/udp 7002/tcp
ENTRYPOINT []
CMD ["broker", "-help"]
