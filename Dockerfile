FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o jetty .

FROM alpine:3.19
RUN apk add --no-cache \
    wireguard-tools \
    iptables \
    docker-cli \
    docker-cli-compose \
    ca-certificates \
    cloudflared

COPY --from=builder /app/jetty /usr/local/bin/jetty

RUN mkdir -p /data
ENV JETTY_DATA_DIR=/data

EXPOSE 8080
EXPOSE 51820/udp

VOLUME ["/data"]
ENTRYPOINT ["jetty"]
