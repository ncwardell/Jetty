# Build stage - use BUILDPLATFORM for native compilation speed
# Default to linux/amd64 for legacy docker build compatibility
ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} golang:1.24-bookworm AS builder
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Sync web UI and build with version injected via ldflags
# Cross-compile for target architecture
RUN go generate ./... && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-X github.com/ncwardell/jetty/agent.Version=${VERSION}" -o jetty .

# Runtime stage - using Debian for cloudflare-warp compatibility
FROM debian:bookworm-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    wireguard-tools \
    iptables \
    nftables \
    iproute2 \
    procps \
    dbus \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y docker-ce-cli docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/*

# Install Cloudflare WARP
RUN curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | gpg --dearmor -o /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ bookworm main" > /etc/apt/sources.list.d/cloudflare-client.list && \
    apt-get update && \
    apt-get install -y cloudflare-warp && \
    rm -rf /var/lib/apt/lists/*

# Install cloudflared (Cloudflare Tunnel)
RUN curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | gpg --dearmor -o /usr/share/keyrings/cloudflare-main.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared bookworm main" > /etc/apt/sources.list.d/cloudflared.list && \
    apt-get update && \
    apt-get install -y cloudflared && \
    rm -rf /var/lib/apt/lists/*

# Pre-accept WARP TOS
RUN mkdir -p /root/.local/share/warp && \
    echo 'yes' > /root/.local/share/warp/accepted-tos.txt

# Copy Jetty binary
COPY --from=builder /app/jetty /usr/local/bin/jetty

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create data directory
RUN mkdir -p /data
ENV JETTY_DATA_DIR=/data

# Expose ports
EXPOSE 8080
EXPOSE 51820/udp

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["jetty"]
