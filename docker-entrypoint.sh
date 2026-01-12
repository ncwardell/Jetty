#!/bin/bash
set -e

# Jetty Docker Entrypoint
# Handles WARP initialization and Jetty startup

log() {
    echo "[jetty-entrypoint] $1"
}

# Create TUN device if it doesn't exist
setup_tun() {
    if [ ! -e /dev/net/tun ]; then
        log "Creating TUN device..."
        mkdir -p /dev/net
        mknod /dev/net/tun c 10 200
        chmod 600 /dev/net/tun
    fi
}

# Start dbus (required for WARP)
start_dbus() {
    if [ ! -d /run/dbus ]; then
        mkdir -p /run/dbus
    fi
    if [ ! -f /run/dbus/pid ] || ! kill -0 $(cat /run/dbus/pid 2>/dev/null) 2>/dev/null; then
        log "Starting dbus..."
        dbus-daemon --system --fork 2>/dev/null || true
    fi
}

# Start WARP service daemon
start_warp_svc() {
    log "Starting WARP service..."
    warp-svc --accept-tos &
    WARP_SVC_PID=$!

    # Wait for WARP to be ready
    local max_wait=${WARP_STARTUP_WAIT:-10}
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if warp-cli --accept-tos status >/dev/null 2>&1; then
            log "WARP service is ready"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    log "Warning: WARP service may not be fully ready"
    return 0
}

# Configure WARP based on environment variables
configure_warp() {
    # Check if already registered
    local status=$(warp-cli --accept-tos registration show 2>&1 || true)

    if echo "$status" | grep -q "Missing registration"; then
        log "WARP not registered, configuring..."

        # WARP Connector mode (for Zero Trust site-to-site)
        if [ -n "$JETTY_WARP_CONNECTOR_TOKEN" ]; then
            log "Configuring WARP Connector mode..."
            warp-cli --accept-tos connector new "$JETTY_WARP_CONNECTOR_TOKEN"

        # Zero Trust / Teams enrollment
        elif [ -n "$JETTY_WARP_ORGANIZATION" ]; then
            log "Enrolling in Zero Trust organization: $JETTY_WARP_ORGANIZATION"
            # This typically requires interactive auth, but MDM can pre-configure
            warp-cli --accept-tos teams-enroll "$JETTY_WARP_ORGANIZATION" || {
                log "Warning: Teams enrollment may require interactive authentication"
            }

        # Consumer WARP (default)
        else
            log "Registering consumer WARP..."
            warp-cli --accept-tos registration new
        fi

        # Apply license key if provided
        if [ -n "$JETTY_WARP_LICENSE_KEY" ]; then
            log "Applying WARP license key..."
            warp-cli --accept-tos registration license "$JETTY_WARP_LICENSE_KEY" || true
        fi
    else
        log "WARP already registered"
    fi

    # Configure WARP mode
    if [ -n "$JETTY_WARP_MODE" ]; then
        log "Setting WARP mode to: $JETTY_WARP_MODE"
        warp-cli --accept-tos mode "$JETTY_WARP_MODE" || true
    fi

    # Connect WARP
    log "Connecting WARP..."
    warp-cli --accept-tos connect

    # Wait for connection
    local max_wait=30
    local waited=0
    while [ $waited -lt $max_wait ]; do
        local conn_status=$(warp-cli --accept-tos status 2>&1 || true)
        if echo "$conn_status" | grep -qi "connected"; then
            log "WARP connected successfully"

            # Get and display WARP IP
            local warp_ip=$(ip -4 addr show CloudflareWARP 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || true)
            if [ -n "$warp_ip" ]; then
                log "WARP IP: $warp_ip"
                export JETTY_WARP_IP="$warp_ip"
            fi
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    log "Warning: WARP connection may not be fully established"
}

# Main entrypoint logic
main() {
    # Check if WARP should be enabled
    if [ "$JETTY_WARP_ENABLED" = "true" ] || [ -n "$JETTY_WARP_CONNECTOR_TOKEN" ] || [ -n "$JETTY_WARP_ORGANIZATION" ]; then
        log "WARP mode enabled"

        setup_tun
        start_dbus
        start_warp_svc
        configure_warp

        log "WARP initialization complete"
    else
        log "WARP mode disabled (set JETTY_WARP_ENABLED=true or provide JETTY_WARP_CONNECTOR_TOKEN)"
    fi

    # Start Cloudflare Tunnel if token provided
    if [ -n "$JETTY_TUNNEL_TOKEN" ]; then
        log "Starting Cloudflare Tunnel..."
        cloudflared tunnel --no-autoupdate run --token "$JETTY_TUNNEL_TOKEN" &
        TUNNEL_PID=$!
        sleep 2
        log "Cloudflare Tunnel started (PID: $TUNNEL_PID)"
    fi

    # Execute the main command (jetty or whatever was passed)
    log "Starting Jetty..."
    exec "$@"
}

# Handle signals for graceful shutdown
cleanup() {
    log "Shutting down..."
    if [ -n "$WARP_SVC_PID" ]; then
        warp-cli --accept-tos disconnect 2>/dev/null || true
        kill $WARP_SVC_PID 2>/dev/null || true
    fi
    if [ -n "$TUNNEL_PID" ]; then
        kill $TUNNEL_PID 2>/dev/null || true
    fi
    exit 0
}

trap cleanup SIGTERM SIGINT

main "$@"
