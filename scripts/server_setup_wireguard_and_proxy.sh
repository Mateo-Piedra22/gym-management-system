#!/usr/bin/env bash
set -euo pipefail

# GymMS: WireGuard server + HAProxy TLS proxy for PostgreSQL
# Usage:
#   sudo CLIENT_PUBLIC=<CLIENT_PUB_KEY> WG_ENDPOINT=<SERVER_PUBLIC_IP>:51820 bash server_setup_wireguard_and_proxy.sh
# Notes:
# - This script targets Ubuntu/Debian.
# - It creates wg0 with 10.10.10.1/24 and peers the client at 10.10.10.2/32.
# - It exposes a TLS (self-signed) proxy on TCP 5433 forwarding to 10.10.10.2:5432 over WireGuard.
# - After running, copy the printed Server Public Key back to the Windows client.

if [[ $EUID -ne 0 ]]; then
  echo "[ERROR] Please run as root (sudo)." >&2
  exit 1
fi

: "${CLIENT_PUBLIC:?Set CLIENT_PUBLIC env to the client's public key}"
WG_ENDPOINT="${WG_ENDPOINT:-}"  # e.g. 203.0.113.10:51820

apt-get update -y
apt-get install -y wireguard haproxy openssl

# Generate server keys if missing
mkdir -p /etc/wireguard
if [[ ! -f /etc/wireguard/server_private.key ]]; then
  umask 077
  wg genkey | tee /etc/wireguard/server_private.key | wg pubkey > /etc/wireguard/server_public.key
fi
SERVER_PRIVATE=$(cat /etc/wireguard/server_private.key)
SERVER_PUBLIC=$(cat /etc/wireguard/server_public.key)

# Write wg0.conf
cat > /etc/wireguard/wg0.conf <<EOF
[Interface]
PrivateKey = ${SERVER_PRIVATE}
Address = 10.10.10.1/24
ListenPort = 51820
SaveConfig = true

[Peer]
PublicKey = ${CLIENT_PUBLIC}
AllowedIPs = 10.10.10.2/32
PersistentKeepalive = 25
EOF

# Enable WireGuard
systemctl enable --now wg-quick@wg0
sleep 1
ip -4 addr show wg0 || true

# Create self-signed certificate for HAProxy (TLS)
mkdir -p /etc/haproxy
openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
  -keyout /etc/haproxy/self.key -out /etc/haproxy/self.crt \
  -subj "/CN=pg-proxy" >/dev/null 2>&1
cat /etc/haproxy/self.key /etc/haproxy/self.crt > /etc/haproxy/self.pem
chmod 600 /etc/haproxy/self.pem

# Minimal HAProxy config: TCP TLS frontend on :5433, backend to 10.10.10.2:5432
cat > /etc/haproxy/haproxy.cfg <<'EOF'
global
  daemon
  maxconn 2048

defaults
  mode tcp
  timeout connect 10s
  timeout client  1m
  timeout server  1m

frontend pg_front
  bind 0.0.0.0:5433 ssl crt /etc/haproxy/self.pem
  default_backend pg_back

backend pg_back
  server localdb 10.10.10.2:5432 check
EOF

systemctl enable --now haproxy
sleep 1
ss -ltnp | grep 5433 || true

# Open firewall if UFW is present
if command -v ufw >/dev/null 2>&1; then
  ufw allow 51820/udp || true
  ufw allow 5433/tcp || true
fi

cat <<INFO
[READY]
WireGuard server is up. HAProxy is listening on TCP 5433 with TLS.

Server Public Key:
${SERVER_PUBLIC}

Client should set:
  WG_SERVER_PUBLIC='${SERVER_PUBLIC}'
  WG_ENDPOINT='${WG_ENDPOINT:-<SERVER_PUBLIC_IP>:51820}'

Then re-run on Windows:
  PowerShell -ExecutionPolicy Bypass -File scripts\setup_wireguard_client.ps1

Finally, configure replication with DSN:
  host=<SERVER_PUBLIC_IP> port=5433 dbname=gimnasio user=postgres password=Matute03 sslmode=require
INFO