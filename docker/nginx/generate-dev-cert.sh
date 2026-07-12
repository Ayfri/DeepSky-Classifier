#!/bin/sh
# Self-signed cert for local `docker compose up`. Production terminates TLS with a real
# Let's Encrypt certificate (certbot) instead, per bloc 1 §9.4 — this script never runs there.
set -eu

CERT_DIR="$(dirname "$0")/certs"
mkdir -p "$CERT_DIR"

openssl req -x509 -nodes -newkey rsa:2048 \
	-keyout "$CERT_DIR/privkey.pem" \
	-out "$CERT_DIR/fullchain.pem" \
	-days 365 \
	-subj "/CN=localhost"

echo "Wrote $CERT_DIR/fullchain.pem and $CERT_DIR/privkey.pem"
