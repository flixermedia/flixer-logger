#!/bin/sh
set -eu

if [ "${1:-}" = "--init-cert" ]; then
	data_dir=${DATA_DIR:-/data}
	cert="$data_dir/logs/logger/tls.crt"
	key="$data_dir/logs/logger/tls.key"
	umask 077
	if [ -s "$cert" ] && [ -s "$key" ]; then
		exit 0
	fi
	if [ -s "$cert" ] || [ -s "$key" ]; then
		echo "Logger TLS certificate and key must both be present or both be empty." >&2
		exit 1
	fi
	openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 3650 \
		-keyout "$key" -out "$cert" -subj "/CN=logger" \
		-addext "subjectAltName=DNS:logger" \
		-addext "basicConstraints=critical,CA:TRUE"
	chmod 0600 "$key"
	chmod 0644 "$cert"
	exit 0
fi

exec /usr/local/bin/flixer-logger "$@"
