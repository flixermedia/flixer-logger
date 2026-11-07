#!/usr/bin/env bash
# Synchronize the Go exporter snapshot used by an independently built producer.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1/go.mod" ]; then
  echo "usage: vendor-go-sdk.sh /path/to/producer" >&2
  exit 1
fi

source_root="$(cd "$(dirname "$0")/.." && pwd)"
target="$(cd "$1" && pwd)/third_party/flixer-logger"
if [ -e "$target" ]; then
  if [ ! -f "$target/SHA256SUMS" ]; then
    echo "Refusing to overwrite an unmanaged SDK directory: $target" >&2
    exit 1
  fi
  if ! (cd "$target" && sha256sum --quiet -c SHA256SUMS); then
    echo "Refusing to overwrite locally modified SDK files: $target" >&2
    exit 1
  fi
fi

install -d "$target/pkg/exporter" "$target/internal/redact"
install -m 0644 "$source_root/scripts/sdk-go.mod" "$target/go.mod"
files=(
  pkg/exporter/backoff.go pkg/exporter/backoff_test.go
  pkg/exporter/exporter.go pkg/exporter/exporter_test.go
  pkg/exporter/slog.go pkg/exporter/slog_test.go
  pkg/exporter/control.go pkg/exporter/control_test.go pkg/exporter/control_secret_test.go
  pkg/exporter/control_owner_unix.go pkg/exporter/control_owner_other.go
  pkg/exporter/managed.go pkg/exporter/managed_test.go pkg/exporter/managed_rotation_test.go
  pkg/exporter/service.go pkg/exporter/service_test.go
  internal/redact/redact.go internal/redact/redact_test.go
)
for file in "${files[@]}"; do
  install -m 0644 "$source_root/$file" "$target/$file"
done
(cd "$target" && sha256sum go.mod "${files[@]}" > SHA256SUMS)
echo "Updated exporter snapshot in $target"
