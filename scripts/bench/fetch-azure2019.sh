#!/usr/bin/env bash
# Downloads the per-minute invocation counts of the Azure Functions 2019 trace
# (CC-BY-4.0, https://github.com/Azure/AzurePublicDataset) into the bench cache.
# Idempotent: does nothing when all 14 day files are already extracted.
set -euo pipefail

URL="https://github.com/Azure/AzurePublicDataset/releases/download/dataset-functions-2019/azurefunctions_dataset2019_azurefunctions-dataset2019.tar.xz"
CACHE="${TIDEWATCH_BENCH_CACHE:-$HOME/.cache/tidewatch-bench}"
DEST="$CACHE/azure2019"

SUMS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/azure2019.sha256"

# verify checks every extracted file against the pinned SHA-256s.
verify() {
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$1" && sha256sum -c --quiet "$SUMS")
  else
    (cd "$1" && shasum -a 256 -c --quiet "$SUMS")
  fi
}

have_all() {
  verify "$DEST" >/dev/null 2>&1
}

if have_all; then
  echo "azure2019: already present in $DEST"
  exit 0
fi

mkdir -p "$DEST"
tmp="$(mktemp -d "$CACHE/.fetch.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

echo "azure2019: downloading (~140 MB) from $URL"
curl -fL --retry 3 -o "$tmp/dataset.tar.xz" "$URL"
# Only the invocation-count files are needed. Flatten any directory prefix.
# GNU tar needs --wildcards for patterns; bsdtar (macOS) treats them as globs already.
wild=()
if tar --version 2>/dev/null | grep -q 'GNU tar'; then wild=(--wildcards); fi
tar -xJf "$tmp/dataset.tar.xz" -C "$tmp" ${wild[@]+"${wild[@]}"} '*invocations_per_function_md.anon.d*.csv'
find "$tmp" -name 'invocations_per_function_md.anon.d*.csv' -exec mv -f {} "$DEST/" \;

if ! verify "$DEST"; then
  echo "azure2019: SHA-256 verification failed for files in $DEST (see scripts/bench/azure2019.sha256)" >&2
  exit 1
fi
echo "azure2019: done, files in $DEST"
echo "Data: Azure Functions Trace 2019, CC-BY-4.0, Shahrad et al., USENIX ATC 2020."
