#!/usr/bin/env bash
# Downloads the per-minute invocation counts of the Azure Functions 2019 trace
# (CC-BY-4.0, https://github.com/Azure/AzurePublicDataset) into the bench cache.
# Idempotent: does nothing when all 14 day files are already extracted.
set -euo pipefail

URL="https://github.com/Azure/AzurePublicDataset/releases/download/dataset-functions-2019/azurefunctions_dataset2019_azurefunctions-dataset2019.tar.xz"
CACHE="${TIDEWATCH_BENCH_CACHE:-$HOME/.cache/tidewatch-bench}"
DEST="$CACHE/azure2019"

have_all() {
  local d
  for d in 01 02 03 04 05 06 07 08 09 10 11 12 13 14; do
    [ -s "$DEST/invocations_per_function_md.anon.d$d.csv" ] || return 1
  done
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

have_all || { echo "azure2019: expected 14 invocation files in $DEST" >&2; exit 1; }
echo "azure2019: done, files in $DEST"
echo "Data: Azure Functions Trace 2019, CC-BY-4.0, Shahrad et al., USENIX ATC 2020."
