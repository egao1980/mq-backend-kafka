#!/usr/bin/env bash
# Stage librdkafka shared libraries into lib/<os>-<arch>/ for the native
# overlays and for checkout tests.
#
# The binaries are librdkafka's own release builds, as Confluent publishes
# them in the librdkafka.redist NuGet package: dependencies (OpenSSL, zlib,
# zstd, lz4, ...) are linked in statically on Linux and macOS, and shipped as
# DLLs next to librdkafka.dll on Windows. The package is pinned by version
# and SHA-256.
#
# Usage: scripts/fetch-librdkafka.sh [os-arch ...]   (default: this machine)
#        scripts/fetch-librdkafka.sh all             (every overlay platform)
# Env:   RDKAFKA_VERSION (default 2.16.0), RDKAFKA_SHA256 (checked when set,
#        built in for the default version), DEST_ROOT (default ./lib).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RDKAFKA_VERSION="${RDKAFKA_VERSION:-2.16.0}"
DEFAULT_VERSION="2.16.0"
DEFAULT_SHA256="f5b41d820d6a7e886bcb4c574020a1dcd54d72a965bad681f15c1c788a80af5b"
if [[ -z "${RDKAFKA_SHA256:-}" && "$RDKAFKA_VERSION" == "$DEFAULT_VERSION" ]]; then
  RDKAFKA_SHA256="$DEFAULT_SHA256"
fi
DEST_ROOT="${DEST_ROOT:-$ROOT/lib}"
CACHE="$ROOT/build"
PKG="$CACHE/librdkafka.redist.${RDKAFKA_VERSION}.nupkg"
URL="https://www.nuget.org/api/v2/package/librdkafka.redist/${RDKAFKA_VERSION}"

host_pair() {
  local os arch
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    MINGW*|MSYS*|CYGWIN*|Windows_NT) os=windows ;;
    *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
  esac
  echo "${os}-${arch}"
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

mkdir -p "$CACHE"
if [[ ! -f "$PKG" ]]; then
  echo "==> download $URL"
  curl -fsSL --retry 3 "$URL" -o "$PKG.part"
  mv "$PKG.part" "$PKG"
fi
if [[ -n "${RDKAFKA_SHA256:-}" ]]; then
  got="$(sha256_of "$PKG")"
  if [[ "$got" != "$RDKAFKA_SHA256" ]]; then
    echo "SHA-256 mismatch for $PKG: got $got, want $RDKAFKA_SHA256" >&2
    rm -f "$PKG"
    exit 1
  fi
else
  echo "WARNING: no RDKAFKA_SHA256 for librdkafka ${RDKAFKA_VERSION}; not verified" >&2
fi

# One versioned file per platform (asdf-native tries librdkafka.so.1 /
# librdkafka.1.dylib first); no unversioned symlink, which artifact upload
# would turn into a second copy.
stage() {
  local pair="$1" out="$DEST_ROOT/$1" tmp
  tmp="$(mktemp -d)"
  rm -rf "$out"
  mkdir -p "$out"
  case "$pair" in
    linux-amd64)
      # centos8-librdkafka.so needs only glibc (2.28+); the plain librdkafka.so
      # of this package wants RHEL's libsasl2.so.3, missing on Debian/Ubuntu.
      unzip -q -o "$PKG" "runtimes/linux-x64/native/centos8-librdkafka.so" -d "$tmp"
      cp "$tmp/runtimes/linux-x64/native/centos8-librdkafka.so" "$out/librdkafka.so.1"
      ;;
    linux-arm64)
      unzip -q -o "$PKG" "runtimes/linux-arm64/native/librdkafka.so" -d "$tmp"
      cp "$tmp/runtimes/linux-arm64/native/librdkafka.so" "$out/librdkafka.so.1"
      ;;
    darwin-arm64)
      # Left as built: rewriting its install name would void the signature.
      unzip -q -o "$PKG" "runtimes/osx-arm64/native/librdkafka.dylib" -d "$tmp"
      cp "$tmp/runtimes/osx-arm64/native/librdkafka.dylib" "$out/librdkafka.1.dylib"
      ;;
    windows-amd64)
      # librdkafka.dll and the DLLs it links (the C++ binding is left out).
      unzip -q -o "$PKG" "runtimes/win-x64/native/*" -d "$tmp"
      for f in "$tmp"/runtimes/win-x64/native/*.dll; do
        [[ "$(basename "$f")" == librdkafkacpp.dll ]] && continue
        cp "$f" "$out/"
      done
      ;;
    *)
      echo "no librdkafka build for $pair" >&2
      rm -rf "$tmp"
      exit 1
      ;;
  esac
  unzip -q -o "$PKG" "LICENSES.txt" -d "$tmp"
  cp "$tmp/LICENSES.txt" "$out/LICENSES.librdkafka.txt"
  rm -rf "$tmp"
  echo "==> $pair:"
  ls -la "$out"
}

pairs=("$@")
if [[ ${#pairs[@]} -eq 0 ]]; then
  pairs=("$(host_pair)")
elif [[ "${pairs[0]}" == all ]]; then
  pairs=(linux-amd64 linux-arm64 darwin-arm64 windows-amd64)
fi
for p in "${pairs[@]}"; do
  stage "$p"
done
echo "OK: librdkafka ${RDKAFKA_VERSION} -> ${pairs[*]}"
