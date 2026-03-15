#!/usr/bin/env bash
set -euo pipefail

REPO="${CLAWFS_RELEASE_REPO:-s1liconcow/clawfs}"
VERSION="${CLAWFS_INSTALL_VERSION:-latest}"
INSTALL_ROOT="${CLAWFS_INSTALL_ROOT:-$HOME/.local}"
BIN_DIR="${CLAWFS_INSTALL_BIN_DIR:-$INSTALL_ROOT/bin}"
LIB_DIR="${CLAWFS_INSTALL_LIB_DIR:-$INSTALL_ROOT/lib/clawfs}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

OS="$(uname -s)"
ARCH="$(uname -m)"

if [[ "$OS" != "Linux" ]]; then
  echo "clawfs install.sh currently supports Linux only." >&2
  exit 1
fi

if [[ "$ARCH" != "x86_64" ]]; then
  echo "clawfs install.sh currently supports x86_64 only." >&2
  exit 1
fi

if [[ "$VERSION" == "latest" ]]; then
  API_URL="https://api.github.com/repos/${REPO}/releases/latest"
  VERSION="$(curl -fsSL "$API_URL" | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  if [[ -z "$VERSION" ]]; then
    echo "failed to resolve latest ClawFS release tag" >&2
    exit 1
  fi
fi

TARBALL="clawfs-${VERSION}-linux-x86_64.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"

echo "Installing ClawFS ${VERSION} from ${URL}"
curl -fsSL "$URL" -o "$TMP_DIR/$TARBALL"
tar -xzf "$TMP_DIR/$TARBALL" -C "$TMP_DIR"

mkdir -p "$BIN_DIR" "$LIB_DIR"
install -m 0755 "$TMP_DIR/clawfs" "$BIN_DIR/clawfs"
install -m 0755 "$TMP_DIR/clawfs_checkpoint" "$BIN_DIR/clawfs_checkpoint"
install -m 0755 "$TMP_DIR/clawfs_replay" "$BIN_DIR/clawfs_replay"
install -m 0755 "$TMP_DIR/clawfs-nfs-gateway" "$BIN_DIR/clawfs-nfs-gateway"

if [[ -f "$TMP_DIR/libclawfs_preload.so" ]]; then
  install -m 0755 "$TMP_DIR/libclawfs_preload.so" "$LIB_DIR/libclawfs_preload.so"
fi

echo
echo "Installed the all-in-one ClawFS CLI to $BIN_DIR"
echo "Installed support binaries and preload library to $BIN_DIR and $LIB_DIR"
echo
echo "Next steps:"
echo "  export PATH=\"$BIN_DIR:\$PATH\""
echo "  clawfs login"
echo "  clawfs whoami"
echo "  clawfs summon -- codex"
