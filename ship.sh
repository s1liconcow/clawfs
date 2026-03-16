#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: ./ship.sh [VERSION|vVERSION|--major|--minor|--patch]

Without an argument, `./ship.sh` bumps the current Cargo package version by one patch
release.

Examples:
  ./ship.sh
  ./ship.sh --patch
  ./ship.sh 0.2.0
  ./ship.sh v0.2.0
EOF
}

require_clean_worktree() {
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Refusing to ship from a dirty worktree." >&2
    echo "Commit or stash changes first." >&2
    exit 1
  fi
}

current_branch() {
  git symbolic-ref --quiet --short HEAD || {
    echo "Refusing to ship from a detached HEAD." >&2
    exit 1
  }
}

current_version() {
  sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1
}

normalize_version() {
  local raw=${1#v}
  if [[ ! $raw =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "Invalid version: $1" >&2
    exit 1
  fi
  printf '%s\n' "$raw"
}

bump_version() {
  local version=$1
  local mode=$2
  local major minor patch
  IFS=. read -r major minor patch <<<"$version"
  case "$mode" in
    major) printf '%s.0.0\n' "$((major + 1))" ;;
    minor) printf '%s.%s.0\n' "$major" "$((minor + 1))" ;;
    patch) printf '%s.%s.%s\n' "$major" "$minor" "$((patch + 1))" ;;
    *)
      echo "Unsupported bump mode: $mode" >&2
      exit 1
      ;;
  esac
}

replace_manifest_version() {
  local file=$1
  local version=$2
  perl -0pi -e 's/(\[package\]\n(?:[^\n]*\n)*?version = ")([^"]+)(")/${1}'"$version"'${3}/s' "$file"
}

replace_lock_version() {
  local package=$1
  local version=$2
  perl -0pi -e 's/(name = "'"$package"'"\nversion = ")([^"]+)(")/${1}'"$version"'${3}/s' Cargo.lock
}

target_version() {
  local current=$1
  local arg=${2-}
  case "$arg" in
    "") bump_version "$current" patch ;;
    --major) bump_version "$current" major ;;
    --minor) bump_version "$current" minor ;;
    --patch) bump_version "$current" patch ;;
    -h|--help) usage; exit 0 ;;
    *) normalize_version "$arg" ;;
  esac
}

main() {
  require_clean_worktree

  local branch version tag current
  branch=$(current_branch)
  current=$(current_version)
  version=$(target_version "$current" "${1-}")
  tag="v$version"

  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "Tag already exists: $tag" >&2
    exit 1
  fi

  replace_manifest_version Cargo.toml "$version"
  replace_manifest_version clawfs-nfs-gateway/Cargo.toml "$version"
  replace_manifest_version clawfs-preload/Cargo.toml "$version"

  replace_lock_version clawfs "$version"
  replace_lock_version clawfs-nfs-gateway "$version"
  replace_lock_version clawfs-preload "$version"

  git add Cargo.toml Cargo.lock clawfs-nfs-gateway/Cargo.toml clawfs-preload/Cargo.toml
  git commit -m "release: $tag"
  git tag "$tag"
  git push origin "$branch"
  git push origin "$tag"

  echo "Shipped $tag from branch $branch"
}

main "$@"
