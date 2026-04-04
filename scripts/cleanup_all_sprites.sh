#!/usr/bin/env bash
set -euo pipefail

APPLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/cleanup_all_sprites.sh [--apply]

List all sprites and destroy them.

Options:
  --apply     Actually destroy the sprites. Without this flag, prints a dry run.
  -h, --help  Show help

Examples:
  scripts/cleanup_all_sprites.sh
  scripts/cleanup_all_sprites.sh --apply
USAGE
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd sprite

if ! sprite list >/dev/null 2>&1; then
  echo "sprite is not authenticated. Set SPRITES_TOKEN or run 'sprite login' first." >&2
  exit 1
fi

mapfile -t sprites < <(sprite list 2>/dev/null | sed '/^[[:space:]]*$/d')

if [[ ${#sprites[@]} -eq 0 ]]; then
  echo "No sprites found."
  exit 0
fi

echo "Found ${#sprites[@]} sprite(s):"
printf '  %s\n' "${sprites[@]}"

if [[ "$APPLY" -ne 1 ]]; then
  echo
  echo "Dry run only. Re-run with --apply to destroy all listed sprites."
  exit 0
fi

for sprite_name in "${sprites[@]}"; do
  echo "Destroying $sprite_name"
  sprite destroy --force "$sprite_name"
done

echo "Destroyed ${#sprites[@]} sprite(s)."
