#!/usr/bin/env bash
set -euo pipefail

addon="${1:?usage: check-native-linkage.sh default.node sqlite.node}"
sqlite_addon="${2:?usage: check-native-linkage.sh default.node sqlite.node}"
dependencies() {
  case "$(uname -s)" in
    Linux) readelf -d "$1" ;;
    Darwin) otool -L "$1" ;;
    *) echo "unsupported native release platform" >&2; return 1 ;;
  esac
}

base_dependencies="$(dependencies "$addon")"
sqlite_dependencies="$(dependencies "$sqlite_addon")"
if [[ "$base_dependencies" == *libsqlite3* ]]; then
  echo "$addon must load without SQLite installed" >&2
  exit 1
fi
if [[ "$sqlite_dependencies" != *libsqlite3* ]]; then
  echo "$sqlite_addon must link to system SQLite" >&2
  exit 1
fi
echo "Only the SQLite companion depends on system SQLite"
