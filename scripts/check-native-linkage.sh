#!/usr/bin/env bash
set -euo pipefail

addon="${1:?usage: check-native-linkage.sh path/to/addon.node}"
case "$(uname -s)" in
  Linux) dependencies="$(readelf -d "$addon")" ;;
  Darwin) dependencies="$(otool -L "$addon")" ;;
  *) echo "unsupported native release platform" >&2; exit 1 ;;
esac

# The npm addon must work without a system SQLite installation. Inspect the
# artifact: a successful load on a development machine can mask this dependency.
if [[ "$dependencies" == *libsqlite3* ]]; then
  echo "$addon depends on system SQLite; build with bundled SQLite" >&2
  exit 1
fi
echo "$addon has no system SQLite dependency"
