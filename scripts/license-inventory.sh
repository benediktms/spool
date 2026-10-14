#!/bin/sh
# Regenerates docs/licenses.md from the locked dependency graph.
set -eu
cd "$(dirname "$0")/.."
cargo metadata --format-version 1 --locked | python3 scripts/license_inventory.py > docs/licenses.md
