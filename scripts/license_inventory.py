#!/usr/bin/env python3
"""Reads `cargo metadata --format-version 1` on stdin, prints the license inventory markdown."""
import collections, json, sys

m = json.load(sys.stdin)
by = collections.defaultdict(list)
for p in m["packages"]:
    if p["name"] == "spool":
        continue
    by[p["license"] or "UNSPECIFIED"].append(f'{p["name"]} {p["version"]}')
print("# Dependency license inventory\n")
print("Generated from `cargo metadata --locked` (all packages in the resolved graph, including dev/test and "
      "platform-specific crates that are not linked on macOS). Regenerate with `scripts/license-inventory.sh`. "
      "This is an inventory of declared SPDX expressions, not legal advice.\n")
print(f"Total crates: {sum(len(v) for v in by.values())}\n")
print("| License expression | Crates |\n|---|---|")
for l, v in sorted(by.items(), key=lambda x: -len(x[1])):
    print(f"| {l} | {len(v)} |")
print("\n## Crates by license expression\n")
for l, v in sorted(by.items(), key=lambda x: -len(x[1])):
    print(f"### {l}\n" + ", ".join(sorted(v)) + "\n")
