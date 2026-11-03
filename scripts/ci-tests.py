#!/usr/bin/env python3
import json
import shlex
import subprocess
import sys

groups = {
    "reader": ["reader"],
    "mcp-edge": ["e2e_mcp_edge"],
    "migration-chunking": ["e2e_profile_migration", "chunking"],
}
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]
))
packages = metadata["packages"]
assert len(packages) == 1, "update test sharding for multiple packages"
targets = {target["name"] for target in packages[0]["targets"] if "test" in target["kind"]}
assigned = [target for group in groups.values() for target in group]
assert len(assigned) == len(set(assigned)) and set(assigned) <= targets
groups["core"] = sorted(targets - set(assigned))
assert set().union(*map(set, groups.values())) == targets

group = sys.argv[1]
command = ["cargo", "test", "--locked"]
if group == "core":
    command += ["--lib", "--bins"]
for target in groups[group]:
    command += ["--test", target]
print(shlex.join(command), flush=True)
if sys.argv[2:] != ["--dry-run"]:
    subprocess.run(command, check=True)
