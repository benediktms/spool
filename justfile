# Build this checkout, register its binary with the user service, and restart it.
install:
    #!/usr/bin/env sh
    set -eu
    cargo build --release --locked --bin spool --target-dir target
    ./target/release/spool service install
    ./target/release/spool service restart
    for attempt in $(seq 1 30); do
        if ./target/release/spool status --json >/dev/null 2>&1; then
            exec ./target/release/spool doctor
        fi
        sleep 1
    done
    echo "Daemon did not become ready within 30 seconds" >&2
    exit 1
