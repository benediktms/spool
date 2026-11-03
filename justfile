# Rebuild this checkout's installed binary and restart its existing user service.
install:
    #!/usr/bin/env python3
    import subprocess
    import sys
    import time

    subprocess.run(["cargo", "build", "--release", "--locked", "--bin", "spool", "--target-dir", "target"], check=True)
    spool = "./target/release/spool"
    subprocess.run([spool, "service", "restart"], check=True)
    deadline = time.monotonic() + 30
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            result = subprocess.run([spool, "status", "--json"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=min(5, remaining))
            if result.returncode == 0:
                sys.exit(subprocess.call([spool, "doctor"]))
        except subprocess.TimeoutExpired:
            pass
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    sys.exit("Daemon did not become ready within 30 seconds")
