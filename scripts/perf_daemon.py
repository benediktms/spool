#!/usr/bin/env python3
"""Daemon-level performance harness (spec 20): readiness time, idle CPU, search latency while ingesting,
RSS. Uses an isolated SPOOL_HOME under /tmp, a synthetic OhMyPi corpus, lexical-only operation (hosted
processing off) and the real release binary over the real Unix socket. Starts exactly one daemon child and
kills only that pid.

  python3 scripts/perf_daemon.py --bin target/release/spool --idle-seconds 600 --sessions 300 --messages 200
"""
import argparse, json, os, random, shutil, socket, struct, subprocess, sys, tempfile, time

def ps(pid, field):
    out = subprocess.run(["ps", "-o", field + "=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return out

def cpu_seconds(pid):
    t = ps(pid, "time")  # [dd-]hh:mm:ss or m:ss.cc on macOS
    if not t:
        return 0.0
    parts = t.replace("-", ":").split(":")
    secs = 0.0
    for p in parts:
        secs = secs * 60 + float(p)
    return secs

def rss_mib(pid):
    v = ps(pid, "rss")
    return int(v) / 1024 if v else 0.0

def call(sock_path, req, timeout=60):
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(timeout)
    s.connect(sock_path)
    body = json.dumps(req).encode()
    s.sendall(struct.pack(">I", len(body)) + body)
    n = struct.unpack(">I", s.recv(4))[0]
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            break
        buf += chunk
    s.close()
    return json.loads(buf)

def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, max(0, int(round(p * len(v) + 0.5)) - 1))]

def gen_corpus(src, sessions, messages, rng):
    words = [f"w{i:04d}{rng.choice(['ka','to','mi','re','su'])}" for i in range(3000)]
    now = time.time()
    total = 0
    for s in range(sessions):
        lines = [json.dumps({"type": "title", "v": 1, "title": f"perf session {s}", "source": "user", "updatedAt": "2026-10-12T09:00:00Z", "pad": ""}),
                 json.dumps({"type": "session", "version": 3, "id": f"perf-{s}", "timestamp": "2026-10-12T09:00:00Z", "cwd": f"/tmp/proj{s % 20}", "title": f"perf session {s}"})]
        parent = None
        for m in range(messages):
            mid = f"{s:04x}{m:04x}"
            text = " ".join(rng.choice(words) for _ in range(80))
            role = "user" if m % 2 == 0 else "assistant"
            content = [{"type": "text", "text": text}]
            if role == "assistant" and m % 6 == 1:
                content.append({"type": "toolCall", "id": f"c{m}", "name": "bash", "arguments": {"command": "echo " + "x" * 400}})
            lines.append(json.dumps({"type": "message", "id": mid, "parentId": parent, "timestamp": "2026-10-12T09:%02d:%02dZ" % (m // 60 % 60, m % 60), "message": {"role": role, "content": content}}))
            parent = mid
            total += len(text)
            if role == "assistant" and m % 6 == 1:
                rid = mid + "r"
                lines.append(json.dumps({"type": "message", "id": rid, "parentId": parent, "timestamp": "2026-10-12T09:%02d:%02dZ" % (m // 60 % 60, m % 60), "message": {"role": "toolResult", "toolCallId": f"c{m}", "toolName": "bash", "content": [{"type": "text", "text": "out " * 300}], "isError": False}}))
                parent = rid
        with open(os.path.join(src, f"perf-{s}.jsonl"), "w") as f:
            f.write("\n".join(lines) + "\n")
    return total, words

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--idle-seconds", type=int, default=600)
    ap.add_argument("--sessions", type=int, default=300)
    ap.add_argument("--messages", type=int, default=200)
    ap.add_argument("--populated-idle-seconds", type=int, default=600)
    a = ap.parse_args()
    rng = random.Random(7)
    tmp = tempfile.mkdtemp(prefix="spperf", dir="/tmp")
    home, src = os.path.join(tmp, "h"), os.path.join(tmp, "src")
    os.makedirs(src)
    os.makedirs(os.path.join(tmp, "fakehome"))
    env = dict(os.environ, SPOOL_HOME=home, HOME=os.path.join(tmp, "fakehome"))
    subprocess.run([a.bin, "init", "--enable-source", "omp", "--hosted-processing", "no"], env=env, check=True, capture_output=True)
    cfgp = os.path.join(home, "config.toml")
    cfg = open(cfgp).read().replace("~/.omp/agent/sessions", src)
    # the harness issues thousands of searches; every search pins a lease (default cap 256 over 30 min), so shorten the TTL
    assert "lease_ttl_seconds = 1800" in cfg
    cfg = cfg.replace("lease_ttl_seconds = 1800", "lease_ttl_seconds = 5")
    open(cfgp, "w").write(cfg)
    # privacy guard: every ENABLED source root must be inside this sandbox (an earlier run indexed real transcripts)
    import tomllib
    real = os.path.realpath(tmp)
    enabled = [s for s in tomllib.loads(cfg)["sources"] if s.get("enabled")]
    assert enabled, "no enabled source"
    for s in enabled:
        for root in s["roots"]:
            if not os.path.realpath(os.path.expanduser(root)).startswith(real + os.sep):
                sys.exit(f"refusing to start: enabled root outside sandbox: {root}")
    chars, words = gen_corpus(src, a.sessions, a.messages, rng)
    size = sum(os.path.getsize(os.path.join(src, f)) for f in os.listdir(src))
    print(f"corpus: {a.sessions} sessions x {a.messages} messages, {size/1048576:.1f} MiB source, {chars/1048576:.1f} MiB conversational text", flush=True)
    # empty source dir first so the idle measurement is a daemon with nothing to ingest
    held = os.path.join(tmp, "held")
    shutil.move(src, held)
    os.makedirs(src)
    sock = os.path.join(home, "state", "spool.sock")
    t0 = time.time()
    d = subprocess.Popen([a.bin, "daemon", "run"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        ready = None
        while time.time() - t0 < 60:
            try:
                r = call(sock, {"op": "tool", "id": 1, "tool": "search_sessions", "args": {"query": "x", "mode": "lexical"}}, timeout=5)
                ready = time.time() - t0
                break
            except Exception:
                time.sleep(0.01)
        print(f"ready for search: {ready:.3f}s after spawn (release build, empty index)", flush=True)
        time.sleep(3)
        c0, w0 = cpu_seconds(d.pid), time.time()
        samples = []
        while time.time() - w0 < a.idle_seconds:
            time.sleep(10)
            samples.append(rss_mib(d.pid))
        c1, w1 = cpu_seconds(d.pid), time.time()
        print(f"idle {w1-w0:.0f}s: cpu {c1-c0:.2f}s = {(c1-c0)/(w1-w0)*100:.2f}% of one core; RSS {min(samples):.0f}-{max(samples):.0f} MiB", flush=True)

        # search latency baseline (idle, empty-ish index) then during ingestion
        def q(i):
            t = time.time()
            r = call(sock, {"op": "tool", "id": 100 + i, "tool": "search_sessions", "args": {"query": f"{words[(i*37) % 3000]} {words[(i*91) % 3000]}", "mode": "lexical", "limit": 10}})
            assert "ok" in r, r
            return (time.time() - t) * 1000
        shutil.rmtree(src)
        shutil.move(held, src)  # corpus appears; periodic discovery (or the explicit op) ingests it
        idx = call(sock, {"op": "control", "id": 5, "cmd": "index_since", "args": {"since_ms": int((time.time() - 30 * 86400) * 1000)}}, timeout=120)
        print("index operation:", json.dumps(idx.get("ok", idx))[:200], flush=True)
        lat, rss, cpu = [], [], []
        start = time.time()
        i = 0
        while True:
            lat.append(q(i)); i += 1
            rss.append(rss_mib(d.pid))
            st = call(sock, {"op": "control", "id": 7, "cmd": "status", "args": {}})["ok"]
            queue = st["queue"]
            if queue.get("pending_sessions", 1) == 0 and queue.get("running", 1) == 0 and queue.get("queued", 1) == 0 and i > 20:
                break
            if time.time() - start > 1500:
                print("ingestion did not finish in 1500 s", flush=True)
                break
            time.sleep(0.2)
        ing_secs = time.time() - start
        c2 = cpu_seconds(d.pid)
        print(f"ingestion of {a.sessions} sessions took {ing_secs:.1f}s; daemon cpu total {c2-c1:.1f}s; RSS during ingestion {min(rss):.0f}-{max(rss):.0f} MiB (peak {max(rss):.0f})", flush=True)
        print(f"search during ingestion ({len(lat)} queries): p50 {pct(lat,.5):.1f} ms, p95 {pct(lat,.95):.1f} ms, p99 {pct(lat,.99):.1f} ms, max {max(lat):.1f} ms", flush=True)
        time.sleep(2)
        after = [q(1000 + j) for j in range(60)]
        print(f"search after ingestion, idle ({len(after)} queries): p50 {pct(after,.5):.1f} ms, p95 {pct(after,.95):.1f} ms, p99 {pct(after,.99):.1f} ms", flush=True)
        print(f"ratio p95 during/after: {pct(lat,.95)/max(pct(after,.95),1e-9):.2f}x", flush=True)
        st = call(sock, {"op": "control", "id": 8, "cmd": "status", "args": {}})["ok"]
        wal = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(home) for f in fs if f.endswith("-wal"))
        print(f"steady RSS after ingestion: {rss_mib(d.pid):.0f} MiB; sessions indexed: {st['queue'].get('indexed')}; WAL files {wal/1048576:.1f} MiB", flush=True)
        # empty-index latencies are meaningless: the run is only valid for a fully published corpus with real hits
        hit = call(sock, {"op": "tool", "id": 9, "tool": "search_sessions", "args": {"query": words[0], "mode": "lexical", "limit": 10}})["ok"]
        if st["queue"].get("indexed") != a.sessions or not hit.get("results"):
            sys.exit(f"INVALID RUN: indexed={st['queue'].get('indexed')} of {a.sessions}, hits={len(hit.get('results') or [])}")

        # populated-corpus criteria: restart on the existing index, time to the first search that returns hits, then idle
        import sqlite3
        dbp = next(os.path.join(dp, f) for dp, _, fs in os.walk(home) for f in fs if f == "index.sqlite")
        chunks = sqlite3.connect(f"file:{dbp}?mode=ro", uri=True).execute("select count(*) from chunks").fetchone()[0]
        print(f"populated index: {a.sessions} sessions, {chunks} chunks, {os.path.getsize(dbp)/1048576:.1f} MiB index.sqlite", flush=True)
        d.terminate()
        d.wait(timeout=30)
        t1 = time.time()
        d = subprocess.Popen([a.bin, "daemon", "run"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ready2 = None
        while time.time() - t1 < 60:
            try:
                r = call(sock, {"op": "tool", "id": 11, "tool": "search_sessions", "args": {"query": words[0], "mode": "lexical", "limit": 10}}, timeout=5)
                if r.get("ok", {}).get("results"):
                    ready2 = time.time() - t1
                    break
            except Exception:
                pass
            time.sleep(0.01)
        if ready2 is None:
            sys.exit("INVALID RUN: restarted daemon never answered a search with hits")
        print(f"restart on populated index: first search with hits {ready2:.3f}s after spawn", flush=True)
        time.sleep(3)
        c0, w0, samples = cpu_seconds(d.pid), time.time(), []
        while time.time() - w0 < a.populated_idle_seconds:
            time.sleep(10)
            samples.append(rss_mib(d.pid))
        c1, w1 = cpu_seconds(d.pid), time.time()
        print(f"populated idle {w1-w0:.0f}s (source discovery running): cpu {c1-c0:.2f}s = {(c1-c0)/(w1-w0)*100:.2f}% of one core; RSS {min(samples):.0f}-{max(samples):.0f} MiB", flush=True)
    finally:
        d.terminate()
        try:
            d.wait(timeout=20)
        except subprocess.TimeoutExpired:
            d.kill()
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
