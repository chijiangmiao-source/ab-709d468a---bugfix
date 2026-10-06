"""One-shot acceptance service: build checks + unit tests + API/HTTP smoke.

Runs the build check (byte-compile) and the unit test suite, then exercises
the live HTTP API through the required scenarios:

  1. export keeps following the frozen rules snapshot after rules change
  2. crash recovery (converge a complete staged artifact; clean up a partial one)
  3. business-equivalent retransmission (first receipt, no second artifact)
     and conflict handling (different records or rules snapshot)

Exits 0 when everything passes, 1 otherwise.
"""
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

API = os.environ.get("API_BASE", "http://localhost:8080").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
RESTART_SERVICES = ("app", "worker")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = uuid.uuid4().hex[:6]

FAILURES = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    line = "[%s] %s" % (mark, name)
    if not condition and detail:
        line += " -- " + detail
    print(line, flush=True)
    if not condition:
        FAILURES.append(name)


def step(title):
    print("\n=== %s ===" % title, flush=True)


# ------------------------------------------------------------------ helpers

def req(method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers or {})


def as_json(raw):
    return json.loads(raw.decode("utf-8"))


def wait_for_stage(export_id, stage, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, raw, _ = req("GET", "/api/exports/" + export_id)
        if status == 200:
            last = as_json(raw)
            if last["stage"] == stage:
                return last
        time.sleep(1)
    return last


def published_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "published")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n == export_id + ".json"]


def tmp_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "tmp")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n.startswith(export_id + ".")]


def download(export_id):
    return req("GET", "/api/exports/%s/artifact" % export_id)


# ------------------------------------------------- Docker (compose) restart

def _unix_http_connection():
    import http.client

    class UnixHTTPConnection(http.client.HTTPConnection):
        def __init__(self, socket_path):
            super().__init__("localhost")
            self._socket_path = socket_path

        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(self._socket_path)
            self.sock.settimeout(30)

    return UnixHTTPConnection


def docker_call(method, path, body=None):
    """Call the Docker Engine API over the mounted unix socket."""
    conn = _unix_http_connection()(DOCKER_SOCKET)
    headers = {}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=payload, headers=headers)
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, raw


def self_container_id():
    """Best-effort discovery of this process's full container id.

    Docker sets HOSTNAME to the 12-char short id; /proc/self/cgroup carries the
    full id on cgroup v1/v2 (fallback)."""
    short = os.environ.get("HOSTNAME", "")
    status, raw = docker_call("GET", "/containers/json?all=1")
    if status == 200:
        for container in json.loads(raw):
            cid = container.get("Id", "")
            if len(short) == 12 and cid.startswith(short):
                return cid
    try:
        import re

        with open("/proc/self/cgroup") as fh:
            text = fh.read()
        match = re.search(r"[0-9a-f]{64}", text)
        if match:
            return match.group(0)
    except OSError:
        pass
    return None


def compose_targets():
    """Return [(container_id, service)] for app+worker in this compose project."""
    cid = self_container_id()
    project = None
    if cid:
        status, raw = docker_call("GET", "/containers/%s/json" % cid)
        if status == 200:
            labels = json.loads(raw).get("Config", {}).get("Labels", {})
            project = labels.get("com.docker.compose.project")
    status, raw = docker_call("GET", "/containers/json?all=1")
    if status != 200:
        raise RuntimeError("docker list failed: %s %s" % (status, raw[:200]))
    targets = []
    for container in json.loads(raw):
        labels = container.get("Labels") or {}
        if project and labels.get("com.docker.compose.project") != project:
            continue
        service = labels.get("com.docker.compose.service")
        if service in RESTART_SERVICES:
            targets.append((container["Id"], service, container.get("Names") or ["?"]))
    return targets


def restart_compose_services():
    """Restart app + worker containers through the Docker socket."""
    targets = compose_targets()
    restarted = []
    for cid, service, names in targets:
        status, raw = docker_call("POST", "/containers/%s/restart?t=5" % cid)
        if status not in (204, 304):
            raise RuntimeError("restart %s failed: %s %s" % (names[0], status, raw[:200]))
        restarted.append("%s(%s)" % (service, names[0].lstrip("/")))
    if not restarted:
        raise RuntimeError("no app/worker compose containers found to restart")
    return restarted


def docker_available():
    return os.path.exists(DOCKER_SOCKET)


# ------------------------------------------------------------------ phases

def build_checks():
    step("构建检查：python -m compileall")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify", "tests"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    check("compileall app/verify/tests", proc.returncode == 0, proc.stderr.strip()[:400])


def unit_tests():
    step("代码测试：unittest discover")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    output = (proc.stdout + proc.stderr).strip()
    print("\n".join(output.splitlines()[-25:]))
    check("unit test suite", proc.returncode == 0, output[-400:])


def wait_for_api():
    step("等待 API 健康")
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            status, raw, _ = req("GET", "/healthz")
            if status == 200 and as_json(raw).get("ok"):
                check("health response", True)
                return
        except Exception:
            pass
        time.sleep(1)
    check("health response", False, "API did not become healthy in time")
    raise SystemExit(1)


def smoke():
    e1, e2, e3, e4, e5 = ("VFY%d-%s" % (i, RUN) for i in range(1, 6))
    s1, s2 = "VFY6-%s" % RUN, "VFY7-%s" % RUN
    recs = [
        {"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "lon": 121.473701, "depth_m": 42.51, "vessel_id": "HAICE-01"},
        {"ts": "2026-10-06T01:05:00Z", "lat": 31.231102, "lon": 121.480233, "depth_m": 43.04, "vessel_id": "HAICE-01"},
    ]
    same_rules_recs_a = [
        {"ts": "2026-10-06T02:00:00Z", "lat": 30.1001, "lon": 120.0001, "depth_m": 11.1, "vessel_id": "SHIP-X"},
        {"ts": "2026-10-06T02:05:00Z", "lat": 30.1011, "lon": 120.0011, "depth_m": 12.2, "vessel_id": "SHIP-X"},
    ]
    same_rules_recs_b = [
        {"ts": "2026-10-06T03:00:00Z", "lat": 35.9009, "lon": 130.9009, "depth_m": 77.7, "vessel_id": "SHIP-Y"},
        {"ts": "2026-10-06T03:09:00Z", "lat": 35.9099, "lon": 130.9099, "depth_m": 78.8, "vessel_id": "SHIP-Z"},
    ]
    rules_r1 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 10},
    ]}
    rules_r2 = {"rules": [{"field": "lat", "action": "redact"}]}

    step("页面通过真实 API 轮询（页面可加载且引用 API）")
    status, raw, headers = req("GET", "/")
    check("operator page served", status == 200 and "text/html" in headers.get("Content-Type", ""))
    check("page polls real API", b"/api/exports" in raw and b"fetch(" in raw)

    step("设置规则 R1（遮蔽 depth_m / 散列 vessel_id）")
    status, raw, _ = req("PUT", "/api/rules", rules_r1)
    check("put rules R1", status == 200, "HTTP %s %s" % (status, raw[:200]))
    d1 = as_json(raw)["digest"]

    step("提交导出 E1 → 201 冻结裁决")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    check("submit E1 -> 201", status == 201, "HTTP %s %s" % (status, raw[:300]))
    receipt1 = as_json(raw)
    check("E1 froze rules R1", receipt1.get("rules_digest") == d1)

    step("业务等价重传 E1（键序/空白不同）→ 200 首次回执")
    reordered = [dict(reversed(list(r.items()))) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": reordered})
    replay = as_json(raw)
    check("replay E1 -> 200", status == 200, "HTTP %s %s" % (status, raw[:300]))
    check("replay returns first receipt",
          replay.get("receipt_id") == receipt1["receipt_id"]
          and replay.get("received_at") == receipt1["received_at"]
          and replay.get("replay") is True)

    step("等待 E1 发布并校验工件")
    detail = wait_for_stage(e1, "PUBLISHED", 90)
    check("E1 published", detail is not None and detail["stage"] == "PUBLISHED",
          "last=%s" % (detail and detail.get("stage")))
    a1 = detail["artifact_digest"] if detail else None
    status, raw, _ = download(e1)
    check("download E1 -> 200", status == 200, "HTTP %s" % status)
    if status == 200:
        check("E1 digest matches download", hashlib.sha256(raw).hexdigest() == a1)
        doc = as_json(raw)
        check("E1 masked per R1 (depth redacted, vessel hashed, lat kept)",
              doc["records"][0]["depth_m"] == "***"
              and doc["records"][0]["vessel_id"] != "HAICE-01"
              and doc["records"][0]["lat"] == 31.230416)
        check("E1 artifact carries frozen digests",
              doc["rules_digest"] == d1 and doc["input_digest"] == receipt1["input_digest"])
    check("exactly one published artifact file for E1", len(published_files(e1)) == 1,
          str(published_files(e1)))

    step("同规则两份导出（S1→等待发布→S2）：最终工件身份互不串用")
    status, _, _ = req("POST", "/api/exports", {"export_id": s1, "records": same_rules_recs_a})
    check("submit S1 -> 201", status == 201, "HTTP %s" % status)
    sd1 = wait_for_stage(s1, "PUBLISHED", 90)
    check("S1 published before S2 is submitted", sd1 is not None and sd1["stage"] == "PUBLISHED",
          "last=%s" % (sd1 and sd1.get("stage")))
    status, _, _ = req("POST", "/api/exports", {"export_id": s2, "records": same_rules_recs_b})
    check("submit S2 -> 201", status == 201, "HTTP %s" % status)
    sd2 = wait_for_stage(s2, "PUBLISHED", 90)
    check("S2 published", sd2 is not None and sd2["stage"] == "PUBLISHED")

    def assert_download_identity(tag, export_id, records, frozen_digest, input_digest):
        code, body, headers = download(export_id)
        ok = code == 200
        check("%s: download -> 200" % tag, ok, "HTTP %s" % code)
        if not ok:
            return None
        served_digest = headers.get("X-Artifact-Digest")
        check("%s: X-Artifact-Digest matches bytes" % tag,
              hashlib.sha256(body).hexdigest() == served_digest == frozen_digest,
              "header=%s frozen=%s" % (served_digest, frozen_digest))
        doc = as_json(body)
        check("%s: artifact export_id is its own id" % tag, doc.get("export_id") == export_id,
              "got=%r" % doc.get("export_id"))
        check("%s: input_digest is its own frozen input" % tag,
              doc.get("input_digest") == input_digest)
        # Compare fields R1 leaves untouched (ts/lat/lon); vessel_id is hashed
        # and depth_m redacted by the frozen rules.
        got = [(r.get("ts"), r.get("lat"), r.get("lon")) for r in doc.get("records", [])]
        want = [(r["ts"], r["lat"], r["lon"]) for r in records]
        check("%s: records are its own masked records (identity by ts/lat/lon)" % tag,
              got == want and doc.get("record_count") == len(records),
              "got=%r" % got)
        check("%s: R1 masking applied (vessel hashed, depth redacted)" % tag,
              all(r.get("vessel_id") != src["vessel_id"] and len(r.get("vessel_id", "")) == 10
                  for r, src in zip(doc.get("records", []), records))
              and all(r.get("depth_m") == "***" for r in doc.get("records", [])))
        check("%s: carries integrity digest" % tag, isinstance(doc.get("integrity_digest"), str))
        return doc

    assert_download_identity("S1", s1, same_rules_recs_a,
                             sd1["artifact_digest"], sd1["input_digest"])
    assert_download_identity("S2", s2, same_rules_recs_b,
                             sd2["artifact_digest"], sd2["input_digest"])
    check("S1 and S2 have distinct artifact digests",
          sd1["artifact_digest"] != sd2["artifact_digest"])
    check("S1/S2 share the frozen rules digest (same rules)", sd1["rules_digest"] == sd2["rules_digest"])
    check("S1/S2 have distinct input digests", sd1["input_digest"] != sd2["input_digest"])

    step("植入历史错误态：S2 显示 PUBLISHED 但文件/摘要实为 S1 的工件")
    status, raw, _ = req("POST", "/api/test/corrupt", {"export_id": s2, "donor_id": s1})
    planted = status == 202
    check("plant foreign published artifact (S2<-S1)", planted, "HTTP %s %s" % (status, raw[:200]))
    if planted:
        planted_digest = as_json(raw)["artifact_digest"]
        check("planted digest equals S1 digest", planted_digest == sd1["artifact_digest"])
        code, body, _ = download(s2)
        if code == 200:
            served = as_json(body)
            # The one property that must hold no matter how fast the audit ran:
            # a 200 can never carry the FIRST export's identity/records. If the
            # periodic audit already repaired it, we must see S2's own bytes.
            check("PUBLISHED download never leaks the first export's identity",
                  served.get("export_id") == s2,
                  "got foreign export_id=%r" % served.get("export_id"))
        else:
            # digest-valid foreign bytes are refused until a worker repairs them
            check("download refuses foreign PUBLISHED content", code in (409, 410, 500),
                  "HTTP %s" % code)
        d = wait_for_stage(s2, "PUBLISHED", 3)
        check("S2 stage stays PUBLISHED (never regressed)", d is not None and d["stage"] == "PUBLISHED")

        step("重启 app 与 2×worker，随后再核对第二份导出")
        if docker_available():
            try:
                names = restart_compose_services()
                check("restart app + workers via docker socket", bool(names), str(names))
            except Exception as exc:
                check("restart app + workers via docker socket", False, repr(exc)[:300])
        else:
            # Local (non-Compose) runs cannot restart containers; the same
            # repair code runs in the live workers' startup/periodic audit, so
            # convergence below still exercises the identical path.
            print("[note] %s absent (local run): relying on running worker audit" % DOCKER_SOCKET,
                  flush=True)

        # the app itself must come back healthy regardless of workers
        deadline = time.time() + 90
        healthy = False
        while time.time() < deadline:
            code, raw, _ = req("GET", "/healthz")
            if code == 200 and as_json(raw).get("ok"):
                healthy = True
                break
            time.sleep(1)
        check("health response after restart", healthy)

        # worker startup/periodic audit converges S2 to its own artifact
        sd2_after = wait_for_stage(s2, "PUBLISHED", 120)
        check("S2 still PUBLISHED after restart", sd2_after is not None and sd2_after["stage"] == "PUBLISHED")
        deadline = time.time() + 120
        converged = None
        while time.time() < deadline:
            code, body, headers = download(s2)
            if code == 200:
                doc = as_json(body)
                if doc.get("export_id") == s2:
                    converged = (code, body, headers, doc)
                    break
            time.sleep(1)
        check("S2 converges to its own downloadable, verifiable artifact", converged is not None)
        if converged:
            code, body, headers, doc = converged
            check("S2 repaired digest differs from foreign S1 digest",
                  headers.get("X-Artifact-Digest") != planted_digest
                  and hashlib.sha256(body).hexdigest() == headers.get("X-Artifact-Digest"))
            check("S2 repaired artifact is the originally rendered S2 artifact",
                  headers.get("X-Artifact-Digest") == sd2["artifact_digest"])
            check("S2 repaired records are S2 records (ts/lat/lon)",
                  [(r.get("ts"), r.get("lat"), r.get("lon")) for r in doc["records"]]
                  == [(r["ts"], r["lat"], r["lon"]) for r in same_rules_recs_b])
            check("S2 repaired input_digest is its own",
                  doc.get("input_digest") == sd2["input_digest"])
        code, body, _ = download(s1)
        check("S1 download untouched by S2 repair (identity + digest)",
              code == 200 and as_json(body).get("export_id") == s1
              and hashlib.sha256(body).hexdigest() == sd1["artifact_digest"])
        d = wait_for_stage(s2, "PUBLISHED", 3)
        check("S2 never regressed and stays terminal", d["stage"] == "PUBLISHED")
        check("quarantine holds the displaced foreign artifact",
              len(os.listdir(os.path.join(DATA_DIR, "artifacts", "quarantine"))) >= 1)

    step("值班员改规则 → R2（遮蔽 lat）")
    status, raw, _ = req("PUT", "/api/rules", rules_r2)
    check("put rules R2", status == 200)
    d2 = as_json(raw)["digest"]
    check("R2 digest differs", d2 != d1)

    step("规则改动后重传 E1（同记录）→ 409 冲突（规则快照不同）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    conflict = as_json(raw)
    check("replay under changed rules -> 409", status == 409, "HTTP %s" % status)
    check("conflict preserves original evidence",
          conflict.get("existing", {}).get("rules_digest") == d1
          and conflict.get("existing", {}).get("receipt_id") == receipt1["receipt_id"])

    step("不同记录重传 E1 → 409 冲突（记录不同）")
    changed = [dict(r, depth_m=99.9) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": changed})
    check("different records -> 409", status == 409, "HTTP %s" % status)

    detail = wait_for_stage(e1, "PUBLISHED", 5)
    check("E1 evidence untouched after conflicts",
          detail is not None and detail["artifact_digest"] == a1 and detail["stage"] == "PUBLISHED")

    step("规则改动后，E1 仍按冻结快照导出；新导出 E2 使用 R2")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": recs})
    check("submit E2 -> 201", status == 201)
    check("E2 froze rules R2", as_json(raw).get("rules_digest") == d2)
    detail2 = wait_for_stage(e2, "PUBLISHED", 90)
    check("E2 published", detail2 is not None and detail2["stage"] == "PUBLISHED")
    status, raw, _ = download(e2)
    if status == 200:
        doc = as_json(raw)
        check("E2 masked per R2 (lat redacted, depth kept)",
              doc["records"][0]["lat"] == "***" and doc["records"][0]["depth_m"] == 42.51)
    else:
        check("download E2 -> 200", False, "HTTP %s" % status)
    status, raw, _ = download(e1)
    check("E1 re-download unchanged after rule change",
          status == 200 and hashlib.sha256(raw).hexdigest() == a1)
    if status == 200:
        doc = as_json(raw)
        check("E1 still follows frozen R1 (depth redacted, lat kept)",
              doc["records"][0]["depth_m"] == "***" and doc["records"][0]["lat"] == 31.230416)

    step("崩溃恢复 A：暂存完整工件后进程退出 → 收敛到同一完整工件")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e3, "mode": "crash_after_staged"})
    check("arm fault crash_after_staged", status == 202, "HTTP %s %s" % (status, raw[:200]))
    status, raw, _ = req("POST", "/api/exports", {"export_id": e3, "records": recs})
    check("submit E3 -> 201", status == 201)
    detail3 = wait_for_stage(e3, "PUBLISHED", 150)
    check("E3 published after crash+recovery", detail3 is not None and detail3["stage"] == "PUBLISHED",
          "last=%s" % (detail3 and detail3.get("stage")))
    if detail3:
        events = detail3.get("events", [])
        recovered = any("recovery" in (e.get("detail") or "") or "recover" in e.get("event", "")
                        for e in events)
        check("E3 journal shows recovery convergence", recovered,
              "events=%s" % [e["event"] for e in events])
        status, raw, _ = download(e3)
        check("E3 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail3["artifact_digest"])
        check("exactly one published artifact file for E3", len(published_files(e3)) == 1)
        check("no temp leftovers for E3", tmp_files(e3) == [], str(tmp_files(e3)))

    step("崩溃恢复 B：临时工件写一半进程退出 → 清理残缺工件并重处理")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e4, "mode": "crash_partial_write"})
    check("arm fault crash_partial_write", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e4, "records": recs})
    check("submit E4 -> 201", status == 201)
    detail4 = wait_for_stage(e4, "PUBLISHED", 150)
    check("E4 published after cleanup+requeue", detail4 is not None and detail4["stage"] == "PUBLISHED",
          "last=%s" % (detail4 and detail4.get("stage")))
    if detail4:
        events = [e["event"] for e in detail4.get("events", [])]
        check("E4 journal shows cleanup/requeue",
              "recovery_cleanup" in events or "requeued" in events, "events=%s" % events)
        check("E4 needed a retry", detail4["attempts"] >= 1)
        check("no temp leftovers for E4", tmp_files(e4) == [], str(tmp_files(e4)))
        status, raw, _ = download(e4)
        check("E4 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail4["artifact_digest"])

    step("下载接口不暴露未核验内容（崩溃窗口内只能 409，不能 200）")
    req("POST", "/api/test/fault", {"export_id": e5, "mode": "crash_partial_write"})
    status, raw, _ = req("POST", "/api/exports", {"export_id": e5, "records": recs})
    check("submit E5 -> 201", status == 201)
    saw_409 = False
    exposed = False
    deadline = time.time() + 150
    while time.time() < deadline:
        code, body, _ = download(e5)
        d = wait_for_stage(e5, "PUBLISHED", 1)  # stage probe after the download
        if code == 200:
            # 200 is legitimate only when the export is PUBLISHED (stage never
            # regresses, so a PUBLISHED probe after the 200 is conclusive).
            if d and d["stage"] == "PUBLISHED":
                break
            exposed = True
            break
        if code == 409:
            saw_409 = True
        time.sleep(0.4)
    check("unpublished content never served", not exposed)
    check("download refused while unverified (409 observed)", saw_409)

    step("发布后的业务等价重传：仍返回首次回执且不产生第二个工件")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": reordered})
    replay2 = as_json(raw)
    check("replay E2 -> 200 first receipt", status == 200 and replay2.get("replay") is True)
    detail2b = wait_for_stage(e2, "PUBLISHED", 5)
    check("E2 artifact digest unchanged by replay",
          detail2b is not None and detail2b["artifact_digest"] == detail2["artifact_digest"])
    check("exactly one published artifact file for E2", len(published_files(e2)) == 1)

    step("终态检查：无残缺临时工件残留")
    leftovers = []
    for eid in (e1, e2, e3, e4, e5):
        leftovers.extend(tmp_files(eid))
    check("no temp artifacts left behind", leftovers == [], str(leftovers))


def main():
    print("verify: one-shot acceptance run %s against %s" % (RUN, API), flush=True)
    build_checks()
    unit_tests()
    wait_for_api()
    smoke()
    print("\n==============================================")
    if FAILURES:
        print("verify: FAILED (%d): %s" % (len(FAILURES), ", ".join(FAILURES)), flush=True)
        return 1
    print("verify: ALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
