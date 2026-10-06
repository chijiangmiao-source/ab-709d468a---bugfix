"""One-shot acceptance service: build checks + unit tests + API/HTTP smoke.

Runs the build check (byte-compile) and the unit test suite, then exercises
the live HTTP API through the required scenarios:

  1. export keeps following the frozen rules snapshot after rules change
  2. crash recovery (converge a complete staged artifact; clean up a partial one)
  3. business-equivalent retransmission (first receipt, no second artifact)
     and conflict handling (different records or rules snapshot)
  4. two exports under the SAME rules but with distinct records: each
     published download must carry only its own export id, input digest,
     records and artifact digest; then restart app+workers and re-verify

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
RESTART_CMD = os.environ.get("RESTART_CMD")
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


# ------------------------------------------------------------------ restart

def _docker_raw(method, path, body=b""):
    """Minimal Docker Engine API call over the local unix socket."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(15)
    sock.connect(DOCKER_SOCKET)
    headers = {
        "Host": "docker",
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "Connection": "close",
    }
    head = "%s %s HTTP/1.1\r\n%s\r\n\r\n" % (
        method, path, "\r\n".join("%s: %s" % kv for kv in headers.items()))
    sock.sendall(head.encode("ascii") + body)
    chunks = []
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        chunks.append(chunk)
    sock.close()
    raw = b"".join(chunks)
    head_part, _, payload = raw.partition(b"\r\n\r\n")
    status_line = head_part.split(b"\r\n", 1)[0].decode("ascii", "replace")
    status_code = int(status_line.split(" ")[1])
    # de-chunk trivial responses (Docker JSON is usually sent unchunked here)
    lowered = head_part.lower()
    if b"transfer-encoding: chunked" in lowered:
        out = b""
        rest = payload
        while rest:
            line, _, rest = rest.partition(b"\r\n")
            if not line:
                break
            n = int(line.split(b";", 1)[0], 16)
            if n == 0:
                break
            out += rest[:n]
            rest = rest[n + 2:]
        payload = out
    return status_code, payload


def restart_app_and_workers():
    """Restart the compose app + worker containers; fall back to RESTART_CMD.

    Returns True when a restart was triggered.
    """
    if RESTART_CMD:
        print("restart via RESTART_CMD: %s" % RESTART_CMD, flush=True)
        proc = subprocess.run(RESTART_CMD, shell=True, capture_output=True, text=True)
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr)
        return proc.returncode == 0
    if not os.path.exists(DOCKER_SOCKET):
        print("no docker socket at %s and RESTART_CMD unset" % DOCKER_SOCKET, flush=True)
        return False
    cid = socket.gethostname()
    status, raw = _docker_raw("GET", "/containers/%s/json" % cid)
    if status != 200:
        print("inspect own container failed: %s %s" % (status, raw[:200]), flush=True)
        return False
    labels = (json.loads(raw).get("Config", {}).get("Labels") or {})
    project = labels.get("com.docker.compose.project")
    if not project:
        print("own container has no compose project label", flush=True)
        return False
    filt = json.dumps({"label": ["com.docker.compose.project=%s" % project]})
    status, raw = _docker_raw(
        "GET", "/containers/json?all=1&filters=" + urllib.request.quote(filt, safe=""))
    if status != 200:
        print("list project containers failed: %s" % status, flush=True)
        return False
    targets = []
    for c in json.loads(raw):
        labels = c.get("Labels") or {}
        service = labels.get("com.docker.compose.service")
        if c.get("State") == "running" and service in ("app", "worker"):
            targets.append((c["Id"][:12], service))
    print("restarting compose services: %s" % targets, flush=True)
    ok = True
    for cid12, _name in targets:
        code, _ = _docker_raw("POST", "/containers/%s/restart?t=2" % cid12)
        ok = ok and code in (204, 304)
    return ok


def wait_healthy_after_restart():
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            status, raw, _ = req("GET", "/healthz")
            if status == 200 and as_json(raw).get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


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
    recs = [
        {"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "lon": 121.473701, "depth_m": 42.51, "vessel_id": "HAICE-01"},
        {"ts": "2026-10-06T01:05:00Z", "lat": 31.231102, "lon": 121.480233, "depth_m": 43.04, "vessel_id": "HAICE-01"},
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


def smoke_two_exports_same_rules():
    """核心场景：同规则、不同记录的两份导出，各自身份/摘要/记录互不串用；
    错误发布安全收敛；重启后第二份仍可下载、可核验。"""
    sa, sb = ("SAME%d-%s" % (i, RUN) for i in (1, 3))
    recs_a = [
        {"ts": "2026-10-06T02:00:00Z", "lat": 30.11, "lon": 122.01, "depth_m": 11.1, "vessel_id": "HAICE-A"},
        {"ts": "2026-10-06T02:05:00Z", "lat": 30.12, "lon": 122.02, "depth_m": 11.2, "vessel_id": "HAICE-A"},
    ]
    recs_b = [
        {"ts": "2026-10-06T03:00:00Z", "lat": 41.05, "lon": 113.12, "depth_m": 77.7, "vessel_id": "HAICE-B"},
        {"ts": "2026-10-06T03:09:00Z", "lat": 41.18, "lon": 113.33, "depth_m": 78.2, "vessel_id": "HAICE-B"},
    ]
    rules_r3 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 12},
    ]}

    def expect_own_artifact(eid, recs, receipt, other_body=None):
        detail = wait_for_stage(eid, "PUBLISHED", 90)
        check("%s published" % eid, detail is not None and detail["stage"] == "PUBLISHED",
              "last=%s" % (detail and detail.get("stage")))
        code, body, headers = download(eid)
        check("%s download -> 200" % eid, code == 200, "HTTP %s" % code)
        if code != 200:
            return None, None
        digest_header = headers.get("X-Artifact-Digest")
        body_digest = hashlib.sha256(body).hexdigest()
        check("%s X-Artifact-Digest is verifiable" % eid,
              digest_header == body_digest == detail["artifact_digest"],
              "header=%s body=%s detail=%s" % (digest_header, body_digest[:16], detail["artifact_digest"][:16]))
        doc = as_json(body)
        check("%s artifact binds its own identity" % eid,
              doc["export_id"] == eid
              and doc["receipt_id"] == receipt["receipt_id"]
              and doc["input_digest"] == receipt["input_digest"]
              and doc["rules_digest"] == receipt["rules_digest"],
              "doc identity=%s/%s" % (doc.get("export_id"), doc.get("receipt_id")))
        check("%s artifact carries its own masked records" % eid,
              [r["ts"] for r in doc["records"]] == [r["ts"] for r in recs]
              and all(r["depth_m"] == "***" for r in doc["records"])
              and all(r["vessel_id"] not in ("HAICE-A", "HAICE-B") for r in doc["records"])
              and doc["records"][0]["lat"] == recs[0]["lat"],
              "records=%s" % doc.get("records"))
        check("exactly one published artifact file for %s" % eid,
              len(published_files(eid)) == 1, str(published_files(eid)))
        if other_body is not None:
            check("%s bytes differ from the other export" % eid, body != other_body)
            other = as_json(other_body)
            check("%s download contains no foreign identity" % eid,
                  other["export_id"] not in body.decode("utf-8")
                  and other["receipt_id"] not in body.decode("utf-8"))
        return doc, body_digest

    step("同规则两份导出：设置规则 R3（redact depth / hash vessel）")
    status, raw, _ = req("PUT", "/api/rules", rules_r3)
    check("put rules R3", status == 200, "HTTP %s %s" % (status, raw[:200]))
    d3 = as_json(raw)["digest"]

    step("提交并发布 SA（记录集 A）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": sa, "records": recs_a})
    check("submit SA -> 201", status == 201, "HTTP %s %s" % (status, raw[:200]))
    receipt_a = as_json(raw)
    doc_a, digest_a = expect_own_artifact(sa, recs_a, receipt_a)

    step("同规则、不同记录提交 SB（记录集 B）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": sb, "records": recs_b})
    check("submit SB -> 201", status == 201, "HTTP %s" % status)
    receipt_b = as_json(raw)
    check("SB froze the same rules snapshot", receipt_b["rules_digest"] == d3 == receipt_a["rules_digest"])
    check("SB input digest differs from SA", receipt_b["input_digest"] != receipt_a["input_digest"])
    body_a = download(sa)[1]
    doc_b, digest_b = expect_own_artifact(sb, recs_b, receipt_b, other_body=body_a)
    check("SA/SB artifact digests differ", digest_a != digest_b)

    step("SA 业务等价重传仍返回首次回执，不产生第二个工件")
    reordered_a = [dict(reversed(list(r.items()))) for r in recs_a]
    status, raw, _ = req("POST", "/api/exports", {"export_id": sa, "records": reordered_a})
    replay = as_json(raw)
    check("SA replay -> 200 first receipt",
          status == 200 and replay.get("replay") is True
          and replay["receipt_id"] == receipt_a["receipt_id"], "HTTP %s" % status)
    detail_a = wait_for_stage(sa, "PUBLISHED", 5)
    check("SA digest/file unchanged after replay",
          detail_a is not None and detail_a["artifact_digest"] == digest_a
          and len(published_files(sa)) == 1)

    step("注入历史错误发布：SB 显示 PUBLISHED，但下载字节属于 SA")
    status, raw, _ = req("POST", "/api/test/corrupt",
                         {"export_id": sb, "source_export_id": sa})
    check("corruption injected", status == 202, "HTTP %s %s" % (status, raw[:200]))
    detail_bad = wait_for_stage(sb, "PUBLISHED", 5)
    check("SB stays PUBLISHED (stage never regresses)",
          detail_bad is not None and detail_bad["stage"] == "PUBLISHED")
    code, body_bad, _ = download(sb)
    check("corrupted SB download is refused (never serves SA bytes)",
          code != 200, "unexpected HTTP %s" % code)

    step("后台和解：SB 安全收敛回自己可核验的工件（阶段保持 PUBLISHED）")
    saw_refusal = False
    served_foreign = False
    converged = False
    deadline = time.time() + 60
    while time.time() < deadline:
        detail = wait_for_stage(sb, "PUBLISHED", 1)
        code, body, _ = download(sb)
        if code == 200:
            doc = as_json(body)
            if doc["export_id"] != sb or doc["input_digest"] != receipt_b["input_digest"]:
                served_foreign = True
                break
            if hashlib.sha256(body).hexdigest() == digest_b:
                converged = True
                break
        elif code in (410, 500):
            saw_refusal = True
        check_d = detail
        if check_d and check_d["stage"] != "PUBLISHED":
            check("SB stage never leaves PUBLISHED during repair", False, check_d["stage"])
        time.sleep(0.3)
    check("unverified cross-export bytes never served", not served_foreign)
    check("SB converged to its own artifact", converged)
    if saw_refusal:
        print("[PASS] download refused during the repair window (410/500 observed)", flush=True)
    detail_b = wait_for_stage(sb, "PUBLISHED", 5)
    check("SB stage still PUBLISHED after convergence",
          detail_b is not None and detail_b["stage"] == "PUBLISHED")
    events = [e["event"] for e in detail_b.get("events", [])]
    check("SB journal records the correction", "artifact_corrected" in events, "events=%s" % events)
    code, body, headers = download(sb)
    check("SB re-download fully verifiable after convergence",
          code == 200
          and headers.get("X-Artifact-Digest") == hashlib.sha256(body).hexdigest() == digest_b
          and as_json(body)["records"][0]["vessel_id"] != doc_a["records"][0]["vessel_id"]
          and as_json(body)["records"][0]["ts"] == recs_b[0]["ts"],
          "HTTP %s" % code)
    code, body_a2, _ = download(sa)
    check("SA untouched by SB repair",
          code == 200 and hashlib.sha256(body_a2).hexdigest() == digest_a)

    step("重启 app 与后台 worker，再次核对两份导出（重点是 SB）")
    restarted = restart_app_and_workers()
    check("restart triggered", restarted,
          "set DOCKER_SOCKET mount or RESTART_CMD to exercise the restart check")
    if restarted:
        check("health response after restart", wait_healthy_after_restart())
        # give restarted workers a startup reconcile tick
        time.sleep(2)
        for eid, recs, receipt, want_digest in (
            (sa, recs_a, receipt_a, digest_a),
            (sb, recs_b, receipt_b, digest_b),
        ):
            detail = wait_for_stage(eid, "PUBLISHED", 30)
            code, body, headers = download(eid)
            ok = (
                detail is not None and detail["stage"] == "PUBLISHED"
                and code == 200
                and headers.get("X-Artifact-Digest") == hashlib.sha256(body).hexdigest() == want_digest
            )
            if ok:
                doc = as_json(body)
                ok = (
                    doc["export_id"] == eid
                    and doc["receipt_id"] == receipt["receipt_id"]
                    and doc["input_digest"] == receipt["input_digest"]
                    and [r["ts"] for r in doc["records"]] == [r["ts"] for r in recs]
                    and len(published_files(eid)) == 1
                )
            check("%s still correct after restart" % eid, ok,
                  "stage=%s http=%s" % (detail and detail.get("stage"), code))


def main():
    print("verify: one-shot acceptance run %s against %s" % (RUN, API), flush=True)
    build_checks()
    unit_tests()
    wait_for_api()
    smoke()
    smoke_two_exports_same_rules()
    print("\n==============================================")
    if FAILURES:
        print("verify: FAILED (%d): %s" % (len(FAILURES), ", ".join(FAILURES)), flush=True)
        return 1
    print("verify: ALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
