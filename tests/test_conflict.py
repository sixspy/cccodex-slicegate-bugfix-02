
import threading
import pytest
from fastapi.testclient import TestClient
from app import app, init_db

@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield

def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username":username,"password":password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}

def _slices(c, h, no=None):
    items = c.get("/api/slices", headers=h).json()["items"]
    return [i for i in items if no is None or i["slice_no"] == no]

def _conflicts(c, h, **params):
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return c.get("/api/conflicts" + (f"?{q}" if q else ""), headers=h).json()["items"]

def test_pipeline():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "日切放行"
    h = _login(c)
    assert len(c.get("/api/slices", headers=h).json()["items"]) >= 1
    # partial cannot release
    bad = c.post("/api/slices/1/release", headers=h, json={"consumer":"x"})
    assert bad.status_code == 409
    # register complete and release
    ok = c.post("/api/slices", headers=h, json={"slice_no":"SL-T-1","biz_date":"2026-11-04","partition_key":"mid","row_count":10,"expected_count":10})
    assert ok.status_code == 200
    sid = ok.json()["id"]
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer":"ads_x"})
    assert rel.status_code == 200
    assert any(w["source_slice"]=="SL-T-1" for w in c.get("/api/watermarks", headers=h).json()["items"])


def test_duplicate_slice_no_never_overwrites_old_row():
    c = TestClient(app)
    h = _login(c)
    old = _slices(c, h, "SL-2026-11-01-east")[0]
    before_wm = c.get("/api/watermarks", headers=h).json()["items"]

    r = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-2026-11-01-east", "biz_date": "2026-11-09",
        "partition_key": "east", "row_count": 1000, "expected_count": 1000,
    })
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == "slice_no_conflict"
    assert body["overwrote"] is False
    assert body["watermark_pushed"] is False

    after = _slices(c, h, "SL-2026-11-01-east")
    assert len(after) == 1, "切片号冲突不得新增/复制正式行"
    assert after[0]["id"] == old["id"]
    assert after[0]["row_count"] == old["row_count"] == 980
    assert after[0]["expected_count"] == old["expected_count"] == 1000
    assert after[0]["completeness"] == old["completeness"] == "部分完整"
    assert after[0]["release_status"] == old["release_status"]
    assert c.get("/api/watermarks", headers=h).json()["items"] == before_wm


def test_conflict_is_recorded_in_ledger_and_attempt_ledger():
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-2026-11-01-east", "biz_date": "2026-11-01",
        "partition_key": "east", "row_count": 1000, "expected_count": 1000,
    })
    assert r.status_code == 409
    cid = r.json()["conflict_id"]
    ledger = _conflicts(c, h)
    assert len(ledger) == 1
    item = ledger[0]
    assert item["id"] == cid
    assert item["slice_no"] == "SL-2026-11-01-east"
    assert item["outcome"] == "待审"
    assert item["existing_row_count"] == 980
    assert item["incoming_row_count"] == 1000
    assert item["existing_completeness"] == "部分完整"
    assert item["incoming_completeness"] == "完整"
    detail = c.get(f"/api/conflicts/{cid}", headers=h).json()
    assert detail["existing_slice"]["row_count"] == 980
    attempts = c.get("/api/registration-attempts?slice_no=SL-2026-11-01-east", headers=h).json()["items"]
    assert any(a["conflict_id"] == cid and a["outcome"] == "conflict" for a in attempts)


def test_merge_policy_resolution_creates_new_row_and_keeps_old():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date": "2026-11-10", "policy": "merge_review"})
    first = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-M", "biz_date": "2026-11-10", "partition_key": "north",
        "row_count": 600, "expected_count": 1000,
    })
    assert first.status_code == 200
    old_id = first.json()["id"]
    dup = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-M", "biz_date": "2026-11-10", "partition_key": "north",
        "row_count": 1200, "expected_count": 1000,
    })
    assert dup.status_code == 409
    cid = dup.json()["conflict_id"]
    # 待审期间旧行不放行、不推水位
    assert c.post(f"/api/slices/{old_id}/release", headers=h, json={"consumer":"ads_y"}).status_code == 409
    before = len(c.get("/api/watermarks", headers=h).json()["items"])

    res = c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"merge","note":"确认合并"})
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["outcome"] == "已合并"
    assert out["slice_no"] == "SL-M-R1"
    assert out["row_count"] == 1200
    assert out["expected_count"] == 1000
    assert out["release_status"] == "未放行"
    assert out["watermark_pushed"] is False

    old = [s for s in _slices(c, h) if s["id"] == old_id][0]
    assert old["row_count"] == 600 and old["completeness"] == "部分完整"
    new = _slices(c, h, "SL-M-R1")[0]
    assert new["row_count"] == 1200 and new["release_status"] == "未放行"
    assert len(c.get("/api/watermarks", headers=h).json()["items"]) == before

    # 已处置的冲突不允许再处置
    again = c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"merge"})
    assert again.status_code == 409
    assert again.json()["code"] == "policy_deviation"
    assert len(c.get("/api/deviations", headers=h).json()["items"]) >= 1


def test_merge_rejected_creates_no_new_row():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date": "2026-11-10", "policy": "merge_review"})
    c.post("/api/slices", headers=h, json={"slice_no":"SL-N","biz_date":"2026-11-10","partition_key":"n","row_count":1,"expected_count":1})
    dup = c.post("/api/slices", headers=h, json={"slice_no":"SL-N","biz_date":"2026-11-10","partition_key":"n","row_count":2,"expected_count":2})
    cid = dup.json()["conflict_id"]
    res = c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"reject"})
    assert res.status_code == 200
    assert res.json()["outcome"] == "已驳回"
    assert res.json()["derived_slice_id"] is None
    assert len(_slices(c, h, "SL-N")) == 1


def test_reject_policy_fails_explicitly_and_keeps_row():
    c = TestClient(app)
    h = _login(c)
    lock = c.post("/api/policies", headers=h, json={"biz_date": "2026-11-11", "policy": "reject"})
    assert lock.status_code == 200
    first = c.post("/api/slices", headers=h, json={"slice_no":"SL-R","biz_date":"2026-11-11","partition_key":"s","row_count":5,"expected_count":5})
    assert first.status_code == 200
    dup = c.post("/api/slices", headers=h, json={"slice_no":"SL-R","biz_date":"2026-11-11","partition_key":"s","row_count":9,"expected_count":9})
    assert dup.status_code == 409
    assert dup.json()["outcome"] == "已拒绝"
    assert dup.json()["overwrote"] is False
    assert dup.json()["policy"] == "reject"
    rows = _slices(c, h, "SL-R")
    assert len(rows) == 1 and rows[0]["row_count"] == 5
    assert _conflicts(c, h)[0]["outcome"] == "已拒绝"


def test_day_policy_is_locked_and_mixing_is_rejected():
    c = TestClient(app)
    h = _login(c)
    assert c.post("/api/policies", headers=h, json={"biz_date":"2026-11-12","policy":"reject"}).status_code == 200
    switch = c.post("/api/policies", headers=h, json={"biz_date":"2026-11-12","policy":"merge_review"})
    assert switch.status_code == 409
    assert switch.json()["code"] == "policy_deviation"
    mix = c.post("/api/slices", headers=h, json={
        "slice_no":"SL-Z","biz_date":"2026-11-12","partition_key":"z",
        "row_count":1,"expected_count":1,"policy":"merge_review"})
    assert mix.status_code == 409
    assert mix.json()["code"] == "policy_deviation"
    assert _slices(c, h, "SL-Z") == []
    dev = c.get("/api/deviations", headers=h).json()["items"]
    assert len(dev) >= 2
    assert all("登记口径偏了" in d["detail"] for d in dev)
    locked = c.get("/api/policies/2026-11-12", headers=h).json()
    assert locked["policy"] == "reject"


def test_completeness_rule_cannot_be_changed():
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/slices", headers=h, json={
        "slice_no":"SL-C","biz_date":"2026-11-13","partition_key":"c",
        "row_count":1,"expected_count":1,"completeness_rule":"row_gt_zero_v0"})
    assert r.status_code == 409
    assert r.json()["code"] == "policy_deviation"
    assert _slices(c, h, "SL-C") == []


def test_duplicate_identity_mismatch_cannot_be_merged():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date":"2026-11-14","policy":"merge_review"})
    c.post("/api/slices", headers=h, json={"slice_no":"SL-D","biz_date":"2026-11-14","partition_key":"d1","row_count":1,"expected_count":1})
    dup = c.post("/api/slices", headers=h, json={"slice_no":"SL-D","biz_date":"2026-11-14","partition_key":"d2","row_count":2,"expected_count":2})
    assert dup.status_code == 409
    cid = dup.json()["conflict_id"]
    res = c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"merge"})
    assert res.status_code == 409
    assert res.json()["code"] == "policy_deviation"
    assert _slices(c, h, "SL-D-R1") == []


def test_concurrent_duplicate_registration_queues_and_records_contention():
    c = TestClient(app)
    h = _login(c)
    h2 = _login(c)  # 另一名值班：独立登录会话
    c.post("/api/policies", headers=h, json={"biz_date":"2026-11-15","policy":"merge_review"})
    payload_a = {"slice_no":"SL-Q","biz_date":"2026-11-15","partition_key":"q","row_count":10,"expected_count":10}
    payload_b = {"slice_no":"SL-Q","biz_date":"2026-11-15","partition_key":"q","row_count":20,"expected_count":20}
    results = []
    barrier = threading.Barrier(2)

    def submit(session, payload):
        client = TestClient(app)
        barrier.wait()
        results.append(client.post("/api/slices", headers=session, json=payload))

    threads = [threading.Thread(target=submit, args=(h, payload_a)),
               threading.Thread(target=submit, args=(h2, payload_b))]
    for t in threads: t.start()
    for t in threads: t.join()

    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409], [r.text for r in results]
    rows = _slices(c, h, "SL-Q")
    assert len(rows) == 1
    conflict = [r for r in results if r.status_code == 409][0].json()
    assert conflict["contended"] is True
    assert conflict["overwrote"] is False
    ledger = _conflicts(c, h)
    assert len(ledger) == 1 and ledger[0]["contended"] is True
    attempts = c.get("/api/registration-attempts?slice_no=SL-Q", headers=h).json()["items"]
    assert any(a["contended"] for a in attempts)
    assert all(a["outcome"] != "进行中" for a in attempts)


def test_partition_detail_reconciles_with_list_and_conflict_ledger():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date":"2026-11-16","policy":"merge_review"})
    c.post("/api/slices", headers=h, json={"slice_no":"SL-P","biz_date":"2026-11-16","partition_key":"p","row_count":300,"expected_count":500})
    c.post("/api/slices", headers=h, json={"slice_no":"SL-P","biz_date":"2026-11-16","partition_key":"p","row_count":400,"expected_count":500})

    listed = _slices(c, h)
    detail = c.get("/api/partitions/2026-11-16/p", headers=h).json()
    assert detail["reconciles"] is True
    assert detail["conflict_rows_match"] is True
    assert detail["summary"]["slice_count"] == len(detail["items"])
    assert detail["summary"]["row_total"] == sum(i["row_count"] for i in detail["items"])
    assert detail["summary"]["pending_conflicts"] == len(_conflicts(c, h, outcome="待审"))
    all_listed_rows = sum(i["row_count"] for i in listed if i["biz_date"]=="2026-11-16" and i["partition_key"]=="p")
    assert detail["summary"]["row_total"] == all_listed_rows

    parts = c.get("/api/partitions", headers=h).json()
    assert parts["reconciles"] is True
    assert parts["totals"]["partition_row_total"] == parts["totals"]["row_total"]
    assert parts["totals"]["partition_pending_conflicts"] == parts["totals"]["pending_conflicts"]


def test_repeated_conflicts_derive_distinct_revision_numbers():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date":"2026-12-05","policy":"merge_review"})
    c.post("/api/slices", headers=h, json={"slice_no":"SL-RV","biz_date":"2026-12-05","partition_key":"r","row_count":100,"expected_count":100})
    no = set()
    for i in range(2):
        dup = c.post("/api/slices", headers=h, json={
            "slice_no":"SL-RV","biz_date":"2026-12-05","partition_key":"r",
            "row_count":200+i,"expected_count":100})
        cid = dup.json()["conflict_id"]
        out = c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"merge"}).json()
        no.add(out["slice_no"])
    assert no == {"SL-RV-R1", "SL-RV-R2"}
    assert len(_slices(c, h, "SL-RV")) == 1
    assert _slices(c, h, "SL-RV")[0]["row_count"] == 100


def test_release_unblocks_after_conflict_is_resolved_without_watermark_on_conflict():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date":"2026-12-06","policy":"merge_review"})
    first = c.post("/api/slices", headers=h, json={
        "slice_no":"SL-BK","biz_date":"2026-12-06","partition_key":"b","row_count":50,"expected_count":50})
    sid = first.json()["id"]
    dup = c.post("/api/slices", headers=h, json={
        "slice_no":"SL-BK","biz_date":"2026-12-06","partition_key":"b","row_count":60,"expected_count":60})
    cid = dup.json()["conflict_id"]
    before = len(c.get("/api/watermarks", headers=h).json()["items"])
    blocked = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer":"ads_z"})
    assert blocked.status_code == 409
    assert len(c.get("/api/watermarks", headers=h).json()["items"]) == before

    assert c.post(f"/api/conflicts/{cid}/resolve", headers=h, json={"action":"reject"}).status_code == 200
    assert len(c.get("/api/watermarks", headers=h).json()["items"]) == before, "处置冲突本身不得推水位"
    ok = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer":"ads_z"})
    assert ok.status_code == 200
    assert any(w["source_slice"] == "SL-BK" for w in c.get("/api/watermarks", headers=h).json()["items"])


def test_deviation_registrations_are_closed_in_attempt_ledger():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/policies", headers=h, json={"biz_date":"2026-12-11","policy":"reject"})
    r = c.post("/api/slices", headers=h, json={
        "slice_no":"SL-DV","biz_date":"2026-12-11","partition_key":"d",
        "row_count":1,"expected_count":1,"policy":"merge_review"})
    assert r.status_code == 409 and r.json()["code"] == "policy_deviation"
    attempts = c.get("/api/registration-attempts?slice_no=SL-DV", headers=h).json()["items"]
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "deviation"
    assert attempts[0]["finished_at"] is not None


def test_health_reports_ledgers():
    c = TestClient(app)
    h = _login(c)
    c.post("/api/slices", headers=h, json={"slice_no":"SL-2026-11-01-east","biz_date":"2026-11-01","partition_key":"east","row_count":1,"expected_count":1})
    health = c.get("/api/health").json()
    assert health["conflicts"] == 1
    assert health["pending_conflicts"] == 1
    assert health["day_policies"] >= 1
