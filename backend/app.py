
import hashlib, secrets, sqlite3, threading, uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# ---------------------------------------------------------------------------
# 登记口径（互斥策略）
#
# 甲：切片号冲突直接拒绝登记。
# 乙：切片号冲突进入合并待审队列，不覆盖正式行，人工确认后生成新行或驳回。
#
# 同一业务日只能锁定一种口径，且正确性口径（完整度规则）与合并规则一并锁定，
# 当天不得更换。两种口径绝不会混用于同一天。
# ---------------------------------------------------------------------------
POLICY_REJECT = "reject"
POLICY_MERGE = "merge_review"
POLICIES = (POLICY_MERGE, POLICY_REJECT)
DEFAULT_POLICY = POLICY_MERGE
POLICY_LABEL = {
    POLICY_REJECT: "甲 · 冲突直接拒绝登记",
    POLICY_MERGE: "乙 · 冲突进合并待审队列",
}
DEFAULT_POLICY_SOURCE = "registration_default"

COMPLETENESS_RULE = "row_ge_expected_v1"
COMPLETENESS_RULE_TEXT = (
    "expected_count>0 且 row_count>=expected_count 为“完整”；"
    "row_count>0 为“部分完整”；否则为“待检”"
)
MERGE_RULE = "same_identity_max_counts_revision_v1"
MERGE_RULE_TEXT = (
    "仅同业务日、同分区可合并；新行 row_count/expected_count 取旧行与本次登记两报较大值；"
    "新切片号派生为 <原切片号>-R<序号>；旧正式行不改动；"
    "合并后仍为“未放行”，需重新裁决并显式放行；合并与冲突处置均不推消费水位"
)

CONFLICT_PENDING = "待审"
CONFLICT_REJECTED = "已拒绝"
CONFLICT_MERGED = "已合并"
CONFLICT_DISMISSED = "已驳回"
CONFLICT_OPEN_STATES = (CONFLICT_PENDING,)
# 并发受理的判定窗口：两笔同切片号受理的存活区间重叠即算登记争用。
# 孤儿“进行中”记录只在窗口内有效，避免崩溃残留永久污染判定。
CONTENTION_GRACE_SECONDS = 2
STALE_ATTEMPT_SECONDS = 120

_locks_guard = threading.Lock()
_slice_locks: dict[str, threading.Lock] = {}


def _slice_lock(slice_no: str) -> threading.Lock:
    with _locks_guard:
        lock = _slice_locks.get(slice_no)
        if lock is None:
            lock = threading.Lock()
            _slice_locks[slice_no] = lock
        return lock


def connect() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def read_conn():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx():
    """独占写事务：同切片号由进程内锁排队，跨连接由 BEGIN IMMEDIATE 串行化。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="microseconds")


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def calc_completeness(row_count: int, expected_count: int) -> str:
    if expected_count > 0 and row_count >= expected_count:
        return "完整"
    if row_count > 0:
        return "部分完整"
    return "待检"


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info('{table}')")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS day_policies(
          biz_date TEXT PRIMARY KEY,
          policy TEXT NOT NULL CHECK(policy IN ('reject','merge_review')),
          completeness_rule TEXT NOT NULL,
          merge_rule TEXT NOT NULL,
          locked_by TEXT NOT NULL,
          locked_at TEXT NOT NULL,
          source TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS registration_conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          existing_slice_id INTEGER NOT NULL,
          existing_row_count INTEGER NOT NULL,
          existing_expected_count INTEGER NOT NULL,
          existing_completeness TEXT NOT NULL,
          incoming_row_count INTEGER NOT NULL,
          incoming_expected_count INTEGER NOT NULL,
          incoming_completeness TEXT NOT NULL,
          policy TEXT NOT NULL,
          outcome TEXT NOT NULL,
          reason TEXT NOT NULL,
          operator TEXT NOT NULL,
          contended INTEGER NOT NULL DEFAULT 0,
          attempt_id TEXT NOT NULL,
          resolved_slice_id INTEGER,
          resolved_by TEXT,
          resolved_at TEXT,
          resolution_note TEXT,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS registration_attempts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          attempt_id TEXT NOT NULL,
          slice_no TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          operator TEXT NOT NULL,
          session_id TEXT,
          contended INTEGER NOT NULL DEFAULT 0,
          outcome TEXT NOT NULL,
          conflict_id INTEGER,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL,
          finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS registration_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          biz_date TEXT,
          conflict_id INTEGER,
          operator TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_conflict_outcome ON registration_conflicts(outcome);
        CREATE INDEX IF NOT EXISTS idx_conflict_identity ON registration_conflicts(biz_date, partition_key);
        CREATE INDEX IF NOT EXISTS idx_conflict_slice_no ON registration_conflicts(slice_no);
        CREATE INDEX IF NOT EXISTS idx_slices_identity ON slices(biz_date, partition_key);
        CREATE INDEX IF NOT EXISTS idx_attempt_slice_no ON registration_attempts(slice_no);
        """)
        # 旧文件库升级：补登记时间列，供冲突争用判定与详情核对使用。
        _ensure_column(conn, "slices", "created_at", "created_at TEXT")
        _ensure_column(conn, "registration_attempts", "session_id", "session_id TEXT")
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for no, d, p, rc, ec, comp, rel in rows:
                conn.execute(
                    "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status,created_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (no, d, p, rc, ec, comp, rel, f"{d}T00:00:00"),
                )
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')")


@app.on_event("startup")
def _start():
    init_db()


def _fetch_policy(conn: sqlite3.Connection, biz_date: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM day_policies WHERE biz_date=?", (biz_date,)).fetchone()


def _policy_view(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    d = dict(row)
    d["policy_label"] = POLICY_LABEL.get(d["policy"], d["policy"])
    d["completeness_rule_text"] = COMPLETENESS_RULE_TEXT
    d["merge_rule_text"] = MERGE_RULE_TEXT
    return d


def _insert_deviation(
    conn: sqlite3.Connection,
    kind: str,
    detail: str,
    operator: str,
    biz_date: str | None = None,
    conflict_id: int | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO registration_deviations(kind,biz_date,conflict_id,operator,detail,created_at)"
        " VALUES (?,?,?,?,?,?)",
        (kind, biz_date, conflict_id, operator, detail, now_iso()),
    )
    return int(cur.lastrowid)


def _log_deviation(
    kind: str,
    detail: str,
    operator: str,
    biz_date: str | None = None,
    conflict_id: int | None = None,
) -> int:
    """独立提交的偏离记录，用于拒绝请求时仍留下审计痕迹。"""
    with write_tx() as conn:
        return _insert_deviation(conn, kind, detail, operator, biz_date, conflict_id)


def _insert_attempt(
    conn: sqlite3.Connection,
    attempt_id: str,
    slice_no: str,
    biz_date: str,
    partition_key: str,
    row_count: int,
    expected_count: int,
    operator: str,
    session_id: str,
    contended: bool,
    outcome: str,
    conflict_id: int | None,
    detail: str,
    created_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO registration_attempts(
          attempt_id,slice_no,biz_date,partition_key,row_count,expected_count,operator,session_id,
          contended,outcome,conflict_id,detail,created_at,finished_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (attempt_id, slice_no, biz_date, partition_key, row_count, expected_count, operator, session_id,
         1 if contended else 0, outcome, conflict_id, detail, created_at,
         None if outcome == "进行中" else now_iso()),
    )
    return int(cur.lastrowid)


def _start_attempt(
    attempt_id: str,
    slice_no: str,
    biz_date: str,
    partition_key: str,
    row_count: int,
    expected_count: int,
    operator: str,
    session_id: str,
    arrived_at: str,
) -> None:
    """先落“进行中”再排队；两个人同时登记同一编号时，彼此都能看到对方在途。"""
    with write_tx() as conn:
        _insert_attempt(conn, attempt_id, slice_no, biz_date, partition_key, row_count,
                        expected_count, operator, session_id, False, "进行中", None,
                        "登记受理，等待切片号闸", arrived_at)


def _finish_attempt(
    attempt_id: str,
    outcome: str,
    conflict_id: int | None,
    detail: str,
    contended: bool,
) -> None:
    with write_tx() as conn:
        conn.execute(
            "UPDATE registration_attempts SET outcome=?,conflict_id=?,detail=?,contended=?,finished_at=?"
            " WHERE attempt_id=?",
            (outcome, conflict_id, detail, 1 if contended else 0, now_iso(), attempt_id),
        )


def _has_overlapping_attempt(
    conn: sqlite3.Connection, slice_no: str, attempt_id: str, session_id: str, arrived_at: str
) -> bool:
    """同一时刻另一名值班（不同登录会话）也在登记同一切片号，即登记争用。

    比较两笔受理的存活区间是否重叠：本笔 [arrived_at, now]，对方 [created_at, finished_at]。
    对方若仍在“进行中”，以过期窗口封顶，防止崩溃残留的老记录永久算作争用。
    """
    cutoff = (datetime.now() - timedelta(seconds=STALE_ATTEMPT_SECONDS)).isoformat(timespec="microseconds")
    rows = conn.execute(
        """
        SELECT created_at,finished_at FROM registration_attempts
         WHERE slice_no=? AND attempt_id<>? AND COALESCE(session_id,'')<>?
           AND created_at<=?
         LIMIT 50
        """,
        (slice_no, attempt_id, session_id, now_iso()),
    ).fetchall()
    for r in rows:
        # 未收尾的“进行中”记录超过过期窗口即视为崩溃残留，不再算争用。
        if r["finished_at"] is None and r["created_at"] < cutoff:
            continue
        if r["created_at"] <= now_iso() and (r["finished_at"] or now_iso()) >= arrived_at:
            return True
    return False


def _slice_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d.setdefault("created_at", None)
    return d


def _deviation_response(deviation_id: int, detail: str, **extra: Any) -> JSONResponse:
    content = {
        "detail": detail,
        "code": "policy_deviation",
        "deviation_id": deviation_id,
        "overwrote": False,
        "watermark_pushed": False,
    }
    content.update(extra)
    return JSONResponse(status_code=409, content=content)


def _conflict_response(
    conflict_id: int,
    policy: str,
    outcome: str,
    message: str,
    existing: sqlite3.Row,
    incoming: dict[str, Any],
    contended: bool,
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": message,
            "code": "slice_no_conflict",
            "conflict_id": conflict_id,
            "outcome": outcome,
            "policy": policy,
            "policy_label": POLICY_LABEL.get(policy, policy),
            "contended": contended,
            "overwrote": False,
            "watermark_pushed": False,
            "existing_slice": _slice_dict(existing),
            "incoming": incoming,
        },
    )


@app.get("/api/health")
def health():
    with read_conn() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
        conflicts = c.execute("SELECT COUNT(*) c FROM registration_conflicts").fetchone()["c"]
        pending = c.execute(
            "SELECT COUNT(*) c FROM registration_conflicts WHERE outcome=?", (CONFLICT_PENDING,)
        ).fetchone()["c"]
        policies = c.execute("SELECT COUNT(*) c FROM day_policies").fetchone()["c"]
    return {
        "ok": True,
        "product": PRODUCT,
        "skeleton": SKELETON,
        "slices": n,
        "conflicts": conflicts,
        "pending_conflicts": pending,
        "day_policies": policies,
        "db": str(DB_PATH),
    }


@app.post("/api/login")
def login(body: LoginIn):
    with read_conn() as c:
        row = c.execute(
            "SELECT username, role FROM users WHERE username=? AND password_sha=?",
            (body.username, sha(body.password)),
        ).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


# ---------------------------------------------------------------------------
# 登记口径锁定
# ---------------------------------------------------------------------------
class PolicyIn(BaseModel):
    biz_date: str
    policy: str
    completeness_rule: str | None = None
    merge_rule: str | None = None
    note: str = ""


@app.get("/api/policies")
@app.get("/api/day-policies", include_in_schema=False)
def list_policies(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        rows = c.execute("SELECT * FROM day_policies ORDER BY biz_date").fetchall()
    return {"items": [_policy_view(r) for r in rows]}


@app.get("/api/policies/{biz_date}")
def get_policy(biz_date: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        row = _fetch_policy(c, biz_date)
    if row is None:
        raise HTTPException(404, "该业务日尚未锁定登记口径")
    return _policy_view(row)


@app.post("/api/policies")
@app.post("/api/day-policies", include_in_schema=False)
def lock_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    biz_date = body.biz_date.strip()
    if not biz_date:
        raise HTTPException(400, "业务日不能为空")
    if body.policy not in POLICIES:
        raise HTTPException(400, "未知登记口径，只能是 reject（甲）或 merge_review（乙）")
    if body.completeness_rule and body.completeness_rule != COMPLETENESS_RULE:
        dev = _log_deviation(
            "policy_mismatch",
            f"登记口径偏了：正确性口径只能为 {COMPLETENESS_RULE}（{COMPLETENESS_RULE_TEXT}），"
            f"拒绝改用 {body.completeness_rule}。",
            operator,
            biz_date,
        )
        return _deviation_response(dev, f"登记口径偏了：正确性口径只能为 {COMPLETENESS_RULE}。")
    if body.merge_rule and body.merge_rule != MERGE_RULE:
        dev = _log_deviation(
            "policy_mismatch",
            f"登记口径偏了：合并规则只能为 {MERGE_RULE}，拒绝改用 {body.merge_rule}。",
            operator,
            biz_date,
        )
        return _deviation_response(dev, f"登记口径偏了：合并规则只能为 {MERGE_RULE}。")

    deviation_id = None
    locked_policy = None
    already = False
    with write_tx() as c:
        row = _fetch_policy(c, biz_date)
        if row is None:
            c.execute(
                "INSERT INTO day_policies(biz_date,policy,completeness_rule,merge_rule,locked_by,locked_at,source)"
                " VALUES (?,?,?,?,?,?,?)",
                (biz_date, body.policy, COMPLETENESS_RULE, MERGE_RULE, operator, now_iso(), "explicit"),
            )
            row = _fetch_policy(c, biz_date)
        else:
            already = True
            locked_policy = row["policy"]
            if row["policy"] != body.policy:
                deviation_id = _insert_deviation(
                    c,
                    "policy_change",
                    f"登记口径偏了：业务日 {biz_date} 当天已锁定为“{row['policy']}”（{POLICY_LABEL.get(row['policy'])}），"
                    f"当天不得更换为“{body.policy}”（{POLICY_LABEL.get(body.policy)}），拒绝。",
                    operator,
                    biz_date,
                )
    if deviation_id is not None:
        return _deviation_response(
            deviation_id,
            f"登记口径偏了：业务日 {biz_date} 当天已锁定为 {POLICY_LABEL.get(locked_policy, locked_policy)}，当天不得更换。",
            biz_date=biz_date,
        )
    out = _policy_view(_fetch_policy_with_new_conn(biz_date))
    out["already_locked"] = already
    out["created"] = not already
    return out


def _fetch_policy_with_new_conn(biz_date: str) -> sqlite3.Row:
    with read_conn() as c:
        row = _fetch_policy(c, biz_date)
    assert row is not None
    return row


@app.get("/api/deviations")
def list_deviations(
    biz_date: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
):
    require_user(authorization)
    sql = "SELECT * FROM registration_deviations"
    args: list[Any] = []
    if biz_date:
        sql += " WHERE biz_date=?"
        args.append(biz_date)
    sql += " ORDER BY id DESC"
    with read_conn() as c:
        rows = c.execute(sql, args).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/registration-attempts")
def list_attempts(
    slice_no: str | None = Query(default=None),
    biz_date: str | None = Query(default=None),
    contended_only: bool = Query(default=False),
    authorization: str | None = Header(default=None),
):
    require_user(authorization)
    sql = "SELECT * FROM registration_attempts WHERE 1=1"
    args: list[Any] = []
    if slice_no:
        sql += " AND slice_no=?"
        args.append(slice_no)
    if biz_date:
        sql += " AND biz_date=?"
        args.append(biz_date)
    if contended_only:
        sql += " AND contended=1"
    sql += " ORDER BY id DESC"
    with read_conn() as c:
        rows = c.execute(sql, args).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        d["contended"] = bool(d["contended"])
        items.append(d)
    return {"items": items}


# ---------------------------------------------------------------------------
# 切片登记（唯一冲突显式失败）
# ---------------------------------------------------------------------------
@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        rows = c.execute(
            """
            SELECT s.*, p.policy AS day_policy,
                   (SELECT COUNT(*) FROM registration_conflicts rc
                     WHERE rc.slice_no=s.slice_no AND rc.outcome=?) AS pending_conflicts
              FROM slices s
              LEFT JOIN day_policies p ON p.biz_date=s.biz_date
             ORDER BY s.id
            """,
            (CONFLICT_PENDING,),
        ).fetchall()
    return {"items": [_slice_dict(r) for r in rows]}


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int = Field(ge=0)
    expected_count: int = Field(ge=0)
    policy: str | None = None
    completeness_rule: str | None = None


def _register_locked(
    c: sqlite3.Connection,
    body: "SliceIn",
    operator: str,
    slice_no: str,
    biz_date: str,
    partition_key: str,
    incoming_comp: str,
    attempt_id: str,
    session_id: str,
    arrived_at: str,
    lock_waited: bool,
) -> dict[str, Any]:
    """在已持有的切片号锁 + 写事务内执行登记；唯一冲突只落册，绝不覆盖旧行。"""
    policy_row = _fetch_policy(c, biz_date)
    if policy_row is None:
        policy = body.policy or DEFAULT_POLICY
        source = "registration_requested" if body.policy else DEFAULT_POLICY_SOURCE
        c.execute(
            "INSERT INTO day_policies(biz_date,policy,completeness_rule,merge_rule,locked_by,locked_at,source)"
            " VALUES (?,?,?,?,?,?,?)",
            (biz_date, policy, COMPLETENESS_RULE, MERGE_RULE, operator, now_iso(), source),
        )
        policy_row = _fetch_policy(c, biz_date)
    else:
        policy = policy_row["policy"]
        if body.policy is not None and body.policy != policy:
            dev = _insert_deviation(
                c,
                "policy_mismatch",
                f"登记口径偏了：并发登记时业务日 {biz_date} 已锁定为“{policy}”，"
                f"登记 {slice_no} 按“{body.policy}”提交，拒绝。",
                operator,
                biz_date,
            )
            return {
                "kind": "deviation",
                "response": _deviation_response(
                    dev,
                    f"登记口径偏了：业务日 {biz_date} 当天口径为 {POLICY_LABEL.get(policy, policy)}，不得混用。",
                    slice_no=slice_no,
                ),
            }

    # 唯一索引是最后一道闸：查无旧行才 INSERT，撞上唯一约束也绝不覆盖。
    existing = c.execute("SELECT * FROM slices WHERE slice_no=?", (slice_no,)).fetchone()
    inserted = False
    contended = lock_waited or _has_overlapping_attempt(c, slice_no, attempt_id, session_id, arrived_at)
    if existing is None:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                "completeness,release_status,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (slice_no, biz_date, partition_key, body.row_count, body.expected_count,
                 incoming_comp, "未放行", now_iso()),
            )
            inserted = True
            return {
                "kind": "ok",
                "id": int(cur.lastrowid),
                "slice_no": slice_no,
                "completeness": incoming_comp,
                "policy": policy,
                "policy_label": POLICY_LABEL.get(policy, policy),
                "policy_locked_at": policy_row["locked_at"] if policy_row is not None else None,
                "policy_source": policy_row["source"] if policy_row is not None else None,
                "contended": contended,
                "attempt_id": attempt_id,
                "overwrote": False,
                "watermark_pushed": False,
            }
        except sqlite3.IntegrityError:
            existing = c.execute("SELECT * FROM slices WHERE slice_no=?", (slice_no,)).fetchone()
            contended = True
    if not inserted:
        if existing is None:
            raise HTTPException(500, "切片号唯一冲突但未找到旧行，登记中止")
        identity_same = (
            existing["biz_date"] == biz_date and existing["partition_key"] == partition_key
        )
        outcome = CONFLICT_PENDING if policy == POLICY_MERGE else CONFLICT_REJECTED
        reason_parts = [
            f"切片号 {slice_no} 已存在（正式行 #{existing['id']}，"
            f"旧行数 {existing['row_count']}/{existing['expected_count']}，"
            f"旧完整度 {existing['completeness']}，旧放行态 {existing['release_status']}）。"
        ]
        if not identity_same:
            reason_parts.append(
                f"本次业务日/分区为 {biz_date}/{partition_key}，与旧行 "
                f"{existing['biz_date']}/{existing['partition_key']} 不一致，不可合并。"
            )
        if contended:
            reason_parts.append("登记争用：后手登记未落成。")
        if policy == POLICY_REJECT:
            reason_parts.append("甲口径：冲突直接拒绝登记，旧正式行保持不变。")
        else:
            reason_parts.append("乙口径：不覆盖正式行，已进入合并待审队列，须人工确认后合并或驳回。")
        reason = "".join(reason_parts)
        cur = c.execute(
            """
            INSERT INTO registration_conflicts(
              slice_no,biz_date,partition_key,existing_slice_id,
              existing_row_count,existing_expected_count,existing_completeness,
              incoming_row_count,incoming_expected_count,incoming_completeness,
              policy,outcome,reason,operator,contended,attempt_id,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                slice_no, biz_date, partition_key, int(existing["id"]),
                int(existing["row_count"]), int(existing["expected_count"]), existing["completeness"],
                body.row_count, body.expected_count, incoming_comp,
                policy, outcome, reason, operator, 1 if contended else 0,
                attempt_id, now_iso(),
            ),
        )
        conflict_id = int(cur.lastrowid)
        if policy == POLICY_REJECT:
            message = f"切片号冲突，登记被拒绝（甲口径）：{reason}冲突编号 #{conflict_id}。"
        else:
            message = (
                f"切片号冲突，登记未落成（乙口径），旧正式行未改动，"
                f"已进入合并待审队列 #{conflict_id}，待人工处置。"
            )
        return {
            "kind": "conflict",
            "conflict_id": conflict_id,
            "outcome": outcome,
            "policy": policy,
            "message": message,
            "contended": contended,
            "existing": existing,
        }


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    arrived_at = now_iso()
    session_id = sha(authorization.removeprefix("Bearer ").strip())
    slice_no = body.slice_no.strip()
    biz_date = body.biz_date.strip()
    partition_key = body.partition_key.strip()
    if not slice_no:
        raise HTTPException(400, "切片号不能为空")
    if not biz_date or not partition_key:
        raise HTTPException(400, "业务日与分区不能为空")
    if body.policy is not None and body.policy not in POLICIES:
        raise HTTPException(400, "未知登记口径，只能是 reject（甲）或 merge_review（乙）")

    attempt_id = uuid.uuid4().hex
    _start_attempt(attempt_id, slice_no, biz_date, partition_key,
                   body.row_count, body.expected_count, operator, session_id, arrived_at)

    if body.completeness_rule is not None and body.completeness_rule != COMPLETENESS_RULE:
        dev = _log_deviation(
            "policy_mismatch",
            f"登记口径偏了：切片 {slice_no} 登记试图改用正确性口径 {body.completeness_rule}，"
            f"本仓固定为 {COMPLETENESS_RULE}。",
            operator,
            biz_date,
        )
        _finish_attempt(attempt_id, "deviation", None,
                        "登记口径偏了：正确性口径被改动，登记被拒绝。", False)
        return _deviation_response(
            dev, f"登记口径偏了：正确性口径固定为 {COMPLETENESS_RULE}，登记被拒绝。", slice_no=slice_no
        )

    # 先读该业务日已锁定口径：与本次请求不一致则直接拒绝并记偏离。
    with read_conn() as c:
        locked = _fetch_policy(c, biz_date)
    if locked is not None:
        if body.policy is not None and body.policy != locked["policy"]:
            dev = _log_deviation(
                "policy_mismatch",
                f"登记口径偏了：业务日 {biz_date} 已锁定为“{locked['policy']}”，"
                f"登记 {slice_no} 却按“{body.policy}”提交，拒绝。",
                operator,
                biz_date,
            )
            _finish_attempt(attempt_id, "deviation", None,
                            "登记口径偏了：当天口径与提交口径不符，登记被拒绝。", False)
            return _deviation_response(
                dev,
                f"登记口径偏了：业务日 {biz_date} 当天口径为 {POLICY_LABEL.get(locked['policy'], locked['policy'])}，"
                "不得混用另一口径登记。",
                slice_no=slice_no,
            )

    incoming_comp = calc_completeness(body.row_count, body.expected_count)
    incoming = {
        "slice_no": slice_no,
        "biz_date": biz_date,
        "partition_key": partition_key,
        "row_count": body.row_count,
        "expected_count": body.expected_count,
        "completeness": incoming_comp,
    }

    lock = _slice_lock(slice_no)
    lock_waited = not lock.acquire(blocking=False)
    if lock_waited:
        lock.acquire()
    result: dict[str, Any] | None = None
    try:
        try:
            with write_tx() as c:
                result = _register_locked(c, body, operator, slice_no, biz_date, partition_key,
                                          incoming_comp, attempt_id, session_id, arrived_at, lock_waited)
        except HTTPException:
            raise
        except Exception as exc:  # 任何异常都要给受理记录收尾，避免残留“进行中”
            _finish_attempt(attempt_id, "error", None, f"登记异常中止：{exc}", lock_waited)
            raise
    finally:
        lock.release()

    assert result is not None
    if result["kind"] == "deviation":
        _finish_attempt(attempt_id, "deviation", None,
                        "登记口径偏了：口径与当天锁定不符，登记被拒绝。", lock_waited)
        return result["response"]
    if result["kind"] == "ok":
        _finish_attempt(attempt_id, "ok", None, "登记成功，旧行未覆盖。", result["contended"])
        return {k: v for k, v in result.items() if k != "kind"}
    _finish_attempt(
        attempt_id,
        "rejected" if result["policy"] == POLICY_REJECT else "conflict",
        result["conflict_id"],
        result["message"],
        result["contended"],
    )
    return _conflict_response(
        result["conflict_id"],
        result["policy"],
        result["outcome"],
        result["message"],
        result["existing"],
        incoming,
        result["contended"],
    )


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with write_tx() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = calc_completeness(int(row["row_count"]), int(row["expected_count"]))
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}


class ReleaseIn(BaseModel):
    consumer: str


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    consumer = body.consumer.strip()
    if not consumer:
        raise HTTPException(400, "下游不能为空")
    pending = 0
    with write_tx() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        pending = c.execute(
            "SELECT COUNT(*) c FROM registration_conflicts WHERE slice_no=? AND outcome=?",
            (row["slice_no"], CONFLICT_PENDING),
        ).fetchone()["c"]
        if pending:
            c.execute(
                "INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                (sid, consumer, "拒绝",
                 f"存在 {pending} 条未处置的登记冲突，禁止放行与推水位", now_iso()),
            )
            c.commit()
            raise HTTPException(409, "存在未处置的登记冲突，禁止放行与推水位")
        if row["completeness"] != "完整":
            c.execute(
                "INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                (sid, consumer, "拒绝", "完整度未达标", now_iso()),
            )
            c.commit()
            raise HTTPException(409, "未完整禁止放行")
        c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
        c.execute(
            "INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
            (sid, consumer, "成功", "完整切片放行", now_iso()),
        )
        c.execute(
            "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES (?,?,?,?)",
            (consumer, row["biz_date"], now_iso(), row["slice_no"]),
        )
    return {"ok": True}


@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# 登记冲突册 / 处置
# ---------------------------------------------------------------------------
def _conflict_view(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["policy_label"] = POLICY_LABEL.get(d["policy"], d["policy"])
    d["contended"] = bool(d["contended"])
    d["resolvable"] = d["outcome"] == CONFLICT_PENDING
    return d


@app.get("/api/conflicts")
@app.get("/api/registration-conflicts", include_in_schema=False)
@app.get("/api/conflict-ledger", include_in_schema=False)
def list_conflicts(
    outcome: str | None = Query(default=None),
    biz_date: str | None = Query(default=None),
    slice_no: str | None = Query(default=None),
    pending_only: bool = Query(default=False),
    authorization: str | None = Header(default=None),
):
    require_user(authorization)
    sql = "SELECT * FROM registration_conflicts WHERE 1=1"
    args: list[Any] = []
    if pending_only:
        sql += " AND outcome=?"
        args.append(CONFLICT_PENDING)
    if outcome:
        sql += " AND outcome=?"
        args.append(outcome)
    if biz_date:
        sql += " AND biz_date=?"
        args.append(biz_date)
    if slice_no:
        sql += " AND slice_no=?"
        args.append(slice_no)
    sql += " ORDER BY id DESC"
    with read_conn() as c:
        rows = c.execute(sql, args).fetchall()
    return {"items": [_conflict_view(r) for r in rows]}


@app.get("/api/conflicts/{cid}")
def get_conflict(cid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        row = c.execute("SELECT * FROM registration_conflicts WHERE id=?", (cid,)).fetchone()
        if row is None:
            raise HTTPException(404, "冲突不存在")
        existing = c.execute("SELECT * FROM slices WHERE id=?", (row["existing_slice_id"],)).fetchone()
    out = _conflict_view(row)
    out["existing_slice"] = _slice_dict(existing) if existing else None
    return out


class ResolveIn(BaseModel):
    action: str
    note: str = ""


@app.post("/api/conflicts/{cid}/resolve")
def resolve_conflict(cid: int, body: ResolveIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    action = body.action.strip()
    if action not in ("merge", "reject"):
        raise HTTPException(400, "处置动作只能是 merge（合并）或 reject（驳回）")
    note = body.note.strip()

    with read_conn() as c:
        peek = c.execute("SELECT * FROM registration_conflicts WHERE id=?", (cid,)).fetchone()
    if peek is None:
        raise HTTPException(404, "冲突不存在")

    lock = _slice_lock(peek["slice_no"])
    with lock:
        result: dict[str, Any]
        with write_tx() as c:
            conf = c.execute("SELECT * FROM registration_conflicts WHERE id=?", (cid,)).fetchone()
            if conf is None:
                raise HTTPException(404, "冲突不存在")
            if conf["outcome"] not in CONFLICT_OPEN_STATES:
                dev = _insert_deviation(
                    c,
                    "out_of_ledger",
                    f"登记口径偏了：冲突 #{cid}（切片 {conf['slice_no']}）当前状态为“{conf['outcome']}”，"
                    "不在待审队列，拒绝再次处置，正式行与水位均不变。",
                    operator,
                    conf["biz_date"],
                    cid,
                )
                result = {
                    "kind": "deviation",
                    "response": _deviation_response(
                        dev,
                        f"登记口径偏了：冲突 #{cid} 当前状态为“{conf['outcome']}”，不在待审队列，拒绝处置。",
                        conflict_id=cid,
                    ),
                }
            elif action == "reject":
                c.execute(
                    "UPDATE registration_conflicts SET outcome=?,resolved_by=?,resolved_at=?,resolution_note=?"
                    " WHERE id=?",
                    (CONFLICT_DISMISSED, operator, now_iso(), note or "人工驳回", cid),
                )
                result = {
                    "kind": "ok",
                    "ok": True,
                    "conflict_id": cid,
                    "outcome": CONFLICT_DISMISSED,
                    "derived_slice_id": None,
                    "slice_no": None,
                    "watermark_pushed": False,
                }
            else:
                existing = c.execute("SELECT * FROM slices WHERE id=?", (conf["existing_slice_id"],)).fetchone()
                if existing is None:
                    dev = _insert_deviation(
                        c,
                        "merge_blocked",
                        f"登记口径偏了：冲突 #{cid} 的原正式行 #{conf['existing_slice_id']} 已不存在，"
                        "拒绝合并，不生成新行。",
                        operator,
                        conf["biz_date"],
                        cid,
                    )
                    result = {
                        "kind": "deviation",
                        "response": _deviation_response(
                            dev, f"登记口径偏了：冲突 #{cid} 的原正式行不存在，无法按登记冲突册合并。",
                            conflict_id=cid,
                        ),
                    }
                elif existing["biz_date"] != conf["biz_date"] or existing["partition_key"] != conf["partition_key"]:
                    dev = _insert_deviation(
                        c,
                        "merge_blocked",
                        f"登记口径偏了：冲突 #{cid} 的新报身份 "
                        f"{conf['biz_date']}/{conf['partition_key']} 与旧行身份 "
                        f"{existing['biz_date']}/{existing['partition_key']} 不一致，禁止合并。",
                        operator,
                        conf["biz_date"],
                        cid,
                    )
                    result = {
                        "kind": "deviation",
                        "response": _deviation_response(
                            dev, "登记口径偏了：业务日/分区不一致，按合并规则不得合并，请驳回或在正确分区重新登记。",
                            conflict_id=cid,
                        ),
                    }
                else:
                    merged_rows = max(int(existing["row_count"]), int(conf["incoming_row_count"]))
                    merged_expected = max(int(existing["expected_count"]), int(conf["incoming_expected_count"]))
                    merged_comp = calc_completeness(merged_rows, merged_expected)
                    base = conf["slice_no"]
                    cur = None
                    for n in range(1, 10000):
                        new_no = f"{base}-R{n}"
                        if c.execute("SELECT 1 FROM slices WHERE slice_no=?", (new_no,)).fetchone():
                            continue
                        try:
                            cur = c.execute(
                                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                                "completeness,release_status,created_at) VALUES (?,?,?,?,?,?,?,?)",
                                (new_no, existing["biz_date"], existing["partition_key"], merged_rows,
                                 merged_expected, merged_comp, "未放行", now_iso()),
                            )
                            break
                        except sqlite3.IntegrityError:
                            continue
                    if cur is None:
                        raise HTTPException(500, "无法为合并生成唯一派生切片号，处置中止，旧行未改动")
                    new_id = int(cur.lastrowid)
                    c.execute(
                        "UPDATE registration_conflicts SET outcome=?,resolved_slice_id=?,resolved_by=?,"
                        "resolved_at=?,resolution_note=? WHERE id=?",
                        (CONFLICT_MERGED, new_id, operator, now_iso(),
                         note or f"按合并规则生成 {new_no}", cid),
                    )
                    result = {
                        "kind": "ok",
                        "ok": True,
                        "conflict_id": cid,
                        "outcome": CONFLICT_MERGED,
                        "derived_slice_id": new_id,
                        "slice_no": new_no,
                        "row_count": merged_rows,
                        "expected_count": merged_expected,
                        "completeness": merged_comp,
                        "release_status": "未放行",
                        "original_slice_id": int(existing["id"]),
                        "original_row_count": int(existing["row_count"]),
                        "original_completeness": existing["completeness"],
                        "watermark_pushed": False,
                    }
    if result["kind"] == "deviation":
        return result["response"]
    return {k: v for k, v in result.items() if k != "kind"}


# ---------------------------------------------------------------------------
# 分区详情（与登记列表、冲突队列同库核对）
# ---------------------------------------------------------------------------
def _partition_summary(c: sqlite3.Connection) -> list[dict[str, Any]]:
    slice_rows = c.execute(
        """
        SELECT biz_date, partition_key,
               COUNT(*) AS slice_count,
               COALESCE(SUM(row_count),0) AS row_total,
               COALESCE(SUM(expected_count),0) AS expected_total,
               SUM(CASE WHEN completeness='完整' THEN 1 ELSE 0 END) AS complete_count,
               SUM(CASE WHEN completeness='部分完整' THEN 1 ELSE 0 END) AS partial_count,
               SUM(CASE WHEN completeness='待检' THEN 1 ELSE 0 END) AS unchecked_count,
               SUM(CASE WHEN release_status='已放行' THEN 1 ELSE 0 END) AS released_count
          FROM slices GROUP BY biz_date, partition_key
        """
    ).fetchall()
    conflict_rows = c.execute(
        """
        SELECT biz_date, partition_key,
               COUNT(*) AS conflict_total,
               SUM(CASE WHEN outcome='待审' THEN 1 ELSE 0 END) AS pending_conflicts
          FROM registration_conflicts GROUP BY biz_date, partition_key
        """
    ).fetchall()
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in slice_rows:
        key = (r["biz_date"], r["partition_key"])
        merged[key] = {
            "biz_date": r["biz_date"],
            "partition_key": r["partition_key"],
            "slice_count": int(r["slice_count"]),
            "row_total": int(r["row_total"]),
            "expected_total": int(r["expected_total"]),
            "complete_count": int(r["complete_count"] or 0),
            "partial_count": int(r["partial_count"] or 0),
            "unchecked_count": int(r["unchecked_count"] or 0),
            "released_count": int(r["released_count"] or 0),
            "conflict_total": 0,
            "pending_conflicts": 0,
            "gap_total": max(int(r["expected_total"]) - int(r["row_total"]), 0),
        }
    for r in conflict_rows:
        key = (r["biz_date"], r["partition_key"])
        item = merged.setdefault(
            key,
            {
                "biz_date": r["biz_date"],
                "partition_key": r["partition_key"],
                "slice_count": 0,
                "row_total": 0,
                "expected_total": 0,
                "complete_count": 0,
                "partial_count": 0,
                "unchecked_count": 0,
                "released_count": 0,
                "gap_total": 0,
            },
        )
        item["conflict_total"] = int(r["conflict_total"])
        item["pending_conflicts"] = int(r["pending_conflicts"] or 0)
    for item in merged.values():
        item["blocked_by_conflict"] = item["pending_conflicts"] > 0
    return sorted(merged.values(), key=lambda x: (x["biz_date"], x["partition_key"]))


@app.get("/api/partitions")
def list_partitions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        items = _partition_summary(c)
        totals = c.execute(
            "SELECT COUNT(*) AS slice_count, COALESCE(SUM(row_count),0) AS row_total,"
            " COALESCE(SUM(expected_count),0) AS expected_total FROM slices"
        ).fetchone()
        pending_total = c.execute(
            "SELECT COUNT(*) c FROM registration_conflicts WHERE outcome=?", (CONFLICT_PENDING,)
        ).fetchone()["c"]
    partition_rows = sum(i["row_total"] for i in items)
    partition_expected = sum(i["expected_total"] for i in items)
    partition_count = sum(i["slice_count"] for i in items)
    partition_pending = sum(i["pending_conflicts"] for i in items)
    reconciles = (
        partition_rows == int(totals["row_total"])
        and partition_expected == int(totals["expected_total"])
        and partition_count == int(totals["slice_count"])
        and partition_pending == int(pending_total)
    )
    return {
        "items": items,
        "totals": {
            "slice_count": int(totals["slice_count"]),
            "row_total": int(totals["row_total"]),
            "expected_total": int(totals["expected_total"]),
            "partition_row_total": partition_rows,
            "partition_expected_total": partition_expected,
            "partition_slice_count": partition_count,
            "pending_conflicts": int(pending_total),
            "partition_pending_conflicts": partition_pending,
        },
        "reconciles": reconciles,
        "reconcile_note": "分区汇总与全库切片/冲突队列同库对账一致"
        if reconciles else "分区汇总与全库切片/冲突队列不一致，需排查",
    }


@app.get("/api/partitions/{biz_date}/{partition_key}")
def partition_detail(biz_date: str, partition_key: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with read_conn() as c:
        slice_rows = c.execute(
            "SELECT * FROM slices WHERE biz_date=? AND partition_key=? ORDER BY id",
            (biz_date, partition_key),
        ).fetchall()
        conflict_rows = c.execute(
            "SELECT * FROM registration_conflicts WHERE biz_date=? AND partition_key=? ORDER BY id DESC",
            (biz_date, partition_key),
        ).fetchall()
        if not slice_rows and not conflict_rows:
            raise HTTPException(404, "分区不存在")
        summary_rows = [i for i in _partition_summary(c)
                        if i["biz_date"] == biz_date and i["partition_key"] == partition_key]
        policy = _fetch_policy(c, biz_date)
    summary = summary_rows[0] if summary_rows else {
        "biz_date": biz_date, "partition_key": partition_key, "slice_count": 0,
        "row_total": 0, "expected_total": 0, "complete_count": 0, "partial_count": 0,
        "unchecked_count": 0, "released_count": 0, "gap_total": 0,
        "conflict_total": len(conflict_rows), "pending_conflicts": 0,
    }
    items = [_slice_dict(r) for r in slice_rows]
    conflicts = [_conflict_view(r) for r in conflict_rows]
    item_row_total = sum(int(i["row_count"]) for i in items)
    item_expected_total = sum(int(i["expected_count"]) for i in items)
    pending = sum(1 for i in conflicts if i["outcome"] == CONFLICT_PENDING)
    # 冲突队列里指向本分区正式行的条目，行数/完整度必须与正式行一致才算对得上。
    row_by_id = {int(i["id"]): i for i in items}
    conflict_rows_match = True
    for cf in conflicts:
        base = row_by_id.get(int(cf["existing_slice_id"]))
        if base is None:
            conflict_rows_match = False
            break
        if (
            int(base["row_count"]) != int(cf["existing_row_count"])
            or int(base["expected_count"]) != int(cf["existing_expected_count"])
            or base["completeness"] != cf["existing_completeness"]
        ):
            conflict_rows_match = False
            break
    reconciles = (
        item_row_total == summary["row_total"]
        and item_expected_total == summary["expected_total"]
        and len(items) == summary["slice_count"]
        and pending == summary["pending_conflicts"]
        and conflict_rows_match
    )
    return {
        "biz_date": biz_date,
        "partition_key": partition_key,
        "policy": _policy_view(policy),
        "summary": summary,
        "items": items,
        "conflicts": conflicts,
        "conflict_rows_match": conflict_rows_match,
        "reconciles": reconciles,
        "reconcile_note": (
            "登记列表、冲突队列、分区详情同库同事务口径，可核对"
            if reconciles else "分区详情与登记列表/冲突队列不一致，需排查"
        ),
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
