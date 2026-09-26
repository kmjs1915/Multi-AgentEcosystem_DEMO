# -*- coding: utf-8 -*-
"""
SQLite 日志库（基础设施层）

架构文档来源：
  - 第9章 9.2 日志系统：永久记录 任务日志、Agent调用日志、Token消耗、审批记录、错误日志
  - 第2章 2.4 Token与状态栏数据规则：按 session_id、task_id、Agent角色、模型 四维度统计存储
  - 第3章 3.1/3.2 消息与审批字段
  - 第9章 9.3 数据备份：会话记录、审批记录、加密配置、向量记忆库导出

设计约束：
  - 全部使用标准库 sqlite3，WAL 模式，单连接 + 线程锁（FastAPI 线程池安全）。
  - 不存储任何明文密钥（密钥只存加密信封，落盘于 config/）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

from backend.utils.constants import (
    DEFAULT_WORKSPACE_ID,
    DEFAULT_WORKSPACE_NAME,
    STATUS_PENDING,
    STATUS_WAITING_APPROVAL,
)

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    meta            TEXT NOT NULL DEFAULT '{}'
);

-- 【需求点 二、前端改造】工作区分组：工作区(分组) -> 会话(叶子)
CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    meta            TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    parent_task_id  TEXT,
    title           TEXT NOT NULL,
    agent_role      TEXT NOT NULL,
    status          TEXT NOT NULL,
    iteration       INTEGER NOT NULL DEFAULT 0,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    review_rejects  INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    started_at      REAL,
    finished_at     REAL,
    deadline_at     REAL,
    result          TEXT,
    error_code      TEXT,
    error_message   TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS messages (
    msg_id          TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    parent_task_id  TEXT,
    sender_agent    TEXT NOT NULL,
    receiver_agent  TEXT NOT NULL,
    msg_type        TEXT NOT NULL,
    payload_content TEXT NOT NULL DEFAULT '',
    payload_metadata TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL,
    timestamp       INTEGER NOT NULL,
    stored_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id     TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    agent_role      TEXT NOT NULL,
    operation_type  TEXT NOT NULL,
    risk_level      TEXT NOT NULL,
    operation_desc  TEXT NOT NULL,
    operation_params TEXT NOT NULL DEFAULT '',
    danger_reason   TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL,
    decided_by      TEXT,
    decided_at      REAL,
    backend_recheck TEXT NOT NULL DEFAULT '{}',
    -- 【新增】该审批的 30 秒截止时间（与 task_snapshots.approval_deadline 同源）
    approval_deadline REAL,
    -- ======================================================================
    -- 【第三轮·Bug1 修复】审批"提交后"的执行链路 30 秒超时字段
    --   waiting-for-user 阶段**不计时**；只有后端收到 /api/approval/submit
    --   （或超时扫描代为裁决）之后，才写入 resume_deadline 并开始计时。
    --   resume_state 取值：idle（等待用户操作，不计时）/ running（执行链路计时中）
    --                      / settled（链路已收敛，计时关闭）/ timeout（执行链路 30s 超时）
    -- ======================================================================
    resume_started_at REAL,
    resume_deadline REAL,
    resume_state TEXT NOT NULL DEFAULT 'idle',
    resume_finished_at REAL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_approvals_session ON approvals(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_approvals_state ON approvals(state, created_at DESC);

CREATE TABLE IF NOT EXISTS token_usage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    agent_role      TEXT NOT NULL,
    provider        TEXT NOT NULL,
    model           TEXT NOT NULL,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    cached_tokens   INTEGER NOT NULL DEFAULT 0,
    cache_hit_rate  REAL NOT NULL DEFAULT 0.0,
    elapsed_ms      INTEGER NOT NULL DEFAULT 0,
    steps           INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_token_session ON token_usage(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_task ON token_usage(task_id);

CREATE TABLE IF NOT EXISTS agent_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    agent_role      TEXT NOT NULL,
    event           TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '',
    level           TEXT NOT NULL DEFAULT 'info',
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_logs_task ON agent_logs(task_id, created_at);

CREATE TABLE IF NOT EXISTS error_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT,
    task_id         TEXT,
    agent_role      TEXT,
    error_code      TEXT NOT NULL,
    message         TEXT NOT NULL,
    stack           TEXT,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_error_logs_created ON error_logs(created_at DESC);

CREATE TABLE IF NOT EXISTS think_steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    agent_role      TEXT NOT NULL,
    step_index      INTEGER NOT NULL,
    step_type       TEXT NOT NULL,
    step_text       TEXT NOT NULL,
    level           TEXT NOT NULL DEFAULT 'info',
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_think_task ON think_steps(task_id, step_index);

-- ==========================================================================
-- 【需求点 二、3 任务耗时数据存储】任务执行计时记录
--   任务结束后写入本次任务耗时；历史会话打开时可回显"上一次任务耗时"。
--   兼容旧库：CREATE TABLE IF NOT EXISTS 增量建表，不触碰任何既有表结构。
-- ==========================================================================
CREATE TABLE IF NOT EXISTS session_task_timers (
    timer_id        TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL,
    started_at      REAL NOT NULL,
    finished_at     REAL,
    elapsed_seconds REAL NOT NULL DEFAULT 0,
    task_status     TEXT NOT NULL DEFAULT '',
    reason          TEXT NOT NULL DEFAULT '',
    record          TEXT NOT NULL DEFAULT '{}',
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_timers_session ON session_task_timers(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_timers_task ON session_task_timers(task_id);

-- ==========================================================================
-- 【需求点 Bug2 业务流程重构】任务完整快照（业务语义"while 循环"的持久化载体）
--   业务伪代码里的循环语义，在代码层用「任务状态机 + SQLite 任务快照 + 审批回调恢复」
--   实现（禁止阻塞式 while 循环）：
--     · 每完成一个子任务 / 每次进入审批 / 每次审批结果回灌 → 覆盖写一次完整快照；
--     · 快照包含：session_id、task_id、已跑完的子任务列表、剩余待执行计划、
--       全部消息上下文、队长循环状态（本轮迭代数 / 各子任务重试计数）、
--       以及即将执行的高危操作详情；
--     · 审批通过/拒绝后按 task_id 读取快照 → 注入审批结果 → 恢复队长循环继续跑。
--   兼容旧库：CREATE TABLE IF NOT EXISTS 增量建表，不触碰任何既有表结构。
-- ==========================================================================
CREATE TABLE IF NOT EXISTS task_snapshots (
    task_id         TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    parent_task_id  TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    stage           TEXT NOT NULL DEFAULT '',
    user_input      TEXT NOT NULL DEFAULT '',
    plan            TEXT NOT NULL DEFAULT '{}',
    completed       TEXT NOT NULL DEFAULT '[]',
    remaining       TEXT NOT NULL DEFAULT '[]',
    messages        TEXT NOT NULL DEFAULT '[]',
    approval        TEXT NOT NULL DEFAULT '{}',
    loop_state      TEXT NOT NULL DEFAULT '{}',
    payload         TEXT NOT NULL DEFAULT '{}',
    -- 【新增】30 秒审批超时判定字段：进入 waiting_approval 时写入
    -- （= 审批开始计时时刻 + APPROVAL_TIMEOUT_SECONDS），审批裁决后置空。
    -- 服务重启后仍可据此判定"是否已超时 → 等价 rejected"。
    approval_deadline REAL,
    -- ======================================================================
    -- 【第三轮新增】业务循环计数器（计数必须随快照持久化，重启后不得清零）
    --   fix_rounds        ：JSON {"结果键": 轮次} —— 校验评估Agent 打回队员修改的轮次
    --                       （每子任务最多 3 轮，达到上限上交队长）
    --   reallocations     ：队长"需求对齐审批"不通过后的重新分配次数（最多 3 次）
    --   requirement_state ：JSON 队长需求对齐审批结果（latest/verdict/reason/at）
    --   evaluator_state   ：JSON 校验评估Agent 最近一次结论（verdict/score/issues）
    -- ======================================================================
    fix_rounds        TEXT NOT NULL DEFAULT '{}',
    reallocations     INTEGER NOT NULL DEFAULT 0,
    -- 【第三轮·正式字段名】按需求命名的两个持久化计数器（与上面两列同义，双写兼容）
    --   code_revise_count  ：代码修改迭代计数（初始 0，最大 3）
    --   reassign_task_count：任务重新分配计数（初始 0，最大 3）
    code_revise_count   INTEGER NOT NULL DEFAULT 0,
    reassign_task_count INTEGER NOT NULL DEFAULT 0,
    requirement_state TEXT NOT NULL DEFAULT '{}',
    evaluator_state   TEXT NOT NULL DEFAULT '{}',
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_session ON task_snapshots(session_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_parent ON task_snapshots(parent_task_id);
"""


class Database:
    """SQLite 封装（线程安全）。"""

    def __init__(self, db_path: Path):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._migrate()
            self._conn.commit()

    # ------------------------------------------------------------------
    # 【需求点 三、强制开发约束3】兼容历史旧会话数据
    # ------------------------------------------------------------------
    def _migrate(self) -> None:
        """轻量迁移：为历史库补齐 workspace_id 列，并把旧会话归入默认工作区。"""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(sessions)").fetchall()}
        if "workspace_id" not in cols:
            self._conn.execute("ALTER TABLE sessions ADD COLUMN workspace_id TEXT")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_workspace ON sessions(workspace_id, updated_at DESC)"
        )
        # ==================================================================
        # 【BUG-C 2/5】思考步骤级别列（info / warn / error）：
        #   前端据此渲染红色错误标识，禁止用 success 状态掩盖失败。
        #   增量 ALTER 兼容历史库，旧记录默认 'info'。
        # ==================================================================
        think_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(think_steps)").fetchall()}
        if think_cols and "level" not in think_cols:
            self._conn.execute("ALTER TABLE think_steps ADD COLUMN level TEXT NOT NULL DEFAULT 'info'")
        # ==================================================================
        # 【新增】30 秒审批超时字段（需求硬性要求 1 / 7）：
        #   · task_snapshots.approval_deadline：任务快照的审批截止时间；
        #   · approvals.approval_deadline      ：审批记录自身的截止时间。
        #   增量 ALTER 兼容历史库（旧库该列为 NULL → 读取时回落到 created_at + 30s，
        #   历史 pending 记录同样能被看门狗正确判定，不会出现"永不过期"的死审批）。
        # ==================================================================
        snap_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(task_snapshots)").fetchall()}
        if snap_cols and "approval_deadline" not in snap_cols:
            self._conn.execute("ALTER TABLE task_snapshots ADD COLUMN approval_deadline REAL")
        approval_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(approvals)").fetchall()}
        if approval_cols and "approval_deadline" not in approval_cols:
            self._conn.execute("ALTER TABLE approvals ADD COLUMN approval_deadline REAL")
        # ==================================================================
        # 【第三轮·业务流程重构】业务循环计数器增量迁移（旧库自动补齐，历史数据零损伤）
        #   fix_rounds / reallocations / requirement_state / evaluator_state
        #   → 校验评估Agent"最多修改 3 轮"与队长"最多重新分配 3 次"必须跨重启保持，
        #     否则快照恢复后计数归零会造成无限循环（Token 失控）。
        # ==================================================================
        for column, ddl in (
            ("fix_rounds", "ALTER TABLE task_snapshots ADD COLUMN fix_rounds TEXT NOT NULL DEFAULT '{}'"),
            ("reallocations", "ALTER TABLE task_snapshots ADD COLUMN reallocations INTEGER NOT NULL DEFAULT 0"),
            # 【需求指定字段名】中断恢复用的两个计数器（不可用内存临时变量替代）
            ("code_revise_count",
             "ALTER TABLE task_snapshots ADD COLUMN code_revise_count INTEGER NOT NULL DEFAULT 0"),
            ("reassign_task_count",
             "ALTER TABLE task_snapshots ADD COLUMN reassign_task_count INTEGER NOT NULL DEFAULT 0"),
            ("requirement_state", "ALTER TABLE task_snapshots ADD COLUMN requirement_state TEXT NOT NULL DEFAULT '{}'"),
            ("evaluator_state", "ALTER TABLE task_snapshots ADD COLUMN evaluator_state TEXT NOT NULL DEFAULT '{}'"),
        ):
            if snap_cols and column not in snap_cols:
                self._conn.execute(ddl)
        # ==================================================================
        # 【第三轮·Bug1 修复】审批"提交后计时"字段增量迁移：
        #   历史库的 pending 审批没有 resume_state → 默认 'idle'（不计时），
        #   由恢复扫描按需开启计时，绝不把"等待用户点击"阶段算进 30 秒。
        # ==================================================================
        for column, ddl in (
            ("resume_started_at", "ALTER TABLE approvals ADD COLUMN resume_started_at REAL"),
            ("resume_deadline", "ALTER TABLE approvals ADD COLUMN resume_deadline REAL"),
            ("resume_state", "ALTER TABLE approvals ADD COLUMN resume_state TEXT NOT NULL DEFAULT 'idle'"),
            ("resume_finished_at", "ALTER TABLE approvals ADD COLUMN resume_finished_at REAL"),
        ):
            if approval_cols and column not in approval_cols:
                self._conn.execute(ddl)
        # 历史 pending 审批统一置为 idle（等待用户操作阶段不计时）
        if approval_cols and "resume_state" not in approval_cols:
            self._conn.execute("UPDATE approvals SET resume_state='idle'")
        # 历史会话（workspace_id 为空）统一归入默认工作区
        self._conn.execute(
            "UPDATE sessions SET workspace_id=? WHERE workspace_id IS NULL OR workspace_id=''",
            (DEFAULT_WORKSPACE_ID,),
        )
        # 确保默认工作区存在（历史数据无损，仅补分组归属）
        row = self._conn.execute(
            "SELECT workspace_id FROM workspaces WHERE workspace_id=?", (DEFAULT_WORKSPACE_ID,)
        ).fetchone()
        if not row:
            now = time.time()
            self._conn.execute(
                "INSERT INTO workspaces(workspace_id,name,created_at,updated_at,sort_order,meta) "
                "VALUES(?,?,?,?,?,?)",
                (DEFAULT_WORKSPACE_ID, DEFAULT_WORKSPACE_NAME, now, now, 0, "{}"),
            )

    # ---------------- 底层 ----------------
    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        with self._lock:
            self._conn.executemany(sql, seq)
            self._conn.commit()

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------- 工作区（【需求点 二、工作区分组管理】） ----------------
    def create_workspace(self, workspace_id: str, name: str, *, sort_order: int | None = None) -> dict:
        now = time.time()
        if sort_order is None:
            row = self.query_one("SELECT COALESCE(MAX(sort_order),0)+1 AS nxt FROM workspaces")
            sort_order = int((row or {}).get("nxt") or 1)
        self.execute(
            "INSERT INTO workspaces(workspace_id,name,created_at,updated_at,sort_order,meta) "
            "VALUES(?,?,?,?,?,?)",
            (workspace_id, name, now, now, sort_order, "{}"),
        )
        return self.get_workspace(workspace_id) or {}

    def get_workspace(self, workspace_id: str) -> dict | None:
        return self.query_one("SELECT * FROM workspaces WHERE workspace_id=?", (workspace_id,))

    def list_workspaces(self) -> list[dict]:
        return self.query(
            """SELECT w.*, (SELECT COUNT(*) FROM sessions s WHERE s.workspace_id=w.workspace_id) AS session_count
               FROM workspaces w ORDER BY w.sort_order ASC, w.created_at ASC"""
        )

    def rename_workspace(self, workspace_id: str, name: str) -> bool:
        cur = self.execute(
            "UPDATE workspaces SET name=?, updated_at=? WHERE workspace_id=?",
            (name, time.time(), workspace_id),
        )
        return bool(cur.rowcount)

    def set_workspace_meta(self, workspace_id: str, patch: dict) -> bool:
        """合并写入工作区扩展元数据（用于保存所选本地文件夹路径）。"""
        row = self.query_one("SELECT meta FROM workspaces WHERE workspace_id=?", (workspace_id,))
        if row is None:
            return False
        try:
            meta = json.loads(row.get("meta") or "{}")
        except json.JSONDecodeError:
            meta = {}
        if not isinstance(meta, dict):
            meta = {}
        meta.update(patch or {})
        cur = self.execute(
            "UPDATE workspaces SET meta=?, updated_at=? WHERE workspace_id=?",
            (json.dumps(meta, ensure_ascii=False), time.time(), workspace_id),
        )
        return bool(cur.rowcount)

    def delete_workspace(self, workspace_id: str) -> dict:
        """删除工作区：同时清理其下全部会话数据（需求点 二、3 删除工作区）。"""
        sessions = self.query(
            "SELECT session_id FROM sessions WHERE workspace_id=?", (workspace_id,)
        )
        session_ids = [r["session_id"] for r in sessions]
        removed = {
            "sessions": 0, "tasks": 0, "messages": 0, "approvals": 0,
            "token_usage": 0, "agent_logs": 0, "think_steps": 0, "error_logs": 0,
            "task_timers": 0,
        }
        for sid in session_ids:
            removed["tasks"] += self._rowcount("DELETE FROM tasks WHERE session_id=?", (sid,))
            removed["messages"] += self._rowcount("DELETE FROM messages WHERE session_id=?", (sid,))
            removed["approvals"] += self._rowcount("DELETE FROM approvals WHERE session_id=?", (sid,))
            removed["token_usage"] += self._rowcount("DELETE FROM token_usage WHERE session_id=?", (sid,))
            removed["agent_logs"] += self._rowcount("DELETE FROM agent_logs WHERE session_id=?", (sid,))
            removed["think_steps"] += self._rowcount("DELETE FROM think_steps WHERE session_id=?", (sid,))
            removed["error_logs"] += self._rowcount("DELETE FROM error_logs WHERE session_id=?", (sid,))
            # 【需求点 二、3】会话删除时同步清理计时记录
            removed["task_timers"] += self._rowcount(
                "DELETE FROM session_task_timers WHERE session_id=?", (sid,))
            removed["sessions"] += self._rowcount("DELETE FROM sessions WHERE session_id=?", (sid,))
        self._rowcount("DELETE FROM workspaces WHERE workspace_id=?", (workspace_id,))
        return {"workspace_id": workspace_id, "session_ids": session_ids, "removed": removed}

    def _rowcount(self, sql: str, params: Sequence[Any]) -> int:
        cur = self.execute(sql, params)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    # ---------------- 会话 ----------------
    def upsert_session(self, session_id: str, title: str, *, meta: dict | None = None,
                       workspace_id: str | None = None) -> None:
        now = time.time()
        existing = self.query_one("SELECT session_id FROM sessions WHERE session_id=?", (session_id,))
        if existing:
            self.execute(
                "UPDATE sessions SET title=?, updated_at=? WHERE session_id=?",
                (title, now, session_id),
            )
        else:
            self.execute(
                """INSERT INTO sessions(session_id,title,created_at,updated_at,status,meta,workspace_id)
                   VALUES(?,?,?,?,?,?,?)""",
                (session_id, title, now, now, "active",
                 json.dumps(meta or {}, ensure_ascii=False), workspace_id or DEFAULT_WORKSPACE_ID),
            )

    def touch_session(self, session_id: str) -> None:
        self.execute("UPDATE sessions SET updated_at=? WHERE session_id=?", (time.time(), session_id))

    def list_sessions(self, limit: int = 200, workspace_id: str | None = None) -> list[dict]:
        if workspace_id:
            return self.query(
                "SELECT * FROM sessions WHERE workspace_id=? ORDER BY updated_at DESC LIMIT ?",
                (workspace_id, limit),
            )
        return self.query("SELECT * FROM sessions ORDER BY updated_at DESC LIMIT ?", (limit,))

    def move_session(self, session_id: str, workspace_id: str) -> bool:
        """会话迁移归属（需求点 二、3 会话移动归属）。"""
        cur = self.execute(
            "UPDATE sessions SET workspace_id=?, updated_at=? WHERE session_id=?",
            (workspace_id, time.time(), session_id),
        )
        return bool(cur.rowcount)

    def delete_session(self, session_id: str) -> dict:
        """删除单个会话及其全部关联数据（工作区删除时复用）。"""
        removed = {
            "tasks": self._rowcount("DELETE FROM tasks WHERE session_id=?", (session_id,)),
            "messages": self._rowcount("DELETE FROM messages WHERE session_id=?", (session_id,)),
            "approvals": self._rowcount("DELETE FROM approvals WHERE session_id=?", (session_id,)),
            "token_usage": self._rowcount("DELETE FROM token_usage WHERE session_id=?", (session_id,)),
            "agent_logs": self._rowcount("DELETE FROM agent_logs WHERE session_id=?", (session_id,)),
            "think_steps": self._rowcount("DELETE FROM think_steps WHERE session_id=?", (session_id,)),
            "error_logs": self._rowcount("DELETE FROM error_logs WHERE session_id=?", (session_id,)),
            # 【需求点 二、3】同步清理该会话的任务计时记录
            "task_timers": self._rowcount(
                "DELETE FROM session_task_timers WHERE session_id=?", (session_id,)),
            "sessions": self._rowcount("DELETE FROM sessions WHERE session_id=?", (session_id,)),
        }
        return {"session_id": session_id, "removed": removed}

    def get_session(self, session_id: str) -> dict | None:
        return self.query_one("SELECT * FROM sessions WHERE session_id=?", (session_id,))

    # ------------------------------------------------------------------
    # 【需求点 二、3】会话元数据（meta）合并写入 —— 用于持久化"上一次任务耗时"
    #   兼容旧会话：meta 缺省为空对象 {}，缺失字段按空处理，不做强制迁移。
    # ------------------------------------------------------------------
    def get_session_meta(self, session_id: str) -> dict:
        row = self.get_session(session_id)
        if not row:
            return {}
        try:
            meta = json.loads(row.get("meta") or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}
        return meta if isinstance(meta, dict) else {}

    def set_session_meta(self, session_id: str, patch: dict) -> bool:
        """合并写入会话元数据（不覆盖其他已有键，旧数据零损伤）。"""
        if not self.get_session(session_id):
            return False
        meta = self.get_session_meta(session_id)
        meta.update(patch or {})
        cur = self.execute(
            "UPDATE sessions SET meta=?, updated_at=? WHERE session_id=?",
            (json.dumps(meta, ensure_ascii=False), time.time(), session_id),
        )
        return bool(cur.rowcount)

    # ------------------------------------------------------------------
    # 【需求点 二、3 任务耗时数据存储】计时记录读写
    # ------------------------------------------------------------------
    def insert_task_timer(self, row: dict) -> None:
        """写入/更新一条任务计时记录（timer_id 幂等，支持同一任务多次收敛）。"""
        self.execute(
            """INSERT OR REPLACE INTO session_task_timers
               (timer_id,session_id,task_id,title,status,started_at,finished_at,
                elapsed_seconds,task_status,reason,record,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["timer_id"], row["session_id"], row.get("task_id") or "",
                row.get("title") or "", row.get("status") or "running",
                float(row.get("started_at") or time.time()),
                row.get("finished_at"),
                float(row.get("elapsed_seconds") or 0.0),
                row.get("task_status") or "",
                row.get("reason") or "",
                json.dumps(row.get("record") or {}, ensure_ascii=False),
                float(row.get("created_at") or time.time()),
            ),
        )

    @staticmethod
    def _timer_row_to_dict(row: dict) -> dict:
        try:
            record = json.loads(row.get("record") or "{}")
        except (json.JSONDecodeError, TypeError):
            record = {}
        return {
            "timer_id": row.get("timer_id"),
            "session_id": row.get("session_id"),
            "task_id": row.get("task_id") or "",
            "title": row.get("title") or "",
            "status": row.get("status"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "elapsed_seconds": float(row.get("elapsed_seconds") or 0.0),
            "task_status": row.get("task_status") or "",
            "reason": row.get("reason") or "",
            "record": record if isinstance(record, dict) else {},
            "created_at": row.get("created_at"),
        }

    def get_task_timer(self, timer_id: str) -> dict | None:
        row = self.query_one("SELECT * FROM session_task_timers WHERE timer_id=?", (timer_id,))
        return self._timer_row_to_dict(row) if row else None

    def get_task_timer_by_task(self, task_id: str) -> dict | None:
        row = self.query_one(
            "SELECT * FROM session_task_timers WHERE task_id=? ORDER BY created_at DESC LIMIT 1",
            (task_id,),
        )
        return self._timer_row_to_dict(row) if row else None

    def latest_task_timer(self, session_id: str) -> dict | None:
        """该会话**最近一次已结束**的任务计时记录（历史会话回显"上一次任务耗时"）。"""
        row = self.query_one(
            """SELECT * FROM session_task_timers
               WHERE session_id=? AND status IN ('success','failed','cancelled')
               ORDER BY created_at DESC, finished_at DESC LIMIT 1""",
            (session_id,),
        )
        return self._timer_row_to_dict(row) if row else None

    def list_task_timers(self, session_id: str, limit: int = 100) -> list[dict]:
        rows = self.query(
            """SELECT * FROM session_task_timers WHERE session_id=?
               ORDER BY created_at DESC LIMIT ?""",
            (session_id, limit),
        )
        return [self._timer_row_to_dict(r) for r in rows]

    def timer_summary(self, session_id: str) -> dict:
        """会话耗时汇总（真实后端数据，供状态栏 / 历史会话展示，禁止前端伪造）。"""
        rows = self.query(
            """SELECT status, COUNT(*) AS cnt, COALESCE(SUM(elapsed_seconds),0) AS total
               FROM session_task_timers WHERE session_id=? GROUP BY status""",
            (session_id,),
        )
        by_status = {r["status"]: {"count": int(r["cnt"]), "total_seconds": float(r["total"] or 0.0)}
                     for r in rows}
        done_total = sum(v["total_seconds"] for k, v in by_status.items()
                         if k in ("success", "failed", "cancelled"))
        return {
            "session_id": session_id,
            "recorded_tasks": sum(v["count"] for v in by_status.values()),
            "finished_tasks": sum(v["count"] for k, v in by_status.items()
                                  if k in ("success", "failed", "cancelled")),
            "total_elapsed_seconds": round(done_total, 3),
            "by_status": by_status,
        }

    # ---------------- 【需求点 Bug2】任务完整快照（业务循环的持久化载体） ----------------
    def save_task_snapshot(self, row: dict) -> None:
        """覆盖写一次任务完整快照（同一 task_id 只保留最新一份）。

        快照是「业务语义循环」的恢复点：审批暂停 / 服务重启后据此恢复上下文继续跑，
        因此这里必须是**完整覆盖**而不是增量追加。
        """
        now = time.time()
        # 【计数器双写】fix_rounds(dict,按子任务) 与其"整型投影" code_revise_count 同源：
        #   整型列 = 本轮最大修改轮次，供接口/运维直读；dict 列保留按子任务的精度。
        fix_rounds = dict(row.get("fix_rounds", {}) or {})
        code_revise_count = row.get("code_revise_count")
        if code_revise_count is None:
            code_revise_count = max([int(v) for v in fix_rounds.values()] or [0])
        reallocations = int(row.get("reallocations") or 0)
        reassign_task_count = row.get("reassign_task_count")
        if reassign_task_count is None:
            reassign_task_count = reallocations
        self.execute(
            """INSERT OR REPLACE INTO task_snapshots
               (task_id,session_id,parent_task_id,status,stage,user_input,plan,completed,
                remaining,messages,approval,loop_state,payload,approval_deadline,
                fix_rounds,reallocations,requirement_state,evaluator_state,
                code_revise_count,reassign_task_count,
                created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["task_id"], row["session_id"], row.get("parent_task_id"),
                row.get("status", STATUS_PENDING), row.get("stage", ""),
                row.get("user_input", ""),
                json.dumps(row.get("plan", {}) or {}, ensure_ascii=False),
                json.dumps(row.get("completed", []) or [], ensure_ascii=False),
                json.dumps(row.get("remaining", []) or [], ensure_ascii=False),
                json.dumps(row.get("messages", []) or [], ensure_ascii=False),
                json.dumps(row.get("approval", {}) or {}, ensure_ascii=False),
                json.dumps(row.get("loop_state", {}) or {}, ensure_ascii=False),
                json.dumps(row.get("payload", {}) or {}, ensure_ascii=False),
                # 【新增】审批截止时间：非审批态写 NULL（裁决后超时判定必须失效）
                (float(row["approval_deadline"])
                 if row.get("approval_deadline") is not None else None),
                # 【第三轮新增】业务循环计数器（校验修改轮次 / 需求重分配次数 / 两类状态）
                json.dumps(fix_rounds, ensure_ascii=False),
                reallocations,
                json.dumps(row.get("requirement_state", {}) or {}, ensure_ascii=False),
                json.dumps(row.get("evaluator_state", {}) or {}, ensure_ascii=False),
                int(code_revise_count or 0),
                int(reassign_task_count or 0),
                float(row.get("created_at") or now), now,
            ),
        )

    @staticmethod
    def _snapshot_row_to_dict(row: dict) -> dict:
        def load(key: str, default):
            try:
                value = json.loads(row.get(key) or "")
            except (json.JSONDecodeError, TypeError):
                return default
            return value if value is not None else default
        return {
            "task_id": row["task_id"],
            "session_id": row["session_id"],
            "parent_task_id": row.get("parent_task_id"),
            "status": row.get("status") or STATUS_PENDING,
            "stage": row.get("stage") or "",
            "user_input": row.get("user_input") or "",
            "plan": load("plan", {}),
            "completed": load("completed", []),
            "remaining": load("remaining", []),
            "messages": load("messages", []),
            "approval": load("approval", {}),
            "loop_state": load("loop_state", {}),
            "payload": load("payload", {}),
            "approval_deadline": (float(row["approval_deadline"])
                                  if row.get("approval_deadline") is not None else None),
            # 【第三轮新增】业务循环计数器（缺列/损坏时回落默认值，历史快照照常可读）
            "fix_rounds": load("fix_rounds", {}),
            "reallocations": int(row.get("reallocations") or 0),
            # 【需求指定字段名】整型投影：优先读新列，历史快照回落由 dict/整型列推导
            "code_revise_count": int(
                row.get("code_revise_count")
                if row.get("code_revise_count") is not None
                else max([int(v) for v in (load("fix_rounds", {}) or {}).values()] or [0])),
            "reassign_task_count": int(
                row.get("reassign_task_count")
                if row.get("reassign_task_count") is not None
                else int(row.get("reallocations") or 0)),
            "requirement_state": load("requirement_state", {}),
            "evaluator_state": load("evaluator_state", {}),
            "created_at": float(row.get("created_at") or 0.0),
            "updated_at": float(row.get("updated_at") or 0.0),
        }

    def get_task_snapshot(self, task_id: str) -> dict | None:
        row = self.query_one("SELECT * FROM task_snapshots WHERE task_id=?", (task_id,))
        return self._snapshot_row_to_dict(row) if row else None

    def latest_task_snapshot(self, session_id: str, *, statuses: Sequence[str] | None = None) -> dict | None:
        sql = "SELECT * FROM task_snapshots WHERE session_id=?"
        params: list[Any] = [session_id]
        if statuses:
            sql += f" AND status IN ({','.join('?' for _ in statuses)})"
            params.extend(statuses)
        sql += " ORDER BY updated_at DESC LIMIT 1"
        row = self.query_one(sql, tuple(params))
        return self._snapshot_row_to_dict(row) if row else None

    def list_pending_approval_snapshots(self, session_id: str | None = None,
                                        limit: int = 50) -> list[dict]:
        """仍处于 waiting_approval 的任务快照（用于拦截新任务提交 + 审批恢复）。"""
        sql = ("SELECT * FROM task_snapshots WHERE status=? "
               "ORDER BY updated_at DESC LIMIT ?")
        params: list[Any] = [STATUS_WAITING_APPROVAL, limit]
        if session_id:
            sql = ("SELECT * FROM task_snapshots WHERE status=? AND session_id=? "
                   "ORDER BY updated_at DESC LIMIT ?")
            params = [STATUS_WAITING_APPROVAL, session_id, limit]
        return [self._snapshot_row_to_dict(r) for r in self.query(sql, tuple(params))]

    def delete_task_snapshot(self, task_id: str) -> int:
        cur = self.execute("DELETE FROM task_snapshots WHERE task_id=?", (task_id,))
        return int(cur.rowcount or 0)

    # ---------------- 任务 ----------------
    def insert_task(self, row: dict) -> None:
        self.execute(
            """INSERT OR REPLACE INTO tasks
               (task_id,session_id,parent_task_id,title,agent_role,status,iteration,retry_count,
                review_rejects,created_at,started_at,finished_at,deadline_at,result,error_code,error_message)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["task_id"], row["session_id"], row.get("parent_task_id"), row["title"],
                row["agent_role"], row["status"], row.get("iteration", 0), row.get("retry_count", 0),
                row.get("review_rejects", 0), row["created_at"], row.get("started_at"),
                row.get("finished_at"), row.get("deadline_at"), row.get("result"),
                row.get("error_code"), row.get("error_message"),
            ),
        )

    def update_task(self, task_id: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        self.execute(f"UPDATE tasks SET {cols} WHERE task_id=?", (*fields.values(), task_id))

    def get_task(self, task_id: str) -> dict | None:
        return self.query_one("SELECT * FROM tasks WHERE task_id=?", (task_id,))

    def list_tasks(self, session_id: str, limit: int = 100) -> list[dict]:
        return self.query(
            "SELECT * FROM tasks WHERE session_id=? ORDER BY created_at ASC LIMIT ?",
            (session_id, limit),
        )

    def list_child_tasks(self, parent_task_id: str) -> list[dict]:
        return self.query("SELECT * FROM tasks WHERE parent_task_id=? ORDER BY created_at ASC", (parent_task_id,))

    # ---------------- 消息 ----------------
    def insert_message(self, row: dict) -> None:
        self.execute(
            """INSERT OR REPLACE INTO messages
               (msg_id,session_id,task_id,parent_task_id,sender_agent,receiver_agent,msg_type,
                payload_content,payload_metadata,status,timestamp,stored_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["msg_id"], row["session_id"], row["task_id"], row.get("parent_task_id"),
                row["sender_agent"], row["receiver_agent"], row["msg_type"],
                json.dumps(row.get("payload", {}).get("content", ""), ensure_ascii=False),
                json.dumps(row.get("payload", {}).get("metadata", {}), ensure_ascii=False),
                row["status"], row["timestamp"], time.time(),
            ),
        )

    def list_messages(self, task_id: str) -> list[dict]:
        return self.query("SELECT * FROM messages WHERE task_id=? ORDER BY timestamp ASC", (task_id,))

    # ---------------- 思考过程 ----------------
    def add_think_step(self, *, session_id: str, task_id: str, agent_role: str,
                       step_index: int, step_type: str, step_text: str,
                       level: str = "info") -> None:
        """【BUG-C 2/5】level: info / warn / error —— error 会在前端展示红色错误标识。"""
        self.execute(
            """INSERT INTO think_steps(session_id,task_id,agent_role,step_index,step_type,step_text,level,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (session_id, task_id, agent_role, step_index, step_type, step_text,
             str(level or "info"), time.time()),
        )

    def list_think_steps(self, task_id: str) -> list[dict]:
        return self.query(
            "SELECT * FROM think_steps WHERE task_id=? ORDER BY id ASC", (task_id,)
        )

    # ---------------- 审批 ----------------
    def insert_approval(self, row: dict) -> None:
        self.execute(
            """INSERT OR REPLACE INTO approvals
               (approval_id,session_id,task_id,agent_role,operation_type,risk_level,operation_desc,
                operation_params,danger_reason,state,decided_by,decided_at,backend_recheck,
                approval_deadline,resume_started_at,resume_deadline,resume_state,
                resume_finished_at,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["approval_id"], row["session_id"], row["task_id"], row["agent_role"],
                row["operation_type"], row["risk_level"], row["operation_desc"],
                row.get("operation_params", ""), row.get("danger_reason", ""),
                row["state"], row.get("decided_by"), row.get("decided_at"),
                json.dumps(row.get("backend_recheck", {}), ensure_ascii=False),
                # 【新增】审批 30 秒截止时间（创建时写入，裁决后保留用于留痕追溯）
                (float(row["approval_deadline"])
                 if row.get("approval_deadline") is not None else None),
                # 【第三轮·Bug1】执行链路计时字段：创建时全部为空/idle（等待用户阶段不计时）
                (float(row["resume_started_at"])
                 if row.get("resume_started_at") is not None else None),
                (float(row["resume_deadline"])
                 if row.get("resume_deadline") is not None else None),
                str(row.get("resume_state") or "idle"),
                (float(row["resume_finished_at"])
                 if row.get("resume_finished_at") is not None else None),
                row["created_at"],
            ),
        )

    def start_approval_resume_window(self, approval_id: str, *, deadline: float,
                                     started_at: float | None = None) -> bool:
        """【第三轮·Bug1 修复】开启"审批提交后"的执行链路 30 秒计时窗口。

        语义：计时起点 = 后端收到 /api/approval/submit 的时刻（**不是**进入
        waiting_approval 的时刻）。等待用户点击按钮的阶段完全不计时。

        【修正】不能要求 state='pending'：本方法在二次校验（verify_and_decide
        已把 state 改为 manual/rejected）**之后**调用，若限制 pending 会永远开启失败
        → 提交后无法计时。改为"窗口未在计时中即可开启"，从而：
          · 幂等：已在 running 的单不会重置计时（避免重复提交刷新窗口）；
          · 补跑恢复：settled / timeout 的单可重新开启（仍以本次收到请求时刻为起点）。
        """
        cur = self.execute(
            """UPDATE approvals SET resume_state='running', resume_started_at=?,
                   resume_deadline=?, resume_finished_at=NULL
               WHERE approval_id=? AND COALESCE(resume_state,'idle') != 'running'""",
            (float(started_at if started_at is not None else time.time()),
             float(deadline), approval_id),
        )
        return bool(cur.rowcount)

    def force_start_approval_resume_window(self, approval_id: str, *, deadline: float,
                                          started_at: float | None = None) -> None:
        """强制重置计时窗口（补跑恢复：即使已在 running 也以本次请求时刻重新计时）。"""
        self.execute(
            """UPDATE approvals SET resume_state='running', resume_started_at=?,
                   resume_deadline=?, resume_finished_at=NULL WHERE approval_id=?""",
            (float(started_at if started_at is not None else time.time()),
             float(deadline), approval_id),
        )

    def finish_approval_resume_window(self, approval_id: str, *, state: str = "settled",
                                      force: bool = False) -> bool:
        """关闭执行链路计时窗口（链路收敛 → settled；超时 → timeout）。

        force=False 时不覆盖 timeout（超时结论优先，供审计追溯）。
        """
        if force:
            cur = self.execute(
                """UPDATE approvals SET resume_state=?, resume_finished_at=?
                   WHERE approval_id=?""",
                (str(state or "settled"), time.time(), approval_id),
            )
        else:
            cur = self.execute(
                """UPDATE approvals SET resume_state=?, resume_finished_at=?
                   WHERE approval_id=? AND resume_state != 'timeout'""",
                (str(state or "settled"), time.time(), approval_id),
            )
        return bool(cur.rowcount)

    def list_running_resume_approvals(self, limit: int = 200) -> list[dict]:
        """【第三轮·Bug1】仍在"执行链路计时中"的审批（超时看门狗消费）。

        只取 resume_state='running'：等待用户点击（idle）的单**永远不会**被超时判定命中。
        """
        return self.query(
            "SELECT * FROM approvals WHERE resume_state='running' "
            "ORDER BY resume_deadline ASC LIMIT ?", (limit,),
        )

    def list_pending_approvals(self, session_id: str | None = None,
                               limit: int = 200) -> list[dict]:
        """全部"仍处于 pending（等待人工审批）"的审批记录。

        【新增】30 秒超时看门狗专用查询：只取 pending，
        已裁决记录（manual / rejected / timeout）不会命中 → 超时裁决天然幂等。
        """
        sql = "SELECT * FROM approvals WHERE state='pending'"
        params: list[Any] = []
        if session_id:
            sql += " AND session_id=?"
            params.append(session_id)
        sql += " ORDER BY created_at ASC LIMIT ?"
        params.append(limit)
        return self.query(sql, tuple(params))

    def set_approval_deadline(self, approval_id: str, deadline: float | None) -> None:
        """更新审批截止时间（审批超时窗口重置 / 裁决后清空时调用）。"""
        self.execute("UPDATE approvals SET approval_deadline=? WHERE approval_id=?",
                     ((float(deadline) if deadline is not None else None), approval_id))

    def get_approval(self, approval_id: str) -> dict | None:
        return self.query_one("SELECT * FROM approvals WHERE approval_id=?", (approval_id,))

    def list_approvals(self, *, session_id: str | None = None, state: str | None = None,
                       limit: int = 200) -> list[dict]:
        sql = "SELECT * FROM approvals WHERE 1=1"
        params: list[Any] = []
        if session_id:
            sql += " AND session_id=?"
            params.append(session_id)
        if state:
            sql += " AND state=?"
            params.append(state)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return self.query(sql, params)

    def latest_approval_for_task(self, task_id: str) -> dict | None:
        """该子任务最近一条审批记录（用于判断"是否已经问过用户 / 是否已裁决"）。"""
        return self.query_one(
            "SELECT * FROM approvals WHERE task_id=? ORDER BY created_at DESC LIMIT 1",
            (task_id,),
        )

    def clear_approvals(self, session_id: str | None = None) -> int:
        if session_id:
            cur = self.execute("DELETE FROM approvals WHERE session_id=?", (session_id,))
        else:
            cur = self.execute("DELETE FROM approvals")
        return cur.rowcount or 0

    def resolve_approval(self, approval_id: str, *, state: str, decided_by: str,
                         recheck: dict) -> bool:
        """后端二次校验通过后落库；仅当记录仍为 pending 才允许变更（防重放）。"""
        cur = self.execute(
            """UPDATE approvals SET state=?, decided_by=?, decided_at=?, backend_recheck=?
               WHERE approval_id=? AND state='pending'""",
            (state, decided_by, time.time(), json.dumps(recheck, ensure_ascii=False), approval_id),
        )
        return bool(cur.rowcount)

    # ---------------- Token 统计（第2章 2.4：四维度） ----------------
    def insert_token_usage(self, row: dict) -> None:
        self.execute(
            """INSERT INTO token_usage
               (session_id,task_id,agent_role,provider,model,input_tokens,output_tokens,
                cached_tokens,cache_hit_rate,elapsed_ms,steps,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["session_id"], row["task_id"], row["agent_role"], row["provider"], row["model"],
                row.get("input_tokens", 0), row.get("output_tokens", 0), row.get("cached_tokens", 0),
                row.get("cache_hit_rate", 0.0), row.get("elapsed_ms", 0), row.get("steps", 0), time.time(),
            ),
        )

    def token_summary(self, *, session_id: str | None = None, task_id: str | None = None) -> dict:
        sql = """SELECT COALESCE(SUM(input_tokens),0) AS input_tokens,
                        COALESCE(SUM(output_tokens),0) AS output_tokens,
                        COALESCE(SUM(cached_tokens),0) AS cached_tokens,
                        COALESCE(SUM(elapsed_ms),0) AS elapsed_ms,
                        COALESCE(SUM(steps),0) AS steps,
                        COUNT(*) AS calls
                 FROM token_usage WHERE 1=1"""
        params: list[Any] = []
        if session_id:
            sql += " AND session_id=?"
            params.append(session_id)
        if task_id:
            sql += " AND task_id=?"
            params.append(task_id)
        row = self.query_one(sql, params) or {}
        inp = int(row.get("input_tokens") or 0)
        cached = int(row.get("cached_tokens") or 0)
        row["cache_hit_rate"] = round((cached / inp * 100.0), 1) if inp else 0.0
        return row

    def token_breakdown(self, session_id: str) -> list[dict]:
        """按 session_id / task_id / Agent角色 / 模型 四维度统计。"""
        return self.query(
            """SELECT task_id, agent_role, provider, model,
                      SUM(input_tokens) AS input_tokens,
                      SUM(output_tokens) AS output_tokens,
                      SUM(cached_tokens) AS cached_tokens
               FROM token_usage WHERE session_id=?
               GROUP BY task_id, agent_role, provider, model
               ORDER BY SUM(input_tokens)+SUM(output_tokens) DESC""",
            (session_id,),
        )

    # ---------------- Agent 调用日志 ----------------
    def log_agent(self, *, session_id: str, task_id: str, agent_role: str, event: str,
                  detail: str = "", level: str = "info") -> None:
        self.execute(
            """INSERT INTO agent_logs(session_id,task_id,agent_role,event,detail,level,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (session_id, task_id, agent_role, event, detail, level, time.time()),
        )

    def list_agent_logs(self, task_id: str) -> list[dict]:
        return self.query("SELECT * FROM agent_logs WHERE task_id=? ORDER BY id ASC", (task_id,))

    # ---------------- 错误日志 ----------------
    def log_error(self, *, error_code: str, message: str, session_id: str | None = None,
                  task_id: str | None = None, agent_role: str | None = None, stack: str | None = None) -> None:
        self.execute(
            """INSERT INTO error_logs(session_id,task_id,agent_role,error_code,message,stack,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (session_id, task_id, agent_role, error_code, message, stack, time.time()),
        )

    def list_errors(self, limit: int = 200) -> list[dict]:
        return self.query("SELECT * FROM error_logs ORDER BY created_at DESC LIMIT ?", (limit,))

    # ---------------- 备份导出 ----------------
    def dump_all(self) -> dict:
        return {
            "sessions": self.query("SELECT * FROM sessions ORDER BY created_at"),
            "tasks": self.query("SELECT * FROM tasks ORDER BY created_at"),
            "messages": self.query("SELECT * FROM messages ORDER BY timestamp"),
            "approvals": self.query("SELECT * FROM approvals ORDER BY created_at"),
            "token_usage": self.query("SELECT * FROM token_usage ORDER BY created_at"),
            "agent_logs": self.query("SELECT * FROM agent_logs ORDER BY created_at"),
            "error_logs": self.query("SELECT * FROM error_logs ORDER BY created_at"),
            "think_steps": self.query("SELECT * FROM think_steps ORDER BY id"),
        }
