# -*- coding: utf-8 -*-
"""任务快照服务层 —— 高危审批中断 / 断点恢复的唯一持久化入口。

需求来源（严格对齐伪代码，不简化）：
  · 队员执行途中触发高危操作 → 立刻中断 Agent 循环（停止 LLM 调用 / 停止 SSE chunk 输出）
    → **持久化保存完整任务快照存入 SQLite**（session_id / task_id / 已完成子任务列表 /
      全部消息上下文 / 待执行高危操作详情）→ 任务状态改为 waiting_approval
    → 通过 SSE 推送 approval_request；
  · 审批超时规则：**审批开始计时 30 秒，30 秒无操作自动判定审批超时，等同 rejected 拒绝**；
  · 审批提交后读取 task_id 对应快照 → 注入审批结果 → **从断点继续原有循环**
    （禁止从头重跑全部任务）。

设计约束：
  · 存储只用 SQLite（task_snapshot_service 不引入 Redis / Celery / 任何中间件）；
  · 不基于 LangGraph 等第三方编排库，快照只是原生调度循环的持久化载体；
  · 本模块是**增量模块**，不改动既有队长 Agent / 队员 Agent 业务逻辑，只提供
    「保存 / 读取 / 更新状态 / 超时检测」四类服务能力，并复用既有 Database 封装。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from backend.infrastructure.database import Database
from backend.infrastructure.logger import EcosystemLogger
from backend.utils.constants import (
    APPROVAL_OUTCOME_ALLOWED_ONCE,
    APPROVAL_OUTCOME_REJECTED,
    APPROVAL_TIMEOUT_SECONDS,
    STATUS_FAILED,
    STATUS_FINISHED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_WAITING_APPROVAL,
    is_terminal_status,
)


# ==========================================================================
# 一、状态 / 结论枚举（严格状态机管控，禁止业务代码自己拼字符串）
# ==========================================================================
# 需求要求的状态机枚举：running / waiting_approval / finished / failed
#   系统既有成功字面量为 success，finished 与 success 等价（见 constants.is_terminal_status）。
TASK_STATE_RUNNING = STATUS_RUNNING
TASK_STATE_WAITING_APPROVAL = STATUS_WAITING_APPROVAL
TASK_STATE_FINISHED = STATUS_FINISHED
TASK_STATE_FAILED = STATUS_FAILED


@dataclass
class ApprovalDeadline:
    """30 秒审批超时判定结果（唯一真值来源，前端只做展示倒计时）。"""

    approval_id: str
    started_at: float
    deadline: float
    timeout_seconds: float = APPROVAL_TIMEOUT_SECONDS

    # ---- 剩余 / 是否超时 ----
    def remaining(self, now: float | None = None) -> float:
        """剩余秒数（已超时返回 0，绝不返回负数）。"""
        return max(0.0, self.deadline - (now if now is not None else time.time()))

    def is_expired(self, now: float | None = None) -> bool:
        """是否已超过 30 秒审批窗口（超时 = 自动 rejected）。"""
        return self.remaining(now) <= 0.0

    def to_dict(self, now: float | None = None) -> dict:
        current = now if now is not None else time.time()
        return {
            "approval_id": self.approval_id,
            "started_at": self.started_at,
            "deadline": self.deadline,
            "timeout_seconds": self.timeout_seconds,
            "remaining_seconds": round(self.remaining(current), 3),
            "expired": self.is_expired(current),
        }


@dataclass
class SnapshotView:
    """任务快照的对外视图（前端/接口层只读，不直接暴露原始 JSON 列）。"""

    task_id: str
    session_id: str
    parent_task_id: str = ""
    status: str = ""
    stage: str = ""
    user_input: str = ""
    completed: list[dict] = field(default_factory=list)
    remaining: list[dict] = field(default_factory=list)
    messages: list[dict] = field(default_factory=list)
    pending_approval: dict = field(default_factory=dict)
    loop_state: dict = field(default_factory=dict)
    approval_deadline: float | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "session_id": self.session_id,
            "parent_task_id": self.parent_task_id,
            "status": self.status,
            "stage": self.stage,
            "user_input": self.user_input,
            "completed_subtasks": self.completed,
            "remaining_subtasks": self.remaining,
            "message_context": self.messages,
            "pending_approval": self.pending_approval,
            "loop_state": self.loop_state,
            "approval_deadline": self.approval_deadline,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class TaskSnapshotService:
    """任务快照服务层：保存快照 / 读取快照 / 更新任务状态 / 审批超时检测。

    该服务被 approval_center（登记截止时间）与 runtime（业务循环 + 审批恢复）共同使用，
    是"审批中断 → 快照 → 断点恢复"链路上**唯一**的数据库访问入口。
    """

    def __init__(self, db: Database, logger: EcosystemLogger):
        self.db = db
        self.logger = logger

    # ==================================================================
    # 1. 保存快照（中断点持久化）
    # ==================================================================
    def save_snapshot(self, *, task_id: str, session_id: str, status: str,
                      completed: list[dict] | None = None,
                      remaining: list[dict] | None = None,
                      messages: list[dict] | None = None,
                      pending_approval: dict | None = None,
                      loop_state: dict | None = None,
                      payload: dict | None = None,
                      plan: dict | None = None,
                      user_input: str = "",
                      parent_task_id: str | None = None,
                      stage: str = "",
                      approval_deadline: float | None = None,
                      created_at: float | None = None) -> dict:
        """把"任务完整快照"覆盖写入 SQLite task_snapshots（同一 task_id 只保留最新一份）。

        覆盖写而非追加：快照是业务循环的**恢复点**，审批通过后按它注入结果并继续跑，
        因此必须反映"截至中断点的最新状态"（已完成列表 / 待执行列表 / 消息上下文 / 高危操作详情）。
        """
        now = time.time()
        # 待执行高危操作详情：既写入 approval 列（历史列，保持兼容），
        # 又写进 payload.pending_approval（结构化读取入口），保证只读一份语义。
        approval_payload = dict(pending_approval or {})
        data = {
            "task_id": task_id,
            "session_id": session_id,
            "parent_task_id": parent_task_id,
            "status": status,
            "stage": stage or status,
            "user_input": user_input,
            "plan": plan or {},
            "completed": list(completed or []),
            "remaining": list(remaining or []),
            "messages": list(messages or []),
            "approval": approval_payload,
            "loop_state": loop_state or {},
            "payload": dict(payload or {}) | {"pending_approval": approval_payload},
            "approval_deadline": (float(approval_deadline)
                                  if approval_deadline is not None else None),
            "created_at": float(created_at or now),
        }
        self.db.save_task_snapshot(data)
        return data

    # ==================================================================
    # 2. 读取快照（断点恢复入口）
    # ==================================================================
    def load_snapshot(self, task_id: str) -> dict | None:
        """按 task_id 读取完整快照（审批恢复 / 服务重启恢复的唯一入口）。"""
        return self.db.get_task_snapshot(task_id)

    def load_latest(self, session_id: str, *,
                    statuses: list[str] | None = None) -> dict | None:
        return self.db.latest_task_snapshot(session_id, statuses=statuses)

    def find_snapshot_for_task(self, task_id: str, *, max_hops: int = 8) -> dict | None:
        """从子任务向上逐级找根任务的快照（审批记录挂在子任务上，快照挂在根任务上）。"""
        seen: set[str] = set()
        current = str(task_id or "")
        hops = 0
        while current and current not in seen and hops < max_hops:
            seen.add(current)
            hops += 1
            snapshot = self.load_snapshot(current)
            if snapshot is not None:
                return snapshot
            row = self.db.get_task(current) or {}
            current = str(row.get("parent_task_id") or "")
        return None

    def pending_approval_snapshots(self, session_id: str | None = None,
                                   limit: int = 50) -> list[dict]:
        """仍处于 waiting_approval（等待人工审批）的任务快照。"""
        return self.db.list_pending_approval_snapshots(session_id, limit=limit)

    def view(self, task_id: str) -> SnapshotView | None:
        """快照 → 对外只读视图（接口层用，字段名与需求文档一致）。"""
        row = self.load_snapshot(task_id)
        if row is None:
            return None
        return self._to_view(row)

    @staticmethod
    def _to_view(row: dict) -> SnapshotView:
        payload = row.get("payload") or {}
        approval = row.get("approval") or {}
        pending = payload.get("pending_approval") or approval or {}
        return SnapshotView(
            task_id=row.get("task_id") or "",
            session_id=row.get("session_id") or "",
            parent_task_id=row.get("parent_task_id") or "",
            status=row.get("status") or "",
            stage=row.get("stage") or "",
            user_input=row.get("user_input") or "",
            completed=list(row.get("completed") or []),
            remaining=list(row.get("remaining") or []),
            messages=list(row.get("messages") or []),
            pending_approval=dict(pending or {}),
            loop_state=dict(row.get("loop_state") or {}),
            approval_deadline=row.get("approval_deadline"),
            created_at=float(row.get("created_at") or 0.0),
            updated_at=float(row.get("updated_at") or 0.0),
        )

    # ==================================================================
    # 3. 更新任务状态（严格状态机：running / waiting_approval / finished / failed）
    # ==================================================================
    def mark_waiting_approval(self, *, task_id: str, session_id: str,
                              snapshot: dict | None = None,
                              deadline: float | None = None) -> dict | None:
        """任务进入 waiting_approval：同步改写 tasks 表状态 + 快照状态与截止时间。"""
        if not task_id:
            return None
        self.db.update_task(task_id, status=STATUS_WAITING_APPROVAL)
        row = self.load_snapshot(task_id)
        if row is None:
            return None
        if snapshot:
            # 合并最新上下文（已完成列表 / 待执行列表 / 消息上下文 / 高危操作详情）
            row.update({k: v for k, v in snapshot.items() if v is not None})
        row["status"] = STATUS_WAITING_APPROVAL
        row["approval_deadline"] = float(deadline) if deadline is not None else (
            row.get("approval_deadline"))
        self.db.save_task_snapshot(row)
        return row

    def mark_running(self, task_id: str) -> None:
        """审批裁决完成、业务循环恢复 → 状态回到 running。"""
        if task_id:
            self.db.update_task(task_id, status=STATUS_RUNNING)

    def mark_finished(self, task_id: str, *, result: str | None = None) -> None:
        """任务整体完成（finished 终态，与 success 等价）。"""
        if not task_id:
            return
        fields: dict[str, Any] = {"status": STATUS_SUCCESS, "finished_at": time.time()}
        if result is not None:
            fields["result"] = result
        self.db.update_task(task_id, **fields)
        row = self.load_snapshot(task_id)
        if row is not None:
            row["status"] = STATUS_FINISHED
            self.db.save_task_snapshot(row)

    def mark_failed(self, task_id: str, *, error_code: str = "",
                    error_message: str = "") -> None:
        """任务/子任务失败终态。"""
        if not task_id:
            return
        self.db.update_task(task_id, status=STATUS_FAILED, error_code=error_code or None,
                            error_message=error_message or None, finished_at=time.time())
        row = self.load_snapshot(task_id)
        if row is not None:
            row["status"] = STATUS_FAILED
            self.db.save_task_snapshot(row)

    def status_of(self, task_id: str) -> str:
        row = self.db.get_task(task_id) or {}
        return str(row.get("status") or "")

    # ==================================================================
    # 4b. 【第三轮·Bug1 修复】审批"提交后"的 30 秒执行链路超时检测
    #   ------------------------------------------------------------------
    #   计时语义（严格按新需求）：
    #     · 进入 waiting_approval、等待用户点击按钮的阶段 → **完全不计时**；
    #     · 后端收到 POST /api/approval/submit（或超时扫描代为裁决）的那一刻
    #       → 写入 resume_deadline = 收到请求时刻 + 30s，开始计时；
    #     · 30 秒内执行链路没有收敛（仍卡在执行中）→ 判定执行链路超时，
    #       **视为任务失败**（子任务 failed + 大任务 failed），并推送 SSE 事件。
    #   判定只认 resume_state='running' 的记录：
    #     · idle（等待用户点击）永不超时，用户想思考多久都行；
    #     · settled / timeout 已是终态，重复扫描不会重复裁决（幂等）。
    # ==================================================================
    RESUME_STATE_IDLE = "idle"          # 等待用户点击（不计时）
    RESUME_STATE_RUNNING = "running"    # 提交后执行链路计时中
    RESUME_STATE_SETTLED = "settled"    # 执行链路已收敛（计时关闭）
    RESUME_STATE_TIMEOUT = "timeout"    # 执行链路 30s 超时（任务失败）

    def start_resume_window(self, *, approval_id: str, now: float | None = None,
                            timeout_seconds: float = APPROVAL_TIMEOUT_SECONDS) -> dict:
        """【Bug1 核心】在后端**收到审批提交请求**的时刻开启 30 秒计时窗口。

        返回 {"started": bool, "started_at": float, "deadline": float, "timeout_seconds": float}。
        只对 state='pending' 的审批生效（防重放）；同一单重复提交不会重置窗口。
        """
        current = float(now if now is not None else time.time())
        deadline = current + float(timeout_seconds)
        started = bool(self.db.start_approval_resume_window(
            approval_id, deadline=deadline, started_at=current))
        return {"started": started, "started_at": current, "deadline": deadline,
                "timeout_seconds": float(timeout_seconds)}

    def finish_resume_window(self, *, approval_id: str, state: str = "settled",
                             force: bool = False) -> bool:
        """关闭执行链路计时窗口（链路收敛 → settled；超时 → timeout）。"""
        return bool(self.db.finish_approval_resume_window(
            approval_id, state=state, force=force))

    def resume_deadline_of(self, *, approval_id: str, row: dict | None = None,
                           snapshot: dict | None = None,
                           timeout_seconds: float = APPROVAL_TIMEOUT_SECONDS) -> ApprovalDeadline | None:
        """执行链路截止时间（未开始计时返回 None —— 前端据此不显示倒计时）。

        真值优先级：审批记录 resume_deadline → 快照 approval.resume_deadline
                     → 快照顶层 approval_deadline（第三轮起同义）→ 无。
        """
        record = row if row is not None else (self.db.get_approval(approval_id) or {})
        snapshot = snapshot or {}
        pending = (snapshot.get("payload") or {}).get("pending_approval") \
            or snapshot.get("approval") or {}
        raw = (record.get("resume_deadline") or pending.get("resume_deadline")
               or (snapshot.get("approval_deadline")
                   if str(record.get("resume_state") or "") == self.RESUME_STATE_RUNNING
                   else None))
        if not raw:
            return None
        started = record.get("resume_started_at") or (float(raw) - float(timeout_seconds))
        return ApprovalDeadline(approval_id=approval_id, started_at=float(started),
                                deadline=float(raw), timeout_seconds=float(timeout_seconds))

    def is_resume_expired(self, *, approval_id: str, row: dict | None = None,
                          now: float | None = None) -> bool:
        """执行链路是否已超过 30 秒（仅在计时窗口 running 时可能为真）。"""
        record = row if row is not None else (self.db.get_approval(approval_id) or {})
        if str(record.get("resume_state") or "") != self.RESUME_STATE_RUNNING:
            return False
        dl = self.resume_deadline_of(approval_id=approval_id, row=record)
        return bool(dl and dl.is_expired(now))

    def expired_resume_approvals(self, limit: int = 200) -> list[dict]:
        """扫描"已提交审批、执行链路超过 30 秒仍未收敛"的审批（看门狗消费）。

        只取 resume_state='running'：等待用户点击阶段的审批永远不会命中，
        因此"用户迟迟不点按钮"不会被误判为任务失败。
        """
        now = time.time()
        expired: list[dict] = []
        for row in self.db.list_running_resume_approvals(limit=limit):
            deadline = row.get("resume_deadline")
            if deadline is None or now < float(deadline):
                continue
            expired.append({**row, "expired_seconds": round(now - float(deadline), 3)})
        return expired

    # ==================================================================
    # 4. 审批 30 秒超时检测（第二轮遗留：裁决等待窗口）
    # ------------------------------------------------------------------
    #   【第三轮已停用】新需求下"等待用户点击"阶段不计时，本组方法仅保留给
    #   历史数据/自检脚本读取落库值使用，运行时链路不再调用（不再产生超时裁决）。
    # ==================================================================
    def deadline_of(self, *, approval_id: str, snapshot: dict | None = None,
                    created_at: float | None = None,
                    timeout_seconds: float = APPROVAL_TIMEOUT_SECONDS) -> ApprovalDeadline:
        """计算某条审批的截止时间（只看落库值，绝不臆造）。"""
        snapshot = snapshot or {}
        pending = (snapshot.get("payload") or {}).get("pending_approval") or snapshot.get("approval") or {}
        record = self.db.get_approval(approval_id) or {}
        raw = (record.get("resume_deadline") or record.get("approval_deadline")
               or pending.get("resume_deadline") or pending.get("approval_deadline")
               or snapshot.get("approval_deadline"))
        started = float(record.get("resume_started_at")
                        or created_at or record.get("created_at") or time.time())
        deadline = float(raw) if raw else started + float(timeout_seconds)
        return ApprovalDeadline(approval_id=approval_id, started_at=started,
                                deadline=deadline, timeout_seconds=float(timeout_seconds))

    def is_approval_expired(self, *, approval_id: str, snapshot: dict | None = None,
                            now: float | None = None) -> bool:
        """【第三轮语义】该审批的**执行链路**是否已超时（等价 rejected）。

        注意：等待用户点击阶段恒为 False（不计时），只有提交后链路卡住才可能为真。
        """
        row = self.db.get_approval(approval_id) or {}
        if str(row.get("resume_state") or "") != self.RESUME_STATE_RUNNING:
            return False
        dl = self.resume_deadline_of(approval_id=approval_id, row=row, snapshot=snapshot)
        return bool(dl and dl.is_expired(now))

    def expired_pending_approvals(self, session_id: str | None = None,
                                  limit: int = 200) -> list[dict]:
        """【第二轮遗留接口，第三轮语义已收敛】返回"执行链路已超时"的审批。

        保留该名称是为了兼容既有调用方；新语义下它等同于 expired_resume_approvals()。
        """
        rows = self.expired_resume_approvals(limit=limit)
        if session_id:
            rows = [r for r in rows if str(r.get("session_id") or "") == session_id]
        return rows

    # ==================================================================
    # 5. 对外统一视图（前端只读，避免前端自行拼状态）
    # ==================================================================
    def snapshot_payload(self, task_id: str) -> dict | None:
        view = self.view(task_id)
        if view is None:
            return None
        data = view.to_dict()
        approval = view.pending_approval or {}
        approval_id = str(approval.get("approval_id") or "")
        if approval_id and view.status == STATUS_WAITING_APPROVAL:
            # 【第三轮·Bug1】只有"审批已提交、执行链路计时中"才给倒计时；
            #   等待用户点击阶段 approval_countdown 为空（前端显示"等待审批提交，无超时限制"）。
            dl = self.resume_deadline_of(approval_id=approval_id,
                                         snapshot=self.load_snapshot(task_id))
            if dl is not None:
                data["approval_countdown"] = dl.to_dict()
        return data

    def snapshot_ok(self, task_id: str) -> bool:
        """终态任务是否已完成快照收口（用于验收/自检：终态快照必须存在且状态一致）。"""
        row = self.load_snapshot(task_id)
        if row is None:
            return False
        return bool(row.get("status")) and str(row.get("status")) in (
            STATUS_RUNNING, STATUS_WAITING_APPROVAL, STATUS_SUCCESS, STATUS_FINISHED,
            STATUS_FAILED)


def outcome_to_approved(outcome: str | None, approved: bool | None = None) -> bool:
    """审批结论口径归一：allowed_once → 放行；rejected → 拒绝。

    兼容历史字段 approved（前端旧版本 / 旧测试仍可能只传 approved）：
      · outcome 存在时以它为准（allowed_once / rejected）；
      · outcome 缺省时回落到 approved 布尔值。
    """
    key = str(outcome or "").strip().lower()
    if key == APPROVAL_OUTCOME_ALLOWED_ONCE:
        return True
    if key == APPROVAL_OUTCOME_REJECTED:
        return False
    return bool(approved)


def approved_to_outcome(approved: bool) -> str:
    return APPROVAL_OUTCOME_ALLOWED_ONCE if approved else APPROVAL_OUTCOME_REJECTED


__all__ = [
    "ApprovalDeadline",
    "SnapshotView",
    "TaskSnapshotService",
    "TASK_STATE_RUNNING",
    "TASK_STATE_WAITING_APPROVAL",
    "TASK_STATE_FINISHED",
    "TASK_STATE_FAILED",
    "outcome_to_approved",
    "approved_to_outcome",
    "is_terminal_status",
]
