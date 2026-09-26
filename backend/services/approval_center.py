# -*- coding: utf-8 -*-
"""
审批中心（服务层）—— 高危操作强制人工审批 + 后端二次校验

架构文档来源：
  - 第2章 2.2 规则3/4：高危操作强制审批；前端弹窗仅展示，后端必须二次校验审批状态，禁止前端绕过
  - 第3章 3.2 审批事件专属字段：risk_level / operation_desc / operation_params / danger_reason
  - 第3章 3.5 审批结果执行规则：
        1. 审批通过：继续执行当前子任务
        2. 审批拒绝：终止当前子任务，父任务可由调度Agent重新规划或结束
  - 第6章 6.3 规则4：高危审批开关永久强制开启，不可关闭
  - 第9章 9.2：审批记录永久记录

核心安全设计（不简化）：
  - 待审批记录状态机严格为 pending -> manual / rejected，同一 approval_id 只允许裁决一次（防重放）。
  - 后端二次校验：审批记录存在性、pending 状态、会话归属、风险等级一致性、
    operation_params 与本次实际待执行动作指纹（SHA256）完全一致 —— 四者全通过才放行。
  - 前端提交的任何 "approved=true" 都不被直接信任，必须通过上述校验。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from backend.bus.message import ApprovalMetadata, Message, new_payload
from backend.infrastructure.database import Database
from backend.infrastructure.logger import EcosystemLogger
from backend.utils.constants import (
    AGENT_CODE,
    APPROVAL_OUTCOME_ALLOWED_ONCE,
    APPROVAL_OUTCOME_REJECTED,
    APPROVAL_STATE_MANUAL,
    APPROVAL_STATE_PENDING,
    APPROVAL_STATE_REJECTED,
    APPROVAL_STATE_TIMEOUT,
    APPROVAL_TIMEOUT_SECONDS,
    HIGH_RISK_APPROVAL_ALWAYS_ON,
    RISK_LEVEL_HIGH,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
)
from backend.utils.paths import SecurityViolation, new_uuid


class ApprovalError(Exception):
    """审批流程异常（校验失败一律拒绝，绝不降级放行）。"""

    def __init__(self, message: str, *, code: str = "APPROVAL_REJECTED_BY_BACKEND"):
        super().__init__(message)
        self.code = code


@dataclass
class PendingExecution:
    """待执行的高危动作（审批通过后才真正执行）。"""

    approval_id: str
    session_id: str
    task_id: str
    agent_role: str
    tool: str
    args: dict
    params_fingerprint: str
    danger_reason: str
    operation_type: str
    created_at: float = field(default_factory=time.time)
    executed: bool = False
    # 【新增】该审批的 30 秒截止时间（= 审批开始计时时刻 + APPROVAL_TIMEOUT_SECONDS）。
    #   超时 = 自动等价 rejected：绝不执行高危动作，当前子任务标记 failed 回传队长。
    approval_deadline: float = 0.0


def params_fingerprint(tool: str, args: dict) -> str:
    """待执行动作指纹：防止审批通过后动作被替换（TOCTOU 攻击）。"""
    payload = json.dumps({"tool": tool, "args": args}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ApprovalCenter:
    """审批中心：创建审批请求、前端展示数据、后端二次校验、放行/终止。"""

    def __init__(self, db: Database, logger: EcosystemLogger):
        self.db = db
        self.logger = logger
        self._pending: dict[str, PendingExecution] = {}
        self._lock = threading.RLock()
        # 【新增】30 秒审批超时回调（由 Runtime 注入；未注入时超时逻辑只在 center 内落库）
        self._timeout_handler = None

    # ==================================================================
    # 一、创建审批请求（由 Agent 触发，任务立即进入 waiting_approval 暂停）
    # ==================================================================
    def create_request(
        self,
        *,
        session_id: str,
        task_id: str,
        agent_role: str,
        meta: ApprovalMetadata,
        tool: str,
        args: dict,
    ) -> tuple[Message, PendingExecution]:
        if not HIGH_RISK_APPROVAL_ALWAYS_ON:
            # 该分支不可达：开关为写死常量 True（第6章 6.3 规则4）
            raise ApprovalError("高危审批开关被试图关闭，已拒绝", code="APPROVAL_SWITCH_IMMUTABLE")

        approval_id = new_uuid()
        fingerprint = params_fingerprint(tool, args)

        # ==================================================================
        # 【第三轮·Bug1 修复】审批创建时**不启动任何 30 秒计时**：
        #   进入 waiting_approval、等待用户点击【✅ 执行一次】/【❌ 拒绝】的阶段
        #   没有超时限制（用户可以思考任意长时间）。
        #   30 秒计时的起点改为「后端收到 POST /api/approval/submit 的时刻」，
        #   由 Runtime.start_approval_resume_window() 写入 resume_deadline，
        #   判定对象是"审批提交之后的 Agent 执行链路是否卡死"（超时 = 任务失败）。
        #   因此这里 approval_deadline / resume_deadline 全部留空，
        #   resume_state='idle'（等待用户操作，看门狗永远不会命中该状态）。
        # ==================================================================
        started_at = time.time()

        record = {
            "approval_id": approval_id,
            "session_id": session_id,
            "task_id": task_id,
            "agent_role": agent_role,
            "operation_type": meta.operation_type or "高危操作",
            "risk_level": meta.risk_level,
            "operation_desc": meta.operation_desc,
            "operation_params": meta.operation_params,
            "danger_reason": meta.danger_reason,
            "state": APPROVAL_STATE_PENDING,
            "approval_deadline": None,          # 【Bug1】等待用户阶段不计时
            "resume_state": "idle",             # 【Bug1】idle = 等待用户点击
            "resume_started_at": None,
            "resume_deadline": None,
            "created_at": started_at,
        }
        self.db.insert_approval(record)
        self.db.update_task(task_id, status=STATUS_WAITING_APPROVAL)

        execution = PendingExecution(
            approval_id=approval_id, session_id=session_id, task_id=task_id,
            agent_role=agent_role, tool=tool, args=args,
            params_fingerprint=fingerprint,
            danger_reason=meta.danger_reason, operation_type=meta.operation_type,
            created_at=started_at, approval_deadline=0.0,
        )
        with self._lock:
            self._pending[approval_id] = execution

        self.logger.approval_log(
            approval_id=approval_id, session_id=session_id, task_id=task_id,
            agent_role=agent_role, operation_type=record["operation_type"],
            risk_level=meta.risk_level, state=APPROVAL_STATE_PENDING,
            detail=(f"{meta.operation_desc}｜等待用户点击（无超时限制）；"
                    f"提交后执行链路限时 {APPROVAL_TIMEOUT_SECONDS:.0f}s"),
        )
        self.logger.task_log(
            session_id=session_id, task_id=task_id, agent_role=agent_role,
            event="approval.waiting", detail=f"{meta.operation_type}: {meta.operation_desc}",
        )

        # 第3章 3.2：审批消息 metadata 强制四字段 + 指纹
        # 【Bug1】不再下发 approval_deadline（等待阶段无倒计时）；
        #   resume_timeout_seconds 仅作为"提交后执行链路限时"的说明字段。
        metadata = meta.to_dict() | {
            "approval_id": approval_id,
            "params_fingerprint": fingerprint,
            "force_high": meta.risk_level == RISK_LEVEL_HIGH,
            "approval_started_at": started_at,
            "resume_state": "idle",
            "resume_timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
            "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
            "outcome_options": ["allowed_once", "rejected"],
        }
        msg = Message(
            session_id=session_id, task_id=task_id, parent_task_id=None,
            sender_agent=agent_role, receiver_agent="user",
            msg_type="approval_request",
            payload=new_payload(meta.operation_desc, metadata),
            status=STATUS_WAITING_APPROVAL,
        )
        return msg, execution

    # ==================================================================
    # 二、前端展示（第8章 GET /api/approval/list）
    # ==================================================================
    def list_records(self, *, session_id: str | None = None, state: str | None = None,
                     limit: int = 200) -> list[dict]:
        rows = self.db.list_approvals(session_id=session_id, state=state, limit=limit)
        out: list[dict] = []
        for r in rows:
            try:
                recheck = json.loads(r.get("backend_recheck") or "{}")
            except json.JSONDecodeError:
                recheck = {}
            # 【新增】待执行动作指纹：前端提交时按原值回传，后端据此做第 V5 道校验
            #   （防止审批期间动作被替换）。取内存态；服务重启后回落为记录里的参数指纹。
            execution = self.get_pending(r["approval_id"])
            fingerprint = (execution.params_fingerprint if execution is not None
                           else recheck.get("checks", {}).get("fingerprint", ""))
            out.append({
                "approval_id": r["approval_id"],
                "session_id": r["session_id"],
                "task_id": r["task_id"],
                "agent_role": r["agent_role"],
                "operation_type": r["operation_type"],
                "risk_level": r["risk_level"],
                "operation_desc": r["operation_desc"],
                "operation_params": r["operation_params"],
                "danger_reason": r["danger_reason"],
                "state": r["state"],
                "state_label": {
                    APPROVAL_STATE_MANUAL: "人工通过",
                    APPROVAL_STATE_REJECTED: "已拒绝",
                    APPROVAL_STATE_TIMEOUT: "审批超时（自动拒绝）",
                    APPROVAL_STATE_PENDING: "等待审批",
                    "auto": "自动放行",
                }.get(r["state"], r["state"]),
                "decided_by": r.get("decided_by"),
                "decided_at": r.get("decided_at"),
                "created_at": r["created_at"],
                "created_at_text": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["created_at"])),
                # ==========================================================
                # 【第三轮·Bug1】两阶段时间语义（前端据此渲染两种完全不同的提示）：
                #   resume_state=idle    → 等待用户点击，**无超时限制**（不给倒计时）
                #   resume_state=running → 审批已提交，执行链路 30 秒计时中（给倒计时）
                #   settled / timeout    → 链路已收敛 / 已超时（终态，不再计时）
                # ==========================================================
                "resume_state": str(r.get("resume_state") or "idle"),
                "resume_state_label": {
                    "idle": "等待用户操作（无超时限制）",
                    "running": "审批已提交，执行链路计时中",
                    "settled": "执行链路已收敛",
                    "timeout": "执行链路超时（任务失败）",
                }.get(str(r.get("resume_state") or "idle"), str(r.get("resume_state") or "idle")),
                "resume_started_at": r.get("resume_started_at"),
                "resume_deadline": r.get("resume_deadline"),
                "resume_finished_at": r.get("resume_finished_at"),
                # 执行链路截止时间（仅 running 时非空；等待用户阶段为 None → 前端不显示倒计时）
                "approval_deadline": (r.get("resume_deadline")
                                      if str(r.get("resume_state") or "") == "running" else None),
                "approval_deadline_text": (
                    time.strftime("%H:%M:%S", time.localtime(r["resume_deadline"]))
                    if (r.get("resume_deadline")
                        and str(r.get("resume_state") or "") == "running") else ""),
                "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
                "remaining_seconds": (
                    round(max(0.0, float(r["resume_deadline"]) - time.time()), 3)
                    if (r.get("resume_deadline")
                        and str(r.get("resume_state") or "") == "running") else None),
                "outcome_options": ["allowed_once", "rejected"],
                "params_fingerprint": fingerprint,
                "backend_recheck": recheck,
            })
        return out

    def clear(self, *, session_id: str | None = None) -> int:
        """清空审批记录（第7.2 审批页面：清空）。仅清历史记录，待审批项不可清空。"""
        waiting = self.db.list_approvals(session_id=session_id, state=APPROVAL_STATE_PENDING)
        if waiting:
            raise ApprovalError(
                f"当前仍有 {len(waiting)} 条待审批记录，禁止清空（审批开关不可绕过）",
                code="PENDING_APPROVAL_EXISTS",
            )
        count = self.db.clear_approvals(session_id=session_id)
        self.logger.info(f"审批记录已清空 {count} 条", agent_role="system")
        return count

    def get_pending(self, approval_id: str) -> PendingExecution | None:
        with self._lock:
            return self._pending.get(approval_id)

    def mark_execution_retryable(self, approval_id: str) -> bool:
        """【修复·可恢复性】把已执行标记复位，允许"补跑恢复"再次领取该动作。

        使用场景：审批已裁决、但业务循环恢复失败（后台协程异常 / 进程被杀），
        用户显式请求补跑。此时审批单仍是终态（不会重复裁决、不会二次审批），
        仅把内存态 executed 标记复位，让恢复路径能重新拿到待执行动作；
        磁盘动作本身另有幂等保护（后端执行回执已落快照 → 不重复执行）。
        """
        with self._lock:
            execution = self._pending.get(approval_id)
            if execution is None:
                return False
            execution.executed = False
            return True

    def describe_decision(self, approval_id: str) -> dict:
        """【修复·补跑恢复】按已落库的审批终态还原"裁决结论"，不触发二次裁决。

        返回结构与 verify_and_decide 的返回保持一致（approved / outcome / execution /
        task_action），供"审批已终态但任务未恢复"的补跑路径直接复用。
        """
        row = self.db.get_approval(approval_id) or {}
        state = str(row.get("state") or "")
        approved = state == APPROVAL_STATE_MANUAL
        execution = self.get_pending(approval_id)
        try:
            recheck = json.loads(row.get("backend_recheck") or "{}")
        except json.JSONDecodeError:
            recheck = {}
        return {
            "approval_id": approval_id,
            "state": state,
            "approved": approved,
            "timed_out": state == APPROVAL_STATE_TIMEOUT,
            "outcome": (APPROVAL_OUTCOME_ALLOWED_ONCE if approved
                        else APPROVAL_OUTCOME_REJECTED),
            "execution": execution,
            "task_action": "resume" if approved else "terminate",
            "backend_recheck": recheck,
        }

    def register_execution(self, execution: PendingExecution) -> None:
        """【需求点 Bug2】把"按任务快照重建"的待执行动作登记回审批中心。

        服务重启后内存态待执行动作会丢失；只要 SQLite 快照里保留了动作参数，
        就可以重建并登记回来，保证审批通过时仍能真实执行（且指纹校验依然生效）。
        """
        if execution is None or not execution.approval_id:
            return
        with self._lock:
            self._pending[execution.approval_id] = execution

    # ==================================================================
    # 【新增】30 秒审批超时：超时回调注册 + 超时裁决
    #   ------------------------------------------------------------------
    #   需求原文：审批开始计时 30 秒，30 秒无操作，自动判定审批超时，
    #             等同于 rejected 拒绝（拒绝 → 当前子任务 failed，回传队长）。
    #   职责边界：
    #     · ApprovalCenter 只负责"落库裁决 + 拒绝语义 + 留痕"；
    #     · "拒绝之后如何恢复队长业务循环"由 Runtime 通过回调接管
    #       （拒绝 = 子任务 failed 回传队长 → 队长重试 / 换人 / 终止）。
    # ==================================================================
    def register_timeout_handler(self, handler) -> None:
        """注册"执行链路超时"回调：handler(execution, decision) -> Awaitable。

        由 Runtime 在装配阶段注册（approval_center 不反向依赖调度层）。
        """
        self._timeout_handler = handler

    def start_resume_window(self, *, approval_id: str, timeout_seconds: float | None = None) -> dict:
        """【第三轮·Bug1】后端收到审批提交请求 → 开始 30 秒执行链路计时。

        调用时机：POST /api/approval/submit 二次校验通过后、恢复协程启动前。
        在此之前（等待用户点击按钮阶段）**没有任何计时**。
        """
        limit = float(timeout_seconds if timeout_seconds is not None
                      else APPROVAL_TIMEOUT_SECONDS)
        now = time.time()
        deadline = now + limit
        started = bool(self.db.start_approval_resume_window(
            approval_id, deadline=deadline, started_at=now))
        self.logger.approval_log(
            approval_id=approval_id, session_id="", task_id="", agent_role="",
            operation_type="", risk_level="", state="resume_running",
            detail=(f"收到审批提交 → 开始执行链路计时：{limit:.0f}s"
                    f"（截止 {time.strftime('%H:%M:%S', time.localtime(deadline))}）"
                    f"{'' if started else '（记录非 pending，未开启）'}"),
        )
        return {"started": started, "started_at": now, "deadline": deadline,
                "timeout_seconds": limit}

    def settle_resume_window(self, *, approval_id: str, state: str = "settled",
                             force: bool = False) -> bool:
        """执行链路收敛 / 超时 → 关闭计时窗口（幂等）。"""
        closed = bool(self.db.finish_approval_resume_window(
            approval_id, state=state, force=force))
        if closed:
            self.logger.approval_log(
                approval_id=approval_id, session_id="", task_id="", agent_role="",
                operation_type="", risk_level="", state=f"resume_{state}",
                detail=f"执行链路计时关闭：resume_state={state}",
            )
        return closed

    def resume_window(self, approval_id: str) -> dict:
        """执行链路计时视图（供接口/前端展示；未开始计时返回计状态下限信息）。"""
        row = self.db.get_approval(approval_id) or {}
        state = str(row.get("resume_state") or "idle")
        deadline = row.get("resume_deadline")
        started = row.get("resume_started_at")
        remaining = None
        if state == "running" and deadline is not None:
            remaining = round(max(0.0, float(deadline) - time.time()), 3)
        return {
            "approval_id": approval_id,
            "resume_state": state,
            "resume_started_at": started,
            "resume_deadline": deadline,
            "resume_finished_at": row.get("resume_finished_at"),
            "remaining_seconds": remaining,
            "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
            "counting": state == "running",
        }

    def decide_resume_timeout(self, approval_id: str, *,
                              operator: str = "system-resume-timeout",
                              now: float | None = None) -> dict:
        """【第三轮·Bug1】审批已提交但执行链路 30 秒未收敛 → 判定超时（等同任务失败）。

        与第二轮语义的区别：
          · 第二轮：等待用户点击阶段计时，超时 = 等价 rejected（子任务失败）；
          · 第三轮：等待用户阶段**不计时**；只有 resume_state='running'（已提交）才判定，
            超时 = **执行链路卡死 → 任务失败**：
              ① 子任务标记 failed（APPROVAL_RESUME_TIMEOUT）；
              ② 整个大任务标记 failed 并终止（需求原文："超时视为任务失败"）；
              ③ 作废该单尚未执行的待执行动作（超时绝不放行高危操作，也不会再执行）；
              ④ 由 Runtime 推送 SSE 事件让前端退出"等待审批"并显示失败原因。

        幂等：只对 resume_state='running' 且已过期生效，重复扫描不会重复裁决。
        """
        current = float(now if now is not None else time.time())
        row = self.db.get_approval(approval_id)
        if not row:
            return {"timed_out": False, "reason": "APPROVAL_NOT_FOUND", "decision": None}
        if str(row.get("resume_state") or "idle") != "running":
            # idle（等待用户点击）→ 永不超时；settled/timeout → 已收敛，不重复裁决
            return {"timed_out": False, "reason": "NOT_COUNTING", "decision": None}
        deadline = row.get("resume_deadline")
        if deadline is None or current < float(deadline):
            return {"timed_out": False, "reason": "NOT_EXPIRED",
                    "remaining_seconds": (round(float(deadline) - current, 3)
                                          if deadline is not None else None),
                    "decision": None, "execution": None}

        execution = self.get_pending(approval_id)
        recheck = {
            "checked_at": current,
            "operator": operator,
            "checks": {
                "result": "resume_timeout_failed",
                "reason": (f"审批已提交，但执行链路在 {APPROVAL_TIMEOUT_SECONDS:.0f} 秒内未收敛，"
                           "判定超时 → 任务失败"),
                "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
                "submitted_at": row.get("resume_started_at"),
                "resume_deadline": float(deadline),
                "expired_seconds": round(current - float(deadline), 3),
                "tool": getattr(execution, "tool", ""),
                "executed": False,
            },
        }
        # ① 关闭计时窗口（超时结论优先，不被后续 settle 覆盖）
        self.db.finish_approval_resume_window(approval_id, state="timeout", force=True)
        # ② 审批记录终态：timeout（语义 = 执行链路失败）。
        #   注意：用户此前可能已裁决为 manual，但"提交后链路 30s 未收敛 = 任务失败"，
        #   因此这里强制把终态收敛为 timeout，保证"审批单状态 / 任务状态 / 前端展示"三者一致。
        self.db.execute(
            "UPDATE approvals SET state=?, decided_by=?, decided_at=?, backend_recheck=? "
            "WHERE approval_id=?",
            (APPROVAL_STATE_TIMEOUT, operator, current,
             json.dumps(recheck, ensure_ascii=False), approval_id),
        )
        # ③ 待执行动作作废（超时绝不放行任何高危操作）
        with self._lock:
            self._pending.pop(approval_id, None)
        # ④ 子任务 + 整个大任务标记 failed（需求："超时视为任务失败"）
        self.db.update_task(
            row["task_id"], status=STATUS_FAILED,
            error_code="APPROVAL_RESUME_TIMEOUT",
            error_message=(f"审批已提交，但后续 Agent 执行链路 {APPROVAL_TIMEOUT_SECONDS:.0f} 秒"
                           "未完成，判定超时 → 任务失败"),
            finished_at=current,
        )
        root_task_id = ""
        try:
            from backend.services.approval_snapshot import TaskSnapshotService
            # 复用快照服务的父任务链上溯（审批挂在子任务上，大任务号需向上找）
            snap = None
            cursor = str(row.get("task_id") or "")
            seen: set[str] = set()
            while cursor and cursor not in seen:
                seen.add(cursor)
                snap = self.db.get_task_snapshot(cursor)
                if snap is not None:
                    break
                cursor = str((self.db.get_task(cursor) or {}).get("parent_task_id") or "")
            if snap is not None:
                root_task_id = str(snap.get("task_id") or "")
                snap["status"] = STATUS_FAILED
                snap["approval_deadline"] = None
                self.db.save_task_snapshot(snap)
            _ = TaskSnapshotService  # 仅用于说明依赖来源
        except Exception:  # noqa: BLE001 快照回写失败不影响超时裁决
            pass
        if root_task_id and root_task_id != str(row.get("task_id") or ""):
            self.db.update_task(
                root_task_id, status=STATUS_FAILED,
                error_code="APPROVAL_RESUME_TIMEOUT",
                error_message="审批提交后的 Agent 执行链路 30 秒未完成，任务判定失败并终止",
                finished_at=current,
            )

        self.logger.approval_log(
            approval_id=approval_id, session_id=row["session_id"], task_id=row["task_id"],
            agent_role=row["agent_role"], operation_type=row["operation_type"],
            risk_level=row["risk_level"], state=APPROVAL_STATE_TIMEOUT,
            detail=(f"审批提交后执行链路 {APPROVAL_TIMEOUT_SECONDS:.0f}s 未收敛 → 任务失败；"
                    "未执行任何高危操作"),
        )
        self.logger.task_log(
            session_id=row["session_id"], task_id=row["task_id"], agent_role=row["agent_role"],
            event="approval.resume_timeout -> 任务失败", level="error",
            detail=("审批提交后执行链路超时（30s 未收敛）：子任务与整个大任务标记 failed；"
                    "高危操作未执行"),
        )
        decision = {
            "approval_id": approval_id,
            "state": APPROVAL_STATE_TIMEOUT,
            "approved": False,
            "timed_out": True,
            "resume_timed_out": True,
            "outcome": APPROVAL_OUTCOME_REJECTED,
            "execution": execution,
            "task_action": "terminate",
            "root_task_id": root_task_id,
            "backend_recheck": recheck,
        }
        return {"timed_out": True, "decision": decision, "execution": execution,
                "resume_deadline": float(deadline)}

    # ---- 第二轮接口保留（新语义下不再产生裁决，仅供历史自检调用） ----
    def decide_on_timeout(self, approval_id: str, *,
                          operator: str = "system-timeout",
                          now: float | None = None) -> dict:
        """兼容入口：新语义下等价于 decide_resume_timeout()。"""
        return self.decide_resume_timeout(approval_id, operator=operator, now=now)

    def timeout_handler(self):
        return getattr(self, "_timeout_handler", None)

    # ==================================================================
    # 【需求点 Bug2】任务被队长终止 → 自动作废该任务下的待审批记录
    #   业务背景：同一轮里可能产生多条高危动作审批（例如模型连续提出删除动作）；
    #   队长一旦裁决"整个大任务终止"，这些仍处于 pending 的审批单就永远等不到用户点击，
    #   若继续留在 pending：
    #     · 会一直阻塞该会话的新消息提交（边界约束1 的拦截闸门）；
    #     · 会让审批面板长期显示"等待审批"，与任务终态不一致。
    #   因此这里以 rejected（未获批准）收口，并写明是系统自动作废，绝不执行任何操作。
    # ==================================================================
    def auto_cancel_pending_approvals(self, task_ids: list[str] | None = None, *,
                                      reason: str = "任务已被终止，审批单自动作废",
                                      operator: str = "system") -> int:
        wanted = {str(t) for t in (task_ids or []) if t}
        if not wanted:
            return 0
        cancelled = 0
        for row in self.db.list_approvals(state=APPROVAL_STATE_PENDING, limit=1000):
            if str(row.get("task_id") or "") not in wanted:
                continue
            execution = self.get_pending(row["approval_id"])
            recheck = {
                "checked_at": time.time(),
                "operator": operator,
                "checks": {"result": "auto_cancelled", "reason": reason,
                           "tool": getattr(execution, "tool", "")},
            }
            updated = self.db.resolve_approval(
                row["approval_id"], state=APPROVAL_STATE_REJECTED,
                decided_by=operator, recheck=recheck,
            )
            if not updated:
                continue
            with self._lock:
                self._pending.pop(row["approval_id"], None)
            cancelled += 1
            self.logger.approval_log(
                approval_id=row["approval_id"], session_id=row["session_id"],
                task_id=row["task_id"], agent_role=row["agent_role"],
                operation_type=row["operation_type"], risk_level=row["risk_level"],
                state=APPROVAL_STATE_REJECTED,
                detail=f"系统自动作废（未执行任何操作）：{reason}",
            )
        if cancelled:
            self.logger.task_log(
                session_id="", task_id="", agent_role="system",
                event="approval.auto_cancelled", level="warn",
                detail=f"任务终止，自动作废待审批记录 {cancelled} 条：{reason}",
            )
        return cancelled

    # ==================================================================
    # 三、后端二次校验 + 裁决（第2章 2.2 规则4）
    # ==================================================================
    def verify_and_decide(
        self,
        *,
        approval_id: str,
        approved: bool,
        operator: str,
        session_id: str,
        submitted_fingerprint: str | None = None,
        submitted_risk_level: str | None = None,
    ) -> dict:
        """后端二次校验审批结果 —— 绝不信任前端提交。

        校验链（任一失败即拒绝，且记录错误日志）：
          V1 approval_id 存在且为合法待审批记录
          V2 记录状态仍为 pending（防重复裁决 / 重放）
          V3 会话归属一致（禁止跨会话审批）
          V4 风险等级一致（前端不得降级 high）
          V5 动作指纹一致（防止审批后动作被替换）
          V6 高危开关仍为开启（写死常量常量校验）
        """
        recheck: dict[str, Any] = {
            "checked_at": time.time(),
            "operator": operator,
            "checks": {},
        }

        def fail(code: str, message: str) -> "ApprovalError":
            recheck["checks"]["result"] = "failed"
            recheck["checks"]["failed_code"] = code
            self.logger.exception_log(
                error_code=code, message=message,
                session_id=session_id, task_id="", agent_role="system",
            )
            return ApprovalError(message, code=code)

        # V6 高危开关永久开启（第6章 6.3 规则4）
        if not HIGH_RISK_APPROVAL_ALWAYS_ON:
            raise fail("APPROVAL_SWITCH_IMMUTABLE", "高危审批开关非开启状态，拒绝一切裁决")
        recheck["checks"]["approval_switch_on"] = True

        # V1 记录存在
        row = self.db.get_approval(approval_id)
        if not row:
            raise fail("APPROVAL_NOT_FOUND", f"审批记录不存在：{approval_id}")
        recheck["checks"]["record_found"] = True

        # V2 状态仍为 pending
        if row["state"] != APPROVAL_STATE_PENDING:
            raise fail(
                "APPROVAL_STATE_CONFLICT",
                f"审批记录已处于终态（{row['state']}），禁止重复裁决（防重放）",
            )
        recheck["checks"]["state_pending"] = True

        # V3 会话归属
        if row["session_id"] != session_id:
            raise fail(
                "APPROVAL_SESSION_MISMATCH",
                f"审批记录不属于当前会话（记录 {row['session_id']} ≠ 提交 {session_id}）",
            )
        recheck["checks"]["session_match"] = True

        # V4 风险等级不得被前端降级
        if submitted_risk_level and submitted_risk_level != row["risk_level"]:
            raise fail(
                "APPROVAL_RISK_DOWNGRADE",
                f"提交的风险等级 {submitted_risk_level} 与记录 {row['risk_level']} 不一致，拒绝",
            )
        if row["risk_level"] == RISK_LEVEL_HIGH and not approved:
            recheck["checks"]["risk_high_rejected"] = True
        recheck["checks"]["risk_level"] = row["risk_level"]

        # V5 动作指纹一致
        execution = self.get_pending(approval_id)
        if execution is None:
            raise fail(
                "APPROVAL_EXECUTION_EXPIRED",
                "该审批对应的待执行动作已失效（服务重启或已执行），拒绝放行",
            )
        if submitted_fingerprint and submitted_fingerprint != execution.params_fingerprint:
            raise fail(
                "APPROVAL_PARAMS_CHANGED",
                "待执行动作参数在审批期间发生变化，指纹不一致，拒绝放行",
            )
        recheck["checks"]["fingerprint_match"] = True
        recheck["checks"]["tool"] = execution.tool

        # ---- 全部校验通过，落库裁决 ----
        new_state = APPROVAL_STATE_MANUAL if approved else APPROVAL_STATE_REJECTED
        recheck["checks"]["result"] = "passed"
        recheck["checks"]["approved"] = bool(approved)
        updated = self.db.resolve_approval(
            approval_id, state=new_state, decided_by=operator or "local-admin", recheck=recheck,
        )
        if not updated:
            raise fail("APPROVAL_RACE", "审批记录已被并发裁决，本次操作放弃")

        self.logger.approval_log(
            approval_id=approval_id, session_id=row["session_id"], task_id=row["task_id"],
            agent_role=row["agent_role"], operation_type=row["operation_type"],
            risk_level=row["risk_level"], state=new_state,
            detail=f"operator={operator} 二次校验通过",
        )

        if approved:
            # 第3章 3.5 规则1：审批通过 -> 继续执行当前子任务
            self.db.update_task(row["task_id"], status=STATUS_RUNNING)
            return {
                "approval_id": approval_id,
                "state": new_state,
                "approved": True,
                "execution": execution,
                "task_action": "resume",
                "backend_recheck": recheck,
            }

        # 第3章 3.5 规则2：审批拒绝 -> 终止当前子任务
        with self._lock:
            self._pending.pop(approval_id, None)
        self.db.update_task(
            row["task_id"], status=STATUS_FAILED,
            error_code="APPROVAL_REJECTED_BY_USER",
            error_message="人工审批拒绝，子任务已终止",
            finished_at=time.time(),
        )
        self.logger.task_log(
            session_id=row["session_id"], task_id=row["task_id"], agent_role=row["agent_role"],
            event="approval.rejected -> 子任务终止", detail="审批拒绝直接终止子任务", level="warn",
        )
        return {
            "approval_id": approval_id,
            "state": new_state,
            "approved": False,
            "execution": execution,
            "task_action": "terminate",
            "backend_recheck": recheck,
        }

    # ==================================================================
    # 四、标记执行完成（防重复执行）
    # ==================================================================
    def mark_executed(self, approval_id: str) -> bool:
        with self._lock:
            ex = self._pending.get(approval_id)
            if ex is None or ex.executed:
                return False
            ex.executed = True
            self._pending.pop(approval_id, None)
            return True

    def stats(self) -> dict:
        rows = self.db.list_approvals(limit=1000)
        by_state: dict[str, int] = {}
        for r in rows:
            by_state[r["state"]] = by_state.get(r["state"], 0) + 1
        with self._lock:
            pending_executions = len(self._pending)
        return {
            "total": len(rows),
            "by_state": by_state,
            "pending_executions": pending_executions,
            "approval_switch_always_on": HIGH_RISK_APPROVAL_ALWAYS_ON,
        }


__all__ = ["ApprovalCenter", "ApprovalError", "ApprovalMetadata", "PendingExecution",
           "params_fingerprint", "AGENT_CODE", "SecurityViolation"]
