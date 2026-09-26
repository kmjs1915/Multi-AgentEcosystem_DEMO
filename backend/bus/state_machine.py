# -*- coding: utf-8 -*-
"""
任务状态机（强状态机管控）

架构文档来源：
  - 第3章 3.4 任务状态机完整定义
      pending：待分发 / running：执行中 / waiting_approval：等待人工审批（任务暂停）
      success：执行完成 / failed：执行失败/超限/循环依赖拦截
  - 第3章 3.5 审批结果执行规则
      1. 审批通过：继续执行当前子任务
      2. 审批拒绝：终止当前子任务，父任务可由调度Agent重新规划或结束
  - 第2章 2.1 任务终止与防死循环规则（最大迭代20 / 最大超时30分钟 / 循环依赖直接终止 / 子任务重试最大2次）
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from backend.utils.constants import (
    ERR_ITERATION_LIMIT,
    ERR_LOOP,
    ERR_TIMEOUT,
    MAX_REVIEW_REJECTS,
    MAX_SUBTASK_RETRIES,
    MAX_TASK_ITERATIONS,
    MAX_TASK_TIMEOUT_SECONDS,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_TRANSITIONS,
    STATUS_WAITING_APPROVAL,
)


class IllegalTransition(Exception):
    """非法状态流转：直接拒绝并记录（第3章 3.4 强状态机管控）。"""

    def __init__(self, current: str, target: str, reason: str = ""):
        super().__init__(f"非法状态流转 {current} -> {target}。{reason}")
        self.current = current
        self.target = target


@dataclass
class TaskState:
    """单任务运行时状态（含全部防死循环计数器）。"""

    task_id: str
    session_id: str
    title: str
    agent_role: str
    parent_task_id: str | None = None
    status: str = STATUS_PENDING
    iteration: int = 0
    retry_count: int = 0
    review_rejects: int = 0
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    deadline_at: float = 0.0
    result: str = ""
    error_code: str = ""
    error_message: str = ""
    depend_on: list[str] = field(default_factory=list)
    history: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.deadline_at:
            self.deadline_at = self.created_at + MAX_TASK_TIMEOUT_SECONDS

    # ---------------- 状态流转 ----------------
    def transition(self, target: str, *, reason: str = "") -> None:
        """严格按 STATUS_TRANSITIONS 流转；必要时沿合法中间态推进。"""
        if target == self.status:
            return
        if target in STATUS_TRANSITIONS.get(self.status, ()):
            self._apply(target, reason)
            return
        # 未登记的直接流转：只允许沿合法中间态（pending -> running -> waiting_approval）推进
        path = _intermediate_path(self.status, target)
        if not path:
            raise IllegalTransition(self.status, target, reason)
        for step in path:
            self._apply(step, reason)

    def _apply(self, target: str, reason: str) -> None:
        self.history.append({
            "from": self.status, "to": target, "at": time.time(), "reason": reason,
        })
        self.status = target
        if target == STATUS_RUNNING and self.started_at is None:
            self.started_at = time.time()
        if target in (STATUS_SUCCESS, STATUS_FAILED):
            self.finished_at = time.time()

    def mark_running(self, reason: str = "分发执行") -> None:
        self.transition(STATUS_RUNNING, reason=reason)

    def rearm_for_redispatch(self, *, retry_count: int | None = None,
                             reason: str = "队长重新分派同一子任务") -> None:
        """【需求点 Bug2】把子任务复位为"可再次分发"状态（队长判定不达预期后重试）。

        业务语义：队长校验不满足预期 → 重新分派给同一队员重试。
        此时该子任务可能已经落 failed（例如人工审批被拒绝），而 failed 在
        状态图里是**终态**（不可再流转）。为了不破坏"五状态集合"这一硬性约束，
        这里把子任务显式复位为 running（等价于一次新的分发），并清空上一轮失败痕迹；
        复位动作只在队长循环内部调用，不会让前端看到非法状态。
        """
        self.status = STATUS_RUNNING
        if retry_count is not None:
            self.retry_count = max(int(self.retry_count or 0), int(retry_count))
        self.error_code = ""
        self.error_message = ""
        self.result = ""
        self.finished_at = None
        if self.started_at is None:
            self.started_at = time.time()
        self.history.append({
            "from": STATUS_FAILED, "to": STATUS_RUNNING, "at": time.time(),
            "reason": f"{reason}（终态复位，重试第 {self.retry_count} 次）",
        })

    def mark_waiting_approval(self, reason: str = "高危操作强制审批") -> None:
        self.transition(STATUS_WAITING_APPROVAL, reason=reason)

    def mark_success(self, result: str = "", reason: str = "执行完成") -> None:
        self.result = result or self.result
        self.transition(STATUS_SUCCESS, reason=reason)

    def mark_failed(self, error_code: str, message: str, reason: str = "执行失败") -> None:
        self.error_code = error_code
        self.error_message = message
        self.transition(STATUS_FAILED, reason=reason)

    # ---------------- 防死循环闸门 ----------------
    def bump_iteration(self) -> None:
        """每次 Agent 迭代 +1；超过 20 次立即终止任务（第2章 2.1 规则1）。"""
        self.iteration += 1
        if self.iteration > MAX_TASK_ITERATIONS:
            raise TaskLimitExceeded(
                ERR_ITERATION_LIMIT,
                f"任务迭代次数超过硬上限 {MAX_TASK_ITERATIONS} 次，强制终止",
            )

    def check_timeout(self) -> None:
        """超时检查：超过 30 分钟自动终止（第2章 2.1 规则2 / 第9章 9.1 规则3）。"""
        if time.time() > self.deadline_at:
            raise TaskLimitExceeded(
                ERR_TIMEOUT,
                f"任务执行超过最大超时 {MAX_TASK_TIMEOUT_SECONDS // 60} 分钟，自动终止",
            )

    def can_retry(self) -> bool:
        """子任务失败重试最大 2 次（第2章 2.1 规则4）。"""
        return self.retry_count < MAX_SUBTASK_RETRIES

    def bump_retry(self) -> None:
        self.retry_count += 1

    def can_reject(self) -> bool:
        """评估校验 Agent 打回最多 2 次（第4章 4.6 打回规则）。"""
        return self.review_rejects < MAX_REVIEW_REJECTS

    def bump_reject(self) -> None:
        self.review_rejects += 1

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at or time.time()
        return max(0.0, end - (self.started_at or self.created_at))

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "session_id": self.session_id,
            "parent_task_id": self.parent_task_id or "",
            "title": self.title,
            "agent_role": self.agent_role,
            "status": self.status,
            "iteration": self.iteration,
            "retry_count": self.retry_count,
            "review_rejects": self.review_rejects,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "deadline_at": self.deadline_at,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "result": self.result,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "limits": {
                "max_iterations": MAX_TASK_ITERATIONS,
                "max_timeout_seconds": MAX_TASK_TIMEOUT_SECONDS,
                "max_retries": MAX_SUBTASK_RETRIES,
                "max_review_rejects": MAX_REVIEW_REJECTS,
            },
            "history": self.history,
        }


class TaskLimitExceeded(Exception):
    """防死循环硬限制被触发（迭代超限 / 超时 / 循环依赖）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _intermediate_path(current: str, target: str, max_hops: int = 3) -> list[str]:
    """在合法流转图上做 BFS，返回从 current 到 target 的中间态路径（不含 current）。

    用途：允许「pending -> waiting_approval」这类"跨越态"请求按合法中间态推进，
    但绝不允许 success/failed 等终态被复活（终态出边为空，BFS 必然无解）。
    """
    if current == target:
        return []
    queue: list[list[str]] = [[current]]
    seen = {current}
    for _ in range(max_hops):
        nxt_queue: list[list[str]] = []
        for path in queue:
            node = path[-1]
            for step in STATUS_TRANSITIONS.get(node, ()):
                if step in seen:
                    continue
                new_path = path + [step]
                if step == target:
                    return new_path[1:]
                seen.add(step)
                nxt_queue.append(new_path)
        queue = nxt_queue
    return []


class LoopDetected(TaskLimitExceeded):
    """任务依赖图真实死循环（第2章 2.1 规则3）。

    【需求点 Bug1 规则4】允许携带 code / detail：
      · detail 为结构化的问题清单（逐条列出），供前端在终止消息中完整展示；
      · 只有在「重生成依赖图 1 次后仍存在真实死循环」时才允许抛出本异常终止任务。
    """

    def __init__(self, message: str = "检测到任务循环依赖，直接终止任务并报错",
                 *, code: str = ERR_LOOP, detail: list[str] | None = None):
        super().__init__(code, message)
        self.detail = list(detail or [])


def db_row_to_state(row: dict) -> TaskState:
    state = TaskState(
        task_id=row["task_id"],
        session_id=row["session_id"],
        title=row.get("title", ""),
        agent_role=row.get("agent_role", ""),
        parent_task_id=row.get("parent_task_id"),
        status=row.get("status", STATUS_PENDING),
        iteration=int(row.get("iteration") or 0),
        retry_count=int(row.get("retry_count") or 0),
        review_rejects=int(row.get("review_rejects") or 0),
        created_at=float(row.get("created_at") or time.time()),
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        deadline_at=float(row.get("deadline_at") or 0) or (float(row.get("created_at") or time.time()) + MAX_TASK_TIMEOUT_SECONDS),
        result=row.get("result") or "",
        error_code=row.get("error_code") or "",
        error_message=row.get("error_message") or "",
    )
    return state
