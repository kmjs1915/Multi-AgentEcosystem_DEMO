# -*- coding: utf-8 -*-
"""
系统运行时编排器（服务层）—— 把四层架构装配起来并驱动完整任务链路

架构文档来源：
  - 第4章 4.1 执行流程：用户输入→记忆检索→任务拆解→依赖校验→分发Agent→结果汇总→迭代判断→交付质检
  - 第3章 全章：统一消息结构体 / 状态机 / 审批规则
  - 第2章 2.1 防死循环 / 2.3 降级兜底 / 2.4 Token统计
  - 第5章 目录规范（会话目录隔离）

职责：
  1. 初始化基础设施层（目录、SQLite、向量库、日志、配置、备份）
  2. 装配消息总线 + Agent_Router + 七大 Agent
  3. 执行完整任务链路，落实状态机、防死循环、降级、审批暂停与恢复
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.agents import build_agents, agent_registry_info
from backend.agents.base import AgentContext, CAPABILITY_MATRIX
from backend.agents.dispatch_agent import DispatchAgent
from backend.agents.memory_agent import MemoryAgent
from backend.bus.message import ApprovalMetadata, Message, MessageValidationError
from backend.bus.router import AgentRouter, MessageBus
from backend.bus.state_machine import (
    IllegalTransition,
    LoopDetected,
    TaskLimitExceeded,
    TaskState,
    db_row_to_state,
)
from backend.infrastructure.backup import BackupManager
from backend.infrastructure.database import Database
from backend.infrastructure.file_guard import SessionFileGuard
from backend.infrastructure.logger import EcosystemLogger, init_logger
from backend.infrastructure.vector_store import VectorStore
from backend.services.approval_center import ApprovalCenter, ApprovalError
from backend.services.approval_snapshot import (
    TaskSnapshotService,
    outcome_to_approved,
)
# 【队长-队员架构 · 需求 2/3】队长仲裁中心（一致性校验 / 冲突裁决 / 唯一基准）
from backend.services.arbitration import (
    ARBITRATION_SYSTEM_PROMPT,
    ArbitrationCenter,
    ArbitrationVerdict,
    build_recheck_instruction,
    render_arbitration_report,
)
from backend.services.stream_broker import StreamBroker
# 【需求点 Bug1】后端硬编码高危动作执行器：磁盘修改/删除的唯一执行端
from backend.services.executor import (
    BackendExecutor,
    DeleteExecution,
    detect_execution_claims,
    infer_delete_operation,
    is_delete_operation,
    normalize_delete_candidates,
    render_receipt_report,
    sanitize_execution_claim_text,
)
from backend.services.config_store import ConfigStore
from backend.services.model_client import ModelClient, ModelUnavailable, _extract_json
from backend.utils.constants import (
    AGENT_BINDINGS,
    AGENT_CODE,
    AGENT_DELIVERY,
    AGENT_DISPATCH,
    AGENT_DOC,
    AGENT_EVALUATOR,
    # 【Bug1 修复·存量缺陷】AGENT_MEMORY 此前未在文件顶部导入，但
    #   _captain_replan_for_failure 的换人重规划 valid_roles 在使用它：
    #   一旦走"校验打回 3 轮耗尽 → 上交队长重新分配"路径就会 NameError
    #   → 整个任务 UNHANDLED_PIPELINE_ERROR。该路径是新流转回到队长的唯一入口，必须可用。
    AGENT_MEMORY,
    AGENT_ROLES,
    AGENT_TEMPERATURES,
    AGENT_VISION,
    APPROVAL_STATE_MANUAL,
    APPROVAL_STATE_PENDING,
    APPROVAL_STATE_REJECTED,
    APPROVAL_STATE_TIMEOUT,
    APPROVAL_OUTCOME_REJECTED,
    APPROVAL_RESUME_TIMEOUT_SECONDS,
    APPROVAL_TIMEOUT_SECONDS,
    APPROVAL_WATCHDOG_INTERVAL_SECONDS,
    CAPTAIN_EXHAUSTED_PROMPT,
    CAPTAIN_VERIFY_PROMPT,
    EVALUATOR_SUBTASK_PROMPT,
    DEFAULT_WORKSPACE_ID,
    DEPENDENCY_TERMINATED_TITLE,
    ERR_ITERATION_LIMIT,
    ERR_ECOSYSTEM_EXHAUSTED,
    ERR_IO,
    ERR_MODEL_CONVERGE_FAIL,
    ERR_MODEL_UNAVAILABLE,
    ERR_REAL_CYCLE,
    ERR_SUBTASK_FAILED,
    ERR_SUBTASK_RETRY_EXHAUSTED,
    ERR_TIMER_RECORD_FAILED,
    ERR_WORKSPACE_UNAVAILABLE,
    FAILURE_KIND_IO,
    FAILURE_KIND_LABEL,
    FAILURE_KIND_MODEL_CONVERGE,
    FAULT_REPORT_NO_SYNTHESIS_NOTE,
    HIGH_RISK_OP_DELETE_SET,
    HIGH_RISK_OP_FILE_BATCH_DELETE,
    HIGH_RISK_OP_FILE_SINGLE_DELETE,
    HIGH_RISK_OP_FOLDER_REMOVE,
    HIGH_RISK_OP_LABEL,
    HIGH_RISK_OP_SET,
    HIGH_RISK_OP_SHELL_RUN,
    HIGH_RISK_REQUIRES_APPROVAL,
    MAX_EVALUATOR_FIX_ROUNDS,
    MAX_PLAN_REGENERATE_RETRIES,
    MAX_REQUIREMENT_REALLOCATIONS,
    MAX_TASK_ITERATIONS,
    MAX_SUBTASK_RETRIES,
    META_IMAGE_RESOURCES,
    MAX_TASK_TIMEOUT_SECONDS,
    MSG_TYPE_APPROVAL_RESULT,
    MSG_TYPE_RESULT,
    RISK_LEVEL_HIGH,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_WAITING_APPROVAL,
    SUBTASK_RETRY_EXHAUSTED_MESSAGE,
    THINK_LEVEL_ERROR,
    THINK_LEVEL_INFO,
    THINK_LEVEL_WARN,
    classify_failure,
    TIMER_END_STATUSES,
    TIMER_IDLE_TEXT,
    TIMER_PREFIX,
    TIMER_STATUS_CANCELLED,
    TIMER_STATUS_FAILED,
    TIMER_STATUS_IDLE,
    TIMER_STATUS_PAUSED,
    TIMER_STATUS_RUNNING,
    TIMER_STATUS_SUCCESS,
    WORKSPACE_KIND_LOCAL,
    WORKSPACE_KIND_SYSTEM,
)
from backend.utils.paths import (
    EcosystemPaths,
    SecurityViolation,
    SessionPaths,
    WorkspaceUnavailable,
    assert_workspace_readable,
    is_within,
    new_uuid,
    probe_workspace_root,
)

# 【需求点 Bug2】前端预先指定的任务 ID 只允许安全字符集（防注入/伪造成已有任务）
_SAFE_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{8,64}$")


@dataclass
class SubtaskResult:
    index: int
    title: str
    agent_role: str
    status: str
    output: str = ""
    task_id: str = ""
    error: str = ""
    metadata: dict = field(default_factory=dict)
    verdict: dict | None = None
    approval_id: str = ""
    # ---- 【BUG-C】失败追踪字段（供链路阻断判定与故障诊断报告） ----
    failure_stage: str = ""            # dispatch / agent_error / retry_exhausted / dependency_blocked
    failure_reason: str = ""           # 可读失败原因
    # 【增量修复 3】失败分类：io_error（目录/权限）/ model_converge_fail（模型不收敛）
    failure_kind: str = ""
    retries_exhausted: bool = False    # 是否已达到重试上限
    retry_count: int = 0               # 实际重试次数
    blocked_by: list[int] = field(default_factory=list)   # 因哪些上游子任务失败被阻断
    think_steps: list[dict] = field(default_factory=list)  # 本子任务思考链（含 error 级别）
    # 【需求点 Bug2】在队长循环共享结果表里的唯一键：默认等于 index，
    #   但"队长换人重做"会生成一个新的重做子任务，此时必须换新键，
    #   否则会把原计划里的同下标子任务结果覆盖掉。
    result_key: int = -1
    # 【需求点 Bug1】后端原生执行回执（唯一可信的"是否真的删掉了"来源）。
    #   落成正式字段（而不是临时属性），快照恢复后依然能判断"是否已执行过"。
    backend_execution: Any = None

    def __post_init__(self) -> None:
        if int(self.result_key) < 0:
            self.result_key = int(self.index)

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class PipelineResult:
    session_id: str
    task_id: str
    status: str
    final_reply: str = ""
    think_steps: list[dict] = field(default_factory=list)
    subtasks: list[dict] = field(default_factory=list)
    pending_approval: dict | None = None
    stats: dict = field(default_factory=dict)
    degradation: dict = field(default_factory=dict)
    error_code: str = ""
    error_message: str = ""
    plan: dict = field(default_factory=dict)
    evaluation: dict | None = None
    # 【需求点 一、3】本次任务实际生效的文件操作根目录（= 当前选中工作区文件夹）。
    #   必须独立保存：`plan` 在调度阶段会被模型返回结果整体覆盖，
    #   把根目录塞进 plan 会连同 plan 一起被冲掉（已修复）。
    workspace_root: str = ""
    # 【需求点 三、2】本次任务周期的生态位补位事件
    ecosystem_fallbacks: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "task_id": self.task_id,
            "status": self.status,
            "final_reply": self.final_reply,
            "think_steps": self.think_steps,
            "subtasks": self.subtasks,
            "subtask_evaluation": self.plan.get("evaluation"),
            "pending_approval": self.pending_approval,
            "stats": self.stats,
            "degradation": self.degradation,
            # 【需求点 三、2】生态位补位事件：前端据此弹出非阻断轻提示
            "ecosystem_fallbacks": self.ecosystem_fallbacks,
            # 【需求点 一、3】文件操作根目录（前端凭此展示越权边界；plan 覆盖不影响本字段）
            "workspace_root": self.workspace_root,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "plan": self.plan,
            "evaluation": self.evaluation,
        }


@dataclass
class TaskTimingState:
    """【需求点 二、任务执行计时器】单个任务的实时计时状态（内存态，运行期权威）。

    计时口径（严格对齐任务状态机，第3章 3.4）：
      · 任务正式提交、Agent 开始执行 → start() 启动计时；
      · 任务进入 waiting_approval（人工审批暂停）→ pause() 停表，
        审批通过 resume_after_approval() → resume() 继续累计；
      · 任务变为 success / failed → finish() 停表并落库；
      · 用户取消 / 终止 → cancel() 停表并落库；
      · 新任务启动 → 复用同一会话键位重新 start()，计时自然清零。
    """

    timer_id: str
    session_id: str
    task_id: str = ""
    title: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    status: str = TIMER_STATUS_RUNNING
    task_status: str = ""
    paused_at: float | None = None
    paused_total: float = 0.0
    reason: str = ""
    workspace_root: str = ""
    last_tick: float = field(default_factory=time.time)

    @property
    def is_active(self) -> bool:
        """是否正在计时（运行中 = 计时器不停表）。"""
        return self.status == TIMER_STATUS_RUNNING and self.finished_at is None

    @property
    def elapsed_seconds(self) -> float:
        """已耗时（秒，浮点）。暂停期间不计入。"""
        if self.finished_at is not None:
            end = self.finished_at - self.paused_total
        elif self.status == TIMER_STATUS_PAUSED and self.paused_at is not None:
            end = self.paused_at - self.paused_total
        else:
            end = time.time() - self.paused_total
        return max(0.0, end - self.started_at)

    # ---- 启停控制（只由运行时调用，前端无法直接改写） ----
    def pause(self) -> None:
        if self.is_active:
            self.status = TIMER_STATUS_PAUSED
            self.paused_at = time.time()

    def resume(self) -> None:
        if self.status == TIMER_STATUS_PAUSED and self.finished_at is None:
            self.paused_total += time.time() - (self.paused_at or time.time())
            self.paused_at = None
            self.status = TIMER_STATUS_RUNNING

    def finish(self, *, status: str, task_status: str, reason: str = "") -> None:
        if self.finished_at is None:
            if self.status == TIMER_STATUS_PAUSED and self.paused_at is not None:
                self.paused_total += time.time() - self.paused_at
                self.paused_at = None
            self.finished_at = time.time()
        self.status = status
        self.task_status = task_status
        if reason:
            self.reason = reason

    def to_dict(self) -> dict:
        """对外结构（前端计时器组件直接消费，全部为真实后端数据）。"""
        elapsed = self.elapsed_seconds
        return {
            "timer_id": self.timer_id,
            "session_id": self.session_id,
            "task_id": self.task_id,
            "title": self.title,
            "status": self.status,
            "task_status": self.task_status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "paused_total_seconds": round(self.paused_total, 3),
            "reason": self.reason,
            "workspace_root": self.workspace_root,
            "running": self.is_active,
            "elapsed_seconds": round(elapsed, 3),
            "elapsed_display": format_elapsed(elapsed),
            "display_text": TIMER_PREFIX + format_elapsed(elapsed),
            "updated_at": time.time(),
        }


def format_elapsed(seconds: float | int | None) -> str:
    """【需求点 二、2】计时格式：时:分:秒（HH:MM:SS，小时不封顶为两位）。"""
    total = int(max(0.0, float(seconds or 0.0)))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


# ==========================================================================
# 【队长-队员架构 · 规则4】需要强制沿用"队长唯一基准清单"的下游子任务特征词：
#   清单 / 筛选 / 删除 / 清理类子任务会消费候选清单，必须注入队长裁决结果；
#   其他子任务（纯文档解析、纯润色等）不受影响，避免无意义地撑大 prompt。
# ==========================================================================
_CANONICAL_CONSUME_HINTS: tuple[str, ...] = (
    "清单", "候选", "筛选", "待删", "删除", "清理", "移除", "清空", "文件作用",
)

# ==========================================================================
# 【需求点 Bug2 业务流程重构】队长（调度规划Agent）业务循环的运行时状态
#   ------------------------------------------------------------------
#   业务伪代码里的「while (整体任务未完成)」在代码层**禁止**写成阻塞式 while 循环，
#   而是用「任务状态机 + SQLite 任务快照 + 审批回调恢复」模拟同样的业务语义：
#     · 每个轮次只做一件事：队长取下一个子任务 → 分派给队员 → 队员执行 → 回传队长校验；
#     · 每轮结束都覆盖写一次完整快照（本轮迭代数 / 已完成子任务 / 剩余计划 /
#       消息上下文 / 各子任务重试计数 / 待审批详情）；
#     · 命中高危操作 → 立刻中断并落快照 → 状态置 waiting_approval → 等审批回调；
#     · 审批回调按 task_id 读回快照 → 注入审批结果 → 继续跑下一轮。
#   本结构就是"循环变量"，它整体序列化进 task_snapshots.loop_state。
# ==========================================================================
@dataclass
class CaptainRun:
    """队长业务循环的上下文（可整体序列化 → SQLite 快照）。"""

    run_id: str
    user_input: str
    image_paths: list[str] = field(default_factory=list)
    plan: dict = field(default_factory=dict)
    work: list[dict] = field(default_factory=list)          # 待执行（按依赖序）
    completed: list[dict] = field(default_factory=list)     # 已完成（成功/失败终态）
    results: dict[int, "SubtaskResult"] = field(default_factory=dict)
    messages: list[dict] = field(default_factory=list)      # 全部消息上下文（含队员回执）
    retries: dict[str, int] = field(default_factory=dict)   # 子任务下标 → 已重试次数
    replan_used: int = 0                                    # 队长换人重规划次数
    degraded_notes: list[str] = field(default_factory=list)
    workspace_root: str = ""
    v: int = 2
    # 【需求点 Bug1】已处理过的审批单号（防止同一审批单被反复处理造成死循环）
    handled_approvals: list[str] = field(default_factory=list)
    # ==================================================================
    # 【第三轮·业务流程重构】新增流程的两道计数器与两类状态（必须随快照持久化）
    #   fix_rounds    ：结果键 → 校验评估Agent 打回原队员修改的轮次（每子任务最多 3 轮）
    #   reallocations ：队长重新分配任务次数（校验打回 3 轮耗尽 / 重试耗尽后换人，最多 3 次）
    #   requirement_state / evaluator_state：最近一次结论（供前端展示与断点回溯）
    # ==================================================================
    fix_rounds: dict[str, int] = field(default_factory=dict)
    reallocations: int = 0
    requirement_state: dict = field(default_factory=dict)
    evaluator_state: dict = field(default_factory=dict)
    code_edits: list[dict] = field(default_factory=list)     # 代码修改痕迹（前后各留摘要）

    # ---------------- 序列化（快照落库 / 恢复） ----------------
    def to_state(self, *, iteration: int = 0, stage: str = "", approval: dict | None = None,
                 updated_at: float | None = None) -> dict:
        return {
            "v": self.v,
            "run_id": self.run_id,
            "user_input": self.user_input,
            "image_paths": list(self.image_paths),
            "plan": _jsonable(self.plan),
            "work": _jsonable(self.work),
            "completed": _jsonable(self.completed),
            "messages": _jsonable(self.messages[-80:]),
            "retries": {str(k): int(v) for k, v in self.retries.items()},
            "replan_used": int(self.replan_used),
            "degraded_notes": list(self.degraded_notes),
            "workspace_root": self.workspace_root,
            "handled_approvals": list(self.handled_approvals),
            # 【第三轮】新流转链路的两道计数器（快照恢复后不得清零）
            "fix_rounds": {str(k): int(v) for k, v in self.fix_rounds.items()},
            "reallocations": int(self.reallocations),
            "requirement_state": _jsonable(self.requirement_state),
            "evaluator_state": _jsonable(self.evaluator_state),
            "code_edits": _jsonable(self.code_edits[-40:]),
            "iteration": int(iteration),
            "stage": stage,
            "results": [_jsonable(r.to_dict()) for r in self.results.values()],
            "approval": _jsonable(approval or {}),
            "updated_at": updated_at or time.time(),
        }

    @classmethod
    def from_state(cls, state: dict) -> "CaptainRun":
        run = cls(
            run_id=str(state.get("run_id") or uuid.uuid4().hex),
            user_input=str(state.get("user_input") or ""),
            image_paths=[str(p) for p in (state.get("image_paths") or [])],
            plan=dict(state.get("plan") or {}),
            work=[dict(w) for w in (state.get("work") or [])],
            completed=[dict(c) for c in (state.get("completed") or [])],
            messages=[dict(m) for m in (state.get("messages") or [])],
            retries={str(k): int(v) for k, v in (state.get("retries") or {}).items()},
            replan_used=int(state.get("replan_used") or 0),
            degraded_notes=[str(n) for n in (state.get("degraded_notes") or [])],
            workspace_root=str(state.get("workspace_root") or ""),
            # 【第三轮】两道计数器 + 两类状态完整还原（缺字段回落默认，兼容历史快照）
            fix_rounds={str(k): int(v) for k, v in (state.get("fix_rounds") or {}).items()},
            reallocations=int(state.get("reallocations") or 0),
            requirement_state=dict(state.get("requirement_state") or {}),
            evaluator_state=dict(state.get("evaluator_state") or {}),
            code_edits=[dict(c) for c in (state.get("code_edits") or [])],
        )
        run.handled_approvals = [str(a) for a in (state.get("handled_approvals") or [])]
        for raw in (state.get("results") or []):
            try:
                res = SubtaskResult(
                    index=int(raw.get("index", 0)), title=str(raw.get("title") or ""),
                    agent_role=str(raw.get("agent_role") or ""), status=str(raw.get("status") or ""),
                    output=str(raw.get("output") or ""), task_id=str(raw.get("task_id") or ""),
                    error=str(raw.get("error") or ""), metadata=dict(raw.get("metadata") or {}),
                    approval_id=str(raw.get("approval_id") or ""),
                    failure_stage=str(raw.get("failure_stage") or ""),
                    failure_reason=str(raw.get("failure_reason") or ""),
                    failure_kind=str(raw.get("failure_kind") or ""),
                    retries_exhausted=bool(raw.get("retries_exhausted")),
                    retry_count=int(raw.get("retry_count") or 0),
                    blocked_by=[int(x) for x in (raw.get("blocked_by") or [])],
                    result_key=int(raw.get("result_key", raw.get("index", 0)) or 0),
                )
            except Exception:  # noqa: BLE001 单条结果损坏不影响整体恢复
                continue
            # 【修复·循环状态污染】还原"后端原生执行回执"对象：
            #   快照 JSON 里 backend_execution 是 dict，若不还原为对象，
            #   `getattr(res, "backend_execution")` 的幂等守卫会失效 → 高危动作可能被重复执行。
            raw_exec = (res.metadata or {}).get("backend_execution")
            if raw_exec is None:
                raw_exec = raw.get("backend_execution")
            restored = DeleteExecution.from_dict(raw_exec)
            if restored is not None:
                res.backend_execution = restored
                res._backend_executed = True       # type: ignore[attr-defined]
            run.results[res.result_key] = res
        return run

    # ---------------- 便捷判定 ----------------
    def work_index(self, index: int) -> dict | None:
        return next((w for w in self.work if int(w.get("index", -1)) == int(index)), None)

    def completed_results(self) -> list["SubtaskResult"]:
        return [self.results[k] for k in sorted(self.results.keys())]

    def completed_dicts(self) -> list[dict]:
        """【需求点 Bug2】"已完成子任务列表"的对外结构（直接来自快照登记）。

        直接用 run.completed 生成，保证：
          · 队长决策①终止后未下发的下游子任务也会出现在故障诊断里（不会被漏掉）；
          · 与 SQLite 快照里记录的 completed 完全一致（同一份数据源）。

        字段名与 SubtaskResult 对齐，兼容历史前端/测试的读取口径
        （index / title / agent_role / status / output / error / task_id /
          failure_stage / failure_reason / retry_count / retries_exhausted / blocked_by）。
        """
        out: list[dict] = []
        for entry in self.completed:
            out.append({
                "index": int(entry.get("index", -1)),
                "title": str(entry.get("title") or ""),
                "agent_role": str(entry.get("agent_role") or ""),
                "status": str(entry.get("status") or ""),
                "output": str(entry.get("output") or ""),
                "error": str(entry.get("error") or ""),
                "task_id": str(entry.get("task_id") or ""),
                "metadata": dict(entry.get("metadata") or {}),
                "verdict": entry.get("verdict"),
                "approval_id": str(entry.get("approval_id") or ""),
                "failure_stage": str(entry.get("failure_stage") or ""),
                "failure_reason": str(entry.get("failure_reason") or ""),
                "failure_kind": str(entry.get("failure_kind") or ""),
                "retries_exhausted": bool(entry.get("retries_exhausted")),
                "retry_count": int(entry.get("retry_count") or 0),
                "blocked_by": [int(x) for x in (entry.get("blocked_by") or [])],
                "think_steps": list(entry.get("think_steps") or []),
                "captain_decision": str(entry.get("captain_decision") or ""),
                "result_key": int(entry.get("index", -1)),
            })
        return out

    def sync_with_work(self) -> None:
        """【需求点 Bug2】把待执行队列里已有结果对象的登记键同步为队列中的 result_key。

        用于"审批快照恢复"等场景：子任务结果已存在于结果表中，
        但待执行项可能带着新的结果键（换人重做），需要先对齐再继续循环。
        """
        for item in self.work:
            key = int(item.get("result_key", item.get("index", 0)))
            if key in self.results:
                continue
            task_id = str(item.get("task_id") or "")
            match = next((r for r in self.results.values()
                          if task_id and r.task_id == task_id), None)
            if match is not None:
                self.results.pop(match.result_key, None)
                match.result_key = key
                self.results[key] = match

    @staticmethod
    def assign_result_keys(items: list[dict]) -> list[dict]:
        """为待执行队列分配**唯一**的结果键（result_key）。

        规则：同一个"计划下标"始终对应同一个键（保证重试计数与结果对象稳定）；
        不同下标必定拿到不同键（避免两个子任务互相覆盖结果、互相争抢重试计数）。
        """
        key_by_index: dict[int, int] = {}
        next_key = 0
        out: list[dict] = []
        for raw in items or []:
            item = dict(raw)
            idx = int(item.get("index", 0))
            if idx not in key_by_index:
                key_by_index[idx] = next_key
                next_key += 1
            item["result_key"] = key_by_index[idx]
            out.append(item)
        return out


def _jsonable(value: Any) -> Any:
    """把任意值收敛为可 JSON 序列化的结构（快照落库用，绝不因脏数据抛异常）。"""
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except (TypeError, ValueError):
        if isinstance(value, dict):
            return {str(k): _jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_jsonable(v) for v in value]
        return str(value)


class EcosystemRuntime:
    """系统运行时（单例装配器）。"""

    def __init__(self, root=None):
        # ---- 基础设施层（第1章 1.4 第1层） ----
        self.paths: EcosystemPaths = EcosystemPaths.build(root).ensure()
        self.db = Database(self.paths.sqlite_db_file)
        self.logger: EcosystemLogger = init_logger(self.paths.logs, self.db)
        self.vector_store = VectorStore(self.paths.vector_db)
        self.config = ConfigStore(self.paths)
        self.backup = BackupManager(self.paths, self.db, self.vector_store, self.config)

        # ---- 服务层组件 ----
        self.model_client = ModelClient(self.config)
        self.approval_center = ApprovalCenter(self.db, self.logger)
        # ==================================================================
        # 【新增】任务快照服务层（高危审批中断 / 断点恢复的唯一持久化入口）
        #   · 保存快照 / 读取快照 / 更新任务状态 / 30 秒审批超时检测；
        #   · 只落 SQLite（无 Redis / Celery），不改动既有队长-队员业务逻辑。
        # ==================================================================
        self.snapshots = TaskSnapshotService(self.db, self.logger)
        # 超时看门狗任务句柄（懒启动：第一次进入 waiting_approval 时创建）
        self._approval_watchdog_task: asyncio.Task | None = None
        # 【新增】非阻塞审批恢复：正在后台恢复的任务集合 + 任务句柄
        #   （审批提交接口据此立刻返回、SSE 推送进度，消除"审批后卡在提交中"）
        self._resume_inflight: set[str] = set()
        self._resume_tasks: dict[str, asyncio.Task] = {}
        # ==================================================================
        # 【队长-队员架构 · 需求 2/3】队长仲裁中心
        #   · 队员（代码工程Agent 等）输出统一上报 → 队长做一致性校验；
        #   · 出现冲突（如两份待删清单不一致）→ 自动触发队长仲裁分支；
        #   · 队长裁决结果 = 任务链路唯一基准，下游子任务强制沿用。
        #   本组件只**新增**仲裁分支，不改变原有串行任务链路与状态机。
        # ==================================================================
        self.arbitration = ArbitrationCenter(self.db, self.logger)
        # 【队长-队员架构】最近一次队长仲裁结果（供 _finalize 随任务一并回传；任务开始时重置）
        self._last_arbitration: dict | None = None

        # ---- 消息总线 + Agent_Router ----
        self.bus = MessageBus(self.db, self.logger)
        self.router = AgentRouter(self.bus, self.db, self.logger)
        # 【需求点 Bug2】协同思维链流式事件总线（仅用于前端展示，不参与业务决策）
        self.stream = StreamBroker()

        # ---- 【需求点 二、任务执行计时器】运行期计时状态（单进程内存态） ----
        #   key = session_id；进程重启后自然清空（历史耗时以 session_task_timers 表为准，
        #   因此不会出现"服务重启后计时器仍在跑"的假状态）。
        self.timers: dict[str, TaskTimingState] = {}

        # ---- Agent能力层：七大 Agent ----
        self.degraded_notes: list[str] = []
        # 【需求点 三、2】本次任务周期内发生的生态位补位事件（前端轻提示 + 状态栏 + 日志）
        self.ecosystem_fallbacks: list[dict] = []
        self.last_task_id: str | None = None
        self.ctx: AgentContext | None = None
        self.agents = self._build_agent_layer()

        # 第4章 4.5：记忆管理Agent 以异步订阅者身份接入总线（不轮询）
        self.bus.start_memory_consumer(self.agents[self.ctx_memory_role()])

        self.logger.info(
            "多Agent生态工作台运行时初始化完成",
            agent_role="system",
            detail=f"root={self.paths.root} 向量库={'可用' if self.vector_store.available else '降级关闭'}",
        )

    # ==================================================================
    # 装配
    # ==================================================================
    @staticmethod
    def ctx_memory_role() -> str:
        from backend.utils.constants import AGENT_MEMORY
        return AGENT_MEMORY

    def _build_agent_layer(self) -> dict:
        ctx = AgentContext(
            session_paths=None, file_guard=None, model_client=self.model_client,
            approval_center=self.approval_center, db=self.db, logger=self.logger,
            memory_agent=None, vector_store=self.vector_store, runtime=self,
        )
        self.ctx = ctx
        agents = build_agents(ctx)
        ctx.memory_agent = agents[self.ctx_memory_role()]

        # 所有 Agent 通过统一总线通信（禁止直连）
        for role, agent in agents.items():
            self.bus.subscribe(role, self._make_handler(agent))
        return agents

    def _make_handler(self, agent):
        """把 Agent.handle(msg) 适配成总线需要的异步处理器，并注入 TaskState。"""
        async def handler(msg: Message) -> Message | None:
            state = self.load_task_state(msg.task_id) or TaskState(
                task_id=msg.task_id, session_id=msg.session_id,
                title="", agent_role=msg.receiver_agent,
            )
            return await agent.handle(msg, state)
        return handler

    # ------------------------------------------------------------------
    # 【需求点 三、2】生态位补位事件登记（仅本次任务周期）
    # ------------------------------------------------------------------
    def register_ecosystem_fallback(self, *, agent_role: str, original: str,
                                    actual: str, note: str, reason: str) -> None:
        entry = {
            "agent_role": agent_role,
            "original_model": original,
            "actual_model": actual,
            "note": note or f"当前缺失{original}模型，生态位补位，实际使用：{actual}",
            "reason": reason,
            "at": time.time(),
        }
        if not any(e["agent_role"] == agent_role and e["actual_model"] == actual
                   for e in self.ecosystem_fallbacks):
            self.ecosystem_fallbacks.append(entry)
        self.logger.task_log(
            session_id="", task_id="", agent_role=agent_role,
            event="ecosystem.fallback",
            detail=(f"Agent={agent_role} | 原指定模型={original} | 补位模型={actual} | "
                    f"原始失败原因={reason}"),
            level="warn",
        )

    def reset_ecosystem_fallbacks(self) -> None:
        self.ecosystem_fallbacks = []

    # ==================================================================
    # 【需求点 二、任务执行计时器】启动 / 暂停 / 恢复 / 结束 / 取消 + 持久化
    #   约束：只新增计时接口与前端组件，消息总线、状态机、高危审批逻辑零改动。
    #   所有异常、任务启动/结束、计时记录写入系统日志（第9章 9.2）。
    # ==================================================================
    def _persist_timer(self, state: TaskTimingState, *, reason: str = "") -> None:
        """把计时记录写入数据库（会话任务记录）+ 合并进会话元数据（meta）。

        兼容旧会话：sessions.meta 缺省即为 {}，本方法只做键合并，
        不要求任何历史字段存在，也不会破坏旧数据。
        """
        record = state.to_dict()
        record["reason"] = reason or state.reason
        try:
            self.db.insert_task_timer({
                "timer_id": state.timer_id,
                "session_id": state.session_id,
                "task_id": state.task_id,
                "title": state.title,
                "status": state.status,
                "started_at": state.started_at,
                "finished_at": state.finished_at,
                "elapsed_seconds": round(state.elapsed_seconds, 3),
                "task_status": state.task_status,
                "reason": reason or state.reason,
                "record": record,
                "created_at": state.started_at,
            })
            # 【需求点 二、3】写入会话元数据 → 历史会话打开即可看到上一次任务耗时
            self.db.set_session_meta(state.session_id, {
                "last_task_timer": {
                    "timer_id": state.timer_id,
                    "task_id": state.task_id,
                    "title": state.title,
                    "status": state.status,
                    "task_status": state.task_status,
                    "elapsed_seconds": round(state.elapsed_seconds, 3),
                    "elapsed_display": format_elapsed(state.elapsed_seconds),
                    "started_at": state.started_at,
                    "finished_at": state.finished_at,
                    "reason": reason or state.reason,
                },
            })
        except Exception as exc:  # noqa: BLE001 计时落库失败绝不影响主链路（第2章 2.3）
            self.logger.exception_log(
                error_code=ERR_TIMER_RECORD_FAILED,
                message=f"任务计时记录写入失败（已降级，不影响任务结果）：{exc}",
                session_id=state.session_id, task_id=state.task_id or None,
                agent_role=AGENT_DISPATCH,
            )

    def start_task_timer(self, *, session_id: str, task_id: str = "", title: str = "",
                         workspace_root: str = "") -> dict:
        """【需求点 二、2-a】任务正式开始提交、Agent 开始执行 → 启动计时器（新任务清零）。

        语义约定（保证"结算口径"可预期）：
          · 若该会话已有**同一任务且在跑**的计时器 → 只补充元信息（title/workspace/task_id），
            不重置起点（入口与链路内部都会调用本方法，必须幂等）；
          · 若已有**另一个任务**在跑 → 视为上一轮异常残留：收敛为 cancelled 后重新计时；
          · 否则 → 生成新计时器（新任务计时归零）。
        """
        previous = self.timers.get(session_id)
        if previous is not None and previous.finished_at is None:
            same_task = bool(task_id) and previous.task_id == task_id
            # task_id 尚未绑定（入口先启动、根任务号稍后生成）时也视为同一轮，直接复用
            if same_task or not previous.task_id:
                previous.title = title or previous.title
                previous.workspace_root = workspace_root or previous.workspace_root
                previous.task_id = task_id or previous.task_id
                self._persist_timer(previous, reason="计时元信息更新")
                return previous.to_dict()
            previous.finish(status=TIMER_STATUS_CANCELLED, task_status=STATUS_FAILED,
                            reason="被新任务取代，计时终止")
            self._persist_timer(previous, reason=previous.reason)
            self.logger.task_log(
                session_id=session_id, task_id=previous.task_id, agent_role=AGENT_DISPATCH,
                event="timer.cancelled", level="warn",
                detail=f"上一轮计时被新任务取代并停止：已耗时 {format_elapsed(previous.elapsed_seconds)}",
            )

        state = TaskTimingState(
            timer_id="tmr_" + uuid.uuid4().hex,
            session_id=session_id,
            task_id=task_id,
            title=title,
            workspace_root=workspace_root,
        )
        self.timers[session_id] = state
        self._persist_timer(state, reason="计时启动")
        self.logger.task_log(
            session_id=session_id, task_id=task_id, agent_role=AGENT_DISPATCH,
            event="timer.started",
            detail=f"任务计时已启动（计时器已清零）：timer_id={state.timer_id} title={title or '未命名任务'}",
        )
        self.logger.info(
            f"任务计时启动：{state.timer_id}（session={session_id}）",
            session_id=session_id, task_id=task_id, agent_role="system",
        )
        return state.to_dict()

    def attach_task_to_timer(self, session_id: str, task_id: str) -> None:
        """根任务创建后把 task_id 绑定到计时器上（计时在任务创建前已随请求启动）。"""
        state = self.timers.get(session_id)
        if state is None or state.finished_at is not None:
            return
        state.task_id = task_id or state.task_id
        self._persist_timer(state, reason="任务号绑定")

    def finish_task_timer(self, session_id: str, *, task_status: str,
                          reason: str = "") -> dict | None:
        """【需求点 二、2-b/c】任务结束（success/failed）或取消/终止 → 停止计时并落库。

        幂等：已结束的计时器再次调用直接返回现有降级结果，不重复累加。
        """
        state = self.timers.get(session_id)
        if state is None:
            return None
        if state.finished_at is not None:
            return state.to_dict()

        if task_status == STATUS_SUCCESS:
            timer_status = TIMER_STATUS_SUCCESS
        elif task_status == STATUS_WAITING_APPROVAL:
            # 审批暂停属于"任务未结束"：只停表，不清空计时
            state.pause()
            self._persist_timer(state, reason=reason or "任务进入人工审批，计时暂停")
            self.logger.task_log(
                session_id=session_id, task_id=state.task_id, agent_role=AGENT_DISPATCH,
                event="timer.paused",
                detail=f"任务暂停等待审批，计时暂停：已耗时 {format_elapsed(state.elapsed_seconds)}",
            )
            return state.to_dict()
        else:
            timer_status = TIMER_STATUS_FAILED

        state.task_status = task_status
        state.finish(status=timer_status, task_status=task_status,
                     reason=reason or ("任务成功交付" if task_status == STATUS_SUCCESS else "任务失败结束"))
        self._persist_timer(state)
        self.logger.task_log(
            session_id=session_id, task_id=state.task_id, agent_role=AGENT_DISPATCH,
            event="timer.finished",
            detail=(f"任务计时已停止：状态={state.status} 任务状态={task_status} "
                    f"耗时={format_elapsed(state.elapsed_seconds)}（{state.elapsed_seconds:.3f}s）"
                    f"{(' 原因=' + state.reason) if state.reason else ''}"),
        )
        self.logger.info(
            f"任务计时结束：{state.timer_id} 耗时 {format_elapsed(state.elapsed_seconds)}",
            session_id=session_id, task_id=state.task_id, agent_role="system",
        )
        return state.to_dict()

    def cancel_task_timer(self, session_id: str, *, task_id: str = "",
                          reason: str = "用户取消 / 终止任务") -> dict | None:
        """【需求点 二、2-c】任务中途取消、终止 → 同样停止计时（并落库）。"""
        state = self.timers.get(session_id)
        if state is None:
            # 内存态缺失（例如服务重启后前端补发取消）→ 尝试按 task_id 从库中收敛
            row = self.db.get_task_timer_by_task(task_id) if task_id else None
            if row is None:
                return None
            return row
        if task_id and not state.task_id:
            state.task_id = task_id
        if state.finished_at is None:
            state.finish(status=TIMER_STATUS_CANCELLED, task_status=STATUS_FAILED, reason=reason)
            self._persist_timer(state, reason=reason)
            self.logger.task_log(
                session_id=session_id, task_id=state.task_id, agent_role=AGENT_DISPATCH,
                event="timer.cancelled", level="warn",
                detail=(f"任务取消/终止，计时已停止：耗时 {format_elapsed(state.elapsed_seconds)}"
                        f"（{state.elapsed_seconds:.3f}s）原因={reason}"),
            )
        return state.to_dict()

    def resume_task_timer(self, session_id: str, *, reason: str = "审批通过，继续计时") -> dict | None:
        """审批通过恢复执行 → 恢复计时（暂停期间不计入耗时）。"""
        state = self.timers.get(session_id)
        if state is None or state.finished_at is not None:
            return state.to_dict() if state else None
        was_paused = state.status == TIMER_STATUS_PAUSED
        state.resume()
        if was_paused:
            self._persist_timer(state, reason=reason)
            self.logger.task_log(
                session_id=session_id, task_id=state.task_id, agent_role=AGENT_DISPATCH,
                event="timer.resumed",
                detail=f"审批通过，计时恢复：累计已耗时 {format_elapsed(state.elapsed_seconds)}",
            )
        return state.to_dict()

    def task_timer_view(self, session_id: str) -> dict:
        """计时器当前视图（供前端左上角计时组件每秒轮询）。

        无正在运行的任务时：
          · 若该会话历史上有已结束的计时记录 → 展示"上一次任务耗时"；
          · 否则 → 展示默认文字「未开始任务」（需求点 二、2 原文）。
        """
        live = self.timers.get(session_id)
        if live is not None and live.finished_at is None:
            payload = live.to_dict()
            payload["source"] = "live"
            payload["label"] = "本次任务"
            return payload

        row = self.db.latest_task_timer(session_id)
        if row is None:
            return {
                "timer_id": "", "session_id": session_id, "task_id": "", "title": "",
                "status": TIMER_STATUS_IDLE, "task_status": "",
                "started_at": None, "finished_at": None,
                "paused_total_seconds": 0.0, "reason": "", "workspace_root": "",
                "running": False,
                "elapsed_seconds": 0.0,
                "elapsed_display": format_elapsed(0),
                # 【需求点 二、2】没有正在运行的任务 → 默认文案「未开始任务」
                #   （不改写后端文案常量，由前端在 status=idle 时原样展示）
                "display_text": TIMER_IDLE_TEXT,
                "source": "idle",
                "label": "未开始任务",
                "updated_at": time.time(),
            }

        running = bool(live is not None and live.is_active)
        elapsed = float(row.get("elapsed_seconds") or 0.0)
        return {
            **row,
            "running": running,
            "elapsed_display": format_elapsed(elapsed),
            "display_text": TIMER_PREFIX + format_elapsed(elapsed),
            "source": "history",
            "label": "上一次任务耗时",
            "updated_at": time.time(),
        }

    def session_timer_history(self, session_id: str, limit: int = 50) -> dict:
        """历史会话计时记录 + 汇总（第9.2 永久记录：历史会话可查看上一次任务耗时）。"""
        return {
            "session_id": session_id,
            "records": self.db.list_task_timers(session_id, limit=limit),
            "latest": self.task_timer_view(session_id),
            "summary": self.db.timer_summary(session_id),
        }

    def active_model_label(self, agent_role: str) -> str:
        """当前 Agent 实际运行的模型名称（优先展示补位结果）。

        【需求点 Bug7】模型绑定以 constants.AGENT_BINDINGS 为唯一来源，
        这里统一输出「模型名称 + 平台标识」便于前端状态栏与思维链展示。
        """
        if not agent_role or agent_role not in AGENT_BINDINGS:
            return ""
        for e in self.ecosystem_fallbacks:
            if e["agent_role"] == agent_role:
                return e["actual_model"]
        binding = AGENT_BINDINGS[agent_role]
        # 【需求点 Bug7】同一 provider 下多个 Agent 可能用不同模型
        # （Qwen：调度=qwen3.8-max / 视觉=qwen3.8-flash；GLM：评估=glm-5.3 / 记忆=glm-5.3-flash），
        # 因此标签必须按 Agent 绑定表求解，不能直接取 provider 级模型。
        try:
            label = self.model_client.effective_label(agent_role)
        except Exception:  # noqa: BLE001
            label = ""
        return label or self.config.provider_model_label(binding["provider"]) or binding["model_name"]

    def model_binding_snapshot(self) -> list[dict]:
        """七大 Agent ↔ 模型绑定快照（前端思维链表头 / 状态栏展示）。"""
        out: list[dict] = []
        for role in AGENT_ROLES:
            b = AGENT_BINDINGS[role]
            out.append({
                "agent": role,
                "model_name": b["model_name"],
                "provider": b["provider"],
                "model": b["model"],
                "model_label": self.active_model_label(role),
                "duty": b["duty"],
            })
        return out

    def model_runtime_overview(self) -> list[dict]:
        """七大 Agent 的实跑模型视图（前端状态栏 / 系统信息）。"""
        out = []
        for role in AGENT_ROLES:
            binding = AGENT_BINDINGS[role]
            fallback = next((e for e in self.ecosystem_fallbacks if e["agent_role"] == role), None)
            # 「指定模型」= 该 Agent 绑定表里的专属模型（不含生态位补位结果）；
            # 「实际模型」= 有补位记录时展示补位模型，否则同指定模型。
            try:
                assigned_label = self.model_client.effective_label(role)
            except Exception:  # noqa: BLE001
                assigned_label = (self.config.provider_model_label(binding["provider"])
                                  or binding["model_name"])
            out.append({
                "agent": role,
                # 【需求点 Bug8】左下角状态栏逐条展示所需的完整绑定信息
                "model_name": binding["model_name"],
                "provider": binding["provider"],
                "bound_model": binding["model"],
                "duty": binding["duty"],
                "assigned_model_name": binding["model_name"],
                "assigned_model_label": assigned_label,
                "actual_model_label": fallback["actual_model"] if fallback else assigned_label,
                "ecosystem_fallback": bool(fallback),
                "fallback_note": fallback["note"] if fallback else "",
                "fallback_reason": fallback["reason"] if fallback else "",
                "available": self.model_client.is_available(role)[0],
            })
        return out

    # ------------------------------------------------------------------
    # 【需求点 二、工作区分组管理 + 工作区安全权限硬约束】
    # ------------------------------------------------------------------
    def _workspace_extra(self, workspace_id: str) -> dict:
        """读取工作区的扩展元数据（含所选本地文件夹路径）。"""
        row = self.db.get_workspace(workspace_id) or {}
        try:
            return json.loads(row.get("meta") or "{}")
        except (json.JSONDecodeError, TypeError):
            return {}

    def set_workspace_folder(self, workspace_id: str, folder_path: str) -> dict:
        """把一个本地磁盘目录绑定为某工作区的操作根目录（持久化 + 写标记文件）。

        【需求点 二、2 规则4】绑定后立即把该会话的文件操作根目录切换到新文件夹。
        """
        folder = self.config.register_workspace_folder(folder_path)
        self.db.set_workspace_meta(workspace_id, {
            "kind": WORKSPACE_KIND_LOCAL,
            "folder_id": folder["folder_id"],
            "folder_path": folder["path"],
            "folder_name": folder["name"],
        })
        # 工作区名称与文件夹名保持一致，便于左树辨识
        self.db.rename_workspace(workspace_id, folder["name"])
        self.logger.info(
            f"工作区根目录已绑定：工作区 {workspace_id} -> {folder['path']}，"
            "后续所有 Agent 文件操作限定在该目录内",
            agent_role="system",
        )
        return folder

    def workspace_root_for(self, workspace_id: str) -> Path | None:
        """返回工作区对应的本地操作根目录；系统隔离工作区返回 None。"""
        extra = self._workspace_extra(workspace_id)
        if extra.get("kind") != WORKSPACE_KIND_LOCAL:
            return None
        raw = extra.get("folder_path") or ""
        if not raw:
            return None
        path = Path(raw)
        if path.is_dir():
            return path
        # 目录已不可用：记录并降级为系统隔离工作区（不崩溃）
        self.logger.warning(
            f"工作区目录不可用，已临时降级为系统隔离目录：{raw}", agent_role="system",
        )
        return None

    # ------------------------------------------------------------------
    # 【BUG-B 1/2/3】工作区目录前置校验（真实物理目录 + 存在性 + 读权限）
    # ------------------------------------------------------------------
    def workspace_precheck(self, session_id: str) -> dict:
        """执行文件类任务前的工作区前置校验（不抛异常，供接口与链路复用）。

        · 工作区未绑定本地文件夹 → 使用系统隔离目录（始终可用）；
        · 已绑定本地文件夹 → 校验真实绝对路径是否存在、是否拥有读权限；
        · 校验失败 → ok=False + 统一可读错误文案
          （错误：当前工作绑定目录【xxx】不存在 / 无读取权限）。
        """
        workspace_id = self.session_workspace(session_id) if self.db.get_session(session_id) else ""
        extra = self._workspace_extra(workspace_id) if workspace_id else {}
        kind = extra.get("kind") or WORKSPACE_KIND_SYSTEM
        if kind != WORKSPACE_KIND_LOCAL:
            sp = self.session_paths(session_id) if not self.db.get_session(session_id) else None
            root = sp.workspace if sp else (self.paths.sessions / str(session_id) / "workspace")
            probe = probe_workspace_root(root)
            return {
                "ok": probe["ok"], "kind": WORKSPACE_KIND_SYSTEM, "path": str(root),
                "reason": probe["reason"], "message": probe["message"],
                "exists": probe["exists"], "readable": probe["readable"],
                "writable": probe["writable"], "folder_id": "", "folder_name": "",
                "workspace_id": workspace_id,
            }

        raw = Path(str(extra.get("folder_path") or ""))
        probe = probe_workspace_root(raw)
        if not probe["ok"]:
            self.logger.exception_log(
                error_code=ERR_WORKSPACE_UNAVAILABLE,
                message=(f"{probe['message']}（工作区={workspace_id}，"
                         f"folder_id={extra.get('folder_id') or '-'}）"),
                session_id=session_id, agent_role="system",
            )
        return {
            "ok": probe["ok"], "kind": WORKSPACE_KIND_LOCAL, "path": str(raw),
            "reason": probe["reason"], "message": probe["message"],
            "exists": probe["exists"], "readable": probe["readable"],
            "writable": probe["writable"],
            "folder_id": extra.get("folder_id", ""),
            "folder_name": extra.get("folder_name", ""),
            "workspace_id": workspace_id,
        }

    def list_workspaces(self, *, q: str | None = None) -> list[dict]:
        """工作区列表（含会话数与依归会话），供左树展示。历史会话按默认工作区归组。"""
        workspaces = self.db.list_workspaces()
        out: list[dict] = []
        for ws in workspaces:
            name = ws["name"]
            extra = self._workspace_extra(ws["workspace_id"])
            sessions = self.db.list_sessions(limit=500, workspace_id=ws["workspace_id"])
            items = [{
                "session_id": s["session_id"],
                "title": s["title"],
                "created_at": s["created_at"],
                "updated_at": s["updated_at"],
                "task_count": len(self.db.list_tasks(s["session_id"], limit=500)),
            } for s in sessions]
            if q and q.strip():
                key = q.strip().lower()
                if key not in name.lower() and not any(key in i["title"].lower() for i in items):
                    continue
            folder_path = extra.get("folder_path") or ""
            root = self.workspace_root_for(ws["workspace_id"])
            out.append({
                "workspace_id": ws["workspace_id"],
                "name": name,
                "created_at": ws["created_at"],
                "updated_at": ws["updated_at"],
                "sort_order": ws["sort_order"],
                "session_count": len(items),
                "sessions": items,
                # 当前工作区的文件操作根目录（本地文件夹 或 系统隔离目录）
                "kind": extra.get("kind") or WORKSPACE_KIND_SYSTEM,
                "folder_id": extra.get("folder_id", ""),
                "folder_path": str(root) if root else folder_path,
                "folder_available": bool(root) if folder_path else True,
                "dir": str(root) if root else str(self.paths.sessions),
            })
        return out

    def create_workspace(self, name: str, *, folder_path: str | None = None) -> dict:
        workspace_id = "ws_" + self.router.new_session_id().replace("-", "")[:20]
        clean = (name or "").strip() or "新工作区"
        ws = self.db.create_workspace(workspace_id, clean)
        result = {
            "workspace_id": ws["workspace_id"], "name": ws["name"],
            "created_at": ws["created_at"], "session_count": 0, "sessions": [],
            "kind": WORKSPACE_KIND_SYSTEM, "folder_path": "",
        }
        if folder_path:
            # 【需求点 二、1】直接用本地文件夹创建工作区
            folder = self.set_workspace_folder(workspace_id, folder_path)
            result.update({
                "name": folder["name"], "kind": WORKSPACE_KIND_LOCAL,
                "folder_path": folder["path"], "folder_id": folder["folder_id"],
            })
        self.logger.info(f"工作区已创建：{clean}（{workspace_id}）", agent_role="system")
        return result

    def rename_workspace(self, workspace_id: str, name: str) -> dict:
        clean = (name or "").strip()
        if not clean:
            raise SecurityViolation("工作区名称不能为空", code="INVALID_WORKSPACE_NAME")
        if not self.db.get_workspace(workspace_id):
            raise SecurityViolation(f"工作区不存在：{workspace_id}", code="WORKSPACE_NOT_FOUND")
        self.db.rename_workspace(workspace_id, clean)
        self.logger.info(f"工作区已重命名：{workspace_id} -> {clean}", agent_role="system")
        return {"workspace_id": workspace_id, "name": clean}

    def delete_workspace(self, workspace_id: str) -> dict:
        """删除工作区（前端需二次确认），清理其下会话数据与系统会话目录。

        注意：工作区绑定的**用户本地文件夹不会被删除**，仅解除登记关系。
        """
        if not self.db.get_workspace(workspace_id):
            raise SecurityViolation(f"工作区不存在：{workspace_id}", code="WORKSPACE_NOT_FOUND")
        extra = self._workspace_extra(workspace_id)
        outcome = self.db.delete_workspace(workspace_id)
        # 清理会话私有目录（仅删除本生态系统 sessions/ 下的对应目录）
        removed_dirs: list[str] = []
        for sid in outcome["session_ids"]:
            try:
                target = SessionPaths.build(self.paths, sid).root
                if target.exists() and is_within(target, self.paths.sessions) and target != self.paths.sessions:
                    shutil.rmtree(target, ignore_errors=True)
                    removed_dirs.append(str(target))
            except Exception as exc:  # noqa: BLE001 目录清理失败不影响数据删除结果
                self.logger.warning(f"会话目录清理失败 {sid}: {exc}", agent_role="system")

        # 解除本地文件夹登记（不删除磁盘目录内容）
        folder_id = extra.get("folder_id")
        folder_path = extra.get("folder_path") or ""
        if folder_id:
            try:
                self.config.remove_workspace_folder(folder_id)
            except SecurityViolation:
                pass
        if folder_path:
            self.config.remove_marker(folder_path)

        self.logger.info(
            f"工作区已删除：{workspace_id}；清理会话 {len(outcome['session_ids'])} 个，"
            f"系统目录 {len(removed_dirs)} 个；用户本地目录未删除：{folder_path or '（无）'}",
            agent_role="system",
        )
        return {**outcome, "removed_dirs": removed_dirs,
                "folder_path_preserved": folder_path,
                "note": "用户本地文件夹内容未被删除，仅解除了工作区登记"}

    def move_session(self, session_id: str, workspace_id: str) -> dict:
        """会话迁移归属：A 工作区 -> B 工作区。

        【需求点 四、3】切换工作区后仅加载当前工作区对应的会话；
        迁移后该会话的文件操作根目录跟随新工作区。
        """
        if not self.db.get_session(session_id):
            raise SecurityViolation(f"会话不存在：{session_id}", code="SESSION_NOT_FOUND")
        if not self.db.get_workspace(workspace_id):
            raise SecurityViolation(f"目标工作区不存在：{workspace_id}", code="WORKSPACE_NOT_FOUND")
        self.db.move_session(session_id, workspace_id)
        new_root = self.workspace_root_for(workspace_id)
        self.logger.info(
            f"会话归属已迁移：{session_id} -> {workspace_id}；"
            f"新文件操作根目录：{new_root or '系统隔离目录'}",
            agent_role="system",
        )
        return {
            "session_id": session_id, "workspace_id": workspace_id,
            "workspace_root": str(new_root) if new_root else "",
        }

    def session_workspace(self, session_id: str) -> str:
        row = self.db.get_session(session_id)
        return (row or {}).get("workspace_id") or DEFAULT_WORKSPACE_ID

    # ------------------------------------------------------------------
    # 【需求点 二、1】本地文件夹选择：名称 → 绝对路径 解析
    # ------------------------------------------------------------------
    def resolve_folder_selection(self, folder_name: str, *, explicit_path: str | None = None) -> dict:
        """把浏览器选中的文件夹解析为可用的绝对路径工作区。

        浏览器 File System Access API 出于安全限制只给出文件夹**名称**，
        因此这里：
          1. 若前端已能提供绝对路径（用户手动粘贴），优先使用并校验；
          2. 否则按名称在常用根目录内扫描候选（受控深度），返回候选列表供用户确认；
          3. 若已有带 `.mae_workspace.json` 标记的目录命中，直接采用。
        """
        if explicit_path:
            folder = self.config.register_workspace_folder(explicit_path)
            return {"resolved": True, "folder": folder, "candidates": []}

        candidates = self.config.scan_folder_candidates(folder_name or "")
        if len(candidates) == 1:
            folder = self.config.register_workspace_folder(candidates[0])
            return {"resolved": True, "folder": folder, "candidates": candidates}
        return {
            "resolved": False,
            "folder": None,
            "folder_name": folder_name,
            "candidates": candidates[:30],
            "message": (
                f"已定位到 {len(candidates)} 个同名文件夹，请选择具体目录" if candidates
                else f"未能在常用位置定位到文件夹「{folder_name}」，请手动填写完整路径"
            ),
        }

    def add_local_folder_workspace(self, *, path: str | None = None,
                                   folder_name: str | None = None,
                                   workspace_id: str | None = None) -> dict:
        """【需求点 二、1】添加工作区：登记本地文件夹并切换为当前激活工作区。"""
        if path:
            folder = self.config.register_workspace_folder(path)
            resolved = {"resolved": True, "folder": folder, "candidates": []}
        else:
            resolved = self.resolve_folder_selection(folder_name or "")
            if not resolved["resolved"]:
                return {"ok": False, **resolved}
            folder = resolved["folder"]

        # 复用同名工作区，避免重复创建
        target_ws = workspace_id if workspace_id and self.db.get_workspace(workspace_id) else None
        if target_ws is None:
            existing = next(
                (w for w in self.list_workspaces()
                 if (w.get("folder_path") or "").lower() == folder["path"].lower()),
                None,
            )
            target_ws = existing["workspace_id"] if existing else None
        if target_ws is None:
            created = self.create_workspace(folder["name"], folder_path=folder["path"])
            target_ws = created["workspace_id"]
        elif not self._workspace_extra(target_ws).get("folder_path"):
            self.set_workspace_folder(target_ws, folder["path"])

        self.logger.info(
            f"当前激活工作区已切换：{folder['name']}（{folder['path']}）；"
            "所有 Agent 文件操作根目录已更新到该文件夹",
            agent_role="system",
        )
        return {
            "ok": True, "resolved": True, "folder": folder,
            "workspace_id": target_ws,
            "workspace": next((w for w in self.list_workspaces()
                               if w["workspace_id"] == target_ws), None),
        }

    # ------------------------------------------------------------------
    # 会话上下文（工作区目录隔离的唯一入口，第5章 / 第2章 2.2 规则1）
    # 【需求点 二、2】文件操作根目录 = 当前选中工作区文件夹
    # ------------------------------------------------------------------
    def session_paths(self, session_id: str) -> SessionPaths:
        """构造会话路径，其中 workspace = 该会话所属工作区的操作根目录。

        · 工作区已绑定本地文件夹 → workspace 指向该本地目录（真实绝对路径）
        · 否则 → 沿用系统隔离目录 sessions/<sid>/workspace（向后兼容）

        【BUG-B 1/2/3】绑定的是本地文件夹时，这里必须做**目录前置校验**：
        目录不存在 / 无读权限 → 抛 WorkspaceUnavailable，让上层给出
        「错误：当前工作绑定目录【xxx】不存在 / 无读取权限」并终止对应子任务，
        而不是悄悄回退到内存虚拟目录（那会让 Agent 拿着空路径一路连锁失败）。
        """
        workspace_id = self.session_workspace(session_id) if self.db.get_session(session_id) else ""
        extra = self._workspace_extra(workspace_id) if workspace_id else {}
        if extra.get("kind") == WORKSPACE_KIND_LOCAL:
            raw = str(extra.get("folder_path") or "")
            # 目录前置校验：存在性 + 读权限（真实物理目录，不是内存路径）
            assert_workspace_readable(raw)
            root: Path | None = Path(raw)
        else:
            root = self.workspace_root_for(workspace_id) if workspace_id else None
        return SessionPaths.build(
            self.paths, session_id,
            workspace_root=root,
            workspace_folder_id=extra.get("folder_id", ""),
            workspace_folder_name=extra.get("folder_name", ""),
        ).ensure()

    def bind_session(self, session_id: str) -> SessionFileGuard:
        """把当前会话（及其工作区根目录）绑定进所有 Agent 的上下文。

        绑定后 ctx.file_guard 的写根目录即当前工作区文件夹；
        切换工作区会重新调用本方法，从而自动更新文件操作根目录。

        【BUG-B 1/2/3】绑定前做目录前置校验：
        本地文件夹不存在 / 无读权限 → 抛 WorkspaceUnavailable（统一可读文案），
        绝不下发空路径或虚假目录给 Agent。
        """
        sp = self.session_paths(session_id)
        guard = SessionFileGuard(sp)
        # 二次确认：guard 持有的根目录必须是可用真实目录
        guard.assert_workspace_usable()
        self.ctx.session_paths = sp
        self.ctx.file_guard = guard
        if sp.is_local_folder:
            self.logger.info(
                f"文件操作根目录已绑定到工作区文件夹（真实绝对路径）：{sp.workspace}",
                session_id=session_id, agent_role="system",
            )
        # 【BUG-A 1】会话绑定即打印各 Agent 将使用的厂商与密钥指纹（日志可核对）
        try:
            rows = self.model_client.consistency_report()
            issues = [f"{r['agent']}：{ '；'.join(r['issues']) }" for r in rows if r["issues"]]
            self.logger.task_log(
                session_id=session_id, task_id="", agent_role="system",
                event="model.binding.consistency",
                level="warn" if issues else "info",
                detail=("各 Agent 运行态绑定核对：" + ("；".join(issues) if issues
                                                       else "全部一致（连通测试与真实调用同源）")),
            )
        except Exception:  # noqa: BLE001 核对失败绝不影响任务主链路
            pass
        return guard

    def stream_emit(self, task_id: str, event: str, *, session_id: str = "",
                    agent_role: str = "", model_label: str = "", **detail: Any) -> None:
        """【需求点 Bug2】发布思维链事件的统一入口。

        自动补齐 provider 与 model_label（取自 constants.AGENT_BINDINGS 的唯一绑定），
        避免各处漏传导致前端拿不到模型信息。任何异常都被 StreamBroker 吞掉，
        绝不影响任务主链路。
        """
        provider = ""
        if agent_role and agent_role in AGENT_BINDINGS:
            provider = AGENT_BINDINGS[agent_role]["provider"]
            if not model_label:
                model_label = self.active_model_label(agent_role)
        self.stream.emit(
            task_id, event, session_id=session_id, agent_role=agent_role,
            model_label=model_label, provider=provider, **detail,
        )

    # ------------------------------------------------------------------
    # 任务状态读写
    # ------------------------------------------------------------------
    def load_task_state(self, task_id: str) -> TaskState | None:
        row = self.db.get_task(task_id)
        return db_row_to_state(row) if row else None

    def persist_task(self, state: TaskState) -> None:
        self.db.update_task(
            state.task_id,
            status=state.status, iteration=state.iteration, retry_count=state.retry_count,
            review_rejects=state.review_rejects, started_at=state.started_at,
            finished_at=state.finished_at, deadline_at=state.deadline_at,
            result=state.result[:8000] if state.result else "", error_code=state.error_code,
            error_message=state.error_message, title=state.title,
        )

    def _record_message(self, *, session_id: str, task_id: str, parent_task_id: str | None,
                        sender: str, receiver: str, msg_type: str, content: str,
                        metadata: dict | None = None, status: str = STATUS_SUCCESS,
                        think_steps: list[dict] | None = None) -> None:
        """把消息落库（统一结构体，第3章 3.1）。

        用户输入与最终交付不经过总线投递（否则会被再次触发 Agent 执行），
        但依然必须按统一消息结构体落库，保证第9.2 永久记录与会话回放一致。
        """
        if think_steps:
            metadata = {**(metadata or {}), "think_steps": think_steps}
        msg = Message(
            session_id=session_id, task_id=task_id, parent_task_id=parent_task_id,
            sender_agent=sender, receiver_agent=receiver, msg_type=msg_type,
            payload={"content": content, "metadata": metadata or {}},
            status=status,
        )
        if not msg.msg_id:
            msg.msg_id = new_uuid()
        self.db.insert_message(msg.to_dict(include_msg_id=True))

    # ------------------------------------------------------------------
    # 逐条落库（会话回放用）
    # ------------------------------------------------------------------
    def record_user_input(self, session_id: str, task_id: str, text: str) -> None:
        self._record_message(
            session_id=session_id, task_id=task_id, parent_task_id=None,
            sender="user", receiver=AGENT_DISPATCH, msg_type="task",
            content=text, metadata={"kind": "user_input"}, status=STATUS_SUCCESS,
        )

    def record_final_reply(self, session_id: str, task_id: str, text: str, *,
                           msg_type: str = "result", status: str = STATUS_SUCCESS,
                           metadata: dict | None = None) -> None:
        # 幂等保护：同一任务只保留一条最终交付记录（审批恢复会再次收敛）
        existing = self.db.query(
            "SELECT msg_id FROM messages WHERE task_id=? AND receiver_agent='user' "
            "AND msg_type IN ('result','error') LIMIT 1",
            (task_id,),
        )
        if existing:
            return
        steps = self.db.list_think_steps(task_id)
        think = [{
            "index": s["step_index"], "type": s["step_type"],
            "text": s["step_text"], "agent": s["agent_role"],
        } for s in steps]
        self._record_message(
            session_id=session_id, task_id=task_id, parent_task_id=None,
            sender=AGENT_DISPATCH, receiver="user", msg_type=msg_type,
            content=text, metadata=metadata, status=status, think_steps=think,
        )

    # ==================================================================
    # 【BUG-C 3/4/5/6】链路故障诊断报告（调度规划Agent 汇总输出给用户）
    # ==================================================================
    def _build_fault_report(self, root: TaskState, sub_results: list[SubtaskResult],
                            failures: list[SubtaskResult]) -> dict:
        """汇总整条任务链路的故障信息：哪一步 Agent、什么任务、失败原因、影响范围。"""
        def _entry(s: SubtaskResult) -> dict:
            kind = s.failure_kind or classify_failure(s.metadata.get("code", "") if isinstance(s.metadata, dict) else "")
            return {
                "index": s.index,
                "title": s.title,
                "agent_role": s.agent_role,
                "task_id": s.task_id,
                "status": s.status,
                "failure_stage": s.failure_stage or "unknown",
                "failure_reason": s.failure_reason or s.error or "未提供失败原因",
                "error": s.error,
                # 【增量修复 3】失败类型：IO 错误（目录/权限） vs 模型收敛失败
                "failure_kind": kind,
                "failure_kind_label": FAILURE_KIND_LABEL.get(kind, ""),
                "retries_exhausted": bool(s.retries_exhausted),
                "retry_count": int(s.retry_count or 0),
                "blocked_by": list(s.blocked_by or []),
                "output_preview": (s.output or "")[:300],
            }

        failed_entries = [_entry(s) for s in failures]
        blocked = [_entry(s) for s in sub_results
                   if s.status == STATUS_FAILED and s.failure_stage == "dependency_blocked"]
        succeeded = [_entry(s) for s in sub_results if s.status == STATUS_SUCCESS]
        skipped = [s.title for s in sub_results
                   if s.status not in (STATUS_SUCCESS, STATUS_FAILED, STATUS_WAITING_APPROVAL)]

        exhausted = [e for e in failed_entries if e["retries_exhausted"]]
        head = exhausted[0] if exhausted else failed_entries[0]
        summary = (
            f"任务链路已中断：子任务「{head['title']}」（执行 Agent：{head['agent_role']}）失败，"
            f"根任务 {root.task_id} 已标记 failed"
            + (f"；{SUBTASK_RETRY_EXHAUSTED_MESSAGE}" if exhausted else "")
            + (f"；另有 {len(blocked)} 个下游子任务被链路阻断" if blocked else "")
        )

        lines: list[str] = [
            "## 🛑 任务执行失败：链路已中断",
            "",
            f"- **根任务**：`{root.task_id}`",
            f"- **最终状态**：`{STATUS_FAILED}`（不是 success，未用成功状态掩盖失败）",
            f"- **失败子任务数**：{len(failed_entries)}"
            + (f"（其中重试达到上限 {len(exhausted)} 个）" if exhausted else ""),
            "",
            "## 失败点定位（哪一步 Agent、什么任务、失败原因）",
        ]
        for i, e in enumerate(failed_entries, 1):
            lines.append(f"{i}. **子任务 #{e['index']} · {e['title']}**")
            lines.append(f"   - 执行 Agent：{e['agent_role']}")
            lines.append(f"   - 子任务号：`{e['task_id'] or '(未创建)'}`")
            lines.append(f"   - 失败阶段：`{e['failure_stage']}`"
                         + (f"（因上游子任务 {e['blocked_by']} 失败被阻断）" if e["blocked_by"] else ""))
            # 【增量修复 3】失败类型：IO 错误 / 模型收敛失败
            lines.append(f"   - 失败类型：{e['failure_kind_label'] or '未分类'}"
                         + (f"（`{e['failure_kind']}`）" if e["failure_kind"] else ""))
            lines.append(f"   - 重试情况：{e['retry_count']}/{MAX_SUBTASK_RETRIES} 次"
                         + ("，**已达到重试上限并终止**" if e["retries_exhausted"] else ""))
            lines.append(f"   - 失败原因：{e['failure_reason']}")
            if e["error"]:
                lines.append(f"   - 错误原文：{str(e['error'])[:500]}")
        if not failed_entries:
            lines.append("· （无失败的子任务条目）")

        if blocked:
            lines.append("")
            lines.append("## 被链路阻断的下游子任务（未下发执行）")
            for e in blocked:
                lines.append(f"- 子任务 #{e['index']}「{e['title']}」（{e['agent_role']}）："
                             f"依赖上游子任务 {e['blocked_by']} 的成功输出，已阻断")

        if succeeded:
            lines.append("")
            lines.append("## 失败前已成功完成的子任务（输出真实有效）")
            for e in succeeded:
                preview = (e["output_preview"] or "").replace("\n", " ")
                lines.append(f"- ✓ 子任务 #{e['index']}「{e['title']}」（{e['agent_role']}）："
                             f"{(preview[:160] + '…') if len(preview) > 160 else preview or '（无输出摘要）'}")

        if skipped:
            lines.append("")
            lines.append("## 未执行的子任务")
            lines.append("- " + "、".join(str(t) for t in skipped[:20]))

        lines += [
            "",
            "## 处理说明",
            f"- {FAULT_REPORT_NO_SYNTHESIS_NOTE}",
            "- 依赖失败子任务输出的下游子任务一律不再分发（链路阻断）；",
            "- 完整的 HTTP 状态码与模型返回原文已写入系统日志，可在「日志」标签页查看。",
            "",
            "## 建议处理方式",
            "- 检查该子任务失败原因（工作区目录是否可用 / 目标文件是否存在 / 权限是否足够）；",
            "- 若是模型侧问题，可在设置面板确认对应厂商的密钥与模型连通性；",
            "- 修正后重新发起该任务。",
        ]

        return {
            "blocked": True,
            "stage": "subtask_failed",
            "root_task_id": root.task_id,
            "root_status": STATUS_FAILED,
            "summary": summary,
            "failed_subtasks": failed_entries,
            "blocked_subtasks": blocked,
            "succeeded_subtasks": succeeded,
            "not_executed": skipped,
            "note": FAULT_REPORT_NO_SYNTHESIS_NOTE,
            "report": "\n".join(lines),
        }

    def _skip_downstream_delivery(self, root: TaskState, result: PipelineResult) -> None:
        """【BUG-C 6】链路失败后跳过评估校验Agent 与 交互交付Agent，并如实记录跳过原因。"""
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        dispatch.think(root, "exec",
                       f"{FAULT_REPORT_NO_SYNTHESIS_NOTE}已由调度规划Agent 直接输出故障诊断。",
                       level=THINK_LEVEL_WARN)
        self.stream.emit(
            root.task_id, "chain_blocked", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, status=STATUS_FAILED,
            model_label=self.active_model_label(AGENT_DISPATCH),
            text=(f"链路阻断：已跳过 {AGENT_EVALUATOR} 与 {AGENT_DELIVERY}，"
                  "不再基于失败/空结果生成汇总报告"),
            level=THINK_LEVEL_ERROR,
            skipped_agents=[AGENT_EVALUATOR, AGENT_DELIVERY],
        )
        result.plan["downstream_skipped"] = {
            "agents": [AGENT_EVALUATOR, AGENT_DELIVERY],
            "reason": FAULT_REPORT_NO_SYNTHESIS_NOTE,
            "root_status": root.status,
        }
        self.logger.task_log(
            session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            event="chain.blocked", level="error",
            detail=(f"子任务失败 → 链路阻断：跳过 {AGENT_EVALUATOR} / {AGENT_DELIVERY}，"
                    "不生成虚假汇总报告"),
        )

    # ==================================================================
    # 【BUG-B 2/3/4】工作区不可用 → 统一终止结果（不落库、不启动任何子任务）
    # ==================================================================
    def _workspace_blocked_result(self, *, session_id: str, path: str, reason: str,
                                  message: str, stage: str, started: float,
                                  detail: str = "") -> PipelineResult:
        """工作区目录不存在 / 无读权限时的统一返回：明确错误 + 故障诊断，终止整条链路。"""
        self.finish_task_timer(session_id, task_status=STATUS_FAILED,
                               reason=f"工作区目录不可用：{reason}")
        self.logger.exception_log(
            error_code=ERR_WORKSPACE_UNAVAILABLE, message=f"{message}（阶段={stage}）",
            session_id=session_id, agent_role=AGENT_DISPATCH,
        )
        result = PipelineResult(
            session_id=session_id, task_id="", status=STATUS_FAILED,
            error_code=ERR_WORKSPACE_UNAVAILABLE, error_message=message,
            workspace_root=path,
            final_reply=(
                "## 🚫 任务未启动：工作区目录不可用\n\n"
                f"- {message}\n"
                f"- 错误码：`{ERR_WORKSPACE_UNAVAILABLE}`（原因：{reason}）\n"
                f"- 绑定目录：`{path or '（空）'}`\n"
                + (f"- 校验明细：{detail}\n" if detail else "")
                + "\n**处理建议**\n"
                "- 确认该文件夹没有被移动 / 重命名 / 删除；\n"
                "- 确认当前进程账号对该目录拥有读取权限（Windows 可在属性→安全里核对）；\n"
                "- 在左侧工作区列表重新绑定一个存在的、有读取权限的文件夹后重试。\n\n"
                "（系统已按要求终止本次任务：**未向任何子 Agent 下发文件路径**，"
                "因此不会产生连锁失败，也不会生成基于空结果的汇总报告。）"
            ),
        )
        result.think_steps = [{
            "index": 1, "type": "exec", "level": THINK_LEVEL_ERROR,
            "text": message, "agent": AGENT_DISPATCH,
        }]
        result.subtasks = [{
            "index": 0, "title": "工作区目录前置校验", "agent_role": AGENT_DISPATCH,
            "status": STATUS_FAILED, "output": "", "error": message, "task_id": "",
            "failure_stage": stage, "failure_reason": reason,
        }]
        result.plan = {
            "decision": "terminate",
            "reasoning": f"工作区目录前置校验未通过（{reason}）：{message}",
            "subtasks": [],
            "fault_report": {
                "blocked": True, "stage": stage, "workspace_root": path,
                "reason": reason, "message": message,
                "failed_subtasks": [{
                    "index": 0, "title": "工作区目录前置校验",
                    "agent_role": AGENT_DISPATCH, "error": message,
                    "failure_stage": stage, "failure_reason": reason,
                }],
                "note": FAULT_REPORT_NO_SYNTHESIS_NOTE,
            },
        }
        result.degradation = {
            **self.model_client.degradation_status(),
            "vector_db_available": self.vector_store.available,
            "notes": [message],
        }
        result.stats = {"wall_clock_ms": int((time.time() - started) * 1000)}
        # 【需求点 Bug2】流式事件：把可读错误推给前端思考链
        self.stream.emit(
            "workspace_precheck", "task_status", session_id=session_id,
            agent_role=AGENT_DISPATCH,
            status=STATUS_FAILED, model_label=self.active_model_label(AGENT_DISPATCH),
            text=message, error_code=ERR_WORKSPACE_UNAVAILABLE, level=THINK_LEVEL_ERROR,
        )
        return result

    # ==================================================================
    # 主链路：第4章 4.1 执行流程
    # ==================================================================
    async def run_pipeline(
        self,
        *,
        session_id: str | None,
        user_input: str,
        uploaded_images: list[dict] | None = None,
        model_hint: str | None = None,
        client_task_id: str | None = None,
    ) -> PipelineResult:
        started = time.time()

        # 【需求点 三、2】仅本次任务周期使用补位模型：每次全新任务先清空上一任务的补位记忆
        self.reset_ecosystem_fallbacks()
        self.model_client.clear_task_fallback(self.last_task_id)
        # 【队长-队员架构】新任务周期：重置队长仲裁态，避免跨任务误用上一轮的"唯一基准清单"
        self._last_arbitration = None
        if session_id:
            try:
                self.arbitration.reset(session_id)
            except Exception:  # noqa: BLE001 仲裁态重置失败不影响任务
                pass
        # ==================================================================
        # 【BUG-A 4】全新任务开始：清空上一轮的模型失败标记与调度降级标记。
        #   配置存在且连通测试通过的模型，必须在新任务里重新尝试专属模型，
        #   绝不允许因为上一轮的偶发失败而"无故触发生态位补位"。
        # ==================================================================
        self.model_client.reset_task_state()

        # 保证记忆管理Agent 的异步消费者已挂载（第4章 4.5：总线主动推送 + 异步消费，不轮询）
        self.bus.start_memory_consumer(self.agents[self.ctx_memory_role()])

        # ---- 0. 会话准备（第5章 会话独立目录） ----
        session_id = session_id or self.router.new_session_id()

        # ==================================================================
        # 【BUG-B 1/2/3】文件类任务前置校验：绑定的本地文件夹必须真实存在且可读。
        #   不通过 → 直接返回明确错误（写入思考链路），不把空路径下发给任何 Agent，
        #   也不启动任何子任务（避免后续连锁失败）。
        # ==================================================================
        precheck = self.workspace_precheck(session_id)
        if not precheck["ok"]:
            return self._workspace_blocked_result(
                session_id=session_id, path=precheck["path"], reason=precheck["reason"],
                message=precheck["message"], stage="workspace_precheck",
                detail=(f"目录存在：{'是' if precheck['exists'] else '否'}；"
                        f"可读：{'是' if precheck['readable'] else '否'}；"
                        f"可写：{'是' if precheck['writable'] else '否'}"),
                started=started,
            )

        # 【需求点 二、2 规则4】绑定当前会话后，文件操作根目录自动跟随其工作区文件夹
        try:
            guard = self.bind_session(session_id)
        except WorkspaceUnavailable as exc:
            # 【BUG-B 3/4】目录不存在 / 无读权限 → 直接返回明确错误，终止任务
            return self._workspace_blocked_result(
                session_id=session_id, path=exc.path, reason=exc.reason,
                message=str(exc), stage="workspace_bind", started=started,
            )
        except SecurityViolation as exc:
            # 【需求点 一、2】路径类异常优雅降级：返回可读提示，绝不整体崩溃
            self.finish_task_timer(session_id, task_status=STATUS_FAILED,
                                   reason=f"工作区绑定失败：{exc.code}")
            return PipelineResult(session_id=session_id, task_id="", status=STATUS_FAILED,
                                  error_code=exc.code, error_message=str(exc))
        workspace_root = str(guard.workspace_root)

        # 【需求点 二、2-a】任务正式开始提交、Agent 开始执行 → 启动计时器（新任务清零）
        #   · 入口（POST /api/session/chat）已启动计时 → 这里复用同一计时器（不重置）；
        #   · 幂等重入（同一任务）也不会重新清零；
        #   · 计时启动失败绝不阻断任务主链路（第2章 2.3 降级兜底）。
        try:
            self.start_task_timer(
                session_id=session_id,
                title=(user_input or "新会话").strip().splitlines()[0][:40] or "新会话",
                workspace_root=workspace_root,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.exception_log(
                error_code=ERR_TIMER_RECORD_FAILED,
                message=f"任务计时启动失败（已降级，任务继续执行）：{exc}",
                session_id=session_id, agent_role=AGENT_DISPATCH,
            )

        title = (user_input or "新会话").strip().splitlines()[0][:40] or "新会话"
        self.db.upsert_session(session_id, title)

        # ---- 1. 图片资源隔离入库（第3章 3.3 / 第6章 6.2 规则4） ----
        image_paths: list[str] = []
        upload_results: list[dict] = []
        for item in (uploaded_images or []):
            try:
                info = guard.save_upload(item["filename"], item["data"])
                upload_results.append(info)
                if info["resource_type"] == "image":
                    image_paths.append(info["path"])
            except SecurityViolation as exc:
                self.logger.exception_log(
                    error_code=exc.code, message=f"上传被安全模块拒绝：{exc}",
                    session_id=session_id, agent_role=AGENT_CODE,
                )
                return PipelineResult(
                    session_id=session_id, task_id="", status=STATUS_FAILED,
                    error_code=exc.code, error_message=str(exc),
                    stats={"uploads": upload_results},
                )

        # ---- 2. 创建根任务（状态 pending） ----
        root = TaskState(
            task_id=self.router.new_task_id(), session_id=session_id,
            title=title, agent_role=AGENT_DISPATCH,
        )
        # 【需求点 Bug2】允许前端预先指定 task_id：前端在发起 POST 之前就能用同一 ID
        #   订阅 SSE 思维链通道，从而在任务执行期间实时收到分派事件
        #   （POST 本身是同步阻塞的，否则前端拿不到流式输出）。
        if client_task_id and _SAFE_TASK_ID_RE.match(str(client_task_id)):
            root.task_id = str(client_task_id)
        root.deadline_at = root.created_at + MAX_TASK_TIMEOUT_SECONDS
        self.router.register_task(root)
        self.last_task_id = root.task_id
        # 【需求点 二、任务执行计时器】把根任务号绑定到本轮计时器上
        try:
            self.attach_task_to_timer(session_id, root.task_id)
        except Exception as exc:  # noqa: BLE001 计时异常不影响任务主链路
            self.logger.exception_log(
                error_code=ERR_TIMER_RECORD_FAILED,
                message=f"计时器任务号绑定失败（已降级）：{exc}",
                session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            )
        self.logger.task_log(
            session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            event="task.created", detail=f"status=pending title={title}",
        )
        # 用户输入按统一消息结构体落库（第9.2 永久记录 / 会话回放）
        self.record_user_input(session_id, root.task_id, user_input or "（仅附件）")

        # ==================================================================
        # 【需求点 Bug2】开启本任务的流式思维链通道，并推送首条事件
        # ==================================================================
        self.stream.open(root.task_id, session_id)
        self.stream.emit(
            root.task_id, "task_created", session_id=session_id, agent_role=AGENT_DISPATCH,
            model_label=self.active_model_label(AGENT_DISPATCH), status=STATUS_PENDING,
            title=title,
            text=f"任务已创建：{title}｜文件操作根目录：{workspace_root}",
            workspace_root=workspace_root,
            model_binding=self.model_binding_snapshot(),
        )

        # 【需求点 一、3】文件操作根目录写入独立字段（不放进 plan，避免被调度结果覆盖）
        result = PipelineResult(session_id=session_id, task_id=root.task_id,
                                status=STATUS_PENDING, workspace_root=workspace_root)
        model_failed = False
        # 【BUG-C 3/6】链路中是否出现最终失败的子任务（为真则阻断下游汇总/评估/交付）
        failed_in_chain = False
        fail_code = ""
        fail_message = ""
        if model_hint:
            self.logger.info(f"用户选择的主模型提示：{model_hint}", session_id=session_id,
                             task_id=root.task_id, agent_role=AGENT_DISPATCH)

        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]

        try:
            # ---- 3. 状态流转 pending -> running ----
            root.mark_running("分发执行")
            self.persist_task(root)

            # ---- 4. 记忆检索（第4章 4.1 执行流程） ----
            memory_hits: list[dict] = []
            memory_agent: MemoryAgent = self.agents[self.ctx_memory_role()]
            try:
                memory_hits = memory_agent.search(session_id, user_input)
            except Exception as exc:  # noqa: BLE001 记忆异常绝不影响主链路（第2章 2.3 规则3）
                self.logger.exception_log(
                    error_code="MEMORY_SEARCH_FAILED", message=f"记忆检索失败（已降级跳过）：{exc}",
                    session_id=session_id, task_id=root.task_id, agent_role=self.ctx_memory_role(),
                )

            history = [{"role": "user", "text": m.get("content", "")}
                       for m in memory_agent.short_term(session_id, limit=6)]

            # ---- 5. 任务拆解 ----
            self.stream.emit(
                root.task_id, "task_status", session_id=session_id,
                agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
                status=STATUS_RUNNING,
                text=f"{AGENT_DISPATCH}（{self.active_model_label(AGENT_DISPATCH)}）开始拆解需求并生成任务依赖图",
            )
            plan = await dispatch.plan(
                root, user_input, image_resources=image_paths,
                memory_hits=memory_hits, history=history,
            )
            result.plan = plan
            self.persist_task(root)

            if plan.get("degraded"):
                note = "当前为备选调度模型"
                if note not in self.degraded_notes:
                    self.degraded_notes.append(note)
                result.degradation["dispatch_degraded"] = True

            # ---- 6. 依赖校验（评估校验Agent 强制校验依赖图，第4章 4.6） ----
            subtasks_plan = plan.get("subtasks") or []

            # 第4章 4.4 触发条件：无图片输入，视觉感知Agent 直接跳过
            if not image_paths:
                filtered = [s for s in subtasks_plan if s.get("agent_role") != AGENT_VISION]
                if len(filtered) != len(subtasks_plan):
                    dispatch.think(root, "think", "用户输入不含图片，已剔除视觉感知Agent 子任务（节省Token）")
                subtasks_plan = filtered
            result.plan["subtasks"] = subtasks_plan

            # ==============================================================
            # 【Bug2 修复】任务拆解完成的第一时间推送 plan 事件（任务栏立即渲染）：
            #   依赖图校验 / 重生成还要再走若干次模型调用，若等校验全部结束才推送，
            #   前端任务栏会滞后数秒才看到子任务。这里在 dispatch.plan() 返回后
            #   立即按"初始拆解结果"推送；若后续触发依赖图重生成且确有改善，
            #   regenerate 分支会再推送一次修复后的 plan（前端按事件幂等覆盖，
            #   不会产生重复任务行）。
            # ==============================================================
            self.stream.emit(
                root.task_id, "plan", session_id=session_id, agent_role=AGENT_DISPATCH,
                model_label=self.active_model_label(AGENT_DISPATCH), status=root.status,
                text=(f"{AGENT_DISPATCH}（{self.active_model_label(AGENT_DISPATCH)}）已完成拆解，"
                      f"共 {len(subtasks_plan)} 个子任务"
                      if subtasks_plan else
                      f"{AGENT_DISPATCH}（{self.active_model_label(AGENT_DISPATCH)}）判定无需子任务，直接应答"),
                decision=plan.get("decision", ""),
                reasoning=str(plan.get("reasoning") or "")[:500],
                subtasks=[{
                    "index": s.get("index"), "title": s.get("title"),
                    "agent_role": s.get("agent_role"),
                    "model_label": self.active_model_label(str(s.get("agent_role") or "")),
                    "depend_on": s.get("depend_on") or [],
                } for s in subtasks_plan],
            )

            # ==============================================================
            # 【需求点 Bug1 规则3/4】依赖图校验：区分真实死循环与多分支可选路径
            #   1) 只有「真实死循环」（自引用 / 闭合拓扑环）才可能终止任务；
            #   2) 检测到疑似依赖异常（缺分支 / 无根节点 / 越界 / 模型判可疑）
            #      → 先重新调用调度规划Agent 重生成依赖图（重试 1 次）；
            #   3) 重生成后仍是真实死循环 → 才终止，并输出结构化详细报错。
            # ==============================================================
            evaluator = self.agents[AGENT_EVALUATOR]
            graph_verdict = None
            graph_report: dict = {}
            if subtasks_plan:
                graph_verdict = await evaluator.verify_dependency_graph(root, plan)
                result.evaluation = graph_verdict.to_dict()
                # 【需求点 Bug1】依赖图校验结论单独留档（result.evaluation 后续会被交付前
                # 轻量化安全扫描覆盖），供前端/排查查看真实环判定与复核痕迹
                result.plan["dependency_graph"] = graph_verdict.to_dict()
                graph_report = evaluator.graph_anomaly_report(subtasks_plan, verdict=graph_verdict)

            regenerate_used = 0
            while subtasks_plan and graph_report.get("structure_issue") \
                    and regenerate_used < MAX_PLAN_REGENERATE_RETRIES:
                regenerate_used += 1
                reasons = list(graph_report.get("reasons") or [])
                self.stream.emit(
                    root.task_id, "plan_repair", session_id=session_id, agent_role=AGENT_DISPATCH,
                    model_label=self.active_model_label(AGENT_DISPATCH), status=root.status,
                    text=(f"依赖图结构校验未通过，正在重新调用 {AGENT_DISPATCH}"
                          f"（{self.active_model_label(AGENT_DISPATCH)}）重新生成任务依赖图"
                          f"（重试 {regenerate_used}/{MAX_PLAN_REGENERATE_RETRIES}）"),
                    issues=reasons,
                )
                dispatch.think(
                    root, "think",
                    "依赖图结构校验未通过，重新生成任务依赖图。问题：" + "；".join(reasons)[:400],
                )
                repaired = await dispatch.regenerate_plan(
                    root, user_input,
                    image_resources=image_paths, memory_hits=memory_hits, history=history,
                    issues=reasons,
                )
                repaired_subtasks = repaired.get("subtasks") or []
                if not image_paths:
                    repaired_subtasks = [s for s in repaired_subtasks
                                         if s.get("agent_role") != AGENT_VISION]

                # 重生成结果必须确实改善才采纳（否则保留原图，避免越修越差）
                repaired_report = evaluator.graph_anomaly_report(repaired_subtasks)
                improved = (
                    bool(repaired_subtasks)
                    and len(repaired_report.get("reasons") or []) <= len(reasons)
                )
                if improved:
                    plan = repaired
                    subtasks_plan = repaired_subtasks
                    result.plan.update({
                        "decision": repaired.get("decision", plan.get("decision")),
                        "reasoning": repaired.get("reasoning", ""),
                        "subtasks": repaired_subtasks,
                        "dependencies": repaired.get("dependencies", {}),
                        "plan_revision": True,
                        "revision_feedback": repaired.get("revision_feedback", ""),
                    })
                    evaluator_verdict = await evaluator.verify_dependency_graph(root, plan)
                    result.evaluation = evaluator_verdict.to_dict()
                    result.plan["dependency_graph"] = evaluator_verdict.to_dict()
                    graph_report = evaluator.graph_anomaly_report(subtasks_plan, verdict=evaluator_verdict)
                    self.stream.emit(
                        root.task_id, "plan", session_id=session_id, agent_role=AGENT_DISPATCH,
                        model_label=self.active_model_label(AGENT_DISPATCH), status=root.status,
                        text=(f"{AGENT_DISPATCH} 已重新生成任务依赖图，共 {len(subtasks_plan)} 个子任务"),
                        decision=plan.get("decision", ""),
                        reasoning=str(plan.get("reasoning") or "")[:500],
                        subtasks=[{
                            "index": s.get("index"), "title": s.get("title"),
                            "agent_role": s.get("agent_role"),
                            "model_label": self.active_model_label(str(s.get("agent_role") or "")),
                            "depend_on": s.get("depend_on") or [],
                        } for s in subtasks_plan],
                    )
                else:
                    dispatch.think(root, "think",
                                   "重新生成的依赖图未改善，保留原依赖图并记录异常")
                    self.logger.task_log(
                        session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
                        event="plan.regenerate.no_improvement", level="warn",
                        detail=f"重生成后问题数 {len(repaired_report.get('reasons') or [])} 未减少",
                    )
                    break

            # 重生成后仍存在真实死循环 → 终止任务并输出结构化详细报错（需求 Bug1 规则4）
            if subtasks_plan and graph_report.get("hard_terminate"):
                detail_lines = [f"{i + 1}. {r}" for i, r in enumerate(graph_report.get("reasons") or [])]
                raise LoopDetected(
                    f"{DEPENDENCY_TERMINATED_TITLE}："
                    + "；".join(graph_report.get("reasons") or ["任务依赖图存在真实死循环"]),
                    code=ERR_REAL_CYCLE,
                )

            # 【Bug2 修复】plan 事件已前移到"拆解完成的第一时间"（依赖校验之前）；
            #   重生成确有改善时由上方 regenerate 分支再推送修复后的 plan。
            #   此处不再重复推送，避免前端思维链出现两条相同的拆解步骤。

            if subtasks_plan:
                evaluator = self.agents[AGENT_EVALUATOR]
                graph_verdict = await evaluator.verify_dependency_graph(root, plan)
                result.evaluation = graph_verdict.to_dict()
                if not graph_verdict.passed:
                    high = [i for i in graph_verdict.issues if i.get("severity") == "high"]
                    if high:
                        # 循环依赖 → 直接终止任务（第2章 2.1 规则3）
                        raise LoopDetected(
                            "任务依赖图校验发现循环依赖/结构性错误，直接终止：" +
                            "；".join(i["detail"] for i in high[:3])
                        )

            # ---- 7. 分发执行 / 直接应答 ----
            if plan.get("decision") == "respond" or not subtasks_plan:
                reply = str(plan.get("final_reply") or "").strip()
                if not reply:
                    reply = f"已收到你的输入：{user_input[:200]}"
                result.subtasks = []
                # 第2章 2.3 规则2：模型失效时不谎报成功，任务标记失败并返回能力不可用提示
                if plan.get("degraded"):
                    model_failed = True
                    fail_code = plan.get("error_code") or ERR_MODEL_UNAVAILABLE
                    fail_message = plan.get("reasoning") or "调度模型不可用"
            else:
                # ==========================================================
                # 【需求点 Bug2 业务流程重构】进入队长业务循环：
                #   队长逐轮"取子任务 → 分派队员 → 校验结果 → 重试/换人/终止"，
                #   命中高危操作立刻中断、落完整快照、置 waiting_approval 并返回；
                #   只有队长判定用户原始任务全部完成，才会汇总并交交互交付Agent。
                #   （禁止阻塞式 while 业务语义：循环状态全部落 SQLite 快照，可恢复）
                # ==========================================================
                captain_run = CaptainRun(
                    run_id=root.task_id,
                    user_input=user_input,
                    image_paths=list(image_paths or []),
                    plan=dict(result.plan or {}),
                    work=CaptainRun.assign_result_keys([dict(s) for s in subtasks_plan]),
                    degraded_notes=list(self.degraded_notes),
                    workspace_root=workspace_root,
                )
                self.save_captain_snapshot(run=captain_run, root=root, result=result,
                                           stage="captain_loop_start")
                result = await self._captain_loop(run=captain_run, result=result, root=root)
                self.degraded_notes = list(captain_run.degraded_notes)
                result.subtasks = captain_run.completed_dicts()
                result.plan["captain_loop"] = result.plan.get("captain_loop") or {
                    "completed": len(captain_run.completed),
                    "remaining": len(captain_run.work),
                    "retries": dict(captain_run.retries),
                    "replan_used": captain_run.replan_used,
                }
                if result.status == STATUS_WAITING_APPROVAL:
                    # 审批暂停：业务循环已中断，不得再往下走任何交付环节
                    await self.bus.flush_memory_events()
                    return self._finalize(result, root, started)
                if result.status == STATUS_FAILED:
                    failed_in_chain = True
                    fail_code = result.error_code
                    fail_message = result.error_message
                # 队长判定完成时，_captain_loop 内已经完成「汇总 → 交付质检 → 交互交付润色」
                # 与任务状态收口，这里直接收尾返回（历史下游步骤已被队长循环取代）。
                # 注意：必须保留内存中的 root（异常收口 / 状态机历史都在它上面），
                # 不使用重新加载的副本，否则会丢失失败错误码、异常记录等收口信息。
                await self.bus.flush_memory_events()
                return self._finalize(result, root, started)

            if failed_in_chain:
                # 【BUG-C 3/6】链路阻断：评估校验 / 交互交付 / 汇总 全部跳过，
                #   直接以调度规划Agent 的故障诊断收口（不生成虚假汇总报告）。
                self._skip_downstream_delivery(root, result)
                await self.bus.flush_memory_events()
                return self._finalize(result, root, started)

            # ---- 9. 交付质检（第4章 4.1 执行流程末段 + 4.6 轻量化校验） ----
            evaluator = self.agents[AGENT_EVALUATOR]
            light = await evaluator.lightweight_verify(root, reply)
            if not light.passed:
                self.logger.exception_log(
                    error_code="DELIVERY_SECURITY_BLOCKED",
                    message="最终回复未通过轻量化安全扫描，已替换为安全提示",
                    session_id=session_id, task_id=root.task_id, agent_role=AGENT_EVALUATOR,
                )
                reply = (
                    "⚠️ 本次生成的回复未通过交付前安全扫描，已按规则拦截。\n\n"
                    "- 拦截维度：安全风险校验\n"
                    "- 处理方式：不向用户输出该内容\n\n"
                    "请调整需求后重试。"
                )
            result.evaluation = light.to_dict()

            # ---- 10. 交互交付润色（不改业务结果，第4章 4.7） ----
            delivery = self.agents[AGENT_DELIVERY]
            self.stream.emit(
                root.task_id, "dispatch", session_id=session_id,
                from_agent=AGENT_EVALUATOR, to_agent=AGENT_DELIVERY,
                agent_role=AGENT_DELIVERY,
                model_label=self.active_model_label(AGENT_DELIVERY), status=root.status,
                text=(f"{AGENT_EVALUATOR} → 分发任务给 {AGENT_DELIVERY}"
                      f"（{self.active_model_label(AGENT_DELIVERY)}），任务描述：最终回复展示层润色"),
            )
            polished = await delivery.polish(root, reply, degraded_notes=self.degraded_notes or None)
            result.final_reply = polished.text
            result.plan["delivery"] = polished.to_dict()

            # ---- 11. 任务收口（模型失效时不得谎报 success，第2章 2.3 规则2） ----
            if failed_in_chain:
                # 【BUG-C 1】子任务失败 → 根任务已经是 failed，禁止改写成 success
                self.persist_task(root)
            elif model_failed:
                root.mark_failed(fail_code or ERR_MODEL_UNAVAILABLE,
                                 fail_message or "模型能力不可用")
                result.status = STATUS_FAILED
                result.error_code = root.error_code
                result.error_message = root.error_message
            else:
                root.mark_success(result=polished.text)

        except TaskLimitExceeded as exc:
            self.logger.exception_log(
                error_code=exc.code, message=str(exc),
                session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            )
            try:
                root.mark_failed(exc.code, str(exc))
            except IllegalTransition:
                root.status = STATUS_FAILED
                root.error_code = exc.code
                root.error_message = str(exc)
            result.status = STATUS_FAILED
            result.error_code = exc.code
            result.error_message = str(exc)
            # 【需求点 Bug1 规则4】依赖图类终止 → 输出结构化详细报错（逐条列出问题与处理建议）
            if exc.code == ERR_REAL_CYCLE:
                details = list(getattr(exc, "detail", None) or [])
                if not details:
                    details = [part for part in str(exc).split("；") if part.strip()]
                detail_lines = "\n".join(f"{i + 1}. {d.strip()}" for i, d in enumerate(details[:8]))
                result.final_reply = (
                    f"🛑 {DEPENDENCY_TERMINATED_TITLE}\n\n"
                    f"- 错误码：`{ERR_REAL_CYCLE}`\n"
                    f"- 判定依据：任务依赖图存在**真实死循环**"
                    f"（自引用 / 闭合拓扑环）——多个子任务互相等待，任何一步都无法开工\n"
                    f"- 已执行的修复动作：重新调用「{AGENT_DISPATCH}」重生成任务依赖图 "
                    f"{MAX_PLAN_REGENERATE_RETRIES} 次，问题依旧存在\n\n"
                    f"**具体问题清单**\n{detail_lines or '· 未取得细化问题清单，请查看后端日志 CIRCULAR_DEPENDENCY'}\n\n"
                    "**建议处理方式**\n"
                    "- 把需求拆成更明确的先后顺序，避免出现「A 要等 B、B 又要等 A」的表述；\n"
                    "- 若确为并列可选路径，请用「二选一 / 任选其一」等措辞，系统会按并列分支编排；\n"
                    "- 也可以在需求中直接给出期望的步骤清单，调度规划Agent 会按你的步骤建图。\n\n"
                    f"（防死循环硬限制保持不变：最大迭代 {MAX_TASK_ITERATIONS} 次 / "
                    f"最大超时 {MAX_TASK_TIMEOUT_SECONDS // 60} 分钟 / 循环依赖仅真实死循环才终止）"
                )
            else:
                result.final_reply = (
                    f"⚠️ 任务被系统硬限制终止：{exc}\n\n"
                    f"（防死循环规则：最大迭代 {MAX_TASK_ITERATIONS} 次 / "
                    f"最大超时 {MAX_TASK_TIMEOUT_SECONDS // 60} 分钟 / 循环依赖直接终止）"
                )

        except ApprovalError as exc:
            self.logger.exception_log(
                error_code=exc.code, message=str(exc),
                session_id=session_id, task_id=root.task_id, agent_role=AGENT_CODE,
            )
            result.status = STATUS_FAILED
            result.error_code = exc.code
            result.error_message = str(exc)
            result.final_reply = f"⚠️ 高危审批流程异常，任务已终止：{exc}"

        except SecurityViolation as exc:
            self.logger.exception_log(
                error_code=exc.code, message=str(exc),
                session_id=session_id, task_id=root.task_id, agent_role=AGENT_CODE,
            )
            root.status = STATUS_FAILED
            root.error_code = exc.code
            root.error_message = str(exc)
            result.status = STATUS_FAILED
            result.error_code = exc.code
            result.error_message = str(exc)
            result.final_reply = f"🚫 安全模块拦截了本次操作：{exc}"

        except MessageValidationError as exc:
            self.logger.exception_log(
                error_code=exc.code, message=str(exc),
                session_id=session_id, task_id=root.task_id, agent_role="Agent_Router",
            )
            root.status = STATUS_FAILED
            root.error_code = exc.code
            result.status = STATUS_FAILED
            result.error_code = exc.code
            result.error_message = str(exc)
            result.final_reply = f"⚠️ 消息协议校验失败：{exc}"

        except Exception as exc:  # noqa: BLE001 最外层兜底：绝不整体崩溃
            self.logger.exception_log(
                error_code="UNHANDLED_PIPELINE_ERROR", message=f"{type(exc).__name__}: {exc}",
                session_id=session_id, task_id=root.task_id, stack=traceback.format_exc(),
            )
            root.status = STATUS_FAILED
            root.error_code = "UNHANDLED_PIPELINE_ERROR"
            root.error_message = f"{type(exc).__name__}: {exc}"
            result.status = STATUS_FAILED
            result.error_code = "UNHANDLED_PIPELINE_ERROR"
            result.error_message = str(exc)
            result.final_reply = f"⚠️ 系统内部异常已被捕获（服务未崩溃）：{type(exc).__name__}: {exc}"

        # ---- 收口：确保记忆管理Agent 已消费完本次全部总线事件（第4章 4.5） ----
        await self.bus.flush_memory_events()
        return self._finalize(result, root, started)

    # ==================================================================
    # 【需求点 Bug2 业务流程重构】队长（调度规划Agent）业务循环
    #   ------------------------------------------------------------------
    #   严格按业务伪代码实现，但**不使用阻塞式 while 循环**：
    #     · 每一轮只做「队长取子任务 → 分派给队员 → 队员执行」；
    #     · 队员产出回传队长 → 队长校验是否满足预期目标；
    #     · 不符合预期 → 重新分派给同一队员重试（≤3 次）；
    #     · 重试耗尽 → 队长二选一：① 整个大任务标记 failed 终止；
    #                              ② 队长重新规划，换其他队员 Agent 尝试；
    #     · 命中高危操作 → 立刻中断 + 完整快照落 SQLite + 状态置 waiting_approval；
    #     · 未被队长判定完成的任务**绝不**交给交互交付Agent 输出最终报告。
    # ==================================================================
    async def _captain_loop(self, *, run: CaptainRun, result: PipelineResult,
                            root: TaskState) -> PipelineResult:
        """队长循环主入口；返回时 root.status 已是终态或 waiting_approval。"""
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        sub_states: dict[int, TaskState] = {}

        for res in run.results.values():
            state = self.load_task_state(res.task_id)
            if state is not None:
                sub_states[res.result_key] = state
        # 恢复场景：待执行项的结果键可能与结果表登记键不一致（换人重做 / 快照恢复）
        run.sync_with_work()

        while run.work:
            # ---- 防死循环闸门（第2章 2.1：超时 + 迭代上限）每轮必查 ----
            root.check_timeout()
            root.bump_iteration()
            self.persist_task(root)

            item = run.work[0]
            idx = int(item.get("index", 0))
            key = int(item.get("result_key", idx))
            stored = self.load_task_state(str(item.get("task_id") or "")) if item.get("task_id") else None
            if stored is not None:
                sub_states[key] = stored

            # ---- 审批恢复后的第一轮：被挂起的子任务结果已由审批回调写入，直接进入队长校验 ----
            #   判定口径：该子任务有待审批单，且审批单已经被裁决（manual / rejected）
            #   → 说明这是"审批挂起后恢复"的那一轮，必须先由队长校验结果，而不是重新下发执行。
            approval_pending = run.results.get(key)
            if approval_pending is not None and not (approval_pending.metadata or {}).get("delete_intent") \
                    and self._approval_already_decided(approval_pending) \
                    and str(approval_pending.approval_id or "") not in run.handled_approvals:
                if approval_pending.approval_id:
                    run.handled_approvals.append(str(approval_pending.approval_id))
                self._captain_step_emit(
                    root, "verify",
                    f"👑 {AGENT_DISPATCH} 队长业务循环第 {root.iteration} 轮："
                    f"审批结果已回灌，开始校验被挂起子任务「{approval_pending.title}」的结果")
                await self._captain_resolve_subtask(
                    run=run, root=root, result=result, item=item, res=approval_pending,
                    sub_states=sub_states, attempt=int(run.retries.get(str(key), 0)))
                continue

            # ---- 【需求点 Bug1】审批已裁决 + 待后端原生执行的删除动作 ----
            #   直接由后端 Python 执行（不再回队员、不再重复审批），
            #   执行回执写入结果后交队长校验。
            # ==============================================================
            # 【第三轮·修缺陷】必须严格区分两种"带审批单的结果"：
            #   ① 审批挂起后被恢复的那一轮（`_resume_*` 已写入审批结论）→ 由后端原生执行删除；
            #   ② 校验评估Agent 打回后**重新下发**的那一轮（审批早已消费）→ 必须让队员
            #      带校验意见重新执行，**不能**再走本分支。
            #   判定口径改用标记位（_approval_consumed / _resume_pending_intent），
            #   而不是仅凭 `approval_id`：工作项上的 delete_commit 是持久的，
            #   一旦沿用旧口径，打回后的重做会被误判成"审批恢复"，队员永远拿不到校验意见
            #   → 3 轮打回全部空转、最终任务失败（历史缺陷）。
            # ==============================================================
            pending_intent = approval_pending
            resume_pending_intent = bool(
                getattr(pending_intent, "_resume_pending_intent", False)) if pending_intent else False
            if (pending_intent is not None and resume_pending_intent
                    and (pending_intent.metadata or {}).get("delete_intent")
                    and self._approval_already_decided(pending_intent)
                    and str(pending_intent.approval_id or "") not in run.handled_approvals
                    and getattr(pending_intent, "backend_execution", None) is None):
                run.handled_approvals.append(str(pending_intent.approval_id or ""))
                self._captain_step_emit(
                    root, "verify",
                    f"👑 {AGENT_DISPATCH} 队长业务循环第 {root.iteration} 轮："
                    f"审批已裁决，交由后端原生执行删除（子任务「{pending_intent.title}」）")
                execution = await self._captain_execute_backend_delete(
                    run=run, root=root, result=result, item=item, res=pending_intent)
                if execution is None:
                    # 没有可执行的后端动作（例如该子任务的删除意图早先已被消费）：
                    # 按审批结论收口，避免在同一子任务上死循环。
                    row_now = (self.db.get_approval(pending_intent.approval_id)
                               if pending_intent.approval_id else None)
                    approved_now = bool(row_now and row_now.get("state") == APPROVAL_STATE_MANUAL)
                    pending_intent._backend_executed = True   # type: ignore[attr-defined]
                    pending_intent.status = STATUS_SUCCESS if approved_now else STATUS_FAILED
                    if not approved_now:
                        pending_intent.error = pending_intent.error or "APPROVAL_REJECTED_BY_USER"
                        pending_intent.failure_stage = "approval_rejected"
                        pending_intent.failure_reason = (
                            pending_intent.failure_reason or "高危删除未获批准，磁盘未做任何改动")
                    else:
                        pending_intent.output = (
                            f"{str(pending_intent.output or '').strip()}\n\n"
                            "（本次没有待后端执行的高危删除动作；审批结论已记录。）").strip()
                run.work = run.work[1:]
                self._captain_record_member_output(run, pending_intent)
                await self._captain_resolve_subtask(
                    run=run, root=root, result=result, item=item, res=pending_intent,
                    sub_states=sub_states, attempt=int(run.retries.get(str(key), 0)))
                continue

            # ---- 队长取下一个子任务（业务语义：队长思考规划 → 分派子任务给队员） ----
            self.stream.emit(
                root.task_id, "agent_step", session_id=root.session_id,
                agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
                status=STATUS_RUNNING, step_type="think",
                text=(f"👑 {AGENT_DISPATCH} 队长业务循环第 {root.iteration} 轮："
                      f"取出子任务「{item.get('title')}」并分派给 {item.get('agent_role')}"
                      f"（剩余待执行 {len(run.work)} 个）"),
            )

            retry_info: dict[int, dict] = {}
            attempt = int(run.retries.get(str(key), 0))
            if attempt:
                retry_info[idx] = {
                    "retry_count": attempt,
                    "previous_error": str((run.results.get(key).error if run.results.get(key) else "")
                                          or item.get("failed_reason") or "")[:800],
                    "reason": "队长校验不满足预期目标，重新分派同一队员重试",
                }

            batch, approval = await self._execute_subtasks(
                root, [item], run.image_paths,
                prebuilt={idx: (sub_states[key], run.results[key])} if key in run.results
                and key in sub_states else None,
                retry_meta=retry_info, results=run.results,
            )

            # ---- 4. 审批中断：立刻暂停、落完整快照、SSE 推送审批事件 ----
            if approval is not None:
                # 被挂起的子任务已记录在快照的 payload.current_work 里，且 sub_task_id 落库为
                # waiting_approval；恢复时按快照把"审批结论"注入该子任务的结果，直接进入
                # 队长校验环节（不重新下发执行，避免重复触发同一高危操作）。
                run.work = run.work[1:]
                root.mark_waiting_approval(
                    f"高危操作 {approval.get('operation_type')} 强制审批：{approval.get('operation_desc')}")
                self.persist_task(root)
                result.status = STATUS_WAITING_APPROVAL
                result.pending_approval = self._approval_record_view(approval["approval_id"])
                snapshot = self.save_captain_snapshot(
                    run=run, root=root, result=result, stage="waiting_approval",
                    approval=approval, current_work=item,
                )
                dispatch.think(
                    root, "exec",
                    "【业务循环中断】命中高危操作，已停止继续调用模型与思维链输出；"
                    f"完整任务快照已写入 SQLite（task_id={root.task_id}，"
                    f"已完成 {len(run.completed)} 个子任务，剩余 {len(run.work)} 个），"
                    f"任务状态 → {STATUS_WAITING_APPROVAL}，等待用户审批回调恢复",
                    level=THINK_LEVEL_WARN,
                )
                self.logger.task_log(
                    session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
                    event="captain.loop.waiting_approval", level="warn",
                    detail=(f"快照已落库：completed={len(run.completed)} remaining={len(run.work)} "
                            f"approval_id={approval.get('approval_id')} "
                            f"approval_deadline={approval.get('approval_deadline')} "
                            f"snapshot_updated_at={snapshot.get('updated_at')}"),
                )
                # 【BugA 修复后语义】拉起看门狗（幂等）：
                #   等待用户点击阶段不计时，看门狗只扫 resume_state='running'
                #   （即 POST /api/approval/submit 受理后的 30 秒执行链路计时），
                #   30 秒未收敛 → decide_resume_timeout 判定任务 failed 收口。
                self.ensure_approval_watchdog()
                result.final_reply = self._approval_pause_reply(approval, run)
                return result

            # ==========================================================
            # 【需求点 Bug1】队员结果一落地就做两件事（必须在队长一致性校验之前，
            #   因为一致性校验会重写 res.metadata）：
            #   ① Agent 输出纠偏：文本里宣称"已删除 X"而后端没有任何真实回执 → 强制纠偏；
            #   ② 后端硬编码高危接管：产出待删候选清单 + 删除型任务 → 登记删除意图，
            #      下一轮由硬编码高危门禁强制审批（Agent 不参与任何执行）。
            # ==========================================================
            batch_res = batch[-1] if batch else None

            # ---- 后端硬编码高危接管：待删清单已确认 → 先插入审批（不允许被"已完成"吞掉） ----
            if batch_res is not None and batch_res.status == STATUS_SUCCESS \
                    and (batch_res.metadata or {}).get("delete_intent"):
                # 注意：当前工作项此刻仍在队首，因此按**对象身份**排除自身，
                # 只判断"除自己以外"是否还有同一子任务的待执行项。
                already = any(w is not item and int(w.get("index", -1)) == int(batch_res.index)
                              for w in run.work)
                if not already:
                    run.work.insert(0, {**item, "delete_commit":
                                        (batch_res.metadata or {}).get("delete_intent") or {}})

            res = batch_res
            run.work = run.work[1:]

            # ---- 审批通过后的第一件事：后端 Python 原生执行磁盘删除 + 采集回执 ----
            if res is not None and res.status == STATUS_SUCCESS \
                    and (res.metadata or {}).get("delete_intent"):
                await self._captain_execute_backend_delete(
                    run=run, root=root, result=result, item=item, res=res)

            # ---- 已跑完的子任务进入"已完成列表"（快照据此恢复上下文） ----
            if res is not None:
                self._captain_record_member_output(run, res)
                # ---- 队员结果回传队长：队长校验子任务是否满足预期目标 → 决定后续走向 ----
                await self._captain_resolve_subtask(
                    run=run, root=root, result=result, item=item, res=res,
                    sub_states=sub_states, attempt=attempt)
                continue

        # ==============================================================
        # 业务循环收口：只有队长确认用户原始任务全部完成，才允许进入交付
        # ==============================================================
        return await self._captain_finish(run=run, result=result, root=root)

    # ------------------------------------------------------------------
    # 队长：登记队员产出（快照的"已完成子任务列表"与"消息上下文"）
    # ------------------------------------------------------------------
    def _approval_already_decided(self, res: SubtaskResult) -> bool:
        """该子任务是否处于"审批已裁决、等待队长校验结果"的状态。

        判定依据是审批记录的**真实状态**（manual / rejected），
        而不是子任务自身状态 —— 恢复时父、子任务状态可能刚被改写，
        只有审批记录的终态才是可靠的"审批已走完"信号。
        """
        approval_id = str(getattr(res, "approval_id", "") or "")
        if not approval_id:
            return False
        try:
            row = self.db.get_approval(approval_id)
        except Exception:  # noqa: BLE001 查询失败按"未裁决"处理（宁可重跑也不误判）
            return False
        if not row:
            return False
        return str(row.get("state") or "") in (APPROVAL_STATE_MANUAL, APPROVAL_STATE_REJECTED)

    @staticmethod
    def _captain_record_member_output(run: CaptainRun, res: SubtaskResult) -> None:
        """登记队员产出：同一子任务（下标）只保留**最后一次**产出。

        队长判定重试时同一子任务会被反复执行，若按追加登记，
        "已完成子任务列表"里会残留上一轮的失败记录（前端展示与故障诊断会被带偏）。
        """
        entry = {
            "index": res.index, "title": res.title, "agent_role": res.agent_role,
            "status": res.status, "task_id": res.task_id,
            "output": str(res.output or "")[:4000],
            "error": res.error, "approval_id": res.approval_id,
            "retry_count": res.retry_count,
            "failure_stage": res.failure_stage,
            "failure_reason": res.failure_reason,
            "failure_kind": res.failure_kind,
            "retries_exhausted": res.retries_exhausted,
            "blocked_by": list(res.blocked_by),
            "think_steps": list(res.think_steps),
            # 候选清单 / 原生扫描摘要等结构化产出随结果回传（队长一致性与前端展示都要用）
            "metadata": res.metadata,
            "verdict": res.verdict,
        }
        run.completed = [c for c in run.completed if int(c.get("index", -1)) != int(res.index)]
        run.completed.append(entry)
        run.messages.append({
            "role": "member", "agent": res.agent_role, "title": res.title,
            "status": res.status, "content": str(res.output or res.error)[:2000],
            "at": time.time(),
        })

    # ==================================================================
    # 【业务流程重构 · Bug1 修复后】最终流转链路
    #   ------------------------------------------------------------------
    #   队员Agent 执行完毕
    #     → ① 校验评估Agent【自动校验】（不需要前端弹窗）
    #           · 校验代码正确性 / 逻辑 / 输出文本格式；
    #           · 发现错误 → 打回**原队员Agent**修改，每个子任务最多 3 轮
    #             （计数持久化于 task_snapshots.code_revise_count / fix_rounds）；
    #           · 3 轮仍不通过 → **唯一**上交调度规划Agent（队长）的入口：
    #             换人重规划（reallocations ≤3，耗尽则整个任务 failed）；
    #           · 校验通过 → 子任务直接收口，**不回传队长**（回流已删除），
    #             全部完成后直达 ②
    #     → ② 交互交付Agent（用户可读最终回答）+ 记忆管理Agent（上下文持久化）
    #   注：高危人工审批分支完全独立（队员执行途中触发，前端弹窗 + 快照恢复），
    #       与这里的"自动校验"是两套互不混淆的流程。
    # ==================================================================
    async def _captain_resolve_subtask(self, *, run: CaptainRun, root: TaskState,
                                       result: PipelineResult, item: dict,
                                       res: SubtaskResult,
                                       sub_states: dict[int, TaskState],
                                       attempt: int) -> None:
        """子任务落地入口：队员执行完毕 → 校验评估Agent 自动校验（不再回传队长复核）。"""
        idx = int(res.index)
        key = int(res.result_key)
        # 审批结论已消费：后续轮次按正常"重新下发执行"处理（避免死循环重入校验）
        res._approval_consumed = True  # type: ignore[attr-defined]
        # ==================================================================
        # 分流规则（不允许混淆两套流程）：
        #   · 队员**执行成功**（有产出）→ 1️⃣ 校验评估Agent 自动校验
        #     → 通过则子任务直接收口，继续下一个子任务；全部完成后
        #       交交互交付Agent + 记忆管理Agent（不经过队长二次需求判断）；
        #   · 队员**执行失败 / 无产出**（含高危审批被拒绝）→ 走"失败子任务"处理
        #     （同一队员重试 ≤3 → 队长二选一：换人重规划 / 终止整个大任务）。
        #     失败结果没有任何"质量"可校验，因此不经校验评估Agent。
        # ==================================================================
        if res.status != STATUS_SUCCESS or self._output_is_empty(res):
            await self._captain_handle_failed_subtask(
                run=run, root=root, result=result, item=item, res=res,
                sub_states=sub_states, attempt=attempt,
                reason=(str(res.failure_reason or res.error)
                        or "队员上报成功但产出为空，无法证明任务已完成"))
            return
        self._captain_step_emit(
            root, "verify",
            f"👑 {AGENT_DISPATCH} 队长业务循环第 {root.iteration} 轮：子任务「{res.title}」"
            f"由 {res.agent_role} 执行完毕 → 按新流转直接交 {AGENT_EVALUATOR} 自动校验"
            f"（不再由队长即时复核质量）")
        await self._captain_evaluate_subtask(
            run=run, root=root, result=result, item=item, res=res,
            sub_states=sub_states, attempt=attempt)

    @staticmethod
    def _output_is_empty(res: SubtaskResult) -> bool:
        """产出是否为空（无法证明任务完成 → 按不达预期处理）。"""
        text = str(res.output or "").strip()
        return len(text) < 2 or text in ("（无输出）", "None", "null")

    # ==================================================================
    # 队员执行失败 / 产出为空 → 失败子任务的既有重试闸门（队长二选一）
    #   注：本段不涉及"质量校验"，只处理"执行没成功"这一客观结果；
    #       与"校验评估Agent 自动校验"（质量层面）是两条互不混淆的路径。
    # ==================================================================
    async def _captain_handle_failed_subtask(self, *, run: CaptainRun, root: TaskState,
                                             result: PipelineResult, item: dict,
                                             res: SubtaskResult,
                                             sub_states: dict[int, TaskState],
                                             attempt: int, reason: str) -> None:
        """失败子任务：同一队员重试 ≤ MAX_SUBTASK_RETRIES → 队长二选一（换人 / 终止）。"""
        key = int(res.result_key)
        idx = int(res.index)
        if res.status == STATUS_SUCCESS and self._output_is_empty(res):
            res.status = STATUS_FAILED
            res.error = res.error or "子任务上报成功但产出为空（按不达预期处理）"
            res.failure_stage = res.failure_stage or "empty_output"
        res.failure_reason = res.failure_reason or reason

        used = int(run.retries.get(str(key), attempt))
        if used < MAX_SUBTASK_RETRIES:
            run.retries[str(key)] = used + 1
            reason_text = str(res.error or res.failure_reason or "未达预期")[:200]
            state = sub_states.get(key) or self.load_task_state(res.task_id)
            if state is not None:
                state.rearm_for_redispatch(retry_count=used + 1)
                self.persist_task(state)
                sub_states[key] = state
            res.status = STATUS_PENDING
            res.output = ""
            res.error = ""
            res.failure_stage = ""
            res.failure_reason = ""
            res.retries_exhausted = False
            # 新一轮重试会生成**新的**审批单，因此清空上一轮的审批单号
            res.approval_id = ""
            run.fix_rounds.pop(str(key), None)      # 重新执行 → 校验修复轮次重新计
            item["failed_reason"] = reason_text
            run.work.insert(0, item)
            self._captain_step_emit(
                root, "retry",
                f"🔁 队长判定子任务「{res.title}」执行失败（{reason_text}），"
                f"第 {used + 1}/{MAX_SUBTASK_RETRIES} 次重新分派给 {res.agent_role} 重试",
                level=THINK_LEVEL_WARN)
            self.save_captain_snapshot(run=run, root=root, result=result, stage="running")
            return

        # ---- 重试耗尽 → 队长二选一：① 换人重规划 ② 终止整个大任务 ----
        verdict = {"action": "terminate", "reason": reason,
                   "replace_agent": "", "source": "retry_exhausted"}
        decision = await self._captain_exhausted_decision(
            run=run, root=root, item=item, res=res, verdict=verdict)
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        state_for_exhaust = sub_states.get(key) or self.load_task_state(res.task_id)
        if state_for_exhaust is not None:
            res.retry_count = max(int(res.retry_count or 0), int(run.retries.get(str(key), 0)))
            self._mark_retry_exhausted(root, state_for_exhaust, res, dispatch)
        res.retries_exhausted = True
        if decision["action"] == "reassign" and run.reallocations < MAX_REQUIREMENT_REALLOCATIONS:
            run.reallocations += 1
            new_item = await self._captain_replan_for_failure(
                run=run, root=root, item=item, res=res, decision=decision)
            if new_item:
                run.retries.pop(str(key), None)
                run.work.insert(0, new_item)
                self._captain_step_emit(
                    root, "reassign",
                    f"🧭 队长决策②：同一队员重试 {MAX_SUBTASK_RETRIES} 次仍失败，"
                    f"重新规划改由 {new_item.get('agent_role')} 尝试子任务"
                    f"「{new_item.get('title')}」（重新分配 {run.reallocations}/"
                    f"{MAX_REQUIREMENT_REALLOCATIONS} 次）",
                    level=THINK_LEVEL_WARN)
                self.save_captain_snapshot(run=run, root=root, result=result, stage="running")
                return

        res.failure_stage = res.failure_stage or "retry_exhausted"
        await self._captain_terminate_task(
            run=run, root=root, result=result, res=res, item=item,
            reason=(f"子任务「{res.title}」重试 "
                    f"{int(run.retries.get(str(key), MAX_SUBTASK_RETRIES))}/"
                    f"{MAX_SUBTASK_RETRIES} 次仍失败，且换人重规划不可用"
                    f"（{str(decision.get('reason') or '队长判定')[:120]}）"),
            stage_label="retry_exhausted")

    # ------------------------------------------------------------------
    # ① 校验评估Agent 自动校验（最多打回原队员修改 3 轮）
    # ------------------------------------------------------------------
    async def _captain_evaluate_subtask(self, *, run: CaptainRun, root: TaskState,
                                        result: PipelineResult, item: dict,
                                        res: SubtaskResult,
                                        sub_states: dict[int, TaskState],
                                        attempt: int) -> None:
        """队员产出 → 校验评估Agent 校验：通过则子任务直接收口（不回传队长）；不通过则打回原队员修改。"""
        key = int(res.result_key)
        idx = int(res.index)
        state = sub_states.get(key) or self.load_task_state(res.task_id)
        verdict = await self._evaluate_member_output(root=root, res=res, item=item)

        # ---- 校验通过 → 子任务直接收口（新流转：不再回传调度规划Agent 复核） ----
        #   【Bug1 修复】删除"评估校验Agent → 调度规划Agent"回流：
        #   校验通过后不再做队长二次需求判断，直接收口该子任务并继续下一个；
        #   全部子任务完成后由 _captain_finish 直达交互交付Agent + 记忆管理Agent。
        if verdict.get("verdict") == "pass":
            run.evaluator_state = {
                "task_id": res.task_id, "index": idx, "verdict": "pass",
                "score": int(verdict.get("score") or 0),
                "checks": list(verdict.get("checks") or []),
                "reason": str(verdict.get("reason") or "")[:400],
                "at": time.time(),
            }
            run.fix_rounds.pop(str(key), None)      # 本轮通过 → 修复计数归零
            run.retries.pop(str(key), None)
            item["failed_reason"] = ""
            item.pop("evaluator_fix", None)
            self._captain_step_emit(
                root, "pass",
                f"✅ {AGENT_EVALUATOR} 校验通过（得分 {verdict.get('score')}）："
                f"子任务「{res.title}」的代码正确性 / 逻辑 / 输出格式均合规"
                f" → 子任务收口完成，不回传 {AGENT_DISPATCH}；"
                f"全部子任务完成后直达 {AGENT_DELIVERY} 生成最终回复")
            self.save_captain_snapshot(run=run, root=root, result=result, stage="running")
            return

        # ---- 校验不通过 → 打回**原队员**修改（最多 MAX_EVALUATOR_FIX_ROUNDS 轮） ----
        used = int(run.fix_rounds.get(str(key), 0))
        issues = list(verdict.get("issues") or [])
        suggestions = "；".join(
            f"[{i.get('dimension')}/{i.get('severity')}] {i.get('detail')}"
            + (f" → 建议：{i.get('suggestion')}" if i.get("suggestion") else "")
            for i in issues[:6]) or str(verdict.get("reason") or "未给出具体问题")
        run.evaluator_state = {
            "task_id": res.task_id, "index": idx, "verdict": "reject",
            "score": int(verdict.get("score") or 0), "issues": issues[:10],
            "reason": str(verdict.get("reason") or "")[:400], "at": time.time(),
        }

        if used < MAX_EVALUATOR_FIX_ROUNDS:
            run.fix_rounds[str(key)] = used + 1
            # 打回原队员：复位该子任务为可再次分发，并把校验问题作为修改指令下发
            if state is not None:
                state.rearm_for_redispatch(retry_count=int(state.retry_count or 0))
                self.persist_task(state)
                sub_states[key] = state
            prev_output = str(res.output or "")[:2000]
            res.status = STATUS_PENDING
            res.output = ""
            res.error = ""
            res.failure_stage = ""
            res.failure_reason = ""
            res.retries_exhausted = False
            res.approval_id = ""            # 新一轮可能重新触发审批，清空旧审批单号
            run.code_edits.append({
                "index": idx, "title": res.title, "agent_role": res.agent_role,
                "round": used + 1, "issues": issues[:6],
                "before_digest": prev_output[:600], "at": time.time(),
            })
            item["failed_reason"] = f"{AGENT_EVALUATOR} 第 {used + 1} 轮打回：{suggestions[:300]}"
            item["evaluator_fix"] = {
                "round": used + 1, "issues": issues[:6],
                "suggestion": suggestions[:1200],
                "previous_output": prev_output,
            }
            # ==========================================================
            # 【第三轮·修缺陷】清掉上一轮遗留的"待执行删除提交"标记：
            #   delete_commit 是本子任务审批阶段写入的持久字段；若重做轮仍带着它，
            #   `_execute_subtasks` 会在调用队员**之前**直接命中硬编码高危门禁
            #   → 又生成一张审批单，队员根本收不到校验修改意见（3 轮打回空转）。
            #   重做轮必须真正让队员重新执行；若重做后仍产出待删清单，
            #   会在队长循环里重新登记 delete_commit 并生成新的审批单（逐条审批）。
            # ==========================================================
            item.pop("delete_commit", None)
            if isinstance(res.metadata, dict) and "delete_intent" in res.metadata:
                res.metadata = {k: v for k, v in res.metadata.items() if k != "delete_intent"}
            run.work.insert(0, item)
            self._captain_step_emit(
                root, "retry",
                f"🔁 {AGENT_EVALUATOR} 校验不通过（第 {used + 1}/{MAX_EVALUATOR_FIX_ROUNDS} 轮）："
                f"打回原队员 {res.agent_role} 修改子任务「{res.title}」｜问题：{suggestions[:200]}",
                level=THINK_LEVEL_WARN)
            self.save_captain_snapshot(run=run, root=root, result=result, stage="fixing")
            return

        # ---- 3 轮修改仍不通过 → 上交队长处理（换人重规划 / 终止，见队长决策） ----
        run.retries.pop(str(key), None)
        res.status = STATUS_FAILED
        res.error = res.error or f"{AGENT_EVALUATOR} 校验 {MAX_EVALUATOR_FIX_ROUNDS} 轮仍未通过"
        res.failure_stage = "evaluator_rejected"
        res.failure_reason = (f"校验评估Agent 连续 {MAX_EVALUATOR_FIX_ROUNDS} 轮判定不通过："
                              f"{suggestions[:400]}")
        res.retry_count = max(int(res.retry_count or 0), MAX_EVALUATOR_FIX_ROUNDS)
        self._captain_step_emit(
            root, "verify",
            f"⚠️ {AGENT_EVALUATOR} 已打回修改 {MAX_EVALUATOR_FIX_ROUNDS} 轮仍不通过"
            f" → 按流程上交 {AGENT_DISPATCH} 队长处理（重新分配 / 终止整个任务）",
            level=THINK_LEVEL_ERROR)
        await self._captain_handle_evaluator_exhausted(
            run=run, root=root, result=result, item=item, res=res,
            sub_states=sub_states, verdict=verdict)

    async def _captain_handle_evaluator_exhausted(self, *, run: CaptainRun, root: TaskState,
                                                  result: PipelineResult, item: dict,
                                                  res: SubtaskResult,
                                                  sub_states: dict[int, TaskState],
                                                  verdict: dict) -> None:
        """校验评估Agent 打回 3 轮仍不通过 → 队长决定的后续走向。

        需求：交给调度规划Agent 处理（重新分配任务 / 终止）。这里复用队长既有的
        "换人重规划"能力，但受**需求重分配 3 次**上限约束；额度耗尽 → 整个任务失败终止。
        """
        if run.reallocations < MAX_REQUIREMENT_REALLOCATIONS:
            run.reallocations += 1
            new_item = await self._captain_replan_for_failure(
                run=run, root=root, item=item, res=res,
                decision={"action": "reassign",
                          "reason": f"校验评估Agent 打回 {MAX_EVALUATOR_FIX_ROUNDS} 轮仍未通过",
                          "replace_agent": "",
                          "instruction": ""})
            if new_item:
                run.work.insert(0, new_item)
                self._captain_step_emit(
                    root, "reassign",
                    f"🧭 {AGENT_DISPATCH} 队长决策：校验评估Agent 打回 {MAX_EVALUATOR_FIX_ROUNDS} 轮"
                    f"仍未通过 → 重新分配子任务给 {new_item.get('agent_role')}"
                    f"（需求重分配 {run.reallocations}/{MAX_REQUIREMENT_REALLOCATIONS} 次）",
                    level=THINK_LEVEL_WARN)
                self.save_captain_snapshot(run=run, root=root, result=result, stage="running")
                return
        await self._captain_terminate_task(
            run=run, root=root, result=result, res=res, item=item,
            reason=(f"子任务「{res.title}」经校验评估Agent {MAX_EVALUATOR_FIX_ROUNDS} 轮打回"
                    f"仍不合格，且需求重分配额度已用尽"
                    f"（{run.reallocations}/{MAX_REQUIREMENT_REALLOCATIONS}）"),
            stage_label="evaluator_exhausted")

    # ------------------------------------------------------------------
    # 【Bug1 修复 · 已删除】原 ②「调度规划Agent（队长）需求对齐审批」
    #   （_captain_requirement_review / _captain_requirement_decision）整段下线：
    #   校验评估Agent 通过后不再回传队长做二次需求判断，
    #   否则每个子任务都会反复进入队长规划逻辑（重复分派 / 重复调用调度模型）。
    #   回到队长的唯一入口 = 校验打回 3 轮耗尽（_captain_handle_evaluator_exhausted）。
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # 校验评估Agent 调用（本地硬校验 + 模型校验）
    # ------------------------------------------------------------------
    async def _evaluate_member_output(self, *, root: TaskState, res: SubtaskResult,
                                      item: dict) -> dict:
        """调用校验评估Agent 校验队员产出 → 归一为 {verdict, score, issues, reason, checks}。

        【第三轮】直接使用校验评估Agent 的模型通道 + 专用提示词 EVALUATOR_SUBTASK_PROMPT，
        校验维度严格按需求：代码正确性 / 逻辑 / 输出文本格式合规；
        同时叠加本地硬校验（语法 / 越界路径 / 密钥泄露等，不消耗 Token 且不可被模型放行）。
        """
        evaluator = self.agents[AGENT_EVALUATOR]
        state = self.load_task_state(res.task_id) or root
        content = str(res.output or "")
        role = str(res.agent_role or "")
        kind = "doc_summary" if role == AGENT_DOC else "code"
        data: dict[str, Any] = {}
        try:
            # ---- AI 校验：按校验维度给结论 + 可直接照改的修改建议 ----
            digest = (
                f"【子任务标题】{item.get('title') or res.title}\n"
                f"【子任务指令（判断格式是否合规的基准）】"
                f"{str(item.get('instruction') or '')[:1200]}\n"
                f"【执行 Agent】{role}\n"
                f"【产出内容类型】{'文档摘要' if kind == 'doc_summary' else '代码/结构化产物'}\n"
                f"【产出内容】\n{content[:6000]}\n\n"
                f"【本次是该子任务的第 {int(res.retry_count or 0)} 次重试】"
            )
            response = await evaluator.call_model(
                state,
                [{"role": "system", "content": EVALUATOR_SUBTASK_PROMPT},
                 {"role": "user", "content": digest}],
                temperature=AGENT_TEMPERATURES.get(AGENT_EVALUATOR, 0.2),
                max_tokens=1200, expect_json=True,
                step_label=f"校验评估Agent 校验子任务「{res.title}」的产出（代码正确性/逻辑/格式）",
            )
            parsed = _extract_json(response.text)
            data = parsed if isinstance(parsed, dict) else {}
            data.setdefault("checks", ["model:logic", "model:format"])
            # ---- 本地硬校验（不消耗 Token，且不可被模型"放行"） ----
            scan = evaluator.safety_scan(content)
            data.setdefault("checks", []).append("security:local")
            if not scan.get("passed"):
                data["verdict"] = "reject"
                data.setdefault("issues", []).extend([
                    {"dimension": "security", "severity": f.get("severity", "high"),
                     "detail": f.get("detail", ""), "suggestion": "移除该内容后重新输出"}
                    for f in scan.get("findings", [])])
        except Exception as exc:  # noqa: BLE001 校验链路异常不得让任务静默死掉
            self.logger.exception_log(
                error_code="EVALUATOR_VERIFY_FAILED",
                message=f"校验评估Agent 校验失败（按『后端保守规则』放行收口）：{exc}",
                session_id=root.session_id, task_id=res.task_id, agent_role=AGENT_EVALUATOR,
            )
            return {"verdict": "pass", "score": 80, "issues": [],
                    "reason": f"校验链路异常（已降级放行收口）：{exc}",
                    "checks": ["evaluator:degraded"]}
        if not isinstance(data, dict):
            data = {"verdict": "pass", "score": 80, "issues": [], "reason": "校验结果不可解析"}
        data.setdefault("verdict", "pass")
        data.setdefault("score", 80)
        data.setdefault("issues", [])
        # 校验不通过但没有任何具体问题时，补一条可执行的默认问题，避免"打回却不给建议"
        if data["verdict"] != "pass" and not data["issues"]:
            data["issues"] = [{
                "dimension": "format", "severity": "mid",
                "detail": "校验评估Agent 未给出具体问题明细",
                "suggestion": "请按子任务指令补齐输出内容与结论，并保证格式合规",
            }]
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_EVALUATOR,
            model_label=self.active_model_label(AGENT_EVALUATOR),
            status=root.status, step_type="verify",
            text=(f"🧪 {AGENT_EVALUATOR} 校验子任务「{res.title}」（{res.agent_role} 产出）："
                  f"verdict={data['verdict']}｜score={data.get('score')}｜"
                  f"问题 {len(data['issues'])} 条"),
            subtask_task_id=res.task_id,
        )
        return data

    async def _captain_terminate_task(self, *, run: CaptainRun, root: TaskState,
                                      result: PipelineResult, res: SubtaskResult,
                                      item: dict, reason: str, stage_label: str) -> None:
        """统一"整个大任务失败终止"收口（重试/校验/需求三类耗尽共用）。"""
        idx = int(res.index)
        res.status = STATUS_FAILED
        res.retries_exhausted = True
        res.failure_stage = res.failure_stage or stage_label
        res.failure_reason = res.failure_reason or reason
        run.completed = [c for c in run.completed if int(c.get("index", -1)) != idx]
        run.completed.append({
            "index": idx, "title": res.title, "agent_role": res.agent_role,
            "status": STATUS_FAILED, "task_id": res.task_id,
            "output": str(res.output or "")[:4000], "error": res.error or reason,
            "retry_count": int(res.retry_count or 0),
            "failure_stage": res.failure_stage,
            "retries_exhausted": True,
            "captain_decision": reason,
        })
        for rest in run.work:
            run.completed.append({
                "index": int(rest.get("index", -1)),
                "title": str(rest.get("title") or ""),
                "agent_role": str(rest.get("agent_role") or ""),
                "status": STATUS_FAILED, "task_id": str(rest.get("task_id") or ""),
                "output": "",
                "error": "队长已终止整个大任务，本子任务不再下发（链路阻断）",
                "failure_stage": "dependency_blocked",
                "failure_reason": f"上游失败并触发队长终止：{reason}",
                "blocked_by": [idx],
                "captain_decision": reason,
            })
        run.work = []
        self._captain_step_emit(
            root, "terminate",
            f"🛑 {AGENT_DISPATCH} 队长决策：{reason} → 整个大任务判定 failed 并终止业务循环",
            level=THINK_LEVEL_ERROR)
        self._cancel_pending_approvals_of_task(root.task_id)
        self.save_captain_snapshot(run=run, root=root, result=result, stage="failed")

    async def _captain_build_reassign_item(self, *, run: CaptainRun, root: TaskState,
                                           item: dict, res: SubtaskResult,
                                           replace_agent: str, instruction: str) -> dict | None:
        """按队长给的新指令构造"改派"工作项（新 result_key，不覆盖原结果对象）。"""
        agent_role = replace_agent if replace_agent in AGENT_ROLES else res.agent_role
        next_key = (max([int(k) for k in run.results.keys()] or [0]) + 1)
        new_item = {
            "index": int(res.index),
            "title": str(res.title),
            "agent_role": agent_role,
            "instruction": instruction,
            "depend_on": list(item.get("depend_on") or []),
            "parallel_group": item.get("parallel_group", 0),
            "result_key": next_key,
            "redispatch_reason": f"队长重新分配任务（第 {run.reallocations} 次重新分配）",
        }
        if run.work_index(int(res.index)) is None:
            self._captain_step_emit(
                root, "dispatch",
                f"📤 {AGENT_DISPATCH} 重新分配子任务「{res.title}」→ {agent_role}"
                f"（新工作项键 {next_key}）")
        return new_item


    # ------------------------------------------------------------------
    # 队长：任务终止 → 作废该任务下仍未裁决的审批单（避免长期悬挂阻塞新消息）
    # ------------------------------------------------------------------
    def _cancel_pending_approvals_of_task(self, root_task_id: str) -> int:
        try:
            child_ids = [str(r.get("task_id") or "") for r in
                         self.db.list_child_tasks(root_task_id)]
            return self.approval_center.auto_cancel_pending_approvals(
                [root_task_id] + child_ids,
                reason="队长判定整个大任务终止，未裁决的高危审批单自动作废（未执行任何操作）")
        except Exception as exc:  # noqa: BLE001 作废失败不得影响终止收口
            self.logger.exception_log(
                error_code="APPROVAL_AUTO_CANCEL_FAILED",
                message=f"待审批记录自动作废失败（已降级）：{exc}",
                task_id=root_task_id, agent_role=AGENT_DISPATCH,
            )
            return 0

    # ------------------------------------------------------------------
    # 【业务流程重构 · Bug1 修复】旧流程的「队长即时复核子任务质量」
    # （_captain_verify_subtask）与「队长需求对齐审批」
    # （_captain_requirement_review / _captain_requirement_decision）均已下线：
    #   队员产出改由 校验评估Agent 自动校验，通过即收口（不回传队长）；
    #   执行失败的子任务走 _captain_handle_failed_subtask 的既有重试闸门。
    # ------------------------------------------------------------------
    # 队长：重试耗尽的二选一决策细化（② 重新规划换队员）
    # ------------------------------------------------------------------
    async def _captain_exhausted_decision(self, *, run: CaptainRun, root: TaskState,
                                          item: dict, res: SubtaskResult,
                                          verdict: dict) -> dict:
        """重试耗尽后请队长显式二选一（终止整个任务 / 换其他队员重新规划）。

        注意：本方法只在**重试额度已耗尽**时调用，因此只接受
        terminate / reassign 两种取值；队长若返回其它取值（例如 retry），
        说明它没意识到额度已尽 → 一律按"终止整个大任务"保守收口。
        """
        if verdict.get("action") == "reassign":
            return verdict
        if verdict.get("action") == "terminate":
            return verdict
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        prompt = (
            f"【用户原始需求】\n{run.user_input[:1000]}\n\n"
            f"【失败子任务】{res.title}（原执行 Agent：{res.agent_role}）\n"
            f"【已重试】{MAX_SUBTASK_RETRIES}/{MAX_SUBTASK_RETRIES} 次，仍未达预期\n"
            f"【失败原因】{(res.failure_reason or res.error or '未知')[:800]}\n\n"
            "请做二选一决策：\n"
            "① terminate：整个大任务标记 failed 并终止（当失败不可挽回、或换人也没有意义时）；\n"
            "② reassign ：由你重新规划，改派**另一个**队员 Agent 尝试完成该子任务。\n"
            "只输出 JSON：{\"action\":\"terminate|reassign\",\"reason\":\"...\","
            "\"replace_agent\":\"换派的队员角色或空\",\"instruction\":\"换派后的完整子任务指令\"}"
        )
        try:
            response = await dispatch.call_model(
                root, [{"role": "system", "content": CAPTAIN_EXHAUSTED_PROMPT},
                       {"role": "user", "content": prompt}],
                temperature=AGENT_TEMPERATURES[AGENT_DISPATCH], max_tokens=1200,
                expect_json=True, step_label="队长重试耗尽二选一决策",
            )
            decision = self._parse_captain_decision(response.text, {
                "action": "terminate", "reason": "队长未给出换人方案，按终止处理",
                "replace_agent": "", "source": "fallback"})
        except Exception as exc:  # noqa: BLE001 调用失败 → 保守终止
            self.logger.exception_log(
                error_code="CAPTAIN_EXHAUSTED_DECISION_FAILED",
                message=f"队长二选一决策失败（保守按终止处理）：{exc}",
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            )
            decision = {"action": "terminate", "reason": "队长决策模型不可用，保守终止",
                        "replace_agent": "", "source": "fallback"}
        if decision.get("action") not in ("terminate", "reassign"):
            decision["action"] = "terminate"
        return decision

    async def _captain_replan_for_failure(self, *, run: CaptainRun, root: TaskState,
                                          item: dict, res: SubtaskResult,
                                          decision: dict) -> dict | None:
        """队长决策②：重新规划并改派其他队员（返回新的工作项，失败返回 None）。"""
        replace = str(decision.get("replace_agent") or "").strip()
        valid_roles = {AGENT_CODE, AGENT_DOC, AGENT_VISION, AGENT_MEMORY,
                       AGENT_EVALUATOR, AGENT_DELIVERY}
        if replace not in valid_roles or replace == res.agent_role:
            replace = AGENT_CODE if res.agent_role != AGENT_CODE else AGENT_DOC
        instruction = str(decision.get("instruction") or "").strip() or (
            f"前一位队员（{res.agent_role}）连续 {MAX_SUBTASK_RETRIES} 次未能完成该子任务，"
            f"现由你（{replace}）接手。\n"
            f"原始子任务：{item.get('title')}\n"
            f"原始指令：{str(item.get('instruction') or '')[:1200]}\n"
            f"此前失败原因：{(res.failure_reason or res.error or '未知')[:600]}\n"
            f"用户原始需求：{run.user_input[:600]}\n"
            "请换一种可行思路完成该子任务，并给出可验证的结果。"
        )
        new_index = max([int(w.get("index", 0)) for w in run.work]
                        + [int(r.index) for r in run.results.values()] + [int(item.get("index", 0))]) + 1
        # 换人重做会产生一份新的结果：用新键存入共享结果表，避免覆盖原计划同下标子任务的结果
        new_key = max([int(w.get("result_key", w.get("index", 0))) for w in run.work]
                      + list(run.results.keys()) + [int(res.result_key)]) + 1
        self.stream.emit(
            root.task_id, "dispatch", session_id=root.session_id,
            from_agent=AGENT_DISPATCH, to_agent=replace, agent_role=replace,
            model_label=self.active_model_label(replace), status=STATUS_RUNNING,
            title=item.get("title") or "", text=(
                f"{AGENT_DISPATCH}（{self.active_model_label(AGENT_DISPATCH)}） → "
                f"重新规划改派任务给 {replace}（{self.active_model_label(replace)}），"
                f"任务描述：接手子任务「{item.get('title')}」"),
            subtask_index=new_index, instruction=instruction[:1500],
        )
        return {
            "index": new_index,
            "result_key": new_key,
            "title": f"{item.get('title')}（改派 {replace} 重做）",
            "agent_role": replace,
            "instruction": instruction,
            "depend_on": [],
            "parallel_group": 0,
            "reassigned_from": res.agent_role,
        }

    @staticmethod
    def _parse_captain_decision(raw_text: str, fallback: dict) -> dict:
        """解析队长裁决 JSON（绝不因解析失败而假成功）。"""
        try:
            data = _extract_json(raw_text)
        except Exception:  # noqa: BLE001 解析失败 → 用兜底决策
            return dict(fallback)
        if not isinstance(data, dict):
            return dict(fallback)
        action = str(data.get("action") or data.get("decision") or "").strip().lower()
        if action not in ("pass", "retry", "terminate", "reassign"):
            action = str(fallback.get("action") or "retry")
        return {
            "action": action,
            "reason": str(data.get("reason") or data.get("reasoning") or fallback.get("reason") or "")[:600],
            "replace_agent": str(data.get("replace_agent") or "").strip(),
            "instruction": str(data.get("instruction") or "").strip(),
            "source": "captain_model",
        }

    def _captain_step_emit(self, root: TaskState, action: str, text: str,
                           level: str = THINK_LEVEL_INFO) -> None:
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=root.status, step_type="think", level=level,
            text=text, captain_action=action,
        )

    # ------------------------------------------------------------------
    # 队长循环收口：只有队长确认全部完成，才交付交互交付Agent
    # ------------------------------------------------------------------
    async def _captain_finish(self, *, run: CaptainRun, result: PipelineResult,
                              root: TaskState) -> PipelineResult:
        successful = [res for res in run.results.values() if res.status == STATUS_SUCCESS]
        failures = [res for res in run.results.values()
                    if res.status in (STATUS_FAILED, STATUS_WAITING_APPROVAL)]

        blocked = bool(failures) or bool(run.work)
        if blocked:
            # 【边界约束2】未被队长判定完成 → 绝不调用交互交付Agent 输出最终报告
            reason = ("仍有子任务未完成（" + "、".join(
                str(w.get("title") or w.get("index")) for w in run.work[:4]) + "）"
                if run.work else "存在失败子任务")
            result.status = STATUS_FAILED
            # 错误码口径与历史链路一致：重试耗尽 → SUBTASK_RETRY_EXHAUSTED，其它失败 → SUBTASK_FAILED
            failed_res = [r for r in failures if r.status == STATUS_FAILED]
            exhausted = [r for r in failed_res if r.retries_exhausted]
            result.error_code = root.error_code or (
                ERR_SUBTASK_RETRY_EXHAUSTED if exhausted else ERR_SUBTASK_FAILED)
            result.error_message = root.error_message or f"任务未全部完成：{reason}"
            fault = self._build_fault_report(
                root, run.completed_results(), [r for r in failures if r.status == STATUS_FAILED])
            result.plan["fault_report"] = fault
            result.plan["captain_loop"] = {
                "completed": len(run.completed),
                "remaining": len(run.work),
                "failed": len([r for r in failures if r.status == STATUS_FAILED]),
                "retries": dict(run.retries),
                "replan_used": run.replan_used,
            }
            if root.status not in (STATUS_FAILED,):
                try:
                    root.mark_failed(result.error_code, str(result.error_message))
                except IllegalTransition:
                    root.status = STATUS_FAILED
                    root.error_code = result.error_code
                    root.error_message = str(result.error_message)
            self.persist_task(root)
            self.stream.emit(
                root.task_id, "captain_done", session_id=root.session_id,
                agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
                status=STATUS_FAILED, level=THINK_LEVEL_ERROR,
                text=(f"👑 {AGENT_DISPATCH} 判定：用户原始任务**未全部完成**"
                      f"（{reason}），按规则不交付交互交付Agent，直接输出故障诊断"),
                fault_report=fault,
            )
            result.final_reply = fault["report"]
            self.save_captain_snapshot(run=run, root=root, result=result, stage="failed")
            self._cancel_pending_approvals_of_task(root.task_id)
            self._skip_downstream_delivery(root, result)
            return result

        # ---- 队长确认全部完成 → 汇总全部队员材料 → 交交互交付Agent 出最终回复 ----
        self.stream.emit(
            root.task_id, "captain_done", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=root.status,
            text=(f"👑 {AGENT_DISPATCH} 判定：用户原始任务已全部完成"
                  f"（共 {len(successful)} 个子任务产出），"
                  f"开始汇总全部队员材料并交付交互交付Agent 生成最终回复"),
            captain_verdict="completed",
        )
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]

        # ==================================================================
        # 【需求点 Bug1 硬性约束4】报告中的删除结果只能来自后端真实磁盘回执：
        #   · Agent 文本里任何"已删除…"声明若无回执支撑 → 再次纠偏（双保险）；
        #   · 后端回执块作为**权威附件**附加在最终回复最前，Agent 文本不得覆盖。
        # ==================================================================
        executed_paths = self._executed_paths(run)
        receipt_blocks: list[str] = []
        member_materials: list[dict] = []
        for item_res in successful:
            cleaned, hits = sanitize_execution_claim_text(
                str(item_res.output or ""), executed_paths=executed_paths,
                context=str(item_res.title or ""))
            if hits:
                item_res.output = cleaned
                self.logger.exception_log(
                    error_code="AGENT_FALSE_EXECUTION_CLAIM",
                    message=("交付前纠偏：队员文本含无回执支撑的完成声明：" + " | ".join(hits[:3])),
                    session_id=root.session_id, task_id=item_res.task_id,
                    agent_role=item_res.agent_role,
                )
            execution = getattr(item_res, "backend_execution", None)
            if execution is not None:
                receipt_blocks.append(
                    render_receipt_report(
                        execution,
                        operator=str((self.db.get_approval(execution.approval_id) or {})
                                     .get("decided_by") or ""),
                        decided_at=(self.db.get_approval(execution.approval_id) or {})
                        .get("decided_at"),
                    ))
            if item_res.output:
                member_materials.append({
                    "agent_role": item_res.agent_role, "title": item_res.title,
                    "output": item_res.output,
                })

        receipt_block = "\n\n".join(receipt_blocks)
        result.plan["backend_receipts"] = [
            getattr(r, "backend_execution").to_dict()   # type: ignore[union-attr]
            for r in successful if getattr(r, "backend_execution", None) is not None
        ]
        reply = await dispatch.synthesize(
            root, run.user_input, member_materials,
            degraded_notes=self.degraded_notes or None,
            receipt_block=receipt_block,
        )
        # 报告以"后端真实回执"为准：回执块置于最终回复最前，Agent 文本不得改写它
        if receipt_block:
            reply = f"{receipt_block}\n\n---\n\n{reply}"
            reply, _tail_hits = sanitize_execution_claim_text(
                reply, executed_paths=executed_paths, context="最终报告复核")
        result.plan["evaluation"] = [{
            "index": r.index, "title": r.title, "agent_role": r.agent_role,
            "status": r.status, "output": r.output[:4000], "error": r.error,
            "task_id": r.task_id,
        } for r in run.completed_results()]
        result.plan["captain_loop"] = {
            "completed": len(run.completed), "remaining": 0,
            "retries": dict(run.retries), "replan_used": run.replan_used,
            "delivered": True,
        }

        # ---- 交付质检（第4章 4.6 轻量化校验）+ 交互交付润色 ----
        evaluator = self.agents[AGENT_EVALUATOR]
        light = await evaluator.lightweight_verify(root, reply)
        if not light.passed:
            self.logger.exception_log(
                error_code="DELIVERY_SECURITY_BLOCKED",
                message="最终回复未通过轻量化安全扫描，已替换为安全提示",
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_EVALUATOR,
            )
            reply = (
                "⚠️ 本次生成的回复未通过交付前安全扫描，已按规则拦截。\n\n"
                "- 拦截维度：安全风险校验\n"
                "- 处理方式：不向用户输出该内容\n\n"
                "请调整需求后重试。"
            )
        result.evaluation = light.to_dict()

        delivery = self.agents[AGENT_DELIVERY]
        self.stream.emit(
            root.task_id, "dispatch", session_id=root.session_id,
            from_agent=AGENT_EVALUATOR, to_agent=AGENT_DELIVERY,
            agent_role=AGENT_DELIVERY,
            model_label=self.active_model_label(AGENT_DELIVERY), status=root.status,
            text=(f"{AGENT_EVALUATOR} → 分发任务给 {AGENT_DELIVERY}"
                  f"（{self.active_model_label(AGENT_DELIVERY)}），任务描述：最终回复展示层润色"),
        )
        polished = await delivery.polish(root, reply, degraded_notes=self.degraded_notes or None)
        result.final_reply = polished.text
        result.plan["delivery"] = polished.to_dict()

        # ==================================================================
        # 【新流转第4步 · Bug1 修复】交互交付Agent 输出结果后 → 同步交给
        #   记忆管理Agent，把完整上下文（最终交付 + 各子任务产出）写入日志持久化。
        #   经由总线 publish（主动推送 + 异步消费），run_pipeline 收口前的
        #   flush_memory_events 保证本条在任务结束前完成消费与落库；
        #   receiver 为记忆Agent（非 user），不会进入前端聊天渲染流。
        # ==================================================================
        try:
            await self.bus.publish(Message(
                session_id=root.session_id, task_id=root.task_id, parent_task_id=None,
                sender_agent=AGENT_DELIVERY, receiver_agent=self.ctx_memory_role(),
                msg_type=MSG_TYPE_RESULT,
                payload={"content": (
                    f"【最终交付】{polished.text[:4000]}\n\n【子任务产出】\n" + (
                        "\n".join(
                            f"- [{m.get('agent_role')}] {m.get('title')}："
                            f"{str(m.get('output') or '')[:400]}"
                            for m in member_materials) or "（无）")),
                    "metadata": {
                        "kind": "final_delivery_persist",
                        "subtask_count": len(member_materials),
                        "polished": bool(polished.polished),
                    }},
                status=STATUS_SUCCESS,
            ))
        except Exception as exc:  # noqa: BLE001 记忆持久化失败不影响交付主链路
            self.logger.exception_log(
                error_code="MEMORY_PERSIST_DELIVERY_FAILED",
                message=f"交付结果写入记忆管理Agent 失败（已降级，交付不受影响）：{exc}",
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DELIVERY,
            )
        result.status = STATUS_SUCCESS
        root.mark_success(result=polished.text)
        self.persist_task(root)
        self.save_captain_snapshot(run=run, root=root, result=result, stage="delivered")
        return result

    # ------------------------------------------------------------------
    # 任务完整快照：落库 / 读取 / 恢复
    # ------------------------------------------------------------------
    def save_captain_snapshot(self, *, run: CaptainRun, root: TaskState,
                              result: PipelineResult | None = None, stage: str = "",
                              approval: dict | None = None,
                              current_work: dict | None = None) -> dict:
        """把队长循环状态 + 全部上下文覆盖写入 SQLite（业务语义循环的恢复点）。

        快照内容严格覆盖需求要求的字段：
          session_id / task_id / 已跑完的子任务列表 / 剩余子任务计划 /
          全部消息上下文 / 即将执行的高危操作详情 / 队长循环状态（迭代与重试计数）。
        """
        approval_payload = dict(approval or {})
        if current_work and not approval_payload:
            approval_payload = {}
        remaining = list(run.work)
        if current_work and not approval_payload:
            remaining = [current_work] + remaining
        loop_state = run.to_state(
            iteration=root.iteration, stage=stage or root.status,
            approval=approval_payload,
        )
        payload = {
            "plan": _jsonable(run.plan),
            "current_work": _jsonable(current_work or {}),
            "result_plan": _jsonable((result.plan if result is not None else {})),
            "workspace_root": run.workspace_root,
            "pending_approval": _jsonable(approval_payload),
        }
        row = {
            "task_id": root.task_id,
            "session_id": root.session_id,
            "parent_task_id": root.parent_task_id,
            "status": root.status,
            "stage": stage or root.status,
            "user_input": run.user_input,
            "plan": run.plan,
            "completed": run.completed,
            "remaining": remaining,
            "messages": run.messages,
            "approval": approval_payload,
            "loop_state": loop_state,
            "payload": payload,
            # 【新增】30 秒审批超时字段：只有"正在等待审批"的快照才落 deadline，
            #   审批裁决 / 业务循环恢复后写 NULL（超时判定随之失效，不会误判已恢复的任务）。
            "approval_deadline": (
                # 【BugA 修复】等待用户点击阶段不计时：0/缺省一律落 NULL，
                #   30 秒执行链路计时只在 submit 受理后写 approvals.resume_deadline。
                (approval_payload.get("approval_deadline") or None)
                if (approval_payload and root.status == STATUS_WAITING_APPROVAL) else None),
            # ==============================================================
            # 【新流转 · Bug1 修复】两道计数器 + 两类结论必须落到**独立列**（不只是 loop_state），
            #   便于运维/接口直接查询，也避免历史 loop_state 结构差异带来的读取兼容问题：
            #     fix_rounds        → 校验评估Agent 打回修改轮次（每子任务 ≤3，
            #                         整型投影 = task_snapshots.code_revise_count）
            #     reallocations     → 队长重新分配任务次数（≤3，
            #                         整型投影 = task_snapshots.reassign_task_count）
            #     requirement_state → 历史字段（需求对齐审批已下线，仅兼容旧快照读取）
            #     evaluator_state   → 校验评估Agent 最近一次结论
            # ==============================================================
            "fix_rounds": dict(run.fix_rounds or {}),
            "reallocations": int(run.reallocations or 0),
            "requirement_state": _jsonable(run.requirement_state or {}),
            "evaluator_state": _jsonable(run.evaluator_state or {}),
            "created_at": root.created_at,
        }
        try:
            self.db.save_task_snapshot(row)
        except Exception as exc:  # noqa: BLE001 快照落库失败必须留痕（但不阻断任务）
            self.logger.exception_log(
                error_code="TASK_SNAPSHOT_SAVE_FAILED",
                message=f"任务快照写入失败（已降级，任务继续）：{exc}",
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            )
        return {**row, "updated_at": time.time()}

    def load_captain_snapshot(self, task_id: str) -> dict | None:
        try:
            return self.db.get_task_snapshot(task_id)
        except Exception as exc:  # noqa: BLE001
            self.logger.exception_log(
                error_code="TASK_SNAPSHOT_READ_FAILED",
                message=f"任务快照读取失败：{exc}",
                task_id=task_id, agent_role=AGENT_DISPATCH,
            )
            return None

    def find_snapshot_for_approval(self, approval_row: dict) -> dict | None:
        """按审批记录找回它属于哪一条任务快照（子任务 → 根任务逐级向上）。"""
        task_id = str(approval_row.get("task_id") or "")
        seen: set[str] = set()
        while task_id and task_id not in seen:
            seen.add(task_id)
            snapshot = self.load_captain_snapshot(task_id)
            if snapshot is not None:
                return snapshot
            row = self.db.get_task(task_id) or {}
            task_id = str(row.get("parent_task_id") or "")
        return None

    def pending_approval_snapshot(self, session_id: str) -> dict | None:
        """当前会话是否还有"待审批未走完"的任务快照（用于拦截新任务提交）。"""
        if not session_id:
            return None
        try:
            rows = self.db.list_pending_approval_snapshots(session_id, limit=1)
        except Exception:  # noqa: BLE001 查询失败不拦截（宁可放行也不误锁用户）
            return None
        return rows[0] if rows else None

    def _approval_record_view(self, approval_id: str) -> dict | None:
        rows = self.approval_center.list_records(limit=1, state=APPROVAL_STATE_PENDING)
        match = next((r for r in rows if r.get("approval_id") == approval_id), None)
        if match:
            return match
        all_rows = self.approval_center.list_records(limit=200)
        return next((r for r in all_rows if r.get("approval_id") == approval_id), None)

    @staticmethod
    def _approval_pause_reply(approval: dict, run: CaptainRun) -> str:
        return (
            "## ⏸ 高危操作待人工审批（业务循环已中断）\n\n"
            f"- 子任务：{approval.get('title')}\n"
            f"- 执行 Agent：{approval.get('agent_role')}\n"
            f"- 操作类型：{approval.get('operation_type')}（风险等级：{approval.get('risk_level')}）\n"
            f"- 操作说明：{approval.get('operation_desc')}\n"
            f"- 风险原因：{approval.get('danger_reason')}\n\n"
            f"- 任务状态：`{STATUS_WAITING_APPROVAL}`（思维链输出已停止，未继续调用模型）\n"
            f"- 已完成子任务：{len(run.completed)} 个；待执行：{len(run.work)} 个\n"
            f"- 完整任务快照已写入 SQLite，审批后按快照恢复上下文继续执行\n\n"
            "请在输入框上方点击【✅ 执行一次】或【❌ 拒绝】完成审批；"
            "审批未完成前，新的消息提交会被拦截，避免新旧任务互相干扰。"
        )

    # ==================================================================
    # 【需求点 Bug1】后端硬编码高危门禁 + 后端原生执行器
    #   ------------------------------------------------------------------
    #   改造原则：
    #     · 高危动作识别**完全由后端程序硬编码**（constants.HIGH_RISK_OP_SET），
    #       不读取任何模型输出文本，也不允许模型自行判断高危与否；
    #     · Agent 只产出分析结果（待删除候选清单 delete_candidates）；
    #     · 磁盘删除动作唯一执行端 = backend/services/executor.BackendExecutor
    #       （os.remove / os.rmdir，逐项回执）。
    # ==================================================================
    def _delete_executor(self) -> BackendExecutor:
        """取当前会话的后端原生执行器（绑定当前工作区，路径越界由 file_guard 兜底）。"""
        return BackendExecutor(self.ctx.file_guard)

    def _build_delete_execution_record(self, *, root: TaskState, state: TaskState, role: str,
                                       plan_item: dict, intent: dict):
        """把"待删清单"转成后端高危动作记录（ToolExecutionRecord）。

        注意：这里**不调用任何 LLM**，只做数据搬运 + 硬编码高危类型判定。
        """
        from backend.agents.code_agent import ToolExecutionRecord

        candidates = list(intent.get("candidates") or [])
        operation = str(intent.get("operation") or "") or infer_delete_operation(len(candidates))
        paths = [str(c.get("path") or "") for c in candidates if c.get("path")]
        params = {
            "operation": operation,
            "paths": paths,
            "candidates": candidates,
            "delete_hint": str(intent.get("delete_hint") or ""),
        }
        reason = (f"后端硬编码高危动作「{HIGH_RISK_OP_LABEL.get(operation, operation)}」："
                  f"即将对工作区内 {len(paths)} 个目标执行真实磁盘删除（不可逆），"
                  "必须人工审批后方可由后端 Python 执行。")
        record = ToolExecutionRecord(
            tool=operation, args=params, ok=False,
            output=f"待删清单已确认（{len(paths)} 项），等待人工审批后由后端原生执行",
            high_risk=True, approved=None,
        )
        return record

    def _hardcoded_high_risk_gate(self, *, root: TaskState, state: TaskState, role: str,
                                  plan_item: dict, execution_record,
                                  model_label: str) -> dict | None:
        """硬编码高危门禁：命中 HIGH_RISK_OP_SET → 强制创建审批单并中断业务循环。

        返回审批挂起载荷（与队员触发审批时完全同一套结构，保证前端 UI 一致）。
        本方法不依赖任何模型判断，operation_type 直接来自 HIGH_RISK_OP_LABEL。
        """
        operation = str(getattr(execution_record, "tool", "") or "")
        if operation not in HIGH_RISK_OP_SET:
            return None
        params = getattr(execution_record, "args", {}) or {}
        paths = [str(p) for p in (params.get("paths") or [])]
        operation_label = HIGH_RISK_OP_LABEL.get(operation, operation)
        operation_desc = f"{operation_label}：{operation} 共 {len(paths)} 项 → " + \
                         "、".join(paths[:8]) + ("…" if len(paths) > 8 else "")
        danger_reason = (
            f"后端硬编码高危动作判定：{operation_label}（{operation}）。"
            "磁盘删除不可逆，且本系统禁止由 Agent 执行删除；"
            "必须由用户在输入框上方明确审批后才由后端 Python 执行。"
        )
        meta = ApprovalMetadata(
            risk_level=RISK_LEVEL_HIGH,
            operation_desc=operation_desc[:1200],
            operation_params=json.dumps(params, ensure_ascii=False),
            danger_reason=danger_reason,
            operation_type=operation_label,
            tool=operation,
            matched_patterns=[f"HIGH_RISK_OP_SET:{operation}"],
        )
        approval_msg, execution = self.approval_center.create_request(
            session_id=root.session_id, task_id=state.task_id, agent_role=role,
            meta=meta, tool=operation, args=params,
        )
        execution_record.approval_id = execution.approval_id
        state.mark_waiting_approval(f"硬编码高危动作 {operation_label} 强制审批")
        self.persist_task(state)
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=STATUS_WAITING_APPROVAL, step_type="exec", level=THINK_LEVEL_WARN,
            title=plan_item.get("title") or "",
            text=(f"🛡 后端硬编码高危识别（不依赖模型判断）：{operation_label}（{operation}）"
                  f"共 {len(paths)} 项 → 强制人工审批，Agent 循环已中断"),
            high_risk_operation=operation,
            high_risk_source="constants.HIGH_RISK_OP_SET",
        )
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        dispatch.think(
            state, "exec",
            f"【硬编码高危门禁】{operation_label}（{operation}）：后端识别为高危动作，"
            f"已中断 Agent 循环并推送人工审批（审批单 {execution.approval_id[:8]}）；"
            "审批通过后由后端 Python 原生执行删除，Agent 不参与执行。",
            level=THINK_LEVEL_WARN,
        )
        return self._high_risk_approval_payload(
            root=root, state=state, role=role, plan_item=plan_item,
            approval_id=execution.approval_id, meta=meta, operation=operation,
            model_label=model_label, operation_params=json.dumps(params, ensure_ascii=False),
            approval_deadline=float(getattr(execution, "approval_deadline", 0.0) or 0.0),
            action_tool=operation, action_params=params,
        )

    def _high_risk_approval_payload(self, *, root: TaskState, state: TaskState, role: str,
                                    plan_item: dict, approval_id: str, meta: ApprovalMetadata,
                                    operation: str, model_label: str,
                                    operation_params: str,
                                    approval_deadline: float = 0.0,
                                    action_tool: str = "",
                                    action_params: dict | None = None) -> dict:
        """审批挂起载荷（前端渲染【✅执行一次】【❌拒绝】所需的全部字段）。

        【修复·断点恢复】额外写入 action_tool / action_params：
        快照因此**自包含**，服务重启后无需依赖内存态即可无损重建待执行动作
        （指纹校验保持一致，审批通过仍能真实执行）。
        """
        # 【BugA 修复】等待用户点击阶段不计时：这里**不再臆造**"审批截止时间"。
        #   30 秒执行链路计时只在 POST /api/approval/submit 受理后由
        #   start_resume_window 写入 approvals.resume_deadline；
        #   等待阶段的快照 approval_deadline 列保持 NULL（超时判定永不命中）。
        deadline = float(approval_deadline or 0.0)
        payload = {
            "approval_id": approval_id,
            "task_id": state.task_id,
            "root_task_id": root.task_id,
            "session_id": root.session_id,
            "subtask_index": int(plan_item.get("index", 0)),
            "agent_role": role,
            "title": plan_item.get("title") or "",
            "instruction": str(plan_item.get("instruction") or ""),
            "operation_type": meta.operation_type,
            "operation_desc": meta.operation_desc,
            "operation_params": operation_params,
            "danger_reason": meta.danger_reason,
            "risk_level": meta.risk_level,
            "high_risk_operation": operation,
            "outcome_options": ["allowed_once", "rejected"],
            # 【BugA 修复】等待阶段无截止时间（0 = 不计时）；提交后 30 秒执行链路
            #   计时以 approvals.resume_deadline 为准（前端按 resume_state 渲染倒计时）
            "approval_deadline": deadline,
            "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
            # 【新增】待执行动作原文（重启后重建 PendingExecution 的唯一权威来源）
            "action_tool": action_tool or operation,
            "action_params": _jsonable(action_params or {}),
            "created_at": time.time(),
        }
        event_text = f"⚠️ 高危操作待审批：{meta.operation_type}（{meta.operation_desc}）"
        # 传给 StreamBroker.emit 的 detail 必须避开其显式命名参数（session_id / task_id / …）
        reserved = {"session_id", "task_id", "agent_role", "model_label", "provider",
                    "status", "title", "text", "level", "from_agent", "to_agent"}
        detail_fields = {k: v for k, v in payload.items() if k not in reserved}
        for event_name in ("approval_request", "approval"):
            self.stream.emit(
                root.task_id, event_name, session_id=root.session_id,
                agent_role=role, model_label=model_label,
                status=STATUS_WAITING_APPROVAL, title=payload["title"],
                text=event_text, level=THINK_LEVEL_WARN,
                subtask_task_id=state.task_id,
                **detail_fields,
            )
        self.stream.emit(
            root.task_id, "stream_paused", session_id=root.session_id,
            agent_role=role, model_label=model_label,
            status=STATUS_WAITING_APPROVAL, title=payload["title"],
            text=("任务已暂停在「等待人工审批」状态，思维链输出已停止；"
                  "请在输入框上方完成审批后由后端继续执行"),
            approval_id=approval_id,
        )
        return payload

    # ==================================================================
    # 【需求点 Bug1】待删清单 → 后端原生删除执行（磁盘改动的唯一路径）
    # ==================================================================
    # 哪些子任务属于"分析 + 删除"型任务：分析产出候选清单后，
    # 由后端硬编码高危门禁接管执行（Agent 绝不执行删除）。
    _DELETE_TASK_HINTS: tuple[str, ...] = (
        "删除", "删掉", "清理", "移除", "清空", "消除", "剔除",
    )
    # 【职责分离】仅"分析型"子任务（只出清单、明确要求不删除）不进入删除执行；
    # 注意：不能把"待删除候选清单"当成分析型——那正是"识别→删除"任务里的正常措辞，
    # 只有明确的否定/禁止措辞才判为分析型。
    _DELETE_TASK_DENY_HINTS: tuple[str, ...] = (
        "不要执行删除", "不要删除", "禁止删除", "请勿删除", "不得删除",
        "仅输出清单", "只输出清单", "仅做分析", "只做分析", "不做任何删除",
        "不要真的删除", "无需删除", "不需要删除", "不进行删除",
    )

    @classmethod
    def _instruction_wants_delete(cls, text: str) -> bool:
        raw = str(text or "")
        if not raw:
            return False
        if any(deny in raw for deny in cls._DELETE_TASK_DENY_HINTS):
            return False
        return any(hint in raw for hint in cls._DELETE_TASK_HINTS)

    @staticmethod
    def _candidates_of(res: SubtaskResult) -> list[dict]:
        """从队员结果里取出结构化待删候选清单（只做数据读取，不做高危判定）。"""
        meta = res.metadata or {}
        candidates = meta.get("candidates") or []
        if not candidates:
            intent = meta.get("delete_intent") or {}
            candidates = intent.get("candidates") or []
        return normalize_delete_candidates(candidates)

    def _attach_delete_intent(self, *, res: SubtaskResult, item: dict,
                              root: TaskState) -> dict:
        """队员分析完成且命中"删除型任务" → 登记后端待执行删除意图。

        返回 intent（空 dict 表示无需后端删除执行）。
        """
        if res.status != STATUS_SUCCESS:
            return {}
        if not self._instruction_wants_delete(str(item.get("instruction") or "")):
            return {}
        candidates = self._candidates_of(res)
        if not candidates:
            return {}
        operation = infer_delete_operation(len(candidates))
        if operation not in HIGH_RISK_OP_SET or not is_delete_operation(operation):
            return {}
        intent = {
            "operation": operation,
            "candidates": candidates,
            "delete_hint": str(item.get("instruction") or "")[:600],
            "requested_by": res.agent_role,
        }
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=STATUS_RUNNING, step_type="think",
            text=(f"🛡 后端识别：队员 {res.agent_role} 产出待删候选 {len(candidates)} 项，"
                  f"按硬编码高危动作清单判定为「{HIGH_RISK_OP_LABEL.get(operation, operation)}」"
                  f"（{operation}）→ 下一轮由后端门禁强制审批，Agent 不执行删除"),
            high_risk_operation=operation,
            high_risk_source="constants.HIGH_RISK_OP_SET",
        )
        return intent

    async def _captain_execute_backend_delete(self, *, run: CaptainRun, root: TaskState,
                                              result: PipelineResult, item: dict,
                                              res: SubtaskResult) -> DeleteExecution | None:
        """审批通过后：由后端 Python 原生执行磁盘删除并采集逐项回执。

        执行完成后：回执写入结果（供告警/报告/前端展示），子任务状态按回执收口，
        并把**真实回执文本**回传给队长（作为报告里删除结果的唯一数据源）。
        """
        # 幂等保护：同一结果不重复执行磁盘删除（回执本身即"已执行"的证据）
        if getattr(res, "backend_execution", None) is not None:
            return res.backend_execution
        intent = dict((res.metadata or {}).get("delete_intent") or {})
        operation = str(intent.get("operation") or "")
        candidates = normalize_delete_candidates(intent.get("candidates") or [])
        approval_id = str(res.approval_id or "")
        approval_row = self.db.get_approval(approval_id) if approval_id else None
        approved = bool(approval_row and approval_row.get("state") == APPROVAL_STATE_MANUAL)
        operator = str((approval_row or {}).get("decided_by") or "")
        row = approval_row
        if not operation or not is_delete_operation(operation):
            return None
        approval_id = str(res.approval_id or "")
        approval_row = self.db.get_approval(approval_id) if approval_id else None
        approved = bool(approval_row and approval_row.get("state") == APPROVAL_STATE_MANUAL)
        operator = str((approval_row or {}).get("decided_by") or "")
        row = approval_row
        if not approved:
            # 未获批准（含拒绝 / 审批单缺失）→ 绝不触碰磁盘
            self._cancel_pending_approvals_of_task(root.task_id)
            return None

        executor = self._delete_executor()
        execution = executor.execute(
            operation=operation, candidates=candidates, approved=True,
            approval_id=approval_id, session_id=root.session_id,
            task_id=res.task_id or root.task_id,
            workspace_root=str(getattr(self.ctx.file_guard, "workspace_root", "") or ""),
        )
        res._backend_executed = True          # type: ignore[attr-defined]
        res.backend_execution = execution
        res.metadata = {**(res.metadata or {}),
                        "backend_execution": execution.to_dict(),
                        "delete_receipts": [r.to_dict() for r in execution.receipts]}
        receipt_text = render_receipt_report(
            execution, operator=operator, decided_at=row.get("decided_at") if row else None)
        res.output = f"{str(res.output or '').strip()}\n\n{receipt_text}".strip()
        state = self.load_task_state(res.task_id)
        if execution.receipts and execution.all_ok:
            res.status = STATUS_SUCCESS
            if state is not None:
                try:
                    state.mark_success(result=receipt_text[:2000])
                except IllegalTransition:
                    state.status = STATUS_SUCCESS
                self.persist_task(state)
        else:
            res.status = STATUS_FAILED
            res.failure_stage = res.failure_stage or "backend_execute_failed"
            res.failure_reason = (
                f"后端原生执行存在失败项（成功 {execution.ok_count}/{execution.total}）：" +
                "；".join(f"{r.path}={r.error or r.detail}" for r in execution.receipts
                          if not r.ok)[:500])
            res.error = res.failure_reason
            if state is not None:
                try:
                    state.mark_failed("BACKEND_DELETE_PARTIAL_FAILED", res.failure_reason)
                except IllegalTransition:
                    state.status = STATUS_FAILED
                    state.error_code = "BACKEND_DELETE_PARTIAL_FAILED"
                    state.error_message = res.failure_reason
                self.persist_task(state)
        result.plan["backend_execution"] = execution.to_dict()
        self.stream.emit(
            root.task_id, "backend_execution", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=res.status, title=res.title, level=THINK_LEVEL_INFO,
            text=(f"🧾 后端原生执行回执：{HIGH_RISK_OP_LABEL.get(operation, operation)} "
                  f"共 {execution.total} 项，成功 {execution.ok_count}，失败 {execution.failed_count}"
                  "（报告中的删除结果只来自本回执）"),
            receipts=[r.to_dict() for r in execution.receipts],
            operation=operation, source="backend_native",
        )
        self.logger.task_log(
            session_id=root.session_id, task_id=root.task_id, agent_role="backend_executor",
            event="backend.delete.receipts",
            detail=(f"op={operation} total={execution.total} ok={execution.ok_count} "
                    f"failed={execution.failed_count} approval={approval_id}"),
        )
        return execution

    def _executed_paths(self, run: "CaptainRun | None" = None,
                        results: dict[int, SubtaskResult] | None = None) -> set[str]:
        """后端真实执行过的路径集合（用于纠偏 Agent 文本里的虚假完成声明）。"""
        paths: set[str] = set()
        pool: list[SubtaskResult] = []
        if run is not None:
            pool.extend(run.results.values())
        if results:
            pool.extend(results.values())
        for res in pool:
            execution = getattr(res, "backend_execution", None)
            if execution is None:
                continue
            for receipt in execution.receipts:
                if receipt.ok:
                    paths.add(str(receipt.path))
        return paths

    # ==================================================================
    # 子任务执行（严格按依赖顺序 + 统一消息总线通信）
    # ==================================================================
    async def _execute_subtasks(self, root: TaskState, subtasks_plan: list[dict],
                                image_paths: list[str], *,
                                prebuilt: dict[int, tuple[TaskState, SubtaskResult]] | None = None,
                                retry_meta: dict[int, dict] | None = None,
                                results: dict[int, SubtaskResult] | None = None,
                                ) -> tuple[list[SubtaskResult], dict[str, Any] | None]:
        """按依赖顺序分发本批子任务（队长循环每轮只下发一个子任务）。

        返回 (本批结果列表, 审批挂起载荷)：
          · 审批挂起载荷不为 None → 队长循环必须立刻暂停并把快照落库（业务语义先中断）；
          · results 为跨轮共享的结果表（队长循环复用，依赖判定需要看到历史子任务结果）。

        prebuilt：索引 → (已存在的子任务 TaskState, 已存在的 SubtaskResult)。
          队长循环重试同一子任务时必须复用同一个子任务号与结果对象
          （与历史重试行为一致：状态机 retry_count 累加、不新建任务）。
        retry_meta：索引 → {"retry_count": n, "previous_error": str, "instruction": str}
          （队长重新分派时携带上次失败原因，让队员知道要改什么）。
        """
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        sub_states: dict[int, TaskState] = {}
        results = results if results is not None else {}
        prebuilt = prebuilt or {}
        retry_meta = retry_meta or {}

        ordered = sorted(subtasks_plan, key=lambda s: (len(s.get("depend_on") or []), s.get("index", 0)))

        for plan_item in ordered:
            idx = int(plan_item["index"])
            role = plan_item["agent_role"]
            # 共享结果表的键：默认 = 计划下标；"队长换人重做"的新子任务用新键，避免覆盖原结果
            key = int(plan_item.get("result_key", idx))

            reused = prebuilt.get(idx)
            # 【第三轮·新流转】本轮若带着"校验评估Agent 的修改意见"(evaluator_fix)，
            #   必须**真正重新下发队员执行**，任何"已是终态 → 跳过重复执行"的复用捷径
            #   都要让位，否则打回意见永远送不到原队员手里（3 轮打回全部空转）。
            #   注意：必须在循环体最前面取值，避免后面分支提前使用导致 UnboundLocalError。
            evaluator_fix = dict(plan_item.get("evaluator_fix") or {})
            fix_round = bool(evaluator_fix)
            if reused is not None:
                state, res = reused
                state.parent_task_id = root.task_id
                state.deadline_at = root.deadline_at
                # 【需求点 Bug2】已经跑到终态的子任务不得被重复执行
                #   （快照恢复 / 重复入队都可能带来这种工作项）。
                #   审批已裁决的删除动作仍由队长循环的后端执行分支处理，不走这里。
                if not fix_round and res.status in (STATUS_SUCCESS, STATUS_FAILED) and not (
                        (res.metadata or {}).get("delete_intent")
                        and self._approval_already_decided(res)) \
                        and getattr(res, "backend_execution", None) is None:
                    self._captain_step_emit(
                        root, "verify",
                        f"队长循环：子任务「{res.title}」已是终态（{res.status}），"
                        "跳过重复执行，直接沿用既有结果")
                    results[key] = res
                    continue
            else:
                state = TaskState(
                    task_id=self.router.new_task_id(), session_id=root.session_id,
                    title=plan_item["title"], agent_role=role, parent_task_id=root.task_id,
                    depend_on=[int(d) for d in (plan_item.get("depend_on") or [])],
                )
                state.deadline_at = root.deadline_at
                res = SubtaskResult(index=idx, title=plan_item["title"], agent_role=role,
                                    status=STATUS_PENDING, task_id=state.task_id,
                                    result_key=key)
            res.result_key = key

            # ---- 循环依赖拦截（Agent_Router，第4章 4.1 / 第2章 2.1 规则3） ----
            dep_task_ids = [sub_states[d].task_id for d in (plan_item.get("depend_on") or [])
                            if d in sub_states]
            for dep_idx in (plan_item.get("depend_on") or []):
                prev = results.get(int(dep_idx))
                if prev is not None and prev.task_id and prev.task_id not in dep_task_ids:
                    dep_task_ids.append(prev.task_id)
            self.router.detect_loop(state.task_id, root.task_id, dep_task_ids)
            if reused is None:
                self.router.register_task(state, depend_on=dep_task_ids)
            sub_states[idx] = state
            results[key] = res

            # 依赖未成功 → 跳过该子任务（父任务由队长决定后续）
            unmet = [d for d in (plan_item.get("depend_on") or [])
                     if d in results and results[d].status not in (STATUS_SUCCESS,)]
            if unmet:
                res.status = STATUS_FAILED
                res.error = f"依赖子任务未成功（{unmet}），已跳过"
                res.blocked_by = unmet
                res.failure_stage = "dependency_blocked"
                res.failure_reason = "上游子任务失败，按链路阻断规则不再下行分发"
                # 【BUG-C 3/5】被阻断也必须写思考链（error 级别）+ 流式事件，
                #   禁止用 success 状态掩盖失败。
                res.think_steps = [{
                    "index": 0, "type": "exec", "level": THINK_LEVEL_ERROR,
                    "text": f"链路阻断：子任务「{plan_item['title']}」因上游子任务 {unmet} 失败被跳过，"
                            f"未下发执行",
                    "agent": AGENT_DISPATCH,
                }]
                dispatch.think(state, "exec",
                               f"链路阻断：子任务「{plan_item['title']}」依赖 {unmet} 未成功，"
                               f"按要求不再向下分发该子任务",
                               level=THINK_LEVEL_ERROR)
                self.stream.emit(
                    root.task_id, "subtask_blocked", session_id=root.session_id,
                    from_agent=AGENT_DISPATCH, to_agent=role, agent_role=role,
                    model_label=self.active_model_label(role), status=STATUS_FAILED,
                    title=plan_item["title"], text=(
                        f"{AGENT_DISPATCH} 阻断子任务「{plan_item['title']}」："
                        f"上游子任务 {unmet} 失败，依赖输出无效，不再向下分发"),
                    error=res.error, blocked_by=unmet, level=THINK_LEVEL_ERROR,
                    subtask_task_id=state.task_id, subtask_index=idx,
                )
                self.persist_task(state)
                continue

            if reused is not None and state.status in (STATUS_FAILED, STATUS_SUCCESS):
                # 【需求点 Bug2】队长判定重试 → 子任务复位为可再次分发（不新增状态值）
                state.rearm_for_redispatch(
                    retry_count=int((retry_meta.get(idx) or {}).get("retry_count") or 0) or None,
                    reason="队长判定结果不达预期，重新分派同一队员重试")
            state.mark_running("分发执行")
            self.persist_task(state)
            res.status = STATUS_RUNNING

            # ==============================================================
            # 【BUG-C 5】思考过程增强：逐步打印该 Agent 的
            #   输入（任务指令）→ 执行动作（本 Agent + 实际模型）→ 输出内容
            # ==============================================================
            subtask_model_label = self.active_model_label(role)
            dispatch.think(
                state, "push",
                f"【{role} · 输入】子任务「{plan_item['title']}」｜"
                f"指令：{str(plan_item.get('instruction') or '')[:300]}",
            )
            dispatch.think(
                state, "push",
                f"【{role} · 执行动作】使用模型 {subtask_model_label}"
                f"（厂商 {AGENT_BINDINGS.get(role, {}).get('provider', '-')}）"
                f"在本工作区执行该子任务",
            )

            # ==============================================================
            # 【需求点 Bug2】推送「任务流转」事件：
            #   {调度规划Agent(模型)} → 分发任务给 {目标Agent(模型)}，任务描述：xxx
            # ==============================================================
            dispatcher_label = self.active_model_label(AGENT_DISPATCH)
            target_label = self.active_model_label(role)
            self.stream.emit(
                root.task_id, "dispatch", session_id=root.session_id,
                from_agent=AGENT_DISPATCH, to_agent=role, agent_role=role,
                model_label=target_label, status=STATUS_RUNNING,
                title=plan_item["title"],
                text=(f"{AGENT_DISPATCH}（{dispatcher_label}） → 分发任务给 {role}（{target_label}），"
                      f"任务描述：{plan_item['title']}"),
                subtask_task_id=state.task_id,
                subtask_index=idx,
                instruction=str(plan_item.get("instruction") or "")[:1500],
                depend_on=list(plan_item.get("depend_on") or []),
            )

            # ==============================================================
            # 【需求点 Bug1】后端硬编码高危拦截：待删清单已确认 → 不再调用任何 Agent
            #   职责分离：Agent 只做分析（输出 delete_candidates），
            #   所有磁盘删除动作一律由后端 Python 代码在审批通过后执行。
            #   因此这一步不派发给模型，直接走「硬编码高危动作 → 强制审批」。
            # ==============================================================
            commit_intent = plan_item.get("delete_commit") or {}
            if commit_intent and not evaluator_fix:
                # ==========================================================
                # 【第三轮·修缺陷】本轮是"校验评估Agent 打回后的重做"时**跳过**本分支：
                #   工作项上的 delete_commit 是上一轮审批留下的持久字段，若照旧用它去判
                #   "已裁决/仍 pending"，会把重做误当成审批恢复 → 直接 continue，
                #   队员永远收不到校验意见（3 轮打回空转、任务必然失败）。
                #   重做轮必须让队员重新执行；新产出的待删清单会在队长循环里
                #   重新登记 delete_commit 并生成**新的**审批单（逐条审批，符合安全设计）。
                # ==========================================================
                # 审批已经裁决过（恢复路径）→ 不再重复创建审批单，
                # 交由队长循环的"审批恢复"分支执行后端原生删除。
                existing_key = int(plan_item.get("result_key", idx))
                existing_res = results.get(existing_key)
                # 【需求点 Bug2 防重复审批】该子任务已经问过用户（存在审批单）时：
                #   · 已裁决（manual/rejected）→ 交回队长循环的"审批恢复"分支执行后端删除；
                #   · 仍 pending           → 保持 waiting_approval 暂停，绝不重复建单。
                latest = self.db.latest_approval_for_task(state.task_id)
                if latest is not None and not fix_round:
                    if str(latest.get("state") or "") == APPROVAL_STATE_PENDING:
                        state.mark_waiting_approval("同一条待审批单仍在等待用户裁决")
                        self.persist_task(state)
                        if existing_res is not None:
                            existing_res.status = STATUS_WAITING_APPROVAL
                            existing_res.approval_id = str(latest.get("approval_id") or "")
                        return list(results.values()), self._high_risk_approval_payload(
                            root=root, state=state, role=role, plan_item=plan_item,
                            approval_id=str(latest.get("approval_id") or ""),
                            meta=ApprovalMetadata(
                                risk_level=str(latest.get("risk_level") or RISK_LEVEL_HIGH),
                                operation_desc=str(latest.get("operation_desc") or ""),
                                operation_params=str(latest.get("operation_params") or "{}"),
                                danger_reason=str(latest.get("danger_reason") or ""),
                                operation_type=str(latest.get("operation_type") or ""),
                                tool=str(commit_intent.get("operation") or ""),
                            ),
                            operation=str(commit_intent.get("operation") or ""),
                            model_label=self.active_model_label(role),
                            operation_params=str(latest.get("operation_params") or "{}"),
                        )
                    # 已裁决 → 不再建单，交回队长循环按快照/结果执行后端删除
                    if existing_res is not None:
                        existing_res.approval_id = str(latest.get("approval_id") or "")
                    continue
                high_risk_record = self._build_delete_execution_record(
                    root=root, state=state, role=role, plan_item=plan_item,
                    intent=commit_intent)
                approve_req = self._hardcoded_high_risk_gate(
                    root=root, state=state, role=role, plan_item=plan_item,
                    execution_record=high_risk_record, model_label=target_label)
                if approve_req is not None:
                    res.status = STATUS_WAITING_APPROVAL
                    res.approval_id = approve_req["approval_id"]
                    res.output = (
                        f"待删清单已确认（{len(commit_intent.get('candidates') or [])} 项），"
                        f"后端识别为硬编码高危动作「{approve_req['operation_type']}」，"
                        "已中断业务循环并推送人工审批；批准后才由后端 Python 执行磁盘删除。"
                    )
                    res.metadata = {**(res.metadata or {}),
                                    "delete_intent": {
                                        "operation": commit_intent.get("operation"),
                                        "candidates": commit_intent.get("candidates") or [],
                                        "delete_hint": commit_intent.get("delete_hint") or "",
                                    }}
                    self.persist_task(state)
                    return list(results.values()), approve_req
                # 理论上不可达（删除类高危一律需要审批）；为安全起见按失败收口
                res.status = STATUS_FAILED
                res.error = "硬编码高危门禁未返回审批请求，已按失败收口（安全优先）"
                res.failure_stage = "high_risk_gate"
                res.failure_reason = res.error
                self.persist_task(state)
                continue

            # ---- 通过统一消息总线分发（禁止 Agent 直连） ----
            # 【队长-队员架构 · 规则4】队长已仲裁出唯一基准清单时，强制注入下游指令：
            #   队员不得再自行判定，必须沿用队长裁决后的清单（唯一数据源）。
            #   注入位置：独立 metadata 字段 → 由接收 Agent 放进 user 消息（干净分片），
            #   不污染系统提示词（避免把数据混进契约说明里）。
            canonical_block = self._canonical_block_for(
                root.session_id, plan_item["instruction"],
                title=plan_item.get("title") or "")
            metadata: dict[str, Any] = {
                "instruction": plan_item["instruction"],
                "subtask_index": idx,
                "plan_title": plan_item["title"],
                "canonical_applied": bool(canonical_block),
            }
            if canonical_block:
                metadata["canonical_block"] = canonical_block
            # 【需求点 Bug2】队长重新分派同一子任务时，把上次失败原因一并下发
            retry_info = retry_meta.get(idx) or {}
            if retry_info:
                metadata["retry"] = int(retry_info.get("retry_count") or 0)
                metadata["previous_error"] = str(retry_info.get("previous_error") or "")[:800]
                metadata["redispatch_reason"] = str(retry_info.get("reason") or "")[:400]
                state.retry_count = max(int(state.retry_count or 0),
                                        int(retry_info.get("retry_count") or 0))
            # ==============================================================
            # 【第三轮·新流转】校验评估Agent 打回时，必须把"具体问题 + 修改建议"
            #   真正送进原队员的**执行指令**：
            #   历史实现只把 previous_error 放进 metadata，而代码工程Agent 的
            #   handle() 只读 metadata["instruction"] → 队员看不到校验意见，
            #   只会重复产出同样的错误结果（3 轮打回全部浪费）。
            # ==============================================================
            evaluator_fix_body = evaluator_fix
            fix_instruction = ""
            if evaluator_fix_body:
                issues_text = "；".join(
                    f"[{i.get('dimension')}/{i.get('severity')}] {i.get('detail')}"
                    + (f"（修改建议：{i.get('suggestion')}）" if i.get("suggestion") else "")
                    for i in (evaluator_fix_body.get("issues") or [])[:6])
                fix_instruction = (
                    f"\n\n【必须修改 · 第 {int(evaluator_fix_body.get('round') or 1)}/"
                    f"{MAX_EVALUATOR_FIX_ROUNDS} 轮校验反馈】\n"
                    f"{AGENT_EVALUATOR} 判定上一轮产出不合格，请**在保留原有正确内容的前提下**"
                    f"逐条修正后重新输出完整结果：\n"
                    f"- 问题清单：{issues_text or '（见下）'}\n"
                    f"- 修改要求：{str(evaluator_fix_body.get('suggestion') or '')[:1200]}\n"
                    f"- 上一轮产出（供你对比修改，不要照抄错误部分）：\n"
                    f"{str(evaluator_fix_body.get('previous_output') or '')[:1500]}\n"
                )
                metadata["evaluator_feedback"] = {
                    "round": int(evaluator_fix_body.get("round") or 1),
                    "issues": evaluator_fix_body.get("issues") or [],
                    "suggestion": str(evaluator_fix_body.get("suggestion") or "")[:1200],
                }
            # 第3章 3.3：图片资源路径随 metadata 传递
            if image_paths:
                metadata[META_IMAGE_RESOURCES] = image_paths
            # 附件路径（文档信息Agent 需要）
            if role == AGENT_DOC:
                metadata.setdefault("doc_path", str(plan_item.get("doc_path") or ""))

            msg = Message(
                session_id=root.session_id, task_id=state.task_id,
                parent_task_id=root.task_id, sender_agent=AGENT_DISPATCH,
                receiver_agent=role, msg_type="task",
                payload={"content": f"{plan_item['instruction']}{fix_instruction}",
                         "metadata": metadata},
                status=STATUS_PENDING,
            )

            try:
                reply = await self.bus.send_and_receive(msg)
            except (MessageValidationError, SecurityViolation) as exc:
                res.status = STATUS_FAILED
                res.error = str(exc)
                state.mark_failed(getattr(exc, "code", "DISPATCH_FAILED"), str(exc))
                self.persist_task(state)
                dispatch.think(state, "exec", f"分发失败：{exc}")
                continue

            await self._apply_reply(state, res, reply)

            # ==============================================================
            # 【需求点 Bug1】队员结果一落地就登记"后端待执行删除意图"
            #   （必须早于队长一致性校验：校验会重写 res.metadata，
            #     也必须早于队长 resolve：resolve 会把工作项出队，
            #     导致后端硬编码门禁没有机会插入审批）。
            #   命中条件：删除型任务 + 已产出结构化待删候选清单。
            #   Agent 到此为止只做分析，绝不执行删除。
            # ==============================================================
            if res.status == STATUS_SUCCESS and not (res.metadata or {}).get("delete_intent"):
                claimed, claim_hits = sanitize_execution_claim_text(
                    str(res.output or ""),
                    executed_paths=self._executed_paths(results=results),
                    context=str(res.title or ""))
                if claim_hits:
                    res.output = claimed
                    self.stream.emit(
                        root.task_id, "claim_corrected", session_id=root.session_id,
                        agent_role=role, status=res.status, level=THINK_LEVEL_WARN,
                        title=plan_item.get("title") or "",
                        text=(f"⚠️ 后端纠偏：队员 {role} 的输出中出现 {len(claim_hits)} 处"
                              "「已执行完成」声明，但后端没有任何真实磁盘回执 → 已改写为"
                              "「未执行任何磁盘改动（仅输出分析清单）」"),
                        corrected_claims=claim_hits[:5],
                    )
                    self.logger.exception_log(
                        error_code="AGENT_FALSE_EXECUTION_CLAIM",
                        message=(f"队员 {role} 虚报执行完成（后端无真实回执，已纠偏）："
                                 + " | ".join(claim_hits[:3])),
                        session_id=root.session_id, task_id=state.task_id, agent_role=role,
                    )
                intent = self._attach_delete_intent(res=res, item=plan_item, root=root)
                if intent:
                    res.metadata = {**(res.metadata or {}), "delete_intent": intent}
                    plan_item["delete_commit"] = intent
                else:
                    plan_item.pop("delete_commit", None)

            # ==============================================================
            # 【队长-队员架构 · 需求 2/3】队员输出上报队长 → 一致性校验 → 冲突仲裁
            #   · 队员（代码工程Agent 等）的"清单/候选"类产出全部上报队长；
            #   · 两份待删清单出现条目差异 → 自动触发队长仲裁分支；
            #   · 队长裁决结果 = 整条链路唯一基准（写入 canonical，下游强制沿用）；
            #   · 本分支只在检测到冲突时执行，原有串行链路行为完全不变；
            #     异常一律降级：仲裁失败绝不中断子任务分发。
            # ==============================================================
            try:
                await self._captain_consistency_check(
                    root=root, role=role, phase=plan_item.get("title") or "",
                    res=res, model_label=target_label,
                )
            except Exception as exc:  # noqa: BLE001 仲裁分支失败不得影响主链路
                self.logger.exception_log(
                    error_code="CAPTAIN_ARBITRATION_FAILED",
                    message=f"队长一致性校验/仲裁分支异常（已降级，链路继续）：{exc}",
                    session_id=root.session_id, task_id=root.task_id,
                    agent_role=AGENT_DISPATCH,
                )

            # ==============================================================
            # 【需求点 Bug2】把该子任务期间 Agent 产生的思考步骤与工具调用
            #   逐条推送到流式通道，前端即可看到分步思维链：
            #   调用哪个模型、正在执行什么、写了哪些文件、状态如何。
            # ==============================================================
            # 【BUG-C 5】输出内容（成功后为真实产出，失败时为错误信息）
            if res.status == STATUS_FAILED:
                dispatch.think(
                    state, "exec",
                    f"【{role} · 输出】失败：{res.failure_reason or res.error}",
                    level=THINK_LEVEL_ERROR,
                )
            else:
                dispatch.think(
                    state, "push",
                    f"【{role} · 输出】{str(res.output or '（无输出）')[:400]}",
                )

            self._flush_subtask_steps(
                root=root, state=state, role=role, model_label=target_label,
                title=plan_item["title"], res=res,
            )

            # 【需求点 Bug2】推送子任务完成/失败回执事件（含结果摘要）
            self.stream.emit(
                root.task_id, "subtask_done", session_id=root.session_id,
                from_agent=role, to_agent=AGENT_DISPATCH, agent_role=role,
                model_label=target_label, status=res.status, title=plan_item["title"],
                text=(f"{role}（{target_label}）完成子任务「{plan_item['title']}」，状态：{res.status}"
                      if res.status == STATUS_SUCCESS else
                      f"{role}（{target_label}）子任务「{plan_item['title']}」状态：{res.status}"
                      + (f"，原因：{res.error}" if res.error else "")),
                subtask_task_id=state.task_id,
                output=str(res.output or "")[:2000],
                error=res.error,
            )

            # 子任务需要审批 → 记录 approval_id 并停止后续子任务（业务语义：先中断）
            if res.status == STATUS_WAITING_APPROVAL:
                # 【修正】必须取"本子任务"最新一条 pending 审批：
                #   同一会话可能存在多条待审批（多个高危动作逐条审批），
                #   用会话级 list_approvals(state=pending) 取首条会张冠李戴。
                row = self.db.latest_approval_for_task(state.task_id)
                if not row or row.get("state") != APPROVAL_STATE_PENDING:
                    pendings = self.db.list_pending_approvals(session_id=root.session_id, limit=20)
                    row = next((r for r in pendings
                                if str(r.get("task_id") or "") == state.task_id), None) \
                        or (pendings[-1] if pendings else None)
                approval_payload: dict[str, Any] | None = None
                if row:
                    res.approval_id = row["approval_id"]
                    # 【修复·断点恢复】解析待执行动作原文（快照自包含，重启可无损重建）
                    action_params: dict[str, Any] = {}
                    try:
                        raw_params = str(row.get("operation_params") or "")
                        if raw_params.strip().startswith("{"):
                            parsed_params = json.loads(raw_params)
                            if isinstance(parsed_params, dict):
                                action_params = parsed_params
                    except (json.JSONDecodeError, ValueError):
                        action_params = {}
                    action_tool = str(action_params.get("operation") or "")
                    if action_tool not in HIGH_RISK_OP_SET:
                        action_tool = ""
                    # 【BugA 修复】等待用户点击阶段不计时：不再臆造"审批截止时间"，
                    #   30 秒执行链路计时只在 submit 受理后写 approvals.resume_deadline。
                    #   快照 approval_deadline 列保持 NULL（save_captain_snapshot 里
                    #   0/缺省一律落 NULL），超时判定在等待阶段永不命中。
                    approval_payload = {
                        "approval_id": row["approval_id"],
                        "task_id": state.task_id,
                        "root_task_id": root.task_id,
                        "session_id": root.session_id,
                        "subtask_index": idx,
                        "agent_role": role,
                        "title": plan_item["title"],
                        "instruction": str(plan_item.get("instruction") or ""),
                        "operation_type": row["operation_type"],
                        "operation_desc": row["operation_desc"],
                        "operation_params": row["operation_params"],
                        "danger_reason": row["danger_reason"],
                        "risk_level": row["risk_level"],
                        "created_at": row.get("created_at"),
                        "approval_deadline": 0.0,
                        "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
                        "outcome_options": ["allowed_once", "rejected"],
                        # 【修复·断点恢复】待执行动作原文随快照落库：
                        #   服务重启后据此无损重建 PendingExecution（含指纹），
                        #   否则审批通过时二次校验第 V5 道必然失败 → 任务永久卡死。
                        "action_tool": action_tool,
                        "action_params": action_params,
                    }
                    # 【需求点 Bug2】SSE 推送给前端的审批事件（数据源：前端据此渲染审批按钮）
                    #   事件名同时给出 approval 与 approval_request 两种：
                    #   · approval_request：新前端按此名渲染【✅执行一次】【❌拒绝】按钮；
                    #   · approval        ：历史事件名，保持向后兼容（旧前端仍可工作）。
                    event_detail = {
                        "approval_id": row["approval_id"],
                        # 子任务 task_id 用 subtask_task_id 传出（emit 的首个位置参数已是任务通道 ID）
                        "subtask_task_id": state.task_id,
                        "root_task_id": root.task_id,
                        "subtask_index": idx,
                        "operation_type": row["operation_type"],
                        "operation_desc": row["operation_desc"],
                        "operation_params": row["operation_params"],
                        "danger_reason": row["danger_reason"],
                        "risk_level": row["risk_level"],
                        "outcome_options": ["allowed_once", "rejected"],
                        # 【BugA 修复】等待阶段无截止时间（0 = 不计时）；
                        #   提交后 30 秒执行链路计时以 resume_deadline 为准（resume_state 渲染）
                        "approval_deadline": 0.0,
                        "approval_created_at": row.get("created_at"),
                        "timeout_seconds": float(APPROVAL_TIMEOUT_SECONDS),
                    }
                    event_text = (f"⚠️ 高危操作待审批：{row['operation_type']}"
                                  f"（{row['operation_desc']}）")
                    self.stream.emit(
                        root.task_id, "approval_request", session_id=root.session_id,
                        agent_role=role, model_label=target_label,
                        status=STATUS_WAITING_APPROVAL, title=plan_item["title"],
                        text=event_text, level=THINK_LEVEL_WARN, **event_detail,
                    )
                    self.stream.emit(
                        root.task_id, "approval", session_id=root.session_id,
                        agent_role=role, model_label=target_label,
                        status=STATUS_WAITING_APPROVAL, title=plan_item["title"],
                        text=event_text, **event_detail,
                    )
                    # 审批暂停时立刻停止 SSE 流式输出（不再继续推送后续 chunk）
                    self.stream.emit(
                        root.task_id, "stream_paused", session_id=root.session_id,
                        agent_role=role, model_label=target_label,
                        status=STATUS_WAITING_APPROVAL, title=plan_item["title"],
                        text=("任务已暂停在「等待人工审批」状态，思维链输出已停止；"
                              "请在输入框上方完成审批后继续"),
                        approval_id=row["approval_id"],
                    )
                self.persist_task(state)
                return [results[k] for k in sorted(results.keys())], approval_payload

            self.persist_task(state)

        return [results[k] for k in sorted(results.keys())], None

    # ==================================================================
    # 【需求点 Bug2】子任务思考链推送（把 Agent 的思考步骤与工具调用转成流式事件）
    # ==================================================================
    _THINK_STEP_LABEL = {
        "think": "思考", "write": "写入", "edit": "编辑", "exec": "执行",
        "push": "推送", "read": "读取",
    }

    def _flush_subtask_steps(self, *, root: TaskState, state: TaskState, role: str,
                             model_label: str, title: str, res: SubtaskResult) -> None:
        """读取该子任务的思考步骤记录，逐条推送到流式通道。

        思考步骤由各 Agent 通过 BaseAgent.think() 落库，天然包含：
          · 调用哪个模型（模型调用的 step_label）
          · 正在执行什么（工具调用、文件写入、命令执行）
          · 补位/降级等异常情况
        因此这里把它们映射为前端可直接渲染的思维链事件。

        【BUG-C 2/5】步骤带 level（info/warn/error）：
        error 级别的步骤在前端渲染为红色错误标识，禁止用 success 掩盖失败。
        """
        try:
            steps = self.db.list_think_steps(state.task_id)
        except Exception:  # noqa: BLE001 流式推送失败绝不阻断任务
            return
        collected: list[dict] = []
        for step in steps:
            step_type = str(step.get("step_type") or "think")
            level = str(step.get("level") or THINK_LEVEL_INFO)
            collected.append({
                "index": step.get("step_index"), "type": step_type,
                "text": step.get("step_text") or "", "agent": step.get("agent_role") or role,
                "level": level,
            })
            self.stream.emit(
                root.task_id,
                "tool_call" if step_type in ("write", "edit", "exec") else "agent_step",
                session_id=root.session_id, agent_role=role, model_label=model_label,
                status=state.status, title=title,
                text=f"{role}（{model_label}）· {self._THINK_STEP_LABEL.get(step_type, step_type)}"
                     f"：{step.get('step_text') or ''}",
                step_type=step_type,
                step_index=step.get("step_index"),
                level=level,
                subtask_task_id=state.task_id,
            )
        res.think_steps = collected

    async def _apply_reply(self, state: TaskState, res: SubtaskResult, reply: Message | None) -> None:
        """把 Agent 回执映射回子任务结果与状态机。

        【BUG-C 1/5】失败一律标记 STATUS_FAILED 并记录失败阶段与可读原因，
        绝不允许把失败写成 success。
        【增量修复 3】同时记录失败分类：IO 错误（目录/权限）与
        模型收敛失败（MODEL_CONVERGE_FAIL）在日志与故障诊断中分开呈现。
        """
        if reply is None:
            res.status = STATUS_FAILED
            res.error = "Agent 未返回消息"
            res.failure_stage = "no_reply"
            res.failure_reason = res.error
            state.mark_failed("NO_REPLY", "Agent 未返回消息")
            return

        content = reply.content
        text = content if isinstance(content, str) else str(content)

        if reply.msg_type == "error":
            code = str(reply.metadata.get("code") or "AGENT_ERROR")
            res.status = STATUS_FAILED
            res.error = text
            res.failure_stage = "agent_error"
            res.failure_reason = code
            # ★ 错误分类：IO / 模型收敛 / 其它（统一走 constants 的分类入口）
            res.failure_kind = classify_failure(code)
            if res.failure_kind:
                self.logger.task_log(
                    session_id=state.session_id, task_id=state.task_id,
                    agent_role=res.agent_role,
                    event="subtask.failure.classified",
                    level="error",
                    detail=(f"failure_kind={res.failure_kind} "
                            f"error_code={code} "
                            f"label={FAILURE_KIND_LABEL.get(res.failure_kind, '')} "
                            f"message={text[:500]}"),
                )
            res.metadata = reply.metadata
            if state.status == STATUS_RUNNING:
                state.mark_failed(code, text)
            return

        if reply.status == STATUS_WAITING_APPROVAL or reply.msg_type == "approval_request":
            res.status = STATUS_WAITING_APPROVAL
            res.output = text
            return

        res.status = STATUS_SUCCESS
        res.output = text
        res.metadata = reply.metadata
        if state.status == STATUS_RUNNING:
            state.mark_success(result=text[:2000])

    # ==================================================================
    # 失败子任务重试（第4章 4.2 / 第2章 2.1 规则4：代码任务错误最多重试 3 次）
    # 【BUG-C 1/2/5】达到重试上限 → 明确失败：
    #   · 子任务状态 = failed（不是 success）；
    #   · 思考链以 error 级别写入「发生错误：代码任务重试达到上限，任务终止」；
    #   · 流式推送同一条 error 事件（前端展示红色错误标识）；
    #   · 记录 retries_exhausted，供链路阻断与故障诊断报告使用。
    # ==================================================================
    # ==================================================================
    # 【队长-队员架构 · 需求 2/3】队长一致性校验 + 冲突仲裁分支
    #
    #   规则1 所有队员产生输出必须上报队长 → register_member_output()
    #   规则2 出现数据不一致/结论矛盾 → 自动触发仲裁     → detect() / arbitrate()
    #   规则3 队长可裁决采信版本 / 发起二次核验           → recheck 子任务下发
    #   规则4 裁决结果 = 链路唯一基准，强制所有队员沿用   → canonical / 指令注入
    #   规则5 原有串行链路保留，这里只新增仲裁分支
    #   规则6 仲裁思考过程在任务面板展示                 → think + 流式事件
    # ==================================================================
    async def _captain_consistency_check(self, *, root: TaskState, role: str, phase: str,
                                         res: SubtaskResult, model_label: str) -> None:
        """队员输出上报队长；仅在检测到冲突时走仲裁分支（无冲突时只记一条校验日志）。

        本方法由调用方 try/except 包裹：任何异常都降级为"链路继续"，绝不影响子任务分发。
        """
        snapshot = self.arbitration.register_member_output(
            session_id=root.session_id, task_id=res.task_id or root.task_id,
            phase=phase or res.title, agent_role=role, metadata=res.metadata,
        )
        if snapshot is None:
            return   # 非清单/候选类输出 → 无需一致性校验

        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        dispatch.think(
            root, "push",
            f"【队员上报】{role} 提交阶段「{snapshot.phase}」产出："
            f"待删候选 {snapshot.count} 项（扫描文件池 {len(snapshot.pool)} 个），已进入队长一致性校验",
        )

        detected = self.arbitration.detect(root.session_id)
        if detected is None:
            dispatch.think(
                root, "think",
                f"队长一致性校验：当前仅 {self.arbitration.snapshot_count(root.session_id)} 份队员清单，"
                "暂无可比对基准，先登记为基准候选",
            )
            return

        snap_a, snap_b, conflict = detected
        if not conflict.has_conflict:
            dispatch.think(
                root, "think",
                f"队长一致性校验通过：{conflict.summary()}",
            )
            self.db.log_agent(
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
                event="captain.consistency_ok", detail=conflict.summary(),
            )
            return

        # ---------------- 冲突 → 触发队长仲裁分支 ----------------
        # 【规则4 · 唯一基准不可被下游推翻】队长已完成仲裁后，唯一清单即为唯一基准：
        #   后续队员若与自己被要求沿用的基准发生分歧，只记为"队员偏离"，
        #   绝不再次仲裁去推翻已生效的裁决（否则会级联仲裁、最终清单被后到的清单带偏）。
        frozen = self.arbitration.canonical(root.session_id)
        if frozen and frozen.get("paths"):
            dispatch.think(
                root, "think",
                f"队长已冻结唯一基准清单（{len(frozen['paths'])} 项）：本次队员输出与基准不一致"
                f"（{conflict.summary()}），按规则以队长基准为准，不再重新仲裁",
                level=THINK_LEVEL_WARN,
            )
            self.db.log_agent(
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
                event="captain.canonical_enforced", level="warn",
                detail=(f"队员输出偏离队长唯一基准：{conflict.summary()}｜基准条目数="
                        f"{len(frozen['paths'])}｜差异文件：{'、'.join(conflict.diff_paths[:20])}"),
            )
            self.stream.emit(
                root.task_id, "agent_step", session_id=root.session_id,
                agent_role=AGENT_DISPATCH, model_label=model_label,
                status=STATUS_RUNNING, level=THINK_LEVEL_WARN,
                text=(f"⚖️ {AGENT_DISPATCH} 强制沿用已冻结的唯一基准清单"
                      f"（{len(frozen['paths'])} 项）：队员本次输出偏离基准，按规则以队长裁决为准"),
                step_type="think",
            )
            return

        dispatch.think(
            root, "think",
            f"⚠️ 队长一致性校验发现冲突：{conflict.summary()}｜"
            f"差异文件：{'、'.join(conflict.diff_paths[:10])}"
            + ("…" if conflict.diff_count > 10 else "") + " → 自动触发队长冲突仲裁流程",
            level=THINK_LEVEL_WARN,
        )
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=model_label,
            status=STATUS_RUNNING, level=THINK_LEVEL_WARN,
            text=(f"⚖️ {AGENT_DISPATCH} 触发冲突仲裁：两份待删清单不一致"
                  f"（{conflict.count_a} 项 vs {conflict.count_b} 项，差异 {conflict.diff_count} 个文件）"),
            step_type="think",
        )

        verdict = await self._run_captain_arbitration(root=root, snap_a=snap_a, snap_b=snap_b,
                                                      conflict=conflict)
        if verdict is None:
            return

        # 【规则3】队长选择"二次核验"时，真正下发核验子任务（只判冲突文件），
        #   并把核验结论回灌进唯一基准清单。
        if verdict.recheck_paths and verdict.decision == "recheck":
            try:
                outcome = await self._recheck_conflict_files(
                    root=root, paths=list(verdict.recheck_paths))
                if outcome.get("success"):
                    self.arbitration.apply_recheck(
                        root.session_id, keep=list(outcome.get("keep") or []),
                        drop=list(outcome.get("drop") or []), task_id=root.task_id)
                    verdict.recheck_done = True
                    self._refresh_canonical_snapshot(root.session_id, verdict)
                    dispatch_for_recheck: DispatchAgent = self.agents[AGENT_DISPATCH]
                    dispatch_for_recheck.think(
                        root, "think",
                        f"队长二次核验结论已并入唯一基准清单：保留 "
                        f"{len(outcome.get('keep') or [])} 个、剔除 "
                        f"{len(outcome.get('drop') or [])} 个；"
                        f"统一清单现 {len(self.arbitration.canonical_paths(root.session_id))} 项",
                    )
            except Exception as exc:  # noqa: BLE001 核验失败按保守裁决继续
                self.logger.exception_log(
                    error_code="CAPTAIN_RECHECK_FAILED",
                    message=f"队长二次核验子任务失败（已按原裁决继续）：{exc}",
                    session_id=root.session_id, task_id=root.task_id,
                    agent_role=AGENT_DISPATCH,
                )

        # 裁判思考过程 → 任务面板可见（规则6）
        report = render_arbitration_report(verdict)
        dispatch.think(root, "think", report)
        for line in self._arbitration_think_lines(verdict):
            dispatch.think(root, "think", line)
        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=model_label,
            status=STATUS_RUNNING,
            text=(f"⚖️ {AGENT_DISPATCH} 仲裁完成：采信 `{verdict.adopt_side or '保守口径'}`"
                  f"（阶段「{verdict.adopted_phase}」），"
                  f"差异 {conflict.diff_count} 个文件已列出，"
                  f"唯一统一待删清单 {len(verdict.canonical_paths)} 项，下游强制沿用"),
            step_type="think",
        )
        # 不覆盖上游已写好的 plan，追加到 result 由 _finalize 一并回传
        self._last_arbitration = verdict.to_dict()

    async def _run_captain_arbitration(self, *, root: TaskState, snap_a, snap_b,
                                       conflict) -> ArbitrationVerdict | None:
        """调用队长（调度规划Agent）模型做裁决，失败则后端保守兜底。"""
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]

        async def judge(user_content: str, *, attempt: int = 0) -> str:
            messages = [
                {"role": "system", "content": ARBITRATION_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ]
            response = await dispatch.call_model(
                root, messages,
                temperature=AGENT_TEMPERATURES[AGENT_DISPATCH],
                max_tokens=3000, expect_json=True,
                step_label=(f"队长一致性仲裁（第 {attempt + 1} 次）"
                            if attempt else "队长一致性仲裁"),
            )
            return response.text or ""

        return await self.arbitration.arbitrate(
            session_id=root.session_id, root_task=root, judge=judge,
            snap_a=snap_a, snap_b=snap_b, conflict=conflict,
        )

    def _refresh_canonical_snapshot(self, session_id: str, verdict: ArbitrationVerdict) -> None:
        """二次核验合并后，同步刷新裁决对象里的唯一清单（供报告展示一致）。"""
        paths = self.arbitration.canonical_paths(session_id)
        if paths:
            verdict.canonical_paths = list(paths)

    @staticmethod
    def _arbitration_think_lines(verdict: ArbitrationVerdict) -> list[str]:
        """把裁决拆成多条简短思考步骤（任务面板逐条展示，可读性更好）。"""
        conflict = verdict.conflict
        lines: list[str] = []
        if conflict is None:
            return lines
        lines.append(
            f"队长仲裁·差异定位：仅前一份列出 {len(conflict.only_a)} 个、"
            f"仅后一份列出 {len(conflict.only_b)} 个，共 {conflict.diff_count} 个冲突文件"
        )
        if conflict.only_a[:8]:
            lines.append("队长仲裁·仅前一份列为待删：" + "、".join(conflict.only_a[:8])
                         + ("…" if len(conflict.only_a) > 8 else ""))
        if conflict.only_b[:8]:
            lines.append("队长仲裁·仅后一份列为待删：" + "、".join(conflict.only_b[:8])
                         + ("…" if len(conflict.only_b) > 8 else ""))
        lines.append(
            f"队长仲裁·裁决落地：方式={verdict.decision}｜采信={verdict.adopt_side or '保守兜底'}"
            f"｜唯一清单 {len(verdict.canonical_paths)} 项（后续所有子任务强制沿用）"
        )
        if verdict.recheck_paths:
            lines.append(f"队长仲裁·二次核验：{len(verdict.recheck_paths)} 个冲突文件将单独复核")
        if verdict.degraded:
            lines.append("队长仲裁·降级说明：队长模型不可用，已由后端按保守口径兜底裁决（取条目更少的一份）")
        return lines

    def _canonical_block_for(self, session_id: str, instruction: str, *,
                             title: str) -> str:
        """【规则4】命中"会消费候选清单"的下游子任务时，返回队长唯一基准清单块。

        仅对清单/筛选/删除类子任务注入；其他子任务（纯文档解析、纯润色等）不注入，
        避免无意义地撑大 prompt。返回空串表示不注入。
        """
        block = self.arbitration.canonical_context_block(session_id)
        if not block:
            return ""
        text = f"{title}\n{instruction}"
        if not any(hint in text for hint in _CANONICAL_CONSUME_HINTS):
            return ""
        return block

    # ------------------------------------------------------------------
    async def _recheck_conflict_files(self, *, root: TaskState, paths: list[str]) -> dict:
        """【规则3】队长发起"冲突文件二次核验"子任务：只对冲突文件单独二次判定。"""
        if not paths:
            return {"keep": [], "drop": []}
        code_agent = self.agents[AGENT_CODE]
        from backend.bus.state_machine import TaskState as _TaskState

        recheck_task = _TaskState(
            task_id=self.router.new_task_id(), session_id=root.session_id,
            title="队长仲裁·冲突文件二次核验", agent_role=AGENT_CODE,
            parent_task_id=root.task_id,
        )
        recheck_task.deadline_at = root.deadline_at
        self.router.register_task(recheck_task)
        self.persist_task(recheck_task)
        recheck_task.mark_running("队长发起冲突文件二次核验")

        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        dispatch.think(
            recheck_task, "push",
            f"队长发起二次核验子任务：对 {len(paths)} 个冲突文件单独判定"
            f"（{'、'.join(paths[:5])}{'…' if len(paths) > 5 else ''}）",
        )
        self.stream.emit(
            root.task_id, "dispatch", session_id=root.session_id,
            from_agent=AGENT_DISPATCH, to_agent=AGENT_CODE, agent_role=AGENT_CODE,
            model_label=self.active_model_label(AGENT_CODE), status=STATUS_RUNNING,
            title="队长仲裁·冲突文件二次核验",
            text=(f"{AGENT_DISPATCH} → 分发任务给 {AGENT_CODE}："
                  f"对 {len(paths)} 个冲突文件单独二次判定"),
            subtask_task_id=recheck_task.task_id,
        )

        out = await code_agent.execute_instruction(
            recheck_task, build_recheck_instruction(paths))
        keep: list[str] = []
        if out.success:
            allowed = set(paths)
            for item in (out.candidates or []):
                p = str(item.get("path") or "").strip()
                if p in allowed and bool(item.get("suggest_delete", True)):
                    keep.append(p)
        # 二次核验结论落库 + 写入思考链（可审计）
        dispatch.think(
            recheck_task, "push",
            f"二次核验完成：{len(keep)} 个文件确认应删除、{len(paths) - len(keep)} 个文件判定保留"
            + ("" if out.success else f"（核验失败已按保守口径保留：{out.denied_reason or out.output[:120]}）"),
        )
        recheck_task.mark_success(result=f"二次核验完成，确认删除 {len(keep)} 项")
        self.persist_task(recheck_task)
        if recheck_task.status in (STATUS_SUCCESS, STATUS_FAILED):
            try:
                self.finish_task_timer(root.session_id, task_status=recheck_task.status,
                                       reason="队长二次核验子任务完成")
            except Exception:  # noqa: BLE001 计时收敛失败不影响业务
                pass
        drop = [p for p in paths if p not in set(keep)]
        return {"keep": sorted(keep), "drop": sorted(drop), "task_id": recheck_task.task_id,
                "success": bool(out.success)}

    async def _handle_failed_subtasks(self, root: TaskState, subtasks_plan: list[dict],
                                      results: list[SubtaskResult],
                                      image_paths: list[str]) -> list[SubtaskResult]:
        """对失败的子任务按重试上限逐条重试（第4章 4.2 / 第2章 2.1 规则4）。

        实现说明（【BUG-C 1】关键修复）：
          历史实现在 `for res in results` 里做重试，重试后**无法再回到本条子任务的重试判定**，
          于是无论上限是多少都只重试 1 次就跳出，最终以 success/普通失败收场 ——
          这正是"重试达到上限后链路仍继续向下流转"的根因之一。
          现在改为：每条失败子任务使用**独立的有界重试循环**（最多 MAX_SUBTASK_RETRIES 次），
          循环内继续失败且额度耗尽 → 统一走 `_mark_retry_exhausted` 明确失败收口。
        """
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        for res in results:
            if res.status != STATUS_FAILED:
                continue
            # 被上游阻断的子任务不做重试（其失败根源在上游）
            if res.failure_stage == "dependency_blocked":
                continue

            retried = 0
            last_decision = True          # 首次进入时的重试决策结果
            while retried < MAX_SUBTASK_RETRIES:
                state = self.load_task_state(res.task_id)
                if state is None:
                    break
                # 审批拒绝导致的失败不再重试（第3章 3.5 规则2）
                if state.error_code == "APPROVAL_REJECTED_BY_USER":
                    res.failure_reason = res.failure_reason or "高危操作被人工审批拒绝"
                    break

                if retried:
                    last_decision = dispatch.decide_retry(
                        state, res.error or state.error_message)
                res.retry_count = int(state.retry_count or 0)
                # 调度规划Agent 已判定无重试额度（且本次尚未使用任何额度）→ 直接收口
                if not last_decision and retried == 0:
                    self._mark_retry_exhausted(root, state, res, dispatch)
                    break

                plan_item = next((p for p in subtasks_plan if int(p["index"]) == res.index), None)
                if not plan_item:
                    break

                retried += 1
                self.persist_task(state)
                state.status = STATUS_RUNNING
                state.started_at = state.started_at or time.time()
                self.persist_task(state)
                res.retries_exhausted = False
                dispatch.think(
                    state, "think",
                    f"失败重试第 {retried}/{MAX_SUBTASK_RETRIES} 次："
                    f"{(res.error or state.error_message or '未知原因')[:200]}",
                )

                msg = Message(
                    session_id=root.session_id, task_id=state.task_id,
                    parent_task_id=root.task_id, sender_agent=AGENT_DISPATCH,
                    receiver_agent=plan_item["agent_role"], msg_type="task",
                    payload={"content": plan_item["instruction"], "metadata": {
                        "instruction": plan_item["instruction"],
                        "retry": res.retry_count or retried,
                        "previous_error": (res.error or "")[:800],
                    }},
                    status=STATUS_PENDING,
                )
                try:
                    reply = await self.bus.send_and_receive(msg)
                    await self._apply_reply(state, res, reply)
                except Exception as exc:  # noqa: BLE001
                    res.status = STATUS_FAILED
                    res.error = f"重试失败：{exc}"
                    res.failure_stage = "retry_failed"
                    res.failure_reason = res.error
                # 同步真实重试计数（以状态机落库值为准）
                state = self.load_task_state(res.task_id) or state
                res.retry_count = int(state.retry_count or 0)

                if res.status != STATUS_FAILED:
                    # 重试成功 → 退出重试循环，继续后续链路
                    self.persist_task(state)
                    break
                if not state.can_retry():
                    # 【BUG-C 1/2】额度耗尽仍失败 → 明确失败收口（不再向下流转）
                    self._mark_retry_exhausted(root, state, res, dispatch)
                    break
                self.persist_task(state)
            else:
                # 循环自然结束（重试次数用尽）仍失败 → 同样明确失败收口
                if res.status == STATUS_FAILED:
                    state = self.load_task_state(res.task_id)
                    if state is not None:
                        self._mark_retry_exhausted(root, state, res, dispatch)
        return results

    # ==================================================================
    # 【BUG-C 1/2/5】重试达到上限的统一失败收口
    #   · 子任务状态 = failed（绝不写成 success）
    #   · 错误码 = SUBTASK_RETRY_EXHAUSTED
    #   · 思考链 error 级别写入原文「发生错误：代码任务重试达到上限，任务终止」
    #   · 流式推送 subtask_failed 事件（前端红色错误标识）
    # ==================================================================
    def _mark_retry_exhausted(self, root: TaskState, state: TaskState,
                              res: SubtaskResult, dispatch: DispatchAgent | None = None) -> None:
        res.status = STATUS_FAILED
        res.retries_exhausted = True
        res.failure_stage = "retry_exhausted"
        res.retry_count = max(int(res.retry_count or 0), int(state.retry_count or 0))
        # 【增量修复 3】失败分类：IO 错误 vs 模型收敛失败（默认按模型收敛失败归类）
        if not res.failure_kind:
            res.failure_kind = classify_failure(state.error_code or res.failure_reason
                                                or res.error or "") \
                or FAILURE_KIND_MODEL_CONVERGE
        kind_label = FAILURE_KIND_LABEL.get(res.failure_kind, res.failure_kind)
        detail_reason = (res.error or state.error_message or f"已重试 {res.retry_count} 次仍失败")
        res.failure_reason = (
            f"{SUBTASK_RETRY_EXHAUSTED_MESSAGE}"
            f"（子任务「{res.title}」执行 Agent：{res.agent_role}；"
            f"失败类型：{kind_label}（{res.failure_kind}）；"
            f"已重试 {res.retry_count}/{MAX_SUBTASK_RETRIES} 次仍失败；根本原因：{detail_reason}）"
        )
        if state.status == STATUS_RUNNING:
            state.mark_failed(state.error_code or ERR_SUBTASK_RETRY_EXHAUSTED, res.failure_reason)
        else:
            state.error_code = state.error_code or ERR_SUBTASK_RETRY_EXHAUSTED
            state.error_message = res.failure_reason
        self.persist_task(state)
        # 思考链：error 级别（原文不得改写，前端展示红色「发生错误」标识）
        #   走调度规划Agent 的 think 落库 → 原始文本进入任务思考链（前端与 /api/task 可见）
        if dispatch is not None:
            dispatch.think(state, "exec", SUBTASK_RETRY_EXHAUSTED_MESSAGE,
                           level=THINK_LEVEL_ERROR)
        self._captain_step_emit(root, "exec", SUBTASK_RETRY_EXHAUSTED_MESSAGE,
                                level=THINK_LEVEL_ERROR)
        # 【增量修复 3】分类错误单独落一条 error 日志（IO / 模型收敛分开可查）
        if res.failure_kind == FAILURE_KIND_IO:
            error_code = ERR_IO
        elif res.failure_kind == FAILURE_KIND_MODEL_CONVERGE:
            error_code = ERR_MODEL_CONVERGE_FAIL
        else:
            error_code = ERR_SUBTASK_RETRY_EXHAUSTED
        self.logger.exception_log(
            error_code=error_code,
            message=(f"[{res.failure_kind or 'failure'}] 子任务「{res.title}」"
                     f"（{res.agent_role}）达到重试上限"
                     f"（{res.retry_count}/{MAX_SUBTASK_RETRIES}）终止：{detail_reason}"),
            session_id=root.session_id, task_id=state.task_id, agent_role=res.agent_role,
        )
        self.stream.emit(
            root.task_id, "subtask_failed", session_id=root.session_id,
            from_agent=res.agent_role, to_agent=AGENT_DISPATCH,
            agent_role=res.agent_role,
            model_label=self.active_model_label(res.agent_role),
            status=STATUS_FAILED, title=res.title,
            text=SUBTASK_RETRY_EXHAUSTED_MESSAGE,
            detail_reason=detail_reason,
            failure_kind=res.failure_kind,
            failure_kind_label=kind_label,
            retry_count=res.retry_count, max_retries=MAX_SUBTASK_RETRIES,
            level=THINK_LEVEL_ERROR,
            subtask_task_id=state.task_id, subtask_index=res.index,
        )

    # ==================================================================
    # 审批结果落地（第8章 POST /api/approval/submit 的核心；后端二次校验后调用）
    # ==================================================================
    async def resume_after_approval(self, *, approval_id: str, approved: bool,
                                    operator: str, session_id: str,
                                    tool: str, args: dict) -> dict:
        """审批通过 → 继续执行当前子任务；拒绝 → 终止子任务（第3章 3.5）。

        【需求点 Bug2 业务流程重构】两条恢复路径：
          ① 存在任务完整快照（正常业务链路：run_pipeline → 队长循环 → 审批中断）
             → 读取快照 → 注入审批结果 → **回到队长循环继续跑**，
               由队长决定下一个子任务 / 重试 / 换人 / 终止；
               未被队长判定完成的任务绝不交给交互交付Agent 出最终报告。
          ② 无快照（子任务级单测 / 直接调用代码工程Agent 的场景）
             → 保留历史行为：执行该动作并产出该子任务的结果与报告。
        """
        row = self.db.get_approval(approval_id)
        if not row:
            raise ApprovalError("审批记录不存在", code="APPROVAL_NOT_FOUND")

        snapshot = self.find_snapshot_for_approval(row)
        if snapshot is not None:
            return await self._resume_captain_loop(
                row=row, snapshot=snapshot, approved=approved, operator=operator,
                session_id=session_id, tool=tool, args=args)

        return await self._resume_subtask_after_approval(
            row=row, approved=approved, operator=operator, session_id=session_id,
            tool=tool, args=args)

    # ==================================================================
    # 【第三轮·Bug1 修复】审批"提交后"的 30 秒执行链路超时看门狗
    #   ------------------------------------------------------------------
    #   新版计时语义（严格按需求）：
    #     · 命中高危操作 → 中断循环 + 落快照 + 推送 approval_request
    #       → 进入 waiting_approval：**不启动任何计时**，用户想思考多久都行；
    #     · 后端收到 POST /api/approval/submit（或超时扫描代为裁决）的那一刻
    #       → start_resume_window() 写入 resume_deadline = 收到请求时刻 + 30s；
    #     · 30 秒内执行链路未收敛 → decide_resume_timeout()：子任务 failed +
    #       整个大任务 failed（需求原文"超时视为任务失败"）+ 推送 SSE 事件；
    #     · 链路正常收敛 → settle_resume_window() 关闭计时。
    #   看门狗只扫描 resume_state='running' 的记录：
    #     · 等待用户点击（idle）永不命中 → 不存在"用户没点按钮被判失败"；
    #     · settled / timeout 已终态 → 幂等，不会重复裁决。
    # ==================================================================
    def register_approval_timeout_handler(self) -> None:
        """把"执行链路超时之后如何收口"的回调注册进审批中心。

        职责分离：ApprovalCenter 负责落库裁决（超时 = 任务失败），
        Runtime 负责推送 SSE 事件并收敛运行时状态（停止在途恢复、终止循环）。
        """
        self.approval_center.register_timeout_handler(self._on_resume_timeout)

    def start_resume_window(self, *, approval_id: str, row: dict | None = None,
                            timeout_seconds: float | None = None) -> dict:
        """【第三轮·Bug1】由 API 层在"收到审批提交请求"时调用，开启 30 秒执行链路计时。

        补跑恢复（审批已是终态、任务未收敛）时窗口已被关闭：
        此时强制重置为 running，保证"提交即计时"的语义对补跑路径同样成立。
        """
        window = self.approval_center.start_resume_window(
            approval_id=approval_id, timeout_seconds=timeout_seconds)
        if not window.get("started"):
            # 已在计时中（同一单重复提交）→ 保留原窗口，绝不重置计时；
            # 其它情况（settled / timeout / 历史库默认 idle）→ 以本次请求时刻重新计时。
            current_state = str((self.db.get_approval(approval_id) or {})
                                .get("resume_state") or "idle")
            if current_state == "running":
                window["kept_existing"] = True
            else:
                self.db.force_start_approval_resume_window(
                    approval_id, deadline=window["deadline"], started_at=window["started_at"])
                window["started"] = True
                window["forced"] = True
        self.ensure_approval_watchdog()
        self.logger.task_log(
            session_id=str((row or {}).get("session_id") or ""),
            task_id=str((row or {}).get("task_id") or ""), agent_role=AGENT_DISPATCH,
            event="approval.resume_window.started",
            detail=(f"审批 {approval_id[:8]} 已受理 → 执行链路计时开始："
                    f"{float(window['timeout_seconds']):.0f}s（截止 "
                    f"{time.strftime('%H:%M:%S', time.localtime(window['deadline']))}）"),
        )
        return window

    async def _on_resume_timeout(self, execution, decision: dict) -> None:
        """审批提交后执行链路超时 → 任务失败收口（不再"等价 rejected 继续跑"）。"""
        approval_id = str(decision.get("approval_id") or "")
        row = self.db.get_approval(approval_id) or {}
        root_task_id = str(decision.get("root_task_id") or row.get("task_id") or "")
        session_id = str(row.get("session_id") or "")
        self.logger.task_log(
            session_id=session_id, task_id=root_task_id, agent_role=AGENT_DISPATCH,
            event="approval.resume_timeout.fail", level="error",
            detail=(f"审批 {approval_id[:8]} 提交后执行链路 "
                    f"{APPROVAL_RESUME_TIMEOUT_SECONDS:.0f}s 未收敛 → 任务判定失败并终止；"
                    "高危操作未执行"),
        )
        # 1) 终止该任务在途的恢复链路（避免超时判定之后旧协程继续跑）
        self.cancel_resume_tasks(root_task_id)
        # 2) 取消该任务仍挂起的其它审批单（同一轮可能有多条高危动作排队）
        self._cancel_pending_approvals_of_task(root_task_id)
        # 3) 推送 SSE：前端退出"等待审批"并显示失败原因（历史聊天记录只增不清）
        try:
            self.stream.emit(
                root_task_id, "approval_timeout",
                session_id=session_id, agent_role=AGENT_DISPATCH,
                status=STATUS_FAILED, level=THINK_LEVEL_ERROR,
                title="审批后执行链路超时",
                text=(f"⏱ 审批已提交，但后续 Agent 执行链路 "
                      f"{APPROVAL_RESUME_TIMEOUT_SECONDS:.0f} 秒未完成 → 判定任务失败并终止"
                      "（高危操作未执行）"),
                approval_id=approval_id,
                outcome=APPROVAL_OUTCOME_REJECTED,
                timed_out=True,
                approved=False,
                resume_timed_out=True,
                timeout_seconds=float(APPROVAL_RESUME_TIMEOUT_SECONDS),
            )
            self.stream.emit(
                root_task_id, "task_status", session_id=session_id,
                agent_role=AGENT_DISPATCH, status=STATUS_FAILED, level=THINK_LEVEL_ERROR,
                text="任务状态：failed（审批提交后执行链路超时）",
                approval_id=approval_id, error_code="APPROVAL_RESUME_TIMEOUT",
            )
        except Exception:  # noqa: BLE001 SSE 推送失败不影响失败收口
            pass
        try:
            self.finish_task_timer(session_id, task_status=STATUS_FAILED,
                                   task_id=root_task_id,
                                   reason="审批提交后执行链路 30s 超时，任务失败")
        except Exception:  # noqa: BLE001 计时收敛失败不影响业务
            pass

    def ensure_approval_watchdog(self) -> None:
        """确保执行链路超时看门狗在运行（幂等）。

        调用点：开始"提交后 30 秒计时"时 + 服务启动恢复时。
        """
        if self._approval_watchdog_task is not None and not self._approval_watchdog_task.done():
            return
        if self.approval_center.timeout_handler() is None:
            self.register_approval_timeout_handler()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return                    # 无事件循环（同步单测直接调审批中心）→ 不启用看门狗
        self._approval_watchdog_task = loop.create_task(self._approval_timeout_watchdog())

    # ==================================================================
    # 【修复·资源泄漏 / 断点恢复】服务启动时重建"待人工审批"的执行上下文
    #   ------------------------------------------------------------------
    #   历史缺陷：PendingExecution 只存在内存态，服务重启后丢失。
    #   此时用户点【✅ 执行一次】会在二次校验第 V5 道直接失败
    #   （APPROVAL_EXECUTION_EXPIRED），审批单既无法放行也无法执行，
    #   任务永久卡在 waiting_approval —— 与本轮"中断后可完整恢复"的硬性要求冲突。
    #   修复：启动时按 SQLite 快照里的 action_params / operation_params 重建并登记，
    #   指纹校验、mark_executed 幂等、30 秒超时看门狗全部继续生效。
    # ==================================================================
    def recover_pending_executions(self) -> dict:
        """启动时扫描 pending 审批 → 按快照/记录重建待执行动作 → 登记回审批中心。"""
        report = {"scanned": 0, "recovered": 0, "unrecoverable": 0, "task_ids": []}
        try:
            rows = self.db.list_pending_approvals(limit=200)
        except Exception as exc:  # noqa: BLE001 启动期失败不得阻止服务启动
            self.logger.exception_log(
                error_code="APPROVAL_RECOVERY_SCAN_FAILED",
                message=f"待审批动作重建扫描失败（已跳过）：{exc}", agent_role="system",
            )
            return report
        report["scanned"] = len(rows)
        for row in rows:
            approval_id = str(row.get("approval_id") or "")
            if not approval_id or self.approval_center.get_pending(approval_id) is not None:
                continue
            snapshot = self.snapshots.find_snapshot_for_task(str(row.get("task_id") or ""))
            execution = self._restore_pending_execution(row, snapshot or {})
            if execution is None:
                report["unrecoverable"] += 1
                self.logger.exception_log(
                    error_code="APPROVAL_EXECUTION_UNRECOVERABLE",
                    message=("待执行动作无法从快照重建（指纹缺失）：该审批单只能拒绝；"
                             "请重新发起任务"),
                    session_id=str(row.get("session_id") or ""),
                    task_id=str(row.get("task_id") or ""), agent_role=AGENT_CODE,
                )
                continue
            report["recovered"] += 1
            report["task_ids"].append(str(row.get("task_id") or ""))
            self.logger.task_log(
                session_id=str(row.get("session_id") or ""), task_id=str(row.get("task_id") or ""),
                agent_role=AGENT_CODE, event="approval.execution.recovered",
                detail=(f"服务重启后按快照重建待执行动作并登记：approval_id={approval_id[:8]} "
                        f"tool={execution.tool} fingerprint={execution.params_fingerprint[:12]}"),
            )
        if report["scanned"]:
            # 有悬挂审批 → 拉起看门狗。注意：等待用户点击阶段（resume_state='idle'）
            # 不会被超时命中；执行链路计时中的（'running'）由它负责秒级收口。
            self.ensure_approval_watchdog()
            self.logger.info(
                f"待审批动作重建完成：扫描 {report['scanned']} 条，"
                f"重建 {report['recovered']} 条，无法重建 {report['unrecoverable']} 条；"
                "等待用户点击阶段不计时（提交后才开始 30 秒执行链路计时）",
                agent_role="system",
            )
        return report

    async def _approval_timeout_watchdog(self) -> None:
        """执行链路超时看门狗：每 1 秒扫描一次"审批已提交且执行链路超 30 秒未收敛"的审批。

        幂等安全：判定与裁决都在 ApprovalCenter.decide_resume_timeout 内完成，
        且只对 resume_state='running' 生效 —— 等待用户点击（idle）的单永不命中，
        已收敛（settled）/ 已超时（timeout）的单不会被重复裁决。
        并发安全：多条审批同时超时时用 asyncio.gather **并行**收口。
        """
        try:
            while True:
                await asyncio.sleep(float(APPROVAL_WATCHDOG_INTERVAL_SECONDS))
                try:
                    expired = self.snapshots.expired_resume_approvals()
                except Exception as exc:  # noqa: BLE001 扫描失败不拖垮服务
                    self.logger.exception_log(
                        error_code="APPROVAL_TIMEOUT_SCAN_FAILED",
                        message=f"执行链路超时扫描失败（已忽略，下轮重试）：{exc}",
                        agent_role="system",
                    )
                    continue
                pending_handlers = []
                for row in expired:
                    approval_id = str(row.get("approval_id") or "")
                    if not approval_id:
                        continue
                    result = self.approval_center.decide_resume_timeout(approval_id)
                    if not result.get("timed_out"):
                        continue
                    decision = result.get("decision") or {}
                    handler = self.approval_center.timeout_handler()
                    if handler is not None:
                        pending_handlers.append((row, result, handler))
                if pending_handlers:
                    # 并行收口：单条失败只记日志，不影响其它已超时审批
                    await asyncio.gather(*[
                        self._run_timeout_handler(row, result, handler)
                        for row, result, handler in pending_handlers
                    ], return_exceptions=True)
        except asyncio.CancelledError:
            return

    async def _run_timeout_handler(self, row: dict, result: dict, handler) -> None:
        """执行单条超时收口（异常收敛为日志，绝不冒泡打断看门狗）。"""
        try:
            await handler(result.get("execution"), result.get("decision") or {})
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.logger.exception_log(
                error_code="APPROVAL_TIMEOUT_RESUME_FAILED",
                message=f"执行链路超时后的任务失败收口失败：{exc}",
                session_id=str(row.get("session_id") or ""),
                task_id=str(row.get("task_id") or ""),
                agent_role=AGENT_DISPATCH,
            )

    def _approval_timeout_emit(self, row: dict, decision: dict) -> None:
        """【第三轮】执行链路超时的 SSE 事件推送（普通 message 流与审批事件严格区分）。

        事件名：approval_timeout（专用）+ task_status（failed）。
        语义：审批**已提交**但执行链路 30 秒未收敛 → 任务失败（不是"用户没点按钮"）。
        """
        root_task_id = str(decision.get("root_task_id") or row.get("task_id") or "")
        if not root_task_id:
            snapshot = self.snapshots.find_snapshot_for_task(str(row.get("task_id") or "")) or {}
            root_task_id = str(snapshot.get("task_id") or row.get("task_id") or "")
        detail = {
            "approval_id": decision.get("approval_id"),
            "outcome": APPROVAL_OUTCOME_REJECTED,
            "timed_out": True,
            "resume_timed_out": True,
            "approved": False,
            "timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS),
            "operation_type": row.get("operation_type") or "",
            "operation_desc": row.get("operation_desc") or "",
            "danger_reason": row.get("danger_reason") or "",
            "risk_level": row.get("risk_level") or "",
        }
        try:
            self.stream.emit(
                root_task_id, "approval_timeout",
                session_id=str(row.get("session_id") or ""),
                agent_role=str(row.get("agent_role") or AGENT_CODE),
                status=STATUS_FAILED, level=THINK_LEVEL_ERROR,
                title="审批后执行链路超时",
                text=(f"⏱ 审批 {str(decision.get('approval_id') or '')[:8]} 已提交，"
                      f"但后续 Agent 执行链路 {APPROVAL_RESUME_TIMEOUT_SECONDS:.0f} 秒未完成"
                      " → 判定任务失败（高危操作未执行）"),
                **detail,
            )
            self.stream.emit(
                root_task_id, "task_status",
                session_id=str(row.get("session_id") or ""),
                agent_role=str(row.get("agent_role") or AGENT_CODE),
                status=STATUS_FAILED, level=THINK_LEVEL_ERROR,
                text="任务状态：failed（审批提交后执行链路超时）",
                error_code="APPROVAL_RESUME_TIMEOUT", **detail,
            )
        except Exception:  # noqa: BLE001 SSE 推送失败不影响超时裁决本身
            pass

    def cancel_resume_tasks(self, task_id: str) -> int:
        """取消该任务在途的"审批恢复"协程（超时判定 / 任务终止 / 关停时调用）。

        返回取消的协程数。取消是安全的：审批单与快照都已落库，
        需要时可重新提交或由启动恢复逻辑接管，不会丢状态。
        """
        cancelled = 0
        wanted = str(task_id or "")
        targets = []
        if wanted and wanted in self._resume_tasks:
            targets.append(self._resume_tasks.pop(wanted))
            self._resume_inflight.discard(wanted)
        for run_task in targets:
            if run_task is not None and not run_task.done():
                run_task.cancel()
                cancelled += 1
        if cancelled:
            self.logger.task_log(
                session_id="", task_id=wanted, agent_role=AGENT_DISPATCH,
                event="approval.resume.cancelled", level="warn",
                detail=f"已取消在途的审批恢复协程 {cancelled} 个（任务 {wanted[:8]}）",
            )
        return cancelled

    async def aclose_approval_watchdog(self) -> None:
        """停止看门狗 + 取消在途的审批恢复协程（服务关停时调用，避免悬挂协程/资源泄漏）。

        注意：取消的是**进程内正在跑的恢复协程**。审批单本身已落库到 SQLite，
        下次启动由 recover_pending_executions() 重建待执行动作后仍可继续，不会丢状态。
        """
        task = self._approval_watchdog_task
        self._approval_watchdog_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 关停容错
                pass
        # ---- 取消在途的审批恢复任务（防止关停后仍有协程调用模型/写库） ----
        inflight = list(self._resume_tasks.values())
        self._resume_tasks.clear()
        self._resume_inflight.clear()
        for run_task in inflight:
            if run_task is not None and not run_task.done():
                run_task.cancel()
        for run_task in inflight:
            if run_task is None:
                continue
            try:
                await run_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 关停容错
                pass

    # ==================================================================
    # 【新增】非阻塞审批恢复（修复"审批提交后卡在提交中"）
    #   ------------------------------------------------------------------
    #   历史缺陷：POST /api/approval/submit 内部同步 await 整轮队长循环（含真实模型调用），
    #   前端按钮因此长时间停在「提交中…」，一旦模型慢/超时，用户以为系统卡死。
    #   修复：接口立刻返回受理回执；恢复执行作为后台任务运行，
    #         进度通过 SSE（approval_result / task_status / done）推送，
    #         前端用 /chain 快照 + 会话接口刷新，**历史聊天内容只追加、绝不清空**。
    # ==================================================================
    def resume_in_progress(self, task_id: str) -> bool:
        """该任务是否正在后台执行"审批后恢复"。"""
        return bool(task_id) and task_id in self._resume_inflight

    def spawn_resume_task(self, coro, *, approval_id: str, session_id: str,
                          task_id: str) -> asyncio.Task:
        """把审批恢复协程登记为后台任务，并接管其结局（成功/异常都要收敛状态）。"""
        task = asyncio.ensure_future(coro)
        if task_id:
            self._resume_inflight.add(task_id)
            self._resume_tasks[task_id] = task

        def _cleanup(_t: asyncio.Task, *, _task_id=task_id) -> None:
            self._resume_inflight.discard(_task_id)
            self._resume_tasks.pop(_task_id, None)

        def _done(finished: asyncio.Task) -> None:
            try:
                outcome = finished.result()
            except asyncio.CancelledError:
                _cleanup(finished)
                return
            except Exception as exc:  # noqa: BLE001 恢复失败必须留痕 + 通知前端
                _cleanup(finished)
                self.logger.exception_log(
                    error_code="APPROVAL_RESUME_FAILED",
                    message=f"审批后恢复业务循环失败：{exc}",
                    session_id=session_id, task_id=task_id, agent_role=AGENT_DISPATCH,
                )
                try:
                    self.stream.emit(
                        task_id, "task_status", session_id=session_id,
                        agent_role=AGENT_DISPATCH, status=STATUS_FAILED,
                        level=THINK_LEVEL_ERROR, title="审批后恢复失败",
                        text=f"审批已受理，但恢复调度失败（已留痕，可重发指令）：{exc}",
                        approval_id=approval_id,
                    )
                except Exception:  # noqa: BLE001
                    pass
                return
            _cleanup(finished)
            final_status = str((outcome or {}).get("status") or "")
            # ==========================================================
            # 【第三轮·Bug1】执行链路已收敛 → 关闭"提交后 30 秒"计时窗口。
            #   settle 用非强制模式：若看门狗已把它判定为 timeout，
            #   则保留 timeout 结论（超时优先，不被迟到的收敛覆盖，便于审计追溯）。
            # ==========================================================
            try:
                self.approval_center.settle_resume_window(approval_id=approval_id)
            except Exception:  # noqa: BLE001 计时收敛失败不影响业务结果
                pass
            # 恢复结束 → 推送终态事件（前端据此收口，不再需要长连接等待）
            try:
                self.stream.emit(
                    task_id, "task_status", session_id=session_id,
                    agent_role=AGENT_DISPATCH,
                    status=final_status or STATUS_RUNNING,
                    title="审批恢复完成",
                    text=(f"审批 {approval_id[:8]} 恢复执行结束：状态={final_status or 'running'}"
                          f"｜已完成 {(outcome or {}).get('loop', {}).get('completed', 0)} 个子任务"),
                    approval_id=approval_id,
                    resume_settled=True,
                )
            except Exception:  # noqa: BLE001 SSE 推送失败不影响业务
                pass

        task.add_done_callback(_done)
        return task

    def approval_result_emit(self, row: dict, *, approved: bool, operator: str,
                            outcome: str) -> None:
        """审批裁决后立刻推送 SSE 事件（普通 message 流与审批事件严格区分）。

        事件名 approval_result（既有）+ stream_resumed（新增语义提示）：
        前端只据此退出"待审批阻塞态"，**不得覆盖或清空任何已有聊天内容**。
        """
        snapshot = self.snapshots.find_snapshot_for_task(str(row.get("task_id") or ""))
        root_task_id = str((snapshot or {}).get("task_id") or row.get("task_id") or "")
        text = (f"✅ 审批 {str(row.get('approval_id') or '')[:8]}：执行一次（{outcome}），"
                "正在按任务快照从断点继续执行" if approved
                else f"❌ 审批 {str(row.get('approval_id') or '')[:8]}：拒绝（{outcome}），"
                     "高危操作未执行，当前子任务标记失败并回传队长")
        for event_name in ("approval_result", "stream_resumed"):
            try:
                self.stream.emit(
                    root_task_id, event_name,
                    session_id=str(row.get("session_id") or ""),
                    agent_role=str(row.get("agent_role") or AGENT_CODE),
                    status=STATUS_RUNNING, level=THINK_LEVEL_INFO,
                    title="审批结果已回灌", text=text,
                    approval_id=str(row.get("approval_id") or ""),
                    outcome=outcome, approved=bool(approved), operator=operator,
                    operation_type=str(row.get("operation_type") or ""),
                    operation_desc=str(row.get("operation_desc") or ""),
                )
            except Exception:  # noqa: BLE001 SSE 推送失败不影响业务
                pass
        # 快照状态回 running，并清空审批截止时间（超时窗口随裁决关闭）
        try:
            self.snapshots.mark_running(root_task_id)
            snap = self.snapshots.load_snapshot(root_task_id)
            if snap is not None and snap.get("status") == STATUS_WAITING_APPROVAL:
                snap["approval_deadline"] = None
                self.db.save_task_snapshot(snap)
        except Exception:  # noqa: BLE001 状态回写失败由后续业务循环纠正
            pass

    # ==================================================================
    # 【需求点 Bug2】路径①：按任务完整快照恢复队长业务循环
    # ==================================================================
    async def _resume_captain_loop(self, *, row: dict, snapshot: dict, approved: bool,
                                   operator: str, session_id: str,
                                   tool: str, args: dict) -> dict:
        root_task_id = str(snapshot.get("task_id") or row["task_id"])
        root = self.load_task_state(root_task_id)
        if root is None:
            raise ApprovalError("审批关联任务不存在", code="TASK_NOT_FOUND")

        run = CaptainRun.from_state(dict(snapshot.get("loop_state") or {}))
        run.plan = dict(snapshot.get("plan") or run.plan or {})
        if not run.user_input:
            run.user_input = str(snapshot.get("user_input") or "")

        # ---- 恢复执行上下文：优先用内存态待执行动作，服务重启时按快照重建 ----
        execution = self.approval_center.get_pending(row["approval_id"])
        if execution is None and approved:
            execution = self._restore_pending_execution(row, snapshot)
            if execution is None:
                raise ApprovalError(
                    "审批对应的待执行动作已失效（服务重启且快照缺少动作参数），拒绝放行",
                    code="APPROVAL_EXECUTION_EXPIRED",
                )
        if execution is not None and (
                execution.tool not in (row.get("operation_params") or "")
                and execution.tool not in (row.get("operation_desc") or "")):
            raise ApprovalError(
                f"审批记录内容与待执行动作不一致（记录 #{row['approval_id'][:8]}），拒绝执行",
                code="APPROVAL_TOOL_MISMATCH",
            )

        # 恢复后立刻把根任务从 waiting_approval 拉回 running（业务循环继续）
        try:
            root.transition(STATUS_RUNNING, reason="审批结果回灌，恢复队长业务循环")
        except IllegalTransition:
            root.status = STATUS_RUNNING
        self.persist_task(root)

        result = PipelineResult(
            session_id=root.session_id, task_id=root.task_id, status=STATUS_RUNNING,
            workspace_root=run.workspace_root,
        )
        result.plan = dict(run.plan or {})
        self.stream.open(root.task_id, root.session_id)

        # ---- 找到当前被挂起的子任务（快照里 remaining 的第一项） ----
        current_item = dict((snapshot.get("payload") or {}).get("current_work") or {})
        sub_task_id = str(row.get("task_id") or current_item.get("task_id") or "")
        sub_state = self.load_task_state(sub_task_id) if sub_task_id else None
        if sub_state is None:
            sub_state = TaskState(
                task_id=sub_task_id or self.router.new_task_id(), session_id=root.session_id,
                title=str(current_item.get("title") or "高危操作子任务"),
                agent_role=str(row.get("agent_role") or AGENT_CODE),
                parent_task_id=root.task_id,
            )
            self.router.register_task(sub_state)
        sub_state.parent_task_id = root.task_id

        idx = int(current_item.get("index", 0))
        key = int(current_item.get("result_key", idx))
        # 必须复用**同一个结果对象**（不能替换）：队长循环、审批裁决判定、
        # 后端执行回执都挂在同一个对象上，换对象会丢掉这些运行时标记。
        res = run.results.get(key)
        if res is None:
            res = SubtaskResult(
                index=idx, title=str(current_item.get("title") or "高危操作子任务"),
                agent_role=str(row.get("agent_role") or AGENT_CODE),
                status=STATUS_WAITING_APPROVAL, task_id=sub_state.task_id,
                result_key=key,
            )
            run.results[key] = res
        res.task_id = sub_state.task_id or res.task_id
        # 审批单号必须回填到结果对象：恢复后按它判断"审批是否已裁决"，
        # 进而决定是"直接后端执行"还是"重新走审批门禁"。
        res.approval_id = row["approval_id"]
        res.status = STATUS_WAITING_APPROVAL
        # 【第三轮·修缺陷】标记"本结果是审批挂起后恢复的那一轮"：
        #   队长循环据此决定是否走后端原生执行分支；校验评估Agent 打回后重新下发的
        #   那一轮没有该标记，因此一定会真正下发队员执行（带上校验修改意见）。
        res._resume_pending_intent = True   # type: ignore[attr-defined]
        # 被挂起的子任务必须留在待执行队首：审批结果回灌后，队长循环第一轮就是
        # "处理这个子任务的结果"，从而不会漏跑 / 跳过被中断的那个子任务。
        # 只有"该子任务的审批结果尚未注入"时才需要把它放回队首：
        # 审批结果已经在回调里写入结果对象时，说明这一轮就是处理它的结果，
        # 再插回队列会导致它被重复执行（历史缺陷：出现同一子任务重复跑的现象）。
        # 需要把被挂起的子任务放回队首的两种情形：
        #   ① 审批结果尚未注入结果对象（旧路径：等回调写入结果）；
        #   ② 审批结果已注入，但该子任务还带"待后端原生执行的删除动作"
        #      （必须让队长循环第一轮就把它交给后端执行器，不能直接收口）。
        need_requeue = bool((res.metadata or {}).get("delete_intent")) or (
            not self._approval_already_decided(res))
        if need_requeue:
            # 该子任务的工作项可能已经在队列里（暂停时保留了它），避免重复插入：
            #   先按 task_id 精确匹配（已分发过的子任务），再按下标匹配（尚未分发、无 task_id）
            idx_hits = [w for w in run.work if int(w.get("index", -1)) == idx]
            queued = next((w for w in run.work
                           if res.task_id and str(w.get("task_id") or "") == str(res.task_id)),
                          None)
            if queued is None and len(idx_hits) == 1:
                queued = idx_hits[0]
            if queued is None and not run.work:
                queued = {**current_item, "index": idx, "result_key": key}
                run.work.insert(0, queued)
                current_item = queued
            if queued is not None:
                queued["result_key"] = key
                queued["delete_commit"] = (queued.get("delete_commit")
                                           or (res.metadata or {}).get("delete_intent") or {})
        current_item = run.work[0] if run.work else (
            dict((snapshot.get("payload") or {}).get("current_work") or {})
            or {"index": idx, "title": res.title, "agent_role": res.agent_role,
                "instruction": "", "depend_on": [], "parallel_group": 0,
                "result_key": key})
        run.sync_with_work()

        self.stream.emit(
            root.task_id, "agent_step", session_id=root.session_id,
            agent_role=AGENT_DISPATCH, model_label=self.active_model_label(AGENT_DISPATCH),
            status=STATUS_RUNNING, step_type="think",
            text=(f"🔓 审批结果已回灌（{'✅ 执行一次' if approved else '❌ 拒绝'}，审批人 {operator}）："
                  f"按 task_id={root.task_id} 读取完整任务快照，恢复队长业务循环继续执行"),
            approval_id=row["approval_id"], approved=bool(approved),
        )

        outcome: dict[str, Any] = {}
        # 【需求点 Bug1】后端硬编码删除动作：没有 Agent 执行上下文（也不需要），
        #   这里只注入审批结论，真正的磁盘动作由队长循环调用后端执行器完成。
        backend_owned = str(row.get("operation_type") or "") in {
            HIGH_RISK_OP_LABEL.get(op, "") for op in HIGH_RISK_OP_DELETE_SET}
        backend_owned = backend_owned or str((res.metadata or {})
                                             .get("delete_intent", {}).get("operation") or "") in \
            HIGH_RISK_OP_DELETE_SET
        if backend_owned:
            if approved:
                res.status = STATUS_SUCCESS
                res.output = (f"高危动作已获人工审批（审批人 {operator}）："
                              f"{row.get('operation_desc')}；由后端 Python 立即执行磁盘删除。")
                outcome = {"task_action": "resume", "executed": True, "backend_native": True,
                           "tool": str(row.get("operation_type") or ""), "output": res.output,
                           "artifacts": [], "tool_records": []}
            else:
                res.status = STATUS_FAILED
                res.output = (f"人工审批已拒绝高危操作「{row.get('operation_desc')}」，"
                              "后端未触碰磁盘，当前子任务标记 failed 并回传队长。")
                res.error = "APPROVAL_REJECTED_BY_USER"
                res.failure_stage = "approval_rejected"
                res.failure_reason = "高危删除被人工审批拒绝，磁盘未做任何改动"
                outcome = {"task_action": "resume", "executed": False, "backend_native": True,
                           "rejected": True, "tool": str(row.get("operation_type") or ""),
                           "output": res.output, "tool_records": []}
            self._approval_result_emit(root, row, approved=approved, text=(
                ("✅ 用户选择【执行一次】：后端将立即执行磁盘删除并逐项回执"
                 if approved else
                 "❌ 用户选择【拒绝】：后端未触碰磁盘，子任务标记 failed 回传队长")))
        elif approved:
            # ---- allowed_once：执行高危操作，继续队员 Agent 任务 ----
            self.resume_task_timer(session_id, reason="人工审批通过，继续执行并恢复计时")
            code_agent = self.agents[AGENT_CODE]
            code_result = await code_agent.resume_after_approval(
                sub_state, row["approval_id"], execution, approved=True)
            try:
                audit = self._approval_audit_block(sub_state)
                if audit:
                    code_result.output = f"{code_result.output}\n\n{audit}".strip()
            except Exception as exc:  # noqa: BLE001 审计块失败不影响业务结果
                self.logger.exception_log(
                    error_code="APPROVAL_AUDIT_BLOCK_FAILED",
                    message=f"审批留痕块生成失败（已降级）：{exc}",
                    session_id=session_id, task_id=sub_state.task_id, agent_role=AGENT_CODE,
                )
            res.status = STATUS_SUCCESS if code_result.success else STATUS_FAILED
            res.output = code_result.output
            res.error = "" if code_result.success else (code_result.denied_reason or code_result.output)
            res.metadata = {"after_approval": True, "approved": True, "operator": operator}
            # 子任务状态按真实执行结果收敛（队员执行路径；删除类动作走后端原生执行分支）
            if sub_state.status not in (STATUS_SUCCESS, STATUS_FAILED):
                try:
                    if code_result.success:
                        sub_state.mark_success(result=str(code_result.output or "")[:2000])
                    else:
                        sub_state.mark_failed(
                            code_result.failure_code or "POST_APPROVAL_FAILED",
                            str(res.error or "审批后执行失败"))
                except IllegalTransition:
                    sub_state.status = STATUS_SUCCESS if code_result.success else STATUS_FAILED
                self.persist_task(sub_state)
            outcome = {
                "task_action": "resume",
                "executed": True,
                "tool": execution.tool,
                "output": code_result.output,
                "artifacts": list(code_result.artifacts or []),
                "tool_records": [
                    {"tool": r.tool, "ok": r.ok, "high_risk": r.high_risk,
                     "approved": r.approved, "output": str(r.output or "")[:1500]}
                    for r in (code_result.tool_records or [])
                ],
            }
            self._approval_result_emit(root, row, approved=True, text=(
                f"✅ 用户选择【执行一次】：高危操作「{execution.tool}」"
                f"{'已执行成功' if code_result.success else '执行失败'}，"
                "队员结果回传队长校验后继续业务循环"))
        else:
            # ---- rejected：标记当前子任务 failed，把失败结果回传给队长 ----
            code_agent = self.agents[AGENT_CODE]
            pending_exec = execution or self._restore_pending_execution(row, snapshot)
            if pending_exec is not None:
                code_result = await code_agent.resume_after_approval(
                    sub_state, row["approval_id"], pending_exec, approved=False)
                res.output = code_result.output
                res.error = code_result.denied_reason or "APPROVAL_REJECTED_BY_USER"
            else:
                res.output = (
                    f"人工审批已拒绝高危操作「{row.get('operation_desc')}」，"
                    "按规则终止当前子任务，未执行任何文件写入或系统命令。"
                )
                res.error = "APPROVAL_REJECTED_BY_USER"
            res.status = STATUS_FAILED
            res.failure_stage = "approval_rejected"
            res.failure_reason = "高危操作被人工审批拒绝，当前子任务标记 failed 并回传队长"
            res.metadata = {"after_approval": True, "approved": False, "operator": operator}
            if sub_state.status != STATUS_FAILED:
                sub_state.mark_failed("APPROVAL_REJECTED_BY_USER", res.failure_reason)
                self.persist_task(sub_state)
            outcome = {
                "task_action": "resume",
                "executed": False,
                "tool": (pending_exec.tool if pending_exec is not None else tool),
                "output": res.output,
                "rejected": True,
                "tool_records": [],
            }
            self._approval_result_emit(root, row, approved=False, text=(
                "❌ 用户选择【拒绝】：高危操作未执行，当前子任务标记 failed，"
                "失败结果已回传队长，由队长按规则决定重试 / 换人 / 终止整个任务"))

        # ---- 快照更新：把审批结论写入快照的审批字段，并记录消息上下文 ----
        # 【修复·口径一致性】超时自动拒绝（timeout）必须如实记录为 rejected，
        #   否则快照里会写成 allowed_once，与审批记录终态（timeout）互相矛盾，
        #   也让"回看快照判断审批结论"的运维/自检误判。
        current_approval_state = str((self.db.get_approval(row["approval_id"]) or {})
                                    .get("state") or "")
        timed_out = current_approval_state == APPROVAL_STATE_TIMEOUT
        approval_snapshot = {
            **dict(snapshot.get("approval") or {}),
            "approval_id": row["approval_id"],
            "approved": bool(approved) and not timed_out,
            "operator": operator,
            "decided_at": row.get("decided_at") or time.time(),
            "outcome": ("rejected" if (timed_out or not approved) else "allowed_once"),
            "timed_out": timed_out,
            "approval_state": current_approval_state or APPROVAL_STATE_MANUAL,
            "result": str(res.output or "")[:2000],
        }
        snapshot["approval"] = approval_snapshot
        run.messages.append({
            "role": "user", "msg_type": "approval_result",
            "content": (f"【人工审批结果】{'✅ 执行一次' if approved else '❌ 拒绝'}"
                        f"｜审批人：{operator}｜操作：{row.get('operation_desc')}"),
            "at": time.time(),
        })
        run.messages.append({
            "role": "member", "agent": res.agent_role, "title": res.title,
            "status": res.status, "content": str(res.output or "")[:2000], "at": time.time(),
        })

        # ---- 审批结果注入后继续跑队长循环（下一轮分派 / 校验 / 终止） ----
        result = await self._captain_loop(run=run, result=result, root=root)
        result.subtasks = run.completed_dicts()
        # 【需求点 Bug1 硬性约束4】把后端原生执行回执一并向调用方回传，
        #   供前端与验收直接读取"真实删除了哪些、成功几项、失败原因"。
        backend_receipts = [
            getattr(r, "backend_execution").to_dict()
            for r in run.results.values()
            if getattr(r, "backend_execution", None) is not None
        ]
        result.plan["backend_receipts"] = backend_receipts
        outcome["backend_receipts"] = backend_receipts
        # 把审批结论保留在最终快照里（交付 / 失败收口时也要能回溯审批结论）
        if result.status in (STATUS_SUCCESS, STATUS_FAILED):
            self.save_captain_snapshot(
                run=run, root=root, result=result,
                stage="delivered" if result.status == STATUS_SUCCESS else "failed",
                approval=approval_snapshot)

        # ---- 回传最终文本（只有队长判定完成才是交付内容） ----
        reply_text = result.final_reply or outcome.get("output") or ""
        if result.status in (STATUS_SUCCESS, STATUS_FAILED):
            self.record_final_reply(
                session_id, root.task_id, reply_text,
                msg_type="result" if result.status == STATUS_SUCCESS else "error",
                status=result.status,
                metadata={"kind": "final_delivery", "status": result.status,
                          "after_approval": True,
                          "captain_completed": result.status == STATUS_SUCCESS},
            )
            try:
                self.finish_task_timer(
                    session_id, task_status=result.status,
                    reason="审批后队长业务循环收口，计时收敛")
            except Exception as exc:  # noqa: BLE001
                self.logger.exception_log(
                    error_code=ERR_TIMER_RECORD_FAILED,
                    message=f"审批后计时收敛失败（已降级）：{exc}",
                    session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
                )
        elif result.status == STATUS_WAITING_APPROVAL:
            try:
                self.finish_task_timer(session_id, task_status=STATUS_WAITING_APPROVAL,
                                       reason="后续高危操作再次审批，计时暂停")
            except Exception:  # noqa: BLE001
                pass

        await self.bus.flush_memory_events()
        self.logger.task_log(
            session_id=session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            event="approval.resumed.captain_loop",
            detail=(f"approved={approved} operator={operator} "
                    f"loop_status={result.status} completed={len(run.completed)} "
                    f"remaining={len(run.work)} retries={dict(run.retries)}"),
        )

        return {
            **outcome,
            "approval_id": row["approval_id"],
            "task_id": root.task_id,
            "subtask_task_id": res.task_id,
            "status": result.status,
            # 【需求点 Bug1 硬性约束4】report = 队长判定完成后交给用户的正式报告
            #   （含后端真实执行回执），output 保留为本次审批动作的简短说明。
            "report": reply_text,
            "final_reply": reply_text,
            "loop": {
                "completed": len(run.completed),
                "remaining": len(run.work),
                "retries": {str(k): int(v) for k, v in run.retries.items()},
                "replan_used": run.replan_used,
                "captain_decided_complete": result.status == STATUS_SUCCESS,
            },
            "pending_approval": result.pending_approval,
        }

    def _restore_pending_execution(self, row: dict, snapshot: dict):
        """服务重启后按快照重建待执行动作（保证审批通过仍能真实执行）。

        【修复·断点恢复】取参优先级（任何一级命中即可重建，指纹必须完全一致）：
          1) snapshot.payload.pending_approval.action_tool / action_params（本轮新增，最权威）；
          2) snapshot.approval.tool + operation_params；
          3) 审批记录 operation_params 里解析出的 operation（硬编码高危动作清单兜底）。
        历史缺陷：只认 (2)，而真实链路写入快照的 approval 载荷只有 operation_type
        没有 tool 字段 → 重启后一律重建失败，用户点【✅ 执行一次】直接 409
        APPROVAL_EXECUTION_EXPIRED，任务永久卡在 waiting_approval。
        """
        try:
            from backend.services.approval_center import PendingExecution, params_fingerprint
        except Exception:  # noqa: BLE001
            return None
        payload = dict(snapshot.get("payload") or {})
        pending_payload = dict(payload.get("pending_approval") or {})
        approval = dict(snapshot.get("approval") or {})

        # 【修复】action_params 在快照里是 **dict**（不是字符串）；
        #   历史缺陷（本轮发现）：直接 str(dict) 会得到单引号 Python 字面量，
        #   json.loads 必然抛错 → 参数解析为空 → 重建失败 → 审批永久卡死。
        #   兼容三种形态：dict 直接使用 / JSON 字符串解析 / 记录里的 operation_params。
        parsed: dict[str, Any] = {}
        raw_action = pending_payload.get("action_params")
        if isinstance(raw_action, dict) and raw_action:
            parsed = dict(raw_action)
        else:
            params_text = str(raw_action or row.get("operation_params")
                              or approval.get("operation_params") or "")
            try:
                if params_text.strip().startswith("{"):
                    loaded = json.loads(params_text)
                    if isinstance(loaded, dict):
                        parsed = loaded
            except (json.JSONDecodeError, ValueError, TypeError):
                parsed = {}

        tool = str(pending_payload.get("action_tool") or approval.get("tool")
                   or row.get("tool") or "")
        if not tool:
            # 兜底：硬编码高危动作清单（不依赖模型输出，纯后端常量）
            candidate = str(parsed.get("operation") or "")
            tool = candidate if candidate in HIGH_RISK_OP_SET else ""
        if not tool or not parsed:
            return None
        execution = PendingExecution(
            approval_id=str(row.get("approval_id") or ""),
            session_id=str(row.get("session_id") or snapshot.get("session_id") or ""),
            task_id=str(row.get("task_id") or ""),
            agent_role=str(row.get("agent_role") or AGENT_CODE),
            tool=tool, args=parsed, params_fingerprint=params_fingerprint(tool, parsed),
            danger_reason=str(row.get("danger_reason") or ""),
            operation_type=str(row.get("operation_type") or ""),
            # 审批 30 秒截止时间同样按记录还原（重启后超时判定口径不变）
            approval_deadline=float(row.get("approval_deadline") or 0.0),
        )
        # 重建后登记进审批中心，使 mark_executed / 二次校验继续可用
        try:
            self.approval_center.register_execution(execution)
        except Exception:  # noqa: BLE001 登记失败不影响"拒绝"路径
            pass
        return execution

    def _approval_result_emit(self, root: TaskState, row: dict, *, approved: bool, text: str) -> None:
        self.stream.emit(
            root.task_id, "approval_result", session_id=root.session_id,
            agent_role=str(row.get("agent_role") or AGENT_CODE),
            model_label=self.active_model_label(str(row.get("agent_role") or AGENT_CODE)),
            status=root.status, level=THINK_LEVEL_INFO,
            text=text, approval_id=row.get("approval_id"), approved=bool(approved),
        )

    async def _resume_subtask_after_approval(self, *, row: dict, approved: bool,
                                             operator: str, session_id: str,
                                             tool: str, args: dict) -> dict:
        """路径②：无任务快照时的历史行为（子任务级恢复，用于单测/直接调用场景）。"""
        task_state = self.load_task_state(row["task_id"])
        if task_state is None:
            raise ApprovalError("审批关联任务不存在", code="TASK_NOT_FOUND")

        self.bind_session(session_id)

        if not approved:
            # 已由审批中心落库为失败；这里补一次显式状态收敛与日志
            self.logger.task_log(
                session_id=session_id, task_id=task_state.task_id, agent_role=AGENT_CODE,
                event="approval.rejected", detail="人工拒绝，子任务终止（第3章 3.5 规则2）", level="warn",
            )
            # 【需求点 二、2-c】审批拒绝 → 子任务被终止 → 停止计时（写错误日志 + 计时记录）
            self.cancel_task_timer(session_id, task_id=task_state.task_id,
                                   reason="人工拒绝审批，任务终止")
            return {"task_action": "terminate", "task_id": task_state.task_id,
                    "status": STATUS_FAILED, "detail": "审批拒绝，子任务已终止"}

        # 【需求点 二、2-a】审批通过 → 恢复执行 → 恢复计时（暂停期间不计入耗时）
        self.resume_task_timer(session_id, reason="人工审批通过，继续执行并恢复计时")

        code_agent = self.agents[AGENT_CODE]
        execution = self.approval_center.get_pending(row["approval_id"])
        if execution is None:
            raise ApprovalError(
                "待执行动作已失效（服务重启或已执行），无法继续执行",
                code="APPROVAL_EXECUTION_EXPIRED",
            )

        # 二次校验：待执行动作的工具/参数必须与审批记录内容一致（后端不信任任何上游）
        if execution.tool not in (row.get("operation_params") or "") and \
                execution.tool not in (row.get("operation_desc") or ""):
            raise ApprovalError(
                f"审批记录内容与待执行动作不一致（记录 #{row['approval_id'][:8]}），拒绝执行",
                code="APPROVAL_TOOL_MISMATCH",
            )

        result = await code_agent.resume_after_approval(
            task_state, row["approval_id"], execution, approved=True
        )
        # 【需求点 1.4】审批留痕随子任务产出一起落库/回传：
        #   审批人 / 审批结果 / 审批时间 直接进入 result.output，供报告与前端直接消费。
        try:
            audit = self._approval_audit_block(task_state)
            if audit:
                result.output = f"{result.output}\n\n{audit}".strip()
        except Exception as exc:  # noqa: BLE001 审计块失败不影响业务结果
            self.logger.exception_log(
                error_code="APPROVAL_AUDIT_BLOCK_FAILED",
                message=f"审批留痕块生成失败（已降级，不影响执行结果）：{exc}",
                session_id=session_id, task_id=task_state.task_id, agent_role=AGENT_CODE,
            )
        self.persist_task(task_state)
        # 【需求点 二、2-b】审批后子任务已进入终态 → 同步收敛本次任务计时（幂等）
        #   仅当子任务已是终态（success/failed）时收敛；仍处于 running 时保持计时继续。
        if task_state.status in (STATUS_SUCCESS, STATUS_FAILED):
            try:
                self.finish_task_timer(
                    session_id, task_status=task_state.status,
                    reason="审批后子任务执行完毕，计时收敛",
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.exception_log(
                    error_code=ERR_TIMER_RECORD_FAILED,
                    message=f"审批后计时收敛失败（已降级）：{exc}",
                    session_id=session_id, task_id=task_state.task_id, agent_role=AGENT_CODE,
                )
        self.logger.task_log(
            session_id=session_id, task_id=task_state.task_id, agent_role=AGENT_CODE,
            event="approval.resumed",
            detail=f"success={result.success} need_approval={result.need_approval} "
                   f"denied={result.denied_reason} "
                   f"tools={[(r.tool, r.ok, r.approved, r.high_risk) for r in result.tool_records]} "
                   f"output={result.output[:200]}",
        )

        # 审批后若任务仍需再次审批（模型提出下一个高危动作），保持 waiting_approval
        if result.need_approval:
            task_state.mark_waiting_approval("后续高危操作再次强制审批")
            self.persist_task(task_state)
            # 【需求点 二、2-b】再次进入审批暂停 → 再次停表（不清零）
            try:
                self.finish_task_timer(session_id, task_status=STATUS_WAITING_APPROVAL,
                                       reason="后续高危操作再次审批，计时暂停")
            except Exception as exc:  # noqa: BLE001
                self.logger.exception_log(
                    error_code=ERR_TIMER_RECORD_FAILED,
                    message=f"再次审批暂停计时失败（已降级）：{exc}",
                    session_id=session_id, task_id=task_state.task_id, agent_role=AGENT_CODE,
                )
            pending = self.db.list_approvals(session_id=session_id, state="pending", limit=1)
            return {
                "task_action": "waiting_approval_again",
                "task_id": task_state.task_id,
                "status": STATUS_WAITING_APPROVAL,
                "pending_approval": pending[0] if pending else None,
                "detail": result.output,
            }

        if result.success:
            task_state.mark_success(result=result.output[:2000])
        else:
            task_state.mark_failed(result.denied_reason or "POST_APPROVAL_FAILED", result.output)
        self.persist_task(task_state)

        # 审批后子任务已终结 → 生成并通过交付Agent 润色最终回复（第4章 4.7 / 第3.5 规则1）
        await self.bus.flush_memory_events()
        reply_text = await self._post_approval_reply(task_state, result)
        self.record_final_reply(
            session_id, task_state.task_id, reply_text,
            msg_type="result" if result.success else "error",
            status=task_state.status,
            metadata={"kind": "final_delivery", "status": task_state.status,
                      "after_approval": True},
        )

        # 审批完成后返回该子任务结果，供前端展示
        return {
            "task_action": "resume",
            "task_id": task_state.task_id,
            "status": task_state.status,
            "output": reply_text,
            "artifacts": result.artifacts,
            "tool_records": [
                {"tool": r.tool, "ok": r.ok, "high_risk": r.high_risk,
                 "approved": r.approved, "output": r.output[:1500]}
                for r in result.tool_records
            ],
        }

    async def _post_approval_reply(self, task_state: TaskState, result) -> str:
        """审批恢复后的最终交付文本（经交互交付Agent 润色，业务结果不变）。

        【需求点 1.4/1.5】报告内必须记录：审批人、审批结果、审批时间；
        用户拒绝审批 → 明确写出「用户拒绝高危操作，无文件改动」。
        """
        dispatch: DispatchAgent = self.agents[AGENT_DISPATCH]
        audit = self._approval_audit_block(task_state)
        action_lines: list[str] = []
        for rec in (result.tool_records or []):
            if not rec.high_risk:
                continue
            action_lines.append(
                f"- `{rec.tool}`：{'已执行成功' if rec.ok else '未执行 / 失败'}"
                f"（{'人工审批通过' if rec.approved else '未获批准'}）"
                f"{'｜' + str(rec.output)[:200] if rec.output else ''}"
            )
        body = (
            f"## 高危操作审批结果\n\n"
            f"- 子任务：{task_state.title}\n"
            f"- 执行 Agent：{task_state.agent_role}\n"
            f"- 执行状态：{'成功' if result.success else '失败'}\n\n"
            f"{audit}\n"
            + ("## 实际执行动作\n\n" + "\n".join(action_lines) + "\n\n" if action_lines else "")
            + f"## 执行详情\n\n{result.output}"
        )
        try:
            polished = await self.agents[AGENT_DELIVERY].polish(task_state, body)
            return polished.text
        except Exception as exc:  # noqa: BLE001 润色失败不影响业务结果
            self.logger.exception_log(
                error_code="POST_APPROVAL_DELIVERY_FAILED",
                message=f"审批后交付润色失败，已回退原始业务文本：{exc}",
                session_id=task_state.session_id, task_id=task_state.task_id,
                agent_role=AGENT_DELIVERY,
            )
            return body

    def _approval_audit_block(self, task_state: TaskState) -> str:
        """【需求点 1.4/1.5】审批留痕：审批人 / 审批结果 / 审批时间（必然写入报告）。"""
        try:
            rows = self.db.list_approvals(session_id=task_state.session_id, limit=50)
        except Exception:  # noqa: BLE001 审计块失败不影响交付
            rows = []
        mine = [r for r in rows if r.get("task_id") == task_state.task_id]
        if not mine:
            return ""
        mine.sort(key=lambda r: float(r.get("decided_at") or r.get("created_at") or 0))
        lines = ["## 审批留痕（审批人 / 审批结果 / 审批时间）", ""]
        rejected = False
        for r in mine:
            state_label = {
                APPROVAL_STATE_MANUAL: "✅ 人工通过",
                APPROVAL_STATE_REJECTED: "❌ 人工拒绝",
                APPROVAL_STATE_PENDING: "⏳ 等待审批",
            }.get(r.get("state"), str(r.get("state") or ""))
            if r.get("state") == APPROVAL_STATE_REJECTED:
                rejected = True
            decided_at = r.get("decided_at")
            time_text = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(decided_at)))
                         if decided_at else "（尚未裁决）")
            lines.append(
                f"- 审批单 `{str(r.get('approval_id'))[:8]}`｜操作类型：{r.get('operation_type')}"
                f"｜风险等级：{r.get('risk_level')}"
                f"\n  - 审批人：**{r.get('decided_by') or '（未记录）'}**"
                f"\n  - 审批结果：**{state_label}**"
                f"\n  - 审批时间：{time_text}"
                f"\n  - 操作说明：{r.get('operation_desc') or '-'}"
            )
        if rejected:
            lines.append("")
            lines.append("> ⛔ **用户拒绝高危操作：删除流程已直接终止，无任何文件改动。**")
            lines.append("> 拒绝后不再下发后续删除动作，磁盘上的文件保持原样。")
        return "\n".join(lines)

    # ==================================================================
    # 收口：思考链 + Token统计 + 任务状态
    # ==================================================================
    def _finalize(self, result: PipelineResult, root: TaskState, started: float) -> PipelineResult:
        self.persist_task(root)

        # 【队长-队员架构 · 需求 3.6】队长仲裁思考过程与裁决结论随任务回传：
        #   前端任务面板 / 消息区可展示"队长做了什么裁决、差异文件是哪些、唯一清单多少项"。
        if self._last_arbitration is not None:
            result.plan["captain_arbitration"] = self._last_arbitration
            canonical = self.arbitration.canonical(root.session_id)
            if canonical:
                result.plan["canonical_candidates"] = {
                    "count": len(canonical.get("paths") or []),
                    "phase": canonical.get("phase") or "",
                    "decision": canonical.get("decision") or "",
                    "adopt_side": canonical.get("adopt_side") or "",
                    "paths": list(canonical.get("paths") or []),
                }

        # ==================================================================
        # 【需求点 二、2-b】任务结束（success/failed）→ 停止计时并写入会话任务记录
        #   · waiting_approval（暂停态）→ 只停表，计时不清零，审批通过后继续累计
        #   · 计时异常一律降级，绝不影响任务交付结果
        # ==================================================================
        try:
            timer_view = self.finish_task_timer(
                root.session_id, task_status=root.status,
                reason=("任务成功交付" if root.status == STATUS_SUCCESS
                        else "任务进入人工审批，计时暂停" if root.status == STATUS_WAITING_APPROVAL
                        else f"任务结束：{root.error_code or root.status}"),
            )
            if timer_view:
                result.plan["task_timer"] = timer_view
        except Exception as exc:  # noqa: BLE001
            self.logger.exception_log(
                error_code=ERR_TIMER_RECORD_FAILED,
                message=f"任务计时收敛失败（已降级，不影响交付）：{exc}",
                session_id=root.session_id, task_id=root.task_id, agent_role=AGENT_DISPATCH,
            )

        # 思考过程（第7.3 对话页面：AI思考过程，分步图标 + 连接线）
        # 【BUG-C 4/5】除根任务步骤外，一并并入各子任务的思考步骤
        #   （含 error 级别错误标识），这样前端思维链里能直接看到"哪一步失败"。
        steps = self.db.list_think_steps(root.task_id)
        think_chain = [{
            "index": s["step_index"], "type": s["step_type"], "text": s["step_text"],
            "agent": s["agent_role"],
            "level": (s["level"] if "level" in s.keys() else THINK_LEVEL_INFO) or THINK_LEVEL_INFO,
            "is_error": ((s["level"] if "level" in s.keys() else "") or "").lower()
                        == THINK_LEVEL_ERROR,
        } for s in steps]
        try:
            for child in self.db.list_child_tasks(root.task_id):
                for s in self.db.list_think_steps(child["task_id"]):
                    think_chain.append({
                        "index": s["step_index"], "type": s["step_type"],
                        "text": s["step_text"], "agent": s["agent_role"],
                        "level": ((s["level"] if "level" in s.keys() else "")
                                  or THINK_LEVEL_INFO),
                        "is_error": ((s["level"] if "level" in s.keys() else "") or "").lower()
                                    == THINK_LEVEL_ERROR,
                    })
        except Exception:  # noqa: BLE001 子任务思考链合并失败不影响交付
            pass
        result.think_steps = think_chain

        # 最终交付落库（统一消息结构体）。
        # 注意：仅终态落库 —— waiting_approval 是暂停态，恢复执行后还需追加审批后的思考过程并重新交付，
        # 若此时抢先落库，record_final_reply 的幂等保护会使审批后的结果无法入账。
        if result.final_reply and root.status in (STATUS_SUCCESS, STATUS_FAILED):
            self.record_final_reply(
                root.session_id, root.task_id, result.final_reply,
                msg_type="error" if root.status == STATUS_FAILED else "result",
                status=root.status,
                metadata={"kind": "final_delivery", "status": root.status,
                          "subtask_evaluation": result.plan.get("evaluation") or []},
            )

        # Token 统计全部来自后端 DB 真实数据（第2章 2.4 规则3，禁止前端伪造）
        summary = self.db.token_summary(session_id=root.session_id, task_id=None)
        session_total = self.db.token_summary(session_id=root.session_id)
        task_total = self.db.token_summary(task_id=root.task_id)
        result.stats = {
            "task": {
                "input_tokens": int(task_total.get("input_tokens") or 0),
                "output_tokens": int(task_total.get("output_tokens") or 0),
                "cached_tokens": int(task_total.get("cached_tokens") or 0),
                "cache_hit_rate": float(task_total.get("cache_hit_rate") or 0.0),
                "elapsed_ms": int(task_total.get("elapsed_ms") or 0),
                "steps": len(steps),
                "calls": int(task_total.get("calls") or 0),
                "iterations": root.iteration,
                "retries": root.retry_count,
                "review_rejects": root.review_rejects,
            },
            "session": {
                "input_tokens": int(session_total.get("input_tokens") or 0),
                "output_tokens": int(session_total.get("output_tokens") or 0),
                "cached_tokens": int(session_total.get("cached_tokens") or 0),
                "cache_hit_rate": float(session_total.get("cache_hit_rate") or 0.0),
                "elapsed_ms": int(session_total.get("elapsed_ms") or 0),
                "steps": len(steps),
                "calls": int(session_total.get("calls") or 0),
            },
            "wall_clock_ms": int((time.time() - started) * 1000),
            "limits": {
                "max_iterations": 20, "max_timeout_seconds": MAX_TASK_TIMEOUT_SECONDS,
                "max_retries": 2, "max_review_rejects": 2,
            },
            "agents": self.bus.stats,
            "router": self.router.stats,
        }
        result.degradation = {
            **self.model_client.degradation_status(),
            "vector_db_available": self.vector_store.available,
            "vector_db_note": "" if self.vector_store.available else "向量库异常，长期记忆已关闭，保留会话短期记忆",
            "notes": list(self.degraded_notes),
        }
        # 【需求点 三、2】补位事件注入结果：前端弹出非阻断轻提示 + 状态栏展示实际模型
        result.ecosystem_fallbacks = list(self.ecosystem_fallbacks)
        result.degradation["ecosystem_fallbacks"] = list(self.ecosystem_fallbacks)
        result.degradation["active_models"] = self.model_runtime_overview()
        if not self.vector_store.available:
            note = "向量库异常，长期记忆已关闭，仅保留会话短期记忆"
            if note not in self.degraded_notes:
                self.degraded_notes.append(note)

        if result.status != STATUS_WAITING_APPROVAL:
            result.status = root.status
        result.error_code = root.error_code or result.error_code
        result.error_message = root.error_message or result.error_message

        # ==================================================================
        # 【需求点 Bug2】推送终态事件并关闭流式通道
        #   waiting_approval 是暂停态：通道保持打开，审批恢复后继续推送。
        # ==================================================================
        try:
            self.stream.emit(
                root.task_id, "task_status", session_id=root.session_id,
                agent_role=AGENT_DISPATCH, status=result.status,
                model_label=self.active_model_label(AGENT_DISPATCH),
                text=(f"任务状态：{result.status}"
                      + (f"｜耗时 {result.stats.get('wall_clock_ms', 0) / 1000:.1f}s"
                         if result.stats else "")),
                final_reply=str(result.final_reply or "")[:4000],
                error_code=result.error_code,
            )
            if result.status != STATUS_WAITING_APPROVAL:
                self.stream.emit(
                    root.task_id, "done", session_id=root.session_id,
                    agent_role=AGENT_DISPATCH, status=result.status,
                    model_label=self.active_model_label(AGENT_DISPATCH),
                    text=f"本次任务链路结束（{result.status}）",
                    final_reply=str(result.final_reply or "")[:4000],
                )
                self.stream.close(root.task_id)
        except Exception:  # noqa: BLE001 流式收口失败绝不影响任务结果
            pass

        self.db.touch_session(root.session_id)
        return result

    # ==================================================================
    # 任务详情（第8章 GET /api/task/{task_id}）
    # ==================================================================
    def task_detail(self, task_id: str) -> dict | None:
        row = self.db.get_task(task_id)
        if not row:
            return None
        state = db_row_to_state(row)
        children = self.db.list_child_tasks(task_id)
        messages = self.db.list_messages(task_id)
        steps = self.db.list_think_steps(task_id)
        # 【BUG-C 2/5】子任务思考步骤一并返回（含 error 级别错误标识），
        #   保证「任务详情」与「消息区思维链」看到同一份失败定位信息。
        try:
            for child in children:
                steps = steps + self.db.list_think_steps(child["task_id"])
        except Exception:  # noqa: BLE001 子任务步骤合并失败不影响详情返回
            pass
        token_total = self.db.token_summary(task_id=task_id)
        token_rows = self.db.query(
            """SELECT agent_role, provider, model, SUM(input_tokens) AS input_tokens,
                      SUM(output_tokens) AS output_tokens, SUM(cached_tokens) AS cached_tokens
               FROM token_usage WHERE task_id=? GROUP BY agent_role, provider, model""",
            (task_id,),
        )
        approvals = [a for a in self.db.list_approvals(session_id=row["session_id"], limit=200)
                     if a["task_id"] == task_id]

        return {
            "task": state.to_dict(),
            "subtasks": [{
                "task_id": c["task_id"], "title": c["title"], "agent_role": c["agent_role"],
                "status": c["status"], "iteration": c["iteration"], "retry_count": c["retry_count"],
                "result": (c.get("result") or "")[:4000],
                "error_code": c.get("error_code") or "", "error_message": c.get("error_message") or "",
                "created_at": c["created_at"], "finished_at": c["finished_at"],
            } for c in children],
            "think_steps": [{
                "index": s["step_index"], "type": s["step_type"],
                "text": s["step_text"], "agent": s["agent_role"],
                # 【BUG-C 2/5】步骤级别（error → 前端红色错误标识）
                "level": (s["level"] if "level" in s.keys() else THINK_LEVEL_INFO)
                         or THINK_LEVEL_INFO,
                "is_error": ((s["level"] if "level" in s.keys() else "") or "").lower()
                            == THINK_LEVEL_ERROR,
            } for s in steps],
            "messages": [{
                "msg_id": m["msg_id"], "sender_agent": m["sender_agent"],
                "receiver_agent": m["receiver_agent"], "msg_type": m["msg_type"],
                "status": m["status"], "timestamp": m["timestamp"],
                "content": m["payload_content"][:4000],
            } for m in messages],
            "tokens": {
                "task": dict(token_total) | {"cache_hit_rate": token_total.get("cache_hit_rate", 0.0)},
                "by_agent": [{
                    "agent_role": r["agent_role"], "provider": r["provider"], "model": r["model"],
                    "input_tokens": r["input_tokens"], "output_tokens": r["output_tokens"],
                    "cached_tokens": r["cached_tokens"],
                } for r in token_rows],
            },
            "approvals": [{
                "approval_id": a["approval_id"], "operation_type": a["operation_type"],
                "risk_level": a["risk_level"], "state": a["state"],
                "operation_desc": a["operation_desc"], "danger_reason": a["danger_reason"],
                "created_at": a["created_at"],
            } for a in approvals],
            "agent_logs": [{
                "agent_role": g["agent_role"], "event": g["event"], "detail": g["detail"][:1500],
                "level": g["level"], "created_at": g["created_at"],
            } for g in self.db.list_agent_logs(task_id)],
            "limits": {
                "max_iterations": 20, "max_timeout_seconds": MAX_TASK_TIMEOUT_SECONDS,
                "max_retries": 2, "max_review_rejects": 2,
            },
            "degradation": {
                **self.model_client.degradation_status(),
                "vector_db_available": self.vector_store.available,
            },
        }

    # ==================================================================
    # 状态快照（供前端状态栏 / 系统信息）
    # ==================================================================
    def system_snapshot(self, session_id: str | None = None,
                        task_id: str | None = None) -> dict:
        session_tokens = self.db.token_summary(session_id=session_id) if session_id else {}
        # 【需求点 Bug9】当前活跃 task 的真实统计（前端底部统计栏 3 秒轮询的数据源）
        task_tokens = self.db.token_summary(task_id=task_id) if task_id else {}
        return {
            "agents": agent_registry_info(),
            # 【需求点 三、2】七大 Agent 实跑模型视图（状态栏展示实际运行模型）
            "agent_models": self.model_runtime_overview(),
            "ecosystem_fallbacks": list(self.ecosystem_fallbacks),
            "bus": self.bus.stats,
            "router": self.router.stats,
            "approval": self.approval_center.stats(),
            "vector_db": self.vector_store.stats(),
            "memory": self.agents[self.ctx_memory_role()].stats(),
            "session_tokens": session_tokens,
            "task_tokens": task_tokens,
            "active_task_id": task_id or "",
            "degradation": {
                **self.model_client.degradation_status(),
                "vector_db_available": self.vector_store.available,
                "notes": list(self.degraded_notes),
            },
            "root": str(self.paths.root),
            "limits": {
                "max_iterations": 20,
                "max_timeout_seconds": MAX_TASK_TIMEOUT_SECONDS,
                "max_retries": 2,
                "max_review_rejects": 2,
                "max_rate_limit_retries": 3,
                "key_test_timeout_seconds": 5,
                "max_upload_mb": 50,
            },
            "security": {
                "high_risk_approval_enabled": True,
                "high_risk_approval_locked": True,
                "session_isolation": True,
                "key_storage": "AES-256-GCM + SHA256 篡改校验",
                "password_storage": "bcrypt",
                "no_docker": True,
            },
            "readiness": self.readiness(),
        }

    def readiness(self) -> dict:
        configured = [p["provider"] for p in self.config.public_config()["providers"] if p["configured"]]
        return {
            "first_launch_completed": self.config.public_config()["first_launch_completed"],
            "configured_providers": configured,
            "any_configured": bool(configured),
            "any_tested_ok": self.config.any_provider_tested_ok(),
        }

    async def shutdown(self) -> None:
        # 【新增】先停掉 30 秒审批超时看门狗（避免关停后残留协程再次拉起调度）
        await self.aclose_approval_watchdog()
        try:
            await self.bus.drain_memory_queue(timeout=2.0)
        finally:
            await self.bus.shutdown()


__all__ = ["EcosystemRuntime", "PipelineResult", "SubtaskResult", "CAPABILITY_MATRIX"]
