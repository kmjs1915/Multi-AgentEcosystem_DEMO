# -*- coding: utf-8 -*-
"""队长仲裁中心（服务层）—— 队长-队员层级架构下的一致性校验与冲突裁决

架构定位（本模块对应"队长-队员"改造的需求 2 / 需求 3）：
    ✅ 队长：调度规划Agent —— 全局总控，唯一裁决权威
    👥 队员：代码工程Agent / 评估校验Agent / 交互交付Agent 等其余角色

    规则1：所有队员产生输出，必须上报队长（`register()` 是唯一上报入口）；
    规则2：队员之间出现信息冲突、数据不一致、结论矛盾（例如两份待删清单
           条目/数量不同）→ 自动触发队长仲裁流程（`detect_conflict()`）；
    规则3：队长可自主裁决采信版本、发起二次核验（`build_recheck_instruction()`
           生成的核验子任务由运行时下发）、裁决冲突结论；
    规则4：队长裁决后的结果 = 整个任务链路唯一基准，强制所有队员沿用
           （`canonical_context_block()` 注入下游指令 + `canonical` 为唯一数据源）；
    规则5：原有串行任务链路保留，本模块只**新增冲突仲裁分支**，不改变既有链路；
    规则6：队长仲裁思考过程通过 think 步骤 / agent_logs / 流式事件在 UI 展示。

硬性约束（本模块严格遵守）：
    · 不改动任务状态机、日志系统、token 统计、审批中心、文件分批与 JSON 清洗逻辑；
    · 不引入任何容器化依赖（纯裸机 Python 标准库）；
    · 仲裁结论一律落库留痕（agent_logs + 会话 artifact 快照），可审计、可追溯。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend.utils.constants import (
    AGENT_CODE,
    AGENT_DISPATCH,
    AGENT_TEMPERATURES,
)

# ==========================================================================
# 常量
# ==========================================================================
# 触发"清单/候选/筛选/文件作用总结"类任务的特征词（用于识别该上报哪些科目）
_LIST_INTENT_HINTS: tuple[str, ...] = (
    "待删除", "待删", "候选清单", "候选", "筛选", "删除清单", "清单",
    "文件作用", "总结每个文件", "识别调试", "识别测试",
)
# 明确执行删除的意图特征（用于判断下游是否会消费"唯一清单"）
_EXECUTE_INTENT_HINTS: tuple[str, ...] = ("删除", "清理", "移除", "清空")

# 冲突条目上限（防止超长 prompt 再次压垮模型）
MAX_CONFLICT_ROWS = 200
# 队长仲裁硬性失败重试次数
MAX_ARBITRATION_LLM_RETRIES = 2

CONFLICT_NONE = "none"                 # 无冲突
CONFLICT_ROW_DIVERGENCE = "row_divergence"       # 条目差异（同一文件池下清单不一致）
CONFLICT_COUNT_MISMATCH = "count_mismatch"       # 数量不一致但条目一致（罕见）

DECISION_ADOPT = "adopt"               # 采信版本
DECISION_INTERSECTION = "intersection"  # 取交集（更保守）
DECISION_RECHECK = "recheck"           # 生成二次核验条目

# 队长仲裁提示词（严格 JSON 契约，与既有 JSON 清洗/重试链路完全兼容）
ARBITRATION_SYSTEM_PROMPT = """你是「调度规划Agent」，在本系统中担任**队长**，是全局唯一总控与裁决权威。
你现在执行的是「队员输出一致性仲裁」职责。

【硬约束】
1. 你只做裁决，不执行任何文件读写/命令/代码落盘。你没有这些能力。
2. 裁决必须基于给定的两份清单事实，禁止臆造未给出的文件路径。
3. 裁决结果将作为整个任务链路**唯一基准**，强制所有队员沿用，因此必须给出明确、可执行的结论。

【输出格式 · 必须是合法 JSON，禁止任何多余文字】
{
  "decision": "adopt" | "intersection" | "recheck",
  "adopt_side": "member_a" | "member_b" | "",
  "reasoning": "裁决依据（≤300字，说明为什么采信该版本）",
  "conflict_kind": "row_divergence",
  "conflict_rows": ["差异文件的完整路径"],
  "recheck_paths": ["需要二次核验的文件路径"],
  "confidence": "high" | "medium" | "low",
  "risk_note": "裁决风险提示（一句话）"
}

【decision 取值规则】
- adopt        ：明确指出采信哪一份（adopt_side），差异文件按该份结论处理。**默认选项**。
- intersection ：两份都列为候选才保留（更保守，适合"删除"这类不可逆操作）。
- recheck      ：差异过大或无法判断时，指定 recheck_paths 交由队员二次核验。

【判定原则】
1. **删除不可逆，宁可保守**：同一文件池下两份清单不一致时，
   若差异文件属于"测试/调试/临时/构建产物/日志"特征，可采信将其列为候选的一方；
   若差异文件是正常源码、配置、文档、依赖清单等业务资产，**不得**列为删除候选。
2. 唯一差异文件路径必须逐字来自给定清单，禁止改写、禁止添加前缀。
3. conflict_rows 必须完整列出全部差异文件（不只是前几个）。
"""


class ArbitrationError(Exception):
    """仲裁流程异常（一律降级为"队长保守裁决"，绝不中断任务链路）。"""


# ==========================================================================
# 数据结构
# ==========================================================================
@dataclass
class ConflictReport:
    """队长对两份队员清单的一致性校验结果。"""

    has_conflict: bool = False
    kind: str = CONFLICT_NONE
    only_a: list[str] = field(default_factory=list)   # 仅 A 版列出
    only_b: list[str] = field(default_factory=list)   # 仅 B 版列出
    count_a: int = 0
    count_b: int = 0
    pool_a: int = 0
    pool_b: int = 0
    pool_same: bool = True
    phase_a: str = ""
    phase_b: str = ""

    @property
    def diff_paths(self) -> list[str]:
        return sorted(set(self.only_a) | set(self.only_b))

    @property
    def diff_count(self) -> int:
        return len(self.diff_paths)

    def summary(self) -> str:
        if not self.has_conflict:
            return (f"两份待删清单一致（均 {self.count_a} 项），无冲突，无需仲裁")
        return (
            f"两份待删清单不一致：{self.phase_a}={self.count_a} 项 / "
            f"{self.phase_b}={self.count_b} 项，差异 {self.diff_count} 个文件"
            f"（仅前一份列出 {len(self.only_a)} 个，仅后一份列出 {len(self.only_b)} 个）"
        )

    def to_dict(self) -> dict:
        return {
            "has_conflict": self.has_conflict,
            "kind": self.kind,
            "only_a": self.only_a,
            "only_b": self.only_b,
            "diff_paths": self.diff_paths,
            "diff_count": self.diff_count,
            "count_a": self.count_a,
            "count_b": self.count_b,
            "pool_a": self.pool_a,
            "pool_b": self.pool_b,
            "pool_same": self.pool_same,
            "phase_a": self.phase_a,
            "phase_b": self.phase_b,
            "summary": self.summary(),
        }


@dataclass
class CandidateSnapshot:
    """一名队员（一次队员输出）提交的待删候选清单快照。"""

    phase: str                 # 阶段标签，如"生成测试文件删除清单"
    task_id: str = ""
    agent_role: str = AGENT_CODE
    paths: list[str] = field(default_factory=list)
    items: list[dict] = field(default_factory=list)
    pool: list[str] = field(default_factory=list)     # 该次扫描到的文件池
    created_at: float = field(default_factory=time.time)

    @property
    def count(self) -> int:
        return len(self.paths)

    def to_dict(self) -> dict:
        return {
            "phase": self.phase, "task_id": self.task_id, "agent_role": self.agent_role,
            "count": self.count, "pool": len(self.pool), "created_at": self.created_at,
        }


@dataclass
class ArbitrationVerdict:
    """队长裁决结果（任务链路唯一基准的产生过程与结论）。"""

    conflict: ConflictReport | None = None
    decision: str = ""
    adopt_side: str = ""
    reasoning: str = ""
    canonical_paths: list[str] = field(default_factory=list)
    canonical_items: list[dict] = field(default_factory=list)
    adopted_phase: str = ""
    recheck_paths: list[str] = field(default_factory=list)
    recheck_done: bool = False
    confidence: str = ""
    risk_note: str = ""
    degraded: bool = False            # 队长模型不可用 → 后端保守裁决
    source: str = ""                  # llm / fallback_deterministic
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "conflict": self.conflict.to_dict() if self.conflict else None,
            "decision": self.decision,
            "adopt_side": self.adopt_side,
            "reasoning": self.reasoning,
            "canonical_count": len(self.canonical_paths),
            "canonical_paths": self.canonical_paths,
            "adopted_phase": self.adopted_phase,
            "recheck_paths": self.recheck_paths,
            "recheck_done": self.recheck_done,
            "confidence": self.confidence,
            "risk_note": self.risk_note,
            "degraded": self.degraded,
            "source": self.source,
            "created_at": self.created_at,
        }


# ==========================================================================
# 纯函数：冲突检测 / 决策落地 / 提示词构造（便于离线单测，不依赖任何外部服务）
# ==========================================================================
def detect_conflict(snap_a: CandidateSnapshot, snap_b: CandidateSnapshot) -> ConflictReport:
    """比较两份队员待删清单，找出差异文件（队长一致性校验的核心）。"""
    set_a, set_b = set(snap_a.paths), set(snap_b.paths)
    only_a = sorted(set_a - set_b)
    only_b = sorted(set_b - set_a)
    pool_same = sorted(set(snap_a.pool)) == sorted(set(snap_b.pool)) if (
        snap_a.pool and snap_b.pool) else True
    report = ConflictReport(
        only_a=only_a, only_b=only_b,
        count_a=snap_a.count, count_b=snap_b.count,
        pool_a=len(snap_a.pool), pool_b=len(snap_b.pool), pool_same=pool_same,
        phase_a=snap_a.phase, phase_b=snap_b.phase,
    )
    if only_a or only_b:
        report.has_conflict = True
        report.kind = CONFLICT_ROW_DIVERGENCE
    elif snap_a.count != snap_b.count:
        # 条目集合一致但数量不同（理论上不可达：路径去重后数量即集合大小）
        report.has_conflict = True
        report.kind = CONFLICT_COUNT_MISMATCH
    return report


def build_arbitration_user_content(snap_a: CandidateSnapshot, snap_b: CandidateSnapshot,
                                   conflict: ConflictReport) -> str:
    """构造队长仲裁的 user 消息（事实 + 差异 + 判定依据，全部为确定性数据）。"""
    def _fmt(paths: list[str], limit: int = MAX_CONFLICT_ROWS) -> str:
        rows = sorted(paths)[:limit]
        text = "\n".join(f"  - {p}" for p in rows)
        if len(paths) > limit:
            text += f"\n  …（另有 {len(paths) - limit} 个，已截断展示）"
        return text or "  （无）"

    lines = [
        "【队员输出一致性仲裁请求】",
        f"两名队员（均为 {AGENT_CODE}）分别在不同阶段输出了待删候选清单，请你作为队长裁决。",
        "",
        f"■ 队员A · 阶段「{snap_a.phase}」：待删候选 {snap_a.count} 项"
        f"｜扫描文件池 {len(snap_a.pool)} 个",
        f"■ 队员B · 阶段「{snap_b.phase}」：待删候选 {snap_b.count} 项"
        f"｜扫描文件池 {len(snap_b.pool)} 个",
        f"■ 两次扫描的文件池是否一致：{'一致' if conflict.pool_same else '不一致'}",
        "",
        f"■ 冲突判定：{conflict.summary()}",
        "",
        f"■ 仅队员A列为待删、队员B未列（{len(conflict.only_a)} 个）：",
        _fmt(conflict.only_a),
        "",
        f"■ 仅队员B列为待删、队员A未列（{len(conflict.only_b)} 个）：",
        _fmt(conflict.only_b),
        "",
        "■ 队员A完整清单：",
        _fmt(snap_a.paths),
        "",
        "■ 队员B完整清单：",
        _fmt(snap_b.paths),
        "",
        "请按系统提示词的 JSON 契约给出裁决（decision / adopt_side / reasoning / "
        "conflict_rows / recheck_paths / confidence / risk_note）。",
        "注意：删除不可逆，差异文件若为正常源码/配置/文档等业务资产，不得列为删除候选。",
    ]
    return "\n".join(lines)


def build_recheck_instruction(paths: list[str], *, phase: str = "") -> str:
    """【规则3】队长发起"冲突文件二次核验"子任务时使用的自包含指令。"""
    listed = "\n".join(f"- {p}" for p in sorted(paths)[:MAX_CONFLICT_ROWS])
    return (
        "【队长仲裁 · 冲突文件二次核验】\n"
        f"背景：两份待删清单在以下 {len(paths)} 个文件上出现分歧"
        + (f"（阶段：{phase}）" if phase else "") + "，队长要求对这**部分文件**单独二次判定。\n\n"
        "【本次只需判定下列文件（其余文件一律不要出现在结果里）】\n"
        f"{listed}\n\n"
        "【判定要求】\n"
        "1. 逐个判断该文件是否属于：调试文件 / 测试文件 / 临时文件 / 构建产物 / 日志文件；\n"
        "2. 正常源码、配置、依赖清单、文档等业务资产**不得**判定为待删除；\n"
        "3. 只需输出一轮：done=true + candidates（仅含上面列出的文件）+ file_notes；\n"
        "4. 禁止输出任何解释文字，禁止 Markdown 代码块，输出纯 JSON。\n"
    )


def render_arbitration_report(verdict: ArbitrationVerdict) -> str:
    """把队长裁决渲染成 UI / 报告可读文本（含差异文件路径与统一结论）。"""
    conflict = verdict.conflict
    lines = ["## ⚖️ 队长（调度规划Agent）冲突仲裁", ""]
    if conflict is None:
        lines.append("（本次无需仲裁）")
        return "\n".join(lines)

    lines.append(f"- 冲突判定：**{conflict.summary()}**")
    lines.append(f"- 采信版本：`{verdict.adopt_side or '（后端保守裁决）'}`"
                 f"（阶段「{verdict.adopted_phase}」）")
    lines.append(f"- 裁决方式：`{verdict.decision}`"
                 + ("（队长模型不可用 → 后端保守裁决）" if verdict.degraded else "（队长模型裁决）"))
    if verdict.confidence:
        lines.append(f"- 裁决置信度：{verdict.confidence}")
    lines.append("")
    lines.append(f"### 差异文件清单（共 {conflict.diff_count} 个）")
    if conflict.only_a:
        lines.append(f"**仅「{conflict.phase_a}」列为待删（{len(conflict.only_a)} 个）**")
        lines.extend(f"- `{p}`" for p in conflict.only_a)
    if conflict.only_b:
        lines.append(f"**仅「{conflict.phase_b}」列为待删（{len(conflict.only_b)} 个）**")
        lines.extend(f"- `{p}`" for p in conflict.only_b)
    if not conflict.diff_paths:
        lines.append("（无差异文件）")
    lines.append("")
    lines.append("### 队长裁决理由")
    lines.append(verdict.reasoning or "（未提供理由）")
    if verdict.risk_note:
        lines.append("")
        lines.append(f"⚠️ 风险提示：{verdict.risk_note}")
    if verdict.recheck_paths:
        lines.append("")
        lines.append(f"### 二次核验（{len(verdict.recheck_paths)} 个文件）")
        lines.append("**队长要求对冲突文件单独二次判定**"
                     + ("（已完成）" if verdict.recheck_done else "（未执行，按保守裁决落地）"))
        lines.extend(f"- `{p}`" for p in verdict.recheck_paths[:50])
    lines.append("")
    lines.append("### 唯一统一待删清单（后续所有子任务强制沿用）")
    lines.append(f"- 共 {len(verdict.canonical_paths)} 项，来源阶段：`{verdict.adopted_phase or '-'}`")
    lines.extend(f"- `{p}`" for p in verdict.canonical_paths[:MAX_CONFLICT_ROWS])
    if len(verdict.canonical_paths) > MAX_CONFLICT_ROWS:
        lines.append(f"- …（另有 {len(verdict.canonical_paths) - MAX_CONFLICT_ROWS} 项）")
    return "\n".join(lines)


# ==========================================================================
# 队长仲裁中心
# ==========================================================================
class ArbitrationCenter:
    """队长仲裁中心：队员结果上报 → 一致性校验 → 队长裁决 → 唯一基准强制沿用。"""

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger
        # session_id -> {"snapshots": [...], "canonical": dict | None, "history": [...]}
        self._state: dict[str, dict] = {}

    # ---------------- 会话态读取 ----------------
    def _bucket(self, session_id: str) -> dict:
        return self._state.setdefault(str(session_id or "-"), {
            "snapshots": [], "canonical": None, "history": [],
        })

    def reset(self, session_id: str) -> None:
        """新任务周期开始时清空该会话的仲裁态（结论为空 → 不跨任务误用旧清单）。"""
        bucket = self._bucket(session_id)
        bucket["snapshots"] = []
        bucket["canonical"] = None
        bucket["history"] = []

    # ---------------- 上报（规则1：所有队员输出必须上报队长） ----------------
    def register_member_output(
        self, *, session_id: str, task_id: str, phase: str, agent_role: str,
        metadata: dict | None = None,
    ) -> CandidateSnapshot | None:
        """把一次队员输出上报队长。仅"清单/候选"类输出需要上报，其余直接忽略。

        返回本次上报的快照（未上报则返回 None）。
        """
        meta = metadata or {}
        candidates = meta.get("candidates")
        if not isinstance(candidates, list):
            return None
        paths: list[str] = []
        items: list[dict] = []
        for c in candidates:
            if not isinstance(c, dict):
                continue
            p = str(c.get("path") or "").strip()
            if not p or p in paths:
                continue
            paths.append(p)
            items.append(c)
        native = meta.get("native_scan") if isinstance(meta.get("native_scan"), dict) else {}
        pool = [str(p) for p in (meta.get("classify_scope") or [])]
        if not pool:
            pool = [str(p) for p in (native.get("files_list") or [])]

        snapshot = CandidateSnapshot(
            phase=str(phase or "未命名阶段")[:60], task_id=str(task_id or ""),
            agent_role=str(agent_role or AGENT_CODE), paths=paths, items=items, pool=pool,
        )
        bucket = self._bucket(session_id)
        bucket["snapshots"].append(snapshot)
        self.db.log_agent(
            session_id=session_id, task_id=task_id, agent_role=AGENT_DISPATCH,
            event="captain.member_report_received",
            detail=(f"队员 {snapshot.agent_role} 上报输出：阶段={snapshot.phase} "
                    f"待删候选={snapshot.count}｜扫描文件池={len(snapshot.pool)}｜"
                    f"累计上报={len(bucket['snapshots'])}"),
        )
        return snapshot

    # ---------------- 冲突检测（规则2） ----------------
    def detect(self, session_id: str) -> tuple[CandidateSnapshot, CandidateSnapshot, ConflictReport] | None:
        """对最近两次队员上报做一致性校验；不足两次或清单类上报不足时返回 None。"""
        bucket = self._bucket(session_id)
        snaps = [s for s in bucket["snapshots"] if s.count > 0 or s.pool]
        if len(snaps) < 2:
            return None
        snap_a, snap_b = snaps[-2], snaps[-1]
        return snap_a, snap_b, detect_conflict(snap_a, snap_b)

    # ---------------- 裁决（规则3/4） ----------------
    async def arbitrate(
        self, *, session_id: str, root_task: Any, judge: Callable[..., Awaitable[str]],
        snap_a: CandidateSnapshot, snap_b: CandidateSnapshot, conflict: ConflictReport,
    ) -> ArbitrationVerdict:
        """队长裁决冲突并产出**唯一统一待删清单**。

        · judge：队长（调度规划Agent）的模型调用回调，返回原始文本（由调用方解析）；
        · 队长模型不可用 / 输出不合法 / 裁决无法落地 → **后端保守裁决兜底**
          （取"更严格"的一份：候选更少的那份），绝不中断任务链路。
        """
        verdict = ArbitrationVerdict(conflict=conflict)
        raw_text = ""
        data: dict | None = None

        for attempt in range(MAX_ARBITRATION_LLM_RETRIES + 1):
            try:
                raw_text = await judge(build_arbitration_user_content(snap_a, snap_b, conflict),
                                       attempt=attempt)
                data = self._parse_verdict(raw_text, conflict)
                if data is not None:
                    break
                self.logger.exception_log(
                    error_code="ARBITRATION_JSON_INVALID",
                    message=f"队长仲裁输出非法 JSON（第 {attempt + 1} 次）：{raw_text[:300]}",
                    session_id=session_id, task_id=root_task.task_id,
                    agent_role=AGENT_DISPATCH,
                    stack=f"--- 队长原始返回 ---\n{raw_text[:4000]}",
                )
            except Exception as exc:  # noqa: BLE001 队长不可用不得中断链路
                self.logger.exception_log(
                    error_code="ARBITRATION_MODEL_UNAVAILABLE",
                    message=f"队长仲裁模型调用失败（第 {attempt + 1} 次）：{exc}",
                    session_id=session_id, task_id=root_task.task_id,
                    agent_role=AGENT_DISPATCH,
                )
                break

        if data is None:
            # ---------- 后端保守裁决兜底 ----------
            verdict.degraded = True
            verdict.source = "fallback_deterministic"
            adopt, side = self._deterministic_choice(snap_a, snap_b, conflict)
            verdict.decision = DECISION_ADOPT
            verdict.adopt_side = side
            verdict.adopted_phase = snap_a.phase if side == "member_a" else snap_b.phase
            verdict.reasoning = (
                "队长模型不可用或输出不合法，后端按**保守原则**兜底裁决："
                f"采信条目更少的一份（{verdict.adopt_side}，{len(adopt)} 项），"
                "即对不可逆的删除操作取更严格口径；差异文件不纳入统一清单。"
            )
            verdict.confidence = "low"
            verdict.risk_note = "本轮裁决未经过队长模型复核，建议人工抽查差异文件。"
            verdict.canonical_paths = list(adopt)
            verdict.canonical_items = list(
                snap_a.items if side == "member_a" else snap_b.items)
        else:
            verdict.source = "llm"
            verdict.decision = str(data.get("decision") or DECISION_ADOPT).lower()
            verdict.adopt_side = str(data.get("adopt_side") or "").strip()
            verdict.reasoning = str(data.get("reasoning") or "")[:2000]
            verdict.confidence = str(data.get("confidence") or "medium").lower()
            verdict.risk_note = str(data.get("risk_note") or "")[:500]
            recheck = [str(p).strip() for p in (data.get("recheck_paths") or []) if str(p).strip()]
            verdict.recheck_paths = [p for p in recheck if p in set(conflict.diff_paths)][:MAX_CONFLICT_ROWS]
            verdict.canonical_paths, verdict.canonical_items, verdict.adopted_phase = \
                self._apply_decision(verdict, snap_a, snap_b, conflict)

        bucket = self._bucket(session_id)
        bucket["canonical"] = {
            "paths": list(verdict.canonical_paths),
            "items": list(verdict.canonical_items),
            "phase": verdict.adopted_phase,
            "decision": verdict.decision,
            "adopt_side": verdict.adopt_side,
            "reasoning": verdict.reasoning,
            "created_at": verdict.created_at,
            "conflict": conflict.to_dict(),
        }
        bucket["history"].append(verdict.to_dict())

        self.db.log_agent(
            session_id=session_id, task_id=root_task.task_id, agent_role=AGENT_DISPATCH,
            event="captain.arbitration_done",
            detail=(f"队长仲裁完成：{conflict.summary()}｜decision={verdict.decision} "
                    f"adopt={verdict.adopt_side}｜唯一清单={len(verdict.canonical_paths)} 项"
                    f"｜来源={verdict.source}"
                    f"｜差异文件：{'、'.join(conflict.diff_paths[:20]) or '无'}"),
        )
        return verdict

    # ---------------- 二次核验结果回灌（规则3） ----------------
    def apply_recheck(self, session_id: str, *, keep: list[str], drop: list[str],
                      task_id: str = "") -> dict | None:
        """把"冲突文件二次核验"的结论合并进唯一基准清单（只影响冲突文件）。"""
        bucket = self._bucket(session_id)
        canonical = bucket.get("canonical")
        if not canonical:
            return None
        keep_set, drop_set = set(keep), set(drop)
        paths = [p for p in canonical["paths"] if p not in drop_set and p not in keep_set]
        paths.extend(sorted(keep_set))
        # 去重并保持确定性顺序
        seen: set[str] = set()
        ordered: list[str] = []
        for p in paths:
            if p not in seen:
                seen.add(p)
                ordered.append(p)
        canonical["paths"] = ordered
        items = [it for it in canonical.get("items") or []
                 if str(it.get("path") or "") not in drop_set]
        existing = {str(it.get("path") or "") for it in items}
        for p in sorted(keep_set):
            if p not in existing:
                items.append({"path": p, "category": "unknown", "confidence": "medium",
                              "reason": "队长二次核验后纳入统一清单",
                              "suggest_delete": True, "risk": "low"})
        canonical["items"] = items
        canonical["recheck_applied_at"] = time.time()
        self.db.log_agent(
            session_id=session_id, task_id=task_id, agent_role=AGENT_DISPATCH,
            event="captain.recheck_merged",
            detail=(f"二次核验结论已并入唯一清单：保留 {len(keep_set)} 个、剔除 {len(drop_set)} 个"
                    f"｜统一清单现 {len(ordered)} 项"),
        )
        return canonical

    # ---------------- 唯一基准读取（规则4） ----------------
    def canonical(self, session_id: str) -> dict | None:
        return self._bucket(session_id).get("canonical")

    def canonical_paths(self, session_id: str) -> list[str]:
        canonical = self.canonical(session_id)
        return list(canonical.get("paths") or []) if canonical else []

    def canonical_context_block(self, session_id: str) -> str:
        """注入下游子任务指令的"唯一基准清单"文本（强制队员沿用，不再各自判定）。"""
        canonical = self.canonical(session_id)
        if not canonical or not canonical.get("paths"):
            return ""
        paths = list(canonical["paths"])
        rows = "\n".join(f"- {p}" for p in paths[:300])
        more = f"\n…（另有 {len(paths) - 300} 项，详见队长裁决报告）" if len(paths) > 300 else ""
        return (
            "【队长（调度规划Agent）已仲裁的唯一待删清单 · 强制沿用，不得自行增删】\n"
            f"来源阶段：{canonical.get('phase') or '-'}｜仲裁方式：{canonical.get('decision')}"
            f"｜条目数：{len(paths)}\n"
            "本次子任务**必须且只能**使用下列清单，禁止重新判定、禁止新增未列出的文件、"
            "禁止遗漏已列出的文件；与你的判断不一致时以本清单为准。\n"
            f"{rows}{more}\n"
        )

    def history(self, session_id: str) -> list[dict]:
        return list(self._bucket(session_id).get("history") or [])

    def snapshot_count(self, session_id: str) -> int:
        return len(self._bucket(session_id).get("snapshots") or [])

    # ---------------- 内部：裁决落地 ----------------
    @staticmethod
    def _parse_verdict(raw_text: str, conflict: ConflictReport) -> dict | None:
        """解析队长裁决输出（复用后端统一的 JSON 清洗链路）。"""
        try:
            from backend.services.model_client import _extract_json
            data = _extract_json(raw_text or "")
        except Exception:  # noqa: BLE001 解析失败由上层重试 / 兜底
            return None
        if not isinstance(data, dict):
            return None
        allowed = {"adopt", "intersection", "recheck"}
        decision = str(data.get("decision") or "adopt").lower()
        if decision not in allowed:
            decision = "adopt"
        # conflict_rows 只允许来自真实差异集合，防臆造
        diff = set(conflict.diff_paths)
        rows = [str(p).strip() for p in (data.get("conflict_rows") or []) if str(p).strip()]
        data["conflict_rows"] = [p for p in rows if p in diff][:MAX_CONFLICT_ROWS]
        data["decision"] = decision
        return data

    @staticmethod
    def _deterministic_choice(snap_a: CandidateSnapshot, snap_b: CandidateSnapshot,
                              conflict: ConflictReport) -> tuple[list[str], str]:
        """后端保守裁决：采信条目更少的一份（对删除这类不可逆操作取更严格口径）。"""
        if snap_a.count <= snap_b.count:
            return list(snap_a.paths), "member_a"
        return list(snap_b.paths), "member_b"

    def _apply_decision(self, verdict: ArbitrationVerdict, snap_a: CandidateSnapshot,
                        snap_b: CandidateSnapshot,
                        conflict: ConflictReport) -> tuple[list[str], list[dict], str]:
        """把队长 decision 落地为唯一清单（确定性执行，不接受模型给出的任意清单）。"""
        decision = verdict.decision
        if decision == DECISION_INTERSECTION:
            keep = sorted(set(snap_a.paths) & set(snap_b.paths))
            items = [it for it in snap_a.items if str(it.get("path") or "") in set(keep)]
            return keep, items, f"{snap_a.phase} ∩ {snap_b.phase}"

        if decision == DECISION_RECHECK:
            # 二次核验：先按保守口径（条目更少的一份）立基准，待核验结论回灌后合并
            adopt, side = self._deterministic_choice(snap_a, snap_b, conflict)
            phase = snap_a.phase if side == "member_a" else snap_b.phase
            items = list(snap_a.items if side == "member_a" else snap_b.items)
            return list(adopt), items, phase

        # adopt：采信队长指定的一方；未指定 / 指定非法 → 同样走保守口径
        side = verdict.adopt_side
        if side not in ("member_a", "member_b"):
            adopt, side = self._deterministic_choice(snap_a, snap_b, conflict)
            verdict.adopt_side = side
            verdict.reasoning = (verdict.reasoning +
                                 "（队长未指定有效采信版本，已按保守口径采信条目更少的一份）")
        if side == "member_a":
            return list(snap_a.paths), list(snap_a.items), snap_a.phase
        return list(snap_b.paths), list(snap_b.items), snap_b.phase


__all__ = [
    "ArbitrationCenter", "ArbitrationError", "ArbitrationVerdict", "CandidateSnapshot",
    "ConflictReport", "ARBITRATION_SYSTEM_PROMPT", "detect_conflict",
    "build_arbitration_user_content", "build_recheck_instruction", "render_arbitration_report",
    "CONFLICT_NONE", "CONFLICT_ROW_DIVERGENCE", "CONFLICT_COUNT_MISMATCH",
    "DECISION_ADOPT", "DECISION_INTERSECTION", "DECISION_RECHECK",
    "MAX_CONFLICT_ROWS", "AGENT_TEMPERATURES",
]
