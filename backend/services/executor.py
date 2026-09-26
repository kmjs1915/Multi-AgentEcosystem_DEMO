# -*- coding: utf-8 -*-
"""
后端硬编码高危动作执行器（服务层）—— 所有磁盘修改动作的唯一执行入口

【需求点 Bug1 改造原则】
  · 高危动作识别**完全由后端程序硬编码实现**（constants.HIGH_RISK_OP_SET），
    不依赖 AI 大模型输出文本，也不允许 AI 自行判断"这是不是高危"；
  · LLM(Agent) 只做分析工作：输出待删除文件候选清单 delete_candidates；
  · 所有磁盘修改 / 删除动作**全部由后端 Python 代码完成**（os.remove / os.rmdir），
    逐个执行并收集每一项目的真实回执（成功 / 失败原因）；
  · 审批通过前绝不触碰磁盘；审批拒绝时本模块根本不会被调用。

职责边界：
  · 本模块只负责"执行 + 收集回执 + 生成用户可读回执报告"；
  · 审批（人在回路）由 services.approval_center 负责，本模块**不自建审批**，
    但强制要求调用方传入 approved=True（否则直接抛 SecurityViolation，双重保险）；
  · 单文件删除 → file_single_delete，批量删除 → file_batch_delete，
    目录删除 → folder_remove（全部落在 HIGH_RISK_OP_SET 内）。
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.infrastructure.logger import get_logger
from backend.utils.constants import (
    EXECUTION_CLAIM_PATTERNS,
    HIGH_RISK_BATCH_DELETE_THRESHOLD,
    HIGH_RISK_OP_DELETE_SET,
    HIGH_RISK_OP_FILE_BATCH_DELETE,
    HIGH_RISK_OP_FILE_SINGLE_DELETE,
    HIGH_RISK_OP_FOLDER_REMOVE,
    HIGH_RISK_OP_LABEL,
    HIGH_RISK_REQUIRES_APPROVAL,
)
from backend.utils.paths import SecurityViolation, is_within


# ==========================================================================
# 一、结构化候选清单解析（Agent 只输出清单，后端负责校验与去重）
# ==========================================================================
def normalize_delete_candidates(raw: Any) -> list[dict]:
    """把 Agent 输出/队长裁决结果收敛为统一的候选清单结构。

    输入可以是：
      · candidates 数组（[{path, category, reason, confidence, ...}, ...]）
      · 纯路径字符串数组（["a.log", "b.tmp"]）
      · 字符串（单个路径）
    输出统一为 [{"path": str, "category": str, "reason": str, "confidence": str}]

    说明：本函数只做"格式收敛与去重"，**不做任何高危判定**（判定是硬编码常量的事），
    也不访问磁盘 —— 磁盘校验在真正执行时才进行，避免"分析阶段就动盘"。
    """
    items: list[Any] = []
    if raw is None:
        items = []
    elif isinstance(raw, str):
        items = [raw]
    elif isinstance(raw, dict):
        # 兼容 {"candidates": [...]} / {"delete_candidates": [...]} / {"paths": [...]}
        for key in ("delete_candidates", "candidates", "paths", "files"):
            if isinstance(raw.get(key), list):
                items = list(raw.get(key) or [])
                break
        else:
            items = [raw.get("path")] if raw.get("path") else []
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)

    out: list[dict] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, dict):
            path = str(item.get("path") or item.get("file") or "").strip()
            category = str(item.get("category") or "").strip()
            reason = str(item.get("reason") or "").strip()
            confidence = str(item.get("confidence") or "").strip()
        else:
            path = str(item or "").strip()
            category = reason = confidence = ""
        if not path:
            continue
        key = path.replace("\\", "/").lstrip("./").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"path": path, "category": category, "reason": reason,
                    "confidence": confidence})
    return out


def infer_delete_operation(candidate_count: int) -> str:
    """【后端硬编码】按候选数量判定高危动作类型（不看模型文本）。

      · 1 个 → file_single_delete
      · ≥2 个 → file_batch_delete
      · 目录 → 由执行阶段实测为目录时升级为 folder_remove（见 execute_delete_plan）
    """
    count = max(0, int(candidate_count or 0))
    if count <= 0:
        return ""
    if count <= 1:
        return HIGH_RISK_OP_FILE_SINGLE_DELETE
    _ = HIGH_RISK_BATCH_DELETE_THRESHOLD
    return HIGH_RISK_OP_FILE_BATCH_DELETE


def is_delete_operation(op: str) -> bool:
    """该高危动作是否属于"删除类"（仅删除类由后端原生执行器执行）。"""
    return str(op or "") in HIGH_RISK_OP_DELETE_SET


# ==========================================================================
# 二、执行回执结构
# ==========================================================================
@dataclass
class DeleteReceipt:
    """单项磁盘删除的真实回执（唯一可信的"是否删掉"来源，绝不来自模型文本）。"""

    path: str
    ok: bool
    detail: str
    kind: str = ""              # file / dir / missing / error
    bytes_freed: int = 0
    elapsed_ms: int = 0
    error: str = ""
    source: str = "backend_native"   # 固定标记：回执来源 = 后端原生磁盘操作

    def to_dict(self) -> dict:
        return {
            "path": self.path, "ok": self.ok, "detail": self.detail, "kind": self.kind,
            "bytes_freed": self.bytes_freed, "elapsed_ms": self.elapsed_ms,
            "error": self.error, "source": self.source,
        }

    @classmethod
    def from_dict(cls, raw: Any) -> "DeleteReceipt":
        """从快照里的 JSON 结构还原回执（服务重启后仍能判定"是否真的执行过"）。"""
        data = raw if isinstance(raw, dict) else {}
        return cls(
            path=str(data.get("path") or ""),
            ok=bool(data.get("ok")),
            detail=str(data.get("detail") or ""),
            kind=str(data.get("kind") or ""),
            # 【修复】JSON 里可能是 float/None，必须强转，避免类型污染导致比较异常
            bytes_freed=int(data.get("bytes_freed") or 0),
            elapsed_ms=int(data.get("elapsed_ms") or 0),
            error=str(data.get("error") or ""),
            source=str(data.get("source") or "backend_native"),
        )


@dataclass
class DeleteExecution:
    """一次删除动作的整体执行回执（审批通过后由后端 Python 原生循环产出）。"""

    operation: str
    approval_id: str
    session_id: str
    task_id: str
    workspace_root: str
    receipts: list[DeleteReceipt] = field(default_factory=list)
    rejected: bool = False
    rejected_reason: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0.0
    source: str = "backend_native"

    @property
    def total(self) -> int:
        return len(self.receipts)

    @property
    def ok_count(self) -> int:
        return len([r for r in self.receipts if r.ok])

    @property
    def failed_count(self) -> int:
        return len([r for r in self.receipts if not r.ok])

    @property
    def all_ok(self) -> bool:
        return bool(self.receipts) and self.failed_count == 0

    def to_dict(self) -> dict:
        return {
            "operation": self.operation,
            "operation_label": HIGH_RISK_OP_LABEL.get(self.operation, self.operation),
            "approval_id": self.approval_id,
            "session_id": self.session_id,
            "task_id": self.task_id,
            "workspace_root": self.workspace_root,
            "rejected": self.rejected,
            "rejected_reason": self.rejected_reason,
            "total": self.total,
            "ok_count": self.ok_count,
            "failed_count": self.failed_count,
            "all_ok": self.all_ok,
            "receipts": [r.to_dict() for r in self.receipts],
            "source": self.source,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, raw: Any) -> "DeleteExecution | None":
        """【修复·幂等保护】从快照 JSON 还原整体执行回执。

        用途：服务重启后 `SubtaskResult.backend_execution` 会丢失（快照里只有 dict），
        若不还原，队长循环与 `_execute_subtasks` 的"是否已执行过"守卫会失效，
        可能出现**同一高危动作被重复执行**（第二次逐项回执必然全部失败并污染报告）。
        """
        data = raw if isinstance(raw, dict) else None
        if not data:
            return None
        receipts_raw = data.get("receipts")
        if not isinstance(receipts_raw, list):
            return None
        return cls(
            operation=str(data.get("operation") or ""),
            approval_id=str(data.get("approval_id") or ""),
            session_id=str(data.get("session_id") or ""),
            task_id=str(data.get("task_id") or ""),
            workspace_root=str(data.get("workspace_root") or ""),
            receipts=[DeleteReceipt.from_dict(r) for r in receipts_raw],
            rejected=bool(data.get("rejected")),
            rejected_reason=str(data.get("rejected_reason") or ""),
            started_at=float(data.get("started_at") or 0.0),
            finished_at=float(data.get("finished_at") or 0.0),
            source=str(data.get("source") or "backend_native"),
        )


# ==========================================================================
# 三、后端硬编码执行器
# ==========================================================================
class BackendExecutor:
    """高危动作的唯一执行端：审批通过后由后端 Python 代码逐项执行并回执。

    与 Agent 的关系：
      · Agent 只提供候选清单（分析产物）；
      · 本执行器不读取任何模型文本，只按清单操作磁盘；
      · 执行结果（回执）是报告里"删除结果"的唯一数据源。
    """

    def __init__(self, guard):
        """guard：当前会话的 SessionFileGuard（负责路径越界校验与工作区绑定）。"""
        self.guard = guard

    # ------------------------------------------------------------------
    # 高危动作入口（硬编码判定 + 强制审批门）
    # ------------------------------------------------------------------
    def execute(self, *, operation: str, candidates: list[dict], approved: bool,
                approval_id: str = "", session_id: str = "", task_id: str = "",
                workspace_root: str = "") -> DeleteExecution:
        """执行高危删除动作。

        · operation 必须在 HIGH_RISK_OP_SET 内，且必须是删除类；
        · approved 必须为 True（审批通过后才允许调用，否则直接拒绝执行）。
        """
        op = str(operation or "")
        if HIGH_RISK_REQUIRES_APPROVAL and not approved:
            raise SecurityViolation(
                f"高危动作 {op or '(未指定)'} 未获人工审批，后端拒绝执行（硬编码门禁）",
                code="APPROVAL_REQUIRED",
            )
        if not is_delete_operation(op):
            raise SecurityViolation(
                f"该动作类型 {op!r} 不属于后端原生删除类（{sorted(HIGH_RISK_OP_DELETE_SET)}）",
                code="OPERATION_NOT_SUPPORTED",
            )

        root = str(workspace_root or getattr(self.guard, "workspace_root", "") or "")
        execution = DeleteExecution(
            operation=op, approval_id=str(approval_id or ""), session_id=str(session_id or ""),
            task_id=str(task_id or ""), workspace_root=root,
        )
        logger = None
        try:
            logger = get_logger()
        except Exception:  # noqa: BLE001 离线单测无日志器
            logger = None

        for item in candidates:
            path = str((item or {}).get("path") or "").strip()
            started = time.time()
            if not path:
                execution.receipts.append(DeleteReceipt(
                    path="", ok=False, detail="候选清单中存在空路径，已跳过",
                    kind="error", error="EMPTY_PATH",
                ))
                continue
            try:
                receipt = self._delete_one(path)
            except SecurityViolation as exc:
                receipt = DeleteReceipt(
                    path=path, ok=False, detail=f"安全模块拒绝：{exc}", kind="error",
                    error=getattr(exc, "code", "SECURITY_VIOLATION"),
                )
            except Exception as exc:  # noqa: BLE001 任何异常都转成失败回执，不中断整批
                receipt = DeleteReceipt(
                    path=path, ok=False, detail=f"删除失败：{type(exc).__name__}: {exc}",
                    kind="error", error=type(exc).__name__,
                )
            receipt.elapsed_ms = int((time.time() - started) * 1000)
            execution.receipts.append(receipt)
            if logger is not None:
                logger.task_log(
                    session_id=execution.session_id, task_id=execution.task_id,
                    agent_role="backend_executor", event="delete.receipt",
                    level="info" if receipt.ok else "warn",
                    detail=(f"op={op} path={path} ok={receipt.ok} kind={receipt.kind} "
                            f"detail={receipt.detail[:300]}"),
                )
        execution.finished_at = time.time()
        if logger is not None:
            logger.task_log(
                session_id=execution.session_id, task_id=execution.task_id,
                agent_role="backend_executor", event="delete.executed",
                level="info" if execution.all_ok else "warn",
                detail=(f"后端原生执行完成：op={op} 共 {execution.total} 项，"
                        f"成功 {execution.ok_count}，失败 {execution.failed_count}"),
            )
        return execution

    # ------------------------------------------------------------------
    # 单项删除（os.remove / os.rmdir，逐项回执）
    # ------------------------------------------------------------------
    def _delete_one(self, path: str) -> DeleteReceipt:
        """删除单个文件或空目录。

        实现要点（硬编码，不依赖任何模型输出）：
          · 路径越界 / 工作区根目录 → 安全模块直接拒绝；
          · 目标不存在 → 失败回执（不谎报成功）；
          · 文件 → os.remove；目录 → os.rmdir（非空目录 → 失败回执，不做递归删除）；
          · 删除后**再次 stat 复核**，确认磁盘上确实不存在，才给出成功回执。
        """
        target = self.guard.resolve(path)
        workspace_root = Path(self.guard.workspace_root)
        if not is_within(target, workspace_root) or target == workspace_root:
            raise SecurityViolation(
                f"删除目标越界或为工作区根目录：{path}", code="WORKSPACE_ACCESS_DENIED",
                detail={"path": path},
            )

        if not target.exists():
            return DeleteReceipt(path=path, ok=False, kind="missing",
                                 detail="目标不存在（未执行删除，无任何改动）",
                                 error="FILE_NOT_FOUND")

        if target.is_dir():
            entries = []
            try:
                entries = os.listdir(target)
            except OSError as exc:
                return DeleteReceipt(path=path, ok=False, kind="error",
                                     detail=f"目录不可读，未执行删除：{exc}", error="READ_FAILED")
            if entries:
                return DeleteReceipt(
                    path=path, ok=False, kind="dir",
                    detail=(f"目录非空（{len(entries)} 项），后端未做递归删除，"
                            "已跳过以避免误删业务文件"),
                    error="DIR_NOT_EMPTY",
                )
            os.rmdir(str(target))
            if target.exists():
                return DeleteReceipt(path=path, ok=False, kind="error",
                                     detail="目录仍存在，删除未生效", error="DELETE_NOT_APPLIED")
            return DeleteReceipt(path=path, ok=True, kind="dir", detail="空目录已删除")

        try:
            size = int(target.stat().st_size)
        except OSError:
            size = 0
        try:
            os.remove(str(target))
        except PermissionError as exc:
            return DeleteReceipt(path=path, ok=False, kind="file",
                                 detail=f"文件被占用或无权限，删除失败：{exc}",
                                 error="PERMISSION_DENIED", bytes_freed=0)
        except OSError as exc:
            return DeleteReceipt(path=path, ok=False, kind="file",
                                 detail=f"删除失败：{exc}", error=type(exc).__name__)
        if target.exists():
            return DeleteReceipt(path=path, ok=False, kind="file",
                                 detail="文件仍存在，删除未生效", error="DELETE_NOT_APPLIED",
                                 bytes_freed=0)
        return DeleteReceipt(path=path, ok=True, kind="file",
                             detail=f"文件已删除（释放 {size} 字节）", bytes_freed=size)


# ==========================================================================
# 五、虚报检测：没有后端真实回执时，文本里"已删除/已完成"一律纠偏
# ==========================================================================
_CLAIM_REPLACEMENT_NOTE = "（注：本条描述已被后端纠偏——该动作未由 Agent 执行）"


def detect_execution_claims(text: str) -> list[str]:
    """检测文本中"宣称已执行完成"的句子（后端硬编码句式，不交给模型判断）。

    返回命中的句子片段列表；空列表表示没有完成态声明。
    """
    raw = str(text or "")
    if not raw:
        return []
    hits: list[str] = []
    for line in raw.splitlines():
        stripped = (line or "").strip().lstrip("-*# ").strip()
        if len(stripped) < 4:
            continue
        if any(re.search(pattern, stripped) for pattern in EXECUTION_CLAIM_PATTERNS):
            hits.append(stripped[:200])
    return hits


def sanitize_execution_claim_text(text: str, *, executed_paths: set[str] | None = None,
                                  context: str = "无") -> tuple[str, list[str]]:
    """把"宣称已执行完成"的句子纠偏为"仅分析、未执行"。

    规则（**只依据后端事实，不看模型是否自信**）：
      · 文本里出现"已删除 / 删除完成 / 已清理 …"这类完成态声明时，
        若该句涉及的路径**不在后端真实执行回执**中（executed_paths），
        则该句被替换为明确声明"未执行任何磁盘改动（仅输出分析清单）"的文本；
      · 返回 (纠偏后的文本, 命中句子列表)，命中情况由调用方落日志。

    context 仅用于日志与替换文案的可读性（如"子任务：清理临时文件"）。
    """
    raw = str(text or "")
    if not raw:
        return raw, []
    done_paths = {str(p).replace("\\", "/").lstrip("./").lower() for p in (executed_paths or set())}

    lines: list[str] = []
    hits: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        claim = stripped.lstrip("-*# ").strip()
        if len(claim) < 4 or not any(
                re.search(pattern, claim) for pattern in EXECUTION_CLAIM_PATTERNS):
            lines.append(line)
            continue
        # 该句提到的路径是否真的有后端回执？
        mentioned = re.findall(r"[\w./\\\-\u4e00-\u9fff]+\.[A-Za-z0-9]{1,8}", claim)
        covered = False
        for path in mentioned:
            key = path.replace("\\", "/").lstrip("./").lower()
            if key in done_paths:
                covered = True
                break
        if covered:
            lines.append(line)          # 有真实回执支撑 → 保留
            continue
        hits.append(claim[:200])
        lines.append(
            f"⚠️ 后端纠偏：以上声明（{claim[:80]}…）没有后端真实执行回执，"
            f"**未执行任何磁盘改动**，本环节只输出分析清单。"
        )
    if hits:
        lines.append("")
        lines.append(
            "> 说明：本系统所有磁盘修改 / 删除动作只能由后端 Python 代码执行；"
            "Agent 仅做分析（输出待删除候选清单），不得声称已完成删除。")
    return "\n".join(lines), hits


def sanitize_subtask_outputs(entries: list[dict], *,
                             executed_paths: set[str] | None = None) -> list[dict]:
    """批量纠偏"已完成子任务"的产出文本（就地返回新列表，不修改入参）。"""
    out: list[dict] = []
    for entry in entries or []:
        item = dict(entry or {})
        cleaned, hits = sanitize_execution_claim_text(
            str(item.get("output") or ""), executed_paths=executed_paths,
            context=str(item.get("title") or ""))
        if hits:
            item["output"] = cleaned
            item["claim_corrected"] = hits
        out.append(item)
    return out


# ==========================================================================
# 六、回执 → 用户可读报告（报告中删除结果的唯一来源）
# ==========================================================================
def render_receipt_report(execution: DeleteExecution, *, operator: str = "",
                          decided_at: float | None = None) -> str:
    """把后端真实执行回执渲染为 Markdown 报告（不引用任何模型文本）。"""
    label = HIGH_RISK_OP_LABEL.get(execution.operation, execution.operation)
    lines = [
        "## 高危操作执行回执（后端 Python 原生执行）",
        "",
        f"- 动作类型：**{label}**（`{execution.operation}`，后端硬编码高危动作）",
        f"- 审批单号：`{str(execution.approval_id)[:8] or '-'}`"
        f"｜审批人：{operator or '（未记录）'}"
        + (f"｜审批时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(decided_at))}"
           if decided_at else ""),
        f"- 执行范围：工作区 `{execution.workspace_root or '-'}`",
        f"- 执行结果：共 **{execution.total}** 项，成功 **{execution.ok_count}** 项，"
        f"失败 **{execution.failed_count}** 项",
        "",
        "### 逐项回执（真实磁盘操作结果）",
        "",
    ]
    if not execution.receipts:
        lines.append("- （无待执行项：候选清单为空，未对磁盘做任何改动）")
    for index, receipt in enumerate(execution.receipts, 1):
        icon = "✅ 成功" if receipt.ok else "❌ 失败"
        lines.append(f"{index}. `{receipt.path}` — {icon}｜{receipt.detail}")
    lines.append("")
    if execution.failed_count:
        lines.append("> ⚠️ 存在失败项（权限 / 占用等），上表为后端真实返回结果，未做美化。")
        lines.append("")
    lines.append("> 本回执由后端程序执行磁盘操作后采集，不来自任何 Agent 的文本描述。")
    return "\n".join(lines)


__all__ = [
    "BackendExecutor", "DeleteExecution", "DeleteReceipt",
    "normalize_delete_candidates", "infer_delete_operation", "is_delete_operation",
    "render_receipt_report", "detect_execution_claims", "sanitize_execution_claim_text",
    "sanitize_subtask_outputs",
]
