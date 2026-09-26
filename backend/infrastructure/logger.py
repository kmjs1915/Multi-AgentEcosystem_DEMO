# -*- coding: utf-8 -*-
"""
日志模块（基础设施层）

架构文档来源：第9章 9.2 日志系统
    永久记录：任务日志、Agent调用日志、Token消耗、审批记录、错误日志

实现：SQLite 结构化落库（可查询/可备份） + 文件滚动日志（logs/*.log）。
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from backend.infrastructure.database import Database

_SECRET_PATTERNS = (
    re.compile(r"(sk-[A-Za-z0-9_\-]{4})[A-Za-z0-9_\-]+"),
    re.compile(r"(\"?(?:api_?key|apikey|password|secret|token)\"?\s*[:=]\s*[\"']?)([^\s\"',}]{4,})", re.I),
    re.compile(r"(Bearer\s+)[A-Za-z0-9._\-]+", re.I),
)


def redact(text: str) -> str:
    """日志脱敏：任何密钥/口令片段一律遮蔽（第6章 6.1 规则3 永远不暴露明文）。"""
    if not text:
        return ""
    out = str(text)
    for pattern in _SECRET_PATTERNS:
        out = pattern.sub(lambda m: m.group(1) + "***REDACTED***", out)
    return out


class EcosystemLogger:
    """统一日志入口：同时写文件与 SQLite（永久记录）。"""

    _instances: dict[str, "EcosystemLogger"] = {}
    _lock = threading.RLock()

    def __init__(self, log_dir: Path, db: Database):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.db = db

        self._logger = logging.getLogger("multi_agent_ecosystem")
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False
        if not self._logger.handlers:
            fmt = logging.Formatter(
                "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            fh = RotatingFileHandler(
                self.log_dir / "ecosystem.log", maxBytes=8 * 1024 * 1024,
                backupCount=5, encoding="utf-8",
            )
            fh.setFormatter(fmt)
            self._logger.addHandler(fh)

            approval_fh = RotatingFileHandler(
                self.log_dir / "approval.log", maxBytes=4 * 1024 * 1024,
                backupCount=5, encoding="utf-8",
            )
            approval_fh.setFormatter(fmt)
            self._approval_logger = logging.getLogger("multi_agent_ecosystem.approval")
            self._approval_logger.setLevel(logging.INFO)
            self._approval_logger.propagate = False
            self._approval_logger.addHandler(approval_fh)
        else:
            self._approval_logger = logging.getLogger("multi_agent_ecosystem.approval")

    # ---------------- 基础 ----------------
    def info(self, message: str, **extra: Any) -> None:
        self._logger.info(self._fmt(message, extra))

    def warning(self, message: str, **extra: Any) -> None:
        self._logger.warning(self._fmt(message, extra))

    def error(self, message: str, **extra: Any) -> None:
        self._logger.error(self._fmt(message, extra))

    def debug(self, message: str, **extra: Any) -> None:
        self._logger.debug(self._fmt(message, extra))

    @staticmethod
    def _fmt(message: str, extra: dict) -> str:
        payload = {
            k: v for k, v in extra.items()
            if k in ("session_id", "task_id", "agent_role", "event", "code", "detail", "elapsed_ms")
        }
        suffix = f" | {json.dumps(payload, ensure_ascii=False)}" if payload else ""
        return redact(str(message)) + suffix

    # ---------------- 结构化日志（落 SQLite） ----------------
    def task_log(self, *, session_id: str, task_id: str, agent_role: str, event: str,
                 detail: str = "", level: str = "info") -> None:
        self.db.log_agent(
            session_id=session_id, task_id=task_id, agent_role=agent_role,
            event=event, detail=redact(detail)[:4000], level=level,
        )
        self._logger.log(
            logging.WARNING if level in ("warn", "warning") else logging.ERROR if level == "error" else logging.INFO,
            self._fmt(f"[{agent_role}] {event}", {
                "session_id": session_id, "task_id": task_id,
                "agent_role": agent_role, "detail": detail,
            }),
        )

    def approval_log(self, *, approval_id: str, session_id: str, task_id: str, agent_role: str,
                     operation_type: str, risk_level: str, state: str, detail: str = "") -> None:
        text = (
            f"审批 {approval_id} | state={state} | risk={risk_level} | type={operation_type} | "
            f"agent={agent_role} | task={task_id} | {detail}"
        )
        self._approval_logger.info(redact(text))

    def token_log(self, row: dict) -> None:
        self.db.insert_token_usage(row)
        self.info(
            "token.usage",
            session_id=row.get("session_id"), task_id=row.get("task_id"),
            agent_role=row.get("agent_role"),
            detail=f"in={row.get('input_tokens')} out={row.get('output_tokens')} "
                   f"cached={row.get('cached_tokens')} rate={row.get('cache_hit_rate')}%",
        )

    def exception_log(self, *, error_code: str, message: str, session_id: str | None = None,
                      task_id: str | None = None, agent_role: str | None = None,
                      stack: str | None = None) -> None:
        self.db.log_error(
            error_code=error_code, message=redact(message)[:4000], session_id=session_id,
            task_id=task_id, agent_role=agent_role, stack=redact(stack or "")[:8000] or None,
        )
        self._logger.error(self._fmt(f"[{error_code}] {message}", {
            "session_id": session_id, "task_id": task_id, "agent_role": agent_role,
        }))


_current: EcosystemLogger | None = None


def init_logger(log_dir: Path, db: Database) -> EcosystemLogger:
    global _current
    with EcosystemLogger._lock:
        _current = EcosystemLogger(log_dir, db)
    return _current


def get_logger() -> EcosystemLogger:
    if _current is None:
        raise RuntimeError("日志模块尚未初始化（应先调用 init_logger）")
    return _current


def try_get_logger() -> EcosystemLogger | None:
    """安全获取日志器：未初始化时返回 None（供初始化早期的模块使用，如配置迁移）。"""
    return _current
