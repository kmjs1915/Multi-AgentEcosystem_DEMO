# -*- coding: utf-8 -*-
"""
向量库封装（基础设施层）—— 长期记忆向量存储

架构文档来源：
  - 第2章 2.3 规则3 向量库异常：关闭长期记忆，保留会话短期记忆，系统正常运行
  - 第4章 4.5 记忆管理Agent：长期记忆向量库持久化，90天 TTL 自动淘汰
  - 第9章 9.3 数据备份：向量记忆库可导出

实现：
  - 使用 numpy 做精确余弦相似度检索（本地裸机、零外部服务、无 Docker）。
  - 任何异常都不会向上抛出导致系统崩溃：一律标记 available=False 并降级为"仅短期记忆"。
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

try:  # numpy 缺失也不允许崩溃（第2章 2.3 降级原则）
    import numpy as np
except Exception:  # noqa: BLE001
    np = None  # type: ignore[assignment]

from backend.utils.constants import MEMORY_LONG_TERM_TTL_DAYS, MEMORY_SEARCH_TOP_K


@dataclass
class VectorRecord:
    memory_id: str
    session_id: str
    text: str
    vector: list[float]
    created_at: float
    source_task_id: str | None = None
    tags: list[str] = field(default_factory=list)


class VectorStore:
    """长期记忆向量库（单文件持久化）。"""

    def __init__(self, store_dir: Path):
        self._dir = Path(store_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / "long_term_memory.jsonl"
        self._lock = threading.RLock()
        self.available = np is not None
        self.degrade_reason = "" if self.available else "numpy 不可用，向量库降级为关闭状态"
        self._records: list[VectorRecord] = []
        if self.available:
            self._load()
            self._evict_expired()

    # ---------------- 持久化 ----------------
    def _load(self) -> None:
        try:
            if not self._file.exists():
                return
            with self._file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        self._records.append(VectorRecord(
                            memory_id=row["memory_id"], session_id=row["session_id"],
                            text=row["text"], vector=list(row["vector"]),
                            created_at=float(row["created_at"]),
                            source_task_id=row.get("source_task_id"),
                            tags=list(row.get("tags") or []),
                        ))
                    except Exception:  # noqa: BLE001 单行损坏不影响整体
                        continue
        except Exception as exc:  # noqa: BLE001
            self._degrade(f"向量库加载失败：{exc}")

    def _persist(self) -> None:
        try:
            tmp = self._file.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for rec in self._records:
                    fh.write(json.dumps({
                        "memory_id": rec.memory_id, "session_id": rec.session_id,
                        "text": rec.text, "vector": rec.vector, "created_at": rec.created_at,
                        "source_task_id": rec.source_task_id, "tags": rec.tags,
                    }, ensure_ascii=False) + "\n")
            tmp.replace(self._file)
        except Exception as exc:  # noqa: BLE001
            self._degrade(f"向量库写入失败：{exc}")

    def _degrade(self, reason: str) -> None:
        """降级：关闭长期记忆（第2章 2.3 规则3），不抛出异常。"""
        self.available = False
        self.degrade_reason = reason

    # ---------------- TTL 淘汰（90天） ----------------
    def _evict_expired(self) -> int:
        if not self.available:
            return 0
        deadline = time.time() - MEMORY_LONG_TERM_TTL_DAYS * 86400
        with self._lock:
            before = len(self._records)
            self._records = [r for r in self._records if r.created_at >= deadline]
            removed = before - len(self._records)
            if removed:
                self._persist()
        return removed

    # ---------------- 读写 ----------------
    def upsert(self, record: VectorRecord) -> bool:
        if not self.available:
            return False
        if not record.vector:
            return False
        try:
            with self._lock:
                self._records = [r for r in self._records if r.memory_id != record.memory_id]
                self._records.append(record)
                if len(self._records) > 20000:          # 硬上限保护
                    self._records = self._records[-20000:]
                self._persist()
            return True
        except Exception as exc:  # noqa: BLE001
            self._degrade(f"向量库 upsert 失败：{exc}")
            return False

    def search(self, query_vector: Sequence[float], *, session_id: str | None = None,
               top_k: int = MEMORY_SEARCH_TOP_K) -> list[dict]:
        """余弦相似度检索。向量库不可用时返回空列表（上层退回短期记忆）。"""
        if not self.available or not query_vector:
            return []
        try:
            with self._lock:
                candidates = [r for r in self._records if session_id is None or r.session_id == session_id]
                if not candidates:
                    return []
                q = np.asarray(list(query_vector), dtype="float32")
                qn = np.linalg.norm(q)
                if qn == 0:
                    return []
                mat = np.asarray([r.vector for r in candidates], dtype="float32")
                norms = np.linalg.norm(mat, axis=1)
                norms[norms == 0] = 1e-9
                sims = (mat @ q) / (norms * qn)
                order = np.argsort(-sims)[: max(1, top_k)]
                out: list[dict] = []
                for idx in order:
                    sim = float(sims[int(idx)])
                    if sim <= 0:
                        continue
                    rec = candidates[int(idx)]
                    out.append({
                        "memory_id": rec.memory_id,
                        "session_id": rec.session_id,
                        "text": rec.text,
                        "score": round(sim, 4),
                        "created_at": rec.created_at,
                        "source_task_id": rec.source_task_id,
                        "tags": rec.tags,
                    })
                return out
        except Exception as exc:  # noqa: BLE001
            self._degrade(f"向量检索失败：{exc}")
            return []

    def clear(self, *, session_id: str | None = None, memory_id: str | None = None) -> int:
        """用户手动清空长期记忆（第4章 4.5 用户可控：支持手动清空单条/全部）。"""
        if not self.available:
            return 0
        with self._lock:
            before = len(self._records)
            if memory_id:
                self._records = [r for r in self._records if r.memory_id != memory_id]
            elif session_id:
                self._records = [r for r in self._records if r.session_id != session_id]
            else:
                self._records = []
            self._persist()
            return before - len(self._records)

    def list_records(self, *, session_id: str | None = None, limit: int = 200) -> list[dict]:
        with self._lock:
            rows = [r for r in self._records if session_id is None or r.session_id == session_id]
        rows = sorted(rows, key=lambda r: r.created_at, reverse=True)[:limit]
        return [{
            "memory_id": r.memory_id, "session_id": r.session_id, "text": r.text,
            "created_at": r.created_at, "source_task_id": r.source_task_id,
            "tags": r.tags, "dim": len(r.vector),
        } for r in rows]

    def export(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "degrade_reason": self.degrade_reason,
            "count": len(self._records),
            "records": [{
                "memory_id": r.memory_id, "session_id": r.session_id, "text": r.text,
                "vector": r.vector, "created_at": r.created_at,
                "source_task_id": r.source_task_id, "tags": r.tags,
            } for r in self._records],
        }

    def stats(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "degrade_reason": self.degrade_reason,
            "count": len(self._records),
            "ttl_days": MEMORY_LONG_TERM_TTL_DAYS,
        }
