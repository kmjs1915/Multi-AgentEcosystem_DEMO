# -*- coding: utf-8 -*-
"""
记忆管理Agent（GLM-5.3-Flash）—— 系统记忆中枢

架构文档来源：第4章 4.5
  定位：系统记忆中枢，负责上下文压缩、知识沉淀、长期存储
  工作机制：消息总线主动推送全量事件，记忆Agent异步消费，不轮询
  记忆分层：
      短期记忆：内存会话级，会话结束归档
      长期记忆：向量库持久化，90天TTL自动淘汰
  用户可控：支持手动清空单条/全部长期记忆

第2章 2.3 规则3：向量库异常 → 关闭长期记忆，保留会话短期记忆，系统正常运行。
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from collections import deque
from typing import Any

from backend.agents.base import AGENT_MEMORY, CAPABILITY_MEMORY_WRITE, BaseAgent
from backend.bus.message import Message
from backend.bus.state_machine import TaskState
from backend.infrastructure.vector_store import VectorRecord
from backend.utils.constants import (
    AGENT_MEMORY,
    AGENT_TEMPERATURES,
    MEMORY_LONG_TERM_TTL_DAYS,
    MEMORY_SEARCH_TOP_K,
    MSG_TYPE_APPROVAL_REQUEST,
    MSG_TYPE_APPROVAL_RESULT,
    MSG_TYPE_ERROR,
    MSG_TYPE_RESULT,
    MSG_TYPE_TASK,
)

MEMORY_SYSTEM_PROMPT = """你是「记忆管理Agent」（GLM-5.3-Flash），本地化多Agent协同开发工作台的记忆中枢。

【职责 · 严格限定】
1. 把消息总线推送来的事件压缩成可长期复用的记忆条目。
2. 记忆必须是「可检索的知识」，不是原始对话复述。
3. 严禁臆造事实；只压缩输入里真实出现的内容。

【输出格式 · 必须是合法 JSON】
{
  "should_remember": true,
  "memory_text": "压缩后的记忆条目（150字以内，自包含、可独立检索）",
  "tags": ["标签1", "标签2"],
  "importance": 1,
  "kind": "requirement/preference/conclusion/code_change/error/approval/fact"
}

规则：
- 纯寒暄、无信息量的内容必须输出 {"should_remember": false}。
- importance 取 1-5，5 表示关键长期事实（用户偏好、架构决策、重要结论）。
"""

# 本地词法向量维度（离线可用，无需外部 embedding 服务）
EMBED_DIM = 384


class MemoryAgent(BaseAgent):
    agent_role = AGENT_MEMORY
    model_name = "GLM-5.3-Flash"   # 【需求点 Bug8】固定绑定 GLM-5.3-Flash

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = MEMORY_SYSTEM_PROMPT
        # 短期记忆：内存会话级（第4章 4.5 记忆分层）
        self._short_term: dict[str, deque[dict]] = {}
        self._max_short_term = 60
        self.consumed_events = 0
        self.compressed_events = 0
        self.archived_sessions = 0

    # ==================================================================
    # 一、异步消费（消息总线主动推送，不轮询）
    # ==================================================================
    async def consume(self, msg: Message, *, bus=None) -> None:
        """总线推送的每一条事件都会到这里。绝不阻塞主链路，绝不抛出异常。"""
        self.consumed_events += 1
        self.remember_short(msg)

        # 仅对"结果/错误/审批"这类有信息量的事件做长期记忆压缩
        if msg.msg_type not in (MSG_TYPE_RESULT, MSG_TYPE_ERROR, MSG_TYPE_APPROVAL_REQUEST, MSG_TYPE_APPROVAL_RESULT):
            return
        try:
            await self.compress_and_store(msg)
        except Exception as exc:  # noqa: BLE001 记忆失败不影响主流程
            self.ctx.logger.exception_log(
                error_code="MEMORY_COMPRESS_FAILED", message=f"记忆压缩失败：{exc}",
                session_id=msg.session_id, task_id=msg.task_id, agent_role=self.agent_role,
            )

    # ==================================================================
    # 二、短期记忆（内存会话级）
    # ==================================================================
    def remember_short(self, msg: Message) -> None:
        bucket = self._short_term.setdefault(msg.session_id, deque(maxlen=self._max_short_term))
        bucket.append({
            "task_id": msg.task_id,
            "sender": msg.sender_agent,
            "receiver": msg.receiver_agent,
            "msg_type": msg.msg_type,
            "content": msg.content_text()[:800],
            "timestamp": msg.timestamp,
        })

    def short_term(self, session_id: str, limit: int = 30) -> list[dict]:
        bucket = self._short_term.get(session_id)
        if not bucket:
            return []
        return list(bucket)[-limit:]

    def archive_session(self, session_id: str) -> int:
        """会话结束归档（第4章 4.5：短期记忆会话结束归档）。"""
        bucket = self._short_term.pop(session_id, None)
        if not bucket:
            return 0
        self.archived_sessions += 1
        return len(bucket)

    # ==================================================================
    # 三、长期记忆（向量库 + 90天TTL）
    # ==================================================================
    async def compress_and_store(self, msg: Message) -> dict | None:
        if not self.ctx.vector_store or not self.ctx.vector_store.available:
            # 第2章 2.3 规则3：向量库异常 → 关闭长期记忆，保留短期记忆
            self.ctx.db.log_agent(
                session_id=msg.session_id, task_id=msg.task_id, agent_role=self.agent_role,
                event="memory.long_term_disabled",
                detail="向量库不可用，长期记忆已关闭，仅保留会话短期记忆", level="warn",
            )
            return None

        text = msg.content_text().strip()
        # 过滤无信息量的短事件（纯回执/空内容不进长期记忆，避免污染向量库）
        if len(text) < 10:
            return None

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": (
                f"【事件来源】{msg.sender_agent} -> {msg.receiver_agent}\n"
                f"【事件类型】{msg.msg_type}\n"
                f"【事件内容】\n{text[:6000]}"
            )},
        ]

        try:
            response = await self.call_model(
                # 【需求点 Bug4】记忆管理 Agent 温度 0.15（后台压缩归档需稳定）
                self._synthetic_task(msg), messages, temperature=AGENT_TEMPERATURES[AGENT_MEMORY],
                max_tokens=800,
                expect_json=True, step_label="GLM-5.3-Flash 压缩记忆条目",
            )
            data = self.parse_json(response.text)
        except Exception as exc:  # noqa: BLE001 模型不可用时退回规则压缩
            self.ctx.db.log_agent(
                session_id=msg.session_id, task_id=msg.task_id, agent_role=self.agent_role,
                event="memory.compress_fallback",
                detail=f"模型压缩不可用，改用规则压缩：{type(exc).__name__}: {exc}",
                level="warn",
            )
            data = self._rule_based_compress(msg)

        if not isinstance(data, dict) or not data.get("should_remember", True):
            return None

        memory_text = str(data.get("memory_text") or "").strip()
        if len(memory_text) < 8:
            return None

        vector = self.embed(memory_text)
        record = VectorRecord(
            memory_id=hashlib.sha256(
                f"{msg.session_id}|{msg.task_id}|{memory_text}".encode("utf-8")
            ).hexdigest()[:32],
            session_id=msg.session_id,
            text=memory_text[:1000],
            vector=vector,
            created_at=time.time(),
            source_task_id=msg.task_id,
            tags=list(data.get("tags") or [])[:10],
        )
        ok = self.ctx.vector_store.upsert(record)
        if ok:
            self.compressed_events += 1
            self.ctx.db.log_agent(
                session_id=msg.session_id, task_id=msg.task_id, agent_role=self.agent_role,
                event="memory.long_term_stored",
                detail=f"importance={data.get('importance')} tags={record.tags} text={memory_text[:200]}",
            )
        return {"memory_id": record.memory_id, "text": memory_text, "tags": record.tags}

    def _rule_based_compress(self, msg: Message) -> dict:
        """模型不可用时的规则压缩兜底（不崩溃、不丢事件）。"""
        text = re.sub(r"\s+", " ", msg.content_text()).strip()
        return {
            "should_remember": len(text) >= 10,
            "memory_text": f"[{msg.sender_agent}] {text[:150]}",
            "tags": [msg.msg_type, msg.sender_agent],
            "importance": 2,
            "kind": "fact",
        }

    def _synthetic_task(self, msg: Message) -> TaskState:
        """为记忆压缩构造轻量任务上下文（不进入用户可见任务面板）。"""
        return TaskState(
            task_id=msg.task_id, session_id=msg.session_id,
            title="记忆压缩（总线异步事件）", agent_role=self.agent_role,
        )

    # ==================================================================
    # 四、向量化（离线本地词法向量 + 可选远端 embedding）
    # ==================================================================
    def embed(self, text: str) -> list[float]:
        """本地确定性向量：字符 n-gram 哈希 + 权重。

        裸机运行、无外部依赖、不产生 Token 消耗；对中文/代码文本均有可用的检索效果。
        """
        vec = [0.0] * EMBED_DIM
        cleaned = re.sub(r"\s+", "", text.lower())
        if not cleaned:
            return vec
        grams: list[str] = []
        for n in (1, 2, 3):
            grams.extend(cleaned[i:i + n] for i in range(max(1, len(cleaned) - n + 1)))
        total = len(grams) or 1
        for gram in grams:
            h = hashlib.sha256(gram.encode("utf-8")).digest()
            idx = int.from_bytes(h[:4], "big") % EMBED_DIM
            sign = 1.0 if h[4] % 2 == 0 else -1.0
            vec[idx] += sign * (1.0 / math.sqrt(total))
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    # ==================================================================
    # 五、检索（调度Agent 规划前的记忆检索）
    # ==================================================================
    def search(self, session_id: str, query: str, *, top_k: int = MEMORY_SEARCH_TOP_K) -> list[dict]:
        if not self.ctx.vector_store or not self.ctx.vector_store.available:
            return []
        hits = self.ctx.vector_store.search(self.embed(query), session_id=session_id, top_k=top_k)
        if hits:
            self.ctx.db.log_agent(
                session_id=session_id, task_id="", agent_role=self.agent_role,
                event="memory.retrieved", detail=f"命中 {len(hits)} 条长期记忆",
            )
        return hits

    # ==================================================================
    # 六、用户可控清空（第4章 4.5）
    # ==================================================================
    def clear(self, *, session_id: str | None = None, memory_id: str | None = None) -> dict:
        if not self.ctx.vector_store:
            return {"removed": 0, "available": False,
                    "note": "向量库不可用，长期记忆已关闭"}
        removed = self.ctx.vector_store.clear(session_id=session_id, memory_id=memory_id)
        self.ctx.db.log_agent(
            session_id=session_id or "all", task_id="", agent_role=self.agent_role,
            event="memory.cleared",
            detail=f"手动清空长期记忆 {removed} 条（范围：{'单条 ' + memory_id if memory_id else (session_id or '全部')}）",
        )
        return {"removed": removed, "available": self.ctx.vector_store.available}

    def clear_short_term(self, session_id: str) -> int:
        bucket = self._short_term.pop(session_id, None)
        return len(bucket) if bucket else 0

    # ==================================================================
    def stats(self) -> dict:
        vs = self.ctx.vector_store.stats() if self.ctx.vector_store else {"available": False, "count": 0}
        return {
            "agent": self.agent_role,
            "model_name": self.model_name,
            "consumed_events": self.consumed_events,
            "compressed_events": self.compressed_events,
            "archived_sessions": self.archived_sessions,
            "short_term_sessions": len(self._short_term),
            "short_term_entries": sum(len(v) for v in self._short_term.values()),
            "long_term": vs,
            "ttl_days": MEMORY_LONG_TERM_TTL_DAYS,
            "mode": "总线上主动推送 + 异步消费（不轮询）",
        }

    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        """记忆Agent 也可被显式调用（例如任务结束时主动沉淀）。"""
        self.require_capability(CAPABILITY_MEMORY_WRITE)
        stored = await self.compress_and_store(msg)
        return self.result_message(
            task,
            (stored or {}).get("text") or "本条事件无需进入长期记忆",
            metadata={"stored": bool(stored), "memory_id": (stored or {}).get("memory_id", "")},
        )


__all__ = ["MemoryAgent", "MEMORY_SYSTEM_PROMPT", "EMBED_DIM"]
