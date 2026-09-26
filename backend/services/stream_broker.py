# -*- coding: utf-8 -*-
"""
协同思维链流式事件总线（服务层）

【需求点 Bug2 流式思维链输出】
  前端需要实时看到多 Agent 协同过程：谁把任务分派给了谁、对方用的哪个模型、
  正在执行什么内容。为此引入"任务级流式事件通道"：

  · 后端在任务链路的关键节点调用 StreamBroker.emit()，事件进入该任务的队列；
  · 前端通过 SSE（GET /api/task/{task_id}/stream）订阅，逐条实时推送；
  · 事件仅用于**展示**，不参与业务决策，任务状态机与消息总线结构体保持不变。

事件类型（event 字段）与前端展示的思维链动作一一对应：
  task_created  任务创建（含任务标题、会话、工作区根目录）
  plan         调度规划Agent 完成拆解（含子任务清单）
  dispatch     调度规划Agent 分派子任务给某个 Agent（**任务流转核心事件**）
  agent_step   Agent 的思考步骤（调用哪个模型、正在做什么）
  model_call   实际发生的模型调用（provider / model / token）
  tool_call    代码工程Agent 的工具调用（写文件、命令等）
  task_status  任务状态机流转（pending/running/waiting_approval/success/failed）
  approval     高危操作进入人工审批
  fallback     模型生态位补位
  done          本次任务链路结束（终态）
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

from backend.utils.constants import AGENT_BINDINGS, PROVIDER_CAPABILITIES

# 单个任务最多缓存的事件条数（防止超长任务把内存撑爆）
MAX_EVENTS_PER_TASK = 2000
# 已完成任务的事件保留时长（秒），供前端断线重连后补看
EVENT_RETENTION_SECONDS = 600


@dataclass
class StreamEvent:
    """单条思维链流式事件（统一结构，前端按 type 渲染）。"""

    seq: int                     # 任务内自增序号（前端可据此去重/排序）
    event: str                   # 事件类型，见模块文档
    task_id: str
    session_id: str = ""
    timestamp: int = 0           # 13 位毫秒时间戳（与消息结构体口径一致）
    agent_role: str = ""         # 相关 Agent 角色
    from_agent: str = ""         # 任务流转：来源 Agent
    to_agent: str = ""           # 任务流转：目标 Agent
    model_label: str = ""        # 该 Agent 实际使用的模型名称
    provider: str = ""
    status: str = ""             # 任务状态机状态（如有）
    title: str = ""              # 子任务标题
    text: str = ""               # 人类可读描述（前端直接展示）
    detail: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = int(time.time() * 1000)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_sse(self) -> str:
        """SSE 帧格式。"""
        return f"event: {self.event}\ndata: {json.dumps(self.to_dict(), ensure_ascii=False)}\n\n"


class TaskStream:
    """单个任务的流式事件通道（生产者-消费者，支持多订阅者）。"""

    def __init__(self, task_id: str, session_id: str = ""):
        self.task_id = task_id
        self.session_id = session_id
        self.created_at = time.time()
        self.closed = False
        self._events: deque[StreamEvent] = deque(maxlen=MAX_EVENTS_PER_TASK)
        self._seq = 0
        self._subscribers: list[asyncio.Queue] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.RLock()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def publish(self, event: StreamEvent) -> None:
        """发布事件：写入历史 + 推送给所有在线订阅者（非阻塞、丢弃式，绝不拖慢主链路）。"""
        with self._lock:
            self._seq += 1
            event.seq = self._seq
            if not event.session_id:
                event.session_id = self.session_id
            self._events.append(event)
            targets = list(self._subscribers)

        for queue in targets:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # 订阅者消费过慢：丢弃最旧一条，保证主链路绝不被阻塞
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        with self._lock:
            self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        with self._lock:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    def history(self, after_seq: int = 0) -> list[StreamEvent]:
        with self._lock:
            return [e for e in self._events if e.seq > after_seq]

    def close(self) -> None:
        self.closed = True

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)


class StreamBroker:
    """任务级流式事件总线（进程内单例，由运行时持有）。"""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskStream] = {}
        self._lock = threading.RLock()
        self.stats = {"tasks": 0, "events": 0, "subscribers": 0}

    # ---------------- 生命周期 ----------------
    def open(self, task_id: str, session_id: str = "") -> TaskStream:
        with self._lock:
            stream = self._tasks.get(task_id)
            if stream is None:
                stream = TaskStream(task_id, session_id)
                try:
                    stream.bind_loop(asyncio.get_running_loop())
                except RuntimeError:
                    pass
                self._tasks[task_id] = stream
                self.stats["tasks"] += 1
            self._gc_locked()
            return stream

    def get(self, task_id: str) -> TaskStream | None:
        with self._lock:
            return self._tasks.get(task_id)

    def close(self, task_id: str) -> None:
        stream = self.get(task_id)
        if stream:
            stream.close()

    def _gc_locked(self) -> None:
        """回收过期任务通道（仅回收已完成且超过保留期的）。"""
        now = time.time()
        stale = [
            tid for tid, s in self._tasks.items()
            if s.closed and (now - s.created_at) > EVENT_RETENTION_SECONDS
            and s.subscriber_count == 0
        ]
        for tid in stale:
            self._tasks.pop(tid, None)

    # ---------------- 发布 ----------------
    def emit(
        self,
        task_id: str,
        event: str,
        *,
        session_id: str = "",
        agent_role: str = "",
        from_agent: str = "",
        to_agent: str = "",
        model_label: str = "",
        provider: str = "",
        status: str = "",
        title: str = "",
        text: str = "",
        **detail: Any,
    ) -> StreamEvent | None:
        """发布一条思维链事件。任何异常都被吞掉，绝不影响任务主链路。

        【需求点 Bug7】若调用方未显式给出 provider / model_label，
        则按 constants.AGENT_BINDINGS（Agent↔模型绑定的唯一来源）自动补齐，
        保证前端每条事件都能看到「哪个 Agent、用的哪个模型」。
        """
        try:
            if agent_role and agent_role in AGENT_BINDINGS:
                binding = AGENT_BINDINGS[agent_role]
                if not provider:
                    provider = binding["provider"]
                if not model_label:
                    meta_name = PROVIDER_CAPABILITIES.get(binding["provider"], {}).get("name")
                    label = f"{meta_name} {binding['model']}".strip() if meta_name else binding["model_name"]
                    model_label = label or binding["model_name"]
            stream = self.get(task_id)
            if stream is None:
                stream = self.open(task_id, session_id)
            evt = StreamEvent(
                seq=0, event=event, task_id=task_id, session_id=session_id or stream.session_id,
                agent_role=agent_role, from_agent=from_agent, to_agent=to_agent,
                model_label=model_label, provider=provider, status=status,
                title=title, text=text, detail=detail or {},
            )
            stream.publish(evt)
            self.stats["events"] += 1
            return evt
        except Exception:  # noqa: BLE001 流式推送失败绝不影响业务
            return None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                **self.stats,
                "active_tasks": len(self._tasks),
                "subscribers": sum(s.subscriber_count for s in self._tasks.values()),
            }
