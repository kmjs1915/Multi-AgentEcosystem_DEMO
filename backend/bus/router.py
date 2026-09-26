# -*- coding: utf-8 -*-
"""
消息总线 + Agent_Router 路由调度器

架构文档来源：
  - 第4章 4.1 调度规划Agent 专属组件 Agent_Router：
        "消息总线内置路由模块，负责生成ID、校验消息、拦截循环依赖、超时监听、消息投递"
  - 第3章 3.1 统一消息结构体 / 3.4 状态机 / 3.5 审批结果执行规则
  - 第4章 4.5 记忆管理Agent："消息总线主动推送全量事件，记忆Agent异步消费，不轮询"
  - 第2章 2.1 防死循环（循环依赖直接终止 / 超时监听 / 迭代上限）

设计：
  - MessageBus  : 订阅/投递中心，所有消息永久落库（第9章 9.2）
  - AgentRouter : 生成ID、校验消息、拦截循环依赖、超时监听、消息投递
  - 记忆管理Agent 以异步订阅者身份接入（asyncio.Queue），不做轮询
"""

from __future__ import annotations

import asyncio
import threading
import traceback
from collections import deque
from typing import Awaitable, Callable

from backend.bus.message import Message, MessageValidationError, validate_message
from backend.bus.state_machine import LoopDetected, TaskState
from backend.infrastructure.database import Database
from backend.infrastructure.logger import EcosystemLogger
from backend.utils.constants import AGENT_MEMORY, AGENT_ROLES, MAX_TASK_TIMEOUT_SECONDS, ERR_TIMEOUT
from backend.utils.paths import new_uuid

AgentHandler = Callable[[Message], Awaitable[Message | None]]


class MessageBus:
    """统一消息总线（系统唯一通信通道，禁止 Agent 之间直连）。"""

    def __init__(self, db: Database, logger: EcosystemLogger):
        self.db = db
        self.logger = logger
        self._handlers: dict[str, AgentHandler] = {}
        # 记忆管理Agent 的事件缓冲（第4章 4.5：总线主动推送 + 异步消费，不轮询）。
        # 使用 deque 而非 asyncio.Queue：投递与消费都是同步原子操作，
        # 不依赖事件循环调度点，确保任何启动顺序下事件都不会滞留。
        self._memory_buffer: deque[Message] = deque()
        self._memory_consumer: asyncio.Task | None = None
        self._memory_agent = None
        self._memory_wakeup: asyncio.Event | None = None
        self._recent_targets: deque[tuple[str, str, str]] = deque(maxlen=500)   # (task_id, sender, receiver)
        self._published_ids: set[str] = set()                                   # 总线发布幂等
        self.stats = {
            "published": 0, "delivered": 0, "rejected": 0,
            "approval_requests": 0, "memory_events": 0,
        }

    # ------------------------------------------------------------------
    # 订阅
    # ------------------------------------------------------------------
    def subscribe(self, agent_role: str, handler: AgentHandler) -> None:
        if agent_role not in AGENT_ROLES:
            raise ValueError(f"未登记 Agent 角色：{agent_role}")
        self._handlers[agent_role] = handler

    def subscribers(self) -> list[str]:
        return sorted(self._handlers.keys())

    # ------------------------------------------------------------------
    # 发布 / 投递
    # ------------------------------------------------------------------
    async def publish(self, msg: Message) -> Message:
        """消息进入总线的唯一入口：校验 -> 落库 -> 推送记忆。

        接收方为 Agent 时校验其已订阅；接收方为 user 的消息（如审批推送、最终交付）
        属于审计/展示事件，只落库并推送记忆，不做 Agent 投递。

        幂等：同一 msg_id 只落库/推送一次（避免 Agent 已自行 publish 的消息被重复投递）。
        """
        validate_message(msg)
        if not msg.msg_id:
            msg.msg_id = new_uuid()
        if msg.msg_id in self._published_ids:
            return msg
        self._published_ids.add(msg.msg_id)
        if len(self._published_ids) > 5000:
            self._published_ids.clear()

        record = msg.to_dict(include_msg_id=True)
        self.db.insert_message(record)
        self.stats["published"] += 1
        if msg.msg_type == "approval_request":
            self.stats["approval_requests"] += 1

        self.logger.task_log(
            session_id=msg.session_id, task_id=msg.task_id, agent_role=msg.sender_agent,
            event=f"bus.publish -> {msg.receiver_agent}",
            detail=f"type={msg.msg_type} status={msg.status} content={msg.content_text()[:300]}",
        )

        # 第4章 4.5：总线上所有事件主动推送给记忆管理Agent（异步消费，不阻塞主流程）
        await self._push_to_memory(msg)

        if msg.receiver_agent == "user":
            # 面向用户的审计事件（审批推送 / 最终交付）：已落库 + 已推送记忆，无需 Agent 投递
            return msg
        if msg.receiver_agent not in self._handlers:
            raise MessageValidationError(f"消息接收方未订阅：{msg.receiver_agent}", code="NO_SUBSCRIBER")
        return msg

    async def deliver(self, msg: Message) -> Message | None:
        """投递到目标 Agent，并把其响应消息也送回总线（统一结构体，全链路可追溯）。

        第4章 4.5：消息总线主动推送「全量事件」给记忆管理Agent —— 因此 Agent 的响应
        消息同样必须经过总线，否则记忆管理Agent 只能看到任务入口而看不到执行结果。
        """
        handler = self._handlers.get(msg.receiver_agent)
        if handler is None:
            raise MessageValidationError(f"消息接收方未订阅：{msg.receiver_agent}", code="NO_SUBSCRIBER")
        self._recent_targets.append((msg.task_id, msg.sender_agent, msg.receiver_agent))
        self.stats["delivered"] += 1
        reply = await handler(msg)

        if reply is not None:
            if not reply.msg_id:
                reply.msg_id = new_uuid()
            if reply.msg_id not in self._published_ids:
                await self.publish(reply)
        return reply

    async def send_and_receive(self, msg: Message) -> Message | None:
        await self.publish(msg)
        if msg.receiver_agent == "user":
            return None
        return await self.deliver(msg)

    # ------------------------------------------------------------------
    # 记忆管理Agent 异步消费（主动推送模型，不轮询）
    # ------------------------------------------------------------------
    _MEMORY_BUFFER_LIMIT = 5000

    async def _push_to_memory(self, msg: Message) -> None:
        self.stats["memory_events"] += 1
        if len(self._memory_buffer) >= self._MEMORY_BUFFER_LIMIT:
            self.logger.warning(
                "记忆缓冲区已满，丢弃最早的一条事件（不影响主流程）", task_id=msg.task_id,
            )
            self._memory_buffer.popleft()
        self._memory_buffer.append(msg)
        # 唤醒异步消费者；若事件循环未运行则跳过（随后由 flush 兜底消费）
        if self._memory_wakeup is not None:
            self._memory_wakeup.set()

    def start_memory_consumer(self, memory_agent) -> None:
        """挂载记忆管理Agent 并启动异步消费协程。

        若调用时事件循环尚未运行（同步初始化路径），只登记 Agent；
        随后在首个异步入口（run_pipeline / flush_memory_events）会自动挂载。
        """
        self._memory_agent = memory_agent
        if self._memory_consumer and not self._memory_consumer.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._memory_wakeup = asyncio.Event()
        self._memory_consumer = loop.create_task(self._memory_loop())

    def _ensure_memory_consumer(self) -> None:
        if self._memory_agent is None:
            return
        if self._memory_consumer and not self._memory_consumer.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._memory_wakeup is None:
            self._memory_wakeup = asyncio.Event()
        self._memory_consumer = loop.create_task(self._memory_loop())

    async def _memory_loop(self) -> None:
        """异步消费者：有新事件才唤醒，不做任何轮询。"""
        while True:
            try:
                if self._memory_wakeup is None:
                    await asyncio.sleep(0.05)
                    continue
                await self._memory_wakeup.wait()
                self._memory_wakeup.clear()
            except asyncio.CancelledError:
                return
            await self._drain_memory_buffer()

    async def _drain_memory_buffer(self) -> int:
        """把缓冲区中的事件逐个交给记忆管理Agent 消费。"""
        processed = 0
        while self._memory_buffer:
            msg = self._memory_buffer.popleft()
            try:
                await self._memory_agent.consume(msg, bus=self)
            except Exception as exc:  # noqa: BLE001 记忆失败绝不影响主任务链路
                self.logger.exception_log(
                    error_code="MEMORY_CONSUME_FAILED",
                    message=f"记忆事件消费失败：{exc}",
                    session_id=msg.session_id, task_id=msg.task_id, agent_role=AGENT_MEMORY,
                    stack=traceback.format_exc(),
                )
            processed += 1
        return processed

    async def flush_memory_events(self, max_rounds: int = 3) -> int:
        """确保事件被消费（任务收口 / 测试 / 关闭服务时调用）。"""
        self._ensure_memory_consumer()
        total = 0
        for _ in range(max_rounds):
            total += await self._drain_memory_buffer()
            if not self._memory_buffer:
                break
            await asyncio.sleep(0)
        return total

    async def drain_memory_queue(self, timeout: float = 3.0) -> None:
        """兼容旧接口名：等待记忆事件消费完毕。"""
        try:
            await asyncio.wait_for(self.flush_memory_events(), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    async def shutdown(self) -> None:
        if self._memory_consumer:
            self._memory_consumer.cancel()
            try:
                await self._memory_consumer
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._memory_consumer = None

    # ------------------------------------------------------------------
    # 消息回环检测辅助（供 Agent_Router 使用）
    # ------------------------------------------------------------------
    def recent_targets(self, task_id: str, limit: int = 8) -> list[tuple[str, str, str]]:
        rows = [t for t in self._recent_targets if t[0] == task_id]
        return rows[-limit:]


class AgentRouter:
    """Agent_Router —— 消息总线内置路由模块（架构文档 4.1 专属组件）。

    职责严格限定为五项：生成ID、校验消息、拦截循环依赖、超时监听、消息投递。
    """

    def __init__(self, bus: MessageBus, db: Database, logger: EcosystemLogger):
        self.bus = bus
        self.db = db
        self.logger = logger
        self._graph: dict[str, dict] = {}      # task_id -> {'parent': id|None, 'deps': set[str]}
        self._lock = threading.RLock()
        self.stats = {"generated_ids": 0, "validated": 0, "loops_blocked": 0, "timeout_checks": 0}

    # ---------------- 1. 生成 ID ----------------
    def new_session_id(self) -> str:
        self.stats["generated_ids"] += 1
        return new_uuid()

    def new_task_id(self) -> str:
        self.stats["generated_ids"] += 1
        return new_uuid()

    def register_task(self, state: TaskState, *, depend_on: list[str] | None = None) -> None:
        deps = list(depend_on or [])
        with self._lock:
            self._graph[state.task_id] = {"parent": state.parent_task_id, "deps": set(deps)}
        self.db.insert_task(state.to_dict() | {"depend_on": deps})

    def known_task_ids(self) -> set[str]:
        with self._lock:
            return set(self._graph.keys())

    def drop_task(self, task_id: str) -> None:
        with self._lock:
            self._graph.pop(task_id, None)

    # ---------------- 3. 拦截循环依赖 ----------------
    def detect_loop(self, task_id: str, parent_task_id: str | None, depend_on: list[str]) -> None:
        """依赖图 DFS 回环检测 + 消息打转检测；命中即抛 LoopDetected（第2章 2.1 规则3）。"""
        with self._lock:
            if parent_task_id and parent_task_id == task_id:
                self.stats["loops_blocked"] += 1
                raise LoopDetected("任务自身作为父任务，构成循环依赖，直接终止")

            self._graph.setdefault(task_id, {"parent": None, "deps": set()})
            self._graph[task_id]["parent"] = parent_task_id or self._graph[task_id].get("parent")
            self._graph[task_id]["deps"] = set(depend_on)

            visiting: set[str] = set()
            visited: set[str] = set()

            def dfs(node: str, path: list[str]) -> None:
                if node in visiting:
                    raise LoopDetected(f"检测到任务循环依赖（{' -> '.join(path + [node])}），直接终止")
                if node in visited:
                    return
                visiting.add(node)
                info = self._graph.get(node, {})
                for nxt in list(info.get("deps") or []):
                    dfs(nxt, path + [node])
                parent = info.get("parent")
                if parent:
                    dfs(parent, path + [node])
                visiting.discard(node)
                visited.add(node)

            try:
                dfs(task_id, [])
            except LoopDetected:
                self.stats["loops_blocked"] += 1
                self.logger.exception_log(
                    error_code="CIRCULAR_DEPENDENCY",
                    message=f"任务 {task_id} 循环依赖被 Agent_Router 拦截",
                    task_id=task_id, agent_role="Agent_Router",
                )
                raise

        # 消息打转检测：必须构成严格交替回环（A->B、B->A、A->B、B->A）才判定为死循环前兆。
        # 仅凭"最近两条方向相反"会误伤正常的两步子任务（分发 + 回执），故要求 4 条严格交替。
        recent = self.bus.recent_targets(task_id, limit=4)
        if len(recent) == 4:
            directions = [(s, r) for _, s, r in recent]
            evens = directions[0::2]
            odds = directions[1::2]
            strictly_alternating = (
                evens[0] == evens[1]
                and odds[0] == odds[1]
                and evens[0][0] == odds[0][1]
                and evens[0][1] == odds[0][0]
                and evens[0] != odds[0]
            )
            if strictly_alternating:
                with self._lock:
                    self.stats["loops_blocked"] += 1
                raise LoopDetected("检测到 Agent 消息来回打转（疑似循环依赖），直接终止")

    # ---------------- 2. 校验消息 ----------------
    def validate(self, msg: Message) -> None:
        validate_message(msg)
        self.stats["validated"] += 1

    # ---------------- 4. 超时监听 ----------------
    def check_timeout(self, state: TaskState) -> None:
        """单任务最大超时 30 分钟（第2章 2.1 规则2 / 第9章 9.1 规则3）。"""
        self.stats["timeout_checks"] += 1
        state.check_timeout()

    def timeout_seconds(self) -> int:
        return MAX_TASK_TIMEOUT_SECONDS

    # ---------------- 5. 消息投递 ----------------
    async def route(self, msg: Message) -> Message | None:
        """校验 + 投递（Agent 间通信必须经过本方法，禁止直连）。"""
        self.validate(msg)
        return await self.bus.send_and_receive(msg)

    async def route_no_reply(self, msg: Message) -> None:
        self.validate(msg)
        await self.bus.publish(msg)

    def stats_snapshot(self) -> dict:
        return {**self.stats, **self.bus.stats}


__all__ = ["MessageBus", "AgentRouter", "LoopDetected", "ERR_TIMEOUT"]
