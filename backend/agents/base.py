# -*- coding: utf-8 -*-
"""
Agent 基类 —— 职责隔离与能力白名单强制约束（Agent能力层）

架构文档来源：
  - 第1章 1.2 核心设计理念 规则2：单职责Agent —— 7个Agent角色完全解耦，各司其职，无职责重叠
  - 第2章 2.2 规则2：操作白名单
  - 第4章 4.1 调度规划Agent 硬约束：不执行任何文件、命令、写操作，仅做调度决策
  - 第4章 4.7 交互交付Agent 硬约束：不修改业务结果、不参与决策、不改变代码与文档内容

实现机制：
  1. 能力矩阵（CAPABILITY_MATRIX）声明每个 Agent 被允许使用的能力；
  2. require_capability() 在调用前强制校验，越权直接抛 SecurityViolation，
     并且必须落入 error 日志（禁止静默忽略）；
  3. "执行类"方法（文件写、命令、删除、下载）在基类中物理不存在，
     仅由 CodeExecutionCapability 提供，而该 capability 只授予代码工程Agent。
"""

from __future__ import annotations

import json
import time
from typing import Any

from backend.bus.message import Message, new_payload
from backend.bus.state_machine import TaskState
from backend.infrastructure.database import Database
from backend.infrastructure.logger import EcosystemLogger
from backend.services.approval_center import ApprovalCenter
from backend.services.model_client import ModelClient, ModelResponse, ModelUnavailable
from backend.utils.constants import (
    AGENT_BINDINGS,
    AGENT_CODE,
    AGENT_DELIVERY,
    AGENT_DISPATCH,
    AGENT_DOC,
    AGENT_EVALUATOR,
    AGENT_MEMORY,
    AGENT_VISION,
    EVENT_MODEL_BINDING_AUDIT,
    STATUS_FAILED,
    THINK_LEVEL_ERROR,
    THINK_LEVEL_INFO,
)
from backend.utils.paths import SecurityViolation

# ==========================================================================
# 能力矩阵（第1章 1.2 规则2：单职责Agent，能力不重叠）
#   file_write   : 会话目录内文件写/改
#   file_delete  : 文件删除（必须审批）
#   command      : 系统命令执行（必须审批）
#   download     : 外网下载（必须审批）
#   doc_parse    : 长文档解析
#   image_parse  : 图像解析（第3章 3.3：仅视觉感知Agent可解析）
#   memory_write : 长期记忆写入/检索
#   evaluate     : 质量闸门校验
#   polish       : 展示润色
#   dispatch     : 任务拆解与调度决策
# ==========================================================================
CAPABILITY_FILE_WRITE = "file_write"
CAPABILITY_FILE_DELETE = "file_delete"
CAPABILITY_COMMAND = "command"
CAPABILITY_DOWNLOAD = "download"
CAPABILITY_DOC_PARSE = "doc_parse"
CAPABILITY_IMAGE_PARSE = "image_parse"
CAPABILITY_MEMORY_WRITE = "memory_write"
CAPABILITY_EVALUATE = "evaluate"
CAPABILITY_POLISH = "polish"
CAPABILITY_DISPATCH = "dispatch"

CAPABILITY_MATRIX: dict[str, set[str]] = {
    # 第4章 4.1：仅调度决策，无任何文件/命令/写能力
    AGENT_DISPATCH: {CAPABILITY_DISPATCH, CAPABILITY_MEMORY_WRITE},
    # 第4章 4.2：裸机核心执行端
    AGENT_CODE: {CAPABILITY_FILE_WRITE, CAPABILITY_FILE_DELETE, CAPABILITY_COMMAND, CAPABILITY_DOWNLOAD},
    # 第4章 4.3：长文档解析，禁止输出原始长文本
    AGENT_DOC: {CAPABILITY_DOC_PARSE},
    # 第4章 4.4：仅图像输入触发，不生成代码
    AGENT_VISION: {CAPABILITY_IMAGE_PARSE},
    # 第4章 4.5：记忆中枢
    AGENT_MEMORY: {CAPABILITY_MEMORY_WRITE},
    # 第4章 4.6：唯一质检/裁判/安全审核中心
    AGENT_EVALUATOR: {CAPABILITY_EVALUATE},
    # 第4章 4.7：仅美化、润色、格式化
    AGENT_DELIVERY: {CAPABILITY_POLISH},
}

ROLE_IMMUTABLE = "角色与模型绑定固定，不得调换"


class AgentContext:
    """单次任务执行上下文（由运行时编排器注入）。"""

    def __init__(
        self,
        *,
        session_paths,
        file_guard,
        model_client: ModelClient,
        approval_center: ApprovalCenter,
        db: Database,
        logger: EcosystemLogger,
        memory_agent=None,
        vector_store=None,
        runtime=None,
    ):
        self.session_paths = session_paths
        self.file_guard = file_guard
        self.model_client = model_client
        self.approval_center = approval_center
        self.db = db
        self.logger = logger
        self.memory_agent = memory_agent
        self.vector_store = vector_store
        self.runtime = runtime


class BaseAgent:
    """七大 Agent 的公共基类。"""

    agent_role: str = ""
    model_name: str = ""
    allowed_capabilities: set[str] = set()
    system_prompt: str = ""

    def __init__(self, ctx: AgentContext):
        self.ctx = ctx
        self.step_counter = 0

    # ------------------------------------------------------------------
    # 元信息（第1章 1.3 模型固定分配）
    # ------------------------------------------------------------------
    def binding(self) -> dict:
        return AGENT_BINDINGS[self.agent_role]

    def info(self) -> dict:
        b = self.binding()
        return {
            "agent": self.agent_role,
            "model_name": b["model_name"],
            "provider": b["provider"],
            "model": b["model"],
            "duty": b["duty"],
            "capabilities": sorted(CAPABILITY_MATRIX[self.agent_role]),
            "role_immutable": ROLE_IMMUTABLE,
        }

    # ------------------------------------------------------------------
    # 能力白名单强制校验（越权 = 安全违规，必须记录）
    # ------------------------------------------------------------------
    def require_capability(self, capability: str, *, detail: str = "") -> None:
        allowed = CAPABILITY_MATRIX.get(self.agent_role, set())
        if capability not in allowed:
            message = (
                f"{self.agent_role} 越权调用能力「{capability}」被拦截。"
                f"该角色允许能力：{sorted(allowed) or '无'}。{detail}"
            )
            self.ctx.logger.exception_log(
                error_code="CAPABILITY_DENIED", message=message, agent_role=self.agent_role,
            )
            raise SecurityViolation(message, code="CAPABILITY_DENIED",
                                    detail={"agent_role": self.agent_role, "capability": capability})

    # ------------------------------------------------------------------
    # 思考过程（第7.3 对话页面：AI思考过程，可折叠、分步图标）
    # 【BUG-C 5】思考过程输出增强：每一步都可标注 level（info/warn/error），
    #   发生错误时用 error 级别落库 → 前端展示红色错误标识，禁止用 success 掩盖失败。
    # ------------------------------------------------------------------
    def think(self, task: TaskState, step_type: str, text: str,
              *, level: str = THINK_LEVEL_INFO) -> dict:
        self.step_counter += 1
        self.ctx.db.add_think_step(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            step_index=self.step_counter, step_type=step_type, step_text=text,
            level=level,
        )
        return {"type": step_type, "text": text, "agent": self.agent_role,
                "index": self.step_counter, "level": level}

    def think_error(self, task: TaskState, text: str, *, step_type: str = "exec") -> dict:
        """【BUG-C 2/5】错误级别的思考步骤（前端红色错误标识）。"""
        return self.think(task, step_type, text, level=THINK_LEVEL_ERROR)

    # ------------------------------------------------------------------
    # 模型调用 + Token 真实采集（第2章 2.4）
    # ------------------------------------------------------------------
    async def call_model(
        self,
        task: TaskState,
        messages: list[dict],
        *,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        expect_json: bool = False,
        step_label: str = "模型推理",
    ) -> ModelResponse:
        available, reason = self.ctx.model_client.is_available(self.agent_role)
        if not available:
            # 第2章 2.3 规则2：能力被禁用时返回"能力不可用提示"，不崩溃整体系统
            self.think(task, "think", f"能力不可用：{reason}", level=THINK_LEVEL_ERROR)
            raise ModelUnavailable(reason, agent_role=self.agent_role)

        # ==================================================================
        # 【BUG-A 1/2】运行态绑定审计：真实业务调用与连通测试必须使用同一套配置数据源。
        #   每次调用前打印「Agent → 厂商 → 模型 → Base URL → 密钥指纹」，
        #   一旦发生"测试成功但实际调用失败"，日志里能直接看出读的是哪家的密钥。
        # ==================================================================
        target = self.ctx.model_client.resolve_target_detail(self.agent_role)
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event=EVENT_MODEL_BINDING_AUDIT,
            detail=(f"Agent={self.agent_role} | 厂商={target['provider_name']}({target['provider']}) | "
                    f"模型标识={target['model']} | 展示名={target['model_name']} | "
                    f"BaseURL={target['base_url']} | 端点={target['url']} | "
                    f"密钥指纹={target['key_fingerprint'] or '(未配置)'} | "
                    f"密钥来源={target['key_source']} | 连通测试={target['test_state']}"),
        )

        self.think(task, "think", step_label)
        started = time.time()
        try:
            response = await self.ctx.model_client.chat(
                self.agent_role, messages, temperature=temperature,
                max_tokens=max_tokens, expect_json=expect_json,
                task_id=task.task_id,   # 【需求点 三、2】按任务周期记忆补位模型
            )
        except ModelUnavailable as exc:
            # 【BUG-C 5】模型不可用属于错误：以 error 级别写进思考链，不用 success 掩盖
            self.think(task, "exec", f"调用失败：{exc}", level=THINK_LEVEL_ERROR)
            raise

        # Token 统计由后端采集并四维度落库（第2章 2.4 规则2）
        self.ctx.logger.token_log({
            "session_id": task.session_id,
            "task_id": task.task_id,
            "agent_role": self.agent_role,
            "provider": response.provider,
            "model": response.model,
            "input_tokens": response.input_tokens,
            "output_tokens": response.output_tokens,
            "cached_tokens": response.cached_tokens,
            "cache_hit_rate": response.cache_hit_rate,
            "elapsed_ms": response.elapsed_ms,
            "steps": 1,
        })
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="model.call",
            detail=f"{response.provider}/{response.model} in={response.input_tokens} "
                   f"out={response.output_tokens} cached={response.cached_tokens} "
                   f"{response.elapsed_ms}ms"
                   + (f" | 生态位补位: {response.fallback_note}" if response.ecosystem_fallback else ""),
        )
        # 【需求点 三、2】生态位补位成功 → 记录到运行时，供前端轻提示与状态栏展示
        if response.ecosystem_fallback and self.ctx.runtime is not None:
            self.ctx.runtime.register_ecosystem_fallback(
                agent_role=self.agent_role,
                original=response.original_model_label,
                actual=response.actual_model_label or response.model,
                note=response.fallback_note,
                reason=response.failure_reason,
            )
            self.think(task, "think", response.fallback_note)
        elif response.degraded:
            self.think(task, "think", response.degrade_note or "已使用备选模型")
        return response

    # ------------------------------------------------------------------
    # 消息构造工具（统一结构体，禁止自定义格式）
    # ------------------------------------------------------------------
    def make_message(
        self,
        task: TaskState,
        receiver: str,
        msg_type: str,
        content: Any,
        *,
        metadata: dict | None = None,
        status: str,
    ) -> Message:
        return Message(
            session_id=task.session_id,
            task_id=task.task_id,
            parent_task_id=task.parent_task_id,
            sender_agent=self.agent_role,
            receiver_agent=receiver,
            msg_type=msg_type,
            payload=new_payload(content, metadata),
            status=status,
        )

    def result_message(self, task: TaskState, content: Any, metadata: dict | None = None) -> Message:
        return self.make_message(task, AGENT_DISPATCH, "result", content,
                                 metadata=metadata, status="success")

    def error_message(self, task: TaskState, code: str, message: str,
                      receiver: str = AGENT_DISPATCH) -> Message:
        return self.make_message(task, receiver, "error", message,
                                 metadata={"code": code}, status=STATUS_FAILED)

    # ------------------------------------------------------------------
    # 统一 JSON 抽取
    # ------------------------------------------------------------------
    @staticmethod
    def parse_json(text: str) -> Any:
        from backend.services.model_client import _extract_json
        return _extract_json(text)

    def json_or_fail(self, task: TaskState, response: ModelResponse) -> Any:
        try:
            return self.parse_json(response.text)
        except ValueError as exc:
            self.ctx.logger.exception_log(
                error_code="MODEL_JSON_INVALID", message=str(exc),
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            raise

    # ------------------------------------------------------------------
    # 入口（子类实现 handle）
    # ------------------------------------------------------------------
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        raise NotImplementedError

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{type(self).__name__} role={self.agent_role!r} model={self.model_name!r}>"


__all__ = [
    "BaseAgent", "AgentContext", "CAPABILITY_MATRIX", "ROLE_IMMUTABLE",
    "CAPABILITY_FILE_WRITE", "CAPABILITY_FILE_DELETE", "CAPABILITY_COMMAND",
    "CAPABILITY_DOWNLOAD", "CAPABILITY_DOC_PARSE", "CAPABILITY_IMAGE_PARSE",
    "CAPABILITY_MEMORY_WRITE", "CAPABILITY_EVALUATE", "CAPABILITY_POLISH", "CAPABILITY_DISPATCH",
    "AGENT_DISPATCH", "AGENT_CODE", "AGENT_DOC", "AGENT_VISION",
    "AGENT_MEMORY", "AGENT_EVALUATOR", "AGENT_DELIVERY",
]
