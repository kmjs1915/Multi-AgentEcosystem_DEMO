# -*- coding: utf-8 -*-
"""
统一标准消息结构体（消息总线协议层）

架构文档来源：第3章 消息总线与统一通信协议（DSH核心编码依据）
  3.1 标准消息结构体：
      session_id / task_id / parent_task_id / sender_agent / receiver_agent /
      msg_type / payload{content, metadata} / timestamp(13位) / status
  3.2 审批事件专属字段（强制）：risk_level / operation_desc / operation_params / danger_reason
  3.3 图片资源传输规则：metadata 携带 image_resources 数组，仅视觉感知Agent可解析

硬约束：系统所有 Agent 通信、任务流转、审批事件、异常通知，全部使用本唯一固定结构体，
       不存在任何自定义零散消息格式。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from backend.utils.constants import (
    META_IMAGE_RESOURCES,
    MSG_TYPE_APPROVAL_REQUEST,
    VALID_MSG_TYPES,
    VALID_RISK_LEVELS,
    VALID_STATUSES,
    AGENT_ROLES,
    RISK_LEVEL_HIGH,
)
from backend.utils.paths import SecurityViolation


class MessageValidationError(Exception):
    """消息校验失败（Agent_Router 校验环节）。"""

    def __init__(self, message: str, *, code: str = "INVALID_MESSAGE", detail: dict | None = None):
        super().__init__(message)
        self.code = code
        self.detail = detail or {}


# ==========================================================================
# 3.2 审批事件专属 metadata（强制字段）
# ==========================================================================
@dataclass
class ApprovalMetadata:
    """审批事件 metadata 强制三字段 + 扩展（第3章 3.2）。"""

    risk_level: str                                  # high / mid / low
    operation_desc: str                              # 完整操作描述
    operation_params: str                            # 执行参数/命令/文件路径
    danger_reason: str                               # 风险说明
    operation_type: str = ""                         # 文件删除 / 批量修改 / 系统命令 / 外网下载
    tool: str = ""
    matched_patterns: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.risk_level not in VALID_RISK_LEVELS:
            raise MessageValidationError(
                f"risk_level 非法：{self.risk_level!r}（必须为 {VALID_RISK_LEVELS}）",
                code="INVALID_RISK_LEVEL",
            )
        for name in ("operation_desc", "operation_params", "danger_reason"):
            if getattr(self, name) in (None, ""):
                raise MessageValidationError(
                    f"审批 metadata 缺少强制字段 {name}（第3章 3.2）", code="MISSING_APPROVAL_FIELD",
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_level": self.risk_level,
            "operation_desc": self.operation_desc,
            "operation_params": self.operation_params,
            "danger_reason": self.danger_reason,
            "operation_type": self.operation_type,
            "tool": self.tool,
            "matched_patterns": list(self.matched_patterns),
        }

    @classmethod
    def from_dict(cls, meta: Mapping[str, Any]) -> "ApprovalMetadata":
        return cls(
            risk_level=str(meta.get("risk_level", "")),
            operation_desc=str(meta.get("operation_desc", "")),
            operation_params=str(meta.get("operation_params", "")),
            danger_reason=str(meta.get("danger_reason", "")),
            operation_type=str(meta.get("operation_type", "")),
            tool=str(meta.get("tool", "")),
            matched_patterns=list(meta.get("matched_patterns") or []),
        )


# ==========================================================================
# 3.1 标准消息结构体
# ==========================================================================
@dataclass
class Message:
    session_id: str
    task_id: str
    sender_agent: str
    receiver_agent: str
    msg_type: str
    payload: dict[str, Any] = field(default_factory=lambda: {"content": "", "metadata": {}})
    parent_task_id: str | None = None
    timestamp: int = 0                # 13 位毫秒时间戳
    status: str = "pending"
    msg_id: str = ""                  # 内部主键（非协议字段，仅用于存储/去重）

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = int(time.time() * 1000)

    # ---------------- 序列化 ----------------
    def to_dict(self, *, include_msg_id: bool = False) -> dict[str, Any]:
        """输出严格符合第3章 3.1 的 JSON 结构体。"""
        payload = {
            "content": self.payload.get("content", ""),
            "metadata": self.payload.get("metadata", {}) or {},
        }
        data: dict[str, Any] = {
            "session_id": self.session_id,
            "task_id": self.task_id,
            "parent_task_id": self.parent_task_id or "",
            "sender_agent": self.sender_agent,
            "receiver_agent": self.receiver_agent,
            "msg_type": self.msg_type,
            "payload": payload,
            "timestamp": int(self.timestamp),
            "status": self.status,
        }
        if include_msg_id:
            data["msg_id"] = self.msg_id
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Message":
        payload = data.get("payload") or {}
        return cls(
            session_id=str(data.get("session_id", "")),
            task_id=str(data.get("task_id", "")),
            parent_task_id=(str(data["parent_task_id"]) if data.get("parent_task_id") else None),
            sender_agent=str(data.get("sender_agent", "")),
            receiver_agent=str(data.get("receiver_agent", "")),
            msg_type=str(data.get("msg_type", "")),
            payload={
                "content": payload.get("content", ""),
                "metadata": payload.get("metadata", {}) or {},
            },
            timestamp=int(data.get("timestamp") or 0),
            status=str(data.get("status", "pending")),
            msg_id=str(data.get("msg_id", "")),
        )

    # ---------------- 便捷属性 ----------------
    @property
    def content(self) -> Any:
        return self.payload.get("content", "")

    @property
    def metadata(self) -> dict[str, Any]:
        return self.payload.get("metadata", {}) or {}

    @property
    def image_resources(self) -> list[str]:
        """第3章 3.3 图片资源数组。"""
        value = self.metadata.get(META_IMAGE_RESOURCES)
        return list(value) if isinstance(value, list) else []

    def approval_metadata(self) -> ApprovalMetadata:
        if self.msg_type != MSG_TYPE_APPROVAL_REQUEST:
            raise MessageValidationError("非审批消息无法读取审批专属字段", code="NOT_APPROVAL_MESSAGE")
        return ApprovalMetadata.from_dict(self.metadata)

    def content_text(self) -> str:
        c = self.content
        return c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)


# ==========================================================================
# 消息校验（Agent_Router：校验消息）
# ==========================================================================
def validate_message(msg: Message, *, known_task_ids: set[str] | None = None) -> None:
    """严格按第3章 3.1 / 3.2 校验。任何不合规消息一律拒绝进入总线。"""
    if not msg.session_id:
        raise MessageValidationError("session_id 不能为空", code="MISSING_SESSION_ID")
    if not msg.task_id:
        raise MessageValidationError("task_id 不能为空", code="MISSING_TASK_ID")
    if not msg.sender_agent:
        raise MessageValidationError("sender_agent 不能为空", code="MISSING_SENDER")
    if not msg.receiver_agent:
        raise MessageValidationError("receiver_agent 不能为空", code="MISSING_RECEIVER")

    if msg.msg_type not in VALID_MSG_TYPES:
        raise MessageValidationError(
            f"msg_type 非法：{msg.msg_type!r}（必须为 {VALID_MSG_TYPES} 之一）",
            code="INVALID_MSG_TYPE",
        )
    if msg.status not in VALID_STATUSES:
        raise MessageValidationError(
            f"status 非法：{msg.status!r}（必须为 {VALID_STATUSES} 之一）", code="INVALID_STATUS",
        )
    if not isinstance(msg.payload, dict) or "content" not in msg.payload:
        raise MessageValidationError("payload 必须为含 content 的对象", code="INVALID_PAYLOAD")
    if msg.payload.get("metadata") is not None and not isinstance(msg.payload["metadata"], dict):
        raise MessageValidationError("payload.metadata 必须为对象", code="INVALID_METADATA")

    # timestamp 必须为 13 位毫秒级
    if len(str(int(msg.timestamp))) != 13:
        raise MessageValidationError(
            f"timestamp 必须为 13 位时间戳，实际：{msg.timestamp}", code="INVALID_TIMESTAMP",
        )

    # 角色合法性：sender 可以是 system/用户入口，receiver 必须是七大 Agent 之一
    sender_ok = msg.sender_agent in AGENT_ROLES or msg.sender_agent in ("user", "system", "Agent_Router")
    if not sender_ok:
        raise MessageValidationError(f"未登记 sender_agent：{msg.sender_agent}", code="UNKNOWN_SENDER")
    if msg.receiver_agent not in AGENT_ROLES and msg.receiver_agent not in ("user", "Agent_Router"):
        raise MessageValidationError(f"未登记 receiver_agent：{msg.receiver_agent}", code="UNKNOWN_RECEIVER")

    # 3.2 审批事件强制字段校验
    if msg.msg_type == MSG_TYPE_APPROVAL_REQUEST:
        meta = msg.metadata
        missing = [f for f in ("risk_level", "operation_desc", "operation_params", "danger_reason")
                   if meta.get(f) in (None, "")]
        if missing:
            raise MessageValidationError(
                f"approval_request 缺少强制 metadata 字段：{missing}（第3章 3.2）",
                code="MISSING_APPROVAL_FIELD",
            )
        if meta["risk_level"] not in VALID_RISK_LEVELS:
            raise MessageValidationError(
                f"risk_level 非法：{meta['risk_level']!r}", code="INVALID_RISK_LEVEL",
            )
        # 高危必须 high（第2章 2.2 规则3）
        if meta["risk_level"] != RISK_LEVEL_HIGH and meta.get("force_high"):
            raise MessageValidationError("高危操作 risk_level 不得降级", code="RISK_DOWNGRADE_DENIED")

    # 3.3 image_resources 必须是字符串数组
    if META_IMAGE_RESOURCES in msg.metadata:
        imgs = msg.metadata[META_IMAGE_RESOURCES]
        if not isinstance(imgs, list) or any(not isinstance(i, str) for i in imgs):
            raise MessageValidationError(
                "metadata.image_resources 必须为字符串路径数组（第3章 3.3）",
                code="INVALID_IMAGE_RESOURCES",
            )

    # 父任务引用合法性
    if known_task_ids is not None and msg.parent_task_id and msg.parent_task_id not in known_task_ids:
        raise MessageValidationError(
            f"parent_task_id 未登记：{msg.parent_task_id}", code="UNKNOWN_PARENT_TASK",
        )


def new_payload(content: Any, metadata: dict | None = None) -> dict[str, Any]:
    return {"content": content, "metadata": metadata or {}}


def precheck_message_dict(data: Mapping[str, Any]) -> Message:
    """先做前置结构校验再构造（入口防污染）。"""
    required = ("session_id", "task_id", "sender_agent", "receiver_agent", "msg_type", "payload", "status")
    missing = [k for k in required if k not in data]
    if missing:
        raise MessageValidationError(f"消息缺少必需字段：{missing}", code="MISSING_FIELDS")
    return Message.from_dict(data)
