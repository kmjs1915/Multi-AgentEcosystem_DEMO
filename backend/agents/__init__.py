# -*- coding: utf-8 -*-
"""
七大 Agent 注册表（Agent能力层）

架构文档来源：第1章 1.3 模型固定分配（最终定型，不可修改）
本模块是「角色 ↔ 模型绑定」的唯一落实点：七个角色固定实例化，顺序与绑定不可调换。
"""

from __future__ import annotations

from backend.agents.base import AgentContext, CAPABILITY_MATRIX, ROLE_IMMUTABLE, BaseAgent
from backend.agents.code_agent import CodeAgent
from backend.agents.delivery_agent import DeliveryAgent
from backend.agents.dispatch_agent import DispatchAgent
from backend.agents.doc_agent import DocAgent
from backend.agents.evaluator_agent import EvaluatorAgent
from backend.agents.memory_agent import MemoryAgent
from backend.agents.vision_agent import VisionAgent
from backend.utils.constants import (
    AGENT_BINDINGS,
    AGENT_CODE,
    AGENT_DELIVERY,
    AGENT_DISPATCH,
    AGENT_DOC,
    AGENT_EVALUATOR,
    AGENT_MEMORY,
    AGENT_ROLES,
    AGENT_VISION,
)

AGENT_CLASSES: dict[str, type[BaseAgent]] = {
    AGENT_DISPATCH: DispatchAgent,
    AGENT_CODE: CodeAgent,
    AGENT_DOC: DocAgent,
    AGENT_VISION: VisionAgent,
    AGENT_MEMORY: MemoryAgent,
    AGENT_EVALUATOR: EvaluatorAgent,
    AGENT_DELIVERY: DeliveryAgent,
}


def build_agents(ctx: AgentContext) -> dict[str, BaseAgent]:
    """按文档固定顺序构建七个 Agent，并校验绑定一致性。"""
    agents: dict[str, BaseAgent] = {}
    for role in AGENT_ROLES:
        cls = AGENT_CLASSES[role]
        agent = cls(ctx)
        # 绑定一致性断言：角色与模型不得调换（第1章 1.3）
        binding = AGENT_BINDINGS[role]
        assert cls.agent_role == role, f"Agent 类角色错配：{cls.__name__} -> {cls.agent_role} != {role}"
        assert agent.model_name == binding["model_name"], (
            f"{role} 模型绑定错配：{agent.model_name} != {binding['model_name']}（{ROLE_IMMUTABLE}）"
        )
        agents[role] = agent
    return agents


def agent_registry_info() -> list[dict]:
    """供前端/接口展示的七大 Agent 固定信息。"""
    out: list[dict] = []
    for role in AGENT_ROLES:
        b = AGENT_BINDINGS[role]
        out.append({
            "agent": role,
            "model_name": b["model_name"],
            "provider": b["provider"],
            "model": b["model"],
            "duty": b["duty"],
            "capabilities": sorted(CAPABILITY_MATRIX[role]),
            "role_immutable": ROLE_IMMUTABLE,
        })
    return out


__all__ = ["build_agents", "agent_registry_info", "AGENT_CLASSES"]
