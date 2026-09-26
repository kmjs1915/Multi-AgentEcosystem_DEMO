# -*- coding: utf-8 -*-
"""
交互交付Agent（Kimi k2.6）—— 用户交互门面

【需求点 Bug8】豆包（doubao）已从系统移除，本 Agent 现绑定 Kimi k2.6（kimi-k2.6），
  Agent 职责与定位完全不变（用户对话、需求澄清、结果润色、前端展示格式化）。

架构文档来源：第4章 4.7
  定位：用户交互门面，只负责美化、润色、格式化
  硬约束：不修改业务结果、不参与决策、不改变代码与文档内容，仅优化展示形态

后端双重保险（防止模型私自改内容）：
  1. 系统提示词硬约束只做展示层改写；
  2. 交付前后做「事实一致性核查」：原文出现的代码块/文件路径/数字必须原样保留，
     一旦发现关键事实被增删改，直接丢弃润色结果，回退使用原始文本（business content 不可变）。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from backend.agents.base import AGENT_DELIVERY, CAPABILITY_POLISH, BaseAgent
from backend.bus.message import Message
from backend.bus.state_machine import TaskState
from backend.services.model_client import ModelUnavailable
from backend.utils.constants import AGENT_DELIVERY, AGENT_TEMPERATURES

DELIVERY_SYSTEM_PROMPT = """你是「交互交付Agent」（Kimi k2.6），本地化多Agent协同开发工作台的用户交互门面。

【硬约束 · 绝对不可违反】
1. 你只负责展示层的美化、润色、格式化。
2. 严禁修改任何业务结果、严禁参与决策、严禁改变代码与文档内容。
3. 代码块必须逐字原样保留（含缩进、符号、注释）。
4. 文件路径、命令、数字、版本号、报错原文必须逐字保留，不得改写、不得四舍五入、不得补充。
5. 不得新增原文没有的结论、承诺、事实。

【允许做的事】
- 调整层级标题、分段、加粗关键信息、调整列表与表格排版
- 口语化表达改为清晰书面表达（不改变语义）
- 统一术语写法与标点

【输出格式 · 必须是合法 JSON】
{
  "polished": "润色后的完整展示文本（Markdown）",
  "format_notes": ["本次做了哪些纯展示层调整"],
  "content_changed": false,
  "fallback_used": false
}
如果原文本身已足够清晰，直接原样返回，content_changed 保持 false。
"""


@dataclass
class DeliveryResult:
    text: str
    polished: bool = False
    fallback_used: bool = False
    format_notes: list[str] = None
    reason: str = ""

    def __post_init__(self):
        if self.format_notes is None:
            self.format_notes = []

    def to_dict(self) -> dict:
        return {
            "text": self.text, "polished": self.polished,
            "fallback_used": self.fallback_used,
            "format_notes": self.format_notes, "reason": self.reason,
        }


class DeliveryAgent(BaseAgent):
    agent_role = AGENT_DELIVERY
    model_name = "Kimi k2.6"   # 【需求点 Bug8】固定绑定 Kimi k2.6（kimi-k2.6）

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = DELIVERY_SYSTEM_PROMPT

    # ==================================================================
    # 事实一致性核查（业务结果不可修改的后端保险）
    # ==================================================================
    @staticmethod
    def _extract_facts(text: str) -> dict[str, set[str]]:
        return {
            "code_blocks": set(re.findall(r"```.*?```", text, flags=re.S)),
            "paths": set(re.findall(r"[\w\-./\\]*[A-Za-z0-9_\-]+\.(?:py|js|ts|tsx|jsx|java|go|rs|c|cpp|h|md|json|yaml|yml|txt|docx|pdf|log|ini|toml)", text)),
            "numbers": set(re.findall(r"\b\d+(?:\.\d+)?%?\b", text)),
            "commands": set(re.findall(r"(?m)^\s*(?:Pwsh|python|pip|npm|git|node)\b.*$", text)),
        }

    def verify_facts_unchanged(self, original: str, polished: str) -> tuple[bool, list[str]]:
        """返回 (是否保持事实不变, 缺失/新增事实说明)。"""
        problems: list[str] = []
        before = self._extract_facts(original)
        after = self._extract_facts(polished)

        for key, label in (("code_blocks", "代码块"), ("paths", "文件路径"), ("commands", "命令")):
            missing = before[key] - after[key]
            if missing:
                problems.append(f"{label}被改动或删除：{list(missing)[:3]}")

        # 数字：允许格式微调（如 3 与 3.0），但必须全部仍然出现
        after_numbers = set(re.findall(r"\d+(?:\.\d+)?", polished))
        before_numbers = set(re.findall(r"\d+(?:\.\d+)?", original))
        lost = {n for n in before_numbers if n not in after_numbers and n.lstrip("0") not in after_numbers}
        if lost:
            problems.append(f"数字被改动或删除：{sorted(lost)[:6]}")

        return (not problems), problems

    # ==================================================================
    # 主流程
    # ==================================================================
    async def polish(self, task: TaskState, text: str, *, degraded_notes: list[str] | None = None) -> DeliveryResult:
        self.require_capability(CAPABILITY_POLISH, detail="交互交付Agent 只做展示润色")

        if not text or not text.strip():
            return DeliveryResult(text="", polished=False, reason="空内容无需润色")

        task.bump_iteration()
        self.ctx.runtime.router.check_timeout(task)
        self.think(task, "edit", "对最终回复做展示层润色（不修改业务结果）")

        note_block = ""
        if degraded_notes:
            note_block = "\n\n【系统降级提示（必须原样保留在回复中）】\n" + "\n".join(f"- {n}" for n in degraded_notes)

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"【待润色的原始回复（业务内容，不可改动）】\n{text}{note_block}"},
        ]

        try:
            response = await self.call_model(
                # 【需求点 Bug4】总结/润色子 Agent 温度 0.45（区间 0.4~0.5）
                task, messages, temperature=AGENT_TEMPERATURES[AGENT_DELIVERY],
                max_tokens=4000, expect_json=True,
                step_label="Kimi k2.6 展示层润色",
            )
            data = self.parse_json(response.text)
        except ModelUnavailable as exc:
            # 第2章 2.3 规则2：交付模型失效 → 该项能力禁用，直接返回原始文本（不阻断）
            self.ctx.logger.exception_log(
                error_code="DELIVERY_MODEL_UNAVAILABLE",
                message=f"交互交付模型不可用，回退原始文本：{exc}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return DeliveryResult(text=text, polished=False, fallback_used=True,
                                  reason=f"交付模型不可用，已回退原始业务文本：{exc}")
        except ValueError:
            return DeliveryResult(text=text, polished=False, fallback_used=True,
                                  reason="交付模型输出非法 JSON，已回退原始业务文本")

        if not isinstance(data, dict):
            return DeliveryResult(text=text, polished=False, fallback_used=True,
                                  reason="交付模型输出结构异常，已回退原始业务文本")

        polished = str(data.get("polished") or "").strip()
        if not polished:
            return DeliveryResult(text=text, polished=False, fallback_used=True,
                                  reason="交付模型未返回内容，已回退原始业务文本")

        ok, problems = self.verify_facts_unchanged(text, polished)
        if not ok:
            # 硬约束：业务结果不可修改 → 丢弃润色结果
            self.ctx.logger.exception_log(
                error_code="DELIVERY_CONTENT_CHANGED",
                message="交付模型改动了业务内容（违反第4章 4.7 硬约束），已丢弃润色结果：" + "；".join(problems),
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            self.think(task, "edit", "润色结果改动了业务事实，按硬约束已丢弃并回退原文")
            return DeliveryResult(text=text, polished=False, fallback_used=True,
                                  reason="交付模型改动了业务内容，已回退原始业务文本：" + "；".join(problems[:3]))

        notes = [str(n) for n in (data.get("format_notes") or [])][:10]
        self.think(task, "edit", "展示层润色完成，业务事实一致性核查通过")
        return DeliveryResult(text=polished, polished=True, format_notes=notes,
                              reason="展示层润色完成，未改动业务内容")

    # ==================================================================
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        self.require_capability(CAPABILITY_POLISH)
        result = await self.polish(task, msg.content_text())
        return self.result_message(task, result.text, metadata={
            "kind": "delivery",
            "polished": result.polished,
            "fallback_used": result.fallback_used,
            "format_notes": result.format_notes,
            "reason": result.reason,
            "business_content_unchanged": not result.fallback_used,
        })


__all__ = ["DeliveryAgent", "DeliveryResult", "DELIVERY_SYSTEM_PROMPT"]
