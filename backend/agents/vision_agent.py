# -*- coding: utf-8 -*-
"""
视觉感知Agent（Qwen3.8-Flash）—— 系统视觉入口

架构文档来源：第4章 4.4
  定位：系统视觉入口，仅处理图像类输入
  触发条件：用户输入包含图片/截图才触发，无图片直接跳过，节省Token
  核心能力：UI界面识别、报错截图分析、页面结构解析、视觉信息结构化输出
  约束：只输出结构化描述，不直接生成代码，代码生成统一交给代码工程Agent

第3章 3.3 图片资源传输规则：用户上传图片存入当前会话资源目录，
        消息 metadata 携带 image_resources 数组传递路径，仅视觉感知Agent可解析。
"""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path

from backend.agents.base import AGENT_VISION, CAPABILITY_IMAGE_PARSE, BaseAgent
from backend.bus.message import Message
from backend.bus.state_machine import TaskState
from backend.services.model_client import ModelUnavailable
from backend.utils.constants import AGENT_CODE, AGENT_TEMPERATURES, AGENT_VISION, CAP_VISION
from backend.utils.paths import display_relative, is_agent_business_file, is_within

VISION_SYSTEM_PROMPT = """你是「视觉感知Agent」（Qwen3.8-Flash），本地化多Agent协同开发工作台的唯一视觉入口。

【硬约束 · 不可违反】
1. 只输出结构化描述。严禁直接生成代码；若识别到需要编码修复的问题，只在 fix_hints 中描述"要改什么"，由代码工程Agent完成编码。
2. 只描述图中真实可见的内容。看不清、不确定的内容必须写进 uncertainty，禁止猜测。
3. 你只能解析当前会话目录内的图片，不得读取目录外文件。

【输出格式 · 必须是合法 JSON】
{
  "image_count": 1,
  "images": [
    {
      "path": "图片相对路径",
      "kind": "ui_screenshot / error_screenshot / diagram / photo / document_scan / unknown",
      "scene": "整体场景一句话描述",
      "elements": [
        {"type": "button/input/text/table/chart/icon/window", "name": "元素名称",
         "position": "在画面中的位置", "state": "可见状态/文字内容"}
      ],
      "texts": ["图中可见的文字（OCR）"],
      "error_analysis": {
        "has_error": true,
        "error_type": "异常类型（如空指针/导入失败/编译错误）",
        "error_message": "报错关键信息",
        "probable_cause": "最可能的原因"
      },
      "uncertainty": ["无法确定的内容"]
    }
  ],
  "structured_description": "面向后续Agent的整合描述（Markdown）",
  "fix_hints": ["给代码工程Agent的修复方向（只描述，不写代码）"]
}
"""

# 单张图片编码上限（防止上下文爆炸）
MAX_IMAGE_BYTES = 8 * 1024 * 1024


class VisionAgent(BaseAgent):
    agent_role = AGENT_VISION
    model_name = "Qwen3.8-Flash"   # 【需求点 BUG-NEW2】固定绑定 Qwen3.8-Flash（qwen3.8-flash）

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = VISION_SYSTEM_PROMPT
        self.skipped_for_no_image = 0

    # ==================================================================
    # 触发判定（第4章 4.4 触发条件：无图片直接跳过，节省Token）
    # ==================================================================
    def should_trigger(self, image_resources: list[str]) -> bool:
        return bool(image_resources)

    # ==================================================================
    # 图片编码
    # ==================================================================
    def _encode_image(self, absolute_path: str) -> tuple[str, str] | None:
        path = Path(absolute_path)
        if not path.exists() or not path.is_file():
            return None

        # ==================================================================
        # 【需求点 一、1】路径归属校验拆分为两类，避免跨盘符误判：
        #   · is_system_meta_file  ：会话私有目录（upload/artifact）内的系统元文件
        #                            → 跳过 subpath 校验，直接放行；
        #   · is_agent_business_file：业务文件（含工作区内的图片）
        #                            → 必须位于当前工作区文件夹内。
        # 历史实现用 path.resolve().relative_to(session_root) 单一判断，
        # 当工作区在 E 盘、会话目录在 C 盘时，合法的工作区图片会被拒绝解析。
        # ==================================================================
        session_root = self.ctx.session_paths.root
        workspace_root = self.ctx.file_guard.workspace_root
        resolved = path.resolve()

        is_meta = (self.ctx.file_guard.is_system_meta_file(resolved)
                   or is_within(resolved, session_root))
        if not is_meta and not is_agent_business_file(resolved, workspace_root):
            # 【需求点 一、2】subpath 计算走 display_relative，跨盘符时不抛 ValueError
            self.ctx.logger.exception_log(
                error_code="VISION_PATH_DENIED",
                message=(f"图片既不属会话目录也不在当前工作区内，拒绝解析："
                         f"{display_relative(resolved, workspace_root)}"),
                agent_role=self.agent_role,
            )
            return None

        if path.stat().st_size > MAX_IMAGE_BYTES:
            self.ctx.logger.exception_log(
                error_code="VISION_IMAGE_TOO_LARGE",
                message=f"图片超过 {MAX_IMAGE_BYTES // 1024 // 1024}MB，跳过：{absolute_path}",
                agent_role=self.agent_role,
            )
            return None

        mime = mimetypes.guess_type(path.name)[0] or "image/png"
        b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        return mime, b64

    # ==================================================================
    # 主流程
    # ==================================================================
    async def analyze(self, task: TaskState, image_resources: list[str], *, focus: str = "") -> dict:
        self.require_capability(CAPABILITY_IMAGE_PARSE)

        if not self.should_trigger(image_resources):
            self.skipped_for_no_image += 1
            self.ctx.db.log_agent(
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                event="vision.skipped", detail="用户输入不含图片，按第4.4 触发条件直接跳过（节省Token）",
            )
            return {"ok": True, "skipped": True,
                    "reason": "用户输入不包含图片/截图，视觉感知Agent 未触发（第4章 4.4）"}

        task.bump_iteration()
        self.ctx.runtime.router.check_timeout(task)
        self.think(task, "read", f"检测到 {len(image_resources)} 张图片资源，触发视觉解析")

        content: list[dict] = [{"type": "text", "text": "\n".join(x for x in [
            f"【解析聚焦点】{focus}" if focus else "【解析聚焦点】无，做通用结构化识别",
            f"【图片数量】{len(image_resources)}",
        ] if x)}]

        usable: list[str] = []
        for res in image_resources[:5]:
            encoded = self._encode_image(res)
            if not encoded:
                continue
            mime, b64 = encoded
            usable.append(res)
            content.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}})

        if not usable:
            return {"ok": False, "error": "未能加载任何可用图片（路径不存在、越界或超出大小上限）"}

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": content},
        ]

        try:
            response = await self.call_model(
                # 【需求点 Bug4/Bug7】视觉感知 Agent 温度 0.2（结构化抽取）；模型固定 Qwen-VL
                task, messages, temperature=AGENT_TEMPERATURES[AGENT_VISION],
                max_tokens=3000, expect_json=True,
                step_label=f"Qwen3.8-Flash 解析 {len(usable)} 张图片",
            )
        except ModelUnavailable as exc:
            return {"ok": False, "error": f"视觉感知能力当前不可用：{exc}"}

        try:
            data = self.parse_json(response.text)
        except ValueError:
            data = {"structured_description": response.text[:3000], "images": []}
        if not isinstance(data, dict):
            data = {"structured_description": str(data), "images": []}

        data["ok"] = True
        data["analyzed_paths"] = usable
        data["image_count"] = len(usable)
        # 后端兜底：视觉Agent 绝不产出代码（第4章 4.4 约束）
        data["code_generation_delegated_to"] = AGENT_CODE
        data["only_structured_description"] = True
        self.think(task, "write", "输出结构化视觉描述（不含代码，代码生成统一交给代码工程Agent）")
        return data

    # ==================================================================
    def build_output(self, data: dict) -> str:
        if data.get("skipped"):
            return data.get("reason", "视觉感知Agent 未触发")
        if not data.get("ok"):
            return f"视觉解析失败：{data.get('error', '未知原因')}"

        lines = ["## 视觉感知结果", "", str(data.get("structured_description") or "").strip()]
        for img in (data.get("images") or [])[:5]:
            if not isinstance(img, dict):
                continue
            lines += ["", f"### 图片：{img.get('path', '未知路径')}（{img.get('kind', 'unknown')}）"]
            if img.get("scene"):
                lines.append(f"- 场景：{img['scene']}")
            for el in (img.get("elements") or [])[:20]:
                if isinstance(el, dict):
                    lines.append(
                        f"- 元素[{el.get('type', '未知')}] {el.get('name', '')} "
                        f"@ {el.get('position', '')} — {el.get('state', '')}"
                    )
            texts = img.get("texts") or []
            if texts:
                lines.append("- 图中文字：" + "；".join(str(t) for t in texts[:15]))
            err = img.get("error_analysis") or {}
            if isinstance(err, dict) and err.get("has_error"):
                lines.append(
                    f"- 报错分析：{err.get('error_type', '')} / {err.get('error_message', '')} "
                    f"→ 可能原因：{err.get('probable_cause', '')}"
                )
            if img.get("uncertainty"):
                lines.append("- 不确定项：" + "；".join(str(u) for u in img["uncertainty"][:8]))
        if data.get("fix_hints"):
            lines += ["", "## 修复方向（仅描述，代码由代码工程Agent生成）"]
            lines += [f"- {h}" for h in data["fix_hints"][:10]]
        return "\n".join(lines)

    # ==================================================================
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        self.require_capability(CAPABILITY_IMAGE_PARSE)
        images = msg.image_resources
        focus = str(msg.metadata.get("instruction") or msg.content_text())
        data = await self.analyze(task, images, focus=focus)
        if data.get("skipped"):
            return self.result_message(task, data["reason"], metadata={"skipped": True})
        if not data.get("ok"):
            return self.error_message(task, "VISION_ANALYZE_FAILED", str(data.get("error")))
        return self.result_message(task, self.build_output(data), metadata={
            "image_count": data.get("image_count"),
            "analyzed_paths": data.get("analyzed_paths"),
            "only_structured_description": True,
        })


__all__ = ["VisionAgent", "VISION_SYSTEM_PROMPT", "MAX_IMAGE_BYTES"]
