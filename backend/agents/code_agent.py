# -*- coding: utf-8 -*-
"""
代码工程Agent（DeepSeek-Flash）【裸机核心执行端】

架构文档来源：第4章 4.2
  定位：系统执行终端，负责裸机代码运行、文件操作、终端命令执行
  核心能力：代码生成、修改、重构、调试、报错修复、依赖安装、项目构建、本地文件管理
  裸机安全逻辑（完全替代Docker）：
      1. 所有操作仅限当前会话独立工作目录
      2. 任何删除、批量修改、系统命令、外网下载自动触发 waiting_approval 状态
      3. 审批通过后裸机执行，拒绝则终止任务
      4. 禁止跨目录读写、禁止系统关键目录操作
  重试规则：代码错误最多重试 3 次

本 Agent 是系统中唯一持有 file_write / file_delete / command / download 能力的角色。
所有落盘动作都必须先经 assess_operation() 风险判定，高危一律走审批中心，绝不直接执行。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from backend.agents.base import (
    AGENT_CODE,
    CAPABILITY_COMMAND,
    CAPABILITY_DOWNLOAD,
    CAPABILITY_FILE_DELETE,
    CAPABILITY_FILE_WRITE,
    BaseAgent,
)
from backend.bus.message import ApprovalMetadata, Message
from backend.bus.state_machine import TaskState
from backend.services.model_client import JSONExtractionError, ModelUnavailable
from backend.utils.constants import (
    AGENT_CODE,
    AGENT_DISPATCH,
    AGENT_TEMPERATURES,
    ERR_CODE_RETRY_EXHAUSTED,
    ERR_IO,
    ERR_MODEL_CONVERGE_FAIL,
    ERR_MODEL_UNAVAILABLE,
    ERR_TOOL_FAILED,
    ERR_WORKSPACE_NOT_A_DIR,
    ERR_WORKSPACE_PATH_NOT_FOUND,
    ERR_WORKSPACE_PERMISSION_DENIED,
    FAILURE_KIND_IO,
    FAILURE_KIND_MODEL_CONVERGE,
    MAX_CODE_FIX_RETRIES,
    MAX_TOOL_CALL_RETRIES,
    META_IMAGE_RESOURCES,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_WAITING_APPROVAL,
    THINK_LEVEL_WARN,
    WORKSPACE_UNAVAILABLE_NOT_DIR,
    WORKSPACE_UNAVAILABLE_NOT_FOUND,
    WORKSPACE_UNAVAILABLE_NO_READ,
    classify_failure,
)
from backend.utils.paths import SecurityViolation, WorkspaceUnavailable
from backend.utils.security import assess_operation, normalize_filename

CODE_SYSTEM_PROMPT = """你是「代码工程Agent」（DeepSeek-Flash），本地化多Agent协同开发工作台的系统执行终端。

======================================================================
【队长-队员层级架构 · 你是队员】
======================================================================
1. 你是**队员**，队长是「调度规划Agent」（全局总控与唯一裁决权威）。
   你的每一次输出都会由运行时**上报队长**做一致性校验（无需你手动汇报）。
2. 当两名队员对同一批文件的判定出现差异（例如两份待删清单条目/数量不一致）时，
   队长会仲裁并给出**唯一统一清单**；该清单以
   【队长（调度规划Agent）已仲裁的唯一待删清单 · 强制沿用，不得自行增删】的形式
   出现在你的任务指令里。
3. **一旦指令中出现上述队长仲裁清单：它就是唯一基准** ——
   · 必须且只能使用该清单，禁止重新判定、禁止新增未列出的文件、禁止遗漏已列出的文件；
   · 你的个人判断与队长清单不一致时，**一律以队长清单为准**，不得输出"我认为还应删除 X"；
   · 队长发起的「冲突文件二次核验」子任务同样以队长指令为准，只判定指令中列出的文件。
4. 若没有队长仲裁清单，则按本任务指令正常判定，你的产出会被上报队长参与一致性校验，
   因此结论必须可复现、依据明确（不要凭感觉变动同一批文件的判定）。
5. 你的产出必须保持**跨阶段一致**：同一文件在同一工作区内的分类结论不得前后矛盾。

【裸机安全硬约束 · 不可违反】
1. 你只能操作当前工作区目录内的文件，所有路径必须是**相对路径**，禁止绝对路径、禁止 .. 越界、禁止跨工作区。
2. ======================================================================
   【职责边界 · 本系统最高优先级硬约束】
   你（LLM Agent）**只做分析与生成**，**绝对不允许执行任何文件删除 / 清理动作**：
     · 删除类任务你只输出**结构化待删除候选清单** candidates（含 path/category/reason/confidence），
       由队长校验后交给**后端 Python 程序**执行真实删除；
     · **严禁**在 summary / reasoning / 任何文本里写「已删除 xx 文件」「清理完成」
       「文件已不存在」这类宣称执行完成的表述 —— 你没有执行能力，这样写属于虚报，
       后端会检测并把该句改写为「未执行任何磁盘改动」并记入错误日志；
     · 删除结果以**后端真实执行回执**为准，你无权描述删除结果。
   你仍然可以调用 write_file / edit_file 等**非删除**类工具完成代码生成与修改；
   高危动作（删除 / 批量修改 / 系统命令 / 外网下载）的识别与放行全部由后端硬编码决定，
   不需要也不允许你自行判断"这是不是高危"。
   ======================================================================
3. 禁止操作系统关键目录（Windows、Program Files、System32、/etc、/usr 等）。
4. 单次写入内容不得超过 5MB。

======================================================================
【输出契约 · 最高优先级，违反即判为失败】
======================================================================
1. 你的**每一条回复都必须是且只能是一个 JSON 对象**，禁止任何自然语言解释、
   禁止 Markdown 代码围栏（不要写 ```json）、禁止 JSON 前后的任何文字。
2. 禁止大段自然语言推理：reasoning 字段**不得超过 200 字**，只写结论性依据。
3. **禁止反复思考**：不要在多轮之间重复分析同一份文件清单。
   后端已经把文件名清单直接给你（见任务指令中的【文件清单（后端原生扫描结果）】），
   **不要再去列举目录、不要重复读取清单、不要逐轮重新审视同一批文件**。
4. 拿到清单后，**最多用 1 轮**完成判断并输出 done=true；只有确实需要写文件/删文件
   时才输出 tool_calls。判断类任务（筛选 / 分类 / 生成候选清单）**直接 done=true**。
5. 如果无法判断某个文件的类型，**不要继续循环思考**：直接在 JSON 中把它标记为
   `"category": "unknown"`、`"confidence": "low"`、`"reason": "无法识别文件类型"`，
   并在顶层写 `"unrecognized": ["文件名", ...]`，然后 done=true 结束。
6. 无法完成时也必须输出合法 JSON，并把原因写进 reasoning ——
   绝不允许输出空回复、纯文本回复或半截 JSON。
7. 【JSON 语法硬规范】你只输出标准 JSON，禁止输出任何前置/后置解释文字，
   禁止 markdown 代码块。JSON 全部使用双引号，数组末尾不能加尾随逗号，
   JSON 内禁止写注释。
8. 【强输出约束 · 违反即判为失败】为便于后端**机器解析**，你的输出必须满足：
   · 只输出标准 JSON —— 禁止任何解释文字、禁止 ``` / ```json 等 Markdown 代码块；
   · 所有键名与字符串值**一律使用英文双引号** `"`，禁止单引号、禁止不加引号的键；
   · 禁止尾随逗号（数组 / 对象最后一个元素后不得有逗号）、禁止注释（// 与 /* */）；
   · 禁止 JSON 字面量 True/False/None，必须写作 true/false/null；
   · 第一个字符必须是 `{` 或 `[`，最后一个字符必须是 `}` 或 `]`，中间不得夹带散文。
   后端已具备容错清洗（自动剥离围栏、剔除前后文字与尾随逗号），
   但**不要依赖清洗**：你的输出本身就必须是合法 JSON。

【工具调用轮次硬限制】
- 单次任务最多 3 轮工具调用；达到限制后**必须**输出 done=true 并给出当前已确定的结果。
- 严禁"再确认一次""再检查一遍"这类无进展的重复动作。

【路径书写规则 · 极易出错，务必严格遵守】
- 工作区文件清单采用「类型：文件名」格式，例如：
      文件：note.txt (12B)
      目录：src
      文件：src/main.py (210B)
- 其中「文件：」「目录：」只是类型标注，**不是文件名的一部分**。
- 引用或创建文件时，必须原样使用标注后的路径，例如 `note.txt`、`src/main.py`。
- **严禁**给文件名添加任何前缀字符：不要写 `.note.txt`（多了一个点）、不要写 `/src/main.py`（多了一个斜杠）。
- 相对路径分隔符统一用 `/`；工作区根目录用 `.` 表示。

【可用工具 · 只能从下列清单中选择，禁止发明新工具】
{"tool":"list_dir","args":{"path":"."}}                             读取目录列表
{"tool":"read_file","args":{"path":"a.py"}}                         读取文件
{"tool":"write_file","args":{"path":"a.py","content":"..."}}        写入/新建文件
{"tool":"write_file","args":{"path":"a.py","content":"...","append":true}}  追加写入
{"tool":"delete_file","args":{"path":"a.py"}}                       删除文件【高危→审批】
{"tool":"run_command","args":{"command":"python a.py","cwd":"."}}   执行命令【高危→审批】
{"tool":"download","args":{"url":"https://...","path":"data/x.zip"}} 外网下载【高危→审批】

======================================================================
【需要继续操作时 · 唯一合法格式】
======================================================================
{
  "done": false,
  "reasoning": "本轮思路（≤200字）",
  "tool_calls": [ {"tool": "...", "args": {...}} ]
}

======================================================================
【任务已完成时 · 唯一合法格式（判断/筛选/生成清单类任务用这个）】
======================================================================
{
  "done": true,
  "reasoning": "完成说明（≤200字）",
  "summary": "给调度Agent的结果摘要（Markdown，含实际结论；不要复述清单全文）",
  "artifacts": ["a.py", "report.md"],
  "file_notes": [
    {"path": "相对路径", "purpose": "该文件的作用，一句话说明"}
  ],
  "candidates": [
    {"path": "相对路径", "category": "debug|test|temp|build|log|unknown",
     "confidence": "high|medium|low", "reason": "一句话判定依据",
     "suggest_delete": true, "risk": "low|mid|high"}
  ],
  "unrecognized": ["无法识别类型的文件名"],
  "stats": {"total": 0, "candidates": 0, "unrecognized": 0}
}

【candidates 字段规则】
- **只列待删除候选**（调试 / 测试 / 临时 / 构建产物 / 日志等），不要列出全部文件；
- 无法判断类型 → `"category":"unknown"` + `"confidence":"low"` + 同时写进 unrecognized；
- candidates 为空时输出 `"candidates": []`，并用 reasoning 说明"未发现待删除候选"；
- 路径必须与后端给出的文件名清单**逐字一致**，不得改写、不得加前缀。

【通用要求】
- 每轮 tool_calls 最多 6 个；拿到工具结果后继续下一轮，直到 done=true。
- 严禁编造工具执行结果；未真正执行过的内容不得写进 summary。
- 代码必须完整可运行，不要写占位符或省略号。
- 文件必须通过 write_file 工具真实写入磁盘；系统会做写后磁盘复核，未落盘的操作会被判定为失败。
"""

# ==========================================================================
# 【增量修复 2】判断 / 筛选类任务专用提示词：
#   文件名清单已由后端原生扫描给出，模型只做"语义分类"，不做遍历、不做多轮确认。
# ==========================================================================
CODE_CLASSIFY_HINT = """【本次任务为「判断 / 筛选 / 生成候选清单 / 文件作用总结」类 · 必须严格遵守】

1. 文件名清单**已由后端原生扫描完成**（见下方【文件清单（后端原生扫描结果）】），
   你**不需要也不允许**再调用 list_dir 去遍历目录、也不需要重复读取清单。
2. 你的唯一职责是**语义判断**：逐个判断清单中的文件是否属于
   调试文件 / 测试文件 / 临时文件 / 构建产物 / 日志文件，
   并在 file_notes 数组中给出每个文件的作用说明（path + purpose 一句话）。
3. 请**只输出一轮**：直接给出最终 JSON（done=true + candidates + file_notes 数组），
   不要在 tool_calls 里做"再确认""再检查"的重复动作。
4. 无法识别类型的文件：写进 candidates 时用
   `"category":"unknown"`、`"confidence":"low"`、`"reason":"无法识别文件类型"`，
   并同时把文件名放进顶层 `"unrecognized"` 数组 —— **不要为它反复思考**。
5. 若清单为空，直接输出 `"candidates": []` 并在 reasoning 中说明。
6. reasoning 不超过 200 字，禁止输出长篇自然语言分析。
7. 你只处理**本批次**给出的文件；不在本批清单中的文件一律不要出现在结果里。
"""

# 触发"原生扫描 + 只做分类"模式的指令关键词（命中任一即启用）
_CLASSIFY_INTENT_HINTS: tuple[str, ...] = (
    "扫描", "列出", "清单", "候选", "识别", "筛选", "分类", "调试文件", "测试文件",
    "临时文件", "待删除", "哪些文件", "文件列表", "目录列表", "文件作用", "总结每个文件",
)

# 分类模式下允许被"直接回绝"的只读工具：清单已由后端原生扫描给出，无需模型再遍历。
#   ★ 高危 / 写类工具（delete_file、run_command、download、write_file …）**不在此表**：
#     它们必须交回统一执行链路做风险判定并进入人工审批，绝不允许被静默忽略。
_CLASSIFY_READONLY_TOOLS: tuple[str, ...] = ("list_dir", "read_file", "list_files", "glob")

# 【需求点 1】文件分片分批上限：每批最多送入模型的文件数
CLASSIFY_BATCH_SIZE = 25
# 分批下限保护（需求要求每批 20~30 个文件：配置值落在区间外时钳制回来）
CLASSIFY_BATCH_MIN = 20
CLASSIFY_BATCH_MAX = 30
# 【需求点 1】单个批次内部的最大轮次：第 1 轮 + 最多 2 轮"带错误反馈的修正重试"
MAX_CLASSIFY_BATCH_ROUNDS = 3

# ==========================================================================
# 【需求点 1/3/4】分批语义分类专用提示词：
#   每批独立成一次模型调用，单批单轮出结果；重试时后端会把上一轮 JSON 语法
#   错误作为反馈追加进本轮 prompt，要求模型修正格式而不是继续解释。
# ==========================================================================
CODE_CLASSIFY_BATCH_HINT = """【本批次任务 · 只处理本批文件，必须严格遵守】

1. 文件清单由后端原生扫描（os.listdir）给出，并按批次切片下发。
   本次只给出**本批次**的文件子集，你**不需要也不允许**调用 list_dir 遍历目录。
2. 【严格限定范围】只处理本批清单中的文件：
   · file_notes 必须**逐个覆盖本批每一个文件**，不得遗漏、不得增加不在本批清单中的文件；
   · candidates 只从本批文件里挑选；不属于本批的文件一律不得出现在结果里。
3. 请**只输出一轮**：直接给出最终 JSON（done=true + file_notes + candidates），
   禁止在 tool_calls 里做"再确认""再检查"这类重复动作。
4. 【输出格式强制约束】只输出标准 JSON：
   · 禁止任何解释文字、禁止 Markdown 代码块（不要写 ```json）；
   · 键名与字符串值一律用英文双引号 `"`，禁止单引号、禁止无引号键；
   · 禁止尾随逗号、禁止注释（// 与 /* */）、禁止 True/False/None（用 true/false/null）；
   · 第一个字符必须是 `{`，最后一个字符必须是 `}`。
5. 无法识别类型的文件：在 file_notes 里把 purpose 写为「无法识别文件作用」，
   并在 candidates 里用 `"category":"unknown"`、`"confidence":"low"`、
   `"reason":"无法识别文件类型"`、`"suggest_delete": false`，
   同时把文件名放进顶层 `"unrecognized"` 数组 —— **不要为它反复思考**。
6. reasoning 不超过 200 字，禁止输出长篇自然语言分析。
7. 本批清单为空时输出 `"file_notes": []`、`"candidates": []` 并在 reasoning 说明。
"""

# 【需求点 3】重试反馈：把上一轮的 JSON 语法错误原文回灌给模型，要求修正格式
_JSON_RETRY_COMMON_RULES = (
    "硬性要求：\n"
    "1) 只输出标准 JSON，第一个字符就是 `{`，最后一个字符就是 `}`；\n"
    "2) 禁止任何解释文字、禁止 Markdown 代码块（不要写 ```json）、禁止 JSON 前后夹带散文；\n"
    "3) 键名与字符串值一律使用英文双引号，禁止单引号、禁止不加引号的键；\n"
    "4) 禁止尾随逗号、禁止注释、禁止 True/False/None（必须写 true/false/null）；\n"
    "5) 若某个文件无法判断，直接标 \"category\":\"unknown\" 并写入 unrecognized 数组，\n"
    "   不要用自然语言解释，也不要继续思考。\n"
    "请立即重新输出**纯 JSON**（不要再有任何解释）。"
)


def _build_json_error_feedback(error: Exception, *, round_no: int, max_rounds: int,
                               batch_label: str = "") -> str:
    """【需求点 3】把上一轮 JSON 语法错误构造成本轮 prompt 的修正反馈。

    反馈必须让模型明确知道三件事：
      ① 上一轮输出非法（附带后端解析器的行/列定位）；
      ② 常见的四类语法错误是什么（尾随逗号 / 单引号 / 注释 / Python 字面量）；
      ③ 本轮必须怎么改。
    """
    if isinstance(error, JSONExtractionError):
        detail = error.detail or str(error)
        cleaned = (error.cleaned_text or "").strip()
        raw = (error.raw_text or "").strip()
        snippet = cleaned or raw
        lines = [
            f"【上一轮输出非法 JSON · 修正反馈（第 {round_no}/{max_rounds} 轮）】"
            + (f"（{batch_label}）" if batch_label else ""),
            f"后端 JSON 解析失败：{detail}",
            f"后端已尝试的清洗策略：{('、'.join(error.attempts) or '无')}",
        ]
        if snippet:
            head = snippet[:600]
            tail = snippet[-200:] if len(snippet) > 800 else ""
            lines.append("你上一轮输出的（清洗后）片段如下，请对照检查语法错误：")
            lines.append("<<<MODEL_OUTPUT")
            lines.append(head + (f"\n…（中间省略）…\n{tail}" if tail else ""))
            lines.append("MODEL_OUTPUT")
        lines.append(
            "常见非法原因自查：① 数组/对象最后一个元素后多了**尾随逗号**；"
            "② 使用了单引号或未加引号的键名；③ 写了 // 或 /* */ 注释；"
            "④ 使用了 Python 字面量 True/False/None；⑤ JSON 前后或中间夹带了自然语言说明。"
        )
    else:
        lines = [
            f"【上一轮输出非法 JSON · 修正反馈（第 {round_no}/{max_rounds} 轮）】"
            + (f"（{batch_label}）" if batch_label else ""),
            f"后端 JSON 解析失败：{type(error).__name__}: {str(error)[:300]}",
        ]
    lines.append(_JSON_RETRY_COMMON_RULES)
    return "\n".join(lines)


def _classify_leftover_tool_calls(tool_calls: list) -> list[dict]:
    """分类/分批模式下筛出"不能静默忽略"的工具调用（非只读动作 = 写 / 删 / 命令 / 下载）。

    ★ 安全不变量：任何高危动作都必须经 assess_operation → 审批中心，
      绝不允许因为"当前是列表/分类类任务"而被静默丢弃（历史缺陷：
      模型给出的 delete_file 被无条件忽略 → 既不执行也不进审批）。
    """
    leftovers: list[dict] = []
    for call in tool_calls or []:
        if not isinstance(call, dict):
            continue
        if str(call.get("tool") or "") in _CLASSIFY_READONLY_TOOLS:
            continue
        leftovers.append(call)
    return leftovers


def _parse_batch_json(text: str) -> tuple[dict | None, JSONExtractionError | None]:
    """解析单批次模型输出；非法时返回 (None, 带原文的异常) 供重试反馈使用。"""
    try:
        decision = BaseAgent.parse_json(text)
    except JSONExtractionError as exc:
        return None, exc
    except ValueError as exc:      # 兜底：非 JSONExtractionError 的解析失败同样可反馈
        return None, JSONExtractionError(str(exc), raw_text=text or "", original=exc)
    if not isinstance(decision, dict):
        coerced = _coerce_decision(decision)
        if coerced is not None:
            return coerced, None
        return None, JSONExtractionError(
            "顶层必须是 JSON 对象（{...}），不能是数组或纯字符串",
            raw_text=text or "",
            cleaned_text=json.dumps(decision, ensure_ascii=False)[:2000],
        )
    return decision, None


def build_classify_batches(files: list[str], batch_size: int = CLASSIFY_BATCH_SIZE) -> list[list[str]]:
    """【需求点 1】把完整文件清单切分为多批次（每批 20~30 个文件）。

    · 批大小被钳制在 [CLASSIFY_BATCH_MIN, CLASSIFY_BATCH_MAX] 区间内；
    · 保持后端原生扫描给出的原始顺序，批次之间不重不漏（并集 = 全量清单）。
    """
    size = int(batch_size or CLASSIFY_BATCH_SIZE)
    size = max(CLASSIFY_BATCH_MIN, min(CLASSIFY_BATCH_MAX, size))
    ordered = [str(p) for p in (files or [])]
    return [ordered[i:i + size] for i in range(0, len(ordered), size)]


def _merge_classify_batches(batches: list[dict], scan: dict) -> dict:
    """【需求点 1】收集并合并全部批次的分类结果 → 完整文件作用清单（确定性合并）。

    合并规则（全部由后端完成，不再依赖任何模型调用）：
      · candidates / file_notes / unrecognized：按 path 去重合并（先到先得）；
      · 只保留确实存在于本批次清单中的路径（防止模型臆造文件）；
      · 批次失败的记录在 failed_batches 中，其文件进入 unrecognized，绝不静默丢弃。
    """
    files_all = [str(p) for p in (scan.get("files") or [])]
    file_set = set(files_all)
    candidates: list[dict] = []
    notes: list[dict] = []
    unrecognized: list[str] = []
    seen_candidates: set[str] = set()
    seen_notes: set[str] = set()
    seen_unknown: set[str] = set()
    failed_files: list[str] = []
    covered_files: list[str] = []
    ok_batches = 0

    for batch in batches or []:
        batch_files = [str(p) for p in (batch.get("files") or [])]
        per_batch = batch.get("result")
        if not batch.get("ok") or not isinstance(per_batch, dict):
            failed_files.extend(batch_files)
            continue
        ok_batches += 1
        covered_files.extend(batch_files)
        for item in (per_batch.get("candidates") or []):
            if not isinstance(item, dict):
                continue
            path = str(item.get("path") or "").strip()
            if not path or path not in file_set or path in seen_candidates:
                continue
            seen_candidates.add(path)
            candidates.append(item)
        for note in (per_batch.get("file_notes") or []):
            if not isinstance(note, dict):
                continue
            path = str(note.get("path") or "").strip()
            if not path or path not in file_set or path in seen_notes:
                continue
            seen_notes.add(path)
            notes.append(note)
        for name in (per_batch.get("unrecognized") or []):
            key = str(name).strip()
            if not key or key in seen_unknown:
                continue
            seen_unknown.add(key)
            unrecognized.append(key)

    # 批次失败 → 该批文件逐条登记为"未取得作用说明"，并在输出中显式说明
    for path in failed_files:
        if path not in seen_unknown:
            seen_unknown.add(path)
            unrecognized.append(path)

    # 批次成功但模型漏给 file_notes 的文件 → 标注"未取得说明"，保证清单完整覆盖
    missing_notes = [p for p in covered_files if p not in seen_notes]
    for path in missing_notes:
        if path not in seen_unknown:
            seen_unknown.add(path)
            unrecognized.append(path)

    stats = {"total": len(files_all), "candidates": len(candidates),
             "unrecognized": len(unrecognized), "by_category": {}}
    for item in candidates:
        cat = str(item.get("category") or "unknown").strip().lower() or "unknown"
        stats["by_category"][cat] = stats["by_category"].get(cat, 0) + 1

    return {
        "candidates": candidates,
        "file_notes": notes,
        "unrecognized": unrecognized,
        "stats": stats,
        "failed_files": failed_files,
        "ok_batches": ok_batches,
        "total_batches": len(batches or []),
        "missing_notes": missing_notes,
    }


def _compose_batched_summary(summary: str, merged: dict, batches: list[dict],
                             scan: dict) -> str:
    """把分批合并结果拼成可读摘要：候选清单 + 完整文件作用清单 + 失败批次透明说明。"""
    lines = [summary.strip()] if summary and summary.strip() else []
    lines.append("")
    lines.append(
        f"## 分批处理概览（共 {merged.get('total_batches', 0)} 批，"
        f"成功 {merged.get('ok_batches', 0)} 批）"
    )
    lines.append(
        f"- 扫描目录：`{scan.get('scan_path') or '.'}`｜递归："
        f"{'是' if scan.get('recursive') else '否'}"
        f"｜文件总数：{merged.get('stats', {}).get('total', 0)}"
        f"｜目录数：{scan.get('counts', {}).get('dirs', 0)}"
    )
    lines.append(
        f"- 批次大小上限：{CLASSIFY_BATCH_SIZE} 个文件/批（区间 "
        f"{CLASSIFY_BATCH_MIN}~{CLASSIFY_BATCH_MAX}）"
        f"｜每批独立输出 JSON，由后端收集合并"
    )
    for batch in batches or []:
        state = "成功" if batch.get("ok") else f"失败（{batch.get('error') or '汇总失败'}）"
        lines.append(
            f"  - 第 {batch.get('index')}/{batch.get('total')} 批"
            f"（{len(batch.get('files') or [])} 个文件）：{state}"
            f"｜文件作用 {batch.get('note_count', 0)} 条"
            f"｜候选 {batch.get('candidate_count', 0)} 条"
            f"｜轮次 {batch.get('rounds', 0)}"
        )

    candidates = merged.get("candidates") or []
    lines.append("")
    lines.append(f"## 待删除候选清单（共 {len(candidates)} 项）")
    if candidates:
        lines.append("| # | 文件 | 类别 | 置信度 | 建议删除 | 依据 |")
        lines.append("|---|------|------|--------|----------|------|")
        for i, c in enumerate(candidates, 1):
            lines.append(
                f"| {i} | `{c.get('path')}` | {c.get('category') or 'unknown'} | "
                f"{c.get('confidence') or 'medium'} | "
                f"{'是' if c.get('suggest_delete') else '否'} | {c.get('reason') or '-'} |"
            )
    else:
        lines.append("（未发现待删除的调试/测试文件）")

    notes = merged.get("file_notes") or []
    lines.append("")
    lines.append(f"## 完整文件作用清单（共 {len(notes)} 个文件已给出作用说明）")
    if notes:
        lines.append("| # | 文件 | 作用说明 |")
        lines.append("|---|------|----------|")
        for i, n in enumerate(notes, 1):
            purpose = str(n.get("purpose") or "-").replace("\n", " ").strip()
            lines.append(f"| {i} | `{n.get('path')}` | {purpose[:200]} |")
    else:
        lines.append("（本批次未产出可用的文件作用说明）")

    unrecognized = merged.get("unrecognized") or []
    if unrecognized:
        lines.append("")
        lines.append(f"## 未取得作用说明 / 无法识别的文件（{len(unrecognized)} 个）")
        lines.extend(f"- `{p}`" for p in unrecognized[:200])

    if merged.get("missing_notes"):
        lines.append("")
        lines.append(
            f"（提示：有 {len(merged['missing_notes'])} 个文件所在批次成功但模型未给出作用说明，"
            "已登记在上述清单中，未做任何删除动作）"
        )

    by_cat = (merged.get("stats") or {}).get("by_category") or {}
    if by_cat:
        lines.append("")
        lines.append("## 类别分布")
        lines.extend(f"- {k}: {v}" for k, v in sorted(by_cat.items()))
    return "\n".join(lines).strip()

# 【增量修复 2】分类模式下的轮次硬上限（清单已由后端原生扫描给出，正常 1 轮即收敛）
MAX_CLASSIFY_ROUNDS = 3


# ==========================================================================
# 【增量修复 1/2/3】模块级辅助：意图识别、原生清单分类、候选清单抽取、错误分类
# ==========================================================================
def _looks_like_classify_task(instruction: str) -> bool:
    """判断该指令是否为「遍历目录 + 判断文件类型」类任务。

    命中即启用「后端原生扫描 + 模型只做语义分类」模式：
    目录遍历不再交给 LLM，从根上消除"反复遍历无法收敛"。
    """
    text = str(instruction or "")
    if not text:
        return False
    return any(hint in text for hint in _CLASSIFY_INTENT_HINTS)


# 从指令中解析候选路径片段（不做位置锚定，避免中文语序导致漏匹配）
_PATH_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-][A-Za-z0-9_\-./\\]{0,200}")


def _infer_scan_path(instruction: str, guard) -> str:
    """从指令中推断要扫描的**已有**目录（默认工作区根目录 `.`）。

    策略：抽取指令中的路径样片段，逐个与工作区真实目录比对；
    命中即使用；全部未命中则回退 `.`（工作区根目录）。
    只做"证书式"匹配（必须真实存在），绝不当成模型一样去猜测路径。
    """
    text = str(instruction or "")
    if not text:
        return "."
    root = guard.workspace_root
    for token in _PATH_TOKEN_RE.findall(text):
        candidate = token.strip().strip(".").replace("\\", "/").strip("/")
        if not candidate or candidate in (".", ".."):
            continue
        try:
            if (root / candidate).is_dir():
                return candidate
        except OSError:
            continue
    return "."


def _io_error_code(reason: str) -> str:
    """把工作区不可用的原因映射为明确的 IO 错误码。"""
    if reason == WORKSPACE_UNAVAILABLE_NO_READ:
        return ERR_WORKSPACE_PERMISSION_DENIED
    if reason == WORKSPACE_UNAVAILABLE_NOT_DIR:
        return ERR_WORKSPACE_NOT_A_DIR
    if reason == WORKSPACE_UNAVAILABLE_NOT_FOUND:
        return ERR_WORKSPACE_PATH_NOT_FOUND
    return ERR_IO


def _coerce_decision(decision: Any) -> dict | None:
    """把"顶层不是对象"的模型输出，在可救的前提下兜底成合法结构。

    · 顶层是数组 → 视为候选清单数组（candidates）；
    · 其他情况 → 返回 None，由调用方判为 MODEL_CONVERGE_FAIL。
    """
    if isinstance(decision, list):
        items = [it for it in decision if isinstance(it, (dict, str))]
        if not items:
            return None
        candidates: list[dict] = []
        for it in items:
            if isinstance(it, str):
                candidates.append({"path": it, "category": "unknown",
                                   "confidence": "low", "reason": "模型仅给出文件名",
                                   "suggest_delete": False, "risk": "low"})
            elif it.get("path") or it.get("file") or it.get("name"):
                row = dict(it)
                row["path"] = str(row.get("path") or row.get("file") or row.get("name"))
                candidates.append(row)
        if not candidates:
            return None
        return {"done": True, "reasoning": "模型返回顶层数组，已按候选清单兜底解析",
                "summary": f"共解析出 {len(candidates)} 条候选（兜底解析）",
                "candidates": candidates, "unrecognized": []}
    if isinstance(decision, str):
        return None
    return None


def _looks_converged(decision: dict) -> bool:
    """判断模型输出是否已构成"可消费的最终结果"（容错收敛判定）。

    · done=true → 已收敛；
    · 明确给出 candidates 数组（哪怕是空数组）→ 视为已收敛；
    · 给出 summary / artifacts 且没有 tool_calls → 视为已收敛。
    """
    if decision.get("done"):
        return True
    if isinstance(decision.get("candidates"), list):
        return True
    if decision.get("summary") or decision.get("artifacts"):
        return True
    return False


def _extract_candidates(decision: dict) -> tuple[list[dict], list[str], dict]:
    """从模型输出中抽取结构化候选清单（严格 JSON 契约的消费点）。"""
    raw = decision.get("candidates")
    candidates: list[dict] = []
    if isinstance(raw, list):
        for item in raw[:500]:
            if isinstance(item, str):
                candidates.append({"path": item, "category": "unknown",
                                   "confidence": "low", "reason": "模型仅给出文件名",
                                   "suggest_delete": False, "risk": "low"})
                continue
            if not isinstance(item, dict):
                continue
            path = str(item.get("path") or item.get("file") or item.get("name") or "").strip()
            if not path:
                continue
            candidates.append({
                "path": path,
                "category": str(item.get("category") or "unknown").strip().lower() or "unknown",
                "confidence": str(item.get("confidence") or "medium").strip().lower() or "medium",
                "reason": str(item.get("reason") or "").strip()[:300],
                "suggest_delete": bool(item.get("suggest_delete", True)),
                "risk": str(item.get("risk") or "low").strip().lower() or "low",
            })
    raw_unknown = decision.get("unrecognized")
    unrecognized = [str(x).strip() for x in raw_unknown if str(x).strip()][:200] \
        if isinstance(raw_unknown, list) else []
    if not unrecognized:
        unrecognized = [c["path"] for c in candidates if c["category"] == "unknown"]

    stats = decision.get("stats") if isinstance(decision.get("stats"), dict) else {}
    stats = {
        "total": int(stats.get("total") or 0),
        "candidates": int(stats.get("candidates") or len(candidates)),
        "unrecognized": int(stats.get("unrecognized") or len(unrecognized)),
        "by_category": {},
    }
    for c in candidates:
        stats["by_category"][c["category"]] = stats["by_category"].get(c["category"], 0) + 1
    return candidates, unrecognized, stats


def _compose_candidate_summary(summary: str, candidates: list[dict],
                               unrecognized: list[str], stats: dict) -> str:
    """把结构化候选清单拼成可读摘要（保留模型 summary，追加确定性清单）。"""
    lines = [summary.strip()] if summary and summary.strip() else []
    lines.append("")
    lines.append(f"## 待删除候选清单（共 {len(candidates)} 项）")
    if candidates:
        lines.append("| # | 文件 | 类别 | 置信度 | 建议删除 | 依据 |")
        lines.append("|---|------|------|--------|----------|------|")
        for i, c in enumerate(candidates, 1):
            lines.append(
                f"| {i} | `{c['path']}` | {c['category']} | {c['confidence']} | "
                f"{'是' if c.get('suggest_delete') else '否'} | {c['reason'] or '-'} |"
            )
    else:
        lines.append("（未发现待删除的调试/测试文件）")
    if unrecognized:
        lines.append("")
        lines.append(f"## 无法识别类型的文件（{len(unrecognized)} 个，未纳入删除候选）")
        lines.extend(f"- `{p}`" for p in unrecognized[:50])
    if stats.get("by_category"):
        lines.append("")
        lines.append("## 类别分布")
        lines.extend(f"- {k}: {v}" for k, v in sorted(stats["by_category"].items()))
    return "\n".join(lines).strip()


@dataclass
class ToolExecutionRecord:
    tool: str
    args: dict
    ok: bool
    output: str
    high_risk: bool = False
    approved: bool | None = None
    approval_id: str = ""


@dataclass
class CodeTaskResult:
    output: str
    artifacts: list[str] = field(default_factory=list)
    tool_records: list[ToolExecutionRecord] = field(default_factory=list)
    need_approval: bool = False
    approval_message: Message | None = None
    denied_reason: str = ""
    success: bool = True
    fix_attempts: int = 0
    # 【BUG-C 1/5】失败错误码：success=False 时必须带错误码，
    #   由 handle() 返回 error 消息（而不是用 success 状态掩盖失败）。
    failure_code: str = ""
    # 【增量修复 3】失败分类：io_error（目录/权限）或 model_converge_fail（模型不收敛）
    failure_kind: str = ""
    # 【增量修复 2】结构化候选清单（模型只做语义判断的产物）
    candidates: list[dict] = field(default_factory=list)
    unrecognized: list[str] = field(default_factory=list)
    candidate_stats: dict = field(default_factory=dict)
    # 【需求点 1】每批次文件作用说明（path + purpose），分批合并为完整文件作用清单
    file_notes: list[dict] = field(default_factory=list)
    # 【需求点 1】本子任务分类覆盖的文件范围（分批模式=全量；单批模式=本批清单）
    classify_scope: list[str] = field(default_factory=list)
    # 【增量修复 1】后端原生扫描摘要（由后端 os.listdir 完成，未经模型）
    native_scan: dict = field(default_factory=dict)


@dataclass
class _ActiveRun:
    """审批挂起期间的执行上下文（用于审批通过后继续执行当前子任务）。"""

    messages: list[dict[str, Any]]
    result: CodeTaskResult
    pending_approval_id: str = ""
    resolved: bool = False
    # 【链路修复】同一轮模型输出里的"后续工具调用"（含高危）：
    #   历史缺陷 —— 分批分类路径遇到第一个高危动作就 return，同一轮里其余动作被丢弃，
    #   表现为"要求删除 N 个文件、实际只删了 1 个"。这里把剩余动作排队保存，
    #   审批通过后逐条继续执行（每条高危动作仍各自单独审批）。
    deferred_calls: list[dict] = field(default_factory=list)


class CodeAgent(BaseAgent):
    agent_role = AGENT_CODE
    model_name = "DeepSeek-Flash"   # 【需求点 BUG-NEW2】固定绑定 DeepSeek-Flash（deepseek-flash）

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = CODE_SYSTEM_PROMPT
        self._exec_times: list[float] = []
        # 会话内活跃执行态（用于"审批通过后继续执行当前子任务"，第3章 3.5 规则1）
        self._active: dict[str, _ActiveRun] = {}

    # ==================================================================
    # 主流程（首次执行）
    # ==================================================================
    async def execute_instruction(self, task: TaskState, instruction: str, *,
                                  image_resources: list[str] | None = None,
                                  canonical_block: str = "") -> CodeTaskResult:
        self.require_capability(CAPABILITY_FILE_WRITE, detail="代码工程Agent 是唯一执行端")
        result = CodeTaskResult(output="")

        # ==================================================================
        # 【增量修复 1】判断 / 筛选 / 清单类任务：
        #   ① 后端原生扫描目录（os.listdir）→ 拿到确定性文件名清单；
        #   ② 清单直接交给模型，模型只做"哪些是调试/测试文件"的语义判断；
        #   ③ 模型被要求单轮输出固定 JSON，不再反复遍历目录 / 反复思考。
        # ==================================================================
        classify_only = _looks_like_classify_task(instruction)
        native_scan_block = ""
        if classify_only:
            scan_path = _infer_scan_path(instruction, self.ctx.file_guard)
            scan = self._native_scan(task, scan_path=scan_path)
            if not scan.get("ok"):
                # ★ IO 类错误（目录不存在 / 权限不足）→ 直接终止子任务，不进入模型推理
                result.success = False
                result.failure_code = scan.get("error_code") or ERR_IO
                result.failure_kind = FAILURE_KIND_IO
                result.denied_reason = scan.get("message") or "原生目录扫描失败"
                result.output = (
                    f"IO 错误[{result.failure_code}]：{result.denied_reason}\n"
                    "（目录扫描由后端原生执行，未进入模型推理；后续文件类子任务不应继续）"
                )
                return result
            result.native_scan = {
                "scan_path": scan.get("scan_path"),
                "files": scan.get("counts", {}).get("files", 0),
                "dirs": scan.get("counts", {}).get("dirs", 0),
                "truncated": scan.get("truncated", False),
                "duration_ms": scan.get("duration_ms", 0),
            }
            # ==================================================================
            # 【需求点 1】文件分片分批处理：文件数超过 CLASSIFY_BATCH_SIZE（25）
            #   时，绝不再把全量清单一次性塞进模型上下文（154 文件单次负载
            #   极易导致模型输出非法 JSON → model_converge_fail）。
            #   改为：后端切片 → 每批独立调用模型输出 JSON → 收集合并全部批次。
            # ==================================================================
            scan_files = list(scan.get("files") or [])
            # 【需求点 1】记录本次分类覆盖的文件范围（用于 file_notes 的作用域校验）
            result.classify_scope = list(scan_files)
            if len(scan_files) > CLASSIFY_BATCH_SIZE:
                return await self._run_batched_classify(
                    task, result, instruction, scan, scan_files,
                    canonical_block=canonical_block)
            native_scan_block = (
                "【文件清单（后端原生扫描结果 · os.listdir，未经模型）】\n"
                + self.ctx.file_guard.format_scan_payload(scan)
                + "\n\n"
                + CODE_CLASSIFY_HINT
            )

        # 会话工作区清单，作为上下文喂给模型（避免其臆造文件）
        # 【需求点 Bug4】原实现用 `.` / `/` 前缀区分文件与目录（如 `.note.txt`、`/.git`），
        #   模型会把前缀误当成文件名的一部分，进而生成带前置点的错误路径。
        #   现改为显式类型标注 `[文件]` / `[目录]`，文件名**原样输出**，绝不做字符串改写。
        listing_text = self.ctx.file_guard.format_listing(limit=120)

        image_note = ""
        if image_resources:
            # 第3章 3.3：图片仅视觉感知Agent可解析；代码Agent只能获得视觉Agent的结构化描述
            image_note = "【关联图片资源路径，仅供引用，禁止自行解析图像】\n" + "\n".join(
                f"- {p}" for p in image_resources[:5]
            )

        sys_prompt = self.system_prompt
        if self.ctx.runtime.degraded_notes:
            sys_prompt += "\n\n【系统当前降级状态】" + "；".join(self.ctx.runtime.degraded_notes)

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": "\n\n".join(x for x in [
                f"【任务指令】\n{instruction}",
                # 【队长-队员架构 · 规则4】队长唯一基准清单：独立分片放在最前，
                #   强制队员沿用（与系统提示词、文件清单物理分隔，避免混淆）
                canonical_block,
                f"【当前工作区文件清单（路径原样使用，禁止添加任何前缀）】\n{listing_text}",
                native_scan_block,
                image_note,
            ] if x)},
        ]
        return await self._run_code_loop(
            task, result, messages, classify_only=classify_only)

    # ==================================================================
    # 【需求点 1】文件分片分批处理【分批语义分类模式】
    #
    #   BUG 根因：一次性把全量清单（154 个文件）塞给 DeepSeek-Flash 做语义分类，
    #   单次任务负载过高 → 模型输出 JSON 语法持续出错 → 子任务 1 判
    #   model_converge_fail → 触发链路阻断 → 后续子任务全部不再分发。
    #
    #   修复：后端把清单切成每批 <= CLASSIFY_BATCH_SIZE（25）个文件的多个批次，
    #   每一批**独立**调用代码工程Agent 输出合法 JSON，后端收集合并为完整清单。
    #   · 迭代计数按"批次"增长（一个批次 = 一次子任务迭代），
    #     因此 154 文件（7 批）不会撞 MAX_TASK_ITERATIONS=20 的防死循环硬限制；
    #   · 单批内部最多 MAX_CLASSIFY_BATCH_ROUNDS 轮：第 1 轮 + 带"上一轮 JSON
    #     语法错误反馈"的修正重试（需求点 3）；
    #   · 某批最终失败 → 只把该批文件登记为"未取得作用说明"，其余批次照常合并，
    #     不再让单个批次的格式问题阻断整条任务链路。
    # ==================================================================
    async def _run_batched_classify(
        self, task: TaskState, result: CodeTaskResult, instruction: str,
        scan: dict, scan_files: list[str], *, canonical_block: str = "",
    ) -> CodeTaskResult:
        batches_files = build_classify_batches(scan_files, CLASSIFY_BATCH_SIZE)
        total_batches = len(batches_files)
        file_set = set(str(p) for p in scan_files)

        self.think(
            task, "read",
            f"文件清单 {len(scan_files)} 个，超过单次负载上限 {CLASSIFY_BATCH_SIZE} 个 → "
            f"启用【分批处理】：拆分为 {total_batches} 批（每批 ≤ {CLASSIFY_BATCH_SIZE} 个文件），"
            "逐批独立调用模型输出 JSON，后端收集合并为完整清单",
        )
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="code.classify_batched",
            detail=(f"文件分批：files={len(scan_files)} batch_size={CLASSIFY_BATCH_SIZE} "
                    f"batches={total_batches} mode=batch_classify"),
        )
        try:
            self.ctx.runtime.stream_emit(
                task.task_id, "agent_step", session_id=task.session_id,
                agent_role=self.agent_role,
                text=(f"文件分批处理：{len(scan_files)} 个文件 → {total_batches} 批"
                      f"（每批 ≤ {CLASSIFY_BATCH_SIZE}）"),
                step_type="read",
            )
        except Exception:  # noqa: BLE001 流式推送失败不影响主链路
            pass

        batches: list[dict] = []
        record: dict = {}
        for index, batch_files in enumerate(batches_files, 1):
            batch_label = f"第 {index}/{total_batches} 批"
            # 迭代计数：一个批次 = 一次迭代（分批不放大迭代消耗，防死循环硬限制不变）
            task.bump_iteration()
            self.ctx.runtime.router.check_timeout(task)

            self.think(
                task, "think",
                f"{batch_label}：本批 {len(batch_files)} 个文件，独立送入 "
                f"{self.model_name} 做语义分类（单批单轮收敛）",
            )
            record = {"index": index, "total": total_batches, "files": batch_files,
                      "ok": False, "error": "", "rounds": 0, "result": None,
                      "note_count": 0, "candidate_count": 0, "tool_calls": []}

            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": self._build_batch_user_content(
                    instruction, batch_files, scan, index, total_batches,
                    canonical_block=canonical_block)},
            ]

            for round_index in range(MAX_CLASSIFY_BATCH_ROUNDS):
                record["rounds"] = round_index + 1
                try:
                    response = await self.call_model(
                        task, messages,
                        # 分类任务温度取区间下沿（0.1~0.2 内）：语义判断要稳定、少发挥
                        temperature=max(0.1, AGENT_TEMPERATURES[AGENT_CODE] - 0.05),
                        max_tokens=6000, expect_json=True,
                        step_label=(f"{self.model_name} {batch_label} 语义分类"
                                    f"（第 {round_index + 1}/{MAX_CLASSIFY_BATCH_ROUNDS} 轮）"),
                    )
                except ModelUnavailable as exc:
                    # 模型不可用：不阻断整条链路，只把本批登记为未完成
                    record["error"] = f"模型不可用：{exc}"
                    self.think(
                        task, "think",
                        f"{batch_label} 模型不可用，已跳过本批（其余批次继续）：{exc}",
                        level=THINK_LEVEL_WARN,
                    )
                    break

                decision, json_error = _parse_batch_json(response.text)
                if decision is not None:
                    candidates, unrecognized, _stats = _extract_candidates(decision)
                    batch_set = set(batch_files)
                    # 防御：模型臆造 / 串批路径 → 只保留确实属于本批清单的文件
                    record["result"] = {
                        "candidates": [c for c in candidates
                                       if c["path"] in file_set and c["path"] in batch_set],
                        "file_notes": self._extract_file_notes(decision, batch_files),
                        "unrecognized": [p for p in unrecognized
                                         if p in file_set and p in batch_set],
                    }
                    record["note_count"] = len(record["result"]["file_notes"])
                    record["candidate_count"] = len(record["result"]["candidates"])
                    record["ok"] = True
                    record["error"] = ""
                    # 【链路修复】分类模式下模型若给出写 / 删 / 命令等非只读动作，
                    #   一律留待下方统一执行链路做风险判定（高危必进人工审批），
                    #   绝不因为在分批分类里就静默丢弃。
                    record["tool_calls"] = _classify_leftover_tool_calls(
                        decision.get("tool_calls") or [])
                    self.think(
                        task, "think",
                        f"{batch_label} 已收敛：文件作用 {record['note_count']} 条、"
                        f"候选 {record['candidate_count']} 条（第 {record['rounds']} 轮）",
                    )
                    break

                # ---------------- 非法 JSON：完整落日志 + 错误反馈进入下一轮 prompt ----------------
                raw_text = response.text or ""
                self.ctx.logger.exception_log(
                    error_code=ERR_MODEL_CONVERGE_FAIL,
                    message=(f"[分批分类] {batch_label} 模型输出非法 JSON："
                             f"{(json_error.detail if json_error else '解析失败')}｜"
                             f"原始返回长度={len(raw_text)} 字符"),
                    session_id=task.session_id, task_id=task.task_id,
                    agent_role=self.agent_role,
                    # 【需求点 2③】JSONDecodeError 场景：完整记录模型原始返回文本，便于调试
                    stack=(f"round={record['rounds']}/{MAX_CLASSIFY_BATCH_ROUNDS} "
                           f"batch={index}/{total_batches} files={len(batch_files)}\n"
                           f"--- 模型原始返回（完整文本） ---\n{raw_text}"),
                )
                record["error"] = f"第 {record['rounds']} 轮 JSON 非法"
                self.think(
                    task, "think",
                    f"{batch_label} 第 {record['rounds']}/{MAX_CLASSIFY_BATCH_ROUNDS} 轮输出非法 JSON，"
                    "已把语法错误作为反馈回灌，要求其修正格式后重试",
                    level=THINK_LEVEL_WARN,
                )
                messages.append({"role": "assistant", "content": raw_text[:4000]})
                messages.append({
                    "role": "user",
                    "content": _build_json_error_feedback(
                        json_error or ValueError("模型输出无法解析为 JSON"),
                        round_no=record["rounds"], max_rounds=MAX_CLASSIFY_BATCH_ROUNDS,
                        batch_label=batch_label),
                })

            if not record["ok"]:
                self.think(
                    task, "think",
                    f"{batch_label} 在 {record['rounds']} 轮内未取得合法 JSON，"
                    f"本批 {len(batch_files)} 个文件登记为「未取得作用说明」，"
                    "其余批次继续合并（不阻断整条任务链路）",
                    level=THINK_LEVEL_WARN,
                )
                self.ctx.db.log_agent(
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                    event="code.classify_batch_failed", level="warn",
                    detail=(f"batch={index}/{total_batches} files={len(batch_files)} "
                            f"rounds={record['rounds']} error={record['error']}"),
                )
            batches.append(record)

        # ==================================================================
        # 【链路修复】分批分类期间模型给出的非只读动作（写 / 删 / 命令 / 下载）：
        #   统一交回 _run_tool 做风险判定 —— 高危 → 任务暂停 waiting_approval
        #   等待人工审批；审批拒绝 → 终止子任务。绝不静默丢弃。
        # ==================================================================
        leftover_calls = [c for b in batches for c in (b.get("tool_calls") or [])]
        if leftover_calls:
            self.think(
                task, "exec",
                f"分批分类期间模型给出 {len(leftover_calls)} 个非只读工具调用，"
                "交由统一执行链路做风险判定（高危一律进入人工审批）",
                level=THINK_LEVEL_WARN,
            )
            for call_index, call in enumerate(leftover_calls[:6]):
                tool = str((call or {}).get("tool") or "")
                args = (call or {}).get("args") or {}
                if not isinstance(args, dict):
                    args = {}
                outcome = await self._run_tool(task, tool, args)
                result.tool_records.append(outcome)
                if outcome.approved is False:
                    result.success = False
                    result.denied_reason = f"高危操作被人工审批拒绝：{tool}"
                    result.output = (
                        f"已按人工审批结果终止该子任务。被拒绝的操作：{tool} "
                        f"({json.dumps(_redact_args(args), ensure_ascii=False)[:300]})。"
                        "未执行任何写入或命令。"
                    )
                    return result
                if outcome.approved is None and outcome.high_risk:
                    # 第2章 2.2 规则3：高危 → 子任务暂停在 waiting_approval
                    result.need_approval = True
                    result.approval_message = getattr(outcome, "_approval_msg", None)
                    result.success = False
                    result.output = (
                        f"检测到高危操作「{tool}」，已暂停任务并推送人工审批。"
                        "审批通过后将继续执行，拒绝则终止该子任务。"
                    )
                    self.ctx.db.log_agent(
                        session_id=task.session_id, task_id=task.task_id,
                        agent_role=self.agent_role, event="code.approval_requested",
                        detail=(f"分批分类期间的高危动作进入审批：tool={tool} "
                                f"approval_id={outcome.approval_id} "
                                f"同一轮剩余动作={len(leftover_calls) - call_index - 1}"),
                    )
                    # 与单批路径一致：保存执行上下文，供审批通过后继续当前子任务
                    #   （避免审批通过却报"执行上下文已失效"）；
                    #   同时把同一轮剩余动作排队，审批后逐条继续执行。
                    resumed = _ActiveRun(
                        messages=messages,
                        result=result,
                        pending_approval_id=outcome.approval_id,
                        deferred_calls=[
                            {"tool": str((c or {}).get("tool") or ""),
                             "args": (c or {}).get("args") if isinstance((c or {}).get("args"), dict) else {}}
                            for c in leftover_calls[call_index + 1:]
                        ],
                    )
                    self._active[self._active_key(task.task_id, outcome.approval_id)] = resumed
                    return result

        # ================= 收集合并全部批次 → 完整文件作用清单 =================
        merged = _merge_classify_batches(batches, scan)
        failed_batches = [b for b in batches if not b.get("ok")]
        headline = (
            f"已按 {CLASSIFY_BATCH_SIZE} 个文件/批拆分为 {total_batches} 批分批处理，"
            f"成功 {merged['ok_batches']}/{total_batches} 批；"
            f"合并得到文件作用说明 {len(merged['file_notes'])} 条、"
            f"待删除候选 {len(merged['candidates'])} 条、"
            f"未取得说明/无法识别 {len(merged['unrecognized'])} 个。"
        )
        if failed_batches:
            headline += (f"其中 {len(failed_batches)} 个批次（"
                         + "、".join(str(b["index"]) for b in failed_batches)
                         + "）未取得合法 JSON，对应文件已逐条登记，未执行任何删除操作。")
        result.output = _compose_batched_summary(headline, merged, batches, scan)
        result.success = True
        result.need_approval = False
        result.artifacts = []
        result.candidates = merged["candidates"]
        result.unrecognized = merged["unrecognized"]
        result.candidate_stats = merged["stats"]
        result.file_notes = merged["file_notes"]
        result.native_scan = {
            **result.native_scan,
            "batched": True,
            "batch_size": CLASSIFY_BATCH_SIZE,
            "batches": total_batches,
            "batches_ok": merged["ok_batches"],
            "batches_failed": [b["index"] for b in failed_batches],
            "file_notes": len(merged["file_notes"]),
        }

        event = "code.converged" if not failed_batches else "code.converge_partial"
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event=event, level=("info" if not failed_batches else "warn"),
            detail=(f"分批分类完成：batches={total_batches} ok={merged['ok_batches']} "
                    f"failed={[b['index'] for b in failed_batches]} "
                    f"file_notes={len(merged['file_notes'])} "
                    f"candidates={len(merged['candidates'])} "
                    f"unrecognized={len(merged['unrecognized'])}"),
        )
        self.think(
            task, "think",
            f"分批处理完成并已合并：{total_batches} 批｜文件作用 {len(merged['file_notes'])} 条｜"
            f"候选 {len(merged['candidates'])} 条｜未取得说明 {len(merged['unrecognized'])} 个",
        )
        return result

    # ==================================================================
    # 【需求点 1】单批次 user 消息：任务指令 + 本批清单 + 分批专用提示词
    # ==================================================================
    def _build_batch_user_content(self, instruction: str, batch_files: list[str],
                                  scan: dict, index: int, total: int, *,
                                  canonical_block: str = "") -> str:
        lines = [
            # 【队长-队员架构 · 规则4】队长唯一基准清单（独立分片，强制沿用）
            canonical_block,
            f"【任务指令】\n{instruction}",
            (
                f"【本批次信息】第 {index}/{total} 批｜本批文件 {len(batch_files)} 个"
                f"｜全量文件 {scan.get('counts', {}).get('files', 0)} 个"
                f"｜扫描目录：{scan.get('scan_path') or '.'}"
                f"（清单由后端原生 os.listdir 扫描，未经模型）"
            ),
            (
                "【本批文件清单（路径原样使用，禁止添加任何前缀；只处理这些文件）】\n"
                + "\n".join(f"{i + 1}. {p}" for i, p in enumerate(batch_files))
            ),
            (
                "【统一交付要求】\n"
                "本次任务需为**每一个文件**给出一句话作用说明（file_notes: path + purpose），"
                "并筛出其中属于调试 / 测试 / 临时 / 构建产物 / 日志的待删除候选（candidates）。"
                "后端会把你本批的输出与其他批次的输出合并成完整清单，"
                "因此**只需且必须**覆盖本批文件。"
            ),
            CODE_CLASSIFY_BATCH_HINT,
        ]
        return "\n\n".join(x for x in lines if x)

    # ==================================================================
    # 【需求点 1】抽取 file_notes（path + purpose），只保留属于本批清单的条目
    # ==================================================================
    @staticmethod
    def _extract_file_notes(decision: dict, batch_files: list[str]) -> list[dict]:
        """从模型输出中抽取"每个文件的作用说明"，并做范围与去重约束。

        · 只接受本批清单内、逐字一致的路径（防臆造 / 防串批）；
        · 同一路径只保留第一条；purpose 为空时保留占位说明，保证清单完整覆盖。
        """
        raw = decision.get("file_notes")
        allowed = {str(p) for p in batch_files}
        notes: list[dict] = []
        seen: set[str] = set()
        if not isinstance(raw, list):
            return notes
        for item in raw:
            if isinstance(item, str):
                path, purpose = item.strip(), ""
            elif isinstance(item, dict):
                path = str(item.get("path") or item.get("file") or item.get("name") or "").strip()
                purpose = str(item.get("purpose") or item.get("desc")
                              or item.get("description") or "").strip()
            else:
                continue
            if not path or path not in allowed or path in seen:
                continue
            seen.add(path)
            notes.append({"path": path, "purpose": purpose[:300] or "（模型未给出作用说明）"})
        return notes

    # ==================================================================
    # 【增量修复 1】后端原生目录扫描（不经过任何模型）
    #   `列出文件夹内文件名` 这类确定性动作由后端 os.listdir 完成；
    #   模型只负责"判断哪些文件属于调试/测试文件"这一语义任务。
    #   IO 类失败（目录不存在 / 权限不足）在此直接归类并返回，不进入模型推理。
    # ==================================================================
    def _native_scan(self, task: TaskState, *, scan_path: str = ".") -> dict:
        """执行后端原生扫描并写思考链 + 日志（返回结构化结果，失败时 ok=False）。"""
        started = time.time()
        try:
            scan = self.ctx.file_guard.scan_workspace(scan_path)
        except WorkspaceUnavailable as exc:
            # ★ IO 类错误：文件夹不存在 / 权限不足 —— 与模型无关，单独记录
            code = _io_error_code(exc.reason)
            self.think_error(
                task,
                f"IO 错误[{code}]：{exc}"
                f"（原生目录扫描由后端执行，未进入模型推理）",
            )
            self.ctx.logger.exception_log(
                error_code=code,
                message=(f"原生目录扫描失败（IO 类错误）：{exc} | "
                         f"路径={exc.path} 原因={exc.reason} "
                         f"detail={json.dumps(exc.detail, ensure_ascii=False)}"),
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return {"ok": False, "error_code": code, "message": str(exc),
                    "path": exc.path, "reason": exc.reason,
                    "kind": FAILURE_KIND_IO}
        except SecurityViolation as exc:
            self.think_error(task, f"IO 错误[{exc.code}]：{exc}")
            self.ctx.logger.exception_log(
                error_code=exc.code, message=f"原生目录扫描被安全模块拦截：{exc}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return {"ok": False, "error_code": exc.code, "message": str(exc),
                    "path": "", "reason": exc.code, "kind": FAILURE_KIND_IO}

        counts = scan["counts"]
        self.think(
            task, "read",
            f"后端原生扫描完成（os.listdir，未经模型）：目录={scan['scan_path']}｜"
            f"文件 {counts['files']} 个｜目录 {counts['dirs']} 个｜"
            f"遍历目录 {counts['dirs_scanned']} 个｜耗时 {scan['duration_ms']}ms"
            + ("｜清单已截断" if scan["truncated"] else ""),
        )
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="code.native_scan",
            detail=(f"后端原生目录扫描：path={scan['scan_path']} recursive={scan['recursive']} "
                    f"files={counts['files']} dirs={counts['dirs']} "
                    f"dirs_scanned={counts['dirs_scanned']} truncated={scan['truncated']} "
                    f"duration_ms={scan['duration_ms']}"),
        )
        # 扫描摘要推送流式通道（前端思维链可见"后端原生扫描"这一步）
        try:
            self.ctx.runtime.stream_emit(
                task.task_id, "agent_step", session_id=task.session_id,
                agent_role=self.agent_role,
                text=(f"后端原生扫描目录 {scan['scan_path']}："
                      f"文件 {counts['files']} 个 / 目录 {counts['dirs']} 个"
                      f"（os.listdir，未经模型推理，耗时 {scan['duration_ms']}ms）"),
                step_type="read",
            )
        except Exception:  # noqa: BLE001 流式推送失败绝不影响主链路
            pass
        scan["duration_total_ms"] = int((time.time() - started) * 1000)
        return scan

    # ==================================================================
    # 审批通过后继续执行（第3章 3.5 规则1：审批通过 -> 继续执行当前子任务）
    # ==================================================================
    async def resume_after_approval(
        self, task: TaskState, approval_id: str, execution, *, approved: bool
    ) -> CodeTaskResult:
        """审批结果回来后：通过则裸机执行该动作并继续推理；拒绝则终止子任务。"""
        self.require_capability(CAPABILITY_FILE_WRITE)

        active = self._active.get(self._active_key(task.task_id, approval_id))
        if active is None:
            return CodeTaskResult(
                output="审批对应的执行上下文已失效（服务重启或已处理），该子任务终止。",
                success=False, denied_reason="EXECUTION_CONTEXT_EXPIRED",
            )

        result = active.result

        if not approved:
            # 第3章 3.5 规则2：审批拒绝 -> 终止当前子任务
            active.resolved = True
            self._active.pop(self._active_key(task.task_id, approval_id), None)
            result.success = False
            result.denied_reason = "APPROVAL_REJECTED_BY_USER"
            result.output = (
                f"人工审批已拒绝高危操作「{execution.tool}」，按规则终止当前子任务，"
                "未执行任何文件写入或系统命令。父任务可由调度Agent重新规划或结束。"
            )
            return result

        # 审批通过 → 裸机执行该动作（工作区内的实际动作由 execute_tool 完成）
        self.ctx.approval_center.mark_executed(approval_id)
        task.transition("running", reason="审批通过，继续执行当前子任务")
        outcome = await self._run_tool(task, execution.tool, execution.args, approved=True)
        result.tool_records.append(outcome)
        active.pending_approval_id = ""

        # ==================================================================
        # 【链路修复】同一轮模型输出里的**后续动作**（在第一个高危动作处被挂起）：
        #   审批通过后按顺序逐条继续执行；其中高危动作仍各自单独进入人工审批，
        #   绝不因为"前一个已获批"而放行后面的动作。
        # ==================================================================
        while active.deferred_calls:
            nxt = active.deferred_calls.pop(0)
            next_tool = str(nxt.get("tool") or "")
            next_args = nxt.get("args") if isinstance(nxt.get("args"), dict) else {}
            if not next_tool:
                continue
            next_outcome = await self._run_tool(task, next_tool, next_args)
            result.tool_records.append(next_outcome)
            if next_outcome.approved is False:
                result.success = False
                result.denied_reason = f"高危操作被人工审批拒绝：{next_tool}"
                result.output = (
                    f"已按人工审批结果终止该子任务。被拒绝的操作：{next_tool} "
                    f"({json.dumps(_redact_args(next_args), ensure_ascii=False)[:300]})。"
                    "未执行任何写入或命令。"
                )
                self._active.pop(self._active_key(task.task_id, approval_id), None)
                return result
            if next_outcome.approved is None and next_outcome.high_risk:
                # 后续动作同样是高危 → 再次暂停并单独推送审批（剩余动作继续排队）
                result.need_approval = True
                result.approval_message = getattr(next_outcome, "_approval_msg", None)
                result.success = False
                result.output = (
                    f"检测到后续高危操作「{next_tool}」，已暂停任务并推送人工审批"
                    f"（同一轮剩余动作 {len(active.deferred_calls)} 个）。"
                    "审批通过后将继续执行，拒绝则终止该子任务。"
                )
                active.pending_approval_id = next_outcome.approval_id
                self._active[self._active_key(task.task_id, next_outcome.approval_id)] = active
                self._active.pop(self._active_key(task.task_id, approval_id), None)
                self.ctx.db.log_agent(
                    session_id=task.session_id, task_id=task.task_id,
                    agent_role=self.agent_role, event="code.approval_requested",
                    detail=(f"后续高危动作再次进入审批：tool={next_tool} "
                            f"approval_id={next_outcome.approval_id} "
                            f"剩余={len(active.deferred_calls)}"),
                )
                return result

        if not outcome.ok:
            # 执行失败：把失败信息回灌给模型，允许其在重试上限内自我修复
            active.messages.append({"role": "user", "content":
                f"【已获人工审批但执行失败】「{execution.tool}」\n{outcome.output[:2500]}\n"
                "请分析失败原因并给出修正后的 tool_calls（或 done=true 说明情况）。"})
        else:
            active.messages.append({"role": "user", "content":
                f"【人工审批已通过并执行成功】「{execution.tool}」\n{outcome.output[:2500]}\n"
                "请继续：若任务已完成输出 done=true，否则给出下一轮 tool_calls。"})

        return await self._run_code_loop(task, result, active.messages, active=active)

    @staticmethod
    def _active_key(task_id: str, approval_id: str) -> str:
        return f"{task_id}:{approval_id}"

    # ==================================================================
    # 代码推理循环（首次执行与审批恢复共用）
    #
    # 【增量修复 2/3】收敛策略与失败分类：
    #   · classify_only=True（筛选 / 清单类任务）：文件名清单已由后端原生扫描给出，
    #     模型只做一次语义判断。**首轮即给出可解析结果就直接收敛**，
    #     不再进入多轮工具循环；仍不收敛则按 MODEL_CONVERGE_FAIL 明确失败。
    #   · 轮次耗尽统一记 MODEL_CONVERGE_FAIL（模型推理无法收敛 / 输出无法解析），
    #     与 IO 类错误（目录 / 权限）在日志中区分开。
    # ==================================================================
    async def _run_code_loop(self, task: TaskState, result: CodeTaskResult,
                             messages: list[dict[str, Any]],
                             active: "_ActiveRun | None" = None,
                             classify_only: bool = False) -> CodeTaskResult:
        # classify_only 模式：文件清单已由后端原生扫描给出，模型只需一次语义判断，
        #   因此把轮次上限收紧到 MAX_CLASSIFY_ROUNDS（3 轮），
        #   彻底消除"再确认一遍"式的无进展循环；代码执行模式仍用原上限。
        max_rounds = MAX_CLASSIFY_ROUNDS if classify_only else (MAX_CODE_FIX_RETRIES + 1)
        invalid_json_rounds = 0

        for round_index in range(max_rounds):
            task.bump_iteration()                       # 第2章 2.1 规则1
            self.ctx.runtime.router.check_timeout(task)  # 第2章 2.1 规则2

            try:
                response = await self.call_model(
                    # 【需求点 Bug4】编码子 Agent 温度 0.15（区间 0.1~0.2）：代码生成与工具调用必须严谨，不上调
                    task, messages, temperature=AGENT_TEMPERATURES[AGENT_CODE],
                    max_tokens=6000, expect_json=True,
                    step_label=f"DeepSeek-Flash 第 {round_index + 1} 轮代码推理"
                               + ("（原生清单 → 语义分类，单轮收敛）" if classify_only else ""),
                )
            except ModelUnavailable as exc:
                result.success = False
                result.denied_reason = str(exc)
                result.failure_code = getattr(exc, "code", ERR_MODEL_UNAVAILABLE)
                result.failure_kind = FAILURE_KIND_MODEL_CONVERGE
                result.output = f"代码工程能力当前不可用：{exc}"
                # 【BUG-C 5】以 error 级别写入思考链，禁止用 success 掩盖失败
                self.think_error(task, f"代码任务失败：{exc}")
                return result

            try:
                decision = self.parse_json(response.text)
            except ValueError as exc:
                invalid_json_rounds += 1
                # 【增量修复 3】输出无法解析 → 归为 MODEL_CONVERGE_FAIL（不是 IO 错误）
                self.ctx.logger.exception_log(
                    error_code=ERR_MODEL_CONVERGE_FAIL, message=str(exc),
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                    stack=f"round={round_index + 1}/{max_rounds} "
                          f"mode={'classify' if classify_only else 'code'} "
                          f"raw_head={(response.text or '')[:500]}",
                )
                result.fix_attempts = round_index + 1
                result.failure_kind = FAILURE_KIND_MODEL_CONVERGE
                if invalid_json_rounds == 1:
                    self.think(
                        task, "think",
                        f"模型输出无法解析为 JSON（第 {round_index + 1}/{max_rounds} 轮），"
                        "要求其改为纯 JSON 输出",
                        level=THINK_LEVEL_WARN,
                    )
                messages.append({"role": "assistant", "content": (response.text or "")[:4000]})
                messages.append({"role": "user", "content": (
                    "你的上一条回复不是合法 JSON。请严格按约定格式重新输出**纯 JSON**"
                    "（第一个字符就是 {，最后一个字符就是 }），不要任何解释文字、"
                    "不要 Markdown 代码围栏。无法判断的文件请标记 "
                    "\"category\":\"unknown\" 并写入 unrecognized 数组，不要继续思考。"
                )})
                continue

            if not isinstance(decision, dict):
                # 顶层是数组 / 字符串 → 尝试按"候选清单"语义兜底，仍不可用则判收敛失败
                coerced = _coerce_decision(decision)
                if coerced is None:
                    invalid_json_rounds += 1
                    result.failure_kind = FAILURE_KIND_MODEL_CONVERGE
                    messages.append({"role": "assistant", "content": json.dumps(
                        decision, ensure_ascii=False)[:2000]})
                    messages.append({"role": "user", "content":
                        "顶层必须是 JSON 对象（{...}），不能是数组或纯文本。请按约定格式重新输出。"})
                    continue
                decision = coerced

            tool_calls = decision.get("tool_calls") or []
            messages.append({"role": "assistant", "content": json.dumps(decision, ensure_ascii=False)[:8000]})

            # ==========================================================
            # 【增量修复 1/2】原生清单分类模式：模型只判断、不遍历。
            #   · 模型若仍试图 list_dir / read_file 等**只读**动作 → 直接以原生清单回绝；
            #   · 【链路修复】但**高危动作（删除/命令/下载/写入）绝不允许被静默丢弃**：
            #     历史缺陷 —— 列表类指令（含"删除"等关键词）会切到分类模式，
            #     模型给出的 delete_file 被无条件忽略 → 既不执行也不进审批，
            #     高危操作凭空消失。现改为：高危工具一律交回正常链路，
            #     由权威的 assess_operation → 审批中心决定（审批门槛不降低，只是不再绕开）。
            #   · 首轮给出的可解析结果（done / candidates）立即收敛。
            # ==========================================================
            if classify_only and tool_calls:
                read_only_calls = [
                    (c or {}) for c in tool_calls
                    if str((c or {}).get("tool") or "") in _CLASSIFY_READONLY_TOOLS
                ]
                if read_only_calls:
                    rejected = [str(c.get("tool") or "") for c in read_only_calls]
                    self.think(
                        task, "exec",
                        f"原生扫描已完成，忽略模型重复遍历请求：{rejected}（清单已在上下文中，"
                        "不需要重新列出目录 / 重复读取文件）",
                        level=THINK_LEVEL_WARN,
                    )
                    messages.append({"role": "user", "content":
                        "【系统拦截】文件清单已由后端原生扫描给出（见上文【文件清单（后端原生扫描结果）】），"
                        "**禁止再次调用 list_dir 遍历目录 / 重复读取文件**。请立即基于该清单输出最终 JSON："
                        "done=true + file_notes + candidates 数组。"})
                    tool_calls = [c for c in tool_calls if c not in read_only_calls]
                    if not tool_calls:
                        continue
                # 剩余（高危 / 写类）工具调用放行到下方统一执行链路 → 高危必进审批
                self.think(
                    task, "exec",
                    "分类模式检测到非只读工具调用（含高危动作），交由统一执行链路做风险判定，"
                    "高危一律走人工审批，不静默忽略",
                    level=THINK_LEVEL_WARN,
                )
                decision = {**decision, "tool_calls": tool_calls}

            if tool_calls:
                feedback_lines: list[str] = []
                for call_index, call in enumerate(tool_calls[:6]):
                    tool = str((call or {}).get("tool") or "")
                    args = (call or {}).get("args") or {}
                    if not isinstance(args, dict):
                        args = {}

                    outcome = await self._run_tool(task, tool, args)
                    result.tool_records.append(outcome)

                    if outcome.approved is False:
                        # 第3章 3.5 规则2：审批拒绝 → 终止当前子任务
                        result.success = False
                        result.denied_reason = f"高危操作被人工审批拒绝：{tool}"
                        result.output = (
                            f"已按人工审批结果终止该子任务。被拒绝的操作：{tool} "
                            f"({json.dumps(_redact_args(args), ensure_ascii=False)[:300]})。未执行任何写入或命令。"
                        )
                        return result

                    if outcome.approved is None and outcome.high_risk:
                        # 第2章 2.2 规则3：高危 → 任务暂停在 waiting_approval，等待人工审批
                        result.need_approval = True
                        result.approval_message = getattr(outcome, "_approval_msg", None)
                        result.success = False
                        result.output = (
                            f"检测到高危操作「{tool}」，已暂停任务并推送人工审批。"
                            f"审批通过后将继续执行，拒绝则终止该子任务。"
                        )
                        # 保存执行上下文，供审批通过后恢复
                        # 【链路修复】同一轮里**后续工具调用**排队保存：审批通过后逐条继续执行，
                        #   避免"要求删除 N 个文件、实际只删了 1 个"（后续动作被静默丢弃）。
                        run = active or _ActiveRun(messages=messages, result=result)
                        run.messages = messages
                        run.result = result
                        run.pending_approval_id = outcome.approval_id
                        run.deferred_calls = [
                            {"tool": str((c or {}).get("tool") or ""),
                             "args": (c or {}).get("args") if isinstance((c or {}).get("args"), dict) else {}}
                            for c in tool_calls[call_index + 1:]
                        ]
                        self._active[self._active_key(task.task_id, outcome.approval_id)] = run
                        return result

                    feedback_lines.append(
                        f"「{tool}」执行{'成功' if outcome.ok else '失败'}：\n{outcome.output[:2500]}"
                    )

                messages.append({"role": "user", "content":
                    "【工具执行结果】\n" + "\n\n".join(feedback_lines) +
                    "\n\n请据此继续：若任务已完成输出 done=true，否则给出下一轮 tool_calls。"})

                if len(messages) > 26:      # 窗口保护，防止上下文无限增长
                    del messages[2:-20]
                continue

            # ==========================================================
            # 【增量修复 2】容错收敛：模型给了结构化结果却忘了 done=true，
            #   或 done=false 但已给出 candidates → 直接按"完成"收敛，
            #   绝不因为一个布尔字段缺失而判定"无法收敛"并耗尽重试。
            # ==========================================================
            if not tool_calls and _looks_converged(decision):
                self.think(
                    task, "think",
                    "模型已给出结构化结果（candidates/done），按收敛处理"
                    + ("（模型漏写 done=true，系统容错接管）"
                       if not decision.get("done") else ""),
                )
                decision = {**decision, "done": True}

            if decision.get("done"):
                result.output = str(decision.get("summary") or decision.get("reasoning") or "代码任务已完成。")
                arts = decision.get("artifacts") or []
                result.artifacts = [str(a) for a in arts][:50]
                # 【增量修复 2】结构化候选清单：原样保留，供前端/调度Agent 直接消费
                candidates, unrecognized, stats = _extract_candidates(decision)
                if candidates or unrecognized or isinstance(decision.get("candidates"), list):
                    result.candidates = candidates
                    result.unrecognized = unrecognized
                    result.candidate_stats = stats
                    result.output = _compose_candidate_summary(
                        result.output, candidates, unrecognized, stats)
                result.success = True
                result.need_approval = False
                # 【需求点 1】file_notes（每个文件的作用说明）：单批路径同样落库，
                #   与分批模式保持同一份产物结构，便于上游直接消费。
                result.file_notes = self._extract_file_notes(
                    decision, list(result.classify_scope or []))
                self.ctx.db.log_agent(
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                    event="code.converged",
                    detail=(f"第 {round_index + 1} 轮收敛，工具调用 {len(result.tool_records)} 次；"
                            f"候选={len(candidates)} 无法识别={len(unrecognized)}；"
                            f"产出：{result.output[:200]}"),
                )
                if active is not None:
                    self._active.pop(self._active_key(task.task_id, active.pending_approval_id), None)
                return result

            result.fix_attempts = round_index + 1
            messages.append({"role": "user", "content":
                "你既没有给出 tool_calls，也没有 done=true。请重新输出合法 JSON 结构。"})

        # ==================================================================
        # 【增量修复 3】轮次耗尽 → 统一归类为 MODEL_CONVERGE_FAIL
        #   （模型推理无法收敛 / 输出格式无法解析），与 IO 类错误分开记录；
        #   仍保留 CODE_RETRY_EXHAUSTED 作为上游链路阻断的错误码（不改动阻断逻辑）。
        # ==================================================================
        result.success = False
        result.failure_code = ERR_CODE_RETRY_EXHAUSTED
        result.failure_kind = FAILURE_KIND_MODEL_CONVERGE
        result.fix_attempts = max_rounds
        result.output = (
            f"模型推理无法收敛：连续 {max_rounds} 轮未输出可被后端解析的执行结果"
            f"（模式={'原生清单语义分类' if classify_only else '代码执行'}；"
            f"非法 JSON 轮次 {invalid_json_rounds}），已终止。"
        )
        result.denied_reason = result.denied_reason or result.output
        # 【BUG-C 1/2/5】达到重试上限 → error 级别思考链（前端红色错误标识）+ 显式错误消息
        self.think_error(
            task,
            f"{result.output} failure_code={ERR_CODE_RETRY_EXHAUSTED} "
            f"failure_kind={FAILURE_KIND_MODEL_CONVERGE}",
        )
        self.ctx.logger.exception_log(
            error_code=ERR_MODEL_CONVERGE_FAIL,
            message=(f"[{FAILURE_KIND_MODEL_CONVERGE}] 模型推理无法收敛，输出无法解析："
                     f"模式={'classify' if classify_only else 'code'}｜轮次={max_rounds}｜"
                     f"非法JSON轮次={invalid_json_rounds}｜上游错误码={ERR_CODE_RETRY_EXHAUSTED}｜"
                     f"子任务={task.title or task.task_id}"),
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
        )
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="code.converge_failed", level="error",
            detail=(f"failure_kind={FAILURE_KIND_MODEL_CONVERGE} rounds={max_rounds} "
                    f"invalid_json_rounds={invalid_json_rounds} "
                    f"mode={'classify' if classify_only else 'code'}"),
        )
        return result

    # ==================================================================
    # 工具执行（含高危判定 → 审批 → 执行）
    # ==================================================================
    async def _run_tool(self, task: TaskState, tool: str, args: dict, *,
                        approved: bool | None = None) -> ToolExecutionRecord:
        """工具执行总入口。

        approved=None  ：首次请求 —— 高危动作一律暂停任务并推送人工审批（第4章 4.2 规则2）
        approved=True  ：人工审批已通过 —— 直接裸机执行（第4章 4.2 规则3）
        approved=False ：人工审批被拒绝 —— 终止子任务（第3章 3.5 规则2）
        """
        if not tool:
            return ToolExecutionRecord("", args, False, "工具名为空，已忽略")

        # 第4章 4.2 规则2：高危自动触发 waiting_approval
        risk = assess_operation(tool, args)

        # 能力白名单：工具 → 能力映射，越权立即拦截
        cap_map = {
            "write_file": CAPABILITY_FILE_WRITE, "edit_file": CAPABILITY_FILE_WRITE,
            "apply_patch": CAPABILITY_FILE_WRITE, "batch_write": CAPABILITY_FILE_WRITE,
            "batch_edit": CAPABILITY_FILE_WRITE, "move_file": CAPABILITY_FILE_WRITE,
            "rename_file": CAPABILITY_FILE_WRITE,
            "delete_file": CAPABILITY_FILE_DELETE, "delete_dir": CAPABILITY_FILE_DELETE,
            "run_command": CAPABILITY_COMMAND, "shell": CAPABILITY_COMMAND, "exec": CAPABILITY_COMMAND,
            "download": CAPABILITY_DOWNLOAD, "fetch_url": CAPABILITY_DOWNLOAD,
        }
        required_cap = cap_map.get(tool)
        if required_cap:
            self.require_capability(required_cap, detail=f"工具 {tool}")

        # 审批拒绝：直接终止子任务，绝不执行
        if approved is False:
            self.think(task, "exec", f"人工审批已拒绝：{risk.operation_type or tool}，子任务终止")
            return ToolExecutionRecord(
                tool=tool, args=args, ok=False,
                output=f"人工审批已拒绝，未执行：{risk.danger_reason or tool}",
                high_risk=risk.is_high_risk, approved=False,
            )

        # 审批已通过：跳过审批环节，但仍走 execute_tool 内的二次安全复核（后端双重校验）
        if approved is True and risk.is_high_risk:
            self.think(task, "exec", f"审批通过，裸机执行高危操作：{risk.operation_type}（{tool}）")
            return await self.execute_tool(task, tool, args, approved=True)

        if risk.is_high_risk:
            # 第3章 3.2：审批事件 metadata 强制四字段（risk_level/operation_desc/operation_params/danger_reason）
            approval_meta = ApprovalMetadata(
                risk_level=risk.risk_level,
                operation_desc=(
                    f"{risk.operation_type}：{tool} "
                    f"{json.dumps(_redact_args(args), ensure_ascii=False)[:400]}"
                ),
                operation_params=json.dumps(_redact_args(args), ensure_ascii=False),
                danger_reason=risk.danger_reason,
                operation_type=risk.operation_type,
                tool=tool,
                matched_patterns=list(risk.matched),
            )
            approval_msg, execution = self.ctx.approval_center.create_request(
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                meta=approval_meta, tool=tool, args=args,
            )
            task.mark_waiting_approval(f"高危操作 {risk.operation_type} 强制审批")
            self.think(task, "exec", f"高危拦截：{risk.operation_type} → 任务暂停，推送人工审批")
            record = ToolExecutionRecord(
                tool=tool, args=args, ok=False,
                output=f"已进入人工审批：{risk.danger_reason}",
                high_risk=True, approved=None, approval_id=execution.approval_id,
            )
            object.__setattr__(record, "_approval_msg", approval_msg)
            return record

        # 低危：直接执行（仅文件读写/目录列举等无害操作）
        return await self.execute_tool(task, tool, args, approved=True)

    async def execute_tool(self, task: TaskState, tool: str, args: dict, *,
                           approved: bool) -> ToolExecutionRecord:
        """实际执行工具。高危工具必须 approved=True 才允许走到这里。"""
        guard = self.ctx.file_guard
        last_error = ""

        # 第9章 9.1 规则2：工具调用失败重试 2 次
        for attempt in range(MAX_TOOL_CALL_RETRIES + 1):
            try:
                if tool == "list_dir":
                    # 【需求点 Bug4】返回显式类型标注 + 原始文件名，避免 `.`/`/` 前缀被误读为文件名
                    rows = guard.list_dir(str(args.get("path") or "."))
                    text = guard.format_listing_rows(rows) or "（空目录）"
                    return ToolExecutionRecord(tool, args, True, text)

                if tool == "read_file":
                    rel = str(args.get("path") or "")
                    res = guard.read_text(rel)
                    if not res.ok:
                        return ToolExecutionRecord(tool, args, False, res.detail)
                    return ToolExecutionRecord(tool, args, True, str(res.detail)[:20000])

                if tool in ("write_file", "edit_file", "apply_patch", "batch_write"):
                    rel = str(args.get("path") or "")
                    content = str(args.get("content") or args.get("patch") or "")
                    append = bool(args.get("append"))
                    res = guard.write_text(rel, content, append=append)
                    if not res.ok:
                        return ToolExecutionRecord(tool, args, False, res.detail)

                    # ==========================================================
                    # 【需求点 Bug5】写盘后强制**真实读回校验**：
                    #   必须确认文件已在当前工作区磁盘上存在（且内容哈希一致），
                    #   否则一律按失败返回，禁止出现"界面显示已创建、磁盘没有文件"。
                    # ==========================================================
                    verify = guard.verify_written(rel, expect_sha256=res.sha256)
                    if not verify["exists"]:
                        self.ctx.logger.exception_log(
                            error_code="FILE_WRITE_NOT_PERSISTED",
                            message=f"写入后磁盘校验失败：{verify['detail']}",
                            session_id=task.session_id, task_id=task.task_id,
                            agent_role=self.agent_role,
                        )
                        return ToolExecutionRecord(
                            tool, args, False,
                            f"写入未真正落盘：{verify['detail']}",
                        )

                    verb = "追加" if append else "写入"
                    self.think(task, "write",
                               f"磁盘已确认：{rel}（{verify['size']} 字节，sha256={verify['sha256'][:16]}）")
                    return ToolExecutionRecord(
                        tool, args, True,
                        f"{verb}成功并已通过磁盘校验 {rel}"
                        f"（{res.bytes_written} 字节, sha256={res.sha256[:16]}）",
                    )

                if tool in ("delete_file", "delete_dir"):
                    # 第4章 4.2 规则3：审批通过后裸机执行，拒绝则终止任务
                    res = guard.delete(str(args.get("path") or ""), approved=approved)
                    return ToolExecutionRecord(tool, args, res.ok, res.detail, high_risk=True,
                                               approved=approved)

                if tool in ("run_command", "shell", "exec"):
                    return await self._run_command(task, args, approved=approved)

                if tool in ("download", "fetch_url"):
                    return await self._download(task, args, approved=approved)

                if tool == "read_doc":
                    rel = str(args.get("path") or "")
                    return ToolExecutionRecord(tool, args, False,
                                               f"read_doc 不属于代码工程Agent 职责（{rel}）")

                return ToolExecutionRecord(tool, args, False, f"未登记工具：{tool}")
            except SecurityViolation as exc:
                self.ctx.logger.exception_log(
                    error_code=exc.code, message=str(exc),
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                )
                self.think(task, "exec", f"安全拦截：{exc}")
                return ToolExecutionRecord(tool, args, False,
                                           f"安全拦截（{exc.code}）：{exc}", high_risk=True,
                                           approved=approved)
            except Exception as exc:  # noqa: BLE001
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < MAX_TOOL_CALL_RETRIES:
                    await asyncio.sleep(0.3 * (attempt + 1))
                    continue
                self.ctx.logger.exception_log(
                    error_code=ERR_TOOL_FAILED, message=f"工具 {tool} 失败：{last_error}",
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                )
        return ToolExecutionRecord(tool, args, False, f"工具调用重试 {MAX_TOOL_CALL_RETRIES} 次后失败：{last_error}")

    # ==================================================================
    # 系统命令执行（审批通过后才被调用；仍做二次安全加固）
    # ==================================================================
    async def _run_command(self, task: TaskState, args: dict, *, approved: bool) -> ToolExecutionRecord:
        self.require_capability(CAPABILITY_COMMAND)
        command = str(args.get("command") or "").strip()
        if not approved:
            raise SecurityViolation("命令未获审批，拒绝执行", code="APPROVAL_REQUIRED")

        # 二次校验：命令本身不得再含越界/破坏模式（后端双重校验，不信任上游）
        risk = assess_operation("run_command", args)
        if not risk.is_high_risk:
            raise SecurityViolation("命令风险评估结果异常，拒绝执行", code="RISK_RECHECK_FAILED")

        cwd_rel = str(args.get("cwd") or ".")
        cwd = self.ctx.file_guard.resolve(cwd_rel) if cwd_rel not in (".", "") else self.ctx.file_guard.session.workspace
        if not cwd.exists():
            cwd.mkdir(parents=True, exist_ok=True)

        self.think(task, "exec", f"Pwsh · {command[:120]}")
        env = dict(os.environ)
        env["MAE_SESSION_ID"] = self.ctx.file_guard.session.session_id

        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            except asyncio.TimeoutError:
                proc.kill()
                return ToolExecutionRecord("run_command", args, False, "命令执行超时（>120s），已强制终止",
                                           high_risk=True, approved=approved)
        except (OSError, ValueError) as exc:
            return ToolExecutionRecord("run_command", args, False, f"命令启动失败：{exc}",
                                       high_risk=True, approved=approved)

        out = (stdout or b"").decode("utf-8", errors="replace")[-8000:]
        err = (stderr or b"").decode("utf-8", errors="replace")[-4000:]
        text = f"exit_code={proc.returncode}\n--- stdout ---\n{out}\n--- stderr ---\n{err}"
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="command.executed", detail=f"{command[:400]} => exit={proc.returncode}",
        )
        return ToolExecutionRecord("run_command", args, proc.returncode == 0, text,
                                   high_risk=True, approved=approved)

    # ==================================================================
    # 外网下载（审批通过后才被调用；仅允许 http/https，落到会话目录）
    # ==================================================================
    async def _download(self, task: TaskState, args: dict, *, approved: bool) -> ToolExecutionRecord:
        self.require_capability(CAPABILITY_DOWNLOAD)
        if not approved:
            raise SecurityViolation("下载未获审批，拒绝执行", code="APPROVAL_REQUIRED")

        url = str(args.get("url") or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            raise SecurityViolation(f"仅允许 http/https 下载：{url}", code="INVALID_URL")

        rel = str(args.get("path") or "").strip()
        if not rel:
            rel = "download/" + normalize_filename(url.split("/")[-1] or "download.bin")
        target = self.ctx.file_guard.resolve(rel)

        self.think(task, "exec", f"外网下载（已审批）：{url}")
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.content
        except Exception as exc:  # noqa: BLE001
            return ToolExecutionRecord("download", args, False, f"下载失败：{type(exc).__name__}: {exc}",
                                       high_risk=True, approved=approved)

        if len(data) > 50 * 1024 * 1024:
            return ToolExecutionRecord("download", args, False, "下载内容超过 50MB 上限，已丢弃",
                                       high_risk=True, approved=approved)

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return ToolExecutionRecord("download", args, True,
                                   f"下载完成 {rel}（{len(data)} 字节）", high_risk=True, approved=approved)

    # ==================================================================
    # 消息入口
    # ==================================================================
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        self.require_capability(CAPABILITY_FILE_WRITE)
        instruction = str(msg.metadata.get("instruction") or msg.content_text())
        # 【队长-队员架构 · 规则4】队长唯一基准清单：作为独立 metadata 下发，
        #   单独放进 user 消息（干净分片），不污染系统提示词与任务指令正文。
        canonical_block = str(msg.metadata.get("canonical_block") or "")
        res = await self.execute_instruction(
            task, instruction, image_resources=msg.image_resources,
            canonical_block=canonical_block)

        if res.need_approval and res.approval_message is not None:
            # 高危：任务停留在 waiting_approval，审批消息经总线推送（第3章 3.2）
            return res.approval_message

        # ==================================================================
        # 【BUG-C 1/5】失败必须走 error 消息（msg_type=error → 子任务状态 failed）。
        #   历史缺陷：重试达到上限后 success=False 但 denied_reason 为空，
        #   于是走 result_message(status=success)，把失败当成成功上报，
        #   导致系统继续向下流转并生成基于空结果的汇总报告。
        # ==================================================================
        if not res.success:
            code = res.failure_code or ("CODE_TASK_DENIED" if res.denied_reason else "CODE_TASK_FAILED")
            reason = res.denied_reason or res.output or "代码任务执行失败"
            # 【增量修复 3】区分错误类型：IO 错误（目录/权限） vs 模型收敛失败
            kind = res.failure_kind or classify_failure(code)
            reason = f"[{kind or 'error'}] {reason}" if kind else reason
            self.ctx.logger.exception_log(
                error_code=code,
                message=(f"代码子任务失败并上报 error 消息：failure_kind={kind or '-'} "
                         f"{reason[:600]}"),
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return self.error_message(task, code, reason)

        # 【队长-队员架构 · 规则4】队长唯一基准清单是否已注入本次子任务：
        #   随结果回传，供运行时/前端确认"下游确实沿用了队长裁决的清单"。
        canonical_applied = bool(msg.metadata.get("canonical_block"))
        return self.result_message(task, res.output, metadata={
            "artifacts": res.artifacts,
            # 【增量修复 2】结构化候选清单随结果回传（供调度Agent / 前端直接消费）
            "candidates": res.candidates,
            "unrecognized": res.unrecognized,
            "candidate_stats": res.candidate_stats,
            # 【需求点 1】分批合并后的完整文件作用清单（path + purpose）随结果回传
            "file_notes": res.file_notes,
            # 【队长-队员架构】本次判定覆盖的文件范围（队长一致性校验据此比对"文件池是否一致"）
            "classify_scope": res.classify_scope,
            # 【队长-队员架构 · 规则4】是否按队长唯一基准清单执行（强制沿用证据）
            "canonical_applied": canonical_applied,
            "plan_title": str(msg.metadata.get("plan_title") or ""),
            # 【增量修复 1】后端原生扫描摘要（证明目录遍历未经过模型）
            #   分批模式下额外带 batched / batches / batches_ok / batches_failed 字段
            "native_scan": res.native_scan,
            "tool_records": [
                {"tool": r.tool, "ok": r.ok, "high_risk": r.high_risk,
                 "approved": r.approved, "approval_id": r.approval_id, "output": r.output[:1000]}
                for r in res.tool_records
            ],
        })


def _redact_args(args: dict) -> dict:
    """工具参数脱敏（避免把密钥/口令写进审批记录与日志）。"""
    out: dict[str, Any] = {}
    for k, v in (args or {}).items():
        if k.lower() in ("apikey", "api_key", "password", "token", "secret"):
            out[k] = "***REDACTED***"
        elif isinstance(v, str) and len(v) > 600:
            out[k] = v[:600] + f"...(共{len(v)}字符)"
        else:
            out[k] = v
    return out


__all__ = ["CodeAgent", "CodeTaskResult", "ToolExecutionRecord", "CODE_SYSTEM_PROMPT"]
