# -*- coding: utf-8 -*-
"""
模型调用客户端（服务层）—— 真实调用五大商用大模型独立 API Key

架构文档来源：
  - 第1章 1.3 模型固定分配：七大 Agent 绑定五大模型 provider
  - 第2章 2.3 模型降级兜底规则：
      1. 调度模型(DeepSeek-Flash)失效：自动降级为 GLM 临时调度，前端提示「当前为备选调度模型」
      2. 任意专项Agent模型失效：禁用该能力，任务返回能力不可用提示，不崩溃整体系统
  - 第2章 2.4 Token与状态栏数据规则：输入/输出Token、缓存命中率全部由模型API返回，后端采集
  - 第9章 9.1 异常处理：模型 429 限流指数退避重试 3 次
  - 第6章 6.1 规则4：Key 测试接口超时 5 秒

【需求点 三、模型生态位自动补位】
  专属模型不可用（密钥无效 / 鉴权失败 / 超时 / 429 限流 / 接口报错）时，
  自动按固定优先级尝试本机已连通测试通过的其他模型补齐 Agent 生态位：
      DeepSeek > Qwen > GLM > Kimi
  · 能力过滤：视觉感知Agent 只尝试支持图片多模态的模型；代码工程Agent 优先代码能力强的模型；
    文档信息Agent 优先大长上下文模型；能力不匹配的候选直接跳过。
  · 仅本次任务周期使用补位模型（按 task_id 记忆），下一次全新任务优先重试原始指定模型。
  · 全部候选失败 → 返回「缺少可用模型，该Agent生态位无法补位，请检查API密钥配置」，系统不崩溃。

【需求点 Bug6】豆包（doubao）已从系统移除，全部相关分支与专用请求体/错误体解析已删除。

说明：所有 provider 均使用 OpenAI 兼容的 /chat/completions 协议
      （DeepSeek / Qwen / Kimi K3 / Kimi k2.6 / GLM 均支持）。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from backend.infrastructure.logger import get_logger
from backend.services.config_store import ConfigStore
from backend.utils.secrets import sha256_hex
from backend.utils.model_registry import (
    CONSTRAINT_META_KEY,
    apply_model_constraints,
    build_chat_body,
    looks_like_invalid_temperature as _looks_like_invalid_temperature,
    looks_like_insufficient_balance as _looks_like_insufficient_balance,
    looks_like_model_not_found as _looks_like_model_not_found,
    model_constraint_note,
    normalize_chat_content,
    parse_provider_error,
    resolve_model_constraints,
    validate_model_name,
)
from backend.utils.constants import (
    AGENT_BINDINGS,
    AGENT_CODE,
    AGENT_DISPATCH,
    AGENT_DOC,
    AGENT_MEMORY,
    AGENT_REQUIRED_CAPABILITY,
    AGENT_VISION,
    CAP_CODE,
    CAP_LONG_CONTEXT,
    CAP_VISION,
    CONFIG_STATE_CALL_FAILED,
    CONFIG_STATE_KEY_MISSING,
    CONFIG_STATE_MODEL_EMPTY,
    CONFIG_STATE_NOT_TESTED,
    CONFIG_STATE_READY,
    ECOSYSTEM_FALLBACK_EXHAUSTED_MESSAGE,
    ECOSYSTEM_FALLBACK_PRIORITY,
    ECOSYSTEM_FALLBACK_PRIORITY_TEXT,
    ERR_ECOSYSTEM_EXHAUSTED,
    ERR_INSUFFICIENT_BALANCE,
    ERR_MODEL_JSON_INVALID,
    ERR_MODEL_RUNTIME_CALL_FAILED,
    ERR_MODEL_UNAVAILABLE,
    ERR_RATE_LIMITED,
    KEY_ERROR_AUTH,
    KEY_ERROR_EMPTY_RESPONSE,
    KEY_ERROR_ENDPOINT,
    KEY_ERROR_INVALID,
    KEY_ERROR_MISSING,
    KEY_ERROR_MODEL_NOT_FOUND,
    KEY_ERROR_NETWORK,
    KEY_ERROR_RATE_LIMITED,
    KEY_ERROR_SERVER,
    KEY_ERROR_TIMEOUT,
    KEY_ERROR_UNKNOWN,
    KEY_TEST_TIMEOUT_SECONDS,
    KNOWN_MODELS,
    MAX_RATE_LIMIT_RETRIES,
    MODEL_CONSTRAINT_LOG_EVENT,
    MODEL_CONSTRAINTS,
    PROVIDER_CAPABILITIES,
    PROVIDER_DEEPSEEK,
    PROVIDER_DEFAULT_MODELS,
    PROVIDER_GLM,
    PROVIDER_MODEL_DOC_HINT,
    RATE_LIMIT_BASE_BACKOFF_SECONDS,
)


def key_source_for(provider: str) -> str:
    """【BUG-A 1】密钥来源标注：只可能来自加密配置库（AES-256 密文解出）。

    这里把"密钥从哪来"显式写进审计日志，杜绝"读错成其它厂商密钥"这类隐性问题。
    """
    return f"本机加密配置库 config/system_config.json → api_keys[{provider}]（AES-256-GCM 解密）"


class ModelUnavailable(Exception):
    """模型不可用（未配置 Key / 网络失败 / 达到重试上限）→ 触发能力禁用降级。"""

    def __init__(self, message: str, *, code: str = ERR_MODEL_UNAVAILABLE, agent_role: str = "",
                 http_status: int = 0, raw_response: str = "", url: str = "",
                 provider: str = "", model: str = "", attempt_errors: list | None = None):
        super().__init__(message)
        self.code = code
        self.agent_role = agent_role
        # 【BUG-A 3】完整错误上下文：HTTP 状态码 + response 原始信息 + 请求地址 + 尝试明细
        self.http_status = int(http_status or 0)
        self.raw_response = str(raw_response or "")
        self.url = str(url or "")
        self.provider = str(provider or "")
        self.model = str(model or "")
        self.attempt_errors = list(attempt_errors or [])

    def error_detail_text(self) -> str:
        """【BUG-A 3】可直接写入系统日志的完整失败详情。"""
        lines = [f"message={self}", f"code={self.code}"]
        if self.provider or self.model:
            lines.append(f"provider={self.provider or '-'} model={self.model or '-'}")
        if self.url:
            lines.append(f"url={self.url}")
        lines.append(f"http_status={self.http_status or '(无 HTTP 响应：网络/超时类错误)'}")
        if self.raw_response:
            lines.append(f"raw_response={self.raw_response[:2000]}")
        if self.attempt_errors:
            lines.append("attempts=" + " || ".join(str(a)[:300] for a in self.attempt_errors[:6]))
        return "\n".join(lines)


@dataclass
class ModelResponse:
    text: str
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    cache_hit_rate: float = 0.0
    elapsed_ms: int = 0
    attempts: int = 0
    degraded: bool = False
    degrade_note: str = ""
    raw_usage: dict = field(default_factory=dict)
    # ---- 【需求点 三、2】生态位补位信息（前端状态栏 + 轻提示 + 日志） ----
    ecosystem_fallback: bool = False        # 本次是否使用了生态位补位模型
    fallback_note: str = ""                 # 例如：当前缺失Kimi k2.6模型，生态位补位，实际使用：DeepSeek
    original_model_label: str = ""          # 原本指定的模型名称
    actual_model_label: str = ""            # 实际运行的模型名称
    failure_reason: str = ""                # 原始失败原因


def _priority_index(provider: str) -> int:
    """固定补位优先级索引：DeepSeek > Qwen > GLM > Kimi（未登记排最后）。"""
    try:
        return ECOSYSTEM_FALLBACK_PRIORITY.index(provider)
    except ValueError:
        return len(ECOSYSTEM_FALLBACK_PRIORITY)


# 【需求点 Bug1】错误码 → 中文分类，供前端按类别着色展示
_ERROR_CATEGORY_MAP = {
    KEY_ERROR_MISSING: "未配置",
    KEY_ERROR_INVALID: "密钥无效",
    KEY_ERROR_AUTH: "鉴权失败",
    KEY_ERROR_TIMEOUT: "连接超时",
    KEY_ERROR_NETWORK: "网络异常",
    KEY_ERROR_RATE_LIMITED: "触发限流",
    KEY_ERROR_MODEL_NOT_FOUND: "模型不可用",
    KEY_ERROR_ENDPOINT: "接口地址错误",
    KEY_ERROR_SERVER: "平台服务异常",
    KEY_ERROR_EMPTY_RESPONSE: "响应为空",
    KEY_ERROR_UNKNOWN: "未知错误",
}


def _error_category(error_code: str) -> str:
    return _ERROR_CATEGORY_MAP.get(error_code, "未知错误")


# ==========================================================================
# 【需求点 BUG-NEW1】OpenAI 兼容端点规范化
#   设置面板允许用户按需求填写的「裸域名」Base URL（例如 https://api.deepseek.com），
#   而所有 provider 的真实请求路径都是 <base>/chat/completions。
#   DeepSeek / Moonshot 等平台同时接受 https://host 与 https://host/v1 两种写法，
#   但混用（base=https://api.deepseek.com + /chat/completions）会 404。
#   因此这里统一规范化为「裸域名 + /v1 + /chat/completions」，保证：
#     · 设置面板 Base URL 显示值与需求文案一致（不带 /v1）
#     · 实际请求路径始终落在平台支持的 OpenAI 兼容端点上
#   已含版本段（/v1、/v4、/compatible-mode/v1 …）的地址原样保留，不做二次拼接。
# ==========================================================================
_VERSION_SEGMENT_RE = re.compile(r"/v\d+(?:[a-z0-9.\-]*)?$", re.IGNORECASE)


def openai_chat_url(base_url: str) -> str:
    """把任意形式的 OpenAI 兼容 Base URL 规范化为完整的 chat/completions 请求地址。"""
    base = (base_url or "").strip().rstrip("/")
    if not base:
        return "/chat/completions"
    lower = base.lower()
    if lower.endswith("/chat/completions"):
        return base
    # 已含版本段（/v1、/v4、/compatible-mode/v1 …）→ 直接拼接
    if _VERSION_SEGMENT_RE.search(base):
        return base + "/chat/completions"
    # 裸域名 → 补 /v1（DeepSeek / Moonshot / OpenAI 兼容规范）
    return base + "/v1/chat/completions"


class JSONExtractionError(ValueError):
    """模型回复无法抽取为合法 JSON（携带原始文本，供调用方完整落日志）。

    继承 ValueError：既有调用方（`except ValueError`）无需改动即保持兼容。
    """

    def __init__(self, message: str, *, raw_text: str = "", cleaned_text: str = "",
                 attempts: list[str] | None = None, original: Exception | None = None,
                 error_hint: str = ""):
        super().__init__(message)
        self.raw_text = raw_text
        self.cleaned_text = cleaned_text
        self.attempts = list(attempts or [])
        self.original = original
        # 原始回复直接解析时的语法错误定位（清洗成功但结构不可用时也有值）
        self.error_hint = error_hint

    @property
    def detail(self) -> str:
        """JSONDecodeError 的行/列定位信息（用于回灌给模型做修正反馈）。"""
        if isinstance(self.original, json.JSONDecodeError):
            return (f"{self.original.msg}（第 {self.original.lineno} 行第 "
                    f"{self.original.colno} 列，字符位置 {self.original.pos}）")
        if self.error_hint:
            return self.error_hint
        return str(self.original) if self.original else ""


# ==========================================================================
# 【增量修复 4】JSON 预处理清洗：从"模型原始回复"到"可解析 JSON 主体"
#   ① 剥离 ```json / ``` Markdown 围栏，提取 JSON 主体；
#   ② 剔除 JSON 前后多余的自然语言描述文本，只保留 JSON 片段；
#   ③ 字符串外的尾随逗号与 // 、/* */ 注释自动剔除（常见模型 JSON 语法错误）；
#   ④ 全部失败 → 抛 JSONExtractionError，原文完整带出供日志记录。
# ==========================================================================
_MD_FENCE_RE = re.compile(r"```[ \t]*(?:json|JSON|Json)?[ \t]*\r?\n?(.*?)```", flags=re.S)


def _strip_markdown_fences(text: str) -> str:
    """① 剥离 Markdown 代码围栏，提取其中的 JSON 主体（含未闭合围栏的情况）。"""
    if not text:
        return ""
    matches = [m.group(1) for m in _MD_FENCE_RE.finditer(text) if m.group(1) is not None]
    if matches:
        # 多个围栏块时取最长的一块（模型常把 JSON 拆成多个块，最长块即主体）
        return max(matches, key=len)
    # 只有起始围栏 / 没有闭合围栏：直接丢弃围栏行本身
    out = re.sub(r"^\s*```[ \t]*(?:json|JSON|Json)?[ \t]*\r?\n?", "", text)
    out = re.sub(r"```[ \t]*$", "", out)
    return out


def _strip_surrounding_prose(text: str) -> str:
    """② 剔除 JSON 前后的自然语言描述，只保留最外层 JSON 片段。

    策略：定位最早出现的 `{` / `[`，再定位其**配对**的收尾符号，
    截取 `[start:end+1]`；找不到配对时退化为"首个开括号 → 最后一个闭括号"。
    """
    candidate = (text or "").strip()
    if not candidate:
        return ""
    if candidate[0] in "{[":
        return candidate
    starts = [i for i in (candidate.find("{"), candidate.find("[")) if i != -1]
    if not starts:
        return candidate
    start = min(starts)
    opener = candidate[start]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(candidate)):
        ch = candidate[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return candidate[start:i + 1]
    # 未找到配对收尾：退化为首个开括号 → 最后一个闭括号
    end = candidate.rfind(closer)
    if end > start:
        return candidate[start:end + 1]
    return candidate[start:]


def _strip_json_noise(text: str) -> str:
    """③ 剔除**字符串外**的尾随逗号与注释（模型最常见的两类 JSON 语法错误）。

    逐字符扫描并跟踪字符串状态，因此：
      · 字符串内部的 `,` / `//` / `/*` 一律原样保留（绝不破坏业务内容）；
      · 短横线开头的文件路径（如 build/-x.txt）不会被 // 注释规则误删（要求 `//` 连续）。
    """
    src = text or ""
    out: list[str] = []
    i = 0
    n = len(src)
    in_string = False
    quote_char = '"'
    escaped = False
    while i < n:
        ch = src[i]
        if in_string:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote_char:
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            quote_char = ch
            out.append(ch)
            i += 1
            continue
        if ch == "/" and i + 1 < n and src[i + 1] == "/":
            # 行注释：跳到行尾（保留换行，避免相邻 token 粘连）
            j = src.find("\n", i)
            if j == -1:
                break
            i = j
            continue
        if ch == "/" and i + 1 < n and src[i + 1] == "*":
            j = src.find("*/", i + 2)
            i = n if j == -1 else j + 2
            continue
        if ch == ",":
            # 尾随逗号：逗号之后（忽略空白）紧邻 } 或 ] → 判定为尾随逗号，直接丢弃
            j = i + 1
            while j < n and src[j] in " \t\r\n":
                j += 1
            if j < n and src[j] in "}]":
                i += 1
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _strip_bom(text: str) -> str:
    """剔除零宽字符 / BOM（模型偶发在 JSON 头部带上不可见字符 → 解析直接失败）。"""
    return re.sub(r"[\ufeff\u200b\u200c\u200d]", "", text or "")


def _repair_python_literals(text: str) -> str:
    """③′ 最后兜底修复：把**字符串外**的 Python 字面量改写成合法 JSON 字面量。

    仅处理 True→true / False→false / None→null 三类（模型混用 Python 字面量是
    极常见的非法 JSON 成因），且严格避开字符串内部，绝不改动业务文本。
    """
    src = text or ""
    out: list[str] = []
    i = 0
    n = len(src)
    in_string = False
    escaped = False
    while i < n:
        ch = src[i]
        if in_string:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue
        matched = False
        for word, replacement in (("True", "true"), ("False", "false"), ("None", "null")):
            if src.startswith(word, i):
                before = src[i - 1] if i > 0 else ""
                after = src[i + len(word)] if i + len(word) < n else ""
                # 必须是独立 token（前后不是标识符字符），避免改动 TrueValue 这类名字
                if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                    out.append(replacement)
                    i += len(word)
                    matched = True
                    break
        if matched:
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _json_parse_candidates(raw_text: str) -> tuple[Any, list[str], str]:
    """按"由干净到宽松"的顺序尝试解析。

    返回 (解析结果, 尝试过的策略名, 语法错误定位提示)：
      · 每次尝试都会重新做一遍 Markdown 剥离 / 前后文本剔除 / 噪声剔除，
        保证任何一步清洗都可能成为最后成功的那一步；
      · 第三个值来自**原始回复直接解析**时的 JSONDecodeError（行/列），
        即使后续清洗成功也会带出，用于把"上一轮语法到底错在哪"回灌给模型（需求点 3）。
    """
    attempts: list[str] = []
    stages: list[tuple[str, str]] = []
    stage0 = _strip_bom(raw_text or "")
    stages.append(("原始回复", stage0))
    fenced = _strip_bom(_strip_markdown_fences(stage0))
    if fenced != stage0:
        stages.append(("剥离Markdown围栏", fenced))
    body = _strip_surrounding_prose(fenced)
    if body != fenced:
        stages.append(("剔除前后自然语言", body))
    cleaned = _strip_json_noise(body)
    if cleaned != body:
        stages.append(("剔除尾随逗号与注释", cleaned))
    repaired = _repair_python_literals(cleaned)
    if repaired != cleaned:
        stages.append(("修正Python字面量", repaired))
    last_error: Exception | None = None
    first_error: str = ""
    for name, candidate in stages:
        attempts.append(name)
        text = candidate.strip()
        if not text:
            continue
        try:
            return json.loads(text), attempts, first_error
        except json.JSONDecodeError as exc:
            last_error = exc
            if not first_error:
                first_error = (f"{exc.msg}（第 {exc.lineno} 行第 {exc.colno} 列，"
                               f"字符位置 {exc.pos}）")
            continue
    raise JSONExtractionError(
        "回复不是合法 JSON",
        raw_text=raw_text or "", cleaned_text=(cleaned or "").strip(),
        attempts=attempts, original=last_error, error_hint=first_error,
    )


def _log_json_extraction_failure(text: str, exc: JSONExtractionError, *,
                                 where: str = "_extract_json") -> None:
    """【需求点 2③】JSONDecodeError：把模型**原始返回文本**完整写入日志，便于调试。

    未初始化日志器（如离线单测）时静默跳过，绝不影响主链路。
    """
    try:
        from backend.infrastructure.logger import try_get_logger
        logger = try_get_logger()
        if logger is None:
            return
        raw = text or ""
        logger.exception_log(
            error_code=ERR_MODEL_JSON_INVALID,
            message=(f"模型返回非法 JSON（{where}）：{exc.detail or exc}｜"
                     f"清洗策略={exc.attempts}｜原始返回长度={len(raw)} 字符"),
            stack=(f"--- 模型原始返回（完整文本） ---\n{raw}\n"
                   f"--- 清洗后文本 ---\n{exc.cleaned_text}"),
        )
    except Exception:  # noqa: BLE001 日志失败绝不影响主链路
        pass


def _extract_json(text: str) -> Any:
    """从模型回复中稳健抽取 JSON。

    预处理清洗链路（需求点 2①/②）：
      ① 剥离 ```json / ``` Markdown 代码块标记，提取 JSON 主体；
      ② 剔除 JSON 前后多余的自然语言描述文本，只保留 JSON 片段；
      ③ 额外剔除字符串外的尾随逗号与 //、/* */ 注释等常见语法噪声；
      ④ 任一环节失败 → 抛 JSONExtractionError（含原文），并完整写入错误日志。
    """
    if not text or not str(text).strip():
        _log_json_extraction_failure(text or "",
                                     JSONExtractionError("空回复", raw_text=text or ""))
        raise JSONExtractionError("空回复", raw_text=text or "")
    try:
        return _json_parse_candidates(str(text))[0]
    except JSONExtractionError as exc:
        # ③ 捕获解析异常：完整记录模型原始返回文本
        _log_json_extraction_failure(str(text), exc)
        raise


class ModelClient:
    """统一模型客户端：负责真实 API 调用、限流退避、降级、Token 采集。"""

    # 【BUG-A 4】失败标记的自动重试冷却时间（秒）：
    #   配置存在且连通测试通过的模型，不允许因为一次调用失败就被永久禁用
    #   （历史上 disabled_agents 是永久标记，会导致"测试成功但实际调用一直走生态位补位"）。
    FAILURE_COOLDOWN_SECONDS = 60.0

    def __init__(self, config: ConfigStore):
        self.config = config
        # 第2章 2.3 规则2：记录已被禁用能力的 Agent（模型失效 → 禁用对应能力）
        #   【BUG-A 4】值是 {"reason": str, "at": float, "code": str}：
        #   超过 FAILURE_COOLDOWN_SECONDS 后自动放行重试专属模型，
        #   保证"配置存在 + 连通测试通过"时不会无故触发生态位补位。
        self.disabled_agents: dict[str, dict] = {}
        # 第2章 2.3 规则1：调度模型降级标记
        self.dispatch_degraded = False
        self.dispatch_degrade_reason = ""
        # 【需求点 三、2】仅本次任务周期使用的补位模型记忆：task_id -> {provider, model, base_url, ...}
        self._task_fallback: dict[str, dict] = {}

    # ==================================================================
    # 绑定解析
    # ==================================================================
    def binding_for(self, agent_role: str) -> dict:
        return AGENT_BINDINGS[agent_role]

    # ------------------------------------------------------------------
    # 【BUG-A 1/2】运行态绑定详情：真实业务调用与连通测试**同一套配置数据源**
    #   （constants.AGENT_BINDINGS 决定厂商与模型，ConfigStore 决定 BaseURL 与密钥）
    # ------------------------------------------------------------------
    def resolve_target_detail(self, agent_role: str) -> dict:
        """返回该 Agent 真实运行时使用的完整调用目标（供审计日志与前端核对）。"""
        binding = self.binding_for(agent_role)
        binding_provider = binding["provider"]
        provider, model, base_url, degraded, note = self.resolve_target(agent_role)
        if degraded:
            # 调度模型降级为 GLM 临时调度：模型/地址都换成 GLM 厂商的
            binding_provider = provider
        meta = self.config.get_provider_meta(provider)
        url = openai_chat_url(base_url)
        return {
            "agent": agent_role,
            "model_name": binding["model_name"],
            # ★ 该 Agent 在绑定表里被指定的专属模型标识（业务口径）
            "bound_model": binding["model"],
            "bound_provider": binding["provider"],
            # ★ 真实发往平台的厂商 / 模型标识 / 地址（运行口径）
            "provider": provider,
            "provider_name": meta.get("name", provider),
            "model": model,
            "base_url": base_url,
            "url": url,
            "provider_is_binding": provider == binding["provider"],
            "key_source": key_source_for(provider),
            "key_fingerprint": self.config.key_fingerprint(provider),
            "key_present": bool(self.config.get_api_key(provider)),
            "test_state": self.config.model_test_state(provider, model),
            "degraded": bool(degraded),
            "note": note,
        }

    def consistency_report(self) -> list[dict]:
        """【BUG-A 2/5】七大 Agent「连通测试 ↔ 真实调用」一致性核对表。

        逐条给出：绑定模型 / 实际调用的模型标识 / 厂商是否同一家 /
        密钥是否已配置 / 该模型连通测试是否通过 / 结论是否一致。
        """
        rows: list[dict] = []
        for role in AGENT_BINDINGS:
            detail = self.resolve_target_detail(role)
            issues: list[str] = []
            if detail["provider"] != detail["bound_provider"]:
                issues.append(
                    f"实际调用厂商({detail['provider']})与绑定厂商({detail['bound_provider']})不一致")
            if not detail["provider_is_binding"] and not detail["degraded"]:
                issues.append("厂商解析异常，未按 Agent 绑定表取厂商")
            if not detail["key_present"]:
                issues.append(f"厂商 {detail['provider']} 密钥缺失（配置为空）")
            elif detail["test_state"] != CONFIG_STATE_READY:
                issues.append(
                    f"模型 {detail['model']} 未完成连通测试（当前状态：{detail['test_state']}）")
            if detail["model"] != detail["bound_model"] and detail["test_state"] != CONFIG_STATE_READY:
                issues.append(
                    f"实际调用模型 {detail['model']} 未通过连通测试"
                    f"（绑定模型 {detail['bound_model']}）")
            rows.append({
                **detail,
                "consistent": not issues,
                "issues": issues,
            })
        return rows

    def resolve_target(self, agent_role: str) -> tuple[str, str, str, bool, str]:
        """返回 (provider, model, base_url, degraded, note)。

        第2章 2.3 规则1：调度模型失效 → 自动降级为 GLM 临时调度。
        """
        binding = self.binding_for(agent_role)
        provider = binding["provider"]
        model = binding["model"]

        if agent_role == AGENT_DISPATCH and self.dispatch_degraded:
            fallback_provider = PROVIDER_GLM
            if self.config.get_api_key(fallback_provider):
                meta = self.config.get_provider_meta(fallback_provider)
                return (
                    fallback_provider, meta["model"], meta["base_url"], True,
                    "当前为备选调度模型",
                )

        meta = self.config.get_provider_meta(provider)
        base_url = meta["base_url"]
        # 【BUG-A 2】模型求解唯一入口：effective_model 已内含"配置为空"语义，
        # 只有它明确返回空串时才回退到厂商主模型（绝不用默认值掩盖"未配置"）。
        model = self.effective_model(agent_role) or meta["model"] or model
        return provider, model, base_url, False, ""

    # ------------------------------------------------------------------
    # 【需求点 Bug7 / BUG-NEW2】同一 provider 下多个 Agent 使用不同模型时的正确取模
    #   · 用户在设置页自定义过该 provider 的模型（≠ 出厂默认）→ 尊重用户设置，所有该 provider 的 Agent 都用它
    #   · 未自定义（仍为出厂默认）→ 按 Agent 绑定表使用各自专属模型
    #     例：Qwen 的 调度规划Agent=qwen3.8-max、视觉感知Agent=qwen3.8-flash；
    #         Kimi 的 文档信息Agent=kimi-k3、交互交付Agent=kimi-k2.6；
    #         GLM  的 评估校验Agent=glm-5.3、记忆管理Agent=glm-5.3-flash
    # ------------------------------------------------------------------
    def effective_model(self, agent_role: str) -> str:
        """该 Agent 真实生效的模型标识（唯一求解入口）。

        取模优先级（保持既有业务逻辑不变）：
          1. 用户在设置页自定义过该 provider 的模型（≠ 出厂默认）→ 尊重用户设置；
          2. 未自定义（配置为空，或仍是出厂默认）→ 按 Agent 绑定表取各自专属模型
             （例：Qwen 调度=qwen3.8-max / 视觉=qwen3.8-flash；
                  Kimi 文档=kimi-k3 / 交付=kimi-k2.6）；
          3. 绑定模型未登记时兜底为配置值 / 出厂默认值。

        【BUG-A 2/4】"配置里真实保存的模型标识"由
        `config.get_provider_meta()["configured_model"]` 单独暴露，
        补位闸门与一致性核对表据此判定「配置为空」，
        因此本函数保持"可运行模型"语义、不再承担配置状态判定职责。
        """
        binding = self.binding_for(agent_role)
        provider = binding["provider"]
        meta = self.config.get_provider_meta(provider)
        configured = str(meta.get("configured_model") or meta.get("model") or "").strip()
        bound = str(binding.get("model") or "").strip()
        default_model = PROVIDER_DEFAULT_MODELS.get(provider, "")
        if bound and bound in KNOWN_MODELS.get(provider, ()) and (
                not configured or configured == default_model):
            # 【需求点 Bug7】未自定义 → 使用 Agent 专属绑定模型
            return bound
        return configured or default_model or bound

    def effective_label(self, agent_role: str) -> str:
        """该 Agent 实际生效的模型标签，形如「Qwen3.8-Max · qwen3.8-max」。

        【需求点 Bug2/Bug7/BUG-NEW2】思维链需要同时体现：
          · 绑定表的对外模型名称（Qwen3.8-Max / DeepSeek-Flash / Kimi K3 …）
          · 真正发往平台的模型标识（qwen3.8-max / deepseek-flash / kimi-k3 …）
        两者都展示，才能一眼看出「谁派给谁、各自用的哪个模型、是否被串模型」。
        """
        binding = self.binding_for(agent_role)
        model = self.effective_model(agent_role)
        name = str(binding.get("model_name") or "").strip()
        if name and model:
            return f"{name} · {model}"
        return name or model

    def is_available(self, agent_role: str) -> tuple[bool, str]:
        """能力可用性判定（第2章 2.3 规则2）。

        【BUG-A 4】触发条件收紧：**只有密钥缺失 / 配置为空**才算"能力不可用"。
        历史失败标记（disabled_agents）带冷却时间，冷却结束即自动放行重试专属模型，
        避免"配置存在且连通测试通过却一直触发生态位补位"。
        """
        failure = self._active_failure(agent_role)
        if failure is not None:
            return False, (
                f"{AGENT_BINDINGS[agent_role]['model_name']} 上次调用失败"
                f"（{failure['code']}），处于重试冷却中，"
                f"「{AGENT_BINDINGS[agent_role]['duty']}」能力暂不可用：{failure['reason']}"
            )
        provider = self.binding_for(agent_role)["provider"]
        if not self.config.get_api_key(provider):
            return False, f"未配置 {self.config.get_provider_meta(provider)['name']} API Key，该能力不可用"
        return True, ""

    def _active_failure(self, agent_role: str) -> dict | None:
        """读取仍然生效（未过冷却期）的失败标记；过期的自动清除。"""
        record = self.disabled_agents.get(agent_role)
        if not record:
            return None
        at = float(record.get("at") or 0.0)
        if time.time() - at >= self.FAILURE_COOLDOWN_SECONDS:
            self.disabled_agents.pop(agent_role, None)
            get_logger().info(
                f"{agent_role} 调用失败冷却期已过（{self.FAILURE_COOLDOWN_SECONDS:.0f}s），"
                "自动恢复重试专属模型（不再无故生态位补位）",
                agent_role=agent_role,
            )
            return None
        return record

    def mark_agent_failure(self, agent_role: str, reason: str, *, code: str = "") -> None:
        """专项 Agent 模型失效 → 短时间内禁用该能力（带冷却，不永久禁用），不崩溃整体系统。"""
        self.disabled_agents[agent_role] = {
            "reason": str(reason), "at": time.time(), "code": code or ERR_MODEL_UNAVAILABLE,
        }
        get_logger().exception_log(
            error_code="AGENT_CAPABILITY_DISABLED",
            message=(f"{AGENT_BINDINGS[agent_role]['model_name']} 调用失败，"
                     f"「{AGENT_BINDINGS[agent_role]['duty']}」能力进入 "
                     f"{self.FAILURE_COOLDOWN_SECONDS:.0f}s 重试冷却：{reason}"),
            agent_role=agent_role,
        )

    def clear_agent_failure(self, agent_role: str) -> None:
        """清除失败标记（调用成功后立即清除，保证下一次业务调用仍走专属模型）。"""
        self.disabled_agents.pop(agent_role, None)

    def reset_task_state(self) -> None:
        """【BUG-A 4】每次全新任务开始：清空全部失败标记与调度降级标记。

        配置存在且连通测试通过时，新任务必须重新尝试专属模型 —— 禁止把上一轮的
        偶发失败（429 / 网络抖动 / 单次 JSON 异常）带到下一轮并触发无故补位。
        """
        if self.disabled_agents:
            get_logger().info(
                "全新任务开始：已清空上一轮的模型失败标记，重新尝试各 Agent 专属模型",
                agent_role="system",
                detail=f"cleared={list(self.disabled_agents.keys())}",
            )
        self.disabled_agents.clear()
        if self.dispatch_degraded:
            self.dispatch_degraded = False
            self.dispatch_degrade_reason = ""

    def degrade_dispatch(self, reason: str) -> None:
        """调度模型失效 → 自动降级为 GLM 临时调度（第2章 2.3 规则1）。"""
        self.dispatch_degraded = True
        self.dispatch_degrade_reason = reason
        get_logger().exception_log(
            error_code="DISPATCH_MODEL_DEGRADED",
            message=f"调度模型失效，已自动降级为 GLM 临时调度：{reason}",
            agent_role=AGENT_DISPATCH,
        )

    # ==================================================================
    # 真实 API 调用
    # ==================================================================
    async def chat(
        self,
        agent_role: str,
        messages: list[dict[str, Any]],
        *,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        expect_json: bool = False,
        timeout_seconds: float = 180.0,
        task_id: str | None = None,
    ) -> ModelResponse:
        """调用 Agent 专属模型；失败时按【需求点 三】执行生态位自动补位。

        task_id 用于「仅本次任务周期使用补位模型」的记忆（下一次全新任务重试原始指定模型）。
        """
        binding = self.binding_for(agent_role)
        original_label = f"{binding['model_name']}"

        # ---- 1. 任务周期内的补位记忆：同一任务后续调用直接复用已补位成功的模型 ----
        remembered = self._task_fallback.get(task_id) if task_id else None
        if remembered and self.config.ecosystem_fallback_enabled():
            try:
                return await self._request(
                    agent_role, remembered["provider"], remembered["model"], remembered["base_url"],
                    messages, temperature=temperature, max_tokens=max_tokens,
                    expect_json=expect_json, timeout_seconds=timeout_seconds,
                    degraded=True, note=remembered["note"],
                    fallback_meta={
                        "fallback": True, "note": remembered["note"],
                        "original": remembered["original"], "actual": remembered["actual"],
                        "reason": remembered["reason"],
                    },
                )
            except Exception:  # noqa: BLE001 记忆中的补位模型也失效 → 清空并重新走完整流程
                self._task_fallback.pop(task_id, None)

        # ---- 2. 优先使用架构文档指定的专属模型 ----
        primary_error: str = ""
        primary_exc: ModelUnavailable | None = None
        # 【BUG-A 4】记录本次真实准备调用的模型标识（供补位闸门同源判定，避免二次解析串味）
        effective_model: str = ""
        available, reason = self.is_available(agent_role)
        if available:
            provider, model, base_url, degraded, note = self.resolve_target(agent_role)
            effective_model = str(model or "")
            # 【BUG-A 1】该调用真实使用的厂商与密钥来源（连通测试同源核对）
            detail = self.resolve_target_detail(agent_role)
            get_logger().info(
                f"Agent 真实调用：{agent_role} → {detail['provider_name']}({detail['provider']}) / "
                f"{detail['model']} @ {detail['url']} | 密钥指纹="
                f"{detail['key_fingerprint'] or '(未配置)'}（来源：{detail['key_source']}）"
                f" | 连通测试状态={detail['test_state']}",
                agent_role=agent_role, task_id=task_id,
                detail=f"bound_model={detail['bound_model']} effective_model={detail['model']}",
            )
            try:
                response = await self._request(
                    agent_role, provider, model, base_url, messages,
                    temperature=temperature, max_tokens=max_tokens, expect_json=expect_json,
                    timeout_seconds=timeout_seconds, degraded=degraded, note=note,
                )
                # 【BUG-A 4】调用成功 → 立即清除失败标记（下一次调用仍走专属模型）
                self.clear_agent_failure(agent_role)
                return response
            except ModelUnavailable as exc:
                primary_exc = exc
                primary_error = str(exc)
                self._log_runtime_call_failure(agent_role, exc, task_id=task_id,
                                               phase="专属模型调用")
                get_logger().exception_log(
                    error_code=exc.code,
                    message=f"{original_label} 调用失败，尝试生态位补位：{exc}",
                    agent_role=agent_role, task_id=task_id,
                )
        else:
            primary_error = reason

        # ---- 3. 生态位自动补位（开关关闭时直接失败，不做跨模型补位） ----
        if not self.config.ecosystem_fallback_enabled():
            self._handle_failure(agent_role, ModelUnavailable(primary_error, agent_role=agent_role))
            raise ModelUnavailable(
                f"{original_label} 不可用：{primary_error}（模型生态位自动补位已关闭，不执行跨模型补位）",
                agent_role=agent_role,
            )

        # ==================================================================
        # 【BUG-A 4】补位触发条件收紧：
        #   只有「密钥缺失 / 配置为空」才允许触发降级补位。
        #   配置存在（已加密落盘）+ 该模型连通测试通过 → 不允许无故补位，
        #   而是把真实失败原因（含 HTTP 状态码与返回原文）如实抛出，供上层诊断。
        # ==================================================================
        gate = self.fallback_gate(agent_role, primary_exc=primary_exc,
                                  model_override=effective_model)
        if not gate["allowed"]:
            blocked = ModelUnavailable(
                f"{original_label} 调用失败，且不满足生态位补位触发条件（{gate['reason']}）："
                f"{primary_error}",
                code=gate["code"], agent_role=agent_role,
                http_status=getattr(primary_exc, "http_status", 0),
                raw_response=getattr(primary_exc, "raw_response", ""),
                url=getattr(primary_exc, "url", ""),
                provider=self.binding_for(agent_role)["provider"],
                model=effective_model or self.resolve_target_detail(agent_role)["model"],
            )
            get_logger().exception_log(
                error_code=gate["code"],
                message=(f"未触发生态位补位：{gate['reason']} | Agent={agent_role} | "
                         f"配置状态={gate['test_state']} | 原始失败={primary_error}"),
                agent_role=agent_role, task_id=task_id,
            )
            raise blocked

        candidates = self.fallback_candidates(agent_role)
        if not candidates:
            self._handle_failure(agent_role, ModelUnavailable(primary_error, agent_role=agent_role))
            raise ModelUnavailable(
                f"{ECOSYSTEM_FALLBACK_EXHAUSTED_MESSAGE}（{original_label} 不可用：{primary_error}）",
                code=ERR_ECOSYSTEM_EXHAUSTED, agent_role=agent_role,
            )
        attempted: list[str] = []
        for candidate in candidates:
            provider = candidate["provider"]
            model = candidate["model"]
            base_url = candidate["base_url"]
            label = candidate["label"]
            attempted.append(label)
            try:
                response = await self._request(
                    agent_role, provider, model, base_url, messages,
                    temperature=temperature, max_tokens=max_tokens, expect_json=expect_json,
                    timeout_seconds=timeout_seconds, degraded=True,
                    note=f"当前缺失{original_label}模型，生态位补位，实际使用：{label}",
                    fallback_meta={
                        "fallback": True,
                        "note": f"当前缺失{original_label}模型，生态位补位，实际使用：{label}",
                        "original": original_label,
                        "actual": label,
                        "reason": primary_error,
                    },
                )
                # 【需求点 三、2】系统日志完整记录：Agent角色 / 原指定模型 / 补位模型 / 原始失败原因
                get_logger().exception_log(
                    error_code="ECOSYSTEM_FALLBACK_SUCCESS",
                    message=(
                        f"生态位补位成功 | Agent={agent_role} | 原指定模型={original_label} | "
                        f"补位模型={label} | 原始失败原因={primary_error}"
                    ),
                    agent_role=agent_role, task_id=task_id,
                )
                if task_id:
                    # 仅本次任务周期记忆补位结果（下一次全新任务重试原始指定模型）
                    self._task_fallback[task_id] = {
                        "provider": provider, "model": model, "base_url": base_url,
                        "note": f"当前缺失{original_label}模型，生态位补位，实际使用：{label}",
                        "original": original_label, "actual": label, "reason": primary_error,
                    }
                return response
            except ModelUnavailable as exc:
                get_logger().warning(
                    f"生态位候选 {label} 不可用，继续尝试下一个候选：{exc}",
                    agent_role=agent_role, task_id=task_id,
                )
                continue

        # ---- 4. 全部候选失败 → 标记失败并返回可读提示，整体系统不崩溃 ----
        detail = "、".join(attempted) if attempted else "无可用候选（需至少一个模型通过连通性测试）"
        message = (
            f"{ECOSYSTEM_FALLBACK_EXHAUSTED_MESSAGE}"
            f"（Agent={agent_role}，原指定模型={original_label}，已尝试候选：{detail}；"
            f"原始失败原因={primary_error}）"
        )
        exhausted = ModelUnavailable(
            message, code=ERR_ECOSYSTEM_EXHAUSTED, agent_role=agent_role,
        )
        get_logger().exception_log(
            error_code=ERR_ECOSYSTEM_EXHAUSTED, message=message,
            agent_role=agent_role, task_id=task_id,
        )
        # 传入 exhausted 而非原始错误：补位耗尽不做永久能力禁用，保证下次任务重试专属模型
        self._handle_failure(agent_role, exhausted)
        raise exhausted

    # ==================================================================
    # 【BUG-A 4】补位触发闸门：只有「密钥缺失 / 配置为空」才允许降级补位
    # ==================================================================
    def fallback_gate(self, agent_role: str, *, primary_exc: Exception | None = None,
                      model_override: str | None = None) -> dict:
        """返回 {"allowed", "code", "reason", "test_state", "config_state"}。

        判定顺序（配置存在且连通测试通过 → 一律不补位）：
          1. 该 Agent 绑定厂商的密钥缺失（配置为空）        → 允许补位；
          2. 生效模型标识为空                               → 允许补位；
          3. 厂商密钥存在且生效模型已连通测试通过（READY）  → **拒绝补位**，如实抛出真实错误；
          4. 密钥存在但该模型未完成连通测试（not_tested）    → 拒绝补位，
             提示"连通测试未完成"，避免把没测过的配置当成可用配置；
          5. 其他情形（配置存在、测试通过与否不明确）        → 拒绝补位兜底，
             由上层如实展示失败原因并把完整 HTTP 详情写入系统日志。

        model_override：本次调用**实际使用的模型标识**（来自 resolve_target），
        保证闸门判定与真实调用同一个模型，不会因为二次解析而串味。
        """
        detail = self.resolve_target_detail(agent_role)
        provider = detail["provider"]
        model = str(model_override if model_override is not None else detail["model"] or "").strip()
        key_present = bool(detail["key_present"])
        if model == str(detail["model"] or "").strip():
            test_state = detail["test_state"]
        else:
            # 覆盖模型与解析结果不一致 → 按该模型的真实配置状态判定
            test_state = self.config.model_test_state(provider, model)
        missing_key = not key_present
        empty_model = not model

        if missing_key:
            return {
                "allowed": True, "code": KEY_ERROR_MISSING,
                "reason": f"厂商 {detail['provider_name']} 密钥缺失（配置为空）",
                "test_state": test_state, "config_state": CONFIG_STATE_KEY_MISSING,
            }
        if empty_model:
            return {
                "allowed": True, "code": KEY_ERROR_MODEL_NOT_FOUND,
                "reason": f"厂商 {detail['provider_name']} 生效模型标识为空",
                "test_state": test_state, "config_state": CONFIG_STATE_MODEL_EMPTY,
            }
        if test_state == CONFIG_STATE_READY:
            return {
                "allowed": False, "code": CONFIG_STATE_READY,
                "reason": (f"配置存在且 {provider}/{model} 连通测试通过，"
                           "不允许无故触发生态位补位"),
                "test_state": test_state, "config_state": CONFIG_STATE_READY,
            }
        if test_state == CONFIG_STATE_NOT_TESTED:
            return {
                "allowed": False, "code": CONFIG_STATE_NOT_TESTED,
                "reason": (f"厂商 {detail['provider_name']} 已配置密钥，但模型 {model} "
                           "尚未完成连通测试，请先在设置面板测试连通后重试"),
                "test_state": test_state, "config_state": CONFIG_STATE_NOT_TESTED,
            }
        return {
            "allowed": False, "code": CONFIG_STATE_CALL_FAILED,
            "reason": (f"配置存在（密钥指纹 {detail['key_fingerprint'] or '-'}），"
                       "本次调用失败属于运行期异常，不触发生态位补位"),
            "test_state": test_state, "config_state": CONFIG_STATE_CALL_FAILED,
        }

    # ==================================================================
    # 【BUG-A 3】真实业务调用失败的完整错误记录
    #   （HTTP 状态码 + response 原始信息 + 请求地址 + 尝试明细 → 系统日志）
    # ==================================================================
    def _log_runtime_call_failure(self, agent_role: str, exc: ModelUnavailable, *,
                                  task_id: str | None, phase: str) -> None:
        detail = self.resolve_target_detail(agent_role)
        get_logger().exception_log(
            error_code=ERR_MODEL_RUNTIME_CALL_FAILED,
            message=(
                f"[真实业务调用失败] 阶段={phase} | Agent={agent_role} | "
                f"厂商={detail['provider_name']}({detail['provider']}) | "
                f"绑定模型={detail['bound_model']} | 实际调用模型={detail['model']} | "
                f"BaseURL={detail['base_url']} | 端点={detail['url']} | "
                f"密钥指纹={detail['key_fingerprint'] or '(未配置)'} | "
                f"密钥来源={detail['key_source']} | 连通测试状态={detail['test_state']} | "
                f"错误码={exc.code} | HTTP状态码={exc.http_status or '(无 HTTP 响应)'}\n"
                f"---- response 原始信息 ----\n{exc.raw_response or '(空)'}\n"
                f"---- 尝试明细 ----\n"
                + " || ".join(str(a)[:300] for a in exc.attempt_errors[:6] or [str(exc)])
            ),
            agent_role=agent_role, task_id=task_id,
        )

    # ==================================================================
    # 生态位补位候选（能力过滤 + 固定优先级）
    # ==================================================================
    def fallback_candidates(self, agent_role: str) -> list[dict]:
        """按 DeepSeek > Qwen > GLM > Kimi 顺序返回能力匹配的候选模型。

        候选来源：本机已「连通测试通过」且已配置 Key 的模型（缺失专属模型的密钥不会被选中）。
        """
        binding = self.binding_for(agent_role)
        primary_provider = binding["provider"]
        tested_ok = self.config.tested_ok_providers()
        if not tested_ok:
            return []

        required_cap = AGENT_REQUIRED_CAPABILITY.get(agent_role, "text")
        candidates: list[dict] = []
        for provider in ECOSYSTEM_FALLBACK_PRIORITY:
            # 跳过专属模型自身（它刚刚失败）；跳过未连通测试通过的模型
            if provider == primary_provider or provider not in tested_ok:
                continue
            caps = PROVIDER_CAPABILITIES.get(provider, {})
            # 能力过滤：不满足硬性能力的模型直接跳过
            if required_cap and required_cap not in (caps.get("caps") or ()):
                continue
            if required_cap == CAP_VISION and not caps.get("vision"):
                continue
            meta = self.config.get_provider_meta(provider)
            candidates.append({
                "provider": provider,
                "model": meta["model"],
                "base_url": meta["base_url"],
                "label": f"{meta['name']} {meta['model']}".strip(),
                "code_strength": caps.get("code_strength", 0),
                "long_context_strength": caps.get("long_context_strength", 0),
            })

        # 代码工程Agent：优先挑选代码能力较强的模型
        if required_cap == CAP_CODE:
            candidates.sort(key=lambda c: (-c["code_strength"], _priority_index(c["provider"])))
        # 文档信息Agent：优先挑选大长上下文模型
        elif required_cap == CAP_LONG_CONTEXT:
            candidates.sort(key=lambda c: (-c["long_context_strength"], _priority_index(c["provider"])))
        # 其余角色：严格按固定优先级 DeepSeek > Qwen > GLM > Kimi
        else:
            candidates.sort(key=lambda c: _priority_index(c["provider"]))
        return candidates

    def ecosystem_status(self) -> dict:
        return {
            "enabled": self.config.ecosystem_fallback_enabled(),
            "priority": list(ECOSYSTEM_FALLBACK_PRIORITY),
            "priority_text": ECOSYSTEM_FALLBACK_PRIORITY_TEXT,
            "tested_ok_providers": self.config.tested_ok_providers(),
            "in_task_fallbacks": [
                {"task_id": k, "original": v["original"], "actual": v["actual"]}
                for k, v in self._task_fallback.items()
            ],
        }

    def clear_task_fallback(self, task_id: str | None) -> None:
        """任务结束时清理补位记忆（下一次全新任务优先重试原始指定模型）。"""
        if task_id:
            self._task_fallback.pop(task_id, None)

    # ==================================================================
    # 单次真实请求（含 429 指数退避重试）
    # ==================================================================
    async def _request(
        self,
        agent_role: str,
        provider: str,
        model: str,
        base_url: str,
        messages: list[dict[str, Any]],
        *,
        temperature: float,
        max_tokens: int,
        expect_json: bool,
        timeout_seconds: float,
        degraded: bool,
        note: str,
        fallback_meta: dict | None = None,
    ) -> ModelResponse:
        api_key = self.config.get_api_key(provider)
        if not api_key:
            raise ModelUnavailable(
                f"未配置 {self.config.get_provider_meta(provider)['name']} API Key，无法调用",
                agent_role=agent_role,
            )

        url = openai_chat_url(base_url)
        # 【需求点 Bug6】豆包专用请求体分支已删除；所有 provider 统一走 OpenAI 兼容构造
        # ==================================================================
        # 【需求点 Bug1 · 修复】模型参数特殊适配（统一走「模型参数约束清单」）：
        #   · Kimi k2.6 平台强制 temperature 只能等于 1 → 这里强制固定 temperature=1.0，
        #     彻底忽略全局 AGENT_TEMPERATURES 配置，不再返回 400 invalid temperature；
        #   · 其余模型命中不了约束 → 继续沿用系统 temperature 配置（行为零变化）；
        #   · 约束清单位于 constants.MODEL_CONSTRAINTS，后续新模型直接在此扩展。
        # ==================================================================
        body = build_chat_body(
            model, messages,
            temperature=temperature, max_tokens=max_tokens, expect_json=expect_json,
            provider=provider,
            provider_default_model=PROVIDER_DEFAULT_MODELS.get(provider, ""),
        )
        constraint_info = body.pop(CONSTRAINT_META_KEY, None) or {}
        original_temperature = temperature
        if constraint_info.get("matched"):
            temperature = float(body.get("temperature", temperature))
            self._log_constraint_applied(
                agent_role=agent_role, provider=provider, model=model,
                info=constraint_info, original_temperature=original_temperature,
            )
        body["stream"] = False

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        started = time.time()
        last_error: Exception | None = None
        attempts = 0
        # 【BUG-A 3】逐次尝试的原始错误记录（HTTP 状态码 + 返回原文片段 + 请求地址）
        attempt_errors: list[str] = []

        for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
            attempts = attempt
            http_status = 0
            raw_body = ""
            try:
                async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                    resp = await client.post(url, headers=headers, json=body)
                http_status = int(resp.status_code)
                raw_body = (resp.text or "")[:4000]
                # 【GLM 1113】余额不足检测必须先于 429 限流重试：
                #   GLM 平台余额不足同样返回 HTTP 429（错误码 1113），
                #   若不先识别，会触发 3 次无意义的指数退避重试（欠费充值前不可能成功）。
                if http_status >= 400 and _looks_like_insufficient_balance(raw_body):
                    attempt_errors.append(
                        f"attempt={attempt} http_status={http_status} "
                        f"insufficient_balance raw={raw_body[:400] or '(空)'}")
                    get_logger().error(
                        f"模型平台账户余额不足（{provider}/{model}，HTTP {http_status}，"
                        f"错误码 1113），不重试，请充值",
                        agent_role=agent_role,
                    )
                    raise ModelUnavailable(
                        f"模型平台账户余额不足（HTTP {http_status}，错误码 1113）："
                        f"{raw_body[:300] or '(无返回体)'}。重试无意义，请先充值。",
                        code=ERR_INSUFFICIENT_BALANCE, agent_role=agent_role,
                        http_status=http_status, raw_response=raw_body, url=url,
                        provider=provider, model=model, attempt_errors=attempt_errors,
                    )
                if http_status == 429:
                    # 第9章 9.1 规则1：指数退避重试 3 次（2s / 4s / 8s）
                    wait = RATE_LIMIT_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    attempt_errors.append(
                        f"attempt={attempt} http_status=429 raw={raw_body[:400] or '(空)'}")
                    get_logger().warning(
                        f"模型限流 429（{provider}/{model}），{wait:.0f}s 后第 {attempt}/{MAX_RATE_LIMIT_RETRIES} 次重试",
                        agent_role=agent_role,
                    )
                    await asyncio.sleep(wait)
                    last_error = ModelUnavailable(
                        f"模型限流 429，已指数退避重试 {MAX_RATE_LIMIT_RETRIES} 次仍失败",
                        code=ERR_RATE_LIMITED, agent_role=agent_role,
                        http_status=http_status, raw_response=raw_body, url=url,
                        provider=provider, model=model, attempt_errors=attempt_errors,
                    )
                    continue
                if http_status >= 400:
                    detail = raw_body[:600]
                    attempt_errors.append(
                        f"attempt={attempt} http_status={http_status} raw={raw_body[:600]}")
                    # 【需求点 Bug1】平台模型标识不被支持 → 明确归类为模型不可用
                    if _looks_like_model_not_found(detail, model):
                        raise ModelUnavailable(
                            f"模型标识不被平台支持（HTTP {http_status}）：{detail}",
                            code=KEY_ERROR_MODEL_NOT_FOUND, agent_role=agent_role,
                            http_status=http_status, raw_response=raw_body, url=url,
                            provider=provider, model=model, attempt_errors=attempt_errors,
                        )
                    # 【BUG-A 3】完整记录 HTTP 状态码 + 返回原始信息 + 请求地址
                    raise ModelUnavailable(
                        f"模型接口返回 {http_status}：{detail}", agent_role=agent_role,
                        http_status=http_status, raw_response=raw_body, url=url,
                        provider=provider, model=model, attempt_errors=attempt_errors,
                    )

                data = resp.json()
                # 兼容 content 为字符串或分片数组两种返回体
                choice = (data.get("choices") or [{}])[0]
                message_obj = choice.get("message") or {}
                text = normalize_chat_content(message_obj.get("content"))
                usage = data.get("usage") or data.get("usage_details") or {}
                return self._build_response(
                    text=str(text), usage=usage, provider=provider, model=model,
                    started=started, attempts=attempts, degraded=degraded, note=note,
                    fallback_meta=fallback_meta,
                )
            except ModelUnavailable as exc:
                last_error = exc
                if exc.code == ERR_RATE_LIMITED:
                    continue
                break
            except json.JSONDecodeError as exc:
                # 【BUG-A 3】HTTP 200 但返回体不是合法 JSON → 同样完整记录原文与状态码
                attempt_errors.append(
                    f"attempt={attempt} http_status={http_status or 200} "
                    f"JSONDecodeError={exc} raw={raw_body[:600]}")
                last_error = ModelUnavailable(
                    f"模型返回体不是合法 JSON：{exc}", agent_role=agent_role,
                    http_status=http_status or 200, raw_response=raw_body, url=url,
                    provider=provider, model=model, attempt_errors=attempt_errors,
                )
                break
            except (httpx.HTTPError, OSError) as exc:
                attempt_errors.append(
                    f"attempt={attempt} http_status=(无 HTTP 响应) "
                    f"{type(exc).__name__}={exc} url={url}")
                last_error = ModelUnavailable(
                    f"模型网络调用失败：{type(exc).__name__}: {exc}", agent_role=agent_role,
                    url=url, provider=provider, model=model, attempt_errors=attempt_errors,
                )
                # 网络类错误同样走退避重试
                if attempt < MAX_RATE_LIMIT_RETRIES:
                    await asyncio.sleep(RATE_LIMIT_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                    continue
                break

        raise last_error or ModelUnavailable("模型调用失败", agent_role=agent_role)

    # ==================================================================
    # 【需求点 Bug1】模型参数约束执行留痕 + 约束清单对外导出
    # ==================================================================
    def _log_constraint_applied(self, *, agent_role: str, provider: str, model: str,
                                info: dict, original_temperature: float) -> None:
        """约束生效时写一条系统日志：谁被改写了参数、被改成什么、为什么。"""
        forced = info.get("forced") or {}
        removed = info.get("removed") or []
        try:
            get_logger().info(
                f"模型参数约束生效：{provider}/{model} 强制 {forced}"
                f"（本次业务传入 temperature={original_temperature}，已被忽略）"
                f"{'｜剔除平台锁定参数：' + '、'.join(removed) if removed else ''}"
                f"｜原因：{info.get('locked_reason') or '-'}",
                agent_role=agent_role or "system",
                detail=f"event={MODEL_CONSTRAINT_LOG_EVENT} source={info.get('source')}",
            )
        except Exception:  # noqa: BLE001 日志失败绝不影响调用
            pass

    @staticmethod
    def model_constraints_report() -> dict:
        """【需求点 Bug1 规则3】模型参数约束清单（供前端/排查查看，后续在此扩展）。"""
        items: list[dict] = []
        for key, entry in MODEL_CONSTRAINTS.items():
            alias = str(entry.get("alias_of") or "")
            items.append({
                "model": key,
                "alias_of": alias,
                "provider": entry.get("provider", ""),
                "forced_params": dict(entry.get("forced_params") or {}),
                "fixed_params": list(entry.get("fixed_params") or ()),
                "locked_reason": entry.get("locked_reason", ""),
                "enabled": bool(entry.get("forced_params") or entry.get("fixed_params")),
            })
        return {
            "items": items,
            "count": len(items),
            "note": ("模型参数约束清单：命中即强制固定参数并忽略全局配置，"
                     "未命中则沿用系统 temperature 配置"),
        }

    def constraint_note_for(self, provider: str, model: str) -> str:
        """该模型是否存在强制参数约束（用于连通测试结果提示）。"""
        return model_constraint_note(
            model, provider=provider,
            provider_default_model=PROVIDER_DEFAULT_MODELS.get(provider, ""))

    def _build_response(self, *, text: str, usage: dict, provider: str, model: str,
                        started: float, attempts: int, degraded: bool, note: str,
                        fallback_meta: dict | None = None) -> ModelResponse:
        """Token 数据全部取模型 API 真实返回（第2章 2.4 规则1，禁止前端伪造）。"""
        input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        details = usage.get("prompt_tokens_details")
        cached_tokens = int(
            usage.get("prompt_cache_hit_tokens")
            or usage.get("cache_read_input_tokens")
            or usage.get("cached_tokens")
            or (details.get("cached_tokens") if isinstance(details, dict) else 0)
            or 0
        )
        rate = round(cached_tokens / input_tokens * 100.0, 1) if input_tokens else 0.0
        meta = fallback_meta or {}
        label = self.config.provider_model_label(provider)
        return ModelResponse(
            text=text, provider=provider, model=model,
            input_tokens=input_tokens, output_tokens=output_tokens,
            cached_tokens=cached_tokens, cache_hit_rate=rate,
            elapsed_ms=int((time.time() - started) * 1000),
            attempts=attempts, degraded=degraded, degrade_note=note, raw_usage=usage,
            ecosystem_fallback=bool(meta.get("fallback")),
            fallback_note=str(meta.get("note") or ""),
            original_model_label=str(meta.get("original") or ""),
            actual_model_label=str(meta.get("actual") or label),
            failure_reason=str(meta.get("reason") or ""),
        )

    def _handle_failure(self, agent_role: str, error: Exception | None) -> None:
        reason = str(error or "未知错误")
        # 【需求点 三、3】补位耗尽属于"本次任务不可用"，不做永久能力禁用，
        # 保证下一次全新任务仍会优先重试原始指定模型。
        if getattr(error, "code", "") == ERR_ECOSYSTEM_EXHAUSTED:
            get_logger().exception_log(
                error_code=ERR_ECOSYSTEM_EXHAUSTED,
                message=f"{agent_role} 生态位补位耗尽（不做能力禁用，下次任务重试专属模型）：{reason}",
                agent_role=agent_role,
            )
            return
        # 【BUG-A 3】真实调用失败：先按"完整错误记录"落日志（HTTP 状态码 + response 原文）
        if isinstance(error, ModelUnavailable):
            self._log_runtime_call_failure(agent_role, error, task_id=None, phase="专属模型失败收口")
        if agent_role == AGENT_DISPATCH:
            self.degrade_dispatch(reason)          # 第2章 2.3 规则1
        else:
            self.mark_agent_failure(                 # 第2章 2.3 规则2（带冷却，非永久禁用）
                agent_role, reason, code=getattr(error, "code", "") or ERR_MODEL_UNAVAILABLE,
            )

    # ==================================================================
    # Key 连通性测试（第6章 6.1 规则4：超时 5 秒）
    # 【需求点 Bug1】返回结构化错误信息，含可读中文错误原因
    # ==================================================================
    async def test_key(self, provider: str, api_key: str, *, base_url: str | None = None,
                       model: str | None = None) -> dict:
        meta = self.config.get_provider_meta(provider)
        test_base = (base_url or meta["base_url"] or "").strip()
        test_model = (model or meta["model"] or "").strip()
        started = time.time()

        def done(ok: bool, error_code: str, message: str, *, hint: str = "",
                 http_status: int = 0, extra: dict | None = None) -> dict:
            result = {
                "ok": bool(ok),
                # 结构化错误信息（前端完整渲染，不截断）
                "error_code": error_code,
                "error_category": ("" if (ok and error_code != KEY_ERROR_RATE_LIMITED)
                                   else _error_category(error_code)),
                "message": message,
                "hint": hint,
                "provider": provider,
                "name": meta["name"],
                "model": test_model,
                "base_url": test_base,
                "http_status": http_status,
                "elapsed_ms": int((time.time() - started) * 1000),
                "timeout_limit_seconds": KEY_TEST_TIMEOUT_SECONDS,
                "raw_detail": "",
                # ==========================================================
                # 【BUG-A 2/5】连通测试与真实业务调用同源核对字段：
                #   · 本次测试真实使用的 Base URL 与端点（与运行时同一条规范化逻辑）
                #   · 本次测试真实使用的密钥 SHA256（与真实调用读到的密钥比对，
                #     两者不一致即说明"测试用的密钥 ≠ 运行时读取的密钥"）
                # ==========================================================
                "tested_url": openai_chat_url(test_base),
                "key_sha256": sha256_hex((api_key or "").strip().encode("utf-8")) if api_key else "",
                "key_source": key_source_for(provider),
            }
            if extra:
                result.update(extra)
            return result

        # 前置校验：密钥为空
        if not (api_key or "").strip():
            return done(False, KEY_ERROR_MISSING, "未填写 API Key",
                        hint="请在输入框中填入该模型平台的 API Key 后再测试")
        # 前置校验：接口地址非法
        if not test_base.lower().startswith(("http://", "https://")):
            return done(False, KEY_ERROR_ENDPOINT,
                        f"接口地址不合法：{test_base or '（空）'}",
                        hint="请填写以 http:// 或 https:// 开头的 Base URL，例如 https://api.deepseek.com/v1")
        if not test_model:
            return done(False, KEY_ERROR_MODEL_NOT_FOUND, "未填写模型标识（model）",
                        hint="请填写该平台实际可用的模型标识，例如 deepseek-flash")

        # 【需求点 一、Bug1】连通性测试前先做本地模型标识校验，提前暴露 400 MODEL_NOT_FOUND 风险
        model_check = validate_model_name(provider, test_model)
        if not model_check["valid"]:
            return done(False, KEY_ERROR_MODEL_NOT_FOUND, model_check["message"],
                        hint=model_check["hint"],
                        extra={"known_models": model_check["known_models"], "model_check": model_check})

        url = openai_chat_url(test_base)
        # 【需求点 Bug6 / BUG-NEW1】豆包与 Kimi-Flash 专用探测分支均已删除；统一走 OpenAI 兼容探测请求
        # ==================================================================
        # 【需求点 Bug1 · 修复】连通测试同样走「模型参数约束清单」：
        #   历史缺陷：连通测试不带 temperature，所以 Kimi k2.6 "测试通过"，
        #   而真实业务调用带 temperature=0.45 直接 400 invalid temperature。
        #   现在测试与真实调用共用同一套约束适配逻辑（同源），
        #   命中约束的模型测试时也不下发被平台锁定的参数，测试结论与运行结论一致。
        # ==================================================================
        probe_body = build_chat_body(
            test_model, [{"role": "user", "content": "ping"}], max_tokens=4,
            provider=provider, provider_default_model=PROVIDER_DEFAULT_MODELS.get(provider, ""),
        )
        probe_constraint = probe_body.pop(CONSTRAINT_META_KEY, None) or {}
        probe_body["stream"] = False
        constraint_note = self.constraint_note_for(provider, test_model)
        constraint_extra = {
            "model_constraint": probe_constraint,
            "model_constraint_note": constraint_note,
        }
        try:
            async with httpx.AsyncClient(timeout=KEY_TEST_TIMEOUT_SECONDS) as client:
                resp = await client.post(
                    url,
                    headers={"Authorization": f"Bearer {api_key.strip()}",
                             "Content-Type": "application/json"},
                    json=probe_body,
                )
            detail = (resp.text or "")[:600]
        except httpx.TimeoutException:
            return done(False, KEY_ERROR_TIMEOUT,
                        f"接口连接超时：超过 {KEY_TEST_TIMEOUT_SECONDS:.0f} 秒未响应",
                        hint=f"请检查网络与接口地址 {url} 是否可达；企业内网可能需要代理")
        except httpx.ConnectError as exc:
            return done(False, KEY_ERROR_NETWORK,
                        f"网络连接失败：无法建立到 {url} 的连接",
                        hint="请检查本机网络、DNS 与接口地址是否正确（连接被拒绝或域名解析失败）",
                        extra={"raw_detail": str(exc)[:400]})
        except httpx.HTTPError as exc:
            return done(False, KEY_ERROR_NETWORK,
                        f"网络异常：{type(exc).__name__}",
                        hint="请稍后重试；若持续失败请检查本机网络或代理设置",
                        extra={"raw_detail": str(exc)[:400]})
        except Exception as exc:  # noqa: BLE001
            return done(False, KEY_ERROR_UNKNOWN,
                        f"未知错误：{type(exc).__name__}",
                        hint="请把该错误信息反馈给维护者排查",
                        extra={"raw_detail": str(exc)[:400]})

        status = resp.status_code
        # ---- 成功 ----
        if status < 400:
            body_text = ""
            try:
                payload = resp.json()
                content = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
                if isinstance(content, list):
                    content = "".join(str(p.get("text") or "") for p in content if isinstance(p, dict))
                body_text = str(content)
            except Exception:  # noqa: BLE001 部分平台对 ping 返回非标准体，但 HTTP 200 仍视为连通
                body_text = ""
            warn = model_check["level"] == "warn"
            success_hint = (f"注意：{model_check['message']}。{model_check['hint']}" if warn
                            else f"返回内容示例：{(body_text or '（空响应）')[:60]}")
            if constraint_note:
                success_hint = f"{constraint_note}；{success_hint}"
            return done(True, "", f"连通正常，模型 {test_model} 可调用",
                        hint=success_hint,
                        http_status=status,
                        extra={"raw_detail": detail, "model_check": model_check,
                               "model_warning": model_check["message"] if warn else "",
                               **constraint_extra})

        lowered = detail.lower()
        # 【需求点 Bug6】豆包专用错误体解析分支已删除；统一走下方 OpenAI 兼容错误分类
        # ---- 鉴权类 ----
        if status in (401, 403):
            if "not found" in lowered or "no permission" in lowered or "model" in lowered and "not" in lowered:
                return done(False, KEY_ERROR_MODEL_NOT_FOUND,
                            f"模型不存在或无权限（HTTP {status}）：{detail}",
                            hint=f"请确认账号已开通模型「{test_model}」并填写正确标识",
                            http_status=status)
            return done(False, KEY_ERROR_AUTH,
                        f"鉴权失败（HTTP {status}）：API Key 无效或无权限",
                        hint="请核对 API Key 是否复制完整、是否已被禁用或过期",
                        http_status=status)
        # ---- 限流 ----
        if status == 429:
            return done(True, KEY_ERROR_RATE_LIMITED,
                        f"密钥有效，但当前被平台限流（HTTP 429）",
                        hint="连通性验证通过；正式调用时系统会自动指数退避重试 3 次",
                        http_status=status)
        # ---- 参数/模型类 ----
        if status == 404:
            return done(False, KEY_ERROR_ENDPOINT,
                        f"接口地址不存在（HTTP 404）：{detail}",
                        hint=f"请检查 Base URL 是否正确（当前 {test_base}），"
                             "应指向 OpenAI 兼容端点，通常以 /v1 结尾",
                        http_status=status)
        if status in (400, 422):
            # 【需求点 Bug1 · 修复】平台因"参数被锁定"拒绝（如 Kimi k2.6 的 invalid temperature）
            #   → 单独归类并给出约束清单提示，避免被误判成模型标识不可用
            if _looks_like_invalid_temperature(detail):
                return done(False, KEY_ERROR_INVALID,
                            f"平台拒绝该参数取值（HTTP {status}）：{detail}",
                            hint=("该模型对调用参数有强制约束，请查看模型参数约束清单"
                                  "（Kimi k2.6 强制 temperature=1）；"
                                  "系统运行期已自动固定该参数，如仍失败请反馈维护者"),
                            http_status=status, extra=constraint_extra)
            # 【需求点 一、Bug1】平台返回"模型名不受支持"时归类为模型不可用并给出清单
            if "model" in lowered or _looks_like_model_not_found(detail, test_model):
                return done(False, KEY_ERROR_MODEL_NOT_FOUND,
                            f"模型标识不被平台接受（HTTP {status}）：{detail}",
                            hint=f"请在「模型名」中填写该平台实际可用的模型标识，当前填写：{test_model}。"
                                 + PROVIDER_MODEL_DOC_HINT.get(provider, ""),
                            http_status=status,
                            extra={"known_models": model_check["known_models"], **constraint_extra})
            return done(False, KEY_ERROR_INVALID,
                        f"请求被平台拒绝（HTTP {status}）：{detail}",
                        hint="密钥可能有效但参数不被接受，请核对模型名与接口地址",
                        http_status=status, extra=constraint_extra)
        # ---- 服务端类 ----
        if status >= 500:
            return done(False, KEY_ERROR_SERVER,
                        f"平台服务端错误（HTTP {status}）：{detail}",
                        hint="密钥未必有问题，请稍后重试",
                        http_status=status)

        return done(False, KEY_ERROR_UNKNOWN,
                    f"接口返回异常状态（HTTP {status}）：{detail}",
                    hint="请核对接口地址与模型标识", http_status=status)

    # ==================================================================
    # 降级状态（供前端提示）
    # ==================================================================
    def degradation_status(self) -> dict:
        return {
            "dispatch_degraded": self.dispatch_degraded,
            "dispatch_note": "当前为备选调度模型" if self.dispatch_degraded else "",
            "dispatch_reason": self.dispatch_degrade_reason,
            "disabled_agents": [
                {
                    "agent": role,
                    "model_name": AGENT_BINDINGS[role]["model_name"],
                    "reason": (record or {}).get("reason", "") if isinstance(record, dict) else str(record),
                    "code": (record or {}).get("code", "") if isinstance(record, dict) else "",
                    # 【BUG-A 4】这是"重试冷却"而不是永久禁用：冷却结束自动恢复专属模型
                    "cooldown_seconds": self.FAILURE_COOLDOWN_SECONDS,
                    "retry_at": ((record or {}).get("at", 0) + self.FAILURE_COOLDOWN_SECONDS
                                 if isinstance(record, dict) else 0),
                    "note": (f"{AGENT_BINDINGS[role]['model_name']} 上次调用失败，"
                             f"处于 {self.FAILURE_COOLDOWN_SECONDS:.0f}s 重试冷却中"
                             "（配置存在时不会无故生态位补位）"),
                }
                for role, record in self.disabled_agents.items()
            ],
            # 【需求点 三】生态位补位状态
            "ecosystem": self.ecosystem_status(),
            # 【BUG-A 2/5】连通测试 ↔ 真实调用 一致性核对表
            "consistency": self.consistency_report(),
        }
