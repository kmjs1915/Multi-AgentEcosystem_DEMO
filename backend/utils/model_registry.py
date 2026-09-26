# -*- coding: utf-8 -*-
"""
模型标识校验与平台适配规则（工具层）

【需求点 一、Bug1 规则3】保存模型配置时提前校验模型标识，
避免传入平台不支持的名称（如 deepseek-v4.1-flash → 400 MODEL_NOT_FOUND）。

放在 utils 而非 services，是为了让 ConfigStore（服务层）与 ModelClient（服务层）
都能引用，且不产生循环依赖。

【需求点 Bug6 / BUG-NEW1 / BUG-NEW2】模型绑定与请求适配：
    系统已移除豆包（doubao），Kimi-Flash 零散配置项已收拢进 Kimi 厂商分组；
    所有 provider 统一使用 OpenAI 兼容的 /chat/completions 协议
    （DeepSeek / Qwen / Kimi K3 / Kimi k2.6 / GLM 均支持），请求体与错误体解析共用同一实现。
"""

from __future__ import annotations

import json
import re
from typing import Any

from backend.utils.constants import (
    KEY_ERROR_AUTH,
    KEY_ERROR_ENDPOINT,
    KEY_ERROR_INSUFFICIENT_BALANCE,
    KEY_ERROR_INVALID,
    KEY_ERROR_MODEL_NOT_FOUND,
    KEY_ERROR_RATE_LIMITED,
    KEY_ERROR_SERVER,
    KEY_ERROR_UNKNOWN,
    KNOWN_MODELS,
    MODEL_CONSTRAINTS,
    MODEL_CONSTRAINTS_PROVIDER_FALLBACK,
    PROVIDER_CAPABILITIES,
    PROVIDER_MODEL_DOC_HINT,
)

# 模型标识命名规范：字母数字开头，允许 . _ -
_MODEL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]{0,127}$")

# 【Bug1】约束适配元信息在请求体里的临时键名：调用方读取后必须剔除，绝不发给平台
CONSTRAINT_META_KEY = "_model_constraint"

# 【需求点 一、Bug1】已知不被平台支持的模型标识 → 保存时直接拒绝，并给出替代建议
RETIRED_MODEL_IDS: dict[str, str] = {
    "deepseek-v4.1-flash": (
        "DeepSeek 平台不支持 deepseek-v4.1-flash（原报错：The supported API model names are "
        "deepseek-flash, deepseek-v4-pro, but you passed deepseek-v4.1-flash）。"
        "请改用 deepseek-flash。"
    ),
    "deepseek-v4-flash": "DeepSeek 平台不支持该标识，请改用 deepseek-flash。",
    "deepseek-v4.1": "DeepSeek 平台不支持该标识，请改用 deepseek-flash 或 deepseek-v4-pro。",
    # 【需求点 Bug6】豆包模型已从系统移除
    "doubao-seed-1-6": "豆包模型已从本系统移除，交互交付Agent 现使用 Kimi k2.6（kimi-k2.6）。",
    "doubao-seed-2-1-turbo-260628": "豆包模型已从本系统移除，交互交付Agent 现使用 Kimi k2.6（kimi-k2.6）。",
    "doubao-pro-32k": "豆包模型已从本系统移除，交互交付Agent 现使用 Kimi k2.6（kimi-k2.6）。",
    "turbo-2.1": "豆包模型已从本系统移除，交互交付Agent 现使用 Kimi k2.6（kimi-k2.6）。",
    "turbo2.1": "豆包模型已从本系统移除，交互交付Agent 现使用 Kimi k2.6（kimi-k2.6）。",
}

# 【需求点 BUG-NEW2】仅提示、不拒绝的历史标识：平台可能仍可用，但已不作为本系统绑定模型。
#   保存时给出提示，引导用户改用新的固定绑定模型标识。
DEPRECATED_MODEL_HINTS: dict[str, str] = {
    "qwen3-coder-plus": "调度规划Agent 已由 Qwen Coder 改为 Qwen3.8-Max（qwen3.8-max），建议同步更新。",
    "qwen3-vl-plus": "视觉感知Agent 已由 Qwen-VL 改为 Qwen3.8-Flash（qwen3.8-flash），建议同步更新。",
    "kimi-flash": "Kimi-Flash 已收拢进 Kimi 厂商分组，交互交付Agent 现使用 kimi-k2.6。",
    "kimi-flash-8k": "Kimi-Flash 已收拢进 Kimi 厂商分组，交互交付Agent 现使用 kimi-k2.6。",
    "kimi-flash-32k": "Kimi-Flash 已收拢进 Kimi 厂商分组，交互交付Agent 现使用 kimi-k2.6。",
    "deepseek-v4-pro": "代码工程Agent 固定绑定 DeepSeek-Flash（deepseek-flash）。",
}


def validate_model_name(provider: str, model: str) -> dict:
    """校验模型标识（格式 + 本地登记清单）。

    返回：{"valid": bool, "level": "ok"|"warn"|"error", "message": str,
           "hint": str, "known_models": [...]}
    · valid=False（error）：明显非法 → 调用方应拒绝保存
    · level="warn"：不在本地登记清单内 → 允许保存但提示平台可能返回 400
    """
    meta_name = PROVIDER_CAPABILITIES.get(provider, {}).get("name", provider)
    known = list(KNOWN_MODELS.get(provider, ()))
    value = (model or "").strip()

    if not value:
        return {
            "valid": False, "level": "error",
            "message": f"{meta_name} 模型标识不能为空",
            "hint": f"请填写平台可用的模型标识，例如：{'、'.join(known[:3])}" if known else "请填写模型标识",
            "known_models": known,
        }

    if any(ch.isspace() for ch in value):
        return {
            "valid": False, "level": "error",
            "message": f"{meta_name} 模型标识不得包含空格：{value!r}",
            "hint": "模型标识通常是形如 xxx-yyy 的短横线命名，请去除空格后重试",
            "known_models": known,
        }

    if not _MODEL_ID_RE.match(value):
        return {
            "valid": False, "level": "error",
            "message": f"{meta_name} 模型标识格式不合法：{value!r}",
            "hint": "模型标识只允许字母、数字、点、下划线与短横线（例如 deepseek-flash）",
            "known_models": known,
        }

    if value in known:
        return {
            "valid": True, "level": "ok",
            "message": f"{meta_name} 模型标识 {value} 合法（本地登记清单校验通过）",
            "hint": "", "known_models": known,
        }

    # 【需求点 一、Bug1】已知会被平台拒绝的历史错误标识 → 直接判为 error，防止再次保存回去
    if value in RETIRED_MODEL_IDS:
        return {
            "valid": False, "level": "error",
            "message": f"{meta_name} 模型标识 {value} 已被平台弃用/不支持",
            "hint": RETIRED_MODEL_IDS[value],
            "known_models": known,
        }

    # 【需求点 BUG-NEW2】已弃用的绑定标识（平台可能仍可用）→ 允许保存但给出替换建议
    if value in DEPRECATED_MODEL_HINTS:
        return {
            "valid": True, "level": "warn",
            "message": f"{meta_name} 模型标识 {value} 已不是本系统推荐绑定模型",
            "hint": DEPRECATED_MODEL_HINTS[value],
            "known_models": known,
        }

    return {
        "valid": True, "level": "warn",
        "message": f"{meta_name} 模型标识 {value} 不在本地登记清单内，平台可能返回 400 MODEL_NOT_FOUND",
        "hint": (PROVIDER_MODEL_DOC_HINT.get(provider, f"请确认该平台支持 {value}")
                 + (f"；本地登记清单：{'、'.join(known)}" if known else "")),
        "known_models": known,
    }


def looks_like_model_not_found(detail: str, model: str) -> bool:
    """判断错误响应是否属于"模型标识不被支持"。

    典型原文（DeepSeek 400）：
      The supported API model names are deepseek-flash, deepseek-v4-pro,
      but you passed deepseek-v4.1-flash
    """
    lowered = (detail or "").lower()
    if model and model.lower() in lowered and any(
        k in lowered for k in ("not found", "does not exist", "unsupported", "invalid model",
                               "no such model", "model_not_found", "supported api model")
    ):
        return True
    return any(k in lowered for k in (
        "model_not_found", "the supported api model names", "unsupported model",
        "model does not exist", "no such model", "model not found",
    ))


# ==========================================================================
# 统一聊天请求体构造（所有 provider 共用 OpenAI 兼容协议）
# 【需求点 Bug6 / BUG-NEW1】原豆包（方舟）专用请求体构造已随豆包移除而删除；
#   Kimi k2.6 / kimi-k3 / Kimi-Flash 历史标识同属 Moonshot OpenAI 兼容接口，
#   与 DeepSeek / Qwen / GLM 一样使用统一构造逻辑。
# ==========================================================================
def _deprefix_model(model: str) -> str:
    """把 agent_role 形式或带厂商前缀形式收敛为模型标识。

    调用方可能传 'kimi-k2.6'、'kimi/kimi-k2.6' 或 '调度规划Agent'，
    统一取最后一段并按小写比较，保证约束匹配不受写法影响。
    """
    value = str(model or "").strip()
    if "/" in value:
        value = value.rsplit("/", 1)[-1].strip()
    return value


def resolve_model_constraints(model: str, provider: str = "",
                              provider_default_model: str = "") -> dict | None:
    """【Bug1】解析该模型的参数约束（未命中返回 None）。

    匹配顺序（详见 constants.MODEL_CONSTRAINTS 注释）：
      1) 精确模型标识（kimi-k2.6）
      2) 带厂商前缀 / agent_role 写法（kimi/kimi-k2.6）
      3) 同义标识别名（alias_of 递归一层）
      4) 厂商主模型兜底（用户在设置页把该厂商模型改成别的写法时，约束不丢失）
    """
    candidates: list[str] = []
    key = _deprefix_model(model)
    if key:
        candidates.append(key.lower())

    # 别名 / 精确命中
    lookup = {k.lower(): v for k, v in MODEL_CONSTRAINTS.items()}
    for name in list(candidates):
        entry = lookup.get(name)
        if entry is None:
            continue
        alias = str(entry.get("alias_of") or "").strip()
        if alias:
            entry = lookup.get(_deprefix_model(alias).lower()) or {}
        if entry.get("forced_params") or entry.get("fixed_params"):
            return entry

    # 厂商主模型兜底：仅当该模型标识**不在本地登记清单**时启用
    #   （用户把该厂商模型改成自造写法时，约束不丢失）；
    #   同厂商下的其它**已知模型**（如 Kimi 的 kimi-k3）绝不误套 kimi-k2.6 的约束。
    provider_key = str(provider or "").strip().lower()
    if key and key in {str(m).lower() for m in KNOWN_MODELS.get(provider_key, ())}:
        return None
    fallback_model = (MODEL_CONSTRAINTS_PROVIDER_FALLBACK.get(provider_key)
                      or provider_default_model or "")
    fallback_key = _deprefix_model(fallback_model).lower()
    if not key and not fallback_key:
        return None
    if fallback_key:
        entry = lookup.get(fallback_key)
        if entry is not None and (entry.get("forced_params") or entry.get("fixed_params")):
            return entry
    return None


def apply_model_constraints(model: str, params: dict[str, Any], *, provider: str = "",
                            provider_default_model: str = "") -> tuple[dict[str, Any], dict]:
    """【Bug1】按模型参数约束清单适配请求参数。

    返回 (适配后的参数副本, 约束说明)：
      · 命中约束 → 强制写入 forced_params（覆盖全局 temperature 等配置），
        并按 fixed_params 剔除业务侧传入的同名参数（平台锁定参数不下发）；
      · 未命中 → 原样返回参数副本，约束说明为空 dict。
    约束说明结构：{"model","matched","locked_reason","forced","removed","source"}
    """
    adapted = dict(params or {})
    info: dict[str, Any] = {"model": _deprefix_model(model), "matched": False}
    entry = resolve_model_constraints(
        model, provider=provider, provider_default_model=provider_default_model)
    if not entry:
        return adapted, info

    forced = dict(entry.get("forced_params") or {})
    fixed = tuple(entry.get("fixed_params") or ())
    removed: list[str] = []
    for name in fixed:
        # 平台锁定参数：业务侧传什么都无效，先剔除再由 forced 统一写回
        if name in adapted:
            adapted.pop(name, None)
            removed.append(str(name))
    for name, value in forced.items():
        adapted[str(name)] = value

    info.update({
        "matched": True,
        "provider": entry.get("provider", ""),
        "locked_reason": entry.get("locked_reason", ""),
        "forced": {k: v for k, v in forced.items()},
        "removed": removed,
        "source": "constants.MODEL_CONSTRAINTS",
    })
    return adapted, info


def build_chat_body(model: str, messages: list[dict[str, Any]], *,
                    temperature: float | None = None,
                    max_tokens: int | None = None,
                    expect_json: bool = False,
                    provider: str = "",
                    provider_default_model: str = "") -> dict[str, Any]:
    """构造 OpenAI 兼容的 /chat/completions 请求体。

    · model / messages 为核心字段；
    · 多模态 content 数组（image_url + text）原样透传；
    · temperature / max_tokens / response_format 仅在显式传入时附带；
    · 【Bug1】构造完成后统一过一遍「模型参数约束清单」：
      命中约束的模型（如 Kimi k2.6）强制固定参数，忽略全局 temperature 配置；
      未命中的模型保持原样，继续沿用系统温度配置。
    """
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
    }
    if temperature is not None:
        body["temperature"] = temperature
    if max_tokens:
        body["max_tokens"] = max_tokens
    if expect_json:
        body["response_format"] = {"type": "json_object"}

    adapted, info = apply_model_constraints(
        model, {k: v for k, v in body.items() if k in ("temperature", "max_tokens")},
        provider=provider, provider_default_model=provider_default_model,
    )
    if info.get("matched"):
        changed = False
        # 只回写被约束管理的参数，messages / model / response_format 原样保留
        for name in ("temperature", "max_tokens"):
            if name in adapted:
                # 约束要求的取值与当前请求体不一致（含"原本没带这个参数"）→ 视为发生改动
                if name not in body or body.get(name) != adapted[name]:
                    changed = True
                body[name] = adapted[name]
            elif name in body and name in (info.get("removed") or []):
                body.pop(name, None)
                changed = True
        if changed:
            # 仅当约束**真的改变了请求体**时才带出元信息（不污染未受约束的调用）
            body[CONSTRAINT_META_KEY] = info
    return body


def model_constraint_note(model: str, provider: str = "",
                          provider_default_model: str = "") -> str:
    """【Bug1】该模型的强制参数说明文本（未命中返回空串）。"""
    entry = resolve_model_constraints(
        model, provider=provider, provider_default_model=provider_default_model)
    if not entry:
        return ""
    forced = entry.get("forced_params") or {}
    if not forced:
        return ""
    params = "、".join(f"{k}={v}" for k, v in forced.items())
    return f"{_deprefix_model(model)} 平台强制参数：{params}（已自动固定，忽略全局配置）"


__all__ = [
    "RETIRED_MODEL_IDS", "DEPRECATED_MODEL_HINTS", "CONSTRAINT_META_KEY",
    "validate_model_name", "looks_like_model_not_found",
    "looks_like_invalid_parameter", "looks_like_invalid_temperature",
    "resolve_model_constraints", "apply_model_constraints", "model_constraint_note",
    "build_chat_body", "parse_provider_error", "normalize_chat_content",
    "looks_like_insufficient_balance",
]


# ==========================================================================
# 【需求点 Bug1 · 修复】平台"参数被锁定"类错误识别
#   典型原文（Kimi k2.6 400）：
#     {"error":{"message":"invalid temperature: only 1 is allowed for this model",
#               "type":"invalid_request_error"}}
#   识别出来的意义：把这类错误与"模型标识不可用"区分开，给出正确的排查方向
#   （模型参数约束清单 / 强制固定参数），而不是误导用户去改模型名。
# ==========================================================================
_INVALID_PARAM_KEYS = ("temperature", "top_p", "top_k", "presence_penalty",
                       "frequency_penalty", "n", "max_tokens")


def looks_like_invalid_parameter(detail: str, param: str = "") -> bool:
    """判断错误响应是否属于"该模型不接受此参数取值"。"""
    lowered = (detail or "").lower()
    if not lowered:
        return False
    keys = (param.lower(),) if param else _INVALID_PARAM_KEYS
    if not any(k in lowered for k in keys if k):
        return False
    return any(marker in lowered for marker in (
        "invalid ", "only ", "is allowed", "not allowed", "must be", "unsupported value",
        "out of range", "should be", "parameter", "invalid_request_error",
    ))


def looks_like_invalid_temperature(detail: str) -> bool:
    """是否属于"temperature 取值被模型锁定"（Kimi k2.6 → only 1 is allowed）。"""
    return looks_like_invalid_parameter(detail, "temperature")


# ==========================================================================
# 【GLM 1113 · 修复】余额不足识别（不可重试）
#   GLM（智谱）平台返回：HTTP 429 + {"error":{"code":"1113",
#     "message":"Insufficient balance, please recharge"}}。
#   危险点：GLM 把余额不足伪装成 429，会被"限流指数退避重试"无意义地重试 3 次；
#   必须在进入 429 重试分支之前先识别出来，立即失败并提示充值。
# ==========================================================================
_INSUFFICIENT_BALANCE_CODE = "1113"
_INSUFFICIENT_BALANCE_MARKERS = (
    "insufficient balance", "insufficient_quota",
    "余额不足", "欠费", "please recharge", "arithmetic",
)


def looks_like_insufficient_balance(detail: str) -> bool:
    """是否属于"账户余额不足"（GLM 错误码 1113 等）。"""
    lowered = (detail or "").lower()
    if not lowered:
        return False
    # 错误码 1113：可能出现在 error.code 字段或 message 原文中
    if _INSUFFICIENT_BALANCE_CODE in _extract_error_codes(detail):
        return True
    return any(marker in lowered for marker in _INSUFFICIENT_BALANCE_MARKERS)


def _extract_error_codes(detail: str) -> tuple[str, ...]:
    """从错误响应原文中取出结构化错误码（error.code / error.type / 顶层 code）。"""
    codes: list[str] = []
    try:
        payload = json.loads(detail)
    except (ValueError, TypeError):
        return ()
    if not isinstance(payload, dict):
        return ()
    err = payload.get("error")
    if isinstance(err, dict):
        for key in ("code", "type"):
            if err.get(key) is not None:
                codes.append(str(err[key]).strip())
    if payload.get("code") is not None:
        codes.append(str(payload["code"]).strip())
    return tuple(codes)


def parse_provider_error(status: int, detail: str, model: str, *,
                         provider_name: str = "模型平台",
                         base_url_hint: str = "") -> tuple[str, str, str]:
    """统一解析各 provider 的错误响应，返回 (error_code, 中文消息, 排查建议)。

    兼容 OpenAI 风格错误体：
      {"error": {"code": "AuthenticationError", "message": "..."}}
      {"error": {"code": "ModelNotFound", "message": "The model `x` does not exist"}}
      {"error": {"type": "invalid_request_error", "message": "..."}}
    """
    code_text = ""
    message_text = detail
    try:
        payload = json.loads(detail)
        err = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(err, dict):
            code_text = str(err.get("code") or err.get("type") or "")
            message_text = str(err.get("message") or detail)
        elif isinstance(payload, dict) and payload.get("message"):
            message_text = str(payload["message"])
    except (ValueError, TypeError):
        pass

    lowered = f"{code_text} {message_text}".lower()
    if status in (401, 403) or "authentication" in lowered or "api key" in lowered \
            or "unauthorized" in lowered or "invalid key" in lowered:
        return (
            KEY_ERROR_AUTH,
            f"{provider_name} 鉴权失败（HTTP {status}）：{message_text}",
            f"请核对 {provider_name} 的 API Key 是否复制完整、是否已启用或过期。",
        )
    # 【GLM 1113】余额不足：先于 429 限流分支判断（GLM 余额不足也返回 429，不能重试）
    if looks_like_insufficient_balance(detail):
        return (
            KEY_ERROR_INSUFFICIENT_BALANCE,
            f"{provider_name} 账户余额不足（HTTP {status}，错误码 1113）：{message_text}",
            f"密钥有效但 {provider_name} 账户欠费，重试无意义，请先充值后再试。",
        )
    if ("model" in lowered and any(k in lowered for k in (
            "not found", "does not exist", "notfound", "invalid", "unsupported"))) \
            or looks_like_model_not_found(detail, model):
        return (
            KEY_ERROR_MODEL_NOT_FOUND,
            f"{provider_name} 模型不可用（HTTP {status}）：{message_text}",
            f"请确认 {provider_name} 平台已开通模型「{model}」并填写正确标识。",
        )
    if status == 429 or "rate" in lowered or "quota" in lowered or "limit" in lowered:
        return (
            KEY_ERROR_RATE_LIMITED,
            f"{provider_name} 触发限流或配额不足（HTTP {status}）：{message_text}",
            "密钥有效；正式调用时系统会自动指数退避重试 3 次，请留意账号配额。",
        )
    if status == 404:
        return (
            KEY_ERROR_ENDPOINT,
            f"{provider_name} 接口地址不存在（HTTP 404）：{message_text}",
            base_url_hint or "请确认 Base URL 指向 OpenAI 兼容端点。",
        )
    if status in (400, 422):
        return (
            KEY_ERROR_INVALID,
            f"{provider_name} 请求被拒绝（HTTP {status}）：{message_text}",
            "密钥可能有效但请求参数不被接受，请核对 model 标识与 Base URL。",
        )
    if status >= 500:
        return (
            KEY_ERROR_SERVER,
            f"{provider_name} 平台服务端错误（HTTP {status}）：{message_text}",
            "密钥未必有问题，请稍后重试。",
        )
    return (
        KEY_ERROR_UNKNOWN,
        f"{provider_name} 接口返回异常状态（HTTP {status}）：{message_text}",
        "请核对 Base URL 与模型标识。",
    )


def normalize_chat_content(content: Any) -> str:
    """兼容各平台返回体：content 可能是字符串，也可能是分片数组。"""
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                parts.append(str(part.get("text") or ""))
            else:
                parts.append(str(part))
        return "".join(parts)
    return str(content or "")
