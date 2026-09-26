# -*- coding: utf-8 -*-
"""
配置管理（服务层 / 基础设施层交界）

架构文档来源：
  - 第6章 6.1 API Key 安全存储规则（AES-256 加密 + SHA256 篡改校验 + 永不暴露前端 + 测试超时5秒）
  - 第6章 6.3 局域网访问安全（bcrypt 密码；高危审批开关永久开启）
  - 第7章 7.4 首次启动强制流程（无配置文件 → 强制弹 API 配置向导）
  - 第5章 裸机目录规范（config/ 配置文件目录：加密密钥、系统配置）
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from backend.infrastructure.logger import get_logger, try_get_logger  # noqa: F401 (get_logger 供外部引用)
from backend.utils.constants import (
    AGENT_BINDINGS,
    CONFIG_SCHEMA_VERSION,
    CONFIG_STATE_KEY_MISSING,
    CONFIG_STATE_MODEL_EMPTY,
    CONFIG_STATE_NOT_TESTED,
    CONFIG_STATE_READY,
    DEFAULT_ADMIN_PASSWORD,
    DEFAULT_ADMIN_USERNAME,
    DEEPSEEK_DEFAULT_MODEL,
    DEEPSEEK_LEGACY_DEFAULT_MODELS,
    DOUBAO_LEGACY_DEFAULT_MODELS,   # 【Bug6】仅用于识别历史豆包残留
    ECOSYSTEM_FALLBACK_PRIORITY,
    ECOSYSTEM_FALLBACK_PRIORITY_TEXT,
    HIGH_RISK_APPROVAL_ALWAYS_ON,
    KIMI_FLASH_LEGACY_MODELS,       # 【BUG-NEW1】识别并清理历史 Kimi-Flash 残留
    KIMI_LEGACY_DEFAULT_MODELS,
    PROVIDER_DEEPSEEK,
    PROVIDER_DEFAULT_MODELS,
    PROVIDER_DOUBAO,                # 【Bug6】仅用于迁移时移除历史配置
    PROVIDER_KIMI_FLASH,            # 【BUG-NEW1】仅用于迁移时移除历史配置
    PROVIDERS,
    QWEN_DEFAULT_MODEL,
    QWEN_LEGACY_DEFAULT_MODELS,
    RETIRED_VENDOR_PROVIDERS,
    VENDOR_GROUP_BY_PROVIDER,
    VENDOR_GROUPS,
    WORKSPACE_KIND_LOCAL,
    WORKSPACE_MARKER_FILE,
    vendor_agents,
    vendor_models,
)
from backend.utils.model_registry import validate_model_name  # 模型标识校验（需求点 Bug1）
from backend.utils.paths import (
    EcosystemPaths,
    SecurityViolation,
    harden_file,
    is_system_critical_dir,
    normalize_path as _normalize_path,
)
from backend.utils.secrets import (
    SecretTampered,
    SealedSecret,
    fingerprint,
    load_or_create_master_key,
    mask_secret,
    open_secret,
    seal_secret,
    sha256_hex,
)
from backend.utils.security import hash_password, verify_password

# 扫描候选目录时跳过的系统/无关目录（含常见工具与隐藏目录）
_SKIP_SCAN_DIRS = {
    "windows", "system32", "syswow64", "winsxs", "program files", "program files (x86)",
    "programdata", "appdata", "$recycle.bin", "system volume information", "recovery",
    "node_modules", "site-packages", "assembly", "installer", "windowsapps",
    "perflogs", "msocache", "onedrivetemp", "intel", "amd", "nvidia",
    # 常见工具/隐藏目录：作为工作区没有意义
    ".git", ".svn", ".hg", ".vscode", ".idea", ".venv", "venv", ".cache", ".npm",
    ".gradle", ".m2", ".cargo", ".rustup", ".conda", ".ipynb_checkpoints", "__pycache__",
}

# 【需求点 Bug6 / Bug7 / Bug8 / BUG-NEW1 / BUG-NEW2】历史出厂默认模型标识 → 迁移到新的 Agent 绑定模型。
#   仅当配置里仍是这些"历史默认值"（说明用户没在设置页自定义过）时才对齐，
#   用户手动改过的模型标识一律保留，不做静默覆盖。
_LEGACY_PROVIDER_DEFAULTS: dict[str, tuple[str, ...]] = {
    "glm": ("glm-4.6", "glm-4-plus", "glm-4-flash", "chatglm_turbo"),
    "kimi": KIMI_LEGACY_DEFAULT_MODELS + KIMI_FLASH_LEGACY_MODELS,
    # 【BUG-NEW2】qwen3-coder-plus / qwen3-vl-plus 均属历史默认 → 对齐到 qwen3.8-max
    "qwen": QWEN_LEGACY_DEFAULT_MODELS + ("qwen-turbo",),
    "deepseek": DEEPSEEK_LEGACY_DEFAULT_MODELS,
    # 【BUG-NEW1 规则4】Kimi-Flash 已收拢进 Kimi 厂商，其历史标识一律不再保留
    "kimi_flash": KIMI_FLASH_LEGACY_MODELS,
}

# 【需求点 BUG-NEW2】厂商 Base URL 需要对齐到需求给定值的记录（仅当用户仍是旧默认值时改写）
_LEGACY_PROVIDER_BASE_URLS: dict[str, tuple[str, ...]] = {
    # 需求给定：https://api.deepseek.com（模型客户端会自动规范化为 .../v1/chat/completions）
    "deepseek": ("https://api.deepseek.com/v1", "https://api.deepseek.com/v1/"),
}


class ConfigStore:
    """系统配置与加密密钥中心。"""

    def __init__(self, paths: EcosystemPaths):
        self.paths = paths
        self.paths.ensure()
        self._lock = threading.RLock()
        self.master_key = load_or_create_master_key(self.paths.master_key_file)
        self._config: dict[str, Any] = {}
        self._sealed: dict[str, SealedSecret] = {}
        self._load()

    # ==================================================================
    # 加载 / 保存
    # ==================================================================
    def _load(self) -> None:
        file = self.paths.system_config_file
        if not file.exists():
            self._config = self._default_config()
            self._sealed = {}
            self._save()
            self._ensure_auth()
            return

        try:
            raw = json.loads(file.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            # 配置文件损坏 → 不得静默重置，记录后按默认重建
            _log_exception(
                error_code="CONFIG_CORRUPT", message=f"系统配置读取失败：{exc}",
            )
            raw = {}

        self._config = {**self._default_config(), **{k: v for k, v in raw.items() if k != "api_keys"}}
        self._sealed = {}
        for provider, payload in (raw.get("api_keys") or {}).items():
            try:
                self._sealed[provider] = SealedSecret.from_dict(payload)
            except SecretTampered as exc:
                _log_exception(
                    error_code="SECRET_TAMPERED",
                    message=f"密钥信封损坏 provider={provider}: {exc}",
                )
        self._migrate_config()
        self._ensure_auth()

    def _migrate_config(self) -> None:
        """历史配置平滑迁移（幂等）。

        【需求点 Bug1】DeepSeek 历史错误模型标识（deepseek-v4.1-flash，会返回 400 MODEL_NOT_FOUND）
        自动修正为 deepseek-flash。
        【需求点 Bug6】豆包（doubao）已从系统移除：删除历史 provider 段与密钥信封。
        【需求点 BUG-NEW1 规则4】Kimi-Flash 零散配置项下线：
          · 删除历史配置中的 kimi_flash provider 段与密钥信封（其能力已收拢到 Kimi 厂商分组，
            Kimi 分组服务 kimi-k3 + kimi-k2.6，使用同一把 Moonshot 密钥）；
          · 若历史 kimi_flash 密钥存在而 kimi 尚未配置，则把密文**搬迁到 kimi**（仅搬迁密文，不落明文）。
        【需求点 BUG-NEW2】模型绑定映射更新后的标识对齐：
          · Qwen 历史默认（qwen3-coder-plus / qwen3-vl-plus）→ qwen3.8-max（厂商主模型）；
          · DeepSeek Base URL 旧默认（.../v1）→ 需求给定值 https://api.deepseek.com。
        【需求点 三、4】补位开关：历史配置缺字段时补写为默认开启。
        【需求点 二】兼容历史会话数据（工作区归组在 Database._migrate 完成）。
        """
        changed: list[str] = []
        providers_cfg = self._config.setdefault("providers", {})

        # ---- DeepSeek：修正平台不支持的模型标识 + Base URL 对齐 ----
        deepseek = providers_cfg.setdefault(PROVIDER_DEEPSEEK, {})
        ds_current = (deepseek.get("model") or "").strip()
        if ds_current in DEEPSEEK_LEGACY_DEFAULT_MODELS or ds_current == "":
            deepseek["model"] = DEEPSEEK_DEFAULT_MODEL
            changed.append(f"DeepSeek 模型标识 {ds_current or '(空)'} -> {DEEPSEEK_DEFAULT_MODEL}")
        ds_base = str(deepseek.get("base_url") or "").strip()
        if ds_base in _LEGACY_PROVIDER_BASE_URLS.get(PROVIDER_DEEPSEEK, ()):
            new_base = next(p["default_base_url"] for p in PROVIDERS
                            if p["provider"] == PROVIDER_DEEPSEEK)
            deepseek["base_url"] = new_base
            changed.append(f"DeepSeek Base URL {ds_base} -> {new_base}")

        # ---- 【需求点 BUG-NEW1 规则4】Kimi-Flash：先从旧信封搬到 kimi（如可用），再彻底删除 ----
        if PROVIDER_KIMI_FLASH in self._sealed:
            if "kimi" not in self._sealed:
                self._sealed["kimi"] = self._sealed.pop(PROVIDER_KIMI_FLASH)
                changed.append("Kimi-Flash 密钥已迁移到 Kimi 厂商分组（同一把 Moonshot 密钥）")
            else:
                self._sealed.pop(PROVIDER_KIMI_FLASH, None)
                changed.append("移除历史 Kimi-Flash 密钥信封（Kimi 厂商已单独配置，共用同一密钥）")

        # ---- 彻底清理已下线厂商的配置段（豆包 / Kimi-Flash） ----
        for retired in RETIRED_VENDOR_PROVIDERS:
            if retired in providers_cfg:
                providers_cfg.pop(retired, None)
                changed.append(f"移除已下线厂商配置段：{retired}")
            self._sealed.pop(retired, None)

        if "ecosystem_fallback_enabled" not in self._config:
            self._config["ecosystem_fallback_enabled"] = True
            changed.append("新增字段 ecosystem_fallback_enabled=True（默认开启）")

        # ---- 【需求点 Bug6 / Bug7 / BUG-NEW1 / BUG-NEW2】厂商分组配置迁移 ----
        #   · 确保每个厂商都登记了「Agent 绑定表里用到的全部模型标识」；
        #   · 历史默认清单（qwen3-coder-plus / qwen3-vl-plus / glm-4.6 …）自动对齐到新绑定；
        #   · 厂商底座清单只保留需求给定的模型标识（Deepseek 单模型、Qwen3.8 双模型 …）。
        for group in VENDOR_GROUPS:
            provider = group["provider"]
            info = providers_cfg.setdefault(provider, {})
            default_model = PROVIDER_DEFAULT_MODELS.get(provider, "")
            current = str(info.get("model") or "").strip()
            legacy_defaults = _LEGACY_PROVIDER_DEFAULTS.get(provider, ())
            if current in legacy_defaults or current == "":
                info["model"] = default_model
                changed.append(f"{group['title']} 主模型标识 {current or '(空)'} -> {default_model}")
            declared = [m["model"] for m in vendor_models(provider)]
            bound = [b["model"] for b in AGENT_BINDINGS.values() if b["provider"] == provider]
            custom = [str(m).strip() for m in (info.get("custom_models") or []) if str(m).strip()]
            merged: list[str] = []
            for mid in declared + bound + custom:
                if mid and mid != info["model"] and mid not in merged:
                    merged.append(mid)
            # 【BUG-NEW2】清理已弃用的历史模型标识（qwen3-coder-plus / qwen3-vl-plus /
            #   deepseek-v4-pro 等），避免它们继续出现在设置面板的“模型名”输入项里
            retired_models = set(QWEN_LEGACY_DEFAULT_MODELS) | set(DEEPSEEK_LEGACY_DEFAULT_MODELS) \
                | set(KIMI_FLASH_LEGACY_MODELS) | set(DOUBAO_LEGACY_DEFAULT_MODELS) \
                | {"deepseek-v4-pro", "glm-4.6", "glm-4.6-flash", "glm-4-plus", "glm-4-flash"}
            merged = [m for m in merged if m not in retired_models]
            if merged != custom:
                info["custom_models"] = merged
                changed.append(f"{group['title']} 模型标识清单对齐 -> {info['model']} + {merged}")

        if "workspace_folders" not in self._config:
            self._config["workspace_folders"] = []
            self._config["active_workspace_folder_id"] = ""
            changed.append("新增字段 workspace_folders（工作区文件夹列表）")

        if self._config.get("version") != CONFIG_SCHEMA_VERSION:
            self._config["version"] = CONFIG_SCHEMA_VERSION
            changed.append(f"配置版本 -> {CONFIG_SCHEMA_VERSION}")

        if changed:
            self._save()
            logger = try_get_logger()
            if logger is not None:
                logger.info("历史配置已迁移：" + "；".join(changed), agent_role="system")

    def _default_config(self) -> dict[str, Any]:
        return {
            "version": "1.2",
            "created_at": time.time(),
            "first_launch_completed": False,
            "providers": {p["provider"]: {
                "name": p["name"],
                "base_url": p["default_base_url"],
                "model": p["default_model"],
                "enabled": False,
                "last_test_ok": False,
                "last_test_at": None,
            } for p in PROVIDERS},
            "background": {"type": "preset", "value": "night", "url": ""},
            # 第6章 6.3 规则4：高危审批开关永久强制开启，不可关闭
            "high_risk_approval_enabled": HIGH_RISK_APPROVAL_ALWAYS_ON,
            # 【需求点 三、4】模型生态位自动补位开关，默认开启，用户可手动关闭
            "ecosystem_fallback_enabled": True,
            # 【需求点 二、1】工作区文件夹列表（本地文件夹工作区，持久化到系统配置文件）
            "workspace_folders": [],
            "active_workspace_folder_id": "",
            "admin": {"username": DEFAULT_ADMIN_USERNAME},
        }

    def _save(self) -> None:
        self._config["high_risk_approval_enabled"] = HIGH_RISK_APPROVAL_ALWAYS_ON
        payload = dict(self._config)
        payload["api_keys"] = {k: v.to_dict() for k, v in self._sealed.items()}
        file = self.paths.system_config_file
        tmp = file.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, file)
        harden_file(file)

    # ==================================================================
    # 后台登录口令（第6章 6.3 规则2：bcrypt 哈希存储，禁止明文）
    # ==================================================================
    def _ensure_auth(self) -> None:
        auth_file = self.paths.auth_file
        if auth_file.exists():
            return
        data = {
            "username": DEFAULT_ADMIN_USERNAME,
            "password_bcrypt": hash_password(DEFAULT_ADMIN_PASSWORD),
            "created_at": time.time(),
            "must_change": True,
        }
        auth_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        harden_file(auth_file)
        _log_info(
            "后台登录口令已初始化（bcrypt 哈希存储，初始口令需尽快修改）",
            agent_role="system",
        )

    def _read_auth(self) -> dict:
        try:
            return json.loads(self.paths.auth_file.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def verify_admin(self, username: str, password: str) -> bool:
        auth = self._read_auth()
        if not username or username != auth.get("username"):
            return False
        return verify_password(password, auth.get("password_bcrypt", ""))

    def change_admin_password(self, old_password: str, new_password: str) -> tuple[bool, str]:
        if not self.verify_admin(self._config["admin"]["username"], old_password):
            return False, "原密码校验失败"
        if len(new_password or "") < 8:
            return False, "新密码长度不得少于 8 位"
        auth = self._read_auth()
        auth["password_bcrypt"] = hash_password(new_password)
        auth["must_change"] = False
        auth["updated_at"] = time.time()
        self.paths.auth_file.write_text(json.dumps(auth, ensure_ascii=False, indent=2), encoding="utf-8")
        harden_file(self.paths.auth_file)
        return True, "密码已更新（bcrypt 哈希存储）"

    def admin_must_change(self) -> bool:
        return bool(self._read_auth().get("must_change"))

    # ==================================================================
    # API Key（第6章 6.1）
    # ==================================================================
    def get_api_key(self, provider: str) -> str:
        """仅供后端内部调用模型 API 时解密使用，任何情况下不得返回前端。"""
        sealed = self._sealed.get(provider)
        if not sealed:
            return ""
        try:
            return open_secret(self.master_key, sealed)
        except SecretTampered as exc:
            _log_exception(
                error_code="SECRET_TAMPERED", message=f"密钥解密失败 provider={provider}: {exc}",
            )
            return ""

    def set_api_key(self, provider: str, api_key: str) -> None:
        """AES-256 加密保存密钥。

        【需求点 Bug1 排查要求】保存过程不做任何静默失败：
          加密后立即回读校验（open_secret），只有确认能正确解密才落盘，
          落盘后再复读配置文件确认密文与摘要已持久化；任一步失败直接抛异常。
        """
        if provider not in {p["provider"] for p in PROVIDERS}:
            raise ValueError(f"未知模型提供方：{provider}")
        value = (api_key or "").strip()
        with self._lock:
            sealed = seal_secret(self.master_key, value, context=f"api_key:{provider}")
            # 回读校验：确保密文可解（防加密链路损坏导致静默保存失败）
            if value and open_secret(self.master_key, sealed) != value:
                raise SecretTampered(f"{provider} 密钥加密回读校验失败，已放弃保存")

            self._sealed[provider] = sealed
            info = self._config["providers"].setdefault(provider, {})
            info["enabled"] = bool(value)
            info["key_fingerprint"] = fingerprint(value)
            info["key_sha256"] = sha256_hex(value.encode("utf-8")) if value else ""
            self._save()

            # 落盘确认：从磁盘复读，确保密文/摘要确实写入且不含明文
            persisted = self._read_raw_config()
            stored = ((persisted.get("api_keys") or {}).get(provider) or {})
            if value:
                if stored.get("ciphertext") != sealed.ciphertext or stored.get("digest") != sealed.digest:
                    raise SecretTampered(f"{provider} 密钥落盘校验失败：配置文件未包含预期密文")
                if value in json.dumps(persisted, ensure_ascii=False):
                    raise SecretTampered(f"{provider} 密钥存在明文泄露风险，已中止保存")

    def _read_raw_config(self) -> dict:
        try:
            return json.loads(self.paths.system_config_file.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise SecretTampered(f"配置文件读取失败：{exc}") from exc

    def clear_api_key(self, provider: str) -> None:
        with self._lock:
            self._sealed.pop(provider, None)
            info = self._config["providers"].setdefault(provider, {})
            info["enabled"] = False
            info["key_fingerprint"] = ""
            info["key_sha256"] = ""
            self._save()

    def public_config(self) -> dict[str, Any]:
        """返回可安全暴露给前端的配置：只含"是否已配置"与脱敏指纹，绝无明文密钥。

        【需求点 二、Bug2 前后端状态同步】补位开关对外**统一提供嵌套结构**：
          `config.ecosystem_fallback = {enabled, priority, priority_text, tested_ok_providers}`
        历史问题：这里只返回扁平字段 `ecosystem_fallback_enabled`，
        而前端 `renderFallbackSwitch()` 读取 `cfg.ecosystem_fallback.enabled`，
        取到 undefined 后按"默认开启"渲染，导致**后端已关闭、前端开关仍显示开启**。
        现在同时输出嵌套对象 + 扁平字段 + `model_fallback_enable` 别名，彻底消除口径差异。
        """
        enabled = self.ecosystem_fallback_enabled()
        tested_ok = self.tested_ok_providers()
        providers = []
        for meta in PROVIDERS:
            pid = meta["provider"]
            info = self._config["providers"].get(pid, {})
            plain = self.get_api_key(pid)          # 只在内存中用于脱敏，绝不返回
            per_model = info.get("model_test_results") or {}
            model_rows = []
            for item in self.vendor_models(pid):
                mid = item["model"]
                outcome = per_model.get(mid) or {}
                model_rows.append({
                    "model": mid,
                    "label": item.get("label") or mid,
                    "is_primary": mid == (info.get("model") or meta["default_model"]),
                    "tested_ok": bool(outcome.get("ok")),
                    "tested_at": outcome.get("at"),
                })
            providers.append({
                "provider": pid,
                "name": meta["name"],
                "base_url": info.get("base_url") or meta["default_base_url"],
                "model": info.get("model") or meta["default_model"],
                # 【需求点 BUG-NEW1】厂商下需要配置的全部模型标识（逐条可测连通）
                "models": model_rows,
                "model_ids": [r["model"] for r in model_rows],
                "configured": bool(plain),
                "masked": mask_secret(plain),
                "key_fingerprint": info.get("key_fingerprint", ""),
                "last_test_ok": bool(info.get("last_test_ok")),
                "last_test_at": info.get("last_test_at"),
                "enabled": bool(info.get("enabled")),
            })
        # 【Bug2】唯一权威嵌套结构（设置面板开关直接读这里）
        ecosystem_fallback = {
            "enabled": enabled,
            "priority": list(ECOSYSTEM_FALLBACK_PRIORITY),
            "priority_text": ECOSYSTEM_FALLBACK_PRIORITY_TEXT,
            "tested_ok_providers": tested_ok,
            "label": "开启模型生态位自动补位",
            "description": (
                "专属模型不可用时，自动使用本机已连通测试通过的其他模型补齐 Agent 生态位；"
                f"补位优先级 {ECOSYSTEM_FALLBACK_PRIORITY_TEXT}。"
                "关闭后不执行跨模型补位，模型失败直接任务失败。"
            ),
        }
        return {
            "version": self._config.get("version"),
            "first_launch_completed": bool(self._config.get("first_launch_completed")),
            "providers": providers,
            # 【需求点 Bug6 / Bug7】厂商分组配置（一个厂商一次密钥 + 多个模型标识）
            "vendor_groups": self.vendor_groups(),
            # 【需求点 Bug8】七大 Agent → 厂商 → 具体模型标识（设置面板 / 状态栏同源）
            "agent_model_map": self.agent_model_map(),
            "background": self._config.get("background", {}),
            # 第6章 6.3 规则4：恒定 true，前端只读展示
            "high_risk_approval_enabled": True,
            "high_risk_approval_locked": True,
            # 【需求点 二、Bug2】补位开关：嵌套 + 扁平 + 别名三种口径同源同值
            "ecosystem_fallback": ecosystem_fallback,
            "ecosystem_fallback_enabled": enabled,
            "ecosystem_fallback_priority": list(ECOSYSTEM_FALLBACK_PRIORITY),
            "ecosystem_fallback_priority_text": ECOSYSTEM_FALLBACK_PRIORITY_TEXT,
            "model_fallback_enable": enabled,          # 前端约定的别名，语义等同
            "admin": {"username": self._config["admin"]["username"], "must_change_password": self.admin_must_change()},
            "security": {
                "key_storage": "AES-256-GCM + SHA256 篡改校验",
                "password_storage": "bcrypt",
                "master_key_file": str(self.paths.master_key_file),
                "session_isolation": True,
                "login_required": True,
            },
        }

    # ==================================================================
    # 【需求点 三、4】模型生态位自动补位开关
    # ==================================================================
    def ecosystem_fallback_enabled(self) -> bool:
        # 历史配置无该字段时默认视为开启（兼容旧数据）
        return bool(self._config.get("ecosystem_fallback_enabled", True))

    def set_ecosystem_fallback(self, enabled: bool) -> None:
        self._config["ecosystem_fallback_enabled"] = bool(enabled)
        self._save()
        _log_info(
            f"模型生态位自动补位已{'开启' if enabled else '关闭'}"
            f"（补位优先级 {ECOSYSTEM_FALLBACK_PRIORITY_TEXT}）",
            agent_role="system",
        )

    def tested_ok_providers(self) -> list[str]:
        """本机已通过连通性测试的模型提供方（生态位补位的候选来源）。"""
        out = []
        for meta in PROVIDERS:
            pid = meta["provider"]
            info = self._config["providers"].get(pid, {})
            if info.get("last_test_ok") and self.get_api_key(pid):
                out.append(pid)
        return out

    def provider_model_label(self, provider: str) -> str:
        """前端展示用模型名称，例如「通义千问 Qwen qwen3.8-max」。"""
        meta = self.get_provider_meta(provider)
        return f"{meta['name']} {meta['model']}".strip()

    # ==================================================================
    # 【BUG-A 1/2】密钥指纹 + 单模型连通测试状态（连通测试与真实调用同源核对）
    # ==================================================================
    def key_fingerprint(self, provider: str) -> str:
        """该厂商当前密钥指纹（SHA256 前 16 位）——**只用于核对，绝不含明文**。"""
        info = self._config["providers"].get(provider, {})
        return str(info.get("key_fingerprint") or "")

    def key_sha256(self, provider: str) -> str:
        """该厂商已保存密钥的完整 SHA256（供「连通测试 vs 真实调用」同源核对）。

        只返回摘要，**永远不含明文密钥**（第6章 6.1 规则3）。
        """
        info = self._config["providers"].get(provider, {})
        stored = str(info.get("key_sha256") or "")
        if stored:
            return stored
        # 历史配置缺字段时按密文可解出的密钥重新计算一次摘要（不落盘、不返回明文）
        plain = self.get_api_key(provider)
        return sha256_hex(plain.encode("utf-8")) if plain else ""

    def model_test_state(self, provider: str, model: str) -> str:
        """返回该「厂商 + 模型标识」的配置可用性等级（供补位闸门与前端核对）。

        ready      : 密钥已加密落盘 且 该模型连通测试通过
        key_missing: 该厂商密钥缺失（配置为空）
        model_empty: 模型标识为空
        not_tested : 密钥已配置但该模型未通过连通测试
        """
        model_id = str(model or "").strip()
        if not model_id:
            return CONFIG_STATE_MODEL_EMPTY
        if not self.get_api_key(provider):
            return CONFIG_STATE_KEY_MISSING
        info = self._config["providers"].get(provider, {})
        results = info.get("model_test_results") or {}
        outcome = results.get(model_id) or {}
        if outcome.get("ok"):
            return CONFIG_STATE_READY
        return CONFIG_STATE_NOT_TESTED

    def tested_ok_models(self, provider: str) -> list[str]:
        """该厂商下已通过连通测试的模型标识清单。"""
        info = self._config["providers"].get(provider, {})
        results = info.get("model_test_results") or {}
        return [mid for mid, row in results.items() if (row or {}).get("ok")]

    def update_provider_meta(self, provider: str, *, base_url: str | None = None,
                             model: str | None = None) -> dict:
        """更新 provider 的 base_url / model。

        【需求点 一、Bug1 规则3】保存模型配置时先做**模型标识校验**：
          · 格式非法（空/含空格/非法字符）→ 直接拒绝保存（SecurityViolation）
          · 不在本地登记清单内 → 允许保存但返回 warn 级告警，供前端提前提示
        """
        info = self._config["providers"].setdefault(provider, {})
        validation: dict = {"valid": True, "level": "ok", "message": "", "hint": ""}
        if base_url:
            clean_base = base_url.strip()
            if not clean_base.lower().startswith(("http://", "https://")):
                raise SecurityViolation(
                    f"接口地址必须以 http:// 或 https:// 开头：{clean_base}",
                    code="INVALID_BASE_URL",
                )
            info["base_url"] = clean_base
        if model:
            validation = validate_model_name(provider, model)
            if not validation["valid"]:
                # 提前拦截，避免正式调用时才出现 400 MODEL_NOT_FOUND
                raise SecurityViolation(
                    f"{validation['message']}。{validation['hint']}",
                    code="INVALID_MODEL_NAME",
                    detail={"provider": provider, "model": model,
                            "known_models": validation["known_models"]},
                )
            info["model"] = model.strip()
            if validation["level"] == "warn":
                _log_info(
                    f"[模型标识告警] {validation['message']}；{validation['hint']}",
                    agent_role="system",
                )
        self._save()
        return validation

    def validate_provider_model(self, provider: str, model: str | None = None) -> dict:
        """对外暴露模型标识校验（供接口层返回给前端提前提示）。"""
        target = model if model is not None else self.get_provider_meta(provider)["model"]
        return validate_model_name(provider, target)

    def mark_test_result(self, provider: str, ok: bool) -> None:
        info = self._config["providers"].setdefault(provider, {})
        info["last_test_ok"] = bool(ok)
        info["last_test_at"] = time.time()
        self._save()

    def get_provider_meta(self, provider: str) -> dict:
        """厂商当前生效配置（base_url / model / configured_model）。

        【BUG-A 2 关键修复】`configured_model` = **配置里真实保存的模型标识**（可能为空），
        而 `model` = 空时回退到出厂默认（用于展示与兜底）。
        历史实现只返回回退后的 `model`，导致"用户把模型清空"这类**配置为空**状态
        在运行期被悄悄替换成出厂默认模型：
          · 补位闸门永远判不出「配置为空」；
          · 真实调用会用用户从未配置过的默认模型 → 触发失败与无故补位。
        现在两者同时暴露，运行期判定配置状态一律使用 configured_model。
        """
        meta = next((p for p in PROVIDERS if p["provider"] == provider), None)
        if not meta:
            raise ValueError(f"未知模型提供方：{provider}")
        info = self._config["providers"].get(provider, {})
        configured_model = str(info.get("model") or "").strip()
        return {
            "provider": provider,
            "name": meta["name"],
            "base_url": info.get("base_url") or meta["default_base_url"],
            "model": configured_model or meta["default_model"],
            # ★ 配置里真实保存的模型标识（空 = 用户未配置 / 已清空）
            "configured_model": configured_model,
            "model_is_configured": bool(configured_model),
            "provider_default_model": meta["default_model"],
        }

    # ==================================================================
    # 【需求点 Bug6 / Bug7】厂商（Vendor）分组配置
    #   · 一个厂商只填写一次 API Key / Base URL；
    #   · 厂商内部映射多个模型标识（Qwen 含 Qwen-VL；Kimi 含 kimi-k3 / kimi-k2.6 …）；
    #   · 每个 Agent 通过 AGENT_BINDINGS 映射到本厂商下的具体 model_name；
    #   · 明文密钥永不进入返回值，前端只拿到 configured / masked / key_fingerprint。
    # ==================================================================
    def vendor_models(self, provider: str) -> list[dict]:
        """该厂商需要配置的模型标识列表（含厂商出厂默认清单，去重、保序）。"""
        declared = [dict(m) for m in vendor_models(provider)]
        seen = {m["model"] for m in declared}
        # 补齐用户历史自定义 / Agent 绑定表里出现但不在出厂清单内的标识，避免丢失
        for extra in self.get_configured_models(provider):
            if extra and extra not in seen:
                declared.append({"model": extra, "label": extra})
                seen.add(extra)
        for binding in AGENT_BINDINGS.values():
            if binding["provider"] == provider and binding["model"] not in seen:
                declared.append({"model": binding["model"], "label": binding["model_name"]})
                seen.add(binding["model"])
        return declared

    def get_configured_models(self, provider: str) -> list[str]:
        """该厂商已保存的自定义模型标识列表（跨重启持久化）。"""
        info = self._config["providers"].get(provider, {})
        custom = info.get("custom_models")
        if isinstance(custom, list):
            return [str(m).strip() for m in custom if str(m).strip()]
        # 历史配置兼容：单模型字段若不属于出厂默认清单，也视为自定义模型
        single = str(info.get("model") or "").strip()
        default_model = PROVIDER_DEFAULT_MODELS.get(provider, "")
        if single and single != default_model:
            return [single]
        return []

    def set_provider_models(self, provider: str, models: list[str]) -> dict:
        """保存厂商下的模型标识列表（除主模型外都落入 custom_models，供内部映射）。

        【需求点 一、Bug1 规则3】每个模型标识都先做本地校验，
        任一非法立即拒绝保存（SecurityViolation），避免正式调用才出现 400 MODEL_NOT_FOUND。
        """
        if provider not in {p["provider"] for p in PROVIDERS}:
            raise ValueError(f"未知模型提供方：{provider}")
        clean: list[str] = []
        validations: list[dict] = []
        for raw in models or []:
            value = str(raw or "").strip()
            if not value or value in clean:
                continue
            check = validate_model_name(provider, value)
            if not check["valid"]:
                raise SecurityViolation(
                    f"{check['message']}。{check['hint']}",
                    code="INVALID_MODEL_NAME",
                    detail={"provider": provider, "model": value,
                            "known_models": check["known_models"]},
                )
            validations.append(check)
            clean.append(value)

        info = self._config["providers"].setdefault(provider, {})
        default_model = PROVIDER_DEFAULT_MODELS.get(provider, "")
        # 主模型 = 列表首项（该厂商的默认调用目标）；其余进入 custom_models
        info["model"] = clean[0] if clean else default_model
        info["custom_models"] = [m for m in clean[1:]]
        # 保存每个模型标识的校验结论（warn 级保留给前端提示；error 级已在上面直接拒绝）
        info["model_validations"] = [
            {"model": model, "level": check["level"], "message": check["message"]}
            for model, check in zip(clean, validations)
        ]
        self._save()
        _log_info(
            f"厂商模型标识已保存：{provider} -> {', '.join(clean) or '(空)'}",
            agent_role="system",
        )
        return {
            "provider": provider,
            "models": clean,
            "warnings": [v["message"] for v in validations if v["level"] == "warn"],
        }

    def vendor_groups(self) -> list[dict]:
        """厂商分组配置视图（设置面板 / 向导直接消费；永远不含明文密钥）。

        每个厂商返回：
          provider / title / description / base_url / configured / masked / key_fingerprint
          bound_agents  该厂商下绑定的 Agent 与各自专属模型标识
          models        该厂商下需要配置的模型标识（含 label、绑定 Agent、连通状态）
        """
        groups: list[dict] = []
        for group in VENDOR_GROUPS:
            provider = group["provider"]
            meta = self.get_provider_meta(provider)
            plain = self.get_api_key(provider)          # 仅内存内用于脱敏
            info = self._config["providers"].get(provider, {})
            bound = vendor_agents(provider)
            bound_models = {a["model"] for a in bound}
            model_rows: list[dict] = []
            for item in self.vendor_models(provider):
                mid = item["model"]
                model_rows.append({
                    "model": mid,
                    "label": item.get("label") or mid,
                    "bound_agents": [a["agent"] for a in bound if a["model"] == mid],
                    "is_primary": mid == (info.get("model") or meta["model"]),
                    "validation": validate_model_name(provider, mid),
                })
            per_model = info.get("model_test_results") or {}
            for row in model_rows:
                outcome = per_model.get(row["model"]) or {}
                row["tested_ok"] = bool(outcome.get("ok"))
                row["tested_at"] = outcome.get("at")
                row["test_message"] = str(outcome.get("message") or "")
            groups.append({
                "vendor_id": group["vendor_id"],
                "provider": provider,
                "title": group["title"],
                "name": meta["name"],
                "description": group["description"],
                "base_url": meta["base_url"],
                "configured": bool(plain),
                "masked": mask_secret(plain),
                "key_fingerprint": info.get("key_fingerprint", ""),
                "key_sha256": info.get("key_sha256", ""),
                "last_test_ok": bool(info.get("last_test_ok")),
                "last_test_at": info.get("last_test_at"),
                "bound_agents": bound,
                "models": model_rows,
                "model_ids": [r["model"] for r in model_rows],
                "tested_models": [r["model"] for r in model_rows if r["tested_ok"]],
                "bound_models_not_in_list": sorted(bound_models - {r["model"] for r in model_rows}),
            })

        # 【需求点 BUG-NEW1 规则4】不再追加任何「历史遗留厂商」分组：
        #   Kimi-Flash 零散配置项已下线（能力收拢进 Kimi 厂商分组），
        #   因此设置弹窗只会渲染 VENDOR_GROUPS 中登记的 4 个厂商卡片。
        #   兜底自检：若 PROVIDERS 里出现未纳入分组的厂商，记录一条告警（不新增输入项），
        #   保证「一个厂商一次密钥」的界面口径永不被破坏。
        grouped = {g["provider"] for g in VENDOR_GROUPS}
        for meta in PROVIDERS:
            if meta["provider"] not in grouped:
                _log_info(
                    f"[厂商收敛] provider={meta['provider']} 未纳入厂商分组，"
                    "已按收拢策略并入同域厂商分组，不再单独展示配置项",
                    agent_role="system",
                )
        return groups

    def mark_model_test_result(self, provider: str, model: str, ok: bool, *, message: str = "") -> None:
        """记录「该厂商下某个模型」的连通性测试结果（每个模型分别测试）。"""
        info = self._config["providers"].setdefault(provider, {})
        results = info.setdefault("model_test_results", {})
        results[str(model)] = {"ok": bool(ok), "at": time.time(), "message": message[:400]}
        # 厂商级状态：只要该厂商下有一个模型连通通过，即视为该厂商可用
        if ok:
            info["last_test_ok"] = True
            info["last_test_at"] = time.time()
        elif not any(r.get("ok") for r in results.values()):
            info["last_test_ok"] = False
            info["last_test_at"] = time.time()
        self._save()

    def agent_model_map(self) -> list[dict]:
        """七大 Agent ↔ 厂商 ↔ 具体模型标识 的映射（后端唯一权威，供前端/BUG8 状态栏使用）。"""
        out: list[dict] = []
        for role, binding in AGENT_BINDINGS.items():
            out.append({
                "agent": role,
                "provider": binding["provider"],
                "vendor_title": VENDOR_GROUP_BY_PROVIDER.get(
                    binding["provider"], {}).get("title") or binding["provider"],
                "model_name": binding["model_name"],
                "model": binding["model"],
            })
        return out

    # ==================================================================
    # 【需求点 二、1】工作区文件夹（本地磁盘目录）注册与持久化
    # ==================================================================
    def list_workspace_folders(self) -> list[dict]:
        rows = self._config.get("workspace_folders") or []
        out: list[dict] = []
        for row in rows:
            path = Path(str(row.get("path") or ""))
            out.append({
                "folder_id": row.get("folder_id", ""),
                "name": row.get("name") or (path.name or str(path)),
                "path": str(path),
                "kind": row.get("kind") or WORKSPACE_KIND_LOCAL,
                "created_at": row.get("created_at"),
                # 目录可能被外部删除/移动，这里实时校验可用性
                "exists": path.is_dir(),
                "writable": os.access(str(path), os.W_OK) if path.is_dir() else False,
            })
        return out

    def get_workspace_folder(self, folder_id: str) -> dict | None:
        return next((f for f in self.list_workspace_folders() if f["folder_id"] == folder_id), None)

    def folder_by_path(self, path: str | os.PathLike) -> dict | None:
        target = _normalize_path(path)
        return next((f for f in self.list_workspace_folders()
                     if _normalize_path(f["path"]) == target), None)

    def register_workspace_folder(self, raw_path: str, *, name: str | None = None) -> dict:
        """登记一个本地文件夹为工作区（幂等）。

        安全校验：必须是**已存在的目录**；禁止登记系统关键目录与根盘符，
        防止把整块磁盘作为 Agent 操作根目录。
        """
        if not raw_path or not str(raw_path).strip():
            raise SecurityViolation("工作区目录路径不能为空", code="INVALID_WORKSPACE_PATH")
        path = Path(str(raw_path).strip().strip('"')).expanduser()
        try:
            path = path.resolve()
        except OSError as exc:
            raise SecurityViolation(f"工作区目录路径非法：{raw_path}（{exc}）",
                                    code="INVALID_WORKSPACE_PATH") from exc

        if not path.exists():
            raise SecurityViolation(f"工作区目录不存在：{path}", code="WORKSPACE_PATH_NOT_FOUND")
        if not path.is_dir():
            raise SecurityViolation(f"工作区必须是文件夹：{path}", code="WORKSPACE_PATH_NOT_DIR")
        if is_system_critical_dir(path):
            raise SecurityViolation(
                f"禁止将系统关键目录登记为工作区：{path}", code="WORKSPACE_PATH_FORBIDDEN",
            )
        if not os.access(str(path), os.W_OK):
            raise SecurityViolation(f"工作区目录不可写：{path}", code="WORKSPACE_PATH_NOT_WRITABLE")

        existing = self.folder_by_path(path)
        if existing:
            return existing

        folder = {
            "folder_id": "fld_" + uuid.uuid4().hex[:16],
            "name": (name or path.name or str(path)).strip(),
            "path": str(path),
            "kind": WORKSPACE_KIND_LOCAL,
            "created_at": time.time(),
        }
        self._config.setdefault("workspace_folders", []).append(folder)
        self._config["active_workspace_folder_id"] = folder["folder_id"]
        self._save()
        self._write_marker(path, folder)
        _log_info(
            f"工作区文件夹已加入并持久化：{folder['name']} -> {folder['path']}",
            agent_role="system",
        )
        return folder

    def set_active_workspace_folder(self, folder_id: str) -> dict:
        folder = self.get_workspace_folder(folder_id)
        if not folder:
            raise SecurityViolation(f"工作区文件夹不存在：{folder_id}", code="WORKSPACE_NOT_FOUND")
        if not folder["exists"]:
            raise SecurityViolation(
                f"工作区目录已不存在或不可访问：{folder['path']}", code="WORKSPACE_PATH_NOT_FOUND",
            )
        self._config["active_workspace_folder_id"] = folder_id
        self._save()
        _log_info(f"当前激活工作区切换为：{folder['name']}（{folder['path']}）", agent_role="system")
        return folder

    def get_active_workspace_folder(self) -> dict | None:
        folder_id = self._config.get("active_workspace_folder_id") or ""
        if not folder_id:
            return None
        folder = self.get_workspace_folder(folder_id)
        if folder and folder["exists"]:
            return folder
        return None

    def remove_workspace_folder(self, folder_id: str) -> dict:
        rows = self._config.get("workspace_folders") or []
        target = next((r for r in rows if r.get("folder_id") == folder_id), None)
        if not target:
            raise SecurityViolation(f"工作区文件夹不存在：{folder_id}", code="WORKSPACE_NOT_FOUND")
        self._config["workspace_folders"] = [r for r in rows if r.get("folder_id") != folder_id]
        if self._config.get("active_workspace_folder_id") == folder_id:
            remaining = self._config["workspace_folders"]
            self._config["active_workspace_folder_id"] = (
                remaining[0]["folder_id"] if remaining else ""
            )
        self._save()
        _log_info(
            f"工作区文件夹已从列表移除（磁盘目录未删除）：{target.get('path')}",
            agent_role="system",
        )
        return {"folder_id": folder_id, "path": target.get("path", "")}

    # ---------------- 目录选择辅助（浏览器原生文件夹选择器的路径补齐） ----------------
    def scan_folder_candidates(self, folder_name: str, *, max_depth: int = 3,
                               max_entries: int = 20000) -> list[str]:
        """按文件夹名在常用根目录中查找候选绝对路径。

        浏览器 File System Access API 出于安全限制只暴露文件夹**名称**，
        因此这里在受控深度内扫描常用位置建立名称→路径映射；被选中的目录会写入
        标记文件 `.mae_workspace.json`，下次即可直接识别（优先返回带标记的目录）。

        扫描顺序按"命中概率"排序（本地项目目录 → 用户目录 → 盘符根），
        避免把预算浪费在 system 目录上。
        """
        name = (folder_name or "").strip()
        if not name:
            return []

        home = Path(os.environ.get("USERPROFILE") or Path.home())
        # 优先扫描本机项目/用户目录，最后才是盘符根（盘符根只扫浅层）
        primary_roots = [
            Path.cwd(),
            home / "Desktop", home / "Documents", home / "Downloads",
            home / "OneDrive", home / "workspace", home / "projects", home / "code",
            home,
        ]
        drive_roots: list[Path] = []
        if sys.platform.startswith("win"):
            for letter in "CDEFGHIJKLMNOPQRSTUVWXYZ":
                drive = Path(f"{letter}:\\")
                try:
                    if drive.is_dir():
                        drive_roots.append(drive)
                except OSError:
                    continue
        else:
            drive_roots.append(Path("/home"))
            drive_roots.append(Path("/"))

        marked: list[str] = []
        plain: list[str] = []
        seen: set[str] = set()
        budget = [max_entries]

        def record(candidate: Path) -> None:
            norm = _normalize_path(candidate)
            if norm in seen:
                return
            seen.add(norm)
            if (candidate / WORKSPACE_MARKER_FILE).exists():
                marked.append(str(candidate))
            else:
                plain.append(str(candidate))

        # ---- 1. 优先目录：满深度递归 ----
        for root in primary_roots:
            if budget[0] <= 0:
                break
            self._scan_tree(root, name, max_depth=max_depth, budget=budget, record=record)

        # ---- 2. 盘符根：只扫 2 层（避免全盘遍历过慢）----
        for root in drive_roots:
            if budget[0] <= 0:
                break
            self._scan_tree(root, name, max_depth=2, budget=budget, record=record)

        return marked + plain

    def _scan_tree(self, root: Path, name: str, *, max_depth: int,
                   budget: list[int], record) -> None:
        """在单根目录内按名称查找文件夹（受深度与步数预算约束）。"""
        try:
            if not root.is_dir():
                return
        except OSError:
            return
        # 根目录自身名称即命中（仅排除已知工具目录）
        if root.name.lower() == name.lower() and root.name.lower() not in _SKIP_SCAN_DIRS:
            record(root)
        base_depth = len(root.parts)
        for current, dirs, _files in os.walk(root, topdown=True):
            if budget[0] <= 0:
                return
            cur = Path(current)
            depth = len(cur.parts) - base_depth
            dirs[:] = [d for d in dirs
                       if d.lower() not in _SKIP_SCAN_DIRS and not d.startswith("$")]
            budget[0] -= 1
            if depth >= max_depth:
                dirs[:] = []
                continue
            for d in dirs:
                if d.lower() == name.lower() and d.lower() not in _SKIP_SCAN_DIRS:
                    record(cur / d)

    def remove_marker(self, path: str | os.PathLike) -> None:
        try:
            marker = Path(path) / WORKSPACE_MARKER_FILE
            if marker.is_file():
                marker.unlink()
        except OSError:
            pass

    def _write_marker(self, path: Path, folder: dict) -> None:
        """写入工作区标记文件，便于后续按名称快速定位该目录。"""
        try:
            marker = path / WORKSPACE_MARKER_FILE
            if marker.exists():
                return
            marker.write_text(json.dumps({
                "app": "multi-agent-ecosystem",
                "folder_id": folder["folder_id"],
                "name": folder["name"],
                "created_at": folder["created_at"],
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            _log_info(f"工作区标记文件写入失败（不影响使用）：{path} - {exc}",
                      agent_role="system")

    # ==================================================================
    # 首次启动向导（第7章 7.4）
    # ==================================================================
    def any_provider_configured(self) -> bool:
        return any(self.get_api_key(p["provider"]) for p in PROVIDERS)

    def any_provider_tested_ok(self) -> bool:
        return any(
            self.get_api_key(p["provider"]) and self._config["providers"].get(p["provider"], {}).get("last_test_ok")
            for p in PROVIDERS
        )

    def complete_first_launch(self) -> tuple[bool, str]:
        """至少配置一个模型并测试连通后才允许进入工作台。"""
        if not self.any_provider_configured():
            return False, "请至少配置一个模型的 API Key"
        if not self.any_provider_tested_ok():
            return False, "请至少对一个已配置模型完成「测试连通」"
        self._config["first_launch_completed"] = True
        self._save()
        return True, "配置完成"

    # ==================================================================
    # 背景设置（第7章 7.1 背景功能）
    # ==================================================================
    def set_background(self, *, bg_type: str, value: str) -> None:
        self._config["background"] = {"type": bg_type, "value": value, "url": value if bg_type == "custom" else ""}
        self._save()

    def get_background(self) -> dict:
        return self._config.get("background", {"type": "preset", "value": "night"})

    # ==================================================================
    # 备份（第9章 9.3：加密配置导出——仅密文信封）
    # ==================================================================
    def export_sealed_only(self) -> dict:
        return {
            "system_config": {k: v for k, v in self._config.items() if k != "api_keys"},
            "api_keys_sealed": {k: v.to_dict() for k, v in self._sealed.items()},
            "note": "密钥仅以 AES-256-GCM 密文 + SHA256 摘要形式导出，无明文、不可离线还原",
        }

# --------------------------------------------------------------------------
# 容错日志包装：ConfigStore 可能在日志模块初始化之前被构造（配置迁移阶段），
# 因此这里使用 try_get_logger，未就绪时静默跳过而不是抛错。
# --------------------------------------------------------------------------
def _log_info(message: str, **extra) -> None:
    logger = try_get_logger()
    if logger is not None:
        logger.info(message, **extra)


def _log_exception(*, error_code: str, message: str, **extra) -> None:
    logger = try_get_logger()
    if logger is not None:
        logger.exception_log(error_code=error_code, message=message, **extra)
