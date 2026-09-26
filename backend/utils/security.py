# -*- coding: utf-8 -*-
"""
文件安全校验 + bcrypt 密码模块 + 高危操作识别

架构文档来源：
  - 第6章 6.2 文件上传安全限制（单文件最大50MB / 白名单格式 / 禁止可执行文件 / 自动隔离至会话目录）
  - 第6章 6.3 局域网访问安全（后台登录密码 bcrypt 哈希存储，禁止明文；高危审批开关永久强制开启）
  - 第2章 2.2 裸机安全核心规则（操作白名单、高危操作强制审批）
  - 第4章 4.2 代码工程Agent（删除/批量修改/系统命令/外网下载自动触发 waiting_approval）
"""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import bcrypt

from backend.utils.constants import (
    ALLOWED_UPLOAD_EXTENSIONS,
    FORBIDDEN_UPLOAD_EXTENSIONS,
    HIGH_RISK_APPROVAL_ALWAYS_ON,
    IMAGE_EXTENSIONS,
    MAX_UPLOAD_BYTES,
    RISK_LEVEL_HIGH,
    RISK_LEVEL_LOW,
    RISK_LEVEL_MID,
)
from backend.utils.paths import SecurityViolation


# ==========================================================================
# 一、上传文件安全校验（第6章 6.2）
# ==========================================================================
@dataclass
class UploadCheckResult:
    ok: bool
    filename: str
    safe_name: str
    size: int
    ext: str
    resource_type: str          # image / document / text
    reason: str = ""
    code: str = ""


def normalize_filename(name: str) -> str:
    """清洗原始文件名：去路径片段、去控制字符、限制长度，防目录穿越与伪造后缀。"""
    raw = unicodedata.normalize("NFKC", str(name or ""))
    raw = raw.replace("\\", "/").split("/")[-1]        # 丢弃任何路径部分
    raw = "".join(ch for ch in raw if ch.isprintable() and ch not in '<>:"|?*')
    raw = raw.strip().strip(".")
    if not raw:
        raw = "unnamed"
    if len(raw) > 120:
        stem, dot, ext = raw.rpartition(".")
        raw = (stem[:110] + dot + ext) if dot else raw[:120]
    return raw


def detect_resource_type(ext: str) -> str:
    ext = ext.lower()
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".rtf", ".odt", ".csv"}:
        return "document"
    return "text"


def check_upload(filename: str, size: int, content_head: bytes | None = None) -> UploadCheckResult:
    """上传文件全量安全校验。任何一条不满足即拒绝（不允许简化）。"""
    safe = normalize_filename(filename)
    ext = os.path.splitext(safe)[1].lower()
    rtype = detect_resource_type(ext)

    # 规则3：禁止可执行文件上传 exe/bat/sh/bin 等
    if ext in FORBIDDEN_UPLOAD_EXTENSIONS:
        return UploadCheckResult(
            False, filename, safe, size, ext, rtype,
            reason=f"禁止上传可执行文件：{ext}", code="EXECUTABLE_DENIED",
        )

    # 规则2：白名单格式 —— 图片、文档、文本
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        return UploadCheckResult(
            False, filename, safe, size, ext, rtype,
            reason=f"文件格式不在白名单内：{ext or '（无扩展名）'}", code="EXT_NOT_ALLOWED",
        )

    # 规则1：单文件最大 50MB
    if size <= 0:
        return UploadCheckResult(False, filename, safe, size, ext, rtype,
                                reason="空文件不允许上传", code="EMPTY_FILE")
    if size > MAX_UPLOAD_BYTES:
        return UploadCheckResult(
            False, filename, safe, size, ext, rtype,
            reason=f"文件超过上限 {MAX_UPLOAD_BYTES // (1024 * 1024)}MB（当前 {size / 1024 / 1024:.2f}MB）",
            code="FILE_TOO_LARGE",
        )

    # 魔数嗅探：防止把 .exe 改名成 .png 绕过白名单（后端二次校验，不信任前端）
    if content_head:
        magic_reason = _magic_mismatch(ext, content_head)
        if magic_reason:
            return UploadCheckResult(False, filename, safe, size, ext, rtype,
                                     reason=magic_reason, code="MAGIC_DENIED")

    return UploadCheckResult(True, filename, safe, size, ext, rtype)


_EXEC_MAGIC = (b"MZ", b"\x7fELF", b"\xca\xfe\xba\xbe", b"#!", b"\xfe\xed\xfa\xce", b"\xfe\xed\xfa\xcf")
_IMAGE_MAGIC = {
    ".png": (b"\x89PNG\r\n\x1a\n",),
    ".jpg": (b"\xff\xd8\xff",),
    ".jpeg": (b"\xff\xd8\xff",),
    ".gif": (b"GIF87a", b"GIF89a"),
    ".webp": (b"RIFF",),
    ".bmp": (b"BM",),
    ".pdf": (b"%PDF",),
    ".docx": (b"PK\x03\x04",),
    ".xlsx": (b"PK\x03\x04",),
    ".pptx": (b"PK\x03\x04",),
    ".zip": (b"PK\x03\x04",),
}


def _magic_mismatch(ext: str, head: bytes) -> str | None:
    for magic in _EXEC_MAGIC:
        if head.startswith(magic):
            return f"文件内容疑似可执行/脚本文件（魔数 {magic!r}），拒绝上传"
    expected = _IMAGE_MAGIC.get(ext)
    if expected and not any(head.startswith(m) for m in expected):
        # 图片/容器类做严格魔数比对；纯文本类不做（编码多态）
        return f"文件内容与扩展名 {ext} 不符，疑似伪造后缀"
    return None


# ==========================================================================
# 二、操作白名单（第2章 2.2 规则2、第4章 4.2）
# ==========================================================================
# 允许的"无害开发命令"（白名单精确首词匹配，其余一律高危）
COMMAND_WHITELIST: tuple[str, ...] = (
    "python", "python3", "py", "pip", "pip3",
    "node", "npm", "npx", "pnpm", "yarn",
    "git", "dir", "ls", "type", "cat", "echo", "pwd", "cd",
    "pytest", "ruff", "black", "mypy", "flake8",
    "uvicorn", "tsc", "vite", "make", "cmake", "gcc", "clang", "dotnet", "java", "javac", "go", "cargo", "rustc",
)

# 高危命令模式 → 一律 waiting_approval
DANGEROUS_COMMAND_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\brm\s+-rf\b|\brmdir\b|\bdel\b|\berase\b|\bRemove-Item\b|\brm\b", "文件删除"),
    (r"\bformat\b|\bdiskpart\b|\bmkfs\b|\bfdisk\b", "磁盘格式化/分区"),
    (r"\bshutdown\b|\breboot\b|\brestart-computer\b", "系统关机/重启"),
    (r"\breg\s+(add|delete|import)\b|\bregedit\b", "注册表修改"),
    (r"\bnet\s+user\b|\bnet\s+localgroup\b|\bwhoami\s+/|net user", "系统账户操作"),
    (r"\bsc\s+(delete|stop|start|config)\b|\bschtasks\b|\bservice\b", "系统服务操作"),
    (r"\bchmod\s+777\b|\bchown\b|\btakeown\b|\bicacls\b|\battrib\b", "权限变更"),
    (r"\btaskkill\b|\bkill\b|\bkillall\b|\bstop-process\b", "进程强杀"),
    (r"\bcurl\b|\bwget\b|\bInvoke-WebRequest\b|\biwr\b|\bInvoke-RestMethod\b|\bgit\s+clone\b|\bscp\b|\brsync\b|\bftp\b", "外网下载/远程传输"),
    (r"\bnpm\s+(install|i|add)\b|\bpip\s+install\b|\byarn\s+add\b|\bpnpm\s+add\b|\bwinget\b|\bchoco\b|\bapt(-get)?\s+install\b|\bbrew\s+install\b", "依赖安装/外网下载"),
    (r"\b>\s*[A-Za-z]:|>>", "重定向覆盖写入"),
    (r"\bpowershell\b|\bpwsh\b|\bcmd(\.exe)?\b|\bbash\b|\bsh\s+-c\b", "嵌套系统 shell"),
    (r"\bcrontab\b|\bmount\b|\bumount\b|\bdd\b", "系统级操作"),
)

# 高危代码/文件操作模式（Qwen Coder 生成的动作）
DANGEROUS_OPS = {
    "delete": "文件删除",
    "batch_modify": "批量修改",
    "system_command": "系统命令",
    "external_download": "外网下载",
}


@dataclass
class RiskAssessment:
    """高危操作风险评估结果（第3章 3.2 审批 metadata 三字段来源）"""

    is_high_risk: bool
    risk_level: str
    operation_type: str                                # 文件删除 / 批量修改 / 系统命令 / 外网下载
    danger_reason: str = ""
    matched: list[str] = field(default_factory=list)

    def as_metadata(self, *, operation_desc: str, operation_params: dict | str) -> dict:
        """生成审批消息 metadata 强制字段（第3章 3.2）。"""
        return {
            "risk_level": self.risk_level,
            "operation_desc": operation_desc,
            "operation_params": operation_params if isinstance(operation_params, str) else _json_dumps(operation_params),
            "danger_reason": self.danger_reason,
            "operation_type": self.operation_type,
            "matched_patterns": list(self.matched),
        }


def _json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


def assess_operation(tool: str, args: dict) -> RiskAssessment:
    """判定单个工具调用是否为高危操作（第4章 4.2 裸机安全逻辑 规则2）。

    高危四类：文件删除 / 批量修改 / 系统命令 / 外网下载 —— 全部自动触发 waiting_approval。
    注意：高危审批开关永久强制开启（第6章 6.3），本函数不做任何"关闭后可跳过"的分支。
    """
    assert HIGH_RISK_APPROVAL_ALWAYS_ON, "高危审批开关永久强制开启，不可关闭"

    tool = (tool or "").strip()
    args = args or {}
    matched: list[str] = []

    # ---- 文件删除 ----
    if tool in ("delete_file", "delete_dir", "remove_file", "rm"):
        return RiskAssessment(
            True, RISK_LEVEL_HIGH, "文件删除",
            f"请求删除文件系统内容（工具 {tool}），属于不可逆操作，必须人工审批。",
            [tool],
        )

    # ---- 系统命令 ----
    if tool in ("run_command", "shell", "exec", "terminal"):
        cmd = str(args.get("command") or args.get("cmd") or "").strip()
        if not cmd:
            return RiskAssessment(True, RISK_LEVEL_HIGH, "系统命令", "命令为空，无法评估，按高危处理。", [])
        for pattern, label in DANGEROUS_COMMAND_PATTERNS:
            if re.search(pattern, cmd, flags=re.IGNORECASE):
                matched.append(label)
        first = _first_token(cmd)
        if first not in COMMAND_WHITELIST:
            matched.append(f"非白名单命令「{first}」")
        if matched:
            return RiskAssessment(
                True, RISK_LEVEL_HIGH, "系统命令",
                "命令命中高危模式：" + "、".join(dict.fromkeys(matched)) + "；系统命令一律需人工审批。",
                matched,
            )
        # 白名单内、无高危模式 -> 仍需审批（第4章 4.2 规则2：系统命令统一拦截）
        return RiskAssessment(
            True, RISK_LEVEL_HIGH, "系统命令",
            "系统命令属于高危操作类别，一律暂停任务并推送人工审批。",
            [f"白名单命令:{first}"],
        )

    # ---- 外网下载 ----
    if tool in ("download", "fetch_url", "http_get", "external_download"):
        matched.append("外网下载")
        url = str(args.get("url") or "")
        host = ""
        m = re.match(r"https?://([^/]+)", url, flags=re.IGNORECASE)
        if m:
            host = m.group(1)
        return RiskAssessment(
            True, RISK_LEVEL_HIGH, "外网下载",
            f"请求从外网下载资源（{host or url or '未提供URL'}），存在供应链与安全风险，必须人工审批。",
            matched,
        )

    # ---- 批量修改 ----
    if tool in ("write_file", "edit_file", "apply_patch", "batch_edit", "batch_write", "move_file", "rename_file"):
        targets = args.get("files") or args.get("paths") or []
        if isinstance(targets, str):
            targets = [targets]
        count = len(targets) if isinstance(targets, (list, tuple, set)) else 0
        declared = int(args.get("affected_count") or 0)
        total = max(count, declared)
        threshold = 2 if tool in ("move_file", "rename_file") else 3
        if tool in ("batch_edit", "batch_write") or total >= threshold:
            matched.append(f"批量修改 {total} 处")
            return RiskAssessment(
                True, RISK_LEVEL_HIGH, "批量修改",
                f"请求批量修改 {total or '多'} 处文件/内容，超出单文件安全阈值（≥{threshold}），"
                "可能造成大面积不可逆改动，必须人工审批。",
                matched,
            )
        return RiskAssessment(False, RISK_LEVEL_LOW, "")

    if tool in ("read_file", "list_dir", "search", "read_doc", "analyze_image", "memory_search"):
        return RiskAssessment(False, RISK_LEVEL_LOW, "")

    # 未登记工具：宁严勿松
    return RiskAssessment(
        True, RISK_LEVEL_MID, "未登记操作",
        f"未登记的工具调用 {tool!r}，按未授权操作处理，需人工审批。",
        [tool],
    )


def _first_token(cmd: str) -> str:
    stripped = cmd.strip()
    stripped = re.sub(r'^[A-Za-z]:\\[^\\]*\\>', "", stripped).strip()
    parts = re.split(r"[\s|;&]+", stripped)
    for p in parts:
        token = os.path.basename(p).lower()
        if token:
            if token.endswith(".exe") or token.endswith(".bat") or token.endswith(".ps1"):
                token = os.path.splitext(token)[0]
            return token
    return ""


# ==========================================================================
# 三、后台登录密码 bcrypt（第6章 6.3 规则2）
# ==========================================================================
def hash_password(password: str) -> str:
    """bcrypt 哈希存储，禁止明文。"""
    if not password:
        raise ValueError("密码不能为空")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("ascii")


def verify_password(password: str, hashed: str) -> bool:
    if not password or not hashed:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("ascii"))
    except (ValueError, TypeError):
        return False


# ==========================================================================
# 四、会话文件写入安全（第4章 4.2）
# ==========================================================================
DANGEROUS_CONTENT_MARKERS = (
    "rm -rf /", "format c:", "shutdown /s", "del /f /s /q c:\\",
    "vssadmin delete shadows", "bcdedit", "net user administrator /active:yes",
)


def scan_content_for_danger(content: str) -> str | None:
    """内容级安全扫描：命中返回危险说明，用于强制审批/拒绝。"""
    if not content:
        return None
    lowered = content.lower()
    for marker in DANGEROUS_CONTENT_MARKERS:
        if marker in lowered:
            return f"内容包含高危破坏性指令：{marker}"
    return None


def assert_within_size(content: str, *, limit: int = 5 * 1024 * 1024) -> None:
    if len(content.encode("utf-8")) > limit:
        raise SecurityViolation(f"单次写入内容超过 {limit // 1024 // 1024}MB 安全上限", code="CONTENT_TOO_LARGE")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
