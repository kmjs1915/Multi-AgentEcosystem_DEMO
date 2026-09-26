# -*- coding: utf-8 -*-
"""
裸机目录规范 —— 跨操作系统固定路径

架构文档来源：第5章 裸机目录规范（固定路径）
    Windows:  %USERPROFILE%\\.multi_agent_ecosystem\\
    Mac/Linux: ~/.multi_agent_ecosystem/
    子目录：config / sessions / uploads / logs / vector_db
    核心规则：每个会话独立工作目录，文件完全隔离，杜绝跨会话污染

本模块同时提供「工作区目录隔离」的唯一路径解析入口：
    resolve_in_root() —— 所有文件读写、创建、修改、删除必须先经过它，越界直接抛安全异常。
    （resolve_in_session() 保留为兼容包装，内部同样委托 resolve_in_root()）

【需求点 二、2 工作区安全权限硬约束】
    所有 7 个 Agent 的文件操作根目录 = 当前选中工作区文件夹；
    越界（上级目录 / 其他磁盘 / 系统关键目录）一律拒绝，并返回统一文案：
      越权访问禁止：只能操作当前工作目录内文件

【需求点 一、路径校验 Bug 修复】两套路径判断彻底分离（本次新增）
    背景：会话存储目录固定在 %USERPROFILE%\\.multi_agent_ecosystem\\sessions，
          而用户业务工作区可能是任意磁盘目录（如 E:\\Multi-AgentEcosystem），
          两者**不在同一目录树下**。历史实现把"会话元数据/会话 json/系统配置"
          也塞进了"工作区子路径校验"，导致加载会话时抛
          ValueError: '...' is not in the subpath of '...'。

    修复后的职责边界（唯一权威口径）：
      · is_workspace_file()  —— 只校验【Agent 业务文件】是否在当前工作区内，
                                做工作区子路径判断（越权即拒绝）；
      · is_system_meta_file() —— 校验【系统元文件】（会话元数据、会话 json、
                                系统配置、日志、上传、向量库、artifact 等），
                                **不做工作区子路径判断**，只做边界/合法性检查。
      · make_system_meta_checker() —— 把判定器绑定到指定生态系统根目录
                                （运行时注入 paths，避免依赖全局推断）。
      · safe_relative_to()   —— 跨盘符/跨目录树的相对路径计算兜底，
                                彻底消灭 pathlib 的 ValueError（优雅降级不崩溃）。
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from backend.utils.constants import (
    ECOSYSTEM_DIR_NAME,
    ERR_WORKSPACE_ACCESS_DENIED,
    ERR_WORKSPACE_UNAVAILABLE,
    SECURITY_META_CATEGORIES,
    SECURITY_META_CATEGORY_SYSTEM,
    SESSION_SUBDIR_ARTIFACT,
    SESSION_SUBDIR_UPLOAD,
    SESSION_SUBDIR_WORKSPACE,
    SUBDIR_CONFIG,
    SUBDIR_LOGS,
    SUBDIR_SESSIONS,
    SUBDIR_UPLOADS,
    SUBDIR_VECTOR_DB,
    WORKSPACE_ACCESS_DENIED_MESSAGE,
    WORKSPACE_MARKER_FILE,
    WORKSPACE_UNAVAILABLE_NOT_DIR,
    WORKSPACE_UNAVAILABLE_NOT_FOUND,
    WORKSPACE_UNAVAILABLE_NO_READ,
    WORKSPACE_UNAVAILABLE_NO_WRITE,
    workspace_unavailable_message,
)


class SecurityViolation(Exception):
    """安全违规异常（第2章 2.2 / 第6章）：越界读写、非法路径、可执行文件上传等。

    该异常必须被上层捕获并转换为 error 消息 + 日志，绝不允许静默忽略。
    """

    def __init__(self, message: str, *, code: str = "SECURITY_VIOLATION", detail: dict | None = None):
        super().__init__(message)
        self.code = code
        self.detail = detail or {}


class WorkspaceUnavailable(SecurityViolation):
    """【BUG-B】工作区绑定目录不可用（不存在 / 不是文件夹 / 无读权限 / 无写权限）。

    与普通越权拒绝区分开：本异常用于**执行文件类任务前的目录前置校验**，
    上层据此终止对应子任务并把可读错误写入思考链路，绝不下发空路径给 Agent。
    """

    def __init__(self, path: str, reason: str, *, detail: dict | None = None):
        message = workspace_unavailable_message(str(path or ""), reason)
        super().__init__(
            message, code=ERR_WORKSPACE_UNAVAILABLE,
            detail={"path": str(path or ""), "reason": reason, **(detail or {})},
        )
        self.path = str(path or "")
        self.reason = reason


# --------------------------------------------------------------------------
# 根目录解析（第5章）
# --------------------------------------------------------------------------
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


def ecosystem_root() -> Path:
    """返回跨平台裸机根目录。

    Windows -> %USERPROFILE%\\.multi_agent_ecosystem
    Mac/Linux -> ~/.multi_agent_ecosystem
    支持 MAE_ROOT 环境变量覆盖（仅用于测试/迁移，生产默认走系统规范路径）。
    """
    override = os.environ.get("MAE_ROOT", "").strip()
    if override:
        return Path(override).expanduser().resolve()

    if sys.platform.startswith("win"):
        home = os.environ.get("USERPROFILE") or str(Path.home())
        return (Path(home) / ECOSYSTEM_DIR_NAME).resolve()
    return (Path.home() / ECOSYSTEM_DIR_NAME).resolve()


@dataclass(frozen=True)
class EcosystemPaths:
    """裸机目录集合（第5章 子目录结构 1~5）"""

    root: Path
    config: Path
    sessions: Path
    uploads: Path
    logs: Path
    vector_db: Path

    @classmethod
    def build(cls, root: Path | None = None) -> "EcosystemPaths":
        r = Path(root) if root else ecosystem_root()
        return cls(
            root=r,
            config=r / SUBDIR_CONFIG,
            sessions=r / SUBDIR_SESSIONS,
            uploads=r / SUBDIR_UPLOADS,
            logs=r / SUBDIR_LOGS,
            vector_db=r / SUBDIR_VECTOR_DB,
        )

    def all_dirs(self) -> Iterable[Path]:
        return (self.root, self.config, self.sessions, self.uploads, self.logs, self.vector_db)

    def ensure(self) -> "EcosystemPaths":
        """自动初始化全部目录（第二步：目录自动初始化）"""
        for d in self.all_dirs():
            d.mkdir(parents=True, exist_ok=True)
        _harden_dir(self.root)
        _harden_dir(self.config)
        return self

    # ---- 具体文件位置（固定，不随会话变化） ----
    @property
    def system_config_file(self) -> Path:
        return self.config / "system_config.json"

    @property
    def master_key_file(self) -> Path:
        return self.config / "master.key"

    @property
    def auth_file(self) -> Path:
        return self.config / "auth.json"

    @property
    def sqlite_db_file(self) -> Path:
        return self.logs / "ecosystem.sqlite3"

    @property
    def vector_db_file(self) -> Path:
        return self.vector_db / "long_term_memory.sqlite3"

    @property
    def vector_index_file(self) -> Path:
        return self.vector_db / "vectors.npz"


@dataclass(frozen=True)
class SessionPaths:
    """单个会话的独立工作目录（第5章 核心规则：文件完全隔离）

    【需求点 二、2 工作区安全权限硬约束】
      `workspace` 现在是「当前选中工作区文件夹」——
        · 本地文件夹工作区：workspace = 该本地目录（Agent 可读写的主操作根目录）
        · 系统隔离工作区：workspace = sessions/<sid>/workspace（向后兼容）
      会话私有资源（upload / artifact / root）始终留在系统 sessions/ 内，保持上传隔离与审计。
    """

    session_id: str
    root: Path
    upload: Path
    workspace: Path
    artifact: Path
    workspace_folder_id: str = ""
    workspace_folder_name: str = ""
    is_local_folder: bool = False
    sandbox_root: Path | None = None      # 系统内会话目录，用于 upload/artifact 边界校验

    @classmethod
    def build(cls, paths: EcosystemPaths, session_id: str, *,
              workspace_root: Path | None = None,
              workspace_folder_id: str = "",
              workspace_folder_name: str = "") -> "SessionPaths":
        sid = validate_session_id(session_id)
        root = paths.sessions / sid
        is_local = workspace_root is not None
        return cls(
            session_id=sid,
            root=root,
            upload=root / SESSION_SUBDIR_UPLOAD,
            workspace=Path(workspace_root) if is_local else (root / SESSION_SUBDIR_WORKSPACE),
            artifact=root / SESSION_SUBDIR_ARTIFACT,
            workspace_folder_id=workspace_folder_id,
            workspace_folder_name=workspace_folder_name,
            is_local_folder=is_local,
            sandbox_root=root,
        )

    def ensure(self) -> "SessionPaths":
        """创建必需目录。

        注意：workspace 指向用户本地文件夹时**不自动创建**（避免臆造目录），
        但必须在构造阶段已完成存在性校验。
        """
        self.root.mkdir(parents=True, exist_ok=True)
        self.upload.mkdir(parents=True, exist_ok=True)
        self.artifact.mkdir(parents=True, exist_ok=True)
        if not self.is_local_folder:
            self.workspace.mkdir(parents=True, exist_ok=True)
        return self

    @property
    def boundary_root(self) -> Path:
        """文件操作边界根目录 = 当前工作区文件夹（越权校验的唯一基准）。"""
        return self.workspace


# --------------------------------------------------------------------------
# 标识符与路径安全校验（第2章 2.2 裸机安全核心规则）
# --------------------------------------------------------------------------
def validate_session_id(session_id: str) -> str:
    """会话 ID 必须为安全字符集，禁止路径穿越片段。"""
    if not session_id or not isinstance(session_id, str):
        raise SecurityViolation("session_id 不能为空", code="INVALID_SESSION_ID")
    if not _SAFE_ID_RE.match(session_id):
        raise SecurityViolation(
            f"非法 session_id：{session_id!r}（只允许字母数字下划线中划线，长度1-64）",
            code="INVALID_SESSION_ID",
        )
    return session_id


def validate_uuid(value: str, *, field: str = "id") -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        raise SecurityViolation(f"{field} 必须是合法 UUID：{value!r}", code="INVALID_UUID") from None


def new_uuid() -> str:
    """Agent_Router 生成 ID 使用（第4章 4.1）。"""
    return str(uuid.uuid4())


def _normalize(p: Path) -> str:
    s = os.path.normcase(os.path.abspath(str(p)))
    return s.rstrip("\\/") or s


def normalize_path(p: str | Path) -> str:
    """规范化路径字符串，用于路径比较（跨平台大小写/分隔符归一）。"""
    return _normalize(Path(str(p)))


def is_system_critical_dir(path: str | Path) -> bool:
    """判断是否为系统关键目录或磁盘根目录（工作区/文件操作一律禁止）。"""
    target = Path(str(path))
    try:
        target = target.resolve()
    except OSError:
        return True
    if _hits_system_dir(target):
        return True
    # 磁盘根目录（C:\ 或 /）
    if target.parent == target:
        return True
    return False


def is_within(child: Path, parent: Path) -> bool:
    """判断 child 是否位于 parent 目录之内（含 parent 自身）。"""
    c, p = _normalize(child), _normalize(parent)
    return c == p or c.startswith(p + os.sep)


# ==========================================================================
# 【需求点 一、1】两套路径判断函数彻底拆分（本次 Bug 修复核心）
# ==========================================================================
# 说明（唯一权威口径，禁止在别处再写第三份路径判断逻辑）：
#   · Agent 业务文件 → is_workspace_file()   ：必须属于当前工作区子目录
#   · 会话/配置元文件 → is_system_meta_file() ：跳过工作区子路径校验
# ==========================================================================
def is_workspace_file(path: str | Path, workspace_root: str | Path) -> bool:
    """【需求点 一、1-a】校验「Agent 业务文件」是否属于当前工作区子目录。

    只有本函数代表工作区白名单校验，供 Agent 读写业务文件的唯一入口
    resolve_in_root() / SessionFileGuard.resolve() 使用。

    约束（第2章 2.2 规则2 / 第4章 4.2 规则1/4）：
      · 目标解析后必须仍位于 workspace_root 之内（含根本身）；
      · 系统关键目录（C:\\Windows、/etc 等）一律不算工作区文件；
      · 绝不做任何"因为不在会话目录下就报错"的判断。

    返回 True 表示"允许作为业务文件操作"，False 表示越界。
    """
    if path is None or workspace_root is None:
        return False
    try:
        root = Path(str(workspace_root))
        try:
            root = root.resolve()
        except OSError:
            root = Path(os.path.abspath(str(root)))
        target = Path(str(path))
        try:
            target = target.resolve()
        except OSError:
            target = Path(os.path.abspath(str(target)))
    except (TypeError, ValueError, OSError):
        return False
    if is_system_critical_dir(target):
        return False
    return is_within(target, root)


def is_agent_business_file(path: str | Path, workspace_root: str | Path) -> bool:
    """【需求点 一、1-a】Agent 业务文件判定 —— 与工作区白名单**同一套实现**。

    需求文档要求拆分为两个独立判断函数：
      · is_agent_business_file()：Agent 读写用户业务文件 → 必须是当前选中工作区的子路径
      · is_system_meta_file()   ：系统会话元文件 / 工作区配置文件 → 跳过 subpath 校验

    为避免"多份逻辑各写一遍又跑偏"，本函数是 is_workspace_file() 的语义化别名，
    两者共用同一实现（唯一权威口径）。新代码推荐使用本名称，可读性更贴合职责。
    """
    return is_workspace_file(path, workspace_root)


def is_system_meta_file(path: str | Path, *, ecosystem_paths: "EcosystemPaths | None" = None,
                        session_root: str | Path | None = None,
                        category: str = SECURITY_META_CATEGORY_SYSTEM) -> bool:
    """【需求点 一、1-b】判断是否为「系统会话 / 配置元文件」——不做工作区子路径校验。

    覆盖范围（第5章 裸机目录规范：config / sessions / uploads / logs / vector_db）：
      · 会话元数据（sessions/<sid>/… 下的 session.json、meta、upload、artifact、workspace）
      · 系统配置文件（config/system_config.json、master.key、auth.json）
      · 系统日志（logs/ecosystem.sqlite3、*.log）
      · 上传隔离区与向量库（uploads/**、vector_db/**）
      · 工作区标记文件（<工作区根>/.mae_workspace.json）

    **本函数不执行工作区子路径判断**——会话数据存放在用户 C 盘配置目录，
    而业务工作区可能在 E 盘等其他磁盘，两者不属于同一目录树，
    对元文件做子路径校验必然误报。这里只做"合法性 + 是否属于系统元文件"判断。

    category 语义：**过滤条件**（不是放行开关）。
      · 传入 SECURITY_META_CATEGORY_WORKSPACE_BUSINESS（或其他非元文件类别）
        → 恒返回 False，强制走工作区白名单校验，杜绝"把业务文件当元文件免检"。

    返回 True = 属于系统元文件（放行，不参与工作区白名单校验）。
    """
    if path is None:
        return False
    # 0) 类别过滤：只有元文件类别才可能免检（业务文件类别一律返回 False）
    if category not in SECURITY_META_CATEGORIES:
        return False
    try:
        raw = str(path).strip().strip('"').strip("'")
        if not raw:
            return False
        target = Path(raw)
        try:
            target = target.resolve()
        except OSError:
            target = Path(os.path.abspath(raw))
    except (TypeError, ValueError, OSError):
        return False

    # 1) 工作区标记文件：由系统在业务目录内写入，属于系统元文件，不参与越权判断
    if target.name == WORKSPACE_MARKER_FILE:
        return True

    # 2) 会话私有目录（会话作用域）
    if session_root is not None:
        try:
            if is_within(target, Path(str(session_root))):
                return True
        except (TypeError, ValueError, OSError):
            pass

    # 3) 生态系统全局目录（config / sessions / uploads / logs / vector_db）
    #    注意：**刻意不把 paths.root 整体算作元文件区**——
    #    系统元数据只可能落在上述 5 个子目录与根目录下的固定配置文件里；
    #    若把 root 整体放行，紧邻 root 的业务目录（例如 root 为
    #    "E:\\proj\\.mae_ws"，业务目录为 "E:\\proj\\.mae_ws_business"）会被误判为
    #    系统元文件而绕过工作区越权校验。
    paths = ecosystem_paths or EcosystemPaths.build()
    for system_dir in (paths.config, paths.sessions, paths.uploads, paths.logs, paths.vector_db):
        if is_within(target, system_dir):
            return True
    if is_within(target, paths.root):
        # root 之内：仅根目录下的固定系统文件（system_config.json/master.key/auth.json/
        # 日志与向量库文件等）视为元文件，其余一律按"需要正常校验"处理
        #
        # 【需求点 一、2】即便已通过 is_within 前置判断，仍对 relative_to 做兜底：
        #   极端情况（符号链接解析差异、跨卷挂载点）下 relative_to 仍可能抛 ValueError，
        #   这里一旦失败就保守返回 False（不放行），绝不把异常抛给调用方。
        try:
            rel = Path(os.path.abspath(str(target))).relative_to(
                Path(os.path.abspath(str(paths.root)))
            )
        except (ValueError, TypeError, OSError):
            return False
        parts = [p for p in rel.parts if p not in (".", "")]
        return len(parts) <= 1

    # 4) 既不在会话目录、也不在生态系统目录内 → 不是系统元文件（必须走业务校验）
    return False


def make_system_meta_checker(ecosystem_paths: "EcosystemPaths"):
    """【需求点 一、1-b】生成"绑定到指定生态系统根目录"的系统元文件判定器。

    为什么需要它：is_system_meta_file() 默认用 ecosystem_root() 推断系统目录，
    而运行时（可能被 MAE_ROOT 覆盖 / 被测试隔离）的系统目录应由调用方显式注入。
    统一用本工厂生成判定器，避免各处再写一份路径判断逻辑。
    """

    def _check(path, *, session_root: str | Path | None = None, category: str = SECURITY_META_CATEGORY_SYSTEM) -> bool:
        return is_system_meta_file(path, ecosystem_paths=ecosystem_paths,
                                   session_root=session_root, category=category)

    return _check


def safe_relative_to(path: str | Path, base: str | Path, *,
                     fallback: str = "") -> str:
    """【需求点 一、2】安全计算相对路径：跨盘符/跨目录树时优雅降级，绝不抛 ValueError。

    ValueError: 'X' is not in the subpath of 'Y' 是 pathlib.Path.relative_to 在
    "两个路径不在同一目录树"（典型：会话目录在 C 盘、业务工作区在 E 盘）时的
    原生异常。任何"列出会话文件 / 展示工作区相对路径"的展示逻辑都必须走本函数。
    """
    if path is None or base is None:
        return fallback
    try:
        rel = Path(str(path)).relative_to(Path(str(base)))
        return str(rel).replace("\\", "/") or "."
    except (ValueError, TypeError, OSError):
        return fallback or str(path).replace("\\", "/")


def display_relative(path: str | Path, base: str | Path) -> str:
    """展示用相对路径（带越界兜底的语义化包装）。

    位于 base 之内 → 返回相对路径；
    位于 base 之外（例如会话目录在 C 盘、工作区在 E 盘）→ 返回带括号的完整路径，
    保证前端列表一定拿得到可读文本，而不是让整个接口 500。
    """
    raw = str(path)
    try:
        return str(Path(raw).relative_to(Path(str(base)))).replace("\\", "/") or "."
    except (ValueError, TypeError, OSError):
        return f"(工作区外) {raw.replace(chr(92), '/')}"


def resolve_in_root(root: Path, relative_path: str) -> Path:
    """把相对路径解析到指定根目录内；任何越界（相对/绝对路径）一律拒绝。

    【需求点 二、2 工作区安全权限硬约束】
      所有 7 个 Agent 的文件操作根目录 = 当前选中工作区文件夹。
      该函数是该边界的唯一执行点（仅作用于 Agent 业务文件）：
        · 拒绝绝对路径（除 Windows 风格）与盘符路径
        · 拒绝 `..` 逃逸到上级目录
        · 拒绝系统关键目录
        · 解析后再次校验是否仍位于 root 之内（防符号链接穿越）

    【需求点 一、1-a】最终归属校验统一委托 is_workspace_file()，
    保证"工作区白名单"只有一处实现，避免多份逻辑再次跑偏。
    """
    if relative_path is None:
        raise SecurityViolation(WORKSPACE_ACCESS_DENIED_MESSAGE, code=ERR_WORKSPACE_ACCESS_DENIED,
                                detail={"reason": "路径为空"})

    raw = str(relative_path).strip().strip('"').strip("'")
    if not raw:
        raise SecurityViolation(WORKSPACE_ACCESS_DENIED_MESSAGE, code=ERR_WORKSPACE_ACCESS_DENIED,
                                detail={"reason": "路径为空"})

    if os.path.isabs(raw) or Path(raw).is_absolute() or re.match(r"^[A-Za-z]:", raw):
        raise SecurityViolation(
            WORKSPACE_ACCESS_DENIED_MESSAGE,
            code=ERR_WORKSPACE_ACCESS_DENIED,
            detail={"path": raw, "reason": "禁止使用绝对路径/盘符路径，只允许当前工作区内的相对路径"},
        )

    if raw.startswith("~"):
        raise SecurityViolation(
            WORKSPACE_ACCESS_DENIED_MESSAGE,
            code=ERR_WORKSPACE_ACCESS_DENIED,
            detail={"path": raw, "reason": "禁止使用家目录展开路径"},
        )

    root_path = Path(root)
    try:
        root_path = root_path.resolve()
    except OSError:
        pass

    candidate = (root_path / raw).resolve()
    if is_system_critical_dir(candidate):
        raise SecurityViolation(
            WORKSPACE_ACCESS_DENIED_MESSAGE,
            code=ERR_WORKSPACE_ACCESS_DENIED,
            detail={"path": raw, "reason": "目标位于系统关键目录"},
        )
    # 【需求点 一、1-a】工作区白名单校验的唯一实现点
    if not is_workspace_file(candidate, root_path):
        raise SecurityViolation(
            WORKSPACE_ACCESS_DENIED_MESSAGE,
            code=ERR_WORKSPACE_ACCESS_DENIED,
            detail={
                "path": raw,
                "workspace_root": str(root_path),
                "resolved": str(candidate),
                "reason": "路径超出当前工作区文件夹范围（含上级目录与其他磁盘目录）",
            },
        )
    return candidate


def resolve_in_session(session: SessionPaths, relative_path: str) -> Path:
    """兼容包装：等价于 resolve_in_root(session.workspace, path)。

    【需求点 二、2】会话的 workspace 即"当前选中工作区文件夹"，
    因此这里直接委托给唯一执行点 resolve_in_root()，保持边界语义一致。
    新代码请直接使用 resolve_in_root() 或 SessionFileGuard.resolve()。
    """
    return resolve_in_root(session.workspace, relative_path)


# 系统关键目录黑名单（第4章 4.2 规则4：禁止系统关键目录操作）
_SYSTEM_DIR_NAMES = (
    "windows", "system32", "syswow64", "program files", "program files (x86)",
    "programdata", "system", "usr", "etc", "bin", "sbin", "lib", "boot", "dev",
    "proc", "sys", "var", "private", "applications", "library",
)


def _hits_system_dir(path: Path) -> bool:
    """判定是否落在系统关键目录内（只匹配"根级系统目录"，避免误伤用户同名子目录）。"""
    norm = _normalize(path)
    roots: list[Path] = []
    if path.drive:
        roots.append(Path(path.drive + os.sep))
    else:
        roots.append(Path(os.sep))
    for root in roots:
        if norm == _normalize(root):
            return True
        for name in _SYSTEM_DIR_NAMES:
            blocked = root / name
            if norm == _normalize(blocked) or norm.startswith(_normalize(blocked) + os.sep):
                return True
    return False


def assert_no_cross_session(session_id: str, other_session_id: str) -> None:
    """显式跨会话访问断言（第2章 2.2 规则1）。"""
    if session_id != other_session_id:
        raise SecurityViolation(
            f"禁止跨会话读写：{session_id} -> {other_session_id}",
            code="CROSS_SESSION_DENIED",
            detail={"session_id": session_id, "target_session_id": other_session_id},
        )


# ==========================================================================
# 【BUG-B 2/3】工作区目录前置校验：存在性 + 读权限（+ 可选写权限）
#   唯一执行点：所有 Agent 执行文件相关任务前必须先过这里。
#   校验失败 → 抛 WorkspaceUnavailable（含统一可读中文文案），
#   由上层终止对应子任务并写入思考链路，绝不下发空路径给 Agent。
# ==========================================================================
def probe_workspace_root(root: str | Path, *, need_write: bool = False) -> dict:
    """探测工作区根目录可用性，返回结构化结果（不抛异常，供接口/展示复用）。

    返回：{"ok": bool, "path": str, "reason": str, "message": str,
           "exists": bool, "is_dir": bool, "readable": bool, "writable": bool}
    """
    raw = str(root or "").strip()
    result = {
        "ok": False, "path": raw, "reason": WORKSPACE_UNAVAILABLE_NOT_FOUND,
        "message": workspace_unavailable_message(raw, WORKSPACE_UNAVAILABLE_NOT_FOUND),
        "exists": False, "is_dir": False, "readable": False, "writable": False,
    }
    if not raw:
        return result

    path = Path(raw)
    try:
        exists = path.exists()
    except OSError:
        exists = False
    result["exists"] = bool(exists)
    if not exists:
        return result

    try:
        is_dir = path.is_dir()
    except OSError:
        is_dir = False
    result["is_dir"] = bool(is_dir)
    if not is_dir:
        result["reason"] = WORKSPACE_UNAVAILABLE_NOT_DIR
        result["message"] = workspace_unavailable_message(raw, WORKSPACE_UNAVAILABLE_NOT_DIR)
        return result

    # 读权限：os.access 在 Windows 上对 ACL 判定有限，因此再做一次真实的目录列举探测
    readable = False
    try:
        readable = os.access(raw, os.R_OK)
    except OSError:
        readable = False
    if readable:
        try:
            next(iter(os.scandir(raw)), None)
        except (PermissionError, OSError):
            readable = False
    result["readable"] = bool(readable)
    if not readable:
        result["reason"] = WORKSPACE_UNAVAILABLE_NO_READ
        result["message"] = workspace_unavailable_message(raw, WORKSPACE_UNAVAILABLE_NO_READ)
        return result

    result["writable"] = bool(os.access(raw, os.W_OK))
    if need_write and not result["writable"]:
        result["reason"] = WORKSPACE_UNAVAILABLE_NO_WRITE
        result["message"] = workspace_unavailable_message(raw, WORKSPACE_UNAVAILABLE_NO_WRITE)
        return result

    result["ok"] = True
    result["reason"] = ""
    result["message"] = ""
    return result


def assert_workspace_readable(root: str | Path, *, need_write: bool = False) -> None:
    """【BUG-B 2/3】执行文件任务前强制校验工作区目录；不可用即抛 WorkspaceUnavailable。"""
    probe = probe_workspace_root(root, need_write=need_write)
    if not probe["ok"]:
        raise WorkspaceUnavailable(probe["path"], probe["reason"], detail={
            "exists": probe["exists"], "is_dir": probe["is_dir"],
            "readable": probe["readable"], "writable": probe["writable"],
        })


def _harden_dir(path: Path) -> None:
    """目录权限收敛（类 Unix 平台收紧到 0700；Windows 依赖 ACL 默认策略）。"""
    if sys.platform.startswith("win"):
        return
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass


def harden_file(path: Path) -> None:
    """密钥文件权限收敛为 0600（第6章 6.1 API Key 安全存储规则）。"""
    if sys.platform.startswith("win"):
        return
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
