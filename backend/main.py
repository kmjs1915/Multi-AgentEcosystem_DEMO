# -*- coding: utf-8 -*-
"""
FastAPI 服务层（第1章 1.4 第3层 / 第8章 后端核心接口清单）

架构文档第8章 固定接口清单：
    GET  /api/config                 获取配置
    POST /api/config                 保存配置
    POST /api/config/test-key        测试密钥连通性
    POST /api/session/chat           发起对话任务
    GET  /api/task/{task_id}         获取任务详情、思考过程、Token数据
    GET  /api/approval/list          获取审批记录
    POST /api/approval/submit        提交审批结果
    POST /api/background/upload      上传自定义背景

为使第7章前端规范可闭环（会话列表、消息回填、审批面板清空、背景持久化）而必须存在的
补充接口，已在下文逐一标注【补充】并说明理由，不做任何多余扩展。

其他硬约束落实点：
  - 6.3 局域网访问：监听 0.0.0.0:5090；后台登录口令 bcrypt；高危审批开关永久开启
  - 6.2 文件上传：单文件 50MB、白名单、禁可执行、上传隔离到当前会话目录
  - 2.4 Token统计：状态栏数据全部来自后端真实采集值
  - 9.1 异常处理：全链路兜底，模型/向量库异常一律降级不崩溃
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator

# 【需求点 BUG-NEW2】交互交付Agent 的 provider：Kimi 厂商（与文档信息Agent 共用一份密钥）
AGENT_BINDINGS_HINT_PROVIDER = "kimi"

# 【需求点 Bug2】SSE 响应头：禁用缓冲与缓存，保证思维链逐条实时到达前端
_SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}

# 【需求点 Bug2】前端「先订阅、后 POST」时通道可能尚未创建：等待参数
_SSE_CHANNEL_WAIT_SECONDS = 30.0       # 最多等待通道出现的时间
_SSE_CHANNEL_POLL_SECONDS = 0.2        # 探测间隔

from backend.agents import agent_registry_info
from backend.services.approval_center import ApprovalError
from backend.services.approval_snapshot import (
    TaskSnapshotService,
    outcome_to_approved,
)
from backend.services.runtime import EcosystemRuntime
from backend.utils.constants import (
    AGENT_BINDINGS,
    AGENT_CODE,
    AGENT_DELIVERY,
    AGENT_DISPATCH,
    APPROVAL_OUTCOMES,
    APPROVAL_OUTCOME_ALLOWED_ONCE,
    APPROVAL_OUTCOME_REJECTED,
    APPROVAL_RESUME_TIMEOUT_SECONDS,
    APPROVAL_TIMEOUT_SECONDS,
    CONFIG_STATE_LABEL,
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_WORKSPACE_ID,
    ERR_TIMER_RECORD_FAILED,
    ERR_WORKSPACE_UNAVAILABLE,
    KEY_ERROR_MISSING,
    MSG_TYPE_APPROVAL_RESULT,
    PROVIDER_CAPABILITIES,
    PROVIDERS,
    SECURITY_META_CATEGORY_SESSION,
    SESSION_FILES_UNAVAILABLE_MESSAGE,
    STATUS_FAILED,
    STATUS_FINISHED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_WAITING_APPROVAL,
    THINK_LEVEL_ERROR,
    THINK_LEVEL_INFO,
    WORKSPACE_ACCESS_DENIED_MESSAGE,
    WORKSPACE_MARKER_FILE,
    is_terminal_status,
)
from backend.utils.paths import (
    SecurityViolation,
    WorkspaceUnavailable,
    display_relative,
    is_system_meta_file,
    is_within,
    is_workspace_file,
    make_system_meta_checker,
    probe_workspace_root,
    safe_relative_to,
)
from backend.utils.security import check_upload

# 项目根目录：backend/main.py -> backend -> 项目根
BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
BACKGROUND_DIR_NAME = "backgrounds"

# 会话令牌：签署密钥进程内随机生成（重启即失效，符合本地单机安全模型）
_SESSION_SECRET = secrets.token_bytes(32)
SESSION_COOKIE = "mae_session"
SESSION_TTL_SECONDS = 12 * 3600


# ==========================================================================
# 请求模型
# ==========================================================================
class ConfigSaveRequest(BaseModel):
    """POST /api/config 请求体（data 可含 provider_keys / base_urls / models）"""

    api_keys: dict[str, str] = Field(default_factory=dict)
    base_urls: dict[str, str] = Field(default_factory=dict)
    models: dict[str, str] = Field(default_factory=dict)
    background: dict | None = None
    complete_first_launch: bool = False
    # 【需求点 三、4】模型生态位自动补位开关（默认开启，可手动关闭）
    ecosystem_fallback_enabled: bool | None = None


class TestKeyRequest(BaseModel):
    provider: str
    api_key: str | None = None
    base_url: str | None = None
    model: str | None = None
    # 【需求点 Bug6 / Bug7】该厂商下需要逐个测试的模型标识清单
    models: list[str] = Field(default_factory=list)
    persist: bool = False


class VerifyRuntimeCallRequest(BaseModel):
    """【BUG-A 5】POST /api/config/verify-runtime-call 请求体：只指定 Agent 角色。"""

    agent: str


class ChatRequest(BaseModel):
    session_id: str | None = None
    message: str = ""
    model_hint: str | None = None
    attachments: list[dict] = Field(default_factory=list)
    # 【需求点 二、前端改造】所属工作区（未选中会话时由欢迎页创建会话并归组）
    workspace_id: str | None = None
    create_session: bool = False
    # 【需求点 Bug1】前端要求必须自动新建会话：为 true 时强制创建新会话（忽略传入 session_id）
    force_new_session: bool = False
    # 【需求点 Bug2】前端预先指定的任务 ID：前端据此在 POST 期间并行订阅 SSE 思维链
    task_id: str | None = None


class ApprovalSubmitRequest(BaseModel):
    """POST /api/approval/submit 请求体（需求伪代码第 6 步）。

    主参数（新契约）：task_id + outcome
      · task_id：任务号（可传根任务或子任务号；缺省时后端按 approval_id 反查）
      · outcome：allowed_once（✅ 执行一次）/ rejected（❌ 拒绝）
    兼容参数（历史前端/历史用例的入参原样保留，不破坏既有契约）：
      · approval_id / approved / session_id / operator / params_fingerprint / risk_level
    后端仍以 approved + 六道二次校验为准，前端提交的任何字段都不被直接信任。
    """

    approval_id: str | None = None
    # 【新增】需求指定的入参口径：task_id + outcome（allowed_once / rejected）
    task_id: str | None = None
    outcome: str | None = None
    # 历史契约：approved 布尔值（新前端建议只传 outcome，二者同向、outcome 优先）
    approved: bool | None = None
    session_id: str | None = None
    operator: str | None = None
    params_fingerprint: str | None = None
    # 前端提交的风险等级只用于一致性比对，绝不作为放行依据（第2章 2.2 规则4）
    risk_level: str | None = None
    # 【新增】是否等待调度循环恢复完成再返回。默认 False：
    #   立即返回"审批已受理"，恢复执行在后台进行并用 SSE 推送进度 ——
    #   彻底消除历史缺陷「审批提交后卡在提交中」（模型调用耗时导致 HTTP 长时间挂起）。
    wait_resume: bool = False
    # 【修复·可恢复性】审批已裁决但恢复执行失败/未启动时，允许显式请求"只补跑恢复"
    #   （例如后台恢复协程异常、裁决落库后进程被杀）。默认 False，绝不破坏防重放语义。
    retry_resume: bool = False

    @model_validator(mode="after")
    def _check(self) -> "ApprovalSubmitRequest":
        if self.outcome and self.outcome not in APPROVAL_OUTCOMES:
            raise ValueError(f"outcome 只允许 {' / '.join(APPROVAL_OUTCOMES)}")
        if self.approved is None and not self.outcome:
            raise ValueError("必须提供 outcome（allowed_once / rejected）或 approved")
        return self

    def resolved_approved(self) -> bool:
        """归一审批结论：outcome 优先，缺省回落到 approved。"""
        return outcome_to_approved(self.outcome, self.approved)

    def resolved_outcome(self) -> str:
        if self.outcome in APPROVAL_OUTCOMES:
            return str(self.outcome)
        return (APPROVAL_OUTCOME_ALLOWED_ONCE if self.resolved_approved()
                else APPROVAL_OUTCOME_REJECTED)


# ==========================================================================
# 【需求新增】任务快照 Model（SQLite 表 task_snapshot 的只读视图）
# ==========================================================================
class TaskSnapshotModel(BaseModel):
    """任务完整快照（GET /api/task/{task_id}/snapshot 响应体）。

    字段与建表 SQL 一一对应：
      session_id / task_id / status / completed_subtasks / remaining_subtasks /
      message_context / pending_approval / approval_deadline / create_time。
    """

    session_id: str = ""
    task_id: str = ""
    parent_task_id: str = ""
    status: str = ""
    stage: str = ""
    user_input: str = ""
    completed_subtasks: list[dict] = Field(default_factory=list)
    remaining_subtasks: list[dict] = Field(default_factory=list)
    message_context: list[dict] = Field(default_factory=list)
    pending_approval: dict = Field(default_factory=dict)
    loop_state: dict = Field(default_factory=dict)
    approval_deadline: float | None = None
    approval_countdown: dict = Field(default_factory=dict)
    create_time: float = 0.0
    create_time_text: str = ""
    updated_at: float = 0.0


# ==========================================================================
# 【需求新增】SSE 事件数据模型（普通 message 流 vs 审批特殊事件）
# ==========================================================================
SSE_EVENT_KINDS = (
    "task_created", "plan", "dispatch", "agent_step", "tool_call", "subtask_done",
    "task_status", "message",
    # ---- 审批专用事件（与普通 message 流严格区分） ----
    "approval_request",   # 高危操作触发，任务进入 waiting_approval
    "approval",           # 历史别名（向后兼容）
    "stream_paused",      # 流式输出已停止
    "approval_result",    # 审批裁决结果回灌（含超时自动拒绝）
    "approval_timeout",   # 30 秒超时自动拒绝（等价 rejected）
    "done",
)


class SSEEventModel(BaseModel):
    """统一 SSE 事件结构（所有事件都用这一个模型序列化，便于前后端对齐）。

    event = "message"  → 普通消息流（思维链/分派/子任务结果），前端只能追加渲染；
    event = 审批类事件 → 前端**仅**唤起/更新审批卡片，不得覆盖、清空已有聊天内容。
    """

    seq: int = 0
    event: str = "message"
    session_id: str = ""
    task_id: str = ""
    subtask_task_id: str = ""
    agent_role: str = ""
    model_label: str = ""
    provider: str = ""
    status: str = ""
    level: str = "info"
    step_type: str = ""
    step_index: int | None = None
    title: str = ""
    text: str = ""
    timestamp: int = 0
    # 审批专用字段（仅 approval_* 事件填充）
    approval_id: str = ""
    outcome: str = ""
    outcome_options: list[str] = Field(default_factory=list)
    operation_type: str = ""
    operation_desc: str = ""
    operation_params: str = ""
    danger_reason: str = ""
    risk_level: str = ""
    approval_deadline: float | None = None
    timeout_seconds: float | None = None
    timed_out: bool = False
    approved: bool | None = None

    @property
    def is_approval_event(self) -> bool:
        return self.event.startswith("approval") or self.event == "stream_paused"

    def to_sse(self) -> str:
        """序列化为 SSE 帧（event: <类型>\\ndata: <JSON>）。"""
        return f"event: {self.event}\ndata: {self.model_dump_json()}\n\n"


class LoginRequest(BaseModel):
    username: str
    password: str


class SessionCreateRequest(BaseModel):
    title: str | None = None
    # 【需求点 二、工作区分组】指定归属工作区
    workspace_id: str | None = None


# ---------- 【需求点 二、工作区分组管理】请求体 ----------
class WorkspaceCreateRequest(BaseModel):
    name: str | None = None
    # 【需求点 二、1】可直接用本地文件夹创建工作区
    folder_path: str | None = None


class WorkspaceRenameRequest(BaseModel):
    name: str


class SessionMoveRequest(BaseModel):
    session_id: str
    workspace_id: str


# ---------- 【需求点 二、1】工作区文件夹（本地目录）请求体 ----------
class FolderAddRequest(BaseModel):
    """添加工作区文件夹。

    · path      ：完整绝对路径（用户手动粘贴 / 前端已能取到时优先使用）
    · folder_name：浏览器系统文件夹选择器只暴露文件夹名称，服务端据此在常用位置定位候选
    · workspace_id：可选，指定复用到哪个工作区
    """

    path: str | None = None
    folder_name: str | None = None
    workspace_id: str | None = None


class FolderProbeRequest(BaseModel):
    """按文件夹名探测候选绝对路径（不落库，仅返回候选供用户确认）。"""

    folder_name: str


class ModelValidateRequest(BaseModel):
    """【需求点 一、Bug1 规则3】保存前校验模型标识。"""

    provider: str
    model: str | None = None


class VendorModelsRequest(BaseModel):
    """【需求点 Bug6 / Bug7】按厂商保存模型标识列表（一个厂商一次密钥 + 多个模型）。"""

    provider: str
    models: list[str] = Field(default_factory=list)


# ==========================================================================
# 认证（第6章 6.3 规则2/3：口令 bcrypt、禁止公网暴露）
# ==========================================================================
def _sign(payload: str) -> str:
    return hmac.new(_SESSION_SECRET, payload.encode("utf-8"), hashlib.sha256).hexdigest()


def issue_token(username: str) -> str:
    exp = int(time.time()) + SESSION_TTL_SECONDS
    payload = f"{username}|{exp}"
    raw = f"{payload}|{_sign(payload)}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def verify_token(token: str | None) -> str | None:
    if not token:
        return None
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        username, exp, signature = raw.rsplit("|", 2)
        payload = f"{username}|{exp}"
        if not hmac.compare_digest(signature, _sign(payload)):
            return None
        if int(exp) < int(time.time()):
            return None
        return username
    except Exception:  # noqa: BLE001
        return None


def _client_is_local(request: Request) -> bool:
    host = (request.client.host if request.client else "") or ""
    return host in ("127.0.0.1", "::1", "localhost", "testclient")


# ==========================================================================
# 应用工厂
# ==========================================================================
def create_app(runtime: EcosystemRuntime | None = None) -> FastAPI:
    rt = runtime or EcosystemRuntime()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        rt.logger.info(
            f"服务启动：http://{DEFAULT_HOST}:{DEFAULT_PORT}（局域网可访问，禁止公网暴露）",
            agent_role="system",
        )
        # ==================================================================
        # 【修复·断点恢复】启动即重建"待人工审批"的执行上下文：
        #   历史缺陷：PendingExecution 只在内存态，重启后用户点【✅ 执行一次】
        #   会因 APPROVAL_EXECUTION_EXPIRED 直接失败，任务永久卡在 waiting_approval。
        #   现在按 SQLite 快照重建并登记，同时拉起 30 秒超时看门狗。
        # ==================================================================
        try:
            recovery = rt.recover_pending_executions()
            if recovery.get("scanned"):
                app.state.approval_recovery = recovery
        except Exception as exc:  # noqa: BLE001 恢复失败不得阻止服务启动
            rt.logger.exception_log(
                error_code="APPROVAL_RECOVERY_FAILED",
                message=f"启动期待审批动作重建失败（已降级为不重建）：{exc}",
                agent_role="system",
            )
        try:
            yield
        finally:
            await rt.shutdown()
            rt.logger.info("服务已关闭", agent_role="system")

    app = FastAPI(
        title="Multi-Agent Ecosystem（裸机版）",
        version="1.1",
        description="本地化多Agent协同开发工作台 —— 纯裸机运行、无Docker、目录隔离+白名单+人工审批+后端二次校验",
        lifespan=lifespan,
    )
    app.state.runtime = rt

    # ---------------- 统一异常兜底（第9章 9.1 / 2.3：不崩溃） ----------------
    @app.exception_handler(WorkspaceUnavailable)
    async def _workspace_handler(request: Request, exc: WorkspaceUnavailable):
        """【BUG-B 4】工作区目录不存在 / 无读权限 → 返回可读错误（前端直接展示）。"""
        rt.logger.exception_log(error_code=exc.code, message=str(exc))
        return JSONResponse(status_code=409, content={
            "ok": False, "error": exc.code, "message": str(exc),
            "reason": exc.reason, "path": exc.path, "detail": exc.detail,
        })

    @app.exception_handler(SecurityViolation)
    async def _security_handler(request: Request, exc: SecurityViolation):
        rt.logger.exception_log(error_code=exc.code, message=str(exc))
        return JSONResponse(status_code=403, content={
            "ok": False, "error": exc.code, "message": str(exc), "detail": exc.detail,
        })

    @app.exception_handler(ApprovalError)
    async def _approval_handler(request: Request, exc: ApprovalError):
        rt.logger.exception_log(error_code=exc.code, message=str(exc))
        return JSONResponse(status_code=409, content={
            "ok": False, "error": exc.code, "message": str(exc),
        })

    @app.exception_handler(Exception)
    async def _unhandled_handler(request: Request, exc: Exception):
        import traceback
        rt.logger.exception_log(
            error_code="UNHANDLED_HTTP_ERROR",
            message=f"{type(exc).__name__}: {exc}", stack=traceback.format_exc(),
        )
        return JSONResponse(status_code=500, content={
            "ok": False, "error": "INTERNAL_ERROR",
            "message": f"系统内部异常已被捕获（服务未崩溃）：{type(exc).__name__}: {exc}",
        })

    # ==================================================================
    # 认证网关（第6章 6.3）
    # ==================================================================
    PUBLIC_PATHS = {"/api/auth/status", "/api/auth/login", "/api/health"}
    PUBLIC_PREFIXES = ("/static", "/favicon.ico", "/api/background/file")

    @app.middleware("http")
    async def auth_gateway(request: Request, call_next):
        path = request.url.path
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES) or not path.startswith("/api/"):
            return await call_next(request)

        username = verify_token(request.cookies.get(SESSION_COOKIE)) or \
            verify_token(request.headers.get("X-MAE-Token"))
        if not username:
            # 本机回环访问允许免登录（单机自用场景），局域网访问强制登录
            if not _client_is_local(request):
                return JSONResponse(status_code=401, content={
                    "ok": False, "error": "UNAUTHORIZED",
                    "message": "未登录或会话已过期（禁止公网/局域网匿名访问）",
                })
            username = rt.config.public_config()["admin"]["username"]
        request.state.username = username
        return await call_next(request)

    # ==================================================================
    # 认证接口【补充】—— 第6章 6.3 要求"后台登录密码 bcrypt 哈希存储"，
    # 必然需要登录入口，否则该安全规则无法落实。
    # ==================================================================
    @app.get("/api/health")
    async def health():
        return {"ok": True, "service": "multi-agent-ecosystem", "version": "1.1",
                "root": str(rt.paths.root), "no_docker": True}

    @app.get("/api/auth/status")
    async def auth_status(request: Request):
        username = verify_token(request.cookies.get(SESSION_COOKIE))
        return {
            "ok": True,
            "authenticated": bool(username),
            "username": username,
            "local_bypass": _client_is_local(request),
            "must_change_password": rt.config.admin_must_change(),
        }

    @app.post("/api/auth/login")
    async def auth_login(payload: LoginRequest, response: Response):
        if not rt.config.verify_admin(payload.username, payload.password):
            rt.logger.warning(f"登录失败：{payload.username}", agent_role="system")
            return JSONResponse(status_code=401, content={
                "ok": False, "error": "BAD_CREDENTIALS", "message": "用户名或密码错误",
            })
        token = issue_token(payload.username)
        rt.logger.info(f"登录成功：{payload.username}", agent_role="system")
        resp = JSONResponse(content={"ok": True, "token": token, "username": payload.username,
                                     "must_change_password": rt.config.admin_must_change()})
        resp.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax",
                        max_age=SESSION_TTL_SECONDS)
        return resp

    @app.post("/api/auth/logout")
    async def auth_logout():
        resp = JSONResponse(content={"ok": True})
        resp.delete_cookie(SESSION_COOKIE)
        return resp

    # ==================================================================
    # 第8章  GET /api/config —— 获取配置
    # 密钥永不返回前端，只返回"是否已配置"+脱敏指纹（第6章 6.1 规则3）
    # ==================================================================
    @app.get("/api/config")
    async def get_config():
        cfg = rt.config.public_config()
        return {
            "ok": True,
            "config": cfg,
            "agents": agent_registry_info(),
            "providers_catalog": [
                {"provider": p["provider"], "name": p["name"],
                 "default_base_url": p["default_base_url"], "default_model": p["default_model"]}
                for p in PROVIDERS
            ],
            "limits": {
                "max_iterations": 20, "max_timeout_seconds": 1800, "max_retries": 2,
                "max_review_rejects": 2, "max_rate_limit_retries": 3,
                "key_test_timeout_seconds": 5, "max_upload_mb": 50,
            },
            "first_launch": {
                "completed": cfg["first_launch_completed"],
                "any_configured": rt.config.any_provider_configured(),
                "any_tested_ok": rt.config.any_provider_tested_ok(),
                # 第7章 7.4：无配置文件 → 强制弹出 API 配置向导
                "force_wizard": not cfg["first_launch_completed"],
            },
            # 【需求点 三、4】生态位补位开关与优先级说明（设置页展示）
            "ecosystem_fallback": {
                "enabled": cfg["ecosystem_fallback_enabled"],
                "priority": cfg["ecosystem_fallback_priority"],
                "priority_text": cfg["ecosystem_fallback_priority_text"],
                "label": "开启模型生态位自动补位",
                "description": (
                    "专属模型不可用时，自动使用本机已连通测试通过的其他模型补齐 Agent 生态位；"
                    f"补位优先级 {cfg['ecosystem_fallback_priority_text']}。"
                    "关闭后不执行跨模型补位，模型失败直接任务失败。"
                ),
                "tested_ok_providers": rt.config.tested_ok_providers(),
                "provider_capabilities": {
                    pid: {
                        "name": meta["name"],
                        "caps": list(meta["caps"]),
                        "vision": meta["vision"],
                        "code_strength": meta["code_strength"],
                        "long_context_strength": meta["long_context_strength"],
                    }
                    for pid, meta in PROVIDER_CAPABILITIES.items()
                },
            },
            # 【需求点 BUG-NEW2】交互交付Agent 现绑定 Kimi k2.6（Kimi 厂商分组）
            "delivery_agent_model": {
                "agent": "交互交付Agent",
                "provider": AGENT_BINDINGS_HINT_PROVIDER,
                "model": AGENT_BINDINGS[AGENT_DELIVERY]["model"],
                "model_name": AGENT_BINDINGS[AGENT_DELIVERY]["model_name"],
                "note": "交互交付Agent 绑定 Kimi k2.6（kimi-k2.6），与文档信息Agent 共用 Kimi 厂商密钥",
            },
            # 七大 Agent ↔ 模型绑定快照（前端思维链表头 / 状态栏展示，与 Backend 唯一来源一致）
            "agent_model_bindings": rt.model_binding_snapshot(),
            "security": cfg["security"],
            "router_strict_mode": rt.config.public_config()["high_risk_approval_locked"],
            "no_docker": True,
        }

    # ==================================================================
    # 第8章  POST /api/config —— 保存配置（AES-256 加密落盘）
    # ==================================================================
    @app.post("/api/config")
    async def post_config(payload: ConfigSaveRequest):
        saved: list[str] = []
        skipped: list[str] = []
        keys_encrypted: list[dict] = []
        model_warnings: list[dict] = []
        valid_ids = {p["provider"] for p in PROVIDERS}

        for provider, key in (payload.api_keys or {}).items():
            if provider not in valid_ids:
                raise SecurityViolation(f"未知模型提供方：{provider}", code="UNKNOWN_PROVIDER")
            key = (key or "").strip()
            if not key:
                skipped.append(provider)
                continue
            # 【需求点 Bug1 排查要求】保存失败必须显式报错，不得静默失败
            try:
                rt.config.set_api_key(provider, key)
            except Exception as exc:  # noqa: BLE001
                rt.logger.exception_log(
                    error_code="KEY_SAVE_FAILED",
                    message=f"{provider} 密钥加密保存失败：{type(exc).__name__}: {exc}",
                    agent_role="system",
                )
                return JSONResponse(status_code=500, content={
                    "ok": False, "error": "KEY_SAVE_FAILED",
                    "message": f"{provider} 密钥保存失败（未落盘）：{exc}",
                    "hint": "请检查 config 目录是否可写、主密钥文件是否存在",
                })
            saved.append(provider)
            info = next((p for p in rt.config.public_config()["providers"]
                         if p["provider"] == provider), {})
            keys_encrypted.append({
                "provider": provider,
                "masked": info.get("masked", ""),
                "key_fingerprint": info.get("key_fingerprint", ""),
                "storage": "AES-256-GCM 密文 + SHA256 摘要（已回读校验）",
            })

        for provider, base_url in (payload.base_urls or {}).items():
            if provider in valid_ids and base_url:
                rt.config.update_provider_meta(provider, base_url=base_url)
        for provider, model in (payload.models or {}).items():
            if provider not in valid_ids or not model:
                continue
            # 【需求点 一、Bug1 规则3】保存前校验模型标识，提前拦截平台不支持的名称
            check_result = rt.config.validate_provider_model(provider, model)
            if not check_result["valid"]:
                rt.logger.exception_log(
                    error_code="INVALID_MODEL_NAME",
                    message=f"{provider} 模型标识校验失败：{check_result['message']}",
                    agent_role="system",
                )
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": "INVALID_MODEL_NAME",
                    "message": check_result["message"],
                    "hint": check_result["hint"],
                    "provider": provider, "model": model,
                    "known_models": check_result["known_models"],
                })
            if check_result["level"] == "warn":
                model_warnings.append({
                    "provider": provider, "model": model,
                    "message": check_result["message"], "hint": check_result["hint"],
                })
            rt.config.update_provider_meta(provider, model=model)

        if payload.background:
            rt.config.set_background(
                bg_type=str(payload.background.get("type") or "preset"),
                value=str(payload.background.get("value") or "night"),
            )

        # 【需求点 三、4】模型生态位自动补位开关
        if payload.ecosystem_fallback_enabled is not None:
            rt.config.set_ecosystem_fallback(bool(payload.ecosystem_fallback_enabled))

        completed = None
        if payload.complete_first_launch:
            completed, message = rt.config.complete_first_launch()
            if not completed:
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": "WIZARD_INCOMPLETE", "message": message,
                })

        rt.logger.info(f"配置已保存：saved={saved} skipped={skipped}", agent_role="system")
        return {
            "ok": True,
            "saved": saved,
            "skipped_no_key": skipped,
            "keys_encrypted": keys_encrypted,
            # 【需求点 一、Bug1 规则3】模型标识告警（允许保存但提醒风险）
            "model_warnings": model_warnings,
            "config": rt.config.public_config(),
            "key_storage": "AES-256-GCM 加密 + SHA256 篡改校验，明文密钥不出后端",
        }

    # ==================================================================
    # 【需求点 一、Bug1 规则3】模型标识校验接口（保存前提前提示）
    # ==================================================================
    @app.post("/api/config/validate-model")
    async def validate_model(payload: ModelValidateRequest):
        valid_ids = {p["provider"] for p in PROVIDERS}
        if payload.provider not in valid_ids:
            raise SecurityViolation(f"未知模型提供方：{payload.provider}", code="UNKNOWN_PROVIDER")
        result = rt.config.validate_provider_model(payload.provider, payload.model)
        rt.logger.info(
            f"模型标识校验：provider={payload.provider} model={payload.model} "
            f"valid={result['valid']} level={result['level']}",
            agent_role="system",
        )
        return {"ok": result["valid"], "provider": payload.provider, **result}

    # ==================================================================
    # 【需求点 Bug6 / Bug7】厂商分组配置接口
    #   GET  /api/config/vendors        厂商分组视图（含每模型连通状态，绝不含明文密钥）
    #   POST /api/config/vendor-models  保存某厂商下的多个模型标识
    #   POST /api/config/test-model     逐个模型测试连通性（同一厂商密钥）
    # ==================================================================
    @app.get("/api/config/vendors")
    async def config_vendors():
        groups = rt.config.vendor_groups()
        return {
            "ok": True,
            "vendor_groups": groups,
            "agent_model_map": rt.config.agent_model_map(),
            "note": ("按厂商分组配置：一个厂商只填写一次 API Key / Base URL，"
                     "厂商内部映射多个模型标识；密钥以 AES-256-GCM 加密存储，"
                     "前端只能看到 SHA256 指纹，明文永不返回。"),
        }

    @app.post("/api/config/vendor-models")
    async def config_vendor_models(payload: VendorModelsRequest):
        """保存厂商下的模型标识列表（主模型 + 其余模型标识）。"""
        result = rt.config.set_provider_models(payload.provider, payload.models)
        rt.logger.info(
            f"厂商模型标识已保存：{payload.provider} -> {', '.join(result['models']) or '(空)'}",
            agent_role="system",
        )
        group = next((g for g in rt.config.vendor_groups()
                      if g["provider"] == payload.provider), None)
        return {"ok": True, **result, "vendor_group": group}

    @app.post("/api/config/test-model")
    async def config_test_model(payload: TestKeyRequest):
        """【需求点 Bug6 规则3 / Bug7】分别测试该厂商下每一个模型标识的连通性。

        · 密钥优先级：本次填写的 key > 该厂商已加密存储的 key；
        · 每个模型独立返回结构化结果，前端逐条展示；
        · 结果写入该厂商的 model_test_results，刷新页面后仍然保留。

        【BUG-A 2/5】测试使用的 BaseURL / 模型标识 / 密钥与**真实业务调用同源**，
        并回传密钥 SHA256 供前端核对「测试用的密钥是否就是业务调用会读到的密钥」。
        """
        valid_ids = {p["provider"] for p in PROVIDERS}
        if payload.provider not in valid_ids:
            raise SecurityViolation(f"未知模型提供方：{payload.provider}", code="UNKNOWN_PROVIDER")

        api_key_from_form = (payload.api_key or "").strip()
        api_key = api_key_from_form
        if not api_key:
            api_key = rt.config.get_api_key(payload.provider)
        if not api_key:
            meta = rt.config.get_provider_meta(payload.provider)
            return {
                "ok": False, "provider": payload.provider, "name": meta["name"],
                "error_code": KEY_ERROR_MISSING, "error_category": "未配置",
                "message": "尚未配置该厂商的 API Key，请先填写后再测试",
                "hint": "该厂商下所有模型共用同一密钥，填写一次即可逐个测试。",
                "results": [], "passed": [], "failed": [],
            }

        target_models = [m for m in (payload.models or []) if str(m).strip()]
        if not target_models:
            target_models = [m["model"] for m in rt.config.vendor_models(payload.provider)]
        if payload.model and payload.model not in target_models:
            target_models.append(payload.model)

        results: list[dict] = []
        for mid in target_models:
            outcome = await rt.model_client.test_key(
                payload.provider, api_key,
                base_url=payload.base_url, model=mid,
            )
            rt.config.mark_model_test_result(
                payload.provider, mid, outcome["ok"],
                message=outcome.get("message", ""),
            )
            results.append({
                "model": mid,
                "ok": bool(outcome["ok"]),
                "error_code": outcome.get("error_code", ""),
                "error_category": outcome.get("error_category", ""),
                "message": outcome.get("message", ""),
                "hint": outcome.get("hint", ""),
                "raw_detail": outcome.get("raw_detail", ""),
                "http_status": outcome.get("http_status", 0),
                "elapsed_ms": outcome.get("elapsed_ms", 0),
                "model_warning": outcome.get("model_warning", ""),
            })

        if payload.persist and any(r["ok"] for r in results):
            try:
                rt.config.set_api_key(payload.provider, api_key)
            except Exception as exc:  # noqa: BLE001 保存失败必须显式回传
                rt.logger.exception_log(
                    error_code="KEY_SAVE_FAILED",
                    message=f"{payload.provider} 密钥保存失败：{exc}", agent_role="system",
                )
                return {
                    "ok": False, "provider": payload.provider,
                    "error_code": "KEY_SAVE_FAILED", "error_category": "保存失败",
                    "message": f"连通性测试已完成，但密钥加密落盘失败：{exc}",
                    "hint": "请检查 config 目录写入权限与主密钥文件，然后重新保存",
                    "results": results,
                    "passed": [r["model"] for r in results if r["ok"]],
                    "failed": [r["model"] for r in results if not r["ok"]],
                }

        group = next((g for g in rt.config.vendor_groups()
                      if g["provider"] == payload.provider), None)
        passed = [r["model"] for r in results if r["ok"]]
        # ==================================================================
        # 【BUG-A 2/5】连通测试与真实业务调用同源核对：
        #   本次测试用的密钥 SHA256 必须与"真实业务调用会读到的密钥"一致，
        #   否则测试成功也不代表业务可调用（历史坑：测试用的 key 没落盘 / 读错厂商）。
        # ==================================================================
        saved_sha = rt.config.key_sha256(payload.provider)
        tested_sha = next((r.get("key_sha256") for r in results if r.get("key_sha256")), "")
        key_matches_saved = bool(tested_sha) and tested_sha == saved_sha
        if passed and not key_matches_saved:
            key_verdict = (
                "⚠️ 本次测试用的密钥与已保存的密钥**不一致**（或密钥尚未保存）："
                "真实业务调用读的是已保存密钥，可能出现「测试成功但实际调用失败」。"
                "请点击「保存」把密钥加密落盘后再次测试。"
            )
        elif passed:
            key_verdict = "✅ 测试用的密钥与真实业务调用读取的密钥一致（SHA256 相同），配置可用于真实调用。"
        else:
            key_verdict = "本次连通性测试未全部通过，请按各模型的错误原因排查。"
        return {
            "ok": bool(passed), "provider": payload.provider,
            "name": rt.config.get_provider_meta(payload.provider)["name"],
            "base_url": payload.base_url or rt.config.get_provider_meta(payload.provider)["base_url"],
            "results": results, "passed": passed,
            "failed": [r["model"] for r in results if not r["ok"]],
            "key_persisted": bool(payload.persist and passed),
            "vendor_group": group,
            "message": (f"该厂商下 {len(passed)}/{len(results)} 个模型连通正常"
                        if passed else "该厂商下全部模型连通性测试均未通过"),
            "timeout_limit_seconds": 5,
            # ---- 【BUG-A 2/5】同源核对结论 ----
            "tested_key_sha256": tested_sha,
            "saved_key_sha256": saved_sha,
            "key_matches_saved": key_matches_saved,
            "tested_key_from_form": bool(api_key_from_form),
            "key_verdict": key_verdict,
            "runtime_note": ("真实业务调用使用与本测试相同的 provider/base_url/模型标识；"
                             "测试通过且密钥一致时，业务调用不应触发生态位补位。"),
        }

    # ==================================================================
    # 【BUG-A 1/2/5】GET /api/config/runtime-targets
    #   「连通测试 ↔ 真实业务调用」一致性核对（同一个配置数据源）
    #   逐条给出：Agent → 绑定厂商/模型 → 实际调用厂商/模型/端点 → 密钥指纹/来源 →
    #   该模型连通测试状态 → 结论（一致 / 存在问题）。
    #   用于验证「Kimi k2.6 测试成功但实际调用被生态位补位」这类问题。
    # ==================================================================
    @app.get("/api/config/runtime-targets")
    async def config_runtime_targets(agent: str | None = Query(default=None)):
        rows = rt.model_client.consistency_report()
        if agent:
            rows = [r for r in rows if r["agent"] == agent]
        problems = [f"{r['agent']}：{'；'.join(r['issues'])}" for r in rows if r["issues"]]
        return {
            "ok": True,
            "targets": rows,
            "all_consistent": not problems,
            "problems": problems,
            "state_labels": CONFIG_STATE_LABEL,
            "note": ("连通测试与真实业务调用共用同一套配置数据源："
                     "厂商/模型来自 constants.AGENT_BINDINGS，BaseURL/密钥来自加密配置库。"
                     "任一 Agent 的 issues 非空即说明「测试成功 ≠ 实际可调用」，"
                     "请按 issues 修正配置。"),
        }

    # ==================================================================
    # 【需求点 Bug1 规则3】GET /api/model-constraints
    #   模型参数约束清单（唯一权威来源 constants.MODEL_CONSTRAINTS）：
    #   列出所有"平台强制固定参数"的模型，供前端提示与排查使用；
    #   后续新模型有强制参数时，只需在该清单中扩展即可，无需改动调用链。
    # ==================================================================
    @app.get("/api/model-constraints")
    async def model_constraints():
        report = rt.model_client.model_constraints_report()
        return {"ok": True, **report}

    # ==================================================================
    # 【BUG-A 5】POST /api/config/verify-runtime-call
    #   真实业务调用链路验证：按运行时实际解析出的目标发一次真实请求
    #   （与业务流程完全相同的 provider/model/base_url/密钥，不使用前端参数覆盖）
    # ==================================================================
    @app.post("/api/config/verify-runtime-call")
    async def config_verify_runtime_call(payload: VerifyRuntimeCallRequest):
        row = next((r for r in rt.model_client.consistency_report()
                    if r["agent"] == payload.agent), None)
        if row is None:
            raise SecurityViolation(f"未知 Agent 角色：{payload.agent}", code="UNKNOWN_AGENT")
        if not row["key_present"]:
            return {"ok": False, "agent": payload.agent, "target": row,
                    "error_code": KEY_ERROR_MISSING,
                    "message": f"厂商 {row['provider_name']} 密钥缺失（配置为空），"
                               "该情况下允许生态位补位",
                    "hint": "请先在设置面板填写并保存该厂商 API Key，然后重新验证。"}

        # 完全按运行时目标调用（不传 base_url / model 覆盖）
        outcome = await rt.model_client.test_key(
            row["provider"], rt.config.get_api_key(row["provider"]),
            base_url=rt.config.get_provider_meta(row["provider"])["base_url"],
            model=row["model"],
        )
        rt.logger.task_log(
            session_id="", task_id="", agent_role=payload.agent,
            event="config.verify_runtime_call",
            level="info" if outcome["ok"] else "error",
            detail=(f"运行态调用验证 | provider={row['provider']} model={row['model']} "
                    f"url={outcome.get('tested_url')} key指纹={row['key_fingerprint']} "
                    f"http_status={outcome.get('http_status')} ok={outcome['ok']} "
                    f"message={outcome.get('message')}"),
        )
        return {
            "ok": bool(outcome["ok"]),
            "agent": payload.agent,
            "target": row,
            "error_code": outcome.get("error_code", ""),
            "message": outcome.get("message", ""),
            "hint": outcome.get("hint", ""),
            "http_status": outcome.get("http_status", 0),
            "tested_url": outcome.get("tested_url", ""),
            "elapsed_ms": outcome.get("elapsed_ms", 0),
            "key_fingerprint": row["key_fingerprint"],
            "test_state": row["test_state"],
            "fallback_allowed": (not row["key_present"]),
            "note": ("本次验证使用与业务流程完全相同的 provider/model/base_url/密钥；"
                     "通过后该 Agent 的真实调用不应触发生态位补位。"),
        }

    # ==================================================================
    # 第8章  POST /api/config/test-key —— 测试密钥连通性（超时 5 秒）
    # ==================================================================
    @app.post("/api/config/test-key")
    async def test_key(payload: TestKeyRequest):
        valid_ids = {p["provider"] for p in PROVIDERS}
        if payload.provider not in valid_ids:
            raise SecurityViolation(f"未知模型提供方：{payload.provider}", code="UNKNOWN_PROVIDER")

        api_key = (payload.api_key or "").strip()
        transient = not api_key
        if transient:
            api_key = rt.config.get_api_key(payload.provider)
        if not api_key:
            # 【需求点 Bug1】同样返回结构化错误，前端可完整渲染（含错误类别与排查建议）
            return {
                "ok": False,
                "error_code": KEY_ERROR_MISSING,
                "error_category": "未配置",
                "message": "尚未配置该模型的 API Key，请先填写后再测试",
                "hint": "请在「API Key」输入框中填入该模型平台的密钥，然后点击「测试连通」；"
                        "填入的密钥会以 AES-256-GCM 加密保存到本机 config 目录。",
                "raw_detail": "",
                "provider": payload.provider,
                "name": rt.config.get_provider_meta(payload.provider)["name"],
                "model": rt.config.get_provider_meta(payload.provider)["model"],
                "base_url": rt.config.get_provider_meta(payload.provider)["base_url"],
                "elapsed_ms": 0,
                "http_status": 0,
                "timeout_limit_seconds": 5,
                "key_persisted": False,
            }

        result = await rt.model_client.test_key(
            payload.provider, api_key,
            base_url=payload.base_url, model=payload.model,
        )
        rt.config.mark_test_result(payload.provider, result["ok"])
        if payload.persist and result["ok"] and not transient:
            try:
                # 【需求点 Bug1】保存失败必须显式回传，不得静默失败
                rt.config.set_api_key(payload.provider, api_key)
            except Exception as exc:  # noqa: BLE001
                rt.logger.exception_log(
                    error_code="KEY_SAVE_FAILED",
                    message=f"{payload.provider} 密钥保存失败：{exc}", agent_role="system",
                )
                return {
                    "ok": False,
                    "error_code": "KEY_SAVE_FAILED",
                    "error_category": "保存失败",
                    "message": f"连通性测试通过，但密钥加密落盘失败：{exc}",
                    "hint": "请检查 config 目录写入权限与主密钥文件，然后重新保存",
                    "provider": payload.provider,
                    "name": rt.config.get_provider_meta(payload.provider)["name"],
                    "elapsed_ms": result["elapsed_ms"],
                    "http_status": result.get("http_status", 0),
                    "timeout_limit_seconds": 5,
                    "key_persisted": False,
                }

        # 【需求点 Bug1】完整透传后端结构化错误信息（可读中文原因），前端不截断展示
        return {
            "ok": result["ok"],
            "error_code": result.get("error_code", ""),
            "error_category": result.get("error_category", ""),
            "message": result.get("message", ""),
            "hint": result.get("hint", ""),
            "raw_detail": result.get("raw_detail", ""),
            "provider": payload.provider,
            "name": result.get("name") or rt.config.get_provider_meta(payload.provider)["name"],
            "model": result.get("model", ""),
            "base_url": result.get("base_url", ""),
            "elapsed_ms": result["elapsed_ms"],
            "http_status": result.get("http_status", 0),
            "timeout_limit_seconds": 5,
            "key_persisted": bool(payload.persist and result["ok"]),
        }

    # ==================================================================
    # 第8章  POST /api/session/chat —— 发起对话任务（完整多Agent链路）
    # ==================================================================
    @app.post("/api/session/chat")
    async def session_chat(payload: ChatRequest):
        if not payload.message.strip() and not payload.attachments:
            return JSONResponse(status_code=400, content={
                "ok": False, "error": "EMPTY_MESSAGE", "message": "消息内容不能为空",
            })

        # ==================================================================
        # 【需求点 Bug2 边界约束1】待审批状态下拦截用户新提交消息
        #   只要当前会话还有 waiting_approval 的任务（快照仍在等待审批），
        #   一律拦截本次提交，防止新旧任务互相干扰；提示用户先完成审批。
        # ==================================================================
        guard_session = payload.session_id
        if guard_session and not (payload.force_new_session or payload.create_session):
            snapshot = await asyncio.to_thread(rt.pending_approval_snapshot, guard_session)
            if snapshot:
                pending_rows = rt.db.list_approvals(
                    session_id=guard_session, state="pending", limit=1)
                pending = pending_rows[0] if pending_rows else {}
                message = "存在待审批任务，请先完成审批"
                rt.logger.task_log(
                    session_id=guard_session, task_id=str(snapshot.get("task_id") or ""),
                    agent_role=AGENT_CODE, event="chat.blocked.pending_approval", level="warn",
                    detail=(f"用户新提交被拦截：{message}｜"
                            f"待审批审批单={pending.get('approval_id') or '-'}｜"
                            f"操作类型={pending.get('operation_type') or '-'}"),
                )
                return JSONResponse(status_code=409, content={
                    "ok": False,
                    "error": "PENDING_APPROVAL_EXISTS",
                    "message": message,
                    "detail": ("当前任务处于 waiting_approval（等待人工审批）状态，"
                               "审批未完成前不允许提交新消息"),
                    "session_id": guard_session,
                    "task_id": str(snapshot.get("task_id") or ""),
                    "task_status": STATUS_WAITING_APPROVAL,
                    "pending_approval": {
                        "approval_id": pending.get("approval_id"),
                        # 【修复】同时给出根任务号与子任务号：根任务号用于断点恢复，
                        #   子任务号用于审计定位（前端提交时用根任务号即可反查到审批单）。
                        "task_id": pending.get("task_id"),
                        "root_task_id": str(snapshot.get("task_id") or ""),
                        "risk_level": pending.get("risk_level"),
                        "operation_type": pending.get("operation_type"),
                        "operation_desc": pending.get("operation_desc"),
                        "operation_params": pending.get("operation_params"),
                        "danger_reason": pending.get("danger_reason"),
                        "agent_role": pending.get("agent_role"),
                        "state": pending.get("state"),
                        # 【第三轮·Bug1】等待用户点击阶段不计时 → 不返回倒计时字段，
                        #   只给出"提交后执行链路限时"的说明值（前端显示"无超时限制"）。
                        "resume_state": pending.get("resume_state") or "idle",
                        "resume_timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS),
                        "outcome_options": ["allowed_once", "rejected"],
                    } if pending else None,
                    "approval_submit_url": "/api/approval/submit",
                    "completed_subtasks": len(snapshot.get("completed") or []),
                    "remaining_subtasks": len(snapshot.get("remaining") or []),
                })

        uploaded_images: list[dict] = []
        saved_files: list[dict] = []
        for att in payload.attachments or []:
            name = str(att.get("name") or "unnamed")
            raw_b64 = str(att.get("data_base64") or "")
            if not raw_b64:
                continue
            try:
                data = base64.b64decode(raw_b64, validate=True)
            except Exception:
                # 兼容 data URL 形式
                try:
                    data = base64.b64decode(raw_b64.split(",", 1)[-1])
                except Exception:
                    raise SecurityViolation(f"附件数据非法：{name}", code="INVALID_ATTACHMENT")
            check = check_upload(name, len(data), content_head=data[:16])
            if not check.ok:
                raise SecurityViolation(check.reason, code=check.code,
                                        detail={"filename": name, "size": len(data)})
            uploaded_images.append({"filename": name, "data": data})
            saved_files.append({"name": name, "size": len(data), "type": check.resource_type})

        # ==================================================================
        # 【需求点 Bug1】会话自动创建：由后端先行确定本次会话 ID，
        #   保证「计时器 / run_pipeline / 前端跳转」三处使用**同一个** 会话 ID。
        #   历史缺陷：这里生成的 timer_session_id 没有传给 run_pipeline，
        #   而 run_pipeline 在 session_id 为空时又自行新建了一个会话，
        #   导致前端"输入后并未进入新会话"（旧会话仍被选中）。
        # ==================================================================
        target_session_id = payload.session_id
        if payload.force_new_session or payload.create_session or not target_session_id:
            target_session_id = rt.router.new_session_id()
        payload_session_id = target_session_id

        # 【需求点 二、2-a】任务正式开始提交、Agent 即将开始执行 → 启动计时器（新任务清零）
        #   计时失败绝不阻断任务（第2章 2.3 降级兜底），仅写错误日志
        timer_session_id = payload_session_id
        try:
            rt.start_task_timer(
                session_id=timer_session_id,
                title=(payload.message or "新会话").strip().splitlines()[0][:40] or "新会话",
            )
        except Exception as exc:  # noqa: BLE001
            rt.logger.exception_log(
                error_code=ERR_TIMER_RECORD_FAILED,
                message=f"任务计时启动失败（已降级，任务继续执行）：{type(exc).__name__}: {exc}",
                session_id=timer_session_id, agent_role=AGENT_CODE,
            )

        # ==================================================================
        # 【需求点 二、1 + Bug1】会话**先**归入目标工作区，再执行任务。
        #   历史缺陷：move_session 在 run_pipeline 之后执行，导致任务执行期间
        #   bind_session 读到的归属仍是"默认工作区"，文件被写到系统隔离目录
        #   （用户在自己的工作区文件夹里看不到新建文件）。
        # ==================================================================
        if payload.workspace_id and rt.db.get_workspace(payload.workspace_id):
            rt.db.upsert_session(
                payload_session_id,
                (payload.message or "新会话").strip().splitlines()[0][:40] or "新会话",
                workspace_id=payload.workspace_id,
            )
            rt.db.move_session(payload_session_id, payload.workspace_id)

        result = await rt.run_pipeline(
            session_id=payload_session_id,
            user_input=payload.message,
            uploaded_images=uploaded_images,
            model_hint=payload.model_hint,
            client_task_id=payload.task_id,
        )
        # 兜底：run_pipeline 内部可能新建会话（session_id 为空时），再次确保归属正确
        if payload.workspace_id and rt.session_workspace(result.session_id) != payload.workspace_id:
            rt.db.move_session(result.session_id, payload.workspace_id)
        out = result.to_dict()
        out["uploads"] = saved_files
        out["workspace_id"] = rt.session_workspace(result.session_id)
        # 【需求点 Bug1】明确告知前端"本次是否新建了会话"，前端据此跳转到新会话
        out["session_created"] = bool(
            payload.force_new_session or payload.create_session
            or not payload.session_id
        )
        # 【需求点 二、2】任务计时器视图：本次任务耗时（真实后端数据，前端直接展示）
        out["task_timer"] = rt.task_timer_view(result.session_id)
        return {"ok": True, "result": out}

    # ==================================================================
    # 【需求点 二、任务执行计时器】计时查询 / 取消
    #   · 计时启动与停止全部由后端状态机驱动，前端只做展示与轮询
    #   · 轮询接口必须放在 /api/task/{task_id} 之前声明，避免被路径参数捕获
    # ==================================================================
    @app.get("/api/task/timer")
    async def task_timer(session_id: str = Query(...)):
        """当前计时器视图：运行中返回实时已耗时；无运行任务时返回上一次任务耗时 / 「未开始任务」。"""
        from backend.utils.paths import validate_session_id
        validate_session_id(session_id)
        return {"ok": True, "timer": rt.task_timer_view(session_id), "server_time": time.time()}

    @app.get("/api/task/timer/history")
    async def task_timer_history(session_id: str = Query(...),
                                 limit: int = Query(default=50, ge=1, le=500)):
        """历史会话的任务耗时记录（读取会话元数据，兼容旧会话无记录）。"""
        from backend.utils.paths import validate_session_id
        validate_session_id(session_id)
        return {"ok": True, **rt.session_timer_history(session_id, limit=limit)}

    @app.post("/api/task/cancel")
    async def task_cancel(session_id: str = Query(...), task_id: str | None = Query(default=None),
                          reason: str | None = Query(default=None)):
        """【需求点 二、2-c】任务中途取消 / 终止 → 停止计时并写入会话任务记录。

        说明：按开发约束"只新增计时相关接口与前端组件、原有状态机逻辑不变"，
        本接口只做两件事：
          1) 停止该会话的计时（cancel_task_timer，写错误日志 + 计时记录 + 会话元数据）；
          2) 把进行中的根任务显式收敛为 failed（CANCELLED_BY_USER），
             避免出现"任务仍在 running 但计时已停"的不一致状态。
        """
        from backend.utils.paths import validate_session_id
        validate_session_id(session_id)
        cancel_reason = (reason or "用户取消 / 终止任务").strip()
        target_task_id = task_id or ""
        if not target_task_id:
            running = rt.db.query(
                "SELECT task_id FROM tasks WHERE session_id=? AND status IN ('running','pending') "
                "ORDER BY created_at DESC LIMIT 1", (session_id,))
            target_task_id = running[0]["task_id"] if running else ""

        timer_view = rt.cancel_task_timer(session_id, task_id=target_task_id, reason=cancel_reason)
        rt.logger.task_log(
            session_id=session_id, task_id=target_task_id, agent_role=AGENT_CODE,
            event="task.cancelled", level="warn",
            detail=f"任务被取消/终止，计时已停止：原因={cancel_reason}",
        )

        task_updated = False
        if target_task_id:
            row = rt.db.get_task(target_task_id)
            if row and row["status"] in ("running", "pending"):
                rt.db.update_task(target_task_id, status=STATUS_FAILED,
                                  error_code="CANCELLED_BY_USER",
                                  error_message=cancel_reason,
                                  finished_at=time.time())
                task_updated = True

        return {
            "ok": True,
            "session_id": session_id,
            "task_id": target_task_id,
            "task_status_updated": task_updated,
            "timer": timer_view or rt.task_timer_view(session_id),
            "message": "任务已取消，计时已停止",
        }

    # ==================================================================
    # 【需求点 Bug2】协同思维链流式输出（SSE）与快照接口
    #   GET /api/task/{task_id}/stream  —— 实时事件流（EventSource 订阅）
    #   GET /api/task/{task_id}/chain   —— 已产生链路的快照（刷新后补看）
    #   事件类型：task_created / plan / dispatch / agent_step / tool_call /
    #             subtask_done / approval / task_status / done
    #   说明：流式仅用于展示，任务状态机与消息总线结构体保持不变。
    # ==================================================================
    @app.get("/api/task/{task_id}/stream")
    async def task_stream(task_id: str, after: int = Query(default=0, ge=0)):
        stream = rt.stream.get(task_id)
        if stream is None:
            # 【需求点 Bug1/Bug2】前端会「先订阅 SSE、再发 POST」，此刻流式通道尚未创建。
            # 历史缺陷：此处立刻回一条 done 帧并关闭连接，导致真正的实时链路退化为 /chain 轮询。
            # 现在改为等待通道出现（最多 30 秒，期间发心跳），通道就绪后无缝续传历史+实时事件。
            async def _wait_then_stream():
                try:
                    yield ": wait-channel\n\n"        # 立即返回响应头，避免前端连接超时
                    waited = 0.0
                    while waited < _SSE_CHANNEL_WAIT_SECONDS:
                        await asyncio.sleep(_SSE_CHANNEL_POLL_SECONDS)
                        waited += _SSE_CHANNEL_POLL_SECONDS
                        found = rt.stream.get(task_id)
                        if found is not None:
                            for evt in found.history(after_seq=after):
                                yield evt.to_sse()
                            queue = found.subscribe()
                            try:
                                while True:
                                    try:
                                        evt = await asyncio.wait_for(queue.get(), timeout=15.0)
                                    except asyncio.TimeoutError:
                                        yield ": keep-alive\n\n"
                                        if found.closed:
                                            yield "event: done\ndata: {}\n\n"
                                            return
                                        continue
                                    except asyncio.CancelledError:
                                        return
                                    yield evt.to_sse()
                                    if evt.event == "done":
                                        return
                            finally:
                                found.unsubscribe(queue)
                        yield ": wait-channel\n\n"
                    payload = {
                        "seq": 0, "event": "done", "task_id": task_id,
                        "timestamp": int(time.time() * 1000), "status": "closed",
                        "text": "该任务没有可用的流式通道（可能已完成并被回收）",
                    }
                    yield f"event: done\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.CancelledError:
                    return
            return StreamingResponse(_wait_then_stream(), media_type="text/event-stream",
                                     headers=_SSE_HEADERS)

        queue = stream.subscribe()

        async def _event_source():
            """先补发历史事件（刷新/断线重连不丢内容），再持续推送新事件。"""
            try:
                for evt in stream.history(after_seq=after):
                    yield evt.to_sse()
                if stream.closed:
                    yield "event: done\ndata: {}\n\n"
                    return
                idle = 0
                while True:
                    try:
                        evt = await asyncio.wait_for(queue.get(), timeout=15.0)
                        idle = 0
                        yield evt.to_sse()
                        if evt.event == "done":
                            return
                    except asyncio.TimeoutError:
                        idle += 1
                        yield ": keep-alive\n\n"        # 心跳保活，避免长连接被断开
                        if stream.closed and idle >= 2:
                            yield "event: done\ndata: {}\n\n"
                            return
                        if idle > 120:                  # 15s × 120 ≈ 30min（与任务硬超时一致）
                            yield "event: done\ndata: {}\n\n"
                            return
                    except asyncio.CancelledError:
                        return
            finally:
                stream.unsubscribe(queue)

        return StreamingResponse(_event_source(), media_type="text/event-stream",
                                 headers=_SSE_HEADERS)

    @app.get("/api/task/{task_id}/chain")
    async def task_chain(task_id: str, after: int = Query(default=0, ge=0)):
        stream = rt.stream.get(task_id)
        if stream is None:
            return {"ok": True, "task_id": task_id, "events": [], "count": 0,
                    "closed": True, "message": "该任务没有可用的流式通道"}
        events = [e.to_dict() for e in stream.history(after_seq=after)]
        return {"ok": True, "task_id": task_id, "events": events,
                "count": len(events), "closed": stream.closed}

    @app.get("/api/stream/stats")
    async def stream_stats():
        return {"ok": True, **rt.stream.snapshot()}

    # ==================================================================
    # 第8章  GET /api/task/{task_id} —— 任务详情、思考过程、Token数据
    # ==================================================================
    @app.get("/api/task/{task_id}")
    async def get_task(task_id: str):
        detail = rt.task_detail(task_id)
        if not detail:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "TASK_NOT_FOUND", "message": f"任务不存在：{task_id}",
            })
        return {"ok": True, **detail}

    # ==================================================================
    # 第8章  GET /api/approval/list —— 获取审批记录
    # ==================================================================
    @app.get("/api/approval/list")
    async def approval_list(
        session_id: str | None = Query(default=None),
        state: str | None = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
    ):
        records = rt.approval_center.list_records(session_id=session_id, state=state, limit=limit)
        return {
            "ok": True,
            "records": records,
            "stats": rt.approval_center.stats(),
            "approval_switch_always_on": True,   # 第6章 6.3 规则4：前端只读展示
        }

    # ==================================================================
    # 【修复·重复代码收敛】审批恢复执行的统一出口
    #   publish_event=True ：正常裁决路径（先推 SSE 审批结果事件）
    #   publish_event=False：补跑恢复路径（审批早已裁决，事件此前已推过，避免重复）
    #   两种模式共用同一套"非阻塞 / 同步"返回契约，避免两条路径行为漂移。
    # ==================================================================
    async def _run_approval_resume(*, rt: EcosystemRuntime, payload: ApprovalSubmitRequest,
                                   row: dict, approval_id: str, session_id: str,
                                   operator: str, approved: bool, outcome_value: str,
                                   execution, decision: dict, publish_event: bool) -> dict:
        # ==================================================================
        # 【第三轮·Bug1 核心修复】30 秒计时从"后端收到审批提交请求"这一刻才开始。
        #   · 进入 waiting_approval 等待用户点击的阶段：完全没有计时（用户可任意思考）；
        #   · 本行执行时写入 resume_deadline = now + 30s，resume_state='running'；
        #   · 若执行链路 30 秒内未收敛 → 看门狗判定超时 → 任务失败（子任务+大任务 failed）。
        #   注意：必须放在恢复协程启动**之前**，否则极快收敛的链路会来不及计时；
        #   同时它只对 state='pending' 生效（防重放），补跑路径单独重置窗口。
        # ==================================================================
        resume_window = rt.start_resume_window(approval_id=approval_id, row=row)
        row = dict(row) | {"resume_state": "running",
                           "resume_deadline": resume_window.get("deadline")}
        if publish_event:
            # ---- 审批结果已裁决 → 立刻推送 SSE 事件（前端退出阻塞态，不清空聊天记录） ----
            rt.approval_result_emit(row, approved=approved, operator=operator,
                                    outcome=outcome_value)
        # ---- 审批通过 → 恢复执行；拒绝 → 子任务已终止（都按快照从断点继续队长循环） ----
        # 【需求点 Bug2】无论通过还是拒绝，都回到队长业务循环：读取任务完整快照 →
        # 注入审批结果 → 继续跑下一轮；只有队长判定全部完成才交付交互交付Agent。
        resume_task = asyncio.create_task(rt.resume_after_approval(
            approval_id=approval_id,
            approved=approved,
            operator=operator,
            session_id=session_id,
            tool=execution.tool,
            args=execution.args,
        ))
        response = {
            "ok": True,
            "accepted": True,
            "approval_id": approval_id,
            "task_id": row["task_id"],
            "session_id": session_id,
            "approved": bool(approved),
            "outcome": outcome_value,
            "state": decision.get("state"),
            "backend_recheck": decision.get("backend_recheck"),
            "task_action": decision.get("task_action") or "resume",
            # 提交后执行链路的 30 秒限时（前端据此渲染"执行链路计时"倒计时）
            "resume_state": "running",
            "resume_started_at": resume_window.get("started_at"),
            "resume_deadline": resume_window.get("deadline"),
            "timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS),
        }
        if not payload.wait_resume:
            # 【缺陷修复】不再同步等待整轮模型调用：立刻返回受理回执，
            # 后台任务完成后再推送 task_status 事件；前端靠 /chain 或 SSE 感知进度。
            rt.spawn_resume_task(resume_task, approval_id=approval_id,
                                 session_id=session_id, task_id=str(row["task_id"]))
            return response | {
                "message": ("审批已受理，正在从任务快照断点继续执行（执行链路限时 30 秒）"
                            if approved else
                            "已拒绝，当前子任务已终止并回传队长决策"),
                "resume_mode": "async",
            }

        # wait_resume=true：保留历史同步语义（老前端 / 自动化用例）
        outcome_data = await resume_task
        return response | {
            "task_action": outcome_data.get("task_action"),
            # 【需求点 Bug2】队长业务循环恢复信息：已完成/剩余子任务、重试计数、
            # 是否由队长判定整体完成（决定是否已交付交互交付Agent）
            "loop": outcome_data.get("loop"),
            "subtask_task_id": outcome_data.get("subtask_task_id"),
            "status": outcome_data.get("status"),
            "execution": {
                "tool": getattr(execution, "tool", ""),
                "operation_type": getattr(execution, "operation_type", ""),
                "output": outcome_data.get("output", ""),
            },
            # 【需求点 Bug1】后端原生执行回执 + 队长判定完成后的正式报告
            #   （报告里的删除结果只来自这些回执，绝不来自 Agent 文本）
            "backend_receipts": outcome_data.get("backend_receipts") or [],
            "report": outcome_data.get("report") or "",
            "final_reply": outcome_data.get("final_reply") or outcome_data.get("report") or "",
            "detail": outcome_data.get("detail", ""),
            "pending_approval": outcome_data.get("pending_approval"),
            "resume_mode": "sync",
        }

    # ==================================================================
    # 第8章  POST /api/approval/submit —— 提交审批结果（需求伪代码第 6 / 7 步）
    #   ------------------
    #   入参：task_id + outcome（allowed_once / rejected）
    #         （兼容历史参数 approval_id / approved）
    #   行为：
    #     1) 后端二次校验（六道），绝不信任前端；
    #     2) 读取 task_id 对应的完整任务快照 → 注入审批结果 → **从断点恢复**业务循环；
    #     3) 默认立刻返回"审批已受理"（wait_resume=false），恢复执行在后台跑并用 SSE 推事件：
    #        彻底消除历史缺陷「审批提交后卡在提交中」。需要老同步语义时传 wait_resume=true。
    #   注意事项：本接口**不写、不清空任何聊天消息**，历史 AI 聊天输出完整保留。
    # ==================================================================
    @app.post("/api/approval/submit")
    async def approval_submit(payload: ApprovalSubmitRequest, request: Request):
        # ---- 定位审批单：优先 approval_id；只给 task_id 时按下列顺序反查 ----
        #   ① 该 task_id 自身的最新 pending 审批（task_id 就是子任务号的情况）；
        #   ② 沿父任务链逐级向上（最多 8 跳，防环）；
        #   ③ 兜底：该会话下最新一条 pending 审批（前端传根任务号时审批挂在子任务上）。
        approval_id = (payload.approval_id or "").strip()
        if not approval_id and payload.task_id:
            cursor = payload.task_id.strip()
            seen: set[str] = set()
            for _ in range(8):
                if not cursor or cursor in seen:
                    break
                seen.add(cursor)
                row_by_task = rt.db.latest_approval_for_task(cursor)
                if row_by_task and row_by_task.get("state") == "pending":
                    approval_id = str(row_by_task["approval_id"])
                    break
                cursor = str((rt.db.get_task(cursor) or {}).get("parent_task_id") or "")
            if not approval_id:
                session_for_lookup = payload.session_id or str(
                    (rt.db.get_task(payload.task_id.strip()) or {}).get("session_id") or "")
                pending_rows = rt.db.list_pending_approvals(
                    session_id=session_for_lookup or None, limit=50)
                if pending_rows:
                    approval_id = str(pending_rows[-1]["approval_id"])
        if not approval_id:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "APPROVAL_NOT_FOUND",
                "message": "审批记录不存在（请提供 approval_id，或提供尚有待审批单的 task_id）",
            })
        row = rt.db.get_approval(approval_id)
        if not row:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "APPROVAL_NOT_FOUND", "message": "审批记录不存在",
            })
        session_id = payload.session_id or row["session_id"]
        operator = payload.operator or getattr(request.state, "username", "local-admin")
        approved = payload.resolved_approved()          # outcome 优先，approved 兜底
        outcome_value = payload.resolved_outcome()

        # ==================================================================
        # 【修复·不可恢复卡死】审批已裁决但业务循环未恢复时的补跑通道
        #   场景：① 非阻塞恢复的后台协程异常退出；② 裁决落库后进程被杀。
        #   判定条件：该审批已终态 + 所属任务仍停留在 waiting_approval（未恢复）。
        #   此时无需再次裁决（防重放仍然成立），只按快照补跑一次恢复。
        # ==================================================================
        if row["state"] != "pending":
            snapshot_for_retry = rt.snapshots.find_snapshot_for_task(str(row.get("task_id") or ""))
            root_for_retry = str((snapshot_for_retry or {}).get("task_id") or row.get("task_id") or "")
            task_status_now = rt.snapshots.status_of(root_for_retry)
            needs_resume = bool(payload.retry_resume
                                or task_status_now == STATUS_WAITING_APPROVAL)
            if not needs_resume:
                return JSONResponse(status_code=409, content={
                    "ok": False, "error": "APPROVAL_STATE_CONFLICT",
                    "message": (f"审批记录已处于终态（{row['state']}），禁止重复裁决（防重放）"
                                if not payload.retry_resume else
                                f"审批记录已处于终态（{row['state']}），任务也已收敛"
                                f"（{task_status_now or '未知'}），无需补跑恢复"),
                })
            if rt.resume_in_progress(root_for_retry):
                return {
                    "ok": True, "accepted": True, "already_resuming": True,
                    "approval_id": approval_id, "task_id": root_for_retry,
                    "state": row["state"],
                    "message": "该审批的恢复执行正在进行中，无需重复提交",
                    "resume_mode": "async",
                }
            rt.logger.task_log(
                session_id=session_id, task_id=root_for_retry, agent_role=AGENT_DISPATCH,
                event="approval.resume.retry", level="warn",
                detail=(f"审批 {approval_id[:8]} 已终态（{row['state']}）但任务仍为 "
                        f"{task_status_now or '未知'} → 按快照补跑恢复，不重复裁决"),
            )
            decision = rt.approval_center.describe_decision(approval_id)
            approved = bool(decision.get("approved"))
            outcome_value = str(decision.get("outcome") or outcome_value)
            execution = decision.get("execution")
            if execution is None and approved:
                # 重启场景：按快照重建待执行动作（保证审批通过仍能真实执行）
                execution = rt._restore_pending_execution(row, snapshot_for_retry or {})
            if execution is None:
                execution = rt.approval_center.get_pending(approval_id)
            if execution is None:
                return JSONResponse(status_code=409, content={
                    "ok": False, "error": "APPROVAL_EXECUTION_EXPIRED",
                    "message": ("审批已裁决，但对应的待执行动作无法重建（快照缺少动作参数）；"
                                "请重新发起任务"),
                })
            rt.approval_center.mark_execution_retryable(approval_id)
            return await _run_approval_resume(
                rt=rt, payload=payload, row=row, approval_id=approval_id,
                session_id=session_id, operator=operator, approved=approved,
                outcome_value=outcome_value, execution=execution,
                decision={"state": row["state"],
                          "backend_recheck": {"checks": {"result": "resume_retry",
                                                         "approval_state": row["state"]}},
                          "task_action": "resume"},
                publish_event=False)

        # ---- 后端二次校验（校验链在 ApprovalCenter 内实现并落库留痕） ----
        decision = rt.approval_center.verify_and_decide(
            approval_id=approval_id,
            approved=approved,
            operator=operator,
            session_id=session_id,
            submitted_fingerprint=payload.params_fingerprint,
            submitted_risk_level=payload.risk_level,
        )

        # ---- 审批结果以标准消息结构体回灌总线（第3章 3.1 / 3.5） ----
        execution = decision["execution"]

        # 通过消息总线广播审批结果事件（统一结构体，供记忆Agent异步消费）
        try:
            from backend.bus.message import Message
            event = Message(
                session_id=session_id, task_id=row["task_id"],
                parent_task_id=None, sender_agent="user",
                receiver_agent=AGENT_CODE, msg_type=MSG_TYPE_APPROVAL_RESULT,
                payload={"content": "审批通过（执行一次）" if approved else "审批拒绝",
                         "metadata": {
                             "approval_id": approval_id,
                             "risk_level": row["risk_level"],
                             "operation_desc": row["operation_desc"],
                             "operation_params": row["operation_params"],
                             "danger_reason": row["danger_reason"],
                             "approved": bool(approved),
                             "outcome": outcome_value,
                             "operator": operator,
                         }},
                status=STATUS_SUCCESS if approved else STATUS_FAILED,
            )
            await rt.bus.publish(event)
        except Exception as exc:  # noqa: BLE001 事件广播失败不影响裁决结果
            rt.logger.exception_log(
                error_code="APPROVAL_EVENT_PUBLISH_FAILED",
                message=f"审批结果事件广播失败：{exc}",
                session_id=session_id, task_id=row["task_id"],
            )

        return await _run_approval_resume(
            rt=rt, payload=payload, row=row, approval_id=approval_id,
            session_id=session_id, operator=operator, approved=approved,
            outcome_value=outcome_value, execution=execution, decision=decision,
            publish_event=True)

    # ==================================================================
    # 【需求新增】审批/任务状态查询（非阻塞提交后，前端据此确认恢复进度）
    #   GET /api/approval/status/{approval_id}
    #     → 审批单状态、执行链路计时状态（resume_state / 剩余秒数）、
    #       是否超时失败、所属任务状态、恢复执行是否仍在进行
    #   注意：waiting-for-user 阶段 resume_state=idle 且不返回倒计时（无超时限制）。
    # ==================================================================
    @app.get("/api/approval/status/{approval_id}")
    async def approval_status(approval_id: str):
        row = rt.db.get_approval(approval_id)
        if not row:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "APPROVAL_NOT_FOUND", "message": "审批记录不存在",
            })
        snapshot = rt.snapshots.find_snapshot_for_task(str(row.get("task_id") or ""))
        root_task_id = str((snapshot or {}).get("task_id") or row.get("task_id") or "")
        task_row = rt.db.get_task(root_task_id) or {}
        window = rt.approval_center.resume_window(approval_id)
        return {
            "ok": True,
            "approval_id": approval_id,
            "state": row["state"],
            "approved": row["state"] == "manual",
            "timed_out": row["state"] == "timeout",
            "resume_timed_out": (str(window.get("resume_state")) == "timeout"),
            "task_id": root_task_id,
            "subtask_task_id": row.get("task_id"),
            "task_status": str(task_row.get("status") or ""),
            "resume_in_progress": rt.resume_in_progress(root_task_id),
            # 执行链路计时视图：counting=True 时前端才显示 30 秒倒计时
            "resume_state": window.get("resume_state"),
            "resume_started_at": window.get("resume_started_at"),
            "resume_deadline": window.get("resume_deadline"),
            "remaining_seconds": window.get("remaining_seconds"),
            "counting": window.get("counting"),
            "countdown": ({"deadline": window.get("resume_deadline"),
                           "remaining_seconds": window.get("remaining_seconds"),
                           "expired": bool(window.get("counting")
                                           and (window.get("remaining_seconds") or 0) <= 0),
                           "timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS)}
                          if window.get("counting") else None),
            "timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS),
            "server_time": time.time(),
        }

    # ==================================================================
    # 【需求新增】GET /api/task/{task_id}/snapshot —— 读取任务完整快照
    #   （断点恢复的只读视图：已完成子任务列表 / 剩余计划 / 消息上下文 /
    #    待执行高危操作详情 / 状态 / approval_deadline）
    # ==================================================================
    @app.get("/api/task/{task_id}/snapshot")
    async def task_snapshot(task_id: str):
        payload = rt.snapshots.snapshot_payload(task_id)
        if payload is None:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "SNAPSHOT_NOT_FOUND",
                "message": f"任务快照不存在：{task_id}（可能尚未进入业务循环或已清理）",
            })
        payload = dict(payload)
        payload["create_time"] = payload.get("created_at") or 0.0
        payload["create_time_text"] = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(payload.get("created_at") or 0))
        return {"ok": True, "task_id": task_id, "snapshot": payload}

    # ==================================================================
    # 【需求新增】POST /api/approval/timeout_scan —— 手动触发一次执行链路超时扫描
    #   （看门狗每秒自动扫描；本接口用于运维排查 / 单测验证超时路径）
    #   语义（第三轮）：只扫描"审批已提交（resume_state=running）且执行链路
    #   超过 30 秒未收敛"的审批 → 判定任务失败。等待用户点击阶段的审批永不命中。
    # ==================================================================
    @app.post("/api/approval/timeout_scan")
    async def approval_timeout_scan():
        expired = rt.snapshots.expired_resume_approvals()
        handled: list[dict] = []
        for row in expired:
            result = rt.approval_center.decide_resume_timeout(str(row.get("approval_id") or ""))
            if not result.get("timed_out"):
                continue
            decision = result.get("decision") or {}
            rt._approval_timeout_emit(row, decision)
            handler = rt.approval_center.timeout_handler()
            if handler is not None:
                await handler(result.get("execution"), decision)
            handled.append({
                "approval_id": decision.get("approval_id"),
                "outcome": APPROVAL_OUTCOME_REJECTED,
                "state": decision.get("state"),
                "resume_timed_out": True,
                "expired_seconds": row.get("expired_seconds"),
                "root_task_id": decision.get("root_task_id"),
            })
        return {
            "ok": True, "scanned_expired": len(expired), "handled": handled,
            "timeout_seconds": float(APPROVAL_RESUME_TIMEOUT_SECONDS),
            "rule": ("等待用户点击阶段不计时；后端收到 /api/approval/submit 之后才开始计时，"
                     "30 秒内执行链路未收敛 → 判定任务失败"),
        }

    # ==================================================================
    # 第8章  POST /api/background/upload —— 上传自定义背景
    # 第7章 7.1：背景功能（预设背景 + 自定义图片/视频上传）
    # ==================================================================
    @app.post("/api/background/upload")
    async def background_upload(file: UploadFile = File(...)):
        raw = await file.read()
        check = check_upload(file.filename or "bg", len(raw), content_head=raw[:16])
        if not check.ok:
            raise SecurityViolation(check.reason, code=check.code,
                                    detail={"filename": file.filename, "size": len(raw)})
        if check.resource_type != "image":
            raise SecurityViolation(
                f"背景仅支持图片格式（当前为 {check.resource_type}）", code="BG_TYPE_DENIED",
            )
        if not (file.content_type or "").startswith("image/") and check.ext != ".svg":
            raise SecurityViolation("背景文件 MIME 类型必须为 image/*", code="BG_MIME_DENIED")

        bg_dir = rt.paths.uploads / BACKGROUND_DIR_NAME
        bg_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        target = bg_dir / f"{stamp}_{check.safe_name}"
        target.write_bytes(raw)

        rt.config.set_background(bg_type="custom", value=f"/api/background/file/{target.name}")
        rt.logger.info(f"自定义背景已上传：{target.name}（{len(raw)} 字节）", agent_role="system")
        return {
            "ok": True,
            "background": {"type": "custom", "value": f"/api/background/file/{target.name}"},
            "name": check.safe_name,
            "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }

    @app.get("/api/background/file/{name}")
    async def background_file(name: str):
        from backend.utils.paths import is_within
        bg_root = (rt.paths.uploads / BACKGROUND_DIR_NAME).resolve()
        target = (bg_root / Path(name).name).resolve()
        if not is_within(target, bg_root):
            raise SecurityViolation("背景文件路径越界", code="CROSS_DIRECTORY_DENIED")
        if not target.exists():
            return JSONResponse(status_code=404, content={"ok": False, "message": "背景文件不存在"})
        return FileResponse(target)

    # ==================================================================
    # 【补充】会话接口 —— 第7章 7.1 要求"搜索框 + 工作区会话列表"、
    # 7.3 要求消息区域展示历史消息，前端必须有会话读接口才能闭环。
    # （会话的创建由 POST /api/session/chat 承担，符合第8章）
    # ==================================================================
    @app.get("/api/session/list")
    async def session_list(q: str | None = Query(default=None),
                           workspace_id: str | None = Query(default=None),
                           limit: int = Query(default=100, ge=1, le=500)):
        rows = rt.db.list_sessions(limit=limit, workspace_id=workspace_id)
        items = []
        for r in rows:
            title = r["title"]
            if q and q.strip() and q.strip().lower() not in title.lower():
                continue
            tasks = rt.db.list_tasks(r["session_id"], limit=200)
            # 【BUG-B】会话目录解析失败（工作区文件夹被删除/无权限）不得让整个列表 500：
            #   降级为展示系统会话目录 + 可读提示
            try:
                session_dir = str(rt.session_paths(r["session_id"]).root)
                workspace_ok = True
                workspace_message = ""
            except WorkspaceUnavailable as exc:
                session_dir = str(rt.paths.sessions / r["session_id"])
                workspace_ok = False
                workspace_message = str(exc)
            items.append({
                "session_id": r["session_id"],
                "title": title,
                "workspace_id": r.get("workspace_id") or "ws_default",
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "status": r["status"],
                "dir": session_dir,
                "workspace_ok": workspace_ok,
                "workspace_message": workspace_message,
                "task_count": len(tasks),
            })
        return {"ok": True, "sessions": items}

    @app.get("/api/session/{session_id}")
    async def session_detail(session_id: str, limit: int = Query(default=200, ge=1, le=1000)):
        from backend.utils.paths import validate_session_id
        validate_session_id(session_id)
        row = rt.db.get_session(session_id)
        if not row:
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "SESSION_NOT_FOUND", "message": "会话不存在",
            })
        tasks = rt.db.list_tasks(session_id, limit=limit)
        # 会话消息（第7.3 消息区域：用户消息 + AI 正式回复 + 思考过程）
        messages = rt.db.query(
            """SELECT m.*, t.title AS task_title FROM messages m
               LEFT JOIN tasks t ON t.task_id = m.task_id
               WHERE m.session_id=? ORDER BY m.timestamp ASC LIMIT ?""",
            (session_id, limit),
        )
        think_by_task: dict[str, list[dict]] = {}
        for t in tasks:
            think_by_task[t["task_id"]] = [
                # 【BUG-C 2】步骤级别：error 在前端渲染为红色错误标识
                _think_step_view(s) for s in rt.db.list_think_steps(t["task_id"])
            ]

        tokens = rt.db.token_summary(session_id=session_id)

        # ==============================================================
        # 【需求点 一、2】会话加载路径异常的优雅降级：
        #   会话目录（C 盘配置目录）与业务工作区（可能 E 盘）不在同一目录树时，
        #   文件清单/路径计算不再抛 ValueError 导致整个接口 500；
        #   失败时返回 files=[] + degradation 可读提示，会话主体数据照常返回。
        # ==============================================================
        try:
            sessions_dir = rt.session_paths(session_id)
        except SecurityViolation as exc:
            rt.logger.exception_log(
                error_code=exc.code, message=f"会话路径解析失败（已降级）：{exc}",
                session_id=session_id, agent_role="system",
            )
            return JSONResponse(status_code=409, content={
                "ok": False, "error": exc.code,
                "message": f"会话路径解析失败（会话数据未被破坏）：{exc}",
            })

        files, files_degradation = _safe_session_files(
            rt, sessions_dir, session_id, workspace_id=row.get("workspace_id") or "")

        # 【需求点 二、3】历史会话回显"上一次任务耗时"（旧会话无记录时返回 idle 文案）
        timer_view = rt.task_timer_view(session_id)

        return {
            "ok": True,
            "session": {
                "session_id": row["session_id"], "title": row["title"],
                "created_at": row["created_at"], "updated_at": row["updated_at"],
                "dir": str(sessions_dir.root),
                "workspace_id": row.get("workspace_id") or "ws_default",
                # 【需求点 一、1-b】会话工作区根目录既是业务根目录，也用于展示；
                #   系统会话元数据本身不受工作区子路径校验约束
                "workspace_root": str(sessions_dir.workspace),
                "is_local_folder": bool(sessions_dir.is_local_folder),
                "files": files,
            },
            "session_files_degradation": files_degradation,
            "task_timer": timer_view,
            "tasks": [{
                "task_id": t["task_id"], "title": t["title"], "agent_role": t["agent_role"],
                "status": t["status"], "parent_task_id": t["parent_task_id"] or "",
                "iteration": t["iteration"], "retry_count": t["retry_count"],
                "review_rejects": t["review_rejects"],
                "created_at": t["created_at"], "finished_at": t["finished_at"],
                "error_code": t["error_code"] or "", "error_message": t["error_message"] or "",
                "think_steps": think_by_task.get(t["task_id"], []),
            } for t in tasks],
            "messages": [{
                "msg_id": m["msg_id"], "task_id": m["task_id"], "task_title": m["task_title"],
                "sender_agent": m["sender_agent"], "receiver_agent": m["receiver_agent"],
                "msg_type": m["msg_type"], "status": m["status"],
                "timestamp": m["timestamp"],
                "content": _safe_json_str(m["payload_content"]),
                "metadata": _safe_json(m["payload_metadata"]),
            } for m in messages],
            "tokens": tokens,
            "token_breakdown": rt.db.token_breakdown(session_id),
        }

    # ==================================================================
    # 【需求点 二、任务执行计时器】会话级计时接口（前端计时组件唯一轮询入口）
    # ==================================================================
    @app.get("/api/session/{session_id}/timers")
    async def session_timers(session_id: str, limit: int = Query(default=50, ge=1, le=500)):
        """会话任务耗时：计时器实时视图 + 历史记录 + 汇总（全部真实后端数据）。"""
        from backend.utils.paths import validate_session_id
        validate_session_id(session_id)
        if not rt.db.get_session(session_id):
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "SESSION_NOT_FOUND", "message": "会话不存在",
            })
        data = rt.session_timer_history(session_id, limit=limit)
        return {"ok": True, "server_time": time.time(), **data}

    @app.post("/api/session/create")
    async def session_create(payload: SessionCreateRequest):
        """【补充】新建空会话（第7.1 新会话按钮）：仅建目录与记录，不发起任务。

        【需求点 二、2】新建空工作区/新会话后不得预生成任何会话条目——
        本接口只在用户显式点击「新会话」时被调用，不做任何批量初始化。
        """
        session_id = rt.router.new_session_id()
        title = (payload.title or "").strip() or ("新会话 " + time.strftime("%m-%d %H:%M"))
        workspace_id = payload.workspace_id
        if workspace_id and not rt.db.get_workspace(workspace_id):
            raise SecurityViolation(f"目标工作区不存在：{workspace_id}", code="WORKSPACE_NOT_FOUND")
        rt.db.upsert_session(session_id, title, workspace_id=workspace_id)
        paths = rt.session_paths(session_id)
        rt.logger.info(f"新会话已创建：{session_id}", session_id=session_id, agent_role="system")
        return {"ok": True, "session": {
            "session_id": session_id, "title": title, "dir": str(paths.root),
            "created_at": time.time(), "workspace_id": rt.session_workspace(session_id),
        }}

    # ==================================================================
    # 【补充】审批面板清空（第7.2 审批页面：清空）
    # ==================================================================
    @app.post("/api/approval/clear")
    async def approval_clear(session_id: str | None = Query(default=None)):
        removed = rt.approval_center.clear(session_id=session_id)
        return {"ok": True, "removed": removed}

    # ==================================================================
    # 【补充】状态栏 / 系统信息（第7.3 状态栏 + 2.4 Token统计：真实后端数据）
    # ==================================================================
    @app.get("/api/status")
    async def status(session_id: str | None = Query(default=None),
                     task_id: str | None = Query(default=None)):
        """状态栏 / 系统信息（第7.3 状态栏 + 2.4 Token统计：真实后端数据）。

        【需求点 Bug9】增加 task_id 维度：前端底部统计栏每 3 秒轮询本接口，
        实时获取当前 task 的调用次数 / 步数 / LLM 耗时 / 缓存命中率 / 输入输出 token。
        """
        target_task = task_id or rt.last_task_id
        snapshot = rt.system_snapshot(session_id=session_id, task_id=target_task)
        snapshot["ok"] = True
        snapshot["server_time"] = time.time()
        return snapshot

    # ==================================================================
    # 【需求点 二、工作区分组管理】工作区接口
    #   左树结构：工作区(可折叠分组) -> 归属会话列表
    # ==================================================================
    @app.get("/api/workspace/list")
    async def workspace_list(q: str | None = Query(default=None)):
        return {"ok": True, "workspaces": rt.list_workspaces(q=q)}

    @app.post("/api/workspace/create")
    async def workspace_create(payload: WorkspaceCreateRequest):
        return {"ok": True, "workspace": rt.create_workspace(
            payload.name or "", folder_path=payload.folder_path)}

    # ---------------- 【需求点 二、1】工作区文件夹（本地目录） ----------------
    @app.get("/api/workspace/folders")
    async def workspace_folders():
        """已登记的工作区文件夹列表 + 当前激活项（见左树下拉菜单）。"""
        return {
            "ok": True,
            "folders": rt.config.list_workspace_folders(),
            "active_folder_id": (rt.config.get_active_workspace_folder() or {}).get("folder_id", ""),
            "workspaces": rt.list_workspaces(),
        }

    @app.post("/api/workspace/folder/probe")
    async def workspace_folder_probe(payload: FolderProbeRequest):
        """按文件夹名探测候选绝对路径（浏览器文件夹选择器只给名称）。"""
        resolved = rt.config.scan_folder_candidates(payload.folder_name or "")
        return {
            "ok": True,
            "folder_name": payload.folder_name,
            "resolved": len(resolved) == 1,
            "candidates": resolved[:30],
            "message": ("" if len(resolved) == 1 else
                        (f"检测到 {len(resolved)} 个同名文件夹，请选择具体目录" if resolved
                         else f"未能在常用位置定位到「{payload.folder_name}」，请粘贴完整路径")),
        }

    @app.post("/api/workspace/folder/add")
    async def workspace_folder_add(payload: FolderAddRequest):
        """添加工作区文件夹：登记 + 切换为当前激活工作区 + 持久化到本地配置。"""
        result = rt.add_local_folder_workspace(
            path=payload.path, folder_name=payload.folder_name, workspace_id=payload.workspace_id,
        )
        if not result.get("ok"):
            return JSONResponse(status_code=409, content=result)
        return {"ok": True, **result}

    @app.post("/api/workspace/folder/remove")
    async def workspace_folder_remove(folder_id: str = Query(...)):
        """从工作区列表移除本地文件夹登记（不删除磁盘目录内容）。"""
        return {"ok": True, **rt.config.remove_workspace_folder(folder_id)}

    # ---------------- 工作区安全边界信息（前端展示当前操作根目录） ----------------
    @app.get("/api/workspace/root")
    async def workspace_root(workspace_id: str | None = Query(default=None),
                             session_id: str | None = Query(default=None)):
        """返回当前工作区的文件操作根目录与越权规则说明。"""
        target_ws = workspace_id or (rt.session_workspace(session_id) if session_id else None)
        if not target_ws:
            target_ws = DEFAULT_WORKSPACE_ID
        root = rt.workspace_root_for(target_ws)
        extra = rt._workspace_extra(target_ws)  # noqa: SLF001 内部只读访问
        return {
            "ok": True,
            "workspace_id": target_ws,
            "kind": extra.get("kind") or "system",
            "folder_path": str(root) if root else "",
            "folder_name": extra.get("folder_name", ""),
            "root": str(root) if root else str(rt.paths.sessions),
            "policy": {
                "scope": "当前选中工作区文件夹",
                "deny_message": WORKSPACE_ACCESS_DENIED_MESSAGE,
                # 【需求点 一、1】两套路径判断函数分工（前端可见的口径说明）
                "workspace_check": "is_workspace_file()：仅 Agent 业务文件，必须属于当前工作区子目录",
                "system_meta_check": "is_system_meta_file()：会话/配置元文件，不执行工作区白名单校验",
                "rules": [
                    "所有 7 个 Agent 的文件读写/创建/修改/删除全部限定在当前工作区文件夹内",
                    "禁止访问上级目录、其他磁盘文件夹与系统关键目录",
                    "越权访问直接拒绝并写入安全日志",
                    "切换工作区后文件操作根目录自动更新",
                    "会话元数据 / 会话 json / 系统配置的读写不受工作区子路径校验约束",
                ],
            },
        }

    @app.post("/api/workspace/rename")
    async def workspace_rename(payload: WorkspaceRenameRequest,
                               workspace_id: str = Query(...)):
        return {"ok": True, **rt.rename_workspace(workspace_id, payload.name)}

    @app.post("/api/workspace/delete")
    async def workspace_delete(workspace_id: str = Query(...)):
        """删除工作区（前端已二次确认），同时清理其下会话数据。"""
        return {"ok": True, **rt.delete_workspace(workspace_id)}

    @app.post("/api/session/move")
    async def session_move(payload: SessionMoveRequest):
        """会话移动归属：A 工作区 -> B 工作区。"""
        return {"ok": True, **rt.move_session(payload.session_id, payload.workspace_id)}

    @app.post("/api/session/delete")
    async def session_delete(session_id: str = Query(...)):
        """【补充】删除会话（左树会话项右键/菜单）—— 工作区删除时复用同一清理逻辑。"""
        if not rt.db.get_session(session_id):
            return JSONResponse(status_code=404, content={
                "ok": False, "error": "SESSION_NOT_FOUND", "message": "会话不存在",
            })
        outcome = rt.db.delete_session(session_id)
        try:
            target = rt.session_paths(session_id).root
            if target.exists():
                import shutil as _shutil
                _shutil.rmtree(target, ignore_errors=True)
        except Exception as exc:  # noqa: BLE001
            rt.logger.warning(f"会话目录清理失败 {session_id}: {exc}", agent_role="system")
        return {"ok": True, **outcome}

    # ==================================================================
    # 【补充】记忆管理（第4章 4.5 用户可控：支持手动清空单条/全部长期记忆）
    # ==================================================================
    @app.get("/api/memory/list")
    async def memory_list(session_id: str | None = Query(default=None)):
        memory_agent = rt.agents["记忆管理Agent"]
        return {
            "ok": True,
            "stats": memory_agent.stats(),
            "short_term": memory_agent.short_term(session_id) if session_id else [],
            "long_term": rt.vector_store.list_records(session_id=session_id),
        }

    @app.post("/api/memory/clear")
    async def memory_clear(session_id: str | None = Query(default=None),
                           memory_id: str | None = Query(default=None),
                           scope: str = Query(default="long_term")):
        memory_agent = rt.agents["记忆管理Agent"]
        # 安全：清空全部长期记忆需要显式 scope=all
        if scope == "all":
            out = memory_agent.clear(memory_id=None, session_id=None)
        elif memory_id:
            out = memory_agent.clear(memory_id=memory_id)
        elif session_id:
            out = memory_agent.clear(session_id=session_id)
        else:
            out = {"removed": 0, "available": rt.vector_store.available}
        return {"ok": True, **out, "stats": memory_agent.stats()}

    # ==================================================================
    # 【补充】备份导出（第9章 9.3 数据备份）
    # ==================================================================
    @app.get("/api/backup/export")
    async def backup_export():
        bundle = rt.backup.export_bundle()
        # 备份包内含加密信封（无明文密钥），直接返回由用户本地保存
        return {
            "ok": True,
            "bundle": bundle,
            "note": "备份包含会话记录、审批记录、加密配置信封、向量记忆库；密钥为密文，无明文",
        }

    @app.get("/api/logs/errors")
    async def error_logs(limit: int = Query(default=200, ge=1, le=1000)):
        return {"ok": True, "errors": rt.db.list_errors(limit)}

    # ==================================================================
    # 前端静态资源
    # ==================================================================
    if FRONTEND_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        page = FRONTEND_DIR / "index.html"
        if not page.exists():
            return HTMLResponse(
                "<h1>Multi-Agent Ecosystem</h1><p>前端文件缺失：frontend/index.html</p>",
                status_code=500,
            )
        return HTMLResponse(page.read_text(encoding="utf-8"))

    return app


def _list_session_files(rt: EcosystemRuntime, paths) -> list[dict]:
    """列出会话相关工作目录内的文件（第5章 会话目录隔离）。

    【需求点 一、Bug 修复】会话目录与业务工作区可能不在同一磁盘 / 同一目录树
    （C 盘会话目录 vs E 盘业务工作区）。历史实现直接调用
    `child.relative_to(paths.root)`，跨盘符时 pathlib 抛
        ValueError: 'E:\\...' is not in the subpath of 'C:\\...'
    导致整个会话详情接口 500。现改为：
      · 相对路径统一走 safe_relative_to()（越界时回退为绝对路径，绝不抛异常）
      · 单文件读取异常不影响整体列表（逐项 try/except 降级）

    【需求点 一、1】两套路径判断函数在此明确分工：
      · 会话私有目录（upload / artifact）与工作区标记文件 = is_system_meta_file()
        → 系统元文件，跳过工作区子路径校验；
      · 业务工作区目录内的业务文件 = is_workspace_file()
        → 必须位于当前工作区内，越界则从清单中剔除并记安全日志。
    """
    out: list[dict] = []
    seen: set[str] = set()
    workspace_root = Path(str(paths.workspace))
    # 【需求点 一、1-b】把元文件判定器绑定到本运行时的生态系统目录，
    #   保证 MAE_ROOT 覆盖 / 测试隔离场景下也判定正确（不依赖全局推断）
    meta_check = make_system_meta_checker(rt.paths)
    for base_label, base in (("upload", paths.upload), ("artifact", paths.artifact),
                             ("workspace", paths.workspace)):
        try:
            if base is None or not base.exists():
                continue
            iterator = sorted(base.rglob("*"))
        except (OSError, ValueError):
            # 目录不可读（权限 / 已被移除）→ 跳过该目录，不影响会话加载
            rt.logger.exception_log(
                error_code="SESSION_FILES_UNAVAILABLE",
                message=f"会话目录清单读取失败，已跳过：{base}",
                session_id=getattr(paths, "session_id", None), agent_role="system",
            )
            continue
        for child in iterator:
            try:
                if not child.is_file():
                    continue
                stat = child.stat()
            except (OSError, ValueError):
                continue
            # 【需求点 一、1-b】工作区标记文件 .mae_workspace.json 属于**系统元文件**，
            #   它是业务工作区目录内的系统配置，不属于会话、也不是业务文件：
            #   · 不进入会话清单（避免前端把它当成工作区内容展示）
            #   · 不做工作区 subpath 白名单判断（否则跨盘符时抛 ValueError）
            try:
                if child.name == WORKSPACE_MARKER_FILE:
                    continue
            except (TypeError, ValueError):
                continue
            # 【需求点 一、1-b】系统元文件（会话 upload/artifact）→ 免白名单校验
            is_meta = meta_check(child, session_root=paths.root,
                                 category=SECURITY_META_CATEGORY_SESSION)
            if not is_meta:
                # 【需求点 一、1-a】业务文件必须位于当前工作区内，越界即剔除 + 记日志
                if not is_workspace_file(child, workspace_root):
                    rt.logger.warning(
                        f"会话清单中剔除越界业务文件：{display_relative(child, workspace_root)}"
                        f"（工作区 {workspace_root}）",
                        session_id=getattr(paths, "session_id", None), agent_role="system",
                    )
                    continue
            # 相对路径：同盘符 → 相对路径；跨盘符/跨目录树 → 绝对路径（Bug 修复点）
            rel = safe_relative_to(child, paths.root, fallback=str(child))
            key = f"{base_label}:{rel}"
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "scope": base_label,
                "rel": rel.replace("\\", "/"),
                "abs": str(child),
                "size": stat.st_size,
                # 未落在会话目录树内（例如本地工作区在别的磁盘）时如实标注，便于前端区分
                "outside_session_dir": not is_within(child, paths.root),
            })
            if len(out) >= 500:
                return out
    return out


def _safe_session_files(rt: EcosystemRuntime, paths, session_id: str,
                        workspace_id: str = "") -> tuple[list[dict], dict]:
    """【需求点 一、2】会话文件清单的优雅降级包装。

    返回 (files, degradation)。任何路径类异常（ValueError / OSError /
    SecurityViolation）都被捕获为可读提示，接口整体保持 200，不崩溃。

    【需求点 一、1-b】这里访问的是**会话元数据 / 会话目录清单**，
    按 is_system_meta_file() 口径属于系统元文件 → **不做工作区子路径校验**。
    """
    degradation: dict = {}
    try:
        files = _list_session_files(rt, paths)
        return files, degradation
    except SecurityViolation as exc:            # 路径白名单类异常
        rt.logger.exception_log(
            error_code=exc.code or "SESSION_FILES_DENIED",
            message=f"会话文件清单读取被安全模块拦截（已降级）：{exc}",
            session_id=session_id, agent_role="system",
        )
        degradation = {"files_available": False, "code": exc.code,
                       "message": f"{SESSION_FILES_UNAVAILABLE_MESSAGE}：{exc}"}
    except Exception as exc:  # noqa: BLE001 ValueError / OSError 等一律优雅降级
        rt.logger.exception_log(
            error_code="SESSION_FILES_UNAVAILABLE",
            message=(f"会话文件清单读取异常（已降级，会话数据本身可用）："
                     f"{type(exc).__name__}: {exc}"),
            session_id=session_id, agent_role="system",
            stack=traceback.format_exc(),
        )
        degradation = {"files_available": False,
                       "code": type(exc).__name__,
                       "message": f"{SESSION_FILES_UNAVAILABLE_MESSAGE}：{exc}"}
    return [], degradation


def _think_step_view(step) -> dict:
    """思考步骤的统一对外结构。

    【BUG-C 2/5】新增 level 字段（info / warn / error）：
    前端据此把失败步骤渲染成红色错误标识，禁止用 success 状态掩盖失败。
    """
    try:
        level = step["level"]
    except (KeyError, IndexError, TypeError):
        level = THINK_LEVEL_INFO
    return {
        "index": step["step_index"],
        "type": step["step_type"],
        "text": step["step_text"],
        "agent": step["agent_role"],
        "level": str(level or THINK_LEVEL_INFO),
        # error 布尔量：前端即便不解析 level 也能直接判定错误
        "is_error": str(level or "").lower() == THINK_LEVEL_ERROR,
    }


def _safe_json(raw: str) -> Any:
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {}


def _safe_json_str(raw: str) -> str:
    try:
        value = json.loads(raw)
        return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    except json.JSONDecodeError:
        return str(raw)


app = create_app


def main() -> None:
    import uvicorn
    uvicorn.run(
        "backend.main:create_app",
        factory=True,
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
