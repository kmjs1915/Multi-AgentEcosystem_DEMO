# -*- coding: utf-8 -*-
"""
全局常量（全部在代码中写死，禁止配置化放开）

架构文档来源：
  - 第1章 1.3 模型固定分配（最终定型，不可修改）
  - 第2章 2.1 任务终止与防死循环规则
  - 第2章 2.3 模型降级兜底规则
  - 第3章 3.1 标准消息结构体 / 3.4 任务状态机
  - 第5章 裸机目录规范
  - 第6章 6.2 文件上传安全限制
  - 第9章 9.1 异常处理
"""

# ==========================================================================
# 第3章 3.1 标准消息结构体：msg_type 取值（唯一固定集合，禁止自定义零散格式）
# ==========================================================================
MSG_TYPE_TASK = "task"
MSG_TYPE_RESULT = "result"
MSG_TYPE_ERROR = "error"
MSG_TYPE_APPROVAL_REQUEST = "approval_request"
MSG_TYPE_APPROVAL_RESULT = "approval_result"

VALID_MSG_TYPES = (
    MSG_TYPE_TASK,
    MSG_TYPE_RESULT,
    MSG_TYPE_ERROR,
    MSG_TYPE_APPROVAL_REQUEST,
    MSG_TYPE_APPROVAL_RESULT,
)

# ==========================================================================
# 第3章 3.4 任务状态机完整定义（pending / running / waiting_approval / success / failed）
# ==========================================================================
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_WAITING_APPROVAL = "waiting_approval"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"

VALID_STATUSES = (
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_SUCCESS,
    STATUS_FAILED,
)

# 状态机合法流转表（第3章 3.4 + 3.5）
# pending          -> running（分发执行）
# running          -> waiting_approval（高危拦截）/ success / failed
# waiting_approval -> running（审批通过继续执行）/ failed（审批拒绝直接终止子任务）
STATUS_TRANSITIONS = {
    STATUS_PENDING: (STATUS_RUNNING, STATUS_FAILED),
    STATUS_RUNNING: (STATUS_WAITING_APPROVAL, STATUS_SUCCESS, STATUS_FAILED),
    STATUS_WAITING_APPROVAL: (STATUS_RUNNING, STATUS_FAILED, STATUS_SUCCESS),
    STATUS_SUCCESS: (),
    STATUS_FAILED: (),
}


def is_terminal_status(status: str | None) -> bool:
    """任务是否处于终态（finished / success / failed 三选一）。

    【新增】需求要求的状态机口径为 running / waiting_approval / finished / failed，
    系统既有成功字面量为 success，两者在终态判定上完全等价 —— 本函数是二者唯一的判定入口，
    调用方不得再自行写 `status in ("success", "failed")` 之类的字面量比较。
    """
    return str(status or "") in TERMINAL_STATUSES


def is_success_status(status: str | None) -> bool:
    """任务是否成功终态（finished 与 success 等价）。"""
    return str(status or "") in (STATUS_SUCCESS, STATUS_FINISHED)

# ==========================================================================
# 第2章 2.1 防死循环硬限制
# ==========================================================================
MAX_TASK_ITERATIONS = 20              # 单任务全局最大迭代次数：20 次
MAX_TASK_TIMEOUT_SECONDS = 30 * 60    # 单任务全局最大超时时间：30 分钟
MAX_SUBTASK_RETRIES = 3               # 【Bug2】同一队员子任务重试上限：队长判定不达预期后最多重派 3 次；3 次仍失败 → 队长二选一（① 整个大任务 failed 终止 / ② 重新规划换其他队员 Agent）
MAX_REVIEW_REJECTS = 2                # 评估校验 Agent 打回校验最多 2 次

# ==========================================================================
# 【第三轮·业务流程重构】新流转链路的两道计数器上限
#   链路：队员执行 → 校验评估Agent（自动校验，最多打回原队员修改 3 轮）
#         → 调度规划Agent（队长）需求对齐审批（不通过则重新分配，最多 3 次）
#         → 交互交付Agent（生成用户可读输出）+ 记忆管理Agent（上下文持久化）
#   两个计数都必须随 SQLite 快照持久化（见 task_snapshots.fix_rounds / reallocations），
#   否则审批中断恢复后计数归零 → 无限循环、Token 失控。
# ==========================================================================
MAX_EVALUATOR_FIX_ROUNDS = 3          # 校验评估Agent 打回原队员修改的上限：3 轮
MAX_REQUIREMENT_REALLOCATIONS = 3     # 队长需求对齐审批不通过后重新分配任务的上限：3 次
# 【第三轮·Bug1】30 秒超时的新语义：计时起点 = 后端收到 /api/approval/submit 的时刻，
#   超时判定的对象是"审批提交之后的 Agent 执行链路卡死"；等待用户点击阶段不计时。
APPROVAL_RESUME_TIMEOUT_SECONDS = 30.0

# 第4章 4.2 代码工程Agent：代码错误最多重试 3 次
MAX_CODE_FIX_RETRIES = 3

# 第9章 9.1 模型 429 限流：指数退避重试 3 次
MAX_RATE_LIMIT_RETRIES = 3
RATE_LIMIT_BASE_BACKOFF_SECONDS = 2.0   # 2s / 4s / 8s

# 第9章 9.1 工具调用失败：重试 2 次
MAX_TOOL_CALL_RETRIES = 2

# 第6章 6.1 Key 测试接口超时 5 秒，防止阻塞服务
KEY_TEST_TIMEOUT_SECONDS = 5.0

# ==========================================================================
# 第3章 3.2 审批事件专属字段：risk_level 取值
# ==========================================================================
RISK_LEVEL_HIGH = "high"
RISK_LEVEL_MID = "mid"
RISK_LEVEL_LOW = "low"
VALID_RISK_LEVELS = (RISK_LEVEL_HIGH, RISK_LEVEL_MID, RISK_LEVEL_LOW)

# 审批记录状态（前端审批面板展示语义）
APPROVAL_STATE_PENDING = "pending"       # 等待人工审批（任务暂停）
APPROVAL_STATE_MANUAL = "manual"         # 人工审批通过
APPROVAL_STATE_AUTO = "auto"             # 后端自动放行（低危）
APPROVAL_STATE_REJECTED = "rejected"     # 已拒绝
APPROVAL_STATE_TIMEOUT = "timeout"       # 【新增】30 秒审批超时（等价 rejected，绝不执行高危动作）

# 【新增】审批超时规则（需求硬性要求 7：不许简化）
#   审批开始计时 30 秒，30 秒内无任何用户操作 → 自动判定审批超时，等同于 rejected 拒绝。
APPROVAL_TIMEOUT_SECONDS = 30.0          # 审批等待上限：30 秒
APPROVAL_WATCHDOG_INTERVAL_SECONDS = 1.0  # 超时看门狗扫描间隔：1 秒（保证 30s 判定误差 < 1s）

# 【新增】审批结论口径（需求伪代码第 6 步 / 第 7 步）
#   allowed_once：✅ 执行一次 → 执行高危操作后继续当前队员子任务
#   rejected    ：❌ 拒绝     → 当前子任务标记 failed 并回传队长
APPROVAL_OUTCOME_ALLOWED_ONCE = "allowed_once"
APPROVAL_OUTCOME_REJECTED = "rejected"
APPROVAL_OUTCOMES = (APPROVAL_OUTCOME_ALLOWED_ONCE, APPROVAL_OUTCOME_REJECTED)

# 【新增】需求要求的状态机枚举口径（running / waiting_approval / finished / failed）。
#   系统既有字面量为 success，两套语义必须并行可用（旧数据/旧前端不受影响）：
#   finished 与 success 在**终态判定**上完全等价，仅字面量不同。
STATUS_FINISHED = "finished"             # 成功终态（与 STATUS_SUCCESS 等价）
TERMINAL_STATUSES = (STATUS_SUCCESS, STATUS_FINISHED, STATUS_FAILED)

# 第6章 6.3 高危审批开关永久强制开启，不可关闭
HIGH_RISK_APPROVAL_ALWAYS_ON = True

# ==========================================================================
# 第1章 1.3 模型固定分配（七角色 ↔ 模型绑定，不可修改 / 不可调换）
# ==========================================================================
PROVIDER_DEEPSEEK = "deepseek"
PROVIDER_QWEN = "qwen"
PROVIDER_KIMI = "kimi"
PROVIDER_GLM = "glm"

# 【需求点 BUG-NEW1 规则4】Kimi-Flash 已不再作为独立配置项出现在设置面板：
#   · 交互交付Agent 绑定 Kimi 厂商（kimi-k2.6），与 文档信息Agent 共用同一份 Kimi 密钥；
#   · 本常量仅保留给「历史配置识别 / 旧 provider 键名兼容」使用，
#     不再注册进 PROVIDERS（因此不会渲染任何 Kimi-Flash 零散输入项），
#     也不再作为生态位补位候选（避免与 kimi 重复占用同一把密钥）。
PROVIDER_KIMI_FLASH = "kimi_flash"

# 【需求点 Bug6】历史废弃 provider 键名：**仅用于识别并清理历史配置文件里的豆包残留**，
#   系统中不存在任何豆包调用逻辑（无 provider 注册、无 UI 输入项、无请求分支）。
PROVIDER_DOUBAO = "doubao"
DOUBAO_DEFAULT_MODEL = ""     # 占位：豆包已无默认模型

# 模型提供方（前端「设置」按厂商分组展示的全部厂商；豆包与 Kimi-Flash 零散项均已移除）
PROVIDERS = (
    {
        "provider": PROVIDER_DEEPSEEK,
        "name": "DeepSeek",
        # 【需求点 BUG-NEW1】Base URL 按需求给定值：https://api.deepseek.com
        #   （模型客户端会自动规范化为 .../v1/chat/completions，见 model_client._endpoint）
        "default_base_url": "https://api.deepseek.com",
        # 平台合法标识：deepseek-flash
        # （原 deepseek-v4.1-flash 会返回 400 MODEL_NOT_FOUND：
        #   The supported API model names are deepseek-flash, deepseek-v4-pro）
        "default_model": "deepseek-flash",
    },
    {
        "provider": PROVIDER_QWEN,
        "name": "通义千问 Qwen",
        "default_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        # 【需求点 BUG-NEW2】调度规划Agent = Qwen3.8-Max（全局大脑）
        "default_model": "qwen3.8-max",
    },
    {
        "provider": PROVIDER_KIMI,
        "name": "Kimi 月之暗面",
        "default_base_url": "https://api.moonshot.cn/v1",
        "default_model": "kimi-k3",
    },
    {
        "provider": PROVIDER_GLM,
        "name": "智谱 GLM",
        "default_base_url": "https://open.bigmodel.cn/api/paas/v4",
        # 【需求点 Bug8】评估校验Agent 绑定 GLM 5.3（厂商分组内的主模型标识）
        "default_model": "glm-5.3",
    },
)

# 七大 Agent 角色 ↔ 模型固定绑定（架构文档 1.3）
AGENT_DISPATCH = "调度规划Agent"
AGENT_CODE = "代码工程Agent"
AGENT_DOC = "文档信息Agent"
AGENT_VISION = "视觉感知Agent"
AGENT_MEMORY = "记忆管理Agent"
AGENT_EVALUATOR = "评估校验Agent"
AGENT_DELIVERY = "交互交付Agent"

AGENT_ROLES = (
    AGENT_DISPATCH,
    AGENT_CODE,
    AGENT_DOC,
    AGENT_VISION,
    AGENT_MEMORY,
    AGENT_EVALUATOR,
    AGENT_DELIVERY,
)

# ==========================================================================
# 【需求点 BUG-NEW2】最终固定 Agent-模型绑定映射（本轮唯一权威表）
#   后端调用逻辑 / 日志打印 / 思维链流式输出 / 前端状态栏 / 左下角 Agent 列表 /
#   配置面板 全部以本表为唯一来源，禁止任何一处硬编码或写反。
#     · 代码工程 Agent → DeepSeek-Flash        （deepseek / deepseek-flash）
#     · 文档信息 Agent → Kimi K3                （kimi / kimi-k3）
#     · 视觉感知 Agent → Qwen3.8-Flash          （qwen / qwen3.8-flash）
#     · 调度规划 Agent → Qwen3.8-Max            （qwen / qwen3.8-max）
#     · 评估校验 Agent → GLM 5.3                （glm / glm-5.3）
#     · 交互交付 Agent → Kimi k2.6              （kimi / kimi-k2.6）
#     · 记忆管理 Agent → GLM-5.3-Flash          （glm / glm-5.3-flash）
# ==========================================================================
AGENT_BINDINGS = {
    # 调度规划Agent：全局大脑 → Qwen3.8-Max（工程化逻辑拆解能力）
    AGENT_DISPATCH: {
        "agent": AGENT_DISPATCH,
        "model_name": "Qwen3.8-Max",
        "provider": PROVIDER_QWEN,
        "model": "qwen3.8-max",
        "duty": "全局大脑、任务拆解、流程管控",
    },
    # 代码工程Agent：核心执行端 → DeepSeek-Flash（顶级代码 + 推理能力）
    AGENT_CODE: {
        "agent": AGENT_CODE,
        "model_name": "DeepSeek-Flash",
        "provider": PROVIDER_DEEPSEEK,
        "model": "deepseek-flash",
        "duty": "代码生成、修改、调试、文件操作、命令执行",
    },
    AGENT_DOC: {
        "agent": AGENT_DOC,
        "model_name": "Kimi K3",
        "provider": PROVIDER_KIMI,
        "model": "kimi-k3",
        "duty": "超长文档解析、抽取、摘要、知识库处理",
    },
    AGENT_VISION: {
        "agent": AGENT_VISION,
        # 【需求点 BUG-NEW2 变更点1】视觉感知Agent 由 Qwen-VL 改为 Qwen3.8-Flash
        "model_name": "Qwen3.8-Flash",
        "provider": PROVIDER_QWEN,
        "model": "qwen3.8-flash",
        "duty": "截图识别、UI识别、报错看图分析",
    },
    AGENT_MEMORY: {
        "agent": AGENT_MEMORY,
        "model_name": "GLM-5.3-Flash",
        "provider": PROVIDER_GLM,
        "model": "glm-5.3-flash",
        "duty": "记忆压缩、向量化、检索、归档",
    },
    AGENT_EVALUATOR: {
        "agent": AGENT_EVALUATOR,
        "model_name": "GLM 5.3",
        "provider": PROVIDER_GLM,
        "model": "glm-5.3",
        "duty": "质量校验、格式校验、安全校验、裁判审核",
    },
    # 【需求点 Bug6 / Bug7 / Bug8】交互交付Agent：用户门面（Kimi k2.6，中文润色）
    AGENT_DELIVERY: {
        "agent": AGENT_DELIVERY,
        "model_name": "Kimi k2.6",
        "provider": PROVIDER_KIMI,
        "model": "kimi-k2.6",
        "duty": "用户对话、需求澄清、结果润色、前端展示格式化",
    },
}

# ==========================================================================
# 【需求点 三、模型生态位自动补位】
#   专属模型不可用时，自动按固定优先级使用本机已连通测试通过的其他模型补齐生态位。
#   【BUG-NEW1 规则4】Kimi-Flash 已收拢进 Kimi 厂商分组，不再单独占用补位位次
#   （它与 Kimi 共用同一把 Moonshot 密钥，重复占位会造成候选冗余）。
#   优先级：DeepSeek > Qwen > GLM > Kimi
# ==========================================================================
ECOSYSTEM_FALLBACK_PRIORITY: tuple[str, ...] = (
    PROVIDER_DEEPSEEK,
    PROVIDER_QWEN,
    PROVIDER_GLM,
    PROVIDER_KIMI,
)

ECOSYSTEM_FALLBACK_PRIORITY_TEXT = "DeepSeek > Qwen > GLM > Kimi"

# 配置架构版本：用于把历史配置文件平滑迁移到新字段/新模型标识（不丢失用户自定义）
CONFIG_SCHEMA_VERSION = "1.5"     # 【BUG-NEW1/NEW2】厂商分组模型清单 + Agent↔模型绑定重排

# 【需求点 Bug6】豆包历史模型标识：仅用于识别并清理历史配置残留（不再有任何调用逻辑）
DOUBAO_LEGACY_DEFAULT_MODELS: tuple[str, ...] = (
    "doubao-seed-1-6", "doubao-seed-1.6", "doubao-pro-32k", "turbo-2.1", "turbo2.1",
    "doubao-seed-2-1-turbo-260628", "doubao-lite-32k", "doubao-seed-1-6-250615",
)

# 【需求点 Bug1】DeepSeek 历史错误模型标识 → 现用合法标识
DEEPSEEK_LEGACY_DEFAULT_MODELS: tuple[str, ...] = (
    "deepseek-v4.1-flash", "deepseek-v4-flash", "deepseek-v4.1", "deepseek-flash-preview",
)
DEEPSEEK_DEFAULT_MODEL = "deepseek-flash"

# 【需求点 BUG-NEW2】Qwen 历史模型标识 → 现用合法标识
#   调度规划Agent 由 Qwen Coder(qwen3-coder-plus) 改为 Qwen3.8-Max(qwen3.8-max)；
#   视觉感知Agent 由 Qwen-VL(qwen3-vl-plus) 改为 Qwen3.8-Flash(qwen3.8-flash)。
QWEN_LEGACY_DEFAULT_MODELS: tuple[str, ...] = (
    "qwen3-coder-plus", "qwen3-coder", "qwen3-vl-plus", "qwen-vl-plus",
    "qwen-vl-max", "qwen-plus", "qwen-max", "qwen3-max", "qwen-turbo",
)
QWEN_DEFAULT_MODEL = "qwen3.8-max"
QWEN_VISION_MODEL = "qwen3.8-flash"

# 【需求点 BUG-NEW1 规则4】Kimi-Flash 历史标识：仅用于识别并清理历史配置残留。
#   系统不再注册 Kimi-Flash 独立厂商，其能力全部收拢到 Kimi 厂商分组管理。
KIMI_FLASH_DEFAULT_MODEL = "kimi-flash"
KIMI_FLASH_LEGACY_MODELS: tuple[str, ...] = (
    "kimi-flash", "kimi-flash-8k", "kimi-flash-32k", "moonshot-flash",
)
KIMI_LEGACY_DEFAULT_MODELS: tuple[str, ...] = (
    "moonshot-v1-128k", "moonshot-v1-32k", "kimi-k2",
)

# 能力标签：用于「能力过滤，跳过能力不匹配的模型」
CAP_VISION = "vision"            # 支持图片多模态输入
CAP_CODE = "code"                # 代码能力较强
CAP_LONG_CONTEXT = "long_context"  # 大长上下文
CAP_TEXT = "text"                # 通用文本

# 模型能力画像（用于生态位补位的候选过滤与择优）；豆包已移除
PROVIDER_CAPABILITIES: dict[str, dict] = {
    PROVIDER_DEEPSEEK: {
        "name": "DeepSeek",
        "caps": (CAP_TEXT, CAP_CODE, CAP_LONG_CONTEXT),
        "code_strength": 10, "long_context_strength": 8, "vision": False,
    },
    PROVIDER_QWEN: {
        "name": "通义千问 Qwen",
        "caps": (CAP_TEXT, CAP_VISION, CAP_CODE, CAP_LONG_CONTEXT),
        "code_strength": 9, "long_context_strength": 8, "vision": True,
    },
    PROVIDER_GLM: {
        "name": "智谱 GLM",
        "caps": (CAP_TEXT, CAP_VISION, CAP_LONG_CONTEXT),
        "code_strength": 7, "long_context_strength": 8, "vision": True,
    },
    PROVIDER_KIMI: {
        "name": "Kimi 月之暗面",
        "caps": (CAP_TEXT, CAP_LONG_CONTEXT),
        "code_strength": 6, "long_context_strength": 10, "vision": False,
    },
    # 【需求点 BUG-NEW1 规则4】Kimi-Flash 不再是独立厂商：其能力已并入 Kimi 厂商分组，
    # 不再单独参与生态位补位候选（避免与 kimi 重复占用同一把 Moonshot 密钥）。
}
# Kimi 厂商内部模型标识级能力画像（同厂商多模型时用于能力过滤与取模）：
#   kimi-k3    长上下文最强 → 文档信息Agent
#   kimi-k2.6  中文润色     → 交互交付Agent
KIMI_MODEL_CAPABILITIES: dict[str, dict] = {
    "kimi-k3": {"caps": (CAP_TEXT, CAP_LONG_CONTEXT), "long_context_strength": 10},
    "kimi-k2.6": {"caps": (CAP_TEXT,), "long_context_strength": 8},
}

# Qwen 厂商内部模型标识级能力画像（【BUG-NEW2】同厂商两个 Agent 各用专属模型）：
#   qwen3.8-max    调度规划Agent → 工程化逻辑拆解
#   qwen3.8-flash  视觉感知Agent → 多模态识别（保留 vision 能力标签）
QWEN_MODEL_CAPABILITIES: dict[str, dict] = {
    "qwen3.8-max": {"caps": (CAP_TEXT, CAP_CODE, CAP_LONG_CONTEXT), "vision": False},
    "qwen3.8-flash": {"caps": (CAP_TEXT, CAP_VISION), "vision": True},
}

# 每个 Agent 生态位对候选模型的硬性能力要求（不满足直接跳过）
AGENT_REQUIRED_CAPABILITY: dict[str, str] = {
    AGENT_VISION: CAP_VISION,          # 仅尝试支持图片多模态的模型
    AGENT_CODE: CAP_CODE,              # 优先挑选代码能力较强模型
    AGENT_DOC: CAP_LONG_CONTEXT,       # 优先挑选大长上下文模型
    AGENT_DISPATCH: CAP_TEXT,
    AGENT_MEMORY: CAP_TEXT,
    AGENT_EVALUATOR: CAP_TEXT,
    AGENT_DELIVERY: CAP_TEXT,
}

# 补位失败统一提示（需求点 三、3）
ECOSYSTEM_FALLBACK_EXHAUSTED_MESSAGE = "缺少可用模型，该Agent生态位无法补位，请检查API密钥配置"


# ==========================================================================
# 【需求点 Bug1（硬编码高危识别）】后端硬编码高危动作常量集合
#   ------------------------------------------------------------------
#   改造原则：**高危动作识别完全由后端程序硬编码实现，不依赖 AI 大模型输出文本**，
#   也不允许 AI 自行判断"这是不是高危"。只要准备执行下列类型的动作，
#   程序强制触发人在回路审批，不受模型输出内容影响。
#   写死在代码里，禁止做成可配置项（防止被绕过）。
# ==========================================================================
HIGH_RISK_OP_FILE_BATCH_DELETE = "file_batch_delete"    # 批量删除文件
HIGH_RISK_OP_FILE_SINGLE_DELETE = "file_single_delete"  # 单个文件删除
HIGH_RISK_OP_FOLDER_REMOVE = "folder_remove"            # 删除文件夹
HIGH_RISK_OP_SHELL_RUN = "shell_run"                    # 执行 shell 命令

HIGH_RISK_OP_SET = frozenset({
    HIGH_RISK_OP_FILE_BATCH_DELETE,
    HIGH_RISK_OP_FILE_SINGLE_DELETE,
    HIGH_RISK_OP_FOLDER_REMOVE,
    HIGH_RISK_OP_SHELL_RUN,
})

# 高危动作类型 → 中文可读名称（审批单 / 报告展示）
HIGH_RISK_OP_LABEL: dict[str, str] = {
    HIGH_RISK_OP_FILE_BATCH_DELETE: "批量删除文件",
    HIGH_RISK_OP_FILE_SINGLE_DELETE: "删除单个文件",
    HIGH_RISK_OP_FOLDER_REMOVE: "删除文件夹",
    HIGH_RISK_OP_SHELL_RUN: "执行 shell 命令",
}

# 审批通过后由后端 Python 原生执行的删除动作（本系统只对删除类做原生执行）
HIGH_RISK_OP_DELETE_SET = frozenset({
    HIGH_RISK_OP_FILE_BATCH_DELETE,
    HIGH_RISK_OP_FILE_SINGLE_DELETE,
    HIGH_RISK_OP_FOLDER_REMOVE,
})

# 高危动作是否需要人工审批：集合内一律强制审批（写死 True，不可关闭）
HIGH_RISK_REQUIRES_APPROVAL = True

# 单次批量删除超过该数量 → 归入 file_batch_delete，否则归入 file_single_delete
HIGH_RISK_BATCH_DELETE_THRESHOLD = 2

# 【需求点 Bug1】"虚报执行完成"检测用的**硬编码句式**（后端判定，不交给模型判断）
#   说明：不做"动作词 + 完成词"的粗匹配（那会把"清理完成度分析"这类分析句误判），
#   而是直接匹配明确的完成态声明句式；只有这些句式**没有后端真实回执支撑**时才纠偏。
EXECUTION_CLAIM_PATTERNS: tuple[str, ...] = (
    r"已(经)?(全部|均|成功)?删(除|掉|去)",
    r"已(经)?(全部|均|成功)?移(除|走)",
    r"已(经)?(全部|均|成功)?清(理|空)",
    r"(删除|清理|移除|清空)(操作)?(已)?(全部)?(完成|完毕|成功)",
    r"(已|均)(执行|完成)(了)?(删除|清理|移除|清空)",
    r"删(除|掉)了\s*\d+\s*(个|项|条)?\s*(文件|目录|文件夹)?",
    r"(文件|目录|文件夹)\s*(已|均)\s*不(再)?存在",
    r"已(经)?(成功)?(重命名|改名|移动|覆盖|写入|落盘|改名完成)",
    r"已(经)?(成功)?执行(了)?(shell|命令|command)",
)

# ==========================================================================
# 【需求点 Bug1】后端硬编码高危动作常量结束
# ==========================================================================

# 第4章 4.1 调度规划Agent 专属组件名
AGENT_ROUTER_NAME = "Agent_Router"

# ==========================================================================
# 第5章 裸机目录规范（固定路径）
# ==========================================================================
ECOSYSTEM_DIR_NAME = ".multi_agent_ecosystem"
SUBDIR_CONFIG = "config"
SUBDIR_SESSIONS = "sessions"
SUBDIR_UPLOADS = "uploads"
SUBDIR_LOGS = "logs"
SUBDIR_VECTOR_DB = "vector_db"

# 会话内子目录
SESSION_SUBDIR_UPLOAD = "upload"
SESSION_SUBDIR_WORKSPACE = "workspace"
SESSION_SUBDIR_ARTIFACT = "artifact"

# ==========================================================================
# 第6章 6.2 文件上传安全限制
# ==========================================================================
MAX_UPLOAD_BYTES = 50 * 1024 * 1024   # 单文件最大 50MB

# 白名单格式：图片、文档、文本
ALLOWED_UPLOAD_EXTENSIONS = {
    # 图片
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".avif", ".svg",
    # 文档
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv", ".rtf", ".odt",
    # 文本
    ".txt", ".md", ".markdown", ".json", ".yaml", ".yml", ".xml", ".html", ".htm",
    ".log", ".ini", ".toml", ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go",
    ".rs", ".c", ".h", ".cpp", ".hpp", ".cs", ".sql", ".cfg", ".conf",
}

# 禁止可执行文件上传：exe、bat、sh、bin
FORBIDDEN_UPLOAD_EXTENSIONS = {
    ".exe", ".bat", ".cmd", ".com", ".scr", ".msi", ".sh", ".bin",
    ".dll", ".so", ".dylib", ".vbs", ".ps1", ".jar", ".app", ".apk", ".sys", ".drv",
}

# 资源类型分类（第3章 3.3 图片资源传输规则 / 4.3 文档信息Agent）
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".avif"}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".rtf", ".odt", ".csv"}
TEXT_EXTENSIONS = ALLOWED_UPLOAD_EXTENSIONS - IMAGE_EXTENSIONS - DOCUMENT_EXTENSIONS

# ==========================================================================
# 第6章 6.3 局域网访问安全
# ==========================================================================
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 5090
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123"   # 首次初始化写入，bcrypt 哈希存储，禁止明文落盘

# ==========================================================================
# 第4章 4.5 记忆管理Agent：90天TTL自动淘汰
# ==========================================================================
MEMORY_LONG_TERM_TTL_DAYS = 90

# 第2章 2.3 向量库维度上限保护 / 检索返回条数
MEMORY_SEARCH_TOP_K = 5

# ==========================================================================
# 第3章 3.3 图片资源传输规则：消息 metadata 字段名
# ==========================================================================
META_IMAGE_RESOURCES = "image_resources"

# 本地不可用占位（模型/向量库异常时的统一错误码）
ERR_MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
ERR_RATE_LIMITED = "MODEL_RATE_LIMITED"
# 【GLM 1113】余额不足（GLM 平台以 HTTP 429 + 错误码 1113 返回）→ 不可重试
ERR_INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
ERR_TOOL_FAILED = "TOOL_CALL_FAILED"
ERR_TIMEOUT = "TASK_TIMEOUT"
ERR_LOOP = "CIRCULAR_DEPENDENCY"
ERR_ITERATION_LIMIT = "ITERATION_LIMIT_EXCEEDED"
ERR_SECURITY = "SECURITY_VIOLATION"
# 【需求点 三、3】全部候选模型补位失败
ERR_ECOSYSTEM_EXHAUSTED = "ECOSYSTEM_FALLBACK_EXHAUSTED"

# 密钥连通性测试的结构化错误码（【需求点 Bug1】返回可读中文原因）
KEY_ERROR_INVALID = "KEY_INVALID"              # 密钥无效
KEY_ERROR_AUTH = "AUTH_FAILED"                 # 鉴权失败
KEY_ERROR_TIMEOUT = "CONNECT_TIMEOUT"          # 接口连接超时
KEY_ERROR_NETWORK = "NETWORK_ERROR"            # 网络异常
KEY_ERROR_RATE_LIMITED = "RATE_LIMITED"        # 触发限流
KEY_ERROR_MODEL_NOT_FOUND = "MODEL_NOT_FOUND"  # 模型不存在/无权限
KEY_ERROR_ENDPOINT = "ENDPOINT_ERROR"          # 接口地址不可用
KEY_ERROR_SERVER = "SERVER_ERROR"              # 服务端错误
KEY_ERROR_MISSING = "KEY_MISSING"              # 未配置密钥
KEY_ERROR_EMPTY_RESPONSE = "EMPTY_RESPONSE"    # 返回内容为空
# 【GLM 1113】余额不足：密钥有效但账户欠费，充值前任何重试都无意义
KEY_ERROR_INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
KEY_ERROR_UNKNOWN = "UNKNOWN_ERROR"            # 未知错误

# ==========================================================================
# 【需求点 二、前端改造】工作区分组
# ==========================================================================
DEFAULT_WORKSPACE_ID = "ws_default"
DEFAULT_WORKSPACE_NAME = "默认工作区"

# 工作区类型：
#   local_folder —— 用户通过系统文件夹选择器添加的本地磁盘目录（Agent 操作根目录）
#   system       —— 系统默认隔离工作区（sessions/<sid>/workspace，向后兼容）
WORKSPACE_KIND_LOCAL = "local_folder"
WORKSPACE_KIND_SYSTEM = "system"

# 工作区文件夹标记文件：用于把浏览器选择到的"文件夹名"反查为磁盘绝对路径
WORKSPACE_MARKER_FILE = ".mae_workspace.json"

# 【需求点 二、2 工作区安全权限硬约束】越权访问统一提示（前端直接展示该文案）
WORKSPACE_ACCESS_DENIED_MESSAGE = "越权访问禁止：只能操作当前工作目录内文件"
ERR_WORKSPACE_ACCESS_DENIED = "WORKSPACE_ACCESS_DENIED"

# ==========================================================================
# 【需求点 一、路径校验 Bug 修复】系统元文件 / Agent 业务文件 分类常量
#   区分两个目录概念（架构文档第5章 + 第2章 2.2）：
#     1) 全局会话存储目录：%USERPROFILE%\.multi_agent_ecosystem\（config/sessions/
#        uploads/logs/vector_db）—— 属于系统元数据，**不受工作区白名单限制**；
#     2) 用户业务工作区目录：用户在任意磁盘选择的文件夹 —— **仅 Agent 读写
#        业务文件时受白名单限制**。
#   is_system_meta_file() 只认可下列类别；SECURITY_META_CATEGORY_WORKSPACE_BUSINESS
#   绝不属于元文件，避免把业务文件误判为元文件而绕过越权校验。
# ==========================================================================
SECURITY_META_CATEGORY_SYSTEM = "system_meta"        # 通用系统元文件
SECURITY_META_CATEGORY_SESSION = "session_meta"      # 会话元数据 / 会话 json
SECURITY_META_CATEGORY_CONFIG = "config_meta"        # 系统配置（含加密信封）
SECURITY_META_CATEGORY_LOG = "log_meta"              # 日志库 / 日志文件
SECURITY_META_CATEGORY_UPLOAD = "upload_meta"        # 上传隔离区
SECURITY_META_CATEGORY_VECTOR = "vector_meta"        # 向量记忆库
SECURITY_META_CATEGORY_WORKSPACE_BUSINESS = "workspace_business"   # Agent 业务文件（不在白名单内免检）

SECURITY_META_CATEGORIES: tuple[str, ...] = (
    SECURITY_META_CATEGORY_SYSTEM,
    SECURITY_META_CATEGORY_SESSION,
    SECURITY_META_CATEGORY_CONFIG,
    SECURITY_META_CATEGORY_LOG,
    SECURITY_META_CATEGORY_UPLOAD,
    SECURITY_META_CATEGORY_VECTOR,
)

# 【需求点 一、2】会话加载路径异常的优雅降级：不崩溃，返回可读提示
ERR_SESSION_FILES_UNAVAILABLE = "SESSION_FILES_UNAVAILABLE"
SESSION_FILES_UNAVAILABLE_MESSAGE = (
    "会话目录文件清单读取失败（已优雅降级，会话数据本身仍然可用）"
)

# ==========================================================================
# 【需求点 二、任务执行计时器】状态与错误码
#   计时启停与任务状态机严格对齐（第3章 3.4 五状态）：
#     pending/running → running         （计时中）
#     waiting_approval → paused          （任务暂停 → 停表，恢复后继续）
#     success/failed   → success/failed  （任务结束 → 停表并落库）
#     用户取消/终止     → cancelled       （同样停表并落库）
# ==========================================================================
TIMER_STATUS_IDLE = "idle"            # 未开始任务
TIMER_STATUS_RUNNING = "running"      # 计时中
TIMER_STATUS_PAUSED = "paused"        # 暂停计时（任务进入 waiting_approval）
TIMER_STATUS_SUCCESS = "success"      # 任务成功结束
TIMER_STATUS_FAILED = "failed"        # 任务失败结束
TIMER_STATUS_CANCELLED = "cancelled"  # 任务被取消 / 终止

TIMER_END_STATUSES: tuple[str, ...] = (
    TIMER_STATUS_SUCCESS, TIMER_STATUS_FAILED, TIMER_STATUS_CANCELLED,
)

TIMER_IDLE_TEXT = "未开始任务"
TIMER_PREFIX = "已耗时："

ERR_TIMER_RECORD_FAILED = "TASK_TIMER_RECORD_FAILED"
ERR_TIMER_NOT_FOUND = "TASK_TIMER_NOT_FOUND"

# ==========================================================================
# 【需求点 一、Bug1】模型标识本地校验清单
#   用于保存配置时提前拦截平台不支持的模型名，避免提交后才出现 400 MODEL_NOT_FOUND
# ==========================================================================
KNOWN_MODELS: dict[str, tuple[str, ...]] = {
    PROVIDER_DEEPSEEK: (
        # 平台报错原文：The supported API model names are deepseek-flash, deepseek-v4-pro
        "deepseek-flash", "deepseek-v4-pro", "deepseek-chat", "deepseek-reasoner",
    ),
    PROVIDER_QWEN: (
        # 【需求点 BUG-NEW2】Qwen 厂商当前使用的两个模型标识（同属 DashScope OpenAI 兼容模式）
        "qwen3.8-max", "qwen3.8-flash",
        # 平台仍可能开通的历史标识（保留登记，避免误判为非法）
        "qwen3-coder-plus", "qwen3-vl-plus", "qwen-plus", "qwen-max",
    ),
    PROVIDER_KIMI: (
        "kimi-k3", "kimi-k2.6",
        # 【需求点 BUG-NEW1 规则4】Kimi-Flash 已收拢到 Kimi 厂商：历史标识保留登记
        "kimi-flash", "kimi-flash-8k", "kimi-flash-32k",
        "kimi-k2", "moonshot-v1-128k", "moonshot-v1-32k",
    ),
    PROVIDER_GLM: ("glm-5.3", "glm-5.3-flash", "glm-4.6", "glm-4.6-flash", "glm-4-plus",
                   "glm-4-flash", "glm-4v-plus"),
}

# 各 provider 官方模型列表查询入口（校验失败时给出的可读建议）
PROVIDER_MODEL_DOC_HINT: dict[str, str] = {
    PROVIDER_DEEPSEEK: "DeepSeek 平台当前支持的模型标识示例：deepseek-flash、deepseek-v4-pro",
    PROVIDER_QWEN: "通义千问请使用 OpenAI 兼容模式下的模型标识，如 qwen3.8-max、qwen3.8-flash",
    PROVIDER_KIMI: "Kimi 请使用如 kimi-k3、kimi-k2.6 等平台已开通的模型标识",
    PROVIDER_GLM: "智谱 GLM 请使用如 glm-5.3、glm-5.3-flash 等平台已开通的模型标识",
}

# ==========================================================================
# 【需求点 Bug1 · 修复】模型参数约束清单（唯一权威来源，后续新模型在此扩展）
#   ------------------------------------------------------------------
#   平台对个别模型强制要求固定参数，业务侧传其它值会被直接 400 拒绝。
#   典型：Kimi k2.6 强制 temperature 只能等于 1，否则返回
#         {"error":{"message":"invalid temperature: only 1 is allowed for this model"}}
#   连通测试原先不携带 temperature 参数，所以"测试连通通过、真实调用 400"。
#
#   字段说明：
#     forced_params   ：调用该模型时必须强制写入请求体的参数（覆盖全局配置）
#     fixed_params    ：模型被平台**完全锁定**、业务不得下发任何取值的参数名
#                       （连通测试也必须一并省略，避免测试本身被 400 拒绝）
#     locked_reason   ：约束原因（写入系统日志与前端提示，便于排查）
#     max_output_tokens：平台 max_tokens 上限（0 表示不限制，不做处理）
#
#   匹配顺序（build_chat_body / resolve_model_constraints 共用）：
#     1) 精确模型标识（kimi-k2.6）
#     2) 带厂商前缀的全限定名（kimi/kimi-k2.6）
#     3) 厂商内同义标识别名
#     4) 厂商主模型（provider 级兜底，保证"用户自定义该厂商模型"时约束不丢）
#
#   ⚠️ 只做"参数适配"，不改变任何业务温度配置：约束命中时在调用层强制覆盖，
#      未命中的模型继续沿用系统 AGENT_TEMPERATURES 配置。
# ==========================================================================
MODEL_CONSTRAINTS: dict[str, dict] = {
    # 【Bug1】Kimi k2.6：平台强制 temperature 只能为 1
    "kimi-k2.6": {
        "provider": PROVIDER_KIMI,
        "forced_params": {"temperature": 1.0},
        "fixed_params": ("temperature",),
        "max_output_tokens": 0,
        "locked_reason": (
            "Kimi k2.6 平台限制：temperature 只允许等于 1，"
            "系统已自动固定 temperature=1.0（忽略全局温度配置），不再返回 400 invalid temperature"
        ),
    },
    # 预留同义标识别名（历史 / 变体写法统一收敛到同一约束）
    "kimi-k2-6": {"alias_of": "kimi-k2.6"},
    "moonshot-kimi-k2.6": {"alias_of": "kimi-k2.6"},
}

# 厂商 → 该厂商主模型标识（约束兜底匹配用；未登记则退回 PROVIDER_DEFAULT_MODELS）
MODEL_CONSTRAINTS_PROVIDER_FALLBACK: dict[str, str] = {
    PROVIDER_KIMI: "kimi-k2.6",
}

# 约束命中时写入系统日志的事件名（供排查"为什么 temperature 被改写"）
MODEL_CONSTRAINT_LOG_EVENT = "MODEL_PARAM_CONSTRAINT_APPLIED"

# 【需求点 Bug7 / BUG-NEW2】各 provider 的出厂默认模型标识。
#   用途：当配置里的 provider 模型仍是「出厂默认值」或历史默认值（用户未在设置页自定义）时，
#   允许按 Agent 绑定表使用各自的专属模型 —— 这样同 provider 下的多个 Agent
#   （如 Qwen 的 调度规划Agent=qwen3.8-max / 视觉感知Agent=qwen3.8-flash，
#     GLM 的 评估校验Agent=glm-5.3 / 记忆管理Agent=glm-5.3-flash）不会互相串模型。
PROVIDER_DEFAULT_MODELS: dict[str, str] = {
    p["provider"]: str(p["default_model"]) for p in PROVIDERS
}

# ==========================================================================
# 【需求点 Bug4 / 一、温度参数】七大 Agent 温度统一登记表（唯一权威来源）
#   依据需求文档给出的容错区间划分：
#     · 顶层主控（调度规划Agent）：0.3 黄金平衡点 —— 保留强指令跟随，同时具备容错能力，
#       用户表述不严谨 / 与工程文档轻微不一致时不会直接罢工；不建议一步跳到 0.5 以上。
#     · 编码子 Agent（代码工程Agent）：0.15 —— 代码生成与工具调用必须严谨，不允许过度灵活。
#     · 文档 / 信息检索子 Agent（文档信息Agent）：0.25 —— 区间 0.2~0.3。
#     · 总结 / 润色子 Agent（交互交付Agent）：0.45 —— 区间 0.4~0.5。
#     · 质量闸门（评估校验Agent）：0.0 —— 裁判必须零随机性（不在上调范围内，安全边界不放宽）。
#     · 视觉感知Agent：0.2（结构化抽取，低随机）。
#     · 记忆管理Agent：0.15（后台压缩归档，需稳定）。
#   禁止在 Agent 代码里散落裸温度字面量，一律引用本表。
# ==========================================================================
AGENT_TEMPERATURES: dict[str, float] = {
    AGENT_DISPATCH: 0.30,     # 顶层主控：黄金平衡点（0.3，可调 0.35 / 0.4）
    AGENT_CODE: 0.15,         # 编码子 Agent：严谨，不上调
    AGENT_DOC: 0.25,          # 文档/检索子 Agent：0.2~0.3
    AGENT_VISION: 0.20,       # 视觉感知：结构化抽取
    AGENT_MEMORY: 0.15,       # 记忆管理：后台压缩归档
    AGENT_EVALUATOR: 0.00,    # 质量闸门：零随机（安全边界，保持不放宽）
    AGENT_DELIVERY: 0.45,     # 总结/润色子 Agent：0.4~0.5
}

# 温度区间说明（设置面板 / 日志展示用，纯说明性文案）
AGENT_TEMPERATURE_NOTE = (
    "调度规划 0.3（黄金平衡点）· 代码工程 0.15 · 文档信息 0.25 · 视觉感知 0.2 · "
    "记忆管理 0.15 · 评估校验 0.0（裁判零随机）· 交互交付 0.45"
)


# ==========================================================================
# 【需求点 BUG-NEW1】厂商（Vendor）分组模型配置 —— 设置弹窗严格按此布局渲染
#   一个厂商只填写一次 API Key / Base URL；厂商内部映射多个模型标识，
#   每个 Agent 通过 AGENT_BINDINGS 映射到本厂商下的具体 model_name。
#   UI 与接口一律按本表渲染，禁止前端硬编码模型清单。
#   【规则4】Kimi-Flash 零散配置项已删除：其能力全部收拢到 Kimi 厂商分组下管理
#   （kimi-k3 = 文档信息Agent；kimi-k2.6 = 交互交付Agent，二者共用同一把 Kimi 密钥）。
# ==========================================================================
VENDOR_GROUPS: tuple[dict, ...] = (
    {
        "vendor_id": "deepseek",
        "provider": PROVIDER_DEEPSEEK,
        "title": "Deepseek",
        "description": "核心执行端（代码工程Agent）：顶级代码 + 推理能力",
        # 该厂商下需要用到的模型标识（第一项为该厂商主模型）
        "models": (
            {"model": "deepseek-flash", "label": "deepseek-flash"},
        ),
    },
    {
        "vendor_id": "qwen",
        "provider": PROVIDER_QWEN,
        "title": "通义千问",
        "description": "调度规划 + 视觉感知：Qwen3.8-Max 负责工程化拆解，Qwen3.8-Flash 负责多模态识别",
        "models": (
            {"model": "qwen3.8-max", "label": "Qwen3.8-Max"},
            {"model": "qwen3.8-flash", "label": "Qwen3.8-Flash"},
        ),
    },
    {
        "vendor_id": "kimi",
        "provider": PROVIDER_KIMI,
        "title": "Kimi",
        "description": "知识库核心 + 用户门面：超长文档处理 + 中文润色（含原 Kimi-Flash 能力）",
        "models": (
            {"model": "kimi-k3", "label": "Kimi K3"},
            {"model": "kimi-k2.6", "label": "Kimi k2.6"},
        ),
    },
    {
        "vendor_id": "glm",
        "provider": PROVIDER_GLM,
        "title": "智谱",
        "description": "质量闸门 + 后台支撑：规则遵循 / 低幻觉 + 低成本高并发",
        "models": (
            {"model": "glm-5.3", "label": "GLM 5.3"},
            {"model": "glm-5.3-flash", "label": "GLM-5.3-Flash"},
        ),
    },
)

# 【需求点 BUG-NEW1 规则4】已下线/已收拢的厂商：不再渲染任何输入项，
#   仅用于历史配置识别与清理（密钥信封、模型标识残留）。
RETIRED_VENDOR_PROVIDERS: tuple[str, ...] = (PROVIDER_KIMI_FLASH, PROVIDER_DOUBAO)

# 厂商分组按 provider 反查（model_client / config_store / main 共用）
VENDOR_GROUP_BY_PROVIDER: dict[str, dict] = {
    g["provider"]: g for g in VENDOR_GROUPS
}


def vendor_models(provider: str) -> tuple[dict, ...]:
    """该厂商需要配置的模型清单（第一项为主模型）；未登记厂商回退为单模型。"""
    group = VENDOR_GROUP_BY_PROVIDER.get(provider)
    if group:
        return tuple(group["models"])
    # 【BUG-NEW1 规则4】已收拢厂商不再返回任何模型清单 → 前端不会渲染任何输入项
    if provider in RETIRED_VENDOR_PROVIDERS:
        return ()
    default_model = PROVIDER_DEFAULT_MODELS.get(provider, "")
    return ({"model": default_model, "label": default_model},) if default_model else ()


def vendor_agents(provider: str) -> list[dict]:
    """该厂商下绑定的全部 Agent（角色 → 专属模型标识）。"""
    out: list[dict] = []
    for role, binding in AGENT_BINDINGS.items():
        if binding["provider"] == provider:
            out.append({
                "agent": role,
                "model_name": binding["model_name"],
                "model": binding["model"],
                "duty": binding["duty"],
            })
    return out


# 【需求点 BUG-NEW2】Agent ↔ 模型绑定映射表（对外展示口径，供文档/前端/日志核对）
AGENT_MODEL_MAP_TEXT: tuple[tuple[str, str, str], ...] = tuple(
    (role, AGENT_BINDINGS[role]["model_name"], AGENT_BINDINGS[role]["model"])
    for role in AGENT_ROLES
)


# ==========================================================================
# 【BUG-A 3】真实业务调用失败的完整错误记录（HTTP 状态码 + response 原文）
ERR_MODEL_RUNTIME_CALL_FAILED = "MODEL_RUNTIME_CALL_FAILED"
# 【BUG-A 1/2】Agent ↔ 厂商 ↔ 模型 ↔ 密钥 运行态绑定审计（连通测试 / 真实调用同源核对）
EVENT_MODEL_BINDING_AUDIT = "model.binding.audit"

# 【BUG-A 4】补位触发条件收紧：只有「密钥缺失 / 配置为空」才触发补位；
#   配置存在（已加密落盘 + 连通测试通过）时不得无故补位。
FALLBACK_ONLY_ON_MISSING_KEY = True

# 配置可用性判定等级（供前端与日志展示，禁止用 success 掩盖）
CONFIG_STATE_READY = "ready"                  # 密钥已落盘 + 该模型连通测试通过
CONFIG_STATE_KEY_MISSING = "key_missing"      # 该厂商密钥缺失 → 允许补位
CONFIG_STATE_MODEL_EMPTY = "model_empty"      # 模型标识为空 → 允许补位
CONFIG_STATE_NOT_TESTED = "not_tested"        # 已配置但未连通测试 → 不补位，如实报错
CONFIG_STATE_CALL_FAILED = "call_failed"      # 配置正常但本次调用失败 → 不补位（除非开关放行）
CONFIG_STATE_KEY_MISMATCH = "key_mismatch"    # 连通测试用的密钥 ≠ 已保存的密钥（需重新保存）

CONFIG_STATE_LABEL: dict[str, str] = {
    CONFIG_STATE_READY: "配置就绪（密钥已落盘 + 连通测试通过）",
    CONFIG_STATE_KEY_MISSING: "密钥缺失",
    CONFIG_STATE_MODEL_EMPTY: "模型标识为空",
    CONFIG_STATE_NOT_TESTED: "已配置但未完成连通测试",
    CONFIG_STATE_CALL_FAILED: "配置存在但本次调用失败",
    CONFIG_STATE_KEY_MISMATCH: "连通测试用的密钥与已保存密钥不一致",
}

# ==========================================================================
# 【BUG-B】工作区（本地文件夹）前置校验：存在性 + 读权限
#   统一提示样例：错误：当前工作绑定目录【xxx】不存在 / 无读取权限
# ==========================================================================
ERR_WORKSPACE_UNAVAILABLE = "WORKSPACE_UNAVAILABLE"
WORKSPACE_UNAVAILABLE_NOT_FOUND = "not_found"
WORKSPACE_UNAVAILABLE_NO_READ = "no_read_permission"
WORKSPACE_UNAVAILABLE_NO_WRITE = "no_write_permission"
WORKSPACE_UNAVAILABLE_NOT_DIR = "not_a_directory"


def workspace_unavailable_message(path: str, reason: str) -> str:
    """【BUG-B 4】统一、可直接展示到前端思考链的错误文案。"""
    text = {
        WORKSPACE_UNAVAILABLE_NOT_FOUND: "不存在",
        WORKSPACE_UNAVAILABLE_NOT_DIR: "不是文件夹",
        WORKSPACE_UNAVAILABLE_NO_READ: "无读取权限",
        WORKSPACE_UNAVAILABLE_NO_WRITE: "无写入权限",
    }.get(reason, "不可访问")
    return f"错误：当前工作绑定目录【{path or '（空）'}】{text}"


# ==========================================================================
# 【BUG-C】子任务失败 → 链路阻断 + 故障诊断
# ==========================================================================
ERR_SUBTASK_FAILED = "SUBTASK_FAILED"
ERR_SUBTASK_RETRY_EXHAUSTED = "SUBTASK_RETRY_EXHAUSTED"
# 【BUG-C 1】代码工程Agent 自身"代码错误重试达到上限"的错误码
ERR_CODE_RETRY_EXHAUSTED = "CODE_RETRY_EXHAUSTED"

# ==========================================================================
# 【增量修复 · 日志错误类型区分】两类失败必须分开记录，便于定位
#   ① IO 类错误：文件夹不存在 / 权限不足（后端原生操作即可判定，与模型无关）
#   ② 模型收敛失败：模型推理无法收敛，输出格式无法解析（模型侧问题）
# ==========================================================================
ERR_IO = "IO_ERROR"                                  # 通用 IO 错误
ERR_WORKSPACE_PATH_NOT_FOUND = "WORKSPACE_PATH_NOT_FOUND"      # 文件夹不存在
ERR_WORKSPACE_PERMISSION_DENIED = "WORKSPACE_PERMISSION_DENIED"  # 权限不足
ERR_WORKSPACE_NOT_A_DIR = "WORKSPACE_NOT_A_DIR"      # 目标不是目录
ERR_MODEL_CONVERGE_FAIL = "MODEL_CONVERGE_FAIL"      # 模型推理无法收敛 / 输出无法解析

# 【需求点 2③】模型回复无法解析为 JSON（JSONDecodeError）时的专用错误码：
#   该错误码只用于"JSON 抽取失败"这一确定性失败点，日志中必须带模型原始返回全文。
ERR_MODEL_JSON_INVALID = "MODEL_JSON_INVALID"

# 失败分类（写入思考链、agent_logs、error_logs，前端可直接区分）
FAILURE_KIND_IO = "io_error"
FAILURE_KIND_MODEL_CONVERGE = "model_converge_fail"
FAILURE_KIND_NONE = ""
FAILURE_KIND_LABEL: dict[str, str] = {
    FAILURE_KIND_IO: "IO 错误（目录 / 权限）",
    FAILURE_KIND_MODEL_CONVERGE: "模型收敛失败（输出无法解析）",
}

# IO 类错误码集合（供上层统一分类，避免散落判断）
IO_ERROR_CODES: tuple[str, ...] = (
    ERR_IO,
    ERR_WORKSPACE_PATH_NOT_FOUND,
    ERR_WORKSPACE_PERMISSION_DENIED,
    ERR_WORKSPACE_NOT_A_DIR,
    "FILE_NOT_FOUND",
    "FILE_TOO_LARGE",
    "PERMISSION_DENIED",
)


def classify_failure(error_code: str, *, stage: str = "") -> str:
    """把错误码归类为 IO 错误 / 模型收敛失败（唯一分类入口）。

    · IO 类：文件夹不存在、权限不足、目标不是目录等 —— 与模型无关；
    · 模型收敛类：MODEL_CONVERGE_FAIL / CODE_RETRY_EXHAUSTED / 输出非法 JSON。
    """
    code = str(error_code or "").strip().upper()
    if code in {c.upper() for c in IO_ERROR_CODES}:
        return FAILURE_KIND_IO
    if code in (ERR_MODEL_CONVERGE_FAIL, ERR_CODE_RETRY_EXHAUSTED,
                ERR_SUBTASK_RETRY_EXHAUSTED, "CODE_AGENT_JSON_INVALID",
                "MODEL_JSON_INVALID", "MODEL_OUTPUT_UNPARSEABLE"):
        return FAILURE_KIND_MODEL_CONVERGE
    if str(stage or "").lower() in ("model_converge", "converge_failed", "json_invalid"):
        return FAILURE_KIND_MODEL_CONVERGE
    return FAILURE_KIND_NONE
# 【BUG-C 2】前端思考链红色错误标识上展示的原文（不得改写）
SUBTASK_RETRY_EXHAUSTED_MESSAGE = "发生错误：代码任务重试达到上限，任务终止"
# 【BUG-C 6】不再基于空/失败结果生成汇总报告时的固定说明
FAULT_REPORT_NO_SYNTHESIS_NOTE = (
    "本次未调用评估校验Agent 与 交互交付Agent 生成汇总报告"
    "（按业务规则：子任务失败后禁止基于无效空结果生成虚假汇总）。"
)
# 思考步骤级别（前端据此渲染红色错误标识）
THINK_LEVEL_INFO = "info"
THINK_LEVEL_WARN = "warn"
THINK_LEVEL_ERROR = "error"

# ==========================================================================
# 【需求点 Bug1】任务依赖图 / 分支完整性 相关错误码与文案
# ==========================================================================
# 真实死循环（拓扑环）→ 硬终止
ERR_REAL_CYCLE = "TASK_DEPENDENCY_REAL_CYCLE"
# 疑似依赖异常（缺分支 / 结构可疑）→ 先重生成一次依赖图，仍未修复才终止
ERR_PLAN_GRAPH_INVALID = "TASK_DEPENDENCY_GRAPH_INVALID"

# 疑似依赖异常时重新调用调度规划Agent 重生成依赖图的硬上限（需求：重试 1 次）
MAX_PLAN_REGENERATE_RETRIES = 1

# 依赖图终止时的统一提示前缀（前端直接展示完整结构化报错）
DEPENDENCY_TERMINATED_TITLE = "任务被系统强制终止：任务依赖图检查未通过"

# 【需求点 Bug4】容错规则：Agent 允许在合理范围内推断补全，不因轻微不匹配直接罢工
FAULT_TOLERANCE_RULE = (
    "【容错规则 · 必须遵守】\n"
    "1. 用户输入与工程规范存在微小差异、口语化、省略信息时，**优先做合理补全与语义对齐**，"
    "不要直接拒绝任务；\n"
    "2. 仅当需求完全冲突、安全违规、关键信息缺失导致确实无法继续执行时，才返回拒绝；\n"
    "3. 遇到歧义，优先做出最合理的一种推断并继续推进，同时在 reasoning 中说明你的推断依据；\n"
    "4. 严禁因为细节不完备、命名不规范、风格不统一等非实质问题终止任务。"
)

# ==========================================================================
# 【需求点 Bug2 业务流程重构】队长（调度规划Agent）业务循环的裁决提示词
#   ------------------------------------------------------------------
#   业务伪代码里的"队长校验子任务结果是否满足预期目标"必须真实发生在链路里，
#   因此把它落成一次独立的队长模型裁决调用（输出受控 JSON，后端不信任其自由文本）。
# ==========================================================================
CAPTAIN_VERIFY_PROMPT = """你是「调度规划Agent」，在本系统中担任**队长**，是全局唯一裁决权威。

现在队员刚完成/刚失败一个子任务，你要**校验该子任务结果是否满足预期目标**，并给出后续动作。

【输出格式 · 必须是合法 JSON，禁止任何多余文字】
{
  "action": "pass" | "retry" | "terminate" | "reassign",
  "reason": "你的判定依据（写清楚为什么满足/不满足预期）",
  "replace_agent": "仅当 action=reassign 时填写要换派的队员角色",
  "instruction": "仅当 action=reassign 时填写换派后的完整子任务指令"
}

【action 取值规则】
- pass      ：子任务产出确实满足了该子任务的预期目标 → 队长继续规划下一个子任务；
- retry     ：产出不满足预期，但该子任务仍有重试额度 → 重新分派给同一个队员重试；
- reassign  ：产出不满足预期，且应改派**另一个**队员角色尝试（换思路/换能力）；
- terminate ：产出不满足预期且已无重试额度、换人也无意义 → 整个大任务终止。

【硬约束】
1. 只依据给出的队员回传结果判断，绝不编造未发生的执行结果。
2. 不得因为措辞、格式、命名等非实质问题判定不满足预期。
3. 换派时 replace_agent 必须是以下角色之一：
   代码工程Agent / 文档信息Agent / 视觉感知Agent / 记忆管理Agent / 评估校验Agent / 交互交付Agent。
"""

CAPTAIN_EXHAUSTED_PROMPT = """你是「调度规划Agent」（队长）。某个子任务已由同一队员重试到上限仍未达预期。

现在必须做**二选一决策**，只输出合法 JSON：
{
  "action": "terminate" | "reassign",
  "reason": "决策依据",
  "replace_agent": "action=reassign 时填写换派的队员角色",
  "instruction": "action=reassign 时填写换派后的完整子任务指令（自包含、可直接执行）"
}

- terminate：整个大任务标记 failed 并终止（失败不可挽回，或换人没有意义）；
- reassign ：由你重新规划，改派**另一个**队员 Agent 完成该子任务。
不得输出第三种取值，不得输出任何解释性文字。
"""

# ==========================================================================
# 【第三轮·业务流程重构】队长"需求对齐审批"提示词
#   上游：校验评估Agent 已判定产出质量合格 → 把本轮总结材料上交队长；
#   队长职责：判断本轮产出**是否符合用户原始需求**（不是判质量，质量已由校验评估Agent 判过）。
#   不符合 → 重新分配任务（最多 3 次）；3 次耗尽 → 整个大任务标记 failed 并终止。
# ==========================================================================
CAPTAIN_REQUIREMENT_PROMPT = """你是「调度规划Agent」，在本系统中担任**队长**。

【流程说明】刚才这一轮子任务已通过「校验评估Agent」的质量校验（代码正确性 / 逻辑 /
输出格式均已合格）。现在轮到你的职责：**需求对齐审批** —— 判断本轮产出是否真正
**符合用户的原始需求**，而不是再判一次质量。

【输出格式 · 必须是合法 JSON，禁止任何多余文字】
{
  "aligned": true | false,
  "reason": "你的判定依据（说清楚本轮产出与用户原始需求的对应关系）",
  "action": "proceed" | "reassign",
  "replace_agent": "仅当 action=reassign 时填写要改派的队员角色",
  "instruction": "仅当 action=reassign 时填写改派后的完整子任务指令（自包含、可直接执行）"
}

【判定规则】
- aligned=true / action=proceed  ：本轮产出确实覆盖了用户原始需求 → 继续往下执行
  （交付交互交付Agent 生成面向用户的输出说明，并交记忆管理Agent 持久化本轮上下文）；
- aligned=false / action=reassign：产出质量合格但与用户需求不对齐（做错了方向、漏了需求点、
  理解偏差）→ 由你重新分配任务，改派合适的队员 Agent 重做。

【硬约束】
1. 只依据给出的材料判断，绝不编造未发生的执行结果。
2. 不得因为措辞、排版、命名等非实质问题判定"不符合需求"。
3. 改派时 replace_agent 必须是以下角色之一：
   代码工程Agent / 文档信息Agent / 视觉感知Agent / 记忆管理Agent / 评估校验Agent / 交互交付Agent。
"""

# ==========================================================================
# 【第三轮·业务流程重构】校验评估Agent 的"队员产出校验"提示词
#   队员（代码工程Agent 等）执行完毕后**不再回传队长复核**，直接交校验评估Agent。
# ==========================================================================
EVALUATOR_SUBTASK_PROMPT = """你是「校验评估Agent」，负责对队员 Agent 的产出做**独立质量校验**。

【校验重点】
1. 代码正确性：语法/结构/逻辑是否成立，是否可直接运行，是否存在明显缺陷；
2. 逻辑一致性：结论与给出的材料是否自洽，是否存在前后矛盾；
3. 输出文本格式合规：是否符合该子任务要求的输出结构（例如必须是合法 JSON、
   必须给出清单、必须给出结论），是否缺项。

【输出格式 · 必须是合法 JSON，禁止任何多余文字】
{
  "verdict": "pass" | "reject",
  "score": 0-100,
  "issues": [
    {"dimension": "code|logic|format|dependency|hallucination|security",
     "severity": "low|mid|high",
     "detail": "问题描述（写清位置与现象）",
     "suggestion": "给原队员的**可执行**修改建议"}
  ],
  "reason": "结论依据"
}

【硬约束】
1. 只依据给出的产出内容判断，不得编造未提供的信息。
2. 措辞、命名风格等非实质问题不得判 reject。
3. 判定 reject 时，issues 必须给出**可直接照着改**的具体建议（原队员将据此修改）。
"""


