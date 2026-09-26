# -*- coding: utf-8 -*-
"""
调度规划Agent（Qwen3.8-Max）—— 全局唯一大脑

架构文档来源：第4章 4.1
  定位：全局唯一大脑、任务拆解、依赖分析、流程调度、异常决策
  核心能力：复杂需求拆解、任务依赖图生成、并行/串行任务编排、全局状态管控、失败重试决策、降级调度
  专属组件 Agent_Router：生成ID、校验消息、拦截循环依赖、超时监听、消息投递
  执行流程：用户输入→记忆检索→任务拆解→依赖校验→分发Agent→结果汇总→迭代判断→交付质检
  硬约束：不执行任何文件、命令、写操作，仅做调度决策
"""

from __future__ import annotations

from typing import Any

from backend.agents.base import (
    AGENT_CODE,
    AGENT_DELIVERY,
    AGENT_DOC,
    AGENT_EVALUATOR,
    AGENT_VISION,
    BaseAgent,
    CAPABILITY_DISPATCH,
    CAPABILITY_MEMORY_WRITE,
)
from backend.bus.message import Message
from backend.bus.state_machine import TaskState
from backend.services.model_client import ModelUnavailable
from backend.utils.constants import (
    AGENT_DISPATCH,
    AGENT_MEMORY,
    AGENT_TEMPERATURES,
    ERR_MODEL_UNAVAILABLE,
    FAULT_TOLERANCE_RULE,
    MAX_TASK_ITERATIONS,
    STATUS_FAILED,
    STATUS_SUCCESS,
)
DISPATCH_SYSTEM_PROMPT = """你是「调度规划Agent」，本地化多Agent协同开发工作台的全局唯一大脑。

======================================================================
【队长-队员层级架构 · 本角色的定位】
======================================================================
在本系统中，你担任**队长**，是全局总控与唯一裁决权威；其余 Agent 均为**队员**：
  👥 队员：代码工程Agent、文档信息Agent、视觉感知Agent、记忆管理Agent、
           评估校验Agent、交互交付Agent
队长（你）的职责与权力：
  1. 所有队员产生的输出**必须上报队长**（由运行时统一上报，无需你手动收集）；
  2. 队员之间出现信息冲突、数据不一致、结论矛盾（例如两份待删清单条目/数量不同）时，
     系统会自动触发**队长仲裁流程**，并调用你完成裁决；
  3. 你可以自主裁决采信版本、发起二次核验、裁决冲突结论（拒绝模棱两可）；
  4. 你裁决后的结果 = 整个任务链路**唯一基准**，强制所有队员沿用；
  5. 原有串行任务链路保持不变，仲裁只是**新增的冲突处理分支**；
  6. 你的仲裁思考过程会在 UI 任务面板与思维链中展示，所以裁决理由必须写清楚。
仲裁时的判定底线：**删除类操作不可逆，冲突时取更保守口径**；
正常源码/配置/依赖清单/文档等业务资产不得被列为待删除。

【硬约束 · 不可违反】
1. 你只做调度决策：任务拆解、依赖分析、任务编排、失败重试决策、降级调度。
2. 严禁执行任何文件读写、命令执行、代码落盘、外网下载动作。你没有这些能力。
3. 你必须把子任务分配给下列 6 个固定角色之一，不得发明新角色：
   - 代码工程Agent   ：代码生成/修改/调试/文件操作/命令执行（唯一执行端）
   - 文档信息Agent   ：超长文档解析、抽取、结构化摘要（禁止输出原始长文本）
   - 视觉感知Agent   ：仅在用户输入确实包含图片/截图时分配（无图片严禁分配）
   - 记忆管理Agent   ：记忆沉淀、归档、检索
   - 评估校验Agent   ：质量闸门，代码输出/文档摘要/任务依赖图强制校验
   - 交互交付Agent   ：仅做展示润色，不修改业务结果

【decision 取值规则 · 必须严格遵守，禁止过度保守】
- execute    ：**默认选项**。只要用户提出任何需要动手完成的工作
               （写代码 / 改文件 / 建工程 / 跑命令 / 解析文档 / 分析截图 / 生成方案…），
               一律用 execute 并给出完整 subtasks。
- respond    ：仅用于"不需要任何工具即可回答"的纯寒暄、纯概念问答、
               对已有结果的解释性追问。此时 subtasks 为空数组。
- terminate  ：**极少数情况**，仅当：①需求与系统能力完全冲突且无法变通；
               ②明确要求执行安全违规操作；③关键信息缺失到无法做出任何合理推断。
               ⚠️ 严禁因为下述原因使用 terminate：
                 · 细节不完整、命名不规范、风格不统一、表述口语化；
                 · 用户需求与理想工程规范存在轻微差异；
                 · 你不确定用户的具体偏好（此时应做出最合理推断并继续执行）。
               遇到不确定，选择 execute 并把你采用的合理推断写进 reasoning。

【输出格式 · 必须是合法 JSON，禁止任何多余文字】
{
  "decision": "execute" | "respond" | "terminate",
  "reasoning": "你的调度思考说明",
  "final_reply": "当 decision=respond 时给用户的正式回复（Markdown）",
  "subtasks": [
    {
      "title": "子任务名称",
      "agent_role": "上述六个角色之一",
      "instruction": "给该 Agent 的完整指令（自包含，可直接执行）",
      "depend_on": [0],
      "parallel_group": 0
    }
  ]
}

【任务拆解完整性 · 最高优先级，历史上最大的缺陷就出在这里】
1. **先列步骤、再写 subtasks**：先在 reasoning 中把用户目标拆成 N 个必要步骤，
   然后 subtasks 必须**逐步覆盖 reasoning 中列出的每一个步骤**。
   reasoning 里写到但 subtasks 里缺失的步骤 = 残缺依赖图，属于严重错误。
2. **禁止"只做第一步"**：任何"分析→设计→实现→校验→交付"型需求，
   必须把后续分支全部拆出来，不得只给出第一个子任务就结束。
3. **必须显式补齐收尾分支**（只要本次产生了实质产出）：
   · 评估校验Agent 的校验子任务（校验产出质量与安全）；
   · 交互交付Agent 的交付子任务（把结果整理成给用户的正式回复）。
4. **多分支任务必须完整覆盖全部分支**：例如"同时产出 A 与 B"，
   就要有 A 分支与 B 分支两个子任务；"先读取再改写再验证"要有三个子任务。
   分支之间可以是并列（同一 parallel_group、互相不 depend_on），
   也可以是串行（后一个 depend_on 前一个），但**一个都不能少**。
5. **每个子任务必须自包含**：instruction 要写明输入、要做的事、期望产出与验收标准，
   不允许出现"同上""如前所述""TBD""待补充"等占位表达。
6. **依赖图必须是可拓扑排序的 DAG**：
   · depend_on 使用 subtasks 数组下标，表示"这些前置子任务全部完成后我才能开始"；
   · **禁止自引用**（不允许 depend_on 包含自身下标）；
   · **禁止形成环**（A→B→A 这种互相等待会让任务死锁被系统强制终止）；
   · 并列的可选路径请用**互不 depend_on 的并列分支**表达，不要用互相依赖来表达"二选一"；
   · 依赖图必须至少有一个**无依赖的起始子任务**（根节点），
     否则所有子任务互相等待，系统会判定为结构性错误。
7. 单次最多拆解 8 个子任务：若超过 8 步，请合并同角色、同阶段的相邻步骤，
   但**合并的是步骤粒度，不是丢掉分支**。
8. subtasks 必须能收敛：禁止规划"无限循环改进"这类无法结束的流程。

""" + FAULT_TOLERANCE_RULE


# 【需求点 Bug1】依赖图重生成（repair）追加提示词：
#   检测到疑似依赖异常时，把具体问题回灌给调度规划Agent 重新出图（最多 1 次）。
DISPATCH_REPAIR_PROMPT = """【本次为依赖图重生成任务 · 必须严格遵守】

上一版任务依赖图未通过结构校验，系统已驳回。请**重新输出一份完整可用的任务依赖图**。

【上一版被驳回的具体原因】
{issues}

【重生成硬性要求】
1. 仍然使用同一套 JSON 输出格式（decision / reasoning / subtasks）。
2. 必须在 reasoning 中重新完整列出该需求的全部分支步骤，再逐条落到 subtasks。
3. 逐一消除上面的驳回原因：
   · 缺少后续分支 → 把缺失的步骤补成独立子任务；
   · 自引用 / 循环依赖 → 取消造成环的那条 depend_on，改为并列分支或串行前置；
   · 无根节点 → 至少保留一个 depend_on 为空数组的起始子任务。
4. 不得因为"图有问题"就改成 decision=terminate ——
   除非该需求确实安全违规或完全无法完成；否则一律 decision=execute。
5. 子任务数量保持在 1~8 个，且必须覆盖：实现分支 + 校验分支 + 交付分支（如适用）。
"""


class DispatchAgent(BaseAgent):
    agent_role = AGENT_DISPATCH
    model_name = "Qwen3.8-Max"   # 【需求点 BUG-NEW2】固定绑定 Qwen3.8-Max（qwen3.8-max）

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = DISPATCH_SYSTEM_PROMPT

    # ------------------------------------------------------------------
    # 主流程（第4章 4.1 执行流程）
    # ------------------------------------------------------------------
    def _build_plan_messages(
        self,
        *,
        user_input: str,
        image_resources: list[str],
        memory_hits: list[dict],
        history: list[dict],
        revision_feedback: str = "",
    ) -> list[dict]:
        """构造「任务拆解」请求（首次拆解 / 依赖图重生成 共用同一构造逻辑）。"""
        memory_block = ""
        if memory_hits:
            memory_block = "【长期记忆检索结果（可能相关，需自行判断）】\n" + "\n".join(
                f"- {h.get('text', '')[:300]}" for h in memory_hits[:5]
            )

        image_note = ""
        if image_resources:
            image_note = (
                f"【本次输入包含 {len(image_resources)} 张图片资源】"
                "视觉解析必须分配给「视觉感知Agent」，其余角色不得解析图片。"
            )

        history_block = ""
        if history:
            history_block = "【本会话近期对话】\n" + "\n".join(
                f"{h.get('role')}: {str(h.get('text', ''))[:200]}" for h in history[-6:]
            )

        # 【需求点 Bug1】依赖图重生成：把驳回原因作为独立段落回灌（不覆盖用户需求）
        repair_block = ""
        if revision_feedback:
            repair_block = DISPATCH_REPAIR_PROMPT.format(issues=revision_feedback.strip()[:2000])

        user_content = "\n\n".join(x for x in [
            f"【用户本次输入】\n{user_input}",
            image_note,
            memory_block,
            history_block,
            repair_block,
        ] if x)

        return [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content},
        ]

    async def plan(
        self,
        task: TaskState,
        user_input: str,
        *,
        image_resources: list[str],
        memory_hits: list[dict],
        history: list[dict],
        revision_feedback: str = "",
    ) -> dict:
        self.require_capability(CAPABILITY_DISPATCH, detail="调度规划Agent 的唯一职责")

        task.bump_iteration()          # 第2章 2.1 规则1：迭代计数
        self.ctx.runtime.router.check_timeout(task)   # 第2章 2.1 规则2：超时监听

        if revision_feedback:
            self.think(task, "think",
                       f"依赖图结构校验未通过，重新生成任务依赖图（第 {task.iteration}/{MAX_TASK_ITERATIONS} 轮）")
        else:
            self.think(task, "think",
                       f"解析用户需求并规划调度路径（第 {task.iteration}/{MAX_TASK_ITERATIONS} 轮）")

        messages = self._build_plan_messages(
            user_input=user_input, image_resources=image_resources,
            memory_hits=memory_hits, history=history,
            revision_feedback=revision_feedback,
        )

        try:
            response = await self.call_model(
                task, messages,
                # 【需求点 Bug1 / Bug4】顶层主控温度 0.3（黄金平衡点）：
                #   保留强指令跟随的同时具备容错能力，缓解"过度保守直接停止工作"。
                temperature=AGENT_TEMPERATURES[AGENT_DISPATCH], max_tokens=4000,
                expect_json=True,
                step_label=("调用 Qwen3.8-Max 重新生成任务依赖图" if revision_feedback
                            else "调用 Qwen3.8-Max 生成任务依赖图"),
            )
        except ModelUnavailable as exc:
            # 第2章 2.3 规则1/2：降级提示，不崩溃系统
            # 【需求点 三、3】生态位补位耗尽时保留精确错误码，供上层标记任务失败并给用户可读提示
            self.think(task, "think", f"调度模型不可用：{exc}")
            return {
                "decision": "terminate",
                "reasoning": "调度模型不可用",
                "final_reply": str(exc),
                "subtasks": [],
                "degraded": True,
                "error_code": getattr(exc, "code", ERR_MODEL_UNAVAILABLE),
            }

        plan = self._validate_plan(task, response.text, user_input=user_input)
        plan["plan_revision"] = bool(revision_feedback)
        if revision_feedback:
            plan["revision_feedback"] = revision_feedback[:2000]
        if response.degraded:
            plan["degraded_note"] = response.degrade_note or "当前为备选调度模型"
            self.think(task, "think", "当前为备选调度模型（GLM 临时接管调度）")
        return plan

    async def regenerate_plan(
        self,
        task: TaskState,
        user_input: str,
        *,
        image_resources: list[str],
        memory_hits: list[dict],
        history: list[dict],
        issues: list[str],
    ) -> dict:
        """【需求点 Bug1 规则4】依赖图疑似异常 → 带着具体问题重新拆解一次。

        重用 plan() 主链路，因此迭代计数 / 超时监听 / 降级兜底全部照旧生效。
        """
        feedback = "\n".join(f"- {i}" for i in (issues or []) if i) or "- 依赖图结构可疑"
        self.ctx.logger.task_log(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event="plan.regenerate", level="warn",
            detail=f"依赖图结构校验未通过，重生成任务依赖图（第 1 次重试）：{feedback[:500]}",
        )
        return await self.plan(
            task, user_input,
            image_resources=image_resources, memory_hits=memory_hits, history=history,
            revision_feedback=feedback,
        )

    # ------------------------------------------------------------------
    # 依赖图校验（第4章 4.1 依赖校验 + 第2章 2.1 规则3 循环依赖拦截）
    # ------------------------------------------------------------------
    def _validate_plan(self, task: TaskState, raw_text: str, *, user_input: str = "") -> dict:
        try:
            data = self.parse_json(raw_text)
        except ValueError:
            self.ctx.logger.exception_log(
                error_code="DISPATCH_JSON_INVALID",
                message=f"调度模型输出非法 JSON，回退为直接应答：{raw_text[:200]}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return {
                "decision": "respond",
                "reasoning": "调度输出解析失败，降级为直接应答",
                "final_reply": raw_text.strip()[:4000] or "调度输出为空。",
                "subtasks": [],
            }

        if not isinstance(data, dict):
            data = {"decision": "respond", "final_reply": str(data), "subtasks": []}

        decision = str(data.get("decision") or "execute").lower()
        if decision not in ("execute", "respond", "terminate"):
            decision = "execute"

        valid_roles = {AGENT_CODE, AGENT_DOC, AGENT_VISION, AGENT_MEMORY, AGENT_EVALUATOR, AGENT_DELIVERY}
        subtasks: list[dict] = []
        for idx, item in enumerate(list(data.get("subtasks") or [])[:8]):
            if not isinstance(item, dict):
                continue
            role = str(item.get("agent_role") or "").strip()
            if role not in valid_roles:
                self.ctx.logger.exception_log(
                    error_code="DISPATCH_UNKNOWN_ROLE",
                    message=f"调度输出分配了未登记角色 {role!r}，该项被丢弃",
                    session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                )
                continue
            try:
                deps = [int(d) for d in (item.get("depend_on") or []) if str(d).isdigit()]
            except (TypeError, ValueError):
                deps = []
            deps = [d for d in deps if 0 <= d < len(data.get("subtasks") or []) and d != idx]
            subtasks.append({
                "index": idx,
                "title": str(item.get("title") or f"子任务{idx + 1}")[:200],
                "agent_role": role,
                "instruction": str(item.get("instruction") or item.get("title") or ""),
                "depend_on": sorted(set(deps)),
                "parallel_group": int(item.get("parallel_group") or 0),
            })

        # 无图片输入时，视觉感知Agent 的分配会被运行时剔除（第4章 4.4 触发条件）
        # 【需求点 Bug1】decision=execute 必须有可执行子任务：
        #   模型偶尔会宣称 execute 却给不出子任务，此时不得静默转成"直接应答"
        #   （那会让用户的业务目标完全没有被执行），改为按用户输入补一条兜底执行子任务，
        #   交由代码工程Agent（唯一执行端）实际推进，保证业务任务能够完整走下去。
        if decision == "execute" and not subtasks:
            self.ctx.logger.exception_log(
                error_code="DISPATCH_SUBTASKS_MISSING",
                message="调度输出 decision=execute 但 subtasks 为空，已按兜底策略补一条执行子任务",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            subtasks = [{
                "index": 0,
                "title": "执行用户需求",
                "agent_role": AGENT_CODE,
                "instruction": (
                    "按用户原始需求完成实际工作：先理解目标并说明将要采取的具体步骤，"
                    "然后在当前工作区目录内创建/修改所需文件并给出可验证的结果。\n"
                    f"用户原始需求：{(user_input or data.get('reasoning') or '')[:800]}"
                ),
                "depend_on": [],
                "parallel_group": 0,
            }]

        return {
            "decision": decision if subtasks or decision != "execute" else "respond",
            "reasoning": str(data.get("reasoning") or "")[:2000],
            "final_reply": str(data.get("final_reply") or ""),
            "subtasks": subtasks,
            "dependencies": _build_dep_map(subtasks),
        }

    # ------------------------------------------------------------------
    # 消息入口（由 MessageBus 投递）
    # ------------------------------------------------------------------
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        self.require_capability(CAPABILITY_DISPATCH)
        plan = await self.plan(
            task, msg.content_text(),
            image_resources=msg.image_resources,
            memory_hits=[],
            history=[],
        )
        return self.result_message(task, plan, metadata={"kind": "plan"})

    # ------------------------------------------------------------------
    # 结果汇总 + 交付质检（第4章 4.1 执行流程末段）
    # ------------------------------------------------------------------
    async def synthesize(
        self, task: TaskState, user_input: str, results: list[dict], *,
        degraded_notes: list[str] | None = None, receipt_block: str = "",
    ) -> str:
        """汇总全部队员材料生成正式回复。

        【需求点 Bug1】receipt_block = 后端真实执行回执（磁盘删除结果）。
        它是报告里"删除结果"的唯一数据源：模型只能引用、不得改写或补充。
        """
        self.require_capability(CAPABILITY_DISPATCH)
        task.bump_iteration()
        self.think(task, "think", "汇总各 Agent 结果并生成正式回复")
        digest = "\n\n".join(
            f"【{r.get('agent_role')} · {r.get('title')}】\n{str(r.get('output'))[:2500]}"
            for r in results if r.get("output")
        ) or "（本次没有任何子任务产出内容）"

        note_block = ""
        if degraded_notes:
            note_block = "\n\n【系统降级提示，必须在回复中如实告知用户】\n" + "\n".join(f"- {n}" for n in degraded_notes)

        receipt_hint = ""
        if receipt_block:
            receipt_hint = (
                "\n\n【后端真实执行回执 · 报告中删除结果的唯一数据源】\n"
                + receipt_block[:6000]
                + "\n\n【引用回执的硬性要求】\n"
                "1. 「删除了哪些文件、成功几项、失败几项」必须**逐条照抄**上面的回执，"
                "不得新增、不得遗漏、不得改成「全部删除成功」这类笼统说法；\n"
                "2. 回执里标记为失败的条目，必须如实写成失败并给出真实原因；\n"
                "3. 严禁输出回执之外的任何删除结果描述（不得凭推测补全）。"
            )

        messages = [
            {"role": "system", "content": (
                "你是「调度规划Agent」，负责把多 Agent 的执行结果汇总成给用户的正式回复。\n"
                "要求：\n"
                "1. 只汇总事实，不得编造未执行的操作、不得虚构文件路径或运行结果。\n"
                "2. 使用 Markdown：## 小标题、- 列表、**加粗** 关键结论。\n"
                "3. 如果某项能力因模型失效被禁用，必须如实说明「该能力当前不可用」。\n"
                "4. 直接输出回复正文，不要输出 JSON，不要输出你的思考过程。\n"
                "5. 【职责边界】队员只做分析，磁盘修改/删除全部由后端程序执行；"
                "报告中的执行结果必须以「后端真实执行回执」为准。"
            )},
            {"role": "user", "content": f"【用户原始需求】\n{user_input}\n\n【子任务执行结果】\n{digest}{note_block}{receipt_hint}"},
        ]
        try:
            response = await self.call_model(
                # 【需求点 Bug4】汇总润色阶段沿用主控温度 0.3（容错但不放飞）
                task, messages, temperature=AGENT_TEMPERATURES[AGENT_DISPATCH], max_tokens=4000,
                step_label="汇总结果并生成交付内容",
            )
            return response.text.strip()
        except ModelUnavailable as exc:
            self.ctx.logger.exception_log(
                error_code=ERR_MODEL_UNAVAILABLE, message=f"结果汇总失败：{exc}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            return digest

    # ------------------------------------------------------------------
    # 失败重试决策（【需求点 Bug2】同一队员子任务最多重试 3 次）
    #   重试耗尽的最终二选一由运行时队长循环（runtime._captain_exhausted_decision）落定：
    #   ① 整个大任务标记 failed 终止；② 队长重新规划，换其他队员 Agent 尝试。
    # ------------------------------------------------------------------
    def decide_retry(self, task: TaskState, error: str) -> bool:
        self.require_capability(CAPABILITY_DISPATCH)
        if not task.can_retry():
            self.think(task, "think", f"子任务已重试 {task.retry_count} 次并失败，按硬规则不再重试：{error[:120]}")
            self.ctx.logger.task_log(
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                event="retry.exhausted", detail=error[:500], level="warn",
            )
            return False
        task.bump_retry()
        self.think(task, "think",
                   f"失败重试决策：第 {task.retry_count}/{MAX_SUBTASK_RETRIES} 次重试。原因：{error[:120]}")
        return True

    # ------------------------------------------------------------------
    def allow_memory_capability(self) -> bool:
        """调度Agent 允许做记忆检索（第4章 4.1 执行流程：用户输入→记忆检索）。"""
        return CAPABILITY_MEMORY_WRITE in CAPABILITY_MATRIX[self.agent_role]

    def mark_success(self, task: TaskState, reply: str) -> None:
        task.mark_success(result=reply[:2000])

    def mark_failed(self, task: TaskState, code: str, message: str) -> None:
        task.mark_failed(code, message)


def _build_dep_map(subtasks: list[dict]) -> dict[str, list[int]]:
    return {str(s["index"]): list(s["depend_on"]) for s in subtasks}


__all__ = ["DispatchAgent", "DISPATCH_SYSTEM_PROMPT", "STATUS_SUCCESS"]
