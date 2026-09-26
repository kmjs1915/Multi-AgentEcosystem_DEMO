# -*- coding: utf-8 -*-
"""
评估校验Agent（GLM 5.3）【质量闸门】

架构文档来源：第4章 4.6
  定位：全局唯一质检、裁判、安全审核中心
  强制校验范围：代码输出、文档摘要、任务依赖图
  轻量化校验范围：最终用户对话输出（仅安全扫描）
  核心能力：JSON格式校验、逻辑错误校验、循环依赖拦截、幻觉校验、代码规范校验、安全风险校验
  打回规则：最多打回重写 2 次，两次失败直接上报用户，禁止无限死循环

本 Agent 是全局唯一裁判；它不产出业务内容，只做校验结论。
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from typing import Any

from backend.agents.base import AGENT_EVALUATOR, CAPABILITY_EVALUATE, BaseAgent
from backend.bus.message import Message
from backend.bus.state_machine import TaskState
from backend.services.model_client import ModelUnavailable
from backend.utils.constants import (
    AGENT_EVALUATOR,
    AGENT_TEMPERATURES,
    MAX_PLAN_REGENERATE_RETRIES,
    MAX_REVIEW_REJECTS,
)

EVALUATOR_SYSTEM_PROMPT = """你是「评估校验Agent」（GLM 5.3），本地化多Agent协同开发工作台的全局唯一质量闸门、裁判与安全审核中心。

【职责 · 严格限定】
1. 你只做校验与裁判，绝对不重写业务内容、不产出代码、不修改结论。
2. 你必须独立判断，不得因为"上游说它正确"就放行。

【校验维度】
- format      ：JSON 结构/字段完整性、Markdown 结构
- logic       ：逻辑自洽性、前后矛盾、结论与证据不符
- dependency  ：任务依赖图是否存在环（循环依赖）、是否存在无根节点
- hallucination：是否编造了不存在的文件/接口/运行结果
- code        ：语法可编译、规范、是否含占位符（TODO/省略号/伪代码）
- security    ：是否含越界路径、危险命令、密钥泄露、可执行文件上传意图

【输出格式 · 必须是合法 JSON】
{
  "verdict": "pass" | "reject",
  "score": 0-100,
  "issues": [
    {"dimension": "format/logic/dependency/hallucination/code/security",
     "severity": "high/mid/low", "detail": "问题描述", "suggestion": "修改建议（只描述，不代写）"}
  ],
  "safety_scan": {"passed": true, "findings": []},
  "reason": "裁判结论一句话"
}

规则：
- 存在 high 级别问题必须 verdict=reject。
- 没有问题时 verdict=pass 且 issues 为空数组。

【dependency 维度的严重级别判定 · 必须严格遵守】
- high  **仅限真实死循环**：
    · 子任务 depend_on 包含自身下标（自引用）；
    · 依赖关系构成闭合环（A→B→A）。
  这类问题会让任务永远无法开工，必须判 high。
- mid   以下都属于「可疑但可继续」的形态，**一律判 mid，禁止判 high**：
    · 任务依赖图无根节点；
    · 依赖下标越界；
    · reasoning 中说明了多个步骤，但 subtasks 只覆盖其中一部分（缺少后续分支处理）；
    · 子任务粒度过粗或过细、并列分支划分不理想。
  ⚠️ 特别注意：**「缺少后续分支处理」不是循环依赖，绝对不能判为 high**。
  它只是"拆解不够完整"，系统会要求调度规划Agent 重新拆解一次，而不是终止任务。
- low   风格性建议（命名、顺序、并行分组优化等）。
"""

# 幻觉检测：声称存在但实际不存在的文件
FILE_CLAIM_RE = re.compile(r"[`\"']([\w\-./\\]+\.(?:py|js|ts|tsx|jsx|java|go|rs|c|cpp|h|md|json|yaml|yml|txt|docx|pdf))[`\"']")
CODE_PLACEHOLDER_MARKERS = (
    "TODO", "FIXME", "此处省略", "...省略", "略", "your_code_here", "pass  # 待实现",
    "<placeholder>", "XXX",
)
DANGEROUS_SNIPPETS = (
    "rm -rf /", "format c:", "shutdown /s", "del /f /s /q", "os.remove(\"/",
    "subprocess.call(\"rm", "eval(input(", "exec(input(",
)
SECRET_PATTERNS = (
    re.compile(r"sk-[A-Za-z0-9]{16,}"),
    re.compile(r"(?i)api[_-]?key\s*[:=]\s*[\"'][A-Za-z0-9\-_]{16,}[\"']"),
    re.compile(r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----"),
)


@dataclass
class Verdict:
    verdict: str                       # pass / reject
    score: int = 100
    issues: list[dict] = field(default_factory=list)
    reason: str = ""
    safety: dict = field(default_factory=dict)
    reject_count: int = 0
    escalated: bool = False            # 打回 2 次仍失败 → 上报用户
    checks: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.verdict == "pass"

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict, "score": self.score, "issues": self.issues,
            "reason": self.reason, "safety_scan": self.safety,
            "reject_count": self.reject_count, "escalated": self.escalated,
            "checks": self.checks,
        }


class EvaluatorAgent(BaseAgent):
    agent_role = AGENT_EVALUATOR
    model_name = "GLM 5.3"   # 【需求点 Bug8】固定绑定 GLM 5.3

    def __init__(self, ctx):
        super().__init__(ctx)
        self.system_prompt = EVALUATOR_SYSTEM_PROMPT

    # ==================================================================
    # 强制校验：代码输出 / 文档摘要 / 任务依赖图（第4章 4.6）
    # ==================================================================
    async def verify_code(self, task: TaskState, code_text: str, *, artifacts: list[str] | None = None) -> Verdict:
        return await self._verify(task, kind="code", content=code_text, artifacts=artifacts or [])

    async def verify_doc_summary(self, task: TaskState, summary_text: str) -> Verdict:
        return await self._verify(task, kind="doc_summary", content=summary_text)

    async def verify_dependency_graph(self, task: TaskState, plan: dict) -> Verdict:
        """任务依赖图强制校验（本地结构校验 + 模型逻辑校验）。

        【需求点 Bug1 规则3】必须严格区分两类情况：
          · 真实死循环：自引用 / 拓扑环（closed cycle）→ 任务永远无法开工，high 级拦截；
          · 合法多分支可选路径：多个分支汇入、星型扇出、并列可选路径、缺根节点等
            → **属于可疑但可继续的形态**，只记 low/mid 级，绝不当成循环依赖硬终止。
        历史缺陷：只要模型吐回任何 high 级问题（例如"reasoning 说明了 xxx 但 subtasks 仅包含
        其中一步，缺少后续分支处理"）都会被运行时当作循环依赖直接终止任务。
        """
        subtasks = plan.get("subtasks") or []
        local_issues = self._check_dependency_graph_local(subtasks)
        if any(i["severity"] == "high" for i in local_issues):
            verdict = Verdict(
                verdict="reject", score=30, issues=local_issues,
                reason="任务依赖图存在真实死循环（自引用/拓扑环），已拦截",
                checks=["dependency:local"],
            )
            self._log_verdict(task, verdict, "dependency_graph")
            return verdict

        result = await self._verify(
            task, kind="dependency_graph",
            content=json.dumps(plan, ensure_ascii=False, indent=2)[:12000],
            extra_issues=local_issues, skip_json_local=True,
        )
        # 【需求点 Bug1 规则3】模型裁判声称的"循环依赖"必须经本地拓扑复核才可信：
        #   复核不成立的（典型：模型把"缺少后续分支"误报成循环依赖）降级为 mid，
        #   避免运行时据此终止任务。
        has_cycle, _ = self.has_real_cycle(subtasks)
        if not has_cycle and not result.passed:
            downgraded: list[dict] = []
            for issue in result.issues:
                text = f"{issue.get('dimension','')} {issue.get('detail','')}"
                if issue.get("severity") == "high" and any(
                        kw in text for kw in ("循环", "依赖环", "自引用", "死循环", "环形", "cycle")):
                    issue = {
                        **issue, "severity": "mid",
                        "detail": (
                            "疑似依赖形态问题（本地拓扑复核未发现真实环，"
                            f"按多分支可选路径处理）：{issue.get('detail','')}"
                        ),
                    }
                elif issue.get("severity") == "high":
                    # 其余 high 级问题不属于"死循环"范畴，不构成硬终止依据 → mid
                    issue = {**issue, "severity": "mid"}
                downgraded.append(issue)
            result.issues = downgraded
            result.verdict = "pass"
            result.score = max(result.score, 70)
            result.reason = ("本地拓扑复核未发现真实循环依赖（模型裁判结论已降级为提示）："
                             + (result.reason or ""))[:300]
            result.checks = list(result.checks) + ["dependency:cycle_recheck"]
            self._log_verdict(task, result, "dependency_graph")
        elif not has_cycle:
            # 本地拓扑复核已执行（结论：不存在真实环）→ 留下复核痕迹，便于排查与展示
            result.checks = list(result.checks) + ["dependency:cycle_recheck"]
        return result

    # ==================================================================
    # 【需求点 Bug1 规则3】真实死循环判定（唯一硬终止依据）
    #   真实死循环 = ① 子任务依赖自身；② 依赖关系构成闭合拓扑环（closed cycle）。
    #   以下**都不算**死循环，绝不可据此终止任务：
    #     · 多个分支汇聚到同一节点（多对一收敛，属于正常编排）；
    #     · 星型扇出（一个节点被多个后继依赖）；
    #     · 并列的可选路径（互不 depend_on 的并列分支）；
    #     · 缺根节点 / 依赖越界 / 前置被跳过（可降级继续，只记提示）。
    # ==================================================================
    @staticmethod
    def analyze_dependency_graph(subtasks: list[dict]) -> dict:
        """返回依赖图分析结果：{cycle, self_refs, out_of_range, has_root, dependencies}。"""
        n = len(subtasks)
        deps: dict[int, list[int]] = {}
        for s in subtasks:
            idx = int(s.get("index", -1))
            deps[idx] = [d for d in (s.get("depend_on") or []) if isinstance(d, int)]

        self_refs: list[int] = []
        out_of_range: list[tuple[int, int]] = []
        for idx, ds in deps.items():
            for d in ds:
                if d == idx and idx not in self_refs:
                    self_refs.append(idx)
                if d < 0 or d >= n:
                    out_of_range.append((idx, d))

        # 邻接表：忽略自引用与越界下标（它们由上面两类单独报告）
        adj: dict[int, list[int]] = {
            i: sorted({d for d in deps.get(i, []) if d != i and 0 <= d < n})
            for i in range(n)
        }

        WHITE, GRAY, BLACK = 0, 1, 2
        color = {i: WHITE for i in range(n)}
        path: list[int] = []
        cycle: list[int] = []

        def dfs(node: int) -> bool:
            color[node] = GRAY
            path.append(node)
            for nxt in adj.get(node, []):
                if color.get(nxt) == GRAY:
                    start = path.index(nxt) if nxt in path else 0
                    cycle.extend(path[start:] + [nxt])
                    return True
                if color.get(nxt) == WHITE and dfs(nxt):
                    return True
            path.pop()
            color[node] = BLACK
            return False

        for i in range(n):
            if color[i] == WHITE and dfs(i):
                break

        return {
            "node_count": n,
            "cycle": cycle,                       # 非空 = 真实拓扑环
            "self_refs": self_refs,
            "out_of_range": out_of_range,
            "has_root": any(not deps.get(i) for i in range(n)),
            "dependencies": {str(k): v for k, v in deps.items()},
        }

    @classmethod
    def has_real_cycle(cls, subtasks: list[dict]) -> tuple[bool, list[int]]:
        """是否存在真实死循环（自引用或拓扑环）。"""
        info = cls.analyze_dependency_graph(subtasks)
        if info["self_refs"]:
            return True, [info["self_refs"][0]]
        return bool(info["cycle"]), info["cycle"]

    @classmethod
    def graph_anomaly_report(cls, subtasks: list[dict], *, verdict: Verdict | None = None) -> dict:
        """汇总「疑似依赖异常」的可读原因，供运行时决定是否重生成依赖图并给用户报错。

        structure_issue=True  → 建议重新调用调度规划Agent 重生成依赖图（需求 Bug1 规则4）
        hard_terminate=True   → 真实死循环，重生成后仍存在才允许终止任务
        """
        info = cls.analyze_dependency_graph(subtasks)
        reasons: list[str] = []
        if info["self_refs"]:
            reasons.append(f"子任务 {info['self_refs'][0]} 依赖自身，构成真实死循环（自引用）")
        if info["cycle"]:
            reasons.append("检测到真实循环依赖（闭合拓扑环）："
                           + " -> ".join(str(c) for c in info["cycle"]))
        if not info["has_root"]:
            reasons.append("任务依赖图无根节点（所有子任务互相依赖，无法确定起始步骤）")
        if info["out_of_range"]:
            pairs = "、".join(f"{a}->{b}" for a, b in info["out_of_range"][:5])
            reasons.append(f"存在越界依赖下标（{pairs}），已按忽略处理")
        if verdict is not None and not verdict.passed:
            for issue in verdict.issues:
                if issue.get("severity") in ("high", "mid"):
                    reasons.append(f"[评估校验Agent] {issue.get('detail', '')[:200]}")
        return {
            "info": info,
            "reasons": reasons[:8],
            "hard_terminate": bool(info["self_refs"] or info["cycle"]),
            "structure_issue": bool(reasons),
        }

    # ==================================================================
    # 轻量化校验：最终用户对话输出（仅安全扫描）（第4章 4.6）
    # ==================================================================
    def safety_scan(self, text: str) -> dict:
        findings: list[dict] = []
        for marker in DANGEROUS_SNIPPETS:
            if marker.lower() in (text or "").lower():
                findings.append({"type": "dangerous_code", "detail": f"命中危险片段：{marker}", "severity": "high"})
        for pattern in SECRET_PATTERNS:
            m = pattern.search(text or "")
            if m:
                findings.append({"type": "secret_leak", "detail": "疑似泄露密钥/私钥", "severity": "high"})
        for pattern in (r"\b[A-Za-z]:\\(?:Windows|Program Files|System32)", r"(?<!\w)/(?:etc|usr|bin)/"):
            if re.search(pattern, text or ""):
                findings.append({"type": "out_of_scope_path", "detail": "出现系统关键目录路径", "severity": "high"})
        return {"passed": not findings, "findings": findings}

    async def lightweight_verify(self, task: TaskState, final_text: str) -> Verdict:
        """轻量化：只做安全扫描，不调用模型（节省 Token，符合 4.6 轻量化范围）。"""
        scan = self.safety_scan(final_text)
        verdict = Verdict(
            verdict="pass" if scan["passed"] else "reject",
            score=100 if scan["passed"] else 20,
            issues=[{"dimension": "security", "severity": f["severity"],
                     "detail": f["detail"], "suggestion": "移除该内容后重新输出"}
                    for f in scan["findings"]],
            reason="轻量化安全扫描通过" if scan["passed"] else "轻量化安全扫描发现高危内容",
            safety=scan,
            checks=["security:local"],
        )
        self._log_verdict(task, verdict, "final_output_lightweight")
        return verdict

    # ==================================================================
    # 打回计数与升级（最多打回 2 次，两次失败直接上报用户）
    # ==================================================================
    def register_reject(self, task: TaskState) -> Verdict:
        if not task.can_reject():
            v = Verdict(
                verdict="reject", score=0,
                issues=[{"dimension": "process", "severity": "high",
                         "detail": f"已打回 {task.review_rejects} 次（上限 {MAX_REVIEW_REJECTS} 次）",
                         "suggestion": "停止重写，直接上报用户"}],
                reason="打回次数已达硬上限，禁止无限死循环，直接上报用户",
                reject_count=task.review_rejects, escalated=True,
                checks=["process:reject_limit"],
            )
            self.ctx.logger.task_log(
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
                event="review.escalated_to_user",
                detail=f"打回 {task.review_rejects} 次仍不通过，已升级上报用户", level="warn",
            )
            return v
        task.bump_reject()
        return Verdict(verdict="reject", reject_count=task.review_rejects,
                       reason=f"第 {task.review_rejects}/{MAX_REVIEW_REJECTS} 次打回重写")

    # ==================================================================
    # 内部实现
    # ==================================================================
    async def _verify(self, task: TaskState, *, kind: str, content: str,
                      extra_issues: list[dict] | None = None,
                      artifacts: list[str] | None = None,
                      skip_json_local: bool = False) -> Verdict:
        self.require_capability(CAPABILITY_EVALUATE)
        task.bump_iteration()
        self.ctx.runtime.router.check_timeout(task)

        checks: list[str] = []
        issues: list[dict] = list(extra_issues or [])

        # ---- 本地硬校验（不消耗 Token，且不可被模型"放行"） ----
        scan = self.safety_scan(content)
        checks.append("security:local")
        if not scan["passed"]:
            for f in scan["findings"]:
                issues.append({"dimension": "security", "severity": "high",
                               "detail": f["detail"], "suggestion": "移除高危内容"})

        if kind == "code":
            issues.extend(self._check_code_local(content))
            checks.append("code:syntax")
            issues.extend(self._check_hallucination_local(task, content, artifacts or []))
            checks.append("hallucination:files")
        elif kind == "doc_summary":
            issues.extend(self._check_doc_summary_local(content))
            checks.append("format:doc_summary")
        elif kind == "dependency_graph":
            if not skip_json_local:
                issues.extend(self._check_json_local(content))
            checks.append("format:json")

        if any(i["severity"] == "high" for i in issues):
            verdict = Verdict(verdict="reject", score=25, issues=issues,
                              reason="本地硬校验发现 high 级别问题", safety=scan, checks=checks)
            self._log_verdict(task, verdict, kind)
            return verdict

        # ---- 模型逻辑校验（GLM 5.3 裁判） ----
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": (
                f"【校验对象类型】{kind}\n"
                f"【已知本地校验结果】已通过（{', '.join(checks)}）\n"
                f"【待校验内容】\n{content[:12000]}"
            )},
        ]
        try:
            response = await self.call_model(
                # 【需求点 Bug4】质量闸门温度保持 0.0：裁判必须零随机，安全边界不放宽
                task, messages, temperature=AGENT_TEMPERATURES[AGENT_EVALUATOR],
                max_tokens=2000, expect_json=True,
                step_label="GLM 5.3 执行质量闸门校验",
            )
            data = self.parse_json(response.text)
        except ModelUnavailable as exc:
            # 第2章 2.3 规则2：校验模型失效 → 该能力禁用，退回本地硬校验结论（不放行不阻断）
            self.ctx.logger.exception_log(
                error_code="EVALUATOR_MODEL_UNAVAILABLE",
                message=f"评估校验模型不可用，退回本地硬校验：{exc}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            verdict = Verdict(
                verdict="pass", score=70, issues=issues,
                reason=f"评估校验模型不可用，仅通过本地硬校验（{', '.join(checks)}）",
                safety=scan, checks=checks + ["model:unavailable"],
            )
            self._log_verdict(task, verdict, kind)
            return verdict
        except ValueError as exc:
            self.ctx.logger.exception_log(
                error_code="EVALUATOR_JSON_INVALID", message=f"校验模型输出非法 JSON：{exc}",
                session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            )
            data = {}

        if isinstance(data, dict):
            model_issues = data.get("issues") or []
            for mi in model_issues[:20]:
                if isinstance(mi, dict) and mi.get("severity") in ("high", "mid", "low"):
                    issues.append({
                        "dimension": str(mi.get("dimension") or "logic"),
                        "severity": str(mi["severity"]),
                        "detail": str(mi.get("detail") or "")[:600],
                        "suggestion": str(mi.get("suggestion") or "")[:600],
                    })
            verdict_str = str(data.get("verdict") or "").lower()
            score = int(data.get("score") or 80) if str(data.get("score") or "80").isdigit() else 80
            reason = str(data.get("reason") or "")
            model_scan = data.get("safety_scan") or {}
        else:
            verdict_str, score, reason, model_scan = "", 80, "", {}

        if not scan["passed"] or any(i["severity"] == "high" for i in issues):
            verdict_str = "reject"
        if verdict_str not in ("pass", "reject"):
            verdict_str = "pass" if not issues else "reject"

        merged_scan = {
            "passed": bool(scan["passed"] and (model_scan.get("passed", True) if isinstance(model_scan, dict) else True)),
            "findings": list(scan["findings"]) + list((model_scan.get("findings") or []) if isinstance(model_scan, dict) else []),
        }
        verdict = Verdict(
            verdict=verdict_str, score=max(0, min(100, score)), issues=issues,
            reason=reason or ("校验通过" if verdict_str == "pass" else "校验不通过"),
            safety=merged_scan, checks=checks + ["model:logic"],
        )
        self._log_verdict(task, verdict, kind)
        return verdict

    # ---------------- 本地校验器 ----------------
    @staticmethod
    def _check_json_local(content: str) -> list[dict]:
        issues: list[dict] = []
        try:
            json.loads(content)
        except json.JSONDecodeError as exc:
            issues.append({"dimension": "format", "severity": "high",
                           "detail": f"JSON 格式非法：{exc}", "suggestion": "修正为合法 JSON"})
        return issues

    @staticmethod
    def _check_code_local(content: str) -> list[dict]:
        issues: list[dict] = []
        blocks = re.findall(r"```(?:python|py)\s*(.+?)```", content, flags=re.S)
        snippets = blocks or []
        for i, snippet in enumerate(snippets[:10]):
            try:
                ast.parse(snippet)
            except SyntaxError as exc:
                issues.append({
                    "dimension": "code", "severity": "high",
                    "detail": f"第 {i + 1} 段 Python 代码语法错误：{exc.msg}（第 {exc.lineno} 行）",
                    "suggestion": "修复语法错误后重新提交",
                })
        for marker in CODE_PLACEHOLDER_MARKERS:
            if marker in content and marker not in ("略",):
                issues.append({
                    "dimension": "code", "severity": "mid",
                    "detail": f"代码包含占位符/未实现标记：{marker}",
                    "suggestion": "补齐完整实现，不要保留占位符",
                })
                break
        if content.count("```") % 2 != 0:
            issues.append({"dimension": "format", "severity": "mid",
                           "detail": "Markdown 代码块未闭合", "suggestion": "补齐代码块结束标记"})
        return issues

    @staticmethod
    def _check_doc_summary_local(content: str) -> list[dict]:
        issues: list[dict] = []
        if len(content) > 20000:
            issues.append({"dimension": "format", "severity": "high",
                           "detail": f"文档摘要长度 {len(content)} 字符，疑似输出原始长文本（第4章 4.3 约束）",
                           "suggestion": "压缩为结构化摘要，只保留少量引用片段"})
        # 引用片段超长检测
        for m in re.finditer(r"^-\s*\[[^\]]+\]\s*(.+)$", content, flags=re.M):
            if len(m.group(1)) > 400:
                issues.append({"dimension": "format", "severity": "mid",
                               "detail": "存在超过 400 字的引用片段，疑似搬运原文",
                               "suggestion": "引用片段裁剪到 200 字以内"})
                break
        return issues

    @staticmethod
    def _check_dependency_graph_local(subtasks: list[dict]) -> list[dict]:
        """任务依赖图结构性校验。

        【需求点 Bug1 规则3】只有**真实死循环**才判为 high（唯一硬终止依据）：
          · 子任务依赖自身（自引用）；
          · 依赖关系构成闭合拓扑环。
        其余形态（缺根节点 / 依赖越界 / 分支不完整）一律判为 low/mid —— 提示可继续，
        由调度规划Agent 重生成依赖图或运行时降级推进，绝不因此终止用户的业务任务。
        """
        issues: list[dict] = []
        if not subtasks:
            return issues

        info = EvaluatorAgent.analyze_dependency_graph(subtasks)
        n = info["node_count"]

        for idx in info["self_refs"]:
            issues.append({
                "dimension": "dependency", "severity": "high",
                "detail": f"子任务 {idx} 依赖自身，构成真实死循环（自引用）",
                "suggestion": "移除该子任务的 depend_on 中的自身下标",
            })

        if info["cycle"]:
            issues.append({
                "dimension": "dependency", "severity": "high",
                "detail": "检测到真实循环依赖（闭合拓扑环）："
                          + " -> ".join(str(c) for c in info["cycle"]),
                "suggestion": "打破环：取消其中一条 depend_on，把它改为并列分支或串行前置",
            })

        for idx, bad in info["out_of_range"][:5]:
            issues.append({
                "dimension": "dependency", "severity": "low",
                "detail": f"子任务 {idx} 依赖越界下标 {bad}（有效范围 0~{max(n - 1, 0)}），已按忽略处理",
                "suggestion": "修正依赖下标或直接删除该条依赖",
            })

        if not info["has_root"]:
            # 缺根节点属于可修复的形态问题（不是环），只记 mid 并给出修复建议
            issues.append({
                "dimension": "dependency", "severity": "mid",
                "detail": "任务依赖图无根节点：所有子任务互相等待，无法确定起始步骤",
                "suggestion": "至少保留一个 depend_on 为空数组的起始子任务",
            })
        return issues

    def _check_hallucination_local(self, task: TaskState, content: str, artifacts: list[str]) -> list[dict]:
        """幻觉校验：声称产出的文件必须在会话工作区真实存在。"""
        issues: list[dict] = []
        try:
            existing = {r["rel"].replace("\\", "/").lower() for r in self.ctx.file_guard.list_dir(".")}
            existing |= {a.replace("\\", "/").lower() for a in artifacts}
        except Exception:  # noqa: BLE001
            return issues

        claimed = set()
        for m in FILE_CLAIM_RE.finditer(content):
            name = m.group(1).replace("\\", "/").lstrip("./").lower()
            claimed.add(name)
        existing_bases = {e.split("/")[-1] for e in existing}
        for name in list(claimed)[:30]:
            base = name.split("/")[-1]
            if name not in existing and base not in existing_bases:
                issues.append({
                    "dimension": "hallucination", "severity": "mid",
                    "detail": f"输出中提到的文件在工作区不存在：{name}",
                    "suggestion": "只描述真实存在的文件，或先实际写入该文件",
                })
        return issues

    def _log_verdict(self, task: TaskState, verdict: Verdict, kind: str) -> None:
        self.ctx.db.log_agent(
            session_id=task.session_id, task_id=task.task_id, agent_role=self.agent_role,
            event=f"review.{kind}.{verdict.verdict}",
            detail=f"score={verdict.score} issues={len(verdict.issues)} reason={verdict.reason}",
            level="warn" if verdict.verdict == "reject" else "info",
        )
        self.think(task, "think", f"质检结论：{verdict.verdict}（{verdict.score} 分）— {verdict.reason}")

    # ==================================================================
    async def handle(self, msg: Message, task: TaskState) -> Message | None:
        self.require_capability(CAPABILITY_EVALUATE)
        kind = str(msg.metadata.get("kind") or "")
        content = msg.content_text()
        if kind == "code":
            verdict = await self.verify_code(task, content, artifacts=list(msg.metadata.get("artifacts") or []))
        elif kind == "doc_summary":
            verdict = await self.verify_doc_summary(task, content)
        elif kind == "dependency_graph":
            payload = msg.content if isinstance(msg.content, dict) else {}
            verdict = await self.verify_dependency_graph(task, payload)
        else:
            verdict = await self.lightweight_verify(task, content)
        return self.result_message(task, verdict.to_dict(), metadata={"kind": "verdict", "verdict": verdict.verdict})


__all__ = ["EvaluatorAgent", "Verdict", "EVALUATOR_SYSTEM_PROMPT", "MAX_REVIEW_REJECTS"]
