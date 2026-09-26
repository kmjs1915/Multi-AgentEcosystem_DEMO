# -*- coding: utf-8 -*-
"""
文件安全校验与受限文件操作（基础设施层）

架构文档来源：
  - 第2章 2.2 裸机安全核心规则 规则1/2：会话目录隔离 + 操作白名单
  - 第4章 4.2 代码工程Agent 裸机安全逻辑 规则1/4：仅限当前工作目录、禁止跨目录读写
  - 第6章 6.2 上传文件自动隔离至当前会话目录

【需求点 二、2 工作区安全权限硬约束】
  所有 7 个 Agent 的文件读写/创建/修改/删除，全部限定在「当前选中工作区文件夹」内部。
  `SessionFileGuard` 是该硬约束在运行时唯一的执行点：
    · 写入根目录 = session.workspace（= 当前工作区文件夹）
    · 越界一律抛 SecurityViolation(WORKSPACE_ACCESS_DENIED) 并写入安全日志
    · 切换工作区后重新绑定 guard，写根目录随之更新
"""

from __future__ import annotations

import hashlib
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from backend.utils.constants import (
    ERR_WORKSPACE_ACCESS_DENIED,
    ERR_WORKSPACE_PERMISSION_DENIED,
    ERR_WORKSPACE_UNAVAILABLE,
    WORKSPACE_ACCESS_DENIED_MESSAGE,
    WORKSPACE_MARKER_FILE,
    WORKSPACE_UNAVAILABLE_NOT_DIR,
    WORKSPACE_UNAVAILABLE_NOT_FOUND,
    WORKSPACE_UNAVAILABLE_NO_READ,
    workspace_unavailable_message,
)
from backend.utils.paths import (
    SecurityViolation,
    SessionPaths,
    WorkspaceUnavailable,
    is_within,
    normalize_path as _normalize_path,
    probe_workspace_root,
    resolve_in_root,
    safe_relative_to,
)
from backend.utils.security import (
    assert_within_size,
    check_upload,
    ensure_parent,
    scan_content_for_danger,
)


def _safe_rel(path: str | Path, root: str | Path) -> str:
    """工作区内的相对路径（跨盘符 / 跨目录树时优雅回退为绝对路径，绝不抛异常）。"""
    return safe_relative_to(path, root, fallback=str(path)).replace("\\", "/")


@dataclass
class FileOpResult:
    ok: bool
    path: str
    detail: str
    safety: str = "in-workspace"
    bytes_written: int = 0
    sha256: str = ""


class SessionFileGuard:
    """工作区内受限文件操作器（第4章 4.2 裸机核心执行端）。"""

    def __init__(self, session: SessionPaths):
        self.session = session
        # 最近一次越权拦截事件（供上层写入安全日志）
        self.last_denial: dict | None = None
        session.ensure()

    # ---------------- 路径 ----------------
    @property
    def workspace_root(self) -> Path:
        """当前文件操作根目录 = 当前选中工作区文件夹。"""
        return self.session.workspace

    # ==================================================================
    # 【BUG-B 1/2/3】工作区目录前置校验（Agent 执行文件类任务前的唯一闸门）
    #   · 校验真实物理目录是否存在、进程是否拥有读权限（需要落盘时再校验写权限）；
    #   · 校验失败 → 抛 WorkspaceUnavailable（统一文案：
    #     错误：当前工作绑定目录【xxx】不存在 / 无读取权限），
    #     由上层终止对应子任务并写入思考链路，绝不下发空路径给 Agent；
    #   · 校验对象是**绑定的真实绝对路径**（session.workspace），不是内存虚拟路径。
    # ==================================================================
    def probe_workspace(self, *, need_write: bool = False) -> dict:
        """探测当前工作区目录可用性（不抛异常，供展示 / 接口复用）。"""
        probe = probe_workspace_root(self.workspace_root, need_write=need_write)
        probe["is_local_folder"] = self.session.is_local_folder
        probe["folder_id"] = self.session.workspace_folder_id
        probe["folder_name"] = self.session.workspace_folder_name
        return probe

    def assert_workspace_usable(self, *, need_write: bool = False) -> None:
        """【BUG-B 2/3】不可用即抛 WorkspaceUnavailable，并落安全日志。"""
        probe = probe_workspace_root(self.workspace_root, need_write=need_write)
        if probe["ok"]:
            return
        self.last_denial = {
            "code": ERR_WORKSPACE_UNAVAILABLE,
            "message": probe["message"],
            "path": probe["path"],
            "workspace_root": str(self.workspace_root),
            "detail": {"reason": probe["reason"]},
            "at": time.time(),
        }
        raise WorkspaceUnavailable(probe["path"], probe["reason"], detail={
            "is_local_folder": self.session.is_local_folder,
            "folder_id": self.session.workspace_folder_id,
        })

    def resolve(self, relative_path: str) -> Path:
        """解析并强制校验目标必须位于当前工作区文件夹内（越权即拒绝）。

        【BUG-B 2】解析前先做目录前置校验：工作区目录不存在 / 无读取权限时
        立即抛出可读错误，不再把空路径或虚假路径下发给 Agent。

        【需求点 二、2 规则2】越权时返回统一文案「越权访问禁止：只能操作当前工作目录内文件」，
        并记录 last_denial 事件，由上层写入安全日志。
        """
        # 【BUG-B 2/3】目录前置校验（存在性 + 读权限）
        self.assert_workspace_usable()
        try:
            return resolve_in_root(self.workspace_root, relative_path)
        except SecurityViolation as exc:
            if exc.code == ERR_WORKSPACE_ACCESS_DENIED:
                self.last_denial = {
                    "code": exc.code,
                    "message": str(exc),
                    "path": str(relative_path),
                    "workspace_root": str(self.workspace_root),
                    "detail": exc.detail,
                    "at": time.time(),
                }
            raise

    # ---------------- 读 ----------------
    def read_text(self, relative_path: str, *, max_bytes: int = 2 * 1024 * 1024) -> FileOpResult:
        target = self.resolve(relative_path)
        if not target.exists():
            return FileOpResult(False, str(relative_path), "文件不存在")
        if target.is_dir():
            return FileOpResult(False, str(relative_path), "目标是目录，无法读取文本")
        size = target.stat().st_size
        if size > max_bytes:
            return FileOpResult(False, str(relative_path), f"文件过大（{size} 字节），超出读取上限 {max_bytes}")
        data = target.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")
        return FileOpResult(True, str(relative_path), text)

    def read_bytes(self, relative_path: str, *, max_bytes: int = 50 * 1024 * 1024) -> bytes:
        target = self.resolve(relative_path)
        if not target.exists() or target.is_dir():
            raise SecurityViolation(f"文件不存在：{relative_path}", code="FILE_NOT_FOUND")
        if target.stat().st_size > max_bytes:
            raise SecurityViolation(f"文件超出读取上限：{relative_path}", code="FILE_TOO_LARGE")
        return target.read_bytes()

    def list_dir(self, relative_path: str = ".") -> list[dict]:
        """列出单层目录内容（后端原生执行，不经模型）。

        【增量修复 1】目录列举改用 **os.listdir**（需求指定），并把
        IO 类失败（不存在 / 无读取权限 / 不是目录）明确抛成 WorkspaceUnavailable，
        便于上层按 IO 错误类型记录，而不是静默返回空清单。
        """
        # 【BUG-B 2/3】目录列举前先校验工作区目录（存在性 + 读权限）
        self.assert_workspace_usable()
        target = self.workspace_root if relative_path in (".", "") else self.resolve(relative_path)
        if not is_within(target, self.workspace_root):
            raise SecurityViolation(
                WORKSPACE_ACCESS_DENIED_MESSAGE, code=ERR_WORKSPACE_ACCESS_DENIED,
                detail={"path": relative_path},
            )
        try:
            exists = target.exists()
        except OSError as exc:
            raise WorkspaceUnavailable(str(target), WORKSPACE_UNAVAILABLE_NOT_FOUND,
                                       detail={"reason": f"{type(exc).__name__}: {exc}"}) from exc
        if not exists:
            raise WorkspaceUnavailable(str(target), WORKSPACE_UNAVAILABLE_NOT_FOUND,
                                       detail={"relative_path": relative_path})
        if not target.is_dir():
            raise WorkspaceUnavailable(str(target), WORKSPACE_UNAVAILABLE_NOT_DIR,
                                       detail={"relative_path": relative_path})
        try:
            # ★ 需求指定：使用 os.listdir 完成"列出文件夹内文件名"
            names = sorted(os.listdir(str(target)), key=lambda n: n.lower())
        except PermissionError as exc:
            raise WorkspaceUnavailable(str(target), WORKSPACE_UNAVAILABLE_NO_READ,
                                       detail={"relative_path": relative_path,
                                               "errno": getattr(exc, "errno", None)}) from exc
        except OSError as exc:
            raise WorkspaceUnavailable(str(target), WORKSPACE_UNAVAILABLE_NOT_FOUND,
                                       detail={"relative_path": relative_path,
                                               "reason": f"{type(exc).__name__}: {exc}"}) from exc

        out: list[dict] = []
        for name in names:
            child = target / name
            # 【需求点 一、1-b】工作区标记文件属于系统元文件，不作为工作区内容暴露给 Agent
            if self.is_system_meta_file(child):
                continue
            stat = None
            try:
                stat = child.stat()
            except OSError:
                stat = None
            if stat is None:
                continue
            try:
                is_dir = child.is_dir()
            except OSError:
                is_dir = False
            # 【需求点 一、2】subpath 计算统一走 safe_relative_to：
            #   跨盘符 / 跨目录树时优雅回退为绝对路径，绝不抛 ValueError 打断目录列举
            if child == self.workspace_root:
                rel = "."
            else:
                rel = safe_relative_to(child, self.workspace_root, fallback=str(child))
            out.append({
                "name": name,
                "type": "dir" if is_dir else "file",
                "size": stat.st_size if not is_dir else 0,
                "mtime": stat.st_mtime,
                "rel": rel.replace("\\", "/"),
                "ext": (Path(name).suffix.lower() if not is_dir else ""),
            })
        return out

    def is_system_meta_file(self, path: str | Path) -> bool:
        """【需求点 一、1-b】判断是否为系统元文件（跳过工作区子路径校验）。

        当前工作区模式下，出现在工作区目录内的系统文件只有工作区标记文件
        `.mae_workspace.json`；会话私有目录（upload / artifact）不在业务工作区内，
        由调用方另行按会话作用域处理。
        """
        try:
            name = Path(str(path)).name
        except (TypeError, ValueError):
            return False
        return name == WORKSPACE_MARKER_FILE

    # ==================================================================
    # 【增量修复 1】原生目录遍历：列出文件夹内文件名由后端代码完成（os.listdir）
    #   需求：`列出文件夹内文件名` 这类基础目录读取动作**不交给 LLM** 去遍历/列举，
    #   由后端原生执行 → 把确定性工作从模型推理中剥离，避免模型反复思考无法收敛。
    #   仅"判断哪些文件属于调试/测试文件"这一语义判断才交给模型。
    # ==================================================================
    def scan_workspace(self, relative_path: str = ".", *, recursive: bool = True,
                       max_entries: int = 2000, max_depth: int = 6) -> dict:
        """后端原生遍历工作区目录，返回结构化清单（不经过任何模型）。

        实现要点：
          · 目录列举使用 **os.listdir**（需求指定），不用 pathlib 的隐式遍历；
          · 每个条目独立 stat，单个条目失败只跳过该条，不中断整次扫描；
          · 工作区标记文件等系统元文件不计入业务清单；
          · IO 类失败（目录不存在 / 权限不足 / 不是目录）→ **抛 WorkspaceUnavailable**，
            由上层按 IO 错误类型记录并写入思考链，绝不把空路径下发给 Agent。

        返回：
          {"ok", "root", "scan_path", "recursive", "files": [name...],
           "entries": [{name, rel, type, size, mtime, ext}], "counts": {...},
           "truncated": bool, "duration_ms": int}
        """
        started = time.time()
        self.assert_workspace_usable()          # 存在性 + 读权限前置校验
        root = self.workspace_root
        base = root if relative_path in (".", "") else self.resolve(relative_path)

        if not base.exists():
            raise WorkspaceUnavailable(str(base), WORKSPACE_UNAVAILABLE_NOT_FOUND,
                                       detail={"relative_path": relative_path})
        if not base.is_dir():
            raise WorkspaceUnavailable(str(base), WORKSPACE_UNAVAILABLE_NOT_DIR,
                                       detail={"relative_path": relative_path})

        entries: list[dict] = []
        dirs_scanned = 0
        truncated = False
        skipped: list[dict] = []
        pending: list[tuple[Path, int]] = [(base, 0)]
        seen_dirs: set[str] = set()

        while pending:
            current, depth = pending.pop(0)
            try:
                key = _normalize_path(current)
            except OSError:
                continue
            if key in seen_dirs:                # 防御符号链接造成的目录环
                continue
            seen_dirs.add(key)
            dirs_scanned += 1
            try:
                # ★ 需求指定：使用 os.listdir 完成"列出文件夹内文件名"
                names = sorted(os.listdir(str(current)), key=lambda n: n.lower())
            except PermissionError as exc:
                skipped.append({"dir": _safe_rel(current, root), "reason": "无读取权限"})
                self.last_denial = {
                    "code": ERR_WORKSPACE_PERMISSION_DENIED,
                    "message": workspace_unavailable_message(
                        str(current), WORKSPACE_UNAVAILABLE_NO_READ),
                    "path": str(current), "workspace_root": str(root),
                    "detail": {"errno": getattr(exc, "errno", None)}, "at": time.time(),
                }
                continue
            except OSError as exc:
                skipped.append({"dir": _safe_rel(current, root),
                                "reason": f"{type(exc).__name__}: {exc}"})
                continue

            for name in names:
                if len(entries) >= max_entries:
                    truncated = True
                    break
                child = current / name
                # 系统元文件（工作区标记等）不计入业务清单
                if self.is_system_meta_file(child):
                    continue
                try:
                    is_dir = child.is_dir()
                except OSError:
                    is_dir = False
                stat = None
                try:
                    stat = child.stat()
                except OSError:
                    stat = None
                rel = _safe_rel(child, root)
                entries.append({
                    "name": name,
                    "rel": rel,
                    "type": "dir" if is_dir else "file",
                    "size": (stat.st_size if (stat and not is_dir) else 0),
                    "mtime": (stat.st_mtime if stat else 0.0),
                    "ext": (Path(name).suffix.lower() if not is_dir else ""),
                    "depth": depth,
                })
                if is_dir and recursive and (depth + 1) < max_depth:
                    pending.append((child, depth + 1))
            if truncated:
                break

        files = [e["rel"] for e in entries if e["type"] == "file"]
        dirs = [e["rel"] for e in entries if e["type"] == "dir"]
        return {
            "ok": True,
            "root": str(root),
            "scan_path": _safe_rel(base, root),
            "recursive": bool(recursive),
            "files": files,
            "dirs": dirs,
            "entries": entries,
            "counts": {"files": len(files), "dirs": len(dirs), "total": len(entries),
                       "dirs_scanned": dirs_scanned},
            "skipped": skipped,
            "truncated": truncated,
            "max_entries": max_entries,
            "duration_ms": int((time.time() - started) * 1000),
        }

    @staticmethod
    def format_scan_payload(scan: dict, *, limit: int = 300) -> str:
        """把原生扫描结果格式化为交给模型的**文件名清单**（纯数据，无任何指令）。"""
        files = list(scan.get("files") or [])[:limit]
        dirs = list(scan.get("dirs") or [])[:limit]
        lines = [
            f"扫描目录：{scan.get('scan_path') or '.'}"
            f"｜递归：{'是' if scan.get('recursive') else '否'}"
            f"｜文件 {scan.get('counts', {}).get('files', 0)} 个"
            f"｜目录 {scan.get('counts', {}).get('dirs', 0)} 个",
        ]
        if dirs:
            lines.append("【目录】" + "、".join(dirs))
        if files:
            lines.append("【文件】")
            lines.extend(f"{i + 1}. {p}" for i, p in enumerate(files))
        else:
            lines.append("【文件】（该目录下没有文件）")
        if scan.get("truncated"):
            lines.append(f"（清单已截断至 {scan.get('max_entries')} 条）")
        for row in (scan.get("skipped") or [])[:10]:
            lines.append(f"（跳过：{row.get('dir')} —— {row.get('reason')}）")
        return "\n".join(lines)

    # ==================================================================
    # 【需求点 Bug4】清单展示：文件名原样输出，不再拼接 `.` / `/` 前缀
    # ==================================================================
    @staticmethod
    def format_listing_rows(rows: list[dict], limit: int = 200) -> str:
        """把目录清单格式化为「类型标注 + 原始文件名」文本。

        Bug4 根因：历史实现写作 `{'.' if file else '/'}{rel}`，实际输出形如
        `.note.txt`、`/.git`。模型（与用户）会把前导 `.` 当成文件名的一部分，
        于是生成 `.note.txt` 这种错误路径，导致"识别文件名多了一个点"。
        现改为显式中文类型标注，文件名一律取自文件系统返回的原始名称：
            文件：note.txt (12B)
            目录：src
        """
        lines: list[str] = []
        for row in rows[:limit]:
            name = str(row.get("rel") or row.get("name") or "").replace("\\", "/")
            if row.get("type") == "dir":
                lines.append(f"目录：{name}")
            else:
                lines.append(f"文件：{name} ({row.get('size', 0)}B)")
        return "\n".join(lines)

    def format_listing(self, relative_path: str = ".", *, limit: int = 200) -> str:
        """列出目录并格式化为清单文本（空目录返回占位说明）。

        【BUG-B 4】工作区目录不可用时不再掩盖成「（目录不可访问）」，
        而是原样返回可读错误文案（例如「错误：当前工作绑定目录【xxx】不存在 / 无读取权限」），
        由调用方写入思考链路，保证故障点对用户可见。
        """
        try:
            rows = self.list_dir(relative_path)
        except WorkspaceUnavailable as exc:
            return f"（{exc}）"
        except SecurityViolation:
            return "（目录不可访问）"
        return self.format_listing_rows(rows, limit=limit) or "（工作区为空）"

    # ==================================================================
    # 【需求点 Bug5】写盘后真实读回校验：磁盘必须真的存在该文件
    # ==================================================================
    def verify_written(self, relative_path: str, *, expect_sha256: str = "") -> dict:
        """校验写入结果是否真实落盘。

        返回 {"exists": bool, "size": int, "sha256": str, "detail": str}。
        只有在磁盘上确实存在该文件、且（可选）哈希一致时 exists 才为 True，
        从而杜绝"界面显示已创建、磁盘却没有文件"的假成功。
        """
        try:
            target = self.resolve(relative_path)
        except SecurityViolation as exc:
            return {"exists": False, "size": 0, "sha256": "", "detail": f"路径校验失败：{exc}"}
        if not target.is_file():
            return {"exists": False, "size": 0, "sha256": "",
                    "detail": f"磁盘上不存在该文件：{target}"}
        try:
            data = target.read_bytes()
        except OSError as exc:
            return {"exists": False, "size": 0, "sha256": "", "detail": f"文件读取失败：{exc}"}
        digest = hashlib.sha256(data).hexdigest()
        if expect_sha256 and digest != expect_sha256:
            return {"exists": False, "size": len(data), "sha256": digest,
                    "detail": f"内容哈希不一致（期望 {expect_sha256[:16]}，实际 {digest[:16]}）"}
        return {"exists": True, "size": len(data), "sha256": digest,
                "detail": f"磁盘校验通过：{target}（{len(data)} 字节）"}

    def stat(self) -> dict:
        """工作区根目录概览（供前端显示当前工作目录与文件数）。"""
        root = self.workspace_root
        files = dirs = 0
        # 【BUG-B 1】实时反映绑定的真实物理目录状态（存在性 / 读权限）
        probe = self.probe_workspace()
        try:
            for child in root.iterdir():
                # 【需求点 一、1-b】系统元文件不计入工作区内容统计
                if self.is_system_meta_file(child):
                    continue
                if child.is_dir():
                    dirs += 1
                else:
                    files += 1
        except OSError:
            pass
        return {
            "root": str(root),
            "exists": root.is_dir(),
            # 【BUG-B 2/4】可用性 + 不可用原因（前端可据此展示可读错误）
            "available": probe["ok"],
            "unavailable_reason": probe["reason"],
            "unavailable_message": probe["message"],
            "readable": probe["readable"],
            "writable": probe["writable"],
            "is_local_folder": self.session.is_local_folder,
            "folder_id": self.session.workspace_folder_id,
            "folder_name": self.session.workspace_folder_name,
            "top_level_files": files,
            "top_level_dirs": dirs,
        }

    # ---------------- 写 ----------------
    def write_text(self, relative_path: str, content: str, *, append: bool = False) -> FileOpResult:
        """写入文件（越权校验 + 安全扫描 + 大小限制）。"""
        # 【BUG-B 2/3】落盘前先校验工作区目录：不存在 / 无读权限 → 直接报错；
        # 目录存在但无写权限 → 同样以可读错误终止，绝不静默失败。
        self.assert_workspace_usable(need_write=True)
        target = self.resolve(relative_path)
        dangerous = scan_content_for_danger(content)
        if dangerous:
            return FileOpResult(False, str(relative_path), dangerous, safety="blocked")
        assert_within_size(content)

        ensure_parent(target)
        data = content.encode("utf-8")
        if append and target.exists():
            with target.open("ab") as fh:
                fh.write(data)
        else:
            tmp = target.with_name(target.name + ".mae.tmp")
            tmp.write_bytes(data)
            tmp.replace(target)
        return FileOpResult(
            True, str(relative_path), "写入成功", bytes_written=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )

    # ---------------- 删除（必须已获审批） ----------------
    def delete(self, relative_path: str, *, approved: bool) -> FileOpResult:
        """删除文件/目录。第4章 4.2 规则2/3：删除属高危，必须审批通过后才可执行。"""
        if not approved:
            raise SecurityViolation(
                f"删除操作未经审批，已拒绝执行：{relative_path}", code="APPROVAL_REQUIRED",
            )
        target = self.resolve(relative_path)
        # 双重保险：批准后仍需确认目标在工作区内，且不允许删除工作区根目录本身
        if not is_within(target, self.workspace_root) or target == self.workspace_root:
            raise SecurityViolation(
                WORKSPACE_ACCESS_DENIED_MESSAGE, code=ERR_WORKSPACE_ACCESS_DENIED,
                detail={"path": relative_path, "reason": "不允许删除工作区根目录或越界目标"},
            )
        if not target.exists():
            return FileOpResult(False, str(relative_path), "目标不存在")
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        return FileOpResult(True, str(relative_path), "删除完成")

    # ---------------- 上传隔离（第6章 6.2 规则4） ----------------
    def save_upload(self, filename: str, data: bytes) -> dict:
        """校验后保存到当前会话 upload 目录（会话私有区，避免污染用户工作区）。"""
        result = check_upload(filename, len(data), content_head=data[:16])
        if not result.ok:
            raise SecurityViolation(result.reason, code=result.code,
                                    detail={"filename": filename, "size": len(data)})

        self.session.upload.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = f"{stamp}_{result.safe_name}"
        target = self.session.upload / safe_name
        target.write_bytes(data)

        return {
            "name": result.safe_name,
            "stored_name": safe_name,
            "path": str(target),
            # 【需求点 一、2】subpath 计算走 safe_relative_to，跨盘符时回退绝对路径
            "relative_path": safe_relative_to(target, self.session.upload, fallback=str(target)),            "size": len(data),
            "ext": result.ext,
            "resource_type": result.resource_type,
            "sha256": hashlib.sha256(data).hexdigest(),
            "isolated_to": str(self.session.upload),
            "workspace_root": str(self.workspace_root),
        }

    def artifact_path(self, name: str) -> Path:
        safe = Path(str(name)).name
        target = (self.session.artifact / safe).resolve()
        if not is_within(target, self.session.artifact):
            raise SecurityViolation("artifact 路径越界", code=WORKSPACE_ACCESS_DENIED)
        return target
