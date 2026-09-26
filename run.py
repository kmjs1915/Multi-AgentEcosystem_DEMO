# -*- coding: utf-8 -*-
"""
启动入口（裸机直接运行）

架构文档来源：
  - 第6章 6.3 局域网访问安全：服务监听 0.0.0.0:5090 支持局域网访问，禁止公网暴露
  - 第5章 裸机目录规范：启动时自动初始化 %USERPROFILE%\\.multi_agent_ecosystem\\ 全部子目录

用法：
    python run.py                          # 默认 0.0.0.0:5090
    python run.py --port 5091              # 换端口
    python run.py --host 127.0.0.1         # 仅本机（不对外暴露）
    MAE_ROOT=D:\\mae python run.py         # 自定义数据根目录（默认走系统规范路径）
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Windows 控制台默认 GBK，强制 UTF-8 输出，避免中文/符号 banner 抛 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

from backend.services.runtime import EcosystemRuntime  # noqa: E402
from backend.utils.constants import DEFAULT_HOST, DEFAULT_PORT  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="本地化多Agent协同开发工作台（裸机版）—— 无 Docker、目录隔离 + 白名单 + 人工审批 + 后端二次校验",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"监听地址（默认 {DEFAULT_HOST}，仅本机可用 127.0.0.1）")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"监听端口（默认 {DEFAULT_PORT}）")
    parser.add_argument("--reload", action="store_true", help="开发模式自动重载")
    parser.add_argument("--show-root", action="store_true", help="仅打印数据根目录后退出")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.show_root:
        print(EcosystemRuntime().paths.root)
        return 0

    import uvicorn

    from backend.main import create_app

    runtime = EcosystemRuntime()
    paths = runtime.paths

    banner = f"""
================================================================================
  多Agent生态开发工具（裸机版 V1.1）
  工作台地址 : http://127.0.0.1:{args.port}
  局域网地址 : http://<本机IP>:{args.port}   （监听 {args.host}，禁止公网暴露）
  数据根目录 : {paths.root}
    ├─ config     : {paths.config}
    ├─ sessions   : {paths.sessions}
    ├─ uploads    : {paths.uploads}
    ├─ logs       : {paths.logs}
    └─ vector_db  : {paths.vector_db}
  运行模式   : 纯裸机（无 Docker / 无容器沙箱）
  安全体系   : 会话目录隔离 + 操作白名单 + 高危强制审批 + 后端二次校验
  初始登录   : 用户名 admin / 口令 admin123（bcrypt 存储，请首次登录后立即修改）

  首次启动将强制弹出「API 配置向导」，至少配置一个模型并测试连通后才能进入工作台。

  按 Ctrl+C 停止服务
================================================================================
"""
    print(banner)
    if not runtime.config.any_provider_configured():
        print("  [!] 尚未配置任何模型 API Key —— 浏览器打开后将强制进入 API 配置向导。\n")

    app = create_app(runtime)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
