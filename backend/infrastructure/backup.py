# -*- coding: utf-8 -*-
"""
备份导出模块（基础设施层）

架构文档来源：第9章 9.3 数据备份
    支持会话记录、审批记录、加密配置、向量记忆库导出备份
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from backend.infrastructure.database import Database
from backend.infrastructure.vector_store import VectorStore
from backend.utils.paths import EcosystemPaths


class BackupManager:
    def __init__(self, paths: EcosystemPaths, db: Database, vector_store: VectorStore, config_store=None):
        self.paths = paths
        self.db = db
        self.vector_store = vector_store
        self.config_store = config_store

    def export_bundle(self) -> dict[str, Any]:
        """导出完整备份包（加密配置只导出密文信封，绝不含明文密钥）。"""
        bundle: dict[str, Any] = {
            "format": "multi_agent_ecosystem_backup",
            "version": "1.1",
            "exported_at": time.time(),
            "exported_at_text": time.strftime("%Y-%m-%d %H:%M:%S"),
            "logs": self.db.dump_all(),
            "vector_db": self.vector_store.export(),
            "encrypted_config": {},
        }
        if self.config_store is not None:
            try:
                bundle["encrypted_config"] = self.config_store.export_sealed_only()
            except Exception as exc:  # noqa: BLE001
                bundle["encrypted_config"] = {"error": str(exc)}
        return bundle

    def write_backup_file(self, bundle: dict[str, Any] | None = None) -> Path:
        bundle = bundle or self.export_bundle()
        target_dir = self.paths.root / "backup"
        target_dir.mkdir(parents=True, exist_ok=True)
        name = "mae_backup_" + time.strftime("%Y%m%d_%H%M%S") + ".json"
        path = target_dir / name
        path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
