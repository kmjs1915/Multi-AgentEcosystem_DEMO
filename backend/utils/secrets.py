# -*- coding: utf-8 -*-
"""
AES-256 密钥加密模块 + SHA256 篡改校验

架构文档来源：第6章 6.1 API Key 安全存储规则
    1. 采用 AES-256 对称加密存储原始密钥，可本地解密用于调用 API
    2. 额外保存 SHA256 哈希值用于配置篡改校验
    3. 密钥永不明文上传、永不暴露前端
    4. Key 测试接口超时 5 秒，防止阻塞服务

实现说明（不简化、不做安全降级）：
  - 主密钥 master.key：首次运行用 os.urandom(32) 生成，0600 落盘，永不外泄、永不返回前端。
  - 每个密文使用独立随机 12 字节 nonce；AES-256-GCM 提供机密性 + 完整性（附加 SHA256 双校验）。
  - SHA256 校验覆盖 密文 + nonce + 上下文标签(context)，任何字段被改动 -> 解密直接失败。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from backend.utils.paths import harden_file


class SecretTampered(Exception):
    """配置篡改：SHA256 校验不通过或 GCM 认证失败（第6章 6.1 规则2）。"""


class MasterKeyMissing(Exception):
    """主密钥缺失或非法，必须重新初始化。"""


_KEY_BYTES = 32          # AES-256
_NONCE_BYTES = 12


# --------------------------------------------------------------------------
# 主密钥管理
# --------------------------------------------------------------------------
def load_or_create_master_key(path: Path) -> bytes:
    """加载或首次生成主密钥（32 字节，即 AES-256）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raw = base64.b64decode(path.read_bytes().strip())
        if len(raw) != _KEY_BYTES:
            raise MasterKeyMissing(f"主密钥长度非法：{len(raw)} 字节（应为 {_KEY_BYTES}）")
        return raw

    key = os.urandom(_KEY_BYTES)
    # 原子写入，避免半截文件
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(base64.b64encode(key))
    os.replace(tmp, path)
    harden_file(path)
    return key


def sha256_hex(data: bytes) -> str:
    """SHA256 十六进制摘要（第6章 6.1 规则2）。"""
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# 加解密
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class SealedSecret:
    """加密后的密钥信封（可安全落盘 / 可安全写日志）。"""

    ciphertext: str          # base64(AES-256-GCM 密文+tag)
    nonce: str               # base64(12字节随机数)
    digest: str              # SHA256(ciphertext|nonce|context) 篡改校验值
    context: str             # 绑定上下文，例如 "api_key:deepseek"
    algo: str = "AES-256-GCM"

    def to_dict(self) -> dict[str, Any]:
        return {
            "algo": self.algo,
            "ciphertext": self.ciphertext,
            "nonce": self.nonce,
            "digest": self.digest,
            "context": self.context,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SealedSecret":
        try:
            return cls(
                ciphertext=str(data["ciphertext"]),
                nonce=str(data["nonce"]),
                digest=str(data["digest"]),
                context=str(data["context"]),
                algo=str(data.get("algo", "AES-256-GCM")),
            )
        except KeyError as exc:
            raise SecretTampered(f"加密信封字段缺失：{exc}") from None


def _digest_of(ciphertext: str, nonce: str, context: str) -> str:
    return sha256_hex(f"{ciphertext}|{nonce}|{context}".encode("utf-8"))


def seal_secret(master_key: bytes, plaintext: str, *, context: str) -> SealedSecret:
    """AES-256-GCM 加密 + SHA256 摘要。plaintext 为空时返回空信封（表示未配置）。"""
    if plaintext is None:
        plaintext = ""
    if len(master_key) != _KEY_BYTES:
        raise MasterKeyMissing("主密钥必须为 32 字节")

    if plaintext == "":
        return SealedSecret(ciphertext="", nonce="", digest="", context=context)

    nonce = os.urandom(_NONCE_BYTES)
    aad = context.encode("utf-8")          # 上下文作为附加认证数据，防止密文跨字段搬运
    ct = AESGCM(master_key).encrypt(nonce, plaintext.encode("utf-8"), aad)
    b64_ct = base64.b64encode(ct).decode("ascii")
    b64_nonce = base64.b64encode(nonce).decode("ascii")
    return SealedSecret(
        ciphertext=b64_ct,
        nonce=b64_nonce,
        digest=_digest_of(b64_ct, b64_nonce, context),
        context=context,
    )


def open_secret(master_key: bytes, sealed: SealedSecret) -> str:
    """校验 SHA256 后再解密；任一环节失败一律抛 SecretTampered，绝不返回半成品。"""
    if not sealed.ciphertext:
        return ""
    if sealed.algo != "AES-256-GCM":
        raise SecretTampered(f"不支持的加密算法：{sealed.algo}")
    if not sealed.digest:
        raise SecretTampered("缺少 SHA256 篡改校验值")

    expected = _digest_of(sealed.ciphertext, sealed.nonce, sealed.context)
    if not hmac.compare_digest(expected, sealed.digest):
        raise SecretTampered("SHA256 校验失败：加密配置已被篡改")

    try:
        nonce = base64.b64decode(sealed.nonce)
        ct = base64.b64decode(sealed.ciphertext)
    except Exception as exc:  # noqa: BLE001
        raise SecretTampered(f"密文编码非法：{exc}") from exc

    if len(nonce) != _NONCE_BYTES:
        raise SecretTampered("nonce 长度非法")

    try:
        pt = AESGCM(master_key).decrypt(nonce, ct, sealed.context.encode("utf-8"))
    except InvalidTag as exc:
        raise SecretTampered("AES-256-GCM 认证失败：密文损坏或密钥不匹配") from exc

    return pt.decode("utf-8")


# --------------------------------------------------------------------------
# 展示脱敏（第6章 6.1 规则3：永不暴露前端）
# --------------------------------------------------------------------------
def mask_secret(plaintext: str, *, head: int = 4, tail: int = 4) -> str:
    """生成脱敏指纹，仅用于前端显示"是否已配置"，不可反推。"""
    if not plaintext:
        return ""
    if len(plaintext) <= head + tail:
        return "*" * len(plaintext)
    return f"{plaintext[:head]}{'*' * 8}{plaintext[-tail:]}"


def fingerprint(plaintext: str) -> str:
    """密钥指纹（SHA256 前 16 位），用于前端校验是否变更，不泄露明文。"""
    if not plaintext:
        return ""
    return sha256_hex(plaintext.encode("utf-8"))[:16]


def dumps_sealed(mapping: dict[str, SealedSecret]) -> str:
    return json.dumps({k: v.to_dict() for k, v in mapping.items()}, ensure_ascii=False, indent=2)


def loads_sealed(raw: str) -> dict[str, SealedSecret]:
    data = json.loads(raw or "{}")
    return {k: SealedSecret.from_dict(v) for k, v in data.items()}
