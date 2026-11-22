"""规范化指纹：把策略版本、谱系快照、冻结集合、消费者水位固化为哈希。

一次计划必须绑定这四个输入的指纹；执行前重新计算并比对，
任何变化都会触发暂停并生成差异（见 :mod:`reclaimer.precheck`）。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


def canonical_dumps(obj: Any) -> str:
    """键排序、无空白、保证分隔符稳定的 JSON 序列化。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def sha256_of(obj: Any) -> str:
    return hashlib.sha256(canonical_dumps(obj).encode("utf-8")).hexdigest()


def _none_safe(value):
    """排序键归一化：None 在 Python3 中不能与 str 比较，统一替换为哨兵。"""
    return "\x00" if value is None else value


def fingerprint_policy(rules: Iterable[dict]) -> str:
    """策略指纹：规则按 (类型, 分类, 环境, 期限) 排序后哈希。"""
    payload = sorted(
        (
            _none_safe(r.get("obj_type")),
            _none_safe(r.get("classification")),
            _none_safe(r.get("environment")),
            int(r["retention_seconds"]),
        )
        for r in rules
    )
    return sha256_of({"kind": "policy", "rules": payload})


def fingerprint_snapshot(edges: Iterable[dict]) -> str:
    """谱系快照指纹：边全量规范化后哈希。"""
    payload = sorted(
        (
            e["src_id"], int(e["src_version"]),
            e["dst_id"], int(e["dst_version"]),
            e.get("edge_kind", "DERIVED"),
        )
        for e in edges
    )
    return sha256_of({"kind": "lineage", "edges": payload})


def fingerprint_holds(active_holds: Iterable[dict]) -> str:
    """冻结集合指纹：只统计“当前仍有效”的冻结（未被解除事件取消）。"""
    payload = sorted(
        (
            h["hold_id"], _none_safe(h.get("object_id")),
            _none_safe(h.get("version")),
            _none_safe(h.get("selector_type")),
            _none_safe(h.get("selector_value")),
        )
        for h in active_holds
    )
    return sha256_of({"kind": "holds", "holds": payload})


def fingerprint_watermarks(watermarks: Iterable[dict],
                           issues: Iterable[dict] | None = None) -> str:
    """消费者水位指纹：水位本身与已登记的一致性问题一并固化。"""
    payload = sorted(
        (
            w["consumer_id"], w["object_id"],
            int(w["min_version"]), int(w["recorded_at"]),
        )
        for w in watermarks
    )
    issue_payload = sorted(
        (_none_safe(i["object_id"]), _none_safe(i.get("consumer_id")),
         i["kind"], i["detail"])
        for i in (issues or [])
        if i.get("resolved_at") is None
    )
    return sha256_of(
        {"kind": "watermarks", "watermarks": payload, "issues": issue_payload}
    )
