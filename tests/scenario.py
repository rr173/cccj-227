"""手工构造的湖仓场景（确定性夹具）。

图结构（边 src -> dst 表示 src 派生自/依赖 dst）
=================================================

    raw_a v1 ─────────────► t1 v1 ──┬──► m1 v1 ──► audit_ev v1
    raw_a v2 ──┐                     │      ▲
               ├────────► t1 v2 ─────┘      │
    raw_b v1 ──┘                            └── m2 v1（共享父节点 m1 v1）
    raw_c v1（无消费者、无边，孤立对象）

阶段元素覆盖
============
- 原始文件 -> 规范表 -> 指标 -> 审计证据的完整派生链；
- 两个消费者不同水位（c-bi 低水位 / c-ml 高水位）；
- 一个法律冻结（m2 v1）；
- 策略两版（v1 -> v2，保留期变化）；
- 孤立对象 raw_c v1；
- 共享父节点 m1 v1（被 m2 v1 与 audit_ev v1 共同引用）；
- 注入删除失败（t1 v2）与进程退出崩溃（raw_c v1）。

时间基准：T0=1_000_000；评估时间 AS_OF=10_000_000。
"""

from __future__ import annotations

import copy
import json

T0 = 1_000_000
AS_OF = 10_000_000

# 保留期（秒）。选取原则：在 AS_OF=10_000_000 下让各类型分别落入
# “仍保留 / 已过期但被挡住 / 可回收”三种命运，四阶段都有代表对象。
RET_V1 = {
    "raw_file": 10_000_000,  # 原始文件默认仍在保留期（raw_b/raw_a 为存活种子）
    "table": 8_000_000,      # 表已过期（1_001_100+8M < 10M）
    "metric": 7_000_000,     # 指标已过期（1_002_100+7M < 10M）
    "audit_evidence": 20_000_000,
}
# 更高具体度规则：public 的原始文件保留期只有 3M（只命中孤立对象 raw_c）
RET_V1_EXTRA = [
    {"rule_id": "rule-public-raw", "obj_type": "raw_file",
     "classification": "public", "environment": None,
     "retention_seconds": 3_000_000},
]
RET_V2 = {
    "raw_file": 10_000_000,  # 不变
    "table": 5_000_000,      # 不变（表仍过期）
    "metric": 10_000_000,   # 变长：m1/m2 回到保留期内
    "audit_evidence": 20_000_000,
}

OBJECTS = [
    # (id, version, type, classification, env, layer, created_at)
    ("raw_a", 1, "raw_file", "confidential", "prod", "hot",  T0 + 100),
    ("raw_a", 2, "raw_file", "confidential", "prod", "warm", T0 + 200),
    ("raw_b", 1, "raw_file", "internal",     "prod", "warm", T0 + 300),
    ("raw_c", 1, "raw_file", "public",       "prod", "cold", T0 + 400),
    ("t1",    1, "table",    "confidential", "prod", "hot",  T0 + 1_100),
    ("t1",    2, "table",    "confidential", "prod", "hot",  T0 + 1_200),
    ("m1",    1, "metric",   "confidential", "prod", "hot",  T0 + 2_100),
    ("m2",    1, "metric",   "internal",     "prod", "hot",  T0 + 2_200),
    ("audit_ev", 1, "audit_evidence", "confidential", "prod",
     "audit", T0 + 3_100),
]

EDGES = [
    {"src_id": "t1", "src_version": 1, "dst_id": "raw_a", "dst_version": 1},
    {"src_id": "t1", "src_version": 2, "dst_id": "raw_a", "dst_version": 2},
    {"src_id": "t1", "src_version": 2, "dst_id": "raw_b", "dst_version": 1},
    {"src_id": "m1", "src_version": 1, "dst_id": "t1", "dst_version": 1},
    {"src_id": "m1", "src_version": 1, "dst_id": "t1", "dst_version": 2},
    {"src_id": "m2", "src_version": 1, "dst_id": "m1", "dst_version": 1},
    {"src_id": "audit_ev", "src_version": 1,
     "dst_id": "m1", "dst_version": 1},
]

CONSUMERS = [
    {"consumer_id": "c-bi", "description": "BI 报表（低水位）"},
    {"consumer_id": "c-ml", "description": "ML 特征（高水位）"},
]

# 订阅（仍需读取的对象身份）
INTERESTS = [
    {"consumer_id": "c-bi", "object_id": "t1"},
    {"consumer_id": "c-bi", "object_id": "m1"},
    {"consumer_id": "c-ml", "object_id": "m1"},
    {"consumer_id": "c-ml", "object_id": "m2"},
    {"consumer_id": "c-ml", "object_id": "raw_a"},
]

# 水位：仍需读取的最早版本
WATERMARKS_V1 = [
    {"consumer_id": "c-bi", "object_id": "t1",  "min_version": 2},
    {"consumer_id": "c-bi", "object_id": "m1",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "m1",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "m2",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "raw_a", "min_version": 2},
]
# 漂移场景：c-ml 对 m1 的水位先记 2 再回退到 1——upsert 时检测到冲突
# （引导时按列表顺序写入，最后一条造成水位倒退）
WATERMARKS_CONFLICT = [
    {"consumer_id": "c-bi", "object_id": "t1",  "min_version": 2},
    {"consumer_id": "c-bi", "object_id": "m1",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "m1",  "min_version": 2},
    {"consumer_id": "c-ml", "object_id": "m2",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "raw_a", "min_version": 2},
    {"consumer_id": "c-ml", "object_id": "m1",  "min_version": 1},
]
# 漂移场景：c-bi 对 t1 水位从 2 变为 1（t1 v1 新进入水位保护）
WATERMARKS_V2_UPDATE = [
    {"consumer_id": "c-bi", "object_id": "t1",  "min_version": 1},
    {"consumer_id": "c-bi", "object_id": "m1",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "m1",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "m2",  "min_version": 1},
    {"consumer_id": "c-ml", "object_id": "raw_a", "min_version": 2},
]

HOLDS = [
    {"hold_id": "hold-m2", "object_id": "m2", "version": 1,
     "reason": "legal matter LM-2026-007", "applied_at": T0 + 5_000},
]

# ------------------------------------------------------------------ 手工期望
# v1 策略 + AS_OF 下，逐对象手工判定（保留期算术见 RET_V1 注释）
EXPECTED_V1_RETAINED = {
    ("raw_a", 1), ("raw_a", 2), ("raw_b", 1),  # raw 10M 默认规则
    ("audit_ev", 1),                            # 审计 20M
}
EXPECTED_V1_EXPIRED = {
    ("raw_c", 1),   # public 高具体度规则 3M -> 孤立对象过期
    ("t1", 1), ("t1", 2), ("m1", 1), ("m2", 1),
}
EXPECTED_V1_HOLD_WAITING = {("m2", 1)}
EXPECTED_V1_WATERMARK_BLOCKED = {
    ("m1", 1),      # 两消费者都要求 m1 >= 1
}
EXPECTED_V1_DOWNSTREAM_BLOCKED = {
    ("t1", 1),      # 被存活的 m1 v1（水位保护）沿谱系到达
    ("t1", 2),      # 被 m1 v1 到达，且自身水位 >= 2
}
EXPECTED_V1_BLOCKED = EXPECTED_V1_WATERMARK_BLOCKED \
    | EXPECTED_V1_DOWNSTREAM_BLOCKED
# raw_c 过期、无水位、无边——唯一可回收叶（孤立对象）
EXPECTED_V1_RECYCLABLE = {("raw_c", 1)}
EXPECTED_V1_LEAF_ORDER = [("raw_c", 1)]

# v2 策略（metric 保留期拉长）下的手工期望：
# raws(10M) 全部保留；audit_ev 保留；metrics(10M) 回到保留期；
# 表两版仍过期：t1 v1 被存活 m1 v1 阻断；t1 v2 同时被水位与下游阻断。
EXPECTED_V2_RETAINED = {
    ("raw_a", 1), ("raw_a", 2), ("raw_b", 1), ("raw_c", 1),
    ("m1", 1), ("m2", 1), ("audit_ev", 1),
}
# v2 没有 public 短保留规则：raw_c 按 10M 默认规则保留EXPECTED_V2_EXPIRED = {("t1", 1), ("t1", 2)}
EXPECTED_V2_RECYCLABLE = set()
EXPECTED_V2_BLOCKED = {("t1", 1), ("t1", 2)}
EXPECTED_V2_HOLD_WAITING = set()     # m2 v1 已不过期，无冻结等待条目


def build_objects() -> list[dict]:
    return [
        {
            "object_id": oid, "version": ver, "obj_type": otype,
            "classification": cls, "environment": env,
            "storage_layer": layer, "created_at": created,
            "payload_ref": f"{layer}://{oid}/v{ver}",
        }
        for (oid, ver, otype, cls, env, layer, created) in OBJECTS
    ]


def policy_rules(ret: dict, extra: list | None = None) -> list[dict]:
    # 显式给出通配维度，保证产品端与 oracle 的匹配口径一致
    base = [
        {"rule_id": f"rule-{otype}", "obj_type": otype,
         "classification": None, "environment": None,
         "retention_seconds": seconds}
        for otype, seconds in ret.items()
    ]
    return base + (extra or [])


def build_fixture(*, policy_ret=RET_V1,
                  watermarks=None,
                  holds=None,
                  hold_releases=None,
                  extra_rules=None) -> dict:
    """构造给独立 oracle 读取的 JSON 夹具（不经过产品代码）。"""
    if extra_rules is None:
        extra_rules = RET_V1_EXTRA if policy_ret is RET_V1 else []
    return {
        "as_of_ts": AS_OF,
        "objects": build_objects(),
        "edges": copy.deepcopy(EDGES),
        "policy_rules": policy_rules(policy_ret, extra_rules),
        "consumers": copy.deepcopy(CONSUMERS),
        "interests": copy.deepcopy(INTERESTS),
        "watermarks": copy.deepcopy(
            WATERMARKS_V1 if watermarks is None else watermarks),
        "holds": copy.deepcopy(HOLDS if holds is None else holds),
        "hold_releases": copy.deepcopy(hold_releases or []),
    }


def write_fixture(path: str, **kwargs) -> str:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(build_fixture(**kwargs), fh, ensure_ascii=False,
                  indent=2, sort_keys=True)
    return path
