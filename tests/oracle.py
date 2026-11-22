"""独立参考程序（oracle）。

本模块**不 import 任何产品计划器代码**，只从确定性 JSON 夹具读取
对象、谱系、策略、冻结、消费者水位，用朴素的内存图算法独立计算：

- 每个对象在给定时间点的保留/过期判定；
- 法律冻结命中集合（只能由解除事件取消）；
- 水位保护集合（缺失/冲突/越界时整身份阻断）；
- 沿谱系从存活种子传播的“存活下游”阻断集合；
- 候选 / 冻结等待 / 阻断 / 可回收四个阶段集合；
- 叶序删除顺序（引用计数为 0 的节点先行，删除后级联减计数）。

测试用它与产品计划器的输出逐集合比对，两者实现路径完全独立。
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OracleResult:
    retained: set = field(default_factory=set)
    expired: set = field(default_factory=set)
    held: set = field(default_factory=set)
    watermark_blocked: set = field(default_factory=set)
    problem_identities: set = field(default_factory=set)
    downstream_blocked: set = field(default_factory=set)
    blocked: set = field(default_factory=set)
    hold_waiting: set = field(default_factory=set)
    candidates: set = field(default_factory=set)
    recyclable: set = field(default_factory=set)
    leaf_order: list = field(default_factory=list)
    refcounts: dict = field(default_factory=dict)


def _key(obj_id: str, version: int) -> tuple:
    return (obj_id, int(version))


class ReferenceOracle:
    """与产品代码零共享的朴素实现（仅依赖标准库）。"""

    def __init__(self, fixture: dict[str, Any]):
        self.fx = fixture
        self.objects = {
            _key(o["object_id"], o["version"]): o for o in fixture["objects"]
        }
        self.edges = [
            (_key(e["src_id"], e["src_version"]),
             _key(e["dst_id"], e["dst_version"]))
            for e in fixture["edges"]
        ]
        # src -> 它依赖的上游；反向图：上游 -> 下游
        self.downstream_of: dict[tuple, set] = defaultdict(set)
        self.upstream_of: dict[tuple, set] = defaultdict(set)
        for src, dst in self.edges:
            self.downstream_of[dst].add(src)
            self.upstream_of[src].add(dst)

    # ------------------------------------------------------------ 策略
    def _retention_seconds(self, obj: dict) -> int | None:
        best = None
        best_spec = -1
        for rule in self.fx["policy_rules"]:
            if rule.get("obj_type") is not None \
                    and rule["obj_type"] != obj["obj_type"]:
                continue
            if rule.get("classification") is not None \
                    and rule["classification"] != obj["classification"]:
                continue
            if rule.get("environment") is not None \
                    and rule["environment"] != obj["environment"]:
                continue
            spec = sum(v is not None for v in (
                rule.get("obj_type"), rule.get("classification"),
                rule.get("environment")))
            if spec > best_spec:
                best_spec = spec
                best = int(rule["retention_seconds"])
        return best

    def _active_holds(self) -> list[dict]:
        released = {r["hold_id"] for r in self.fx.get("hold_releases", [])}
        return [h for h in self.fx.get("holds", [])
                if h["hold_id"] not in released]

    def _hold_hit(self, key: tuple) -> str | None:
        obj = self.objects[key]
        for h in self._active_holds():
            if h.get("object_id") is not None:
                if h["object_id"] == key[0] and (
                        h.get("version") is None or h["version"] == key[1]):
                    return h["hold_id"]
            else:
                sel = h["selector_type"]
                if obj[
                    {"obj_type": "obj_type",
                     "classification": "classification",
                     "environment": "environment"}[sel]
                ] == h["selector_value"]:
                    return h["hold_id"]
        return None

    # ------------------------------------------------------------ 水位
    def _watermark_verdict(self) -> tuple[set, set]:
        """返回 (问题身份集合, 受保护版本键集合)。"""
        problems = set()
        wm: dict[str, dict[str, int]] = defaultdict(dict)
        for w in self.fx.get("watermarks", []):
            prev = wm[w["object_id"]].get(w["consumer_id"])
            if prev is not None and int(w["min_version"]) < prev:
                problems.add(w["object_id"])  # 冲突：水位倒退
            wm[w["object_id"]][w["consumer_id"]] = int(w["min_version"])
        # 缺失：有订阅无水位
        for interest in self.fx.get("interests", []):
            cid, oid = interest["consumer_id"], interest["object_id"]
            if cid not in wm.get(oid, {}):
                problems.add(oid)
        # 越界：水位超过已知最大版本
        max_version = defaultdict(int)
        for oid, ver in self.objects:
            max_version[oid] = max(max_version[oid], ver)
        for oid, per_consumer in wm.items():
            if any(v > max_version[oid] for v in per_consumer.values()):
                problems.add(oid)

        protected = set()
        for key, obj in self.objects.items():
            oid = key[0]
            if oid in problems:
                continue  # 问题身份整体阻断，不走正常水位保护
            if oid in wm:
                earliest = min(wm[oid].values())
                if key[1] >= earliest:
                    protected.add(key)
        return problems, protected

    # ============================================================ 主计算
    def evaluate(self, as_of_ts: int) -> OracleResult:
        r = OracleResult()
        r.problem_identities, wm_protected = self._watermark_verdict()

        # 1) 保留期判定
        for key, obj in self.objects.items():
            retention = self._retention_seconds(obj)
            if retention is None or obj["created_at"] + retention > as_of_ts:
                r.retained.add(key)
            else:
                r.expired.add(key)

        # 2) 冻结与水位（只对过期对象有阶段意义，但命中集合全量记录）
        for key in list(r.expired):
            if self._hold_hit(key):
                r.held.add(key)
            if key[0] in r.problem_identities or key in wm_protected:
                r.watermark_blocked.add(key)

        # 3) 存活种子 = 未过期 ∪ 水位保护（冻结不视为存活传播源，
        #    冻结对象本身等待，但其下游保留对象仍独立存活）
        seeds = set(r.retained) | wm_protected
        # 问题身份的所有版本一律视为保留（不得回收）
        for key in self.objects:
            if key[0] in r.problem_identities:
                seeds.add(key)

        # 4) 沿 上游->下游 传播可达性（朴素 BFS，全图在 oracle 内存中）
        reachable = set(seeds)
        queue = deque(seeds)
        while queue:
            cur = queue.popleft()
            for down in self.downstream_of.get(cur, ()):
                if down not in reachable:
                    reachable.add(down)
                    queue.append(down)

        # 5) 组装阶段集合
        for key in r.expired:
            r.candidates.add(key)
            if key in r.held:
                r.hold_waiting.add(key)
                continue
            if key in r.watermark_blocked:
                r.blocked.add(key)
                continue
            if key in reachable:
                r.downstream_blocked.add(key)
                r.blocked.add(key)
                continue
            r.recyclable.add(key)

        # 6) 引用计数：每个对象被多少“存活（retained 种子集）”对象直接依赖
        live = set(seeds)
        for key in self.objects:
            referrers = [d for d in self.downstream_of.get(key, ())
                         if d in live]
            r.refcounts[key] = len(referrers)

        # 7) 叶序删除模拟：计数为 0 的可回收对象按 created_at 序删除，
        #    删除后级联减计数，解锁被阻断的上游
        remaining_live = set(self.objects)  # oracle 内部模拟删除
        counts = defaultdict(int)
        for up, downs in self.downstream_of.items():
            counts[up] = len([d for d in downs if d in remaining_live
                              and d in live])
        # 计数初值只统计存活引用；可回收对象的引用者都不可存活，故为 0
        pending = set(r.recyclable)
        blocked_pending = set(r.blocked)
        while pending:
            leaves = sorted(
                (k for k in pending if counts[k] == 0),
                key=lambda k: (self.objects[k]["created_at"], k[0], k[1]),
            )
            if not leaves:
                break
            leaf = leaves[0]
            r.leaf_order.append(leaf)
            pending.remove(leaf)
            remaining_live.discard(leaf)
            for up in self.upstream_of.get(leaf, ()):
                counts[up] = max(0, counts[up] - 1)
                if up in blocked_pending and counts[up] == 0:
                    # 级联解锁前仍需确认无任何存活路径
                    if not self._has_live_path(up, remaining_live, live
                                               - {leaf}):
                        blocked_pending.discard(up)
                        pending.add(up)
                        r.recyclable.add(up)
                        r.blocked.discard(up)
        return r

    def _has_live_path(self, target, remaining, live_seeds) -> bool:
        seen = set()
        queue = deque(k for k in live_seeds if k in remaining)
        while queue:
            cur = queue.popleft()
            if cur == target:
                return True
            for d in self.downstream_of.get(cur, ()):
                if d in remaining and d not in seen:
                    seen.add(d)
                    queue.append(d)
        return False


def evaluate_fixture_file(path: str, as_of_ts: int) -> OracleResult:
    with open(path, "r", encoding="utf-8") as fh:
        fixture = json.load(fh)
    return ReferenceOracle(fixture).evaluate(as_of_ts)
