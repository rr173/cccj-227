"""分阶段回收计划生成。

阶段机
======
计划内每个过期对象依次经过：

    CANDIDATE（候选：保留期届满）
      ├─ HOLD_WAITING  被有效法律冻结覆盖，等待明确解除事件
      ├─ BLOCKED       水位保护，或仍可从存活对象沿谱系到达
      └─ RECYCLABLE    无任何存活下游引用，按叶序进入有界批次
            └─ RECYCLED（执行器删除后，墓碑终态）

所有图分析在 SQLite 内以递归 CTE 完成；计划条目数受 ``max_plan_items``
约束、批次大小受 ``max_batch_size`` 约束，应用层不全图驻留内存。
"""

from __future__ import annotations

import time
import uuid
from collections import Counter

from . import hashing
from .schema import (
    ITEM_BLOCKED,
    ITEM_CANDIDATE,
    ITEM_HOLD_WAITING,
    ITEM_RECYCLABLE,
    PLAN_DRAFT,
    PLAN_READY,
)

# 证据种类
EV_DOWNSTREAM = "LIVE_DOWNSTREAM"
EV_WATERMARK = "WATERMARK"
EV_HOLD = "HOLD"
EV_RETAINED = "RETAINED"
EV_RECYCLE_PROOF = "RECYCLE_PROOF"


class Planner:
    def __init__(self, conn, clock=time.time):
        self.conn = conn
        self._clock = clock

    # ============================================================ 指纹
    def current_fingerprints(self, policy_version: str, snapshot_id: str
                             ) -> dict:
        """重新计算四类输入的当前指纹（预检复用）。"""
        rules = [
            dict(zip(("obj_type", "classification", "environment",
                      "retention_seconds"), row))
            for row in self.conn.execute(
                """SELECT obj_type, classification, environment,
                          retention_seconds
                   FROM policy_rules WHERE policy_version=?""",
                (policy_version,))
        ]
        edges = [
            dict(zip(("src_id", "src_version", "dst_id", "dst_version",
                      "edge_kind"), row))
            for row in self.conn.execute(
                """SELECT src_id, src_version, dst_id, dst_version, edge_kind
                   FROM lineage_edges WHERE snapshot_id=?""",
                (snapshot_id,))
        ]
        active_holds = [
            dict(zip(("hold_id", "object_id", "version", "selector_type",
                      "selector_value"), row))
            for row in self.conn.execute(
                """SELECT h.hold_id, h.object_id, h.version,
                          h.selector_type, h.selector_value
                   FROM legal_holds h
                   WHERE NOT EXISTS (
                       SELECT 1 FROM hold_releases r
                       WHERE r.hold_id=h.hold_id)""")
        ]
        watermarks = [
            dict(zip(("consumer_id", "object_id", "min_version",
                      "recorded_at"), row))
            for row in self.conn.execute(
                """SELECT consumer_id, object_id, min_version, recorded_at
                   FROM consumer_watermarks""")
        ]
        issues = [
            dict(zip(("object_id", "consumer_id", "kind", "detail",
                      "resolved_at"), row))
            for row in self.conn.execute(
                """SELECT object_id, consumer_id, kind, detail, resolved_at
                   FROM watermark_issues WHERE resolved_at IS NULL""")
        ]
        return {
            "policy": hashing.fingerprint_policy(rules),
            "snapshot": hashing.fingerprint_snapshot(edges),
            "holds": hashing.fingerprint_holds(active_holds),
            "watermarks": hashing.fingerprint_watermarks(watermarks, issues),
        }

    # ============================================================ 生成
    def generate_plan(self, *, policy_version: str, snapshot_id: str,
                      as_of_ts: int | None = None,
                      max_batch_size: int = 500,
                      max_plan_items: int = 5000,
                      plan_id: str | None = None) -> dict:
        as_of_ts = int(as_of_ts if as_of_ts is not None else self._clock())
        plan_id = plan_id or f"plan-{uuid.uuid4().hex[:12]}"
        tag = uuid.uuid4().hex[:10]
        self._assert_inputs_exist(policy_version, snapshot_id)

        # 确保水位缺失问题已登记（缺失/不一致即阻断）
        from .importer import Importer
        Importer(self.conn, clock=self._clock).detect_watermark_issues()

        fps = self.current_fingerprints(policy_version, snapshot_id)
        try:
            # 先建计划行（DRAFT），plan_items / batches 的外键才有父行
            self.conn.execute(
                """INSERT INTO plans(plan_id, created_at, as_of_ts, status,
                       policy_version, snapshot_id, holds_fingerprint,
                       watermarks_fingerprint, max_batch_size, max_plan_items)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (plan_id, int(self._clock()), as_of_ts, PLAN_DRAFT,
                 policy_version, snapshot_id, fps["holds"],
                 fps["watermarks"], max_batch_size, max_plan_items),
            )
            self._build_temp_tables(tag, policy_version, snapshot_id, as_of_ts)
            stats = self._classify(tag, plan_id, snapshot_id,
                                   max_batch_size, max_plan_items)
            self.conn.execute(
                "UPDATE plans SET status=? WHERE plan_id=?",
                (PLAN_READY, plan_id),
            )
            self.conn.execute(
                """INSERT INTO audit_events(at_ts, event_type, plan_id, detail)
                   VALUES(?,?,?,?)""",
                (int(self._clock()), "PLAN_CREATED", plan_id,
                 f"fingerprints={fps} stats={stats}"),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.execute(f"DROP TABLE IF EXISTS tmp_{tag}_cand")
            self.conn.execute(f"DROP TABLE IF EXISTS tmp_{tag}_live")

        summary = self.plan_summary(plan_id)
        summary["plan_id"] = plan_id
        summary["fingerprints"] = fps
        return summary

    def _assert_inputs_exist(self, policy_version, snapshot_id):
        if not self.conn.execute(
            "SELECT 1 FROM policy_versions WHERE policy_version=?",
            (policy_version,),
        ).fetchone():
            raise ValueError(f"unknown policy version: {policy_version}")
        if not self.conn.execute(
            "SELECT 1 FROM lineage_snapshots WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone():
            raise ValueError(f"unknown lineage snapshot: {snapshot_id}")

    # ------------------------------------------------------------ 临时表
    def _build_temp_tables(self, tag, policy_version, snapshot_id, as_of_ts):
        conn = self.conn
        # 1) 保留期编译：最高具体度命中；无规则命中 -> NULL（默认保留）
        conn.execute(
            f"""
            CREATE TEMP TABLE tmp_{tag}_cand AS
            WITH matched AS (
                SELECT o.object_id, o.version, o.obj_type,
                       o.classification, o.environment, o.storage_layer,
                       o.created_at, o.payload_ref,
                       pr.retention_seconds,
                       ROW_NUMBER() OVER (
                           PARTITION BY o.object_id, o.version
                           ORDER BY pr.specificity DESC, pr.rule_id
                       ) AS rn
                FROM objects o
                LEFT JOIN policy_rules pr
                  ON pr.policy_version = :pv
                 AND (pr.obj_type IS NULL OR pr.obj_type = o.obj_type)
                 AND (pr.classification IS NULL
                      OR pr.classification = o.classification)
                 AND (pr.environment IS NULL
                      OR pr.environment = o.environment)
                WHERE o.lifecycle = 'LIVE'
            )
            SELECT object_id, version, obj_type, classification, environment,
                   storage_layer, created_at, payload_ref,
                   retention_seconds,
                   CASE WHEN retention_seconds IS NULL THEN 0
                        WHEN created_at + retention_seconds <= :as_of THEN 1
                        ELSE 0 END AS expired
            FROM matched WHERE rn = 1
            """,
            {"pv": policy_version, "as_of": as_of_ts},
        )
        # 2) 存活种子 = 全部未过期对象（含无策略规则、水位/冻结保护对象）
        conn.execute(
            f"""
            CREATE TEMP TABLE tmp_{tag}_live AS
            SELECT object_id, version FROM tmp_{tag}_cand WHERE expired = 0
            """
        )

    # ------------------------------------------------------------ 分类
    def _classify(self, tag, plan_id, snapshot_id,
                  max_batch_size, max_plan_items) -> dict:
        conn = self.conn
        cand, live = f"tmp_{tag}_cand", f"tmp_{tag}_live"

        # ---- 水位保护：缺失/冲突/越界问题对象全部版本阻断；
        #      正常对象取各消费者水位的最小值，version >= 最小水位即受保护
        conn.execute(
            f"""
            CREATE TEMP TABLE tmp_{tag}_wmp AS
            SELECT object_id, MIN(min_version) AS min_version
            FROM consumer_watermarks
            GROUP BY object_id
            """
        )
        problem_objects = {
            row[0] for row in conn.execute(
                """SELECT DISTINCT object_id FROM watermark_issues
                   WHERE resolved_at IS NULL""")
        }

        # 水位保护对象并入存活种子：受保护版本即便已过保留期也仍被
        # 消费者读取，沿谱系充当传播源（与 oracle 种子口径一致）。
        # 问题身份全部版本并入（缺失/不一致时一律视为存活，不得回收）。
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {live}(object_id, version)
            SELECT o.object_id, o.version
            FROM objects o
            JOIN tmp_{tag}_wmp w ON w.object_id = o.object_id
            WHERE o.lifecycle = 'LIVE'
              AND o.object_id NOT IN (
                  SELECT object_id FROM watermark_issues
                  WHERE resolved_at IS NULL)
              AND o.version >= w.min_version
            """
        )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {live}(object_id, version)
            SELECT o.object_id, o.version
            FROM objects o
            WHERE o.lifecycle = 'LIVE'
              AND o.object_id IN (
                  SELECT object_id FROM watermark_issues
                  WHERE resolved_at IS NULL)
            """
        )

        # ---- 有效法律冻结（解除事件之外的冻结）
        active_holds = conn.execute(
            """SELECT h.hold_id, h.object_id, h.version,
                      h.selector_type, h.selector_value
               FROM legal_holds h
               WHERE NOT EXISTS (
                   SELECT 1 FROM hold_releases r WHERE r.hold_id=h.hold_id)"""
        ).fetchall()

        # ---- 候选全集（有界）：按创建时间确定序，超出上限留给下一计划
        candidates = conn.execute(
            f"""SELECT object_id, version, created_at, storage_layer,
                       payload_ref, retention_seconds
                FROM {cand} WHERE expired = 1
                ORDER BY created_at, object_id, version
                LIMIT ?""",
            (max_plan_items,),
        ).fetchall()

        stats = Counter()
        now = int(self._clock())
        recyclable_rows = []

        for object_id, version, created_at, layer, payload_ref, retention in \
                candidates:
            stats[ITEM_CANDIDATE] += 1
            # 先以 CANDIDATE 入表，随后按判断结果单向推进阶段
            conn.execute(
                """INSERT INTO plan_items(plan_id, object_id, version, stage,
                       refcount_at_plan, current_refcount, updated_at)
                   VALUES(?,?,?,'CANDIDATE',0,0,?)""",
                (plan_id, object_id, version, now),
            )

            # (a) 法律冻结
            hit_hold = self._find_hold(active_holds, object_id, version,
                                       conn)
            if hit_hold:
                self._promote(plan_id, object_id, version,
                              ITEM_HOLD_WAITING, "legal hold active")
                conn.execute(
                    """INSERT INTO reference_evidence(plan_id, object_id,
                           version, evidence_kind, ref_object_id,
                           ref_version, detail)
                       VALUES(?,?,?,?,?,?,?)""",
                    (plan_id, object_id, version, EV_HOLD,
                     object_id, version, f"hold {hit_hold} active"),
                )
                stats[ITEM_HOLD_WAITING] += 1
                continue

            # (b) 水位保护
            wm_reason = None
            if object_id in problem_objects:
                wm_reason = "watermark missing/conflicting/inconsistent"
            else:
                wm = conn.execute(
                    f"SELECT min_version FROM tmp_{tag}_wmp WHERE object_id=?",
                    (object_id,),
                ).fetchone()
                if wm is not None and version >= wm[0]:
                    wm_reason = (
                        f"version {version} >= earliest required "
                        f"version {wm[0]}"
                    )
            if wm_reason:
                self._promote(plan_id, object_id, version,
                              ITEM_BLOCKED, wm_reason)
                conn.execute(
                    """INSERT INTO reference_evidence(plan_id, object_id,
                           version, evidence_kind, consumer_id, detail)
                       VALUES(?,?,?,?,?,?)""",
                    (plan_id, object_id, version, EV_WATERMARK,
                     None, wm_reason),
                )
                stats[ITEM_BLOCKED] += 1
                continue

            recyclable_rows.append((object_id, version, created_at))

        # (c) 存活下游阻断：若对象 X 仍被某个存活对象（直接或传递）依赖，
        #     X 必须保留。边语义为 src 依赖 dst（src 是下游、dst 是上游），
        #     因此从存活种子沿 src->dst 方向传播，命中的候选一律阻断。
        conn.execute(
            f"""
            CREATE TEMP TABLE tmp_{tag}_reached AS
            WITH RECURSIVE reach(oid, ver) AS (
                SELECT object_id, version FROM {live}
                UNION
                SELECT e.dst_id, e.dst_version
                FROM reach r
                JOIN lineage_edges e
                  ON e.snapshot_id = :snap
                 AND e.src_id = r.oid AND e.src_version = r.ver
            )
            SELECT DISTINCT oid, ver FROM reach
            """,
            {"snap": snapshot_id},
        )

        # (d) 引用计数：每个候选被多少“存活对象”直接依赖
        refcount_rows = conn.execute(
            f"""
            SELECT c.object_id, c.version, COUNT(e.src_id)
            FROM {cand} c
            LEFT JOIN lineage_edges e
              ON e.snapshot_id = :snap
             AND e.dst_id = c.object_id AND e.dst_version = c.version
             AND EXISTS (
                 SELECT 1 FROM {live} l
                 WHERE l.object_id = e.src_id
                   AND l.version = e.src_version)
            GROUP BY c.object_id, c.version
            """,
            {"snap": snapshot_id},
        ).fetchall()
        refcounts = {(oid, ver): cnt for oid, ver, cnt in refcount_rows}

        final_recyclable = []
        for object_id, version, created_at in recyclable_rows:
            rc = refcounts.get((object_id, version), 0)
            reachable = conn.execute(
                f"SELECT 1 FROM tmp_{tag}_reached WHERE oid=? AND ver=?",
                (object_id, version),
            ).fetchone()
            conn.execute(
                """UPDATE plan_items
                   SET refcount_at_plan=?, current_refcount=?, updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (rc, rc, now, plan_id, object_id, version),
            )
            if reachable:
                self._promote(plan_id, object_id, version,
                              ITEM_BLOCKED,
                              "reachable from retained downstream object")
                for src_id, src_ver in conn.execute(
                    """SELECT e.src_id, e.src_version
                       FROM lineage_edges e
                       JOIN {live} l
                         ON l.object_id=e.src_id AND l.version=e.src_version
                       WHERE e.snapshot_id=?
                         AND e.dst_id=? AND e.dst_version=?""".format(
                        live=live),
                    (snapshot_id, object_id, version),
                ):
                    conn.execute(
                        """INSERT INTO reference_evidence(plan_id, object_id,
                               version, evidence_kind, ref_object_id,
                               ref_version, detail)
                           VALUES(?,?,?,?,?,?,?)""",
                        (plan_id, object_id, version, EV_DOWNSTREAM,
                         src_id, src_ver,
                         f"retained downstream {src_id} v{src_ver}"),
                    )
                stats[ITEM_BLOCKED] += 1
            else:
                final_recyclable.append((object_id, version, created_at))
                stats[ITEM_RECYCLABLE] += 1

        # (e) 叶序有界批次：created_at 升序，同批内再按身份/版本确定
        batch_no = 0
        for idx, (object_id, version, _) in enumerate(final_recyclable):
            if idx % max_batch_size == 0:
                batch_no += 1
                conn.execute(
                    """INSERT INTO plan_batches(plan_id, batch_no, item_count)
                       VALUES(?,?,0)""",
                    (plan_id, batch_no),
                )
            conn.execute(
                """UPDATE plan_items SET stage='RECYCLABLE', batch_no=?,
                       updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (batch_no, now, plan_id, object_id, version),
            )
            conn.execute(
                "UPDATE plan_batches SET item_count=item_count+1 "
                "WHERE plan_id=? AND batch_no=?",
                (plan_id, batch_no),
            )

        conn.execute(f"DROP TABLE IF EXISTS tmp_{tag}_wmp")
        conn.execute(f"DROP TABLE IF EXISTS tmp_{tag}_reached")
        return dict(stats)

    def _find_hold(self, active_holds, object_id, version, conn):
        """返回命中的 hold_id；对象冻结支持版本为空=全版本。"""
        for hold_id, h_obj, h_ver, sel_type, sel_value in active_holds:
            if h_obj is not None:
                if h_obj == object_id and (h_ver is None or h_ver == version):
                    return hold_id
                continue
            # 选择器冻结
            row = conn.execute(
                "SELECT obj_type, classification, environment FROM objects"
                " WHERE object_id=? AND version=?",
                (object_id, version),
            ).fetchone()
            if row is None:
                continue
            value = row[
                {"obj_type": 0, "classification": 1, "environment": 2}[sel_type]
            ]
            if value == sel_value:
                return hold_id
        return None

    def _promote(self, plan_id, object_id, version, stage, reason):
        self.conn.execute(
            """UPDATE plan_items SET stage=?, block_reason=?, updated_at=?
               WHERE plan_id=? AND object_id=? AND version=?""",
            (stage, reason, int(self._clock()),
             plan_id, object_id, version),
        )

    # ============================================================ 查询
    def plan_summary(self, plan_id: str) -> dict:
        rows = dict(
            self.conn.execute(
                """SELECT stage, COUNT(*) FROM plan_items
                   WHERE plan_id=? GROUP BY stage""",
                (plan_id,),
            ).fetchall()
        )
        plan = self.conn.execute(
            "SELECT status, policy_version, snapshot_id, as_of_ts,"
            " max_batch_size, max_plan_items, last_error FROM plans"
            " WHERE plan_id=?",
            (plan_id,),
        ).fetchone()
        return {
            "plan_id": plan_id,
            "status": plan[0],
            "policy_version": plan[1],
            "snapshot_id": plan[2],
            "as_of_ts": plan[3],
            "max_batch_size": plan[4],
            "max_plan_items": plan[5],
            "last_error": plan[6],
            "stage_counts": rows,
            "batch_count": self.conn.execute(
                "SELECT COUNT(*) FROM plan_batches WHERE plan_id=?",
                (plan_id,),
            ).fetchone()[0],
        }
