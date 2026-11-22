"""谱系与治理输入导入。

导入接口
========
- :meth:`Importer.import_objects`       对象（不可变身份 + 版本 + 层 + 来源）
- :meth:`Importer.import_snapshot`      派生边快照（跨版本边、环检测）
- :meth:`Importer.import_policy`        保留策略（编译、冲突检查、指纹）
- :meth:`Importer.import_hold`          法律冻结（可按对象或选择器）
- :meth:`Importer.release_hold`         明确解除事件（冻结唯一取消方式）
- :meth:`Importer.register_consumer`    消费者与订阅
- :meth:`Importer.upsert_watermark`     消费者水位（冲突/越界/缺失检测）

所有写入均幂等；重复导入同版本对象不会覆盖已回收墓碑。
"""

from __future__ import annotations

import time
import uuid
from typing import Iterable

from . import hashing
from .errors import (
    LineageValidationError,
    PolicyValidationError,
)
from .schema import OBJ_RECYCLED, initialize

VALID_EDGE_KINDS = {"DERIVED", "SOURCE", "COMPACTED"}
VALID_SELECTORS = {"obj_type", "classification", "environment"}


class Importer:
    def __init__(self, conn, clock=time.time):
        self.conn = conn
        self._clock = clock
        initialize(conn)

    # ------------------------------------------------------------ 对象
    def import_objects(self, objects: Iterable[dict]) -> int:
        """批量导入对象。已存在的同身份同版本行被忽略（不可变）。"""
        n = 0
        for o in objects:
            self._validate_object(o)
            cur = self.conn.execute(
                "SELECT lifecycle FROM objects WHERE object_id=? AND version=?",
                (o["object_id"], int(o["version"])),
            ).fetchone()
            if cur is not None:
                if cur[0] == OBJ_RECYCLED:
                    # 墓碑终态：绝不允许通过重新导入复活
                    raise LineageValidationError(
                        f"{o['object_id']} v{o['version']} already recycled; "
                        "re-import cannot revive it"
                    )
                continue
            self.conn.execute(
                """INSERT INTO objects(object_id, version, obj_type,
                       classification, environment, storage_layer,
                       created_at, lifecycle, payload_ref)
                   VALUES(?,?,?,?,?,?,?,'LIVE',?)""",
                (
                    o["object_id"], int(o["version"]), o["obj_type"],
                    o["classification"], o["environment"],
                    o["storage_layer"], int(o["created_at"]),
                    o.get("payload_ref"),
                ),
            )
            n += 1
        self.conn.commit()
        return n

    @staticmethod
    def _validate_object(o: dict) -> None:
        required = ("object_id", "version", "obj_type", "classification",
                    "environment", "storage_layer", "created_at")
        missing = [k for k in required if k not in o]
        if missing:
            raise LineageValidationError(f"object missing fields: {missing}")
        if int(o["version"]) < 1:
            raise LineageValidationError("version must be >= 1")

    # ------------------------------------------------------------ 谱系
    def import_snapshot(self, edges: Iterable[dict],
                        snapshot_id: str | None = None,
                        description: str | None = None) -> str:
        """导入一次谱系快照并做端点、自环、环检测。"""
        snapshot_id = snapshot_id or f"snap-{uuid.uuid4().hex[:12]}"
        edges = list(edges)
        for e in edges:
            for key in ("src_id", "src_version", "dst_id", "dst_version"):
                if key not in e:
                    raise LineageValidationError(f"edge missing field {key}")
            if e.get("edge_kind", "DERIVED") not in VALID_EDGE_KINDS:
                raise LineageValidationError(
                    f"unknown edge kind: {e['edge_kind']}"
                )
            if (e["src_id"], int(e["src_version"])) == (
                e["dst_id"], int(e["dst_version"])
            ):
                raise LineageValidationError("self-loop edge is forbidden")
            for endpoint in (
                (e["src_id"], int(e["src_version"])),
                (e["dst_id"], int(e["dst_version"])),
            ):
                exists = self.conn.execute(
                    "SELECT 1 FROM objects WHERE object_id=? AND version=?",
                    endpoint,
                ).fetchone()
                if not exists:
                    raise LineageValidationError(
                        f"edge endpoint {endpoint[0]} v{endpoint[1]} "
                        "does not exist"
                    )

        self.conn.execute(
            "INSERT INTO lineage_snapshots(snapshot_id, taken_at, description)"
            " VALUES(?,?,?)",
            (snapshot_id, int(self._clock()), description),
        )
        self.conn.executemany(
            """INSERT INTO lineage_edges(snapshot_id, src_id, src_version,
                   dst_id, dst_version, edge_kind)
               VALUES(?,?,?,?,?,?)""",
            [
                (
                    snapshot_id, e["src_id"], int(e["src_version"]),
                    e["dst_id"], int(e["dst_version"]),
                    e.get("edge_kind", "DERIVED"),
                )
                for e in edges
            ],
        )
        self._assert_acyclic(snapshot_id)
        self.conn.commit()
        return snapshot_id

    def _assert_acyclic(self, snapshot_id: str) -> None:
        """递归 CTE 环检测（数据库内完成，应用层不持有全图）。"""
        rows = self.conn.execute(
            """
            WITH RECURSIVE walk(src_key, dst_key, path, cyclic) AS (
                SELECT src_id || '@' || src_version,
                       dst_id || '@' || dst_version,
                       '|' || src_id || '@' || src_version
                            || '|' || dst_id || '@' || dst_version || '|',
                       0
                FROM lineage_edges WHERE snapshot_id=?
                UNION ALL
                SELECT w.src_key,
                       e.dst_id || '@' || e.dst_version,
                       w.path || e.dst_id || '@' || e.dst_version || '|',
                       CASE WHEN instr(w.path, '|' || e.dst_id
                            || '@' || e.dst_version || '|') > 0
                            THEN 1 ELSE 0 END
                FROM walk w
                JOIN lineage_edges e
                  ON e.snapshot_id=?
                 AND e.src_id || '@' || e.src_version = w.dst_key
                WHERE w.cyclic = 0
            )
            SELECT DISTINCT path FROM walk WHERE cyclic = 1
            LIMIT 1
            """,
            (snapshot_id, snapshot_id),
        ).fetchall()
        if rows:
            raise LineageValidationError(
                f"cycle detected in snapshot {snapshot_id}: {rows[0][0]}"
            )

    # ------------------------------------------------------------ 策略
    def import_policy(self, rules: Iterable[dict],
                      policy_version: str,
                      description: str | None = None) -> dict:
        """编译一版保留策略。

        规则维度为 (obj_type, classification, environment)，任何维度
        允许为 ``None`` 表示通配；同一策略版本内匹配组合不得重复。
        具体度 = 非通配维度数，计划生成时取最高具体度命中规则。
        """
        rules = [dict(r) for r in rules]
        seen = set()
        for r in rules:
            if int(r["retention_seconds"]) < 0:
                raise PolicyValidationError("retention_seconds must be >= 0")
            key = (
                r.get("obj_type"), r.get("classification"),
                r.get("environment"),
            )
            if key in seen:
                raise PolicyValidationError(f"duplicate policy match: {key}")
            seen.add(key)
            r["specificity"] = sum(v is not None for v in key)

        if self.conn.execute(
            "SELECT 1 FROM policy_versions WHERE policy_version=?",
            (policy_version,),
        ).fetchone():
            raise PolicyValidationError(
                f"policy version {policy_version} already exists"
            )

        fp = hashing.fingerprint_policy(rules)
        self.conn.execute(
            """INSERT INTO policy_versions(policy_version, compiled_at,
                   fingerprint, description)
               VALUES(?,?,?,?)""",
            (policy_version, int(self._clock()), fp, description),
        )
        self.conn.executemany(
            """INSERT INTO policy_rules(policy_version, rule_id, obj_type,
                   classification, environment, retention_seconds, specificity)
               VALUES(?,?,?,?,?,?,?)""",
            [
                (
                    policy_version,
                    r.get("rule_id", f"rule-{uuid.uuid4().hex[:8]}"),
                    r.get("obj_type"), r.get("classification"),
                    r.get("environment"), int(r["retention_seconds"]),
                    r["specificity"],
                )
                for r in rules
            ],
        )
        self.conn.commit()
        return {"policy_version": policy_version, "fingerprint": fp,
                "rule_count": len(rules)}

    # ------------------------------------------------------------ 冻结
    def import_hold(self, hold: dict) -> str:
        """登记法律冻结。对象冻结与选择器冻结二选一。"""
        hold_id = hold["hold_id"]
        if self.conn.execute(
            "SELECT 1 FROM legal_holds WHERE hold_id=?", (hold_id,)
        ).fetchone():
            raise LineageValidationError(f"hold {hold_id} already exists")
        is_object_hold = hold.get("object_id") is not None
        is_selector_hold = hold.get("selector_type") is not None
        if is_object_hold == is_selector_hold:
            raise LineageValidationError(
                "hold must be either object-scoped or selector-scoped"
            )
        if is_selector_hold and hold["selector_type"] not in VALID_SELECTORS:
            raise LineageValidationError(
                f"invalid selector type: {hold['selector_type']}"
            )
        self.conn.execute(
            """INSERT INTO legal_holds(hold_id, object_id, version,
                   selector_type, selector_value, reason, applied_at)
               VALUES(?,?,?,?,?,?,?)""",
            (
                hold_id, hold.get("object_id"),
                int(hold["version"]) if hold.get("version") is not None else None,
                hold.get("selector_type"), hold.get("selector_value"),
                hold["reason"], int(hold.get("applied_at", self._clock())),
            ),
        )
        self.conn.commit()
        return hold_id

    def release_hold(self, hold_id: str, event_ref: str,
                     release_id: str | None = None,
                     note: str | None = None) -> str:
        """登记明确解除事件——冻结的唯一取消方式。

        解除事件一经写入不可撤销、不可删除；没有对应冻结则报错。
        """
        if not self.conn.execute(
            "SELECT 1 FROM legal_holds WHERE hold_id=?", (hold_id,)
        ).fetchone():
            raise LineageValidationError(f"unknown hold: {hold_id}")
        if self.conn.execute(
            "SELECT 1 FROM hold_releases WHERE hold_id=?", (hold_id,)
        ).fetchone():
            raise LineageValidationError(
                f"hold {hold_id} already released; release is irreversible"
            )
        release_id = release_id or f"rel-{uuid.uuid4().hex[:12]}"
        self.conn.execute(
            """INSERT INTO hold_releases(release_id, hold_id, released_at,
                   event_ref, note)
               VALUES(?,?,?,?,?)""",
            (release_id, hold_id, int(self._clock()), event_ref, note),
        )
        self.conn.execute(
            """INSERT INTO audit_events(at_ts, event_type, detail)
               VALUES(?,?,?)""",
            (int(self._clock()), "HOLD_RELEASED",
             f"hold {hold_id} released by event {event_ref}"),
        )
        self.conn.commit()
        return release_id

    # ------------------------------------------------------------ 消费者
    def register_consumer(self, consumer_id: str,
                          interests: Iterable[str] = (),
                          description: str | None = None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO consumers(consumer_id, description)"
            " VALUES(?,?)",
            (consumer_id, description),
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO consumer_interests(consumer_id, object_id)"
            " VALUES(?,?)",
            [(consumer_id, oid) for oid in interests],
        )
        self.conn.commit()

    def add_interest(self, consumer_id: str, object_id: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO consumer_interests(consumer_id, object_id)"
            " VALUES(?,?)",
            (consumer_id, object_id),
        )
        self.conn.commit()

    def upsert_watermark(self, consumer_id: str, object_id: str,
                         min_version: int, recorded_at: int | None = None
                         ) -> dict:
        """写入/更新水位并做一致性检查。

        - CONFLICT     ：同一 (消费者, 对象) 出现水位倒退（历史值更大）
        - OUT_OF_RANGE ：水位高于该身份已知最大版本
        水位缺失（有订阅无水位）在 :meth:`detect_watermark_issues` 中检测。
        冲突时保留较高水位（最保守），同时保留问题记录；存在未解决
        问题的对象在计划阶段一律阻断回收。
        """
        recorded_at = int(recorded_at if recorded_at is not None
                          else self._clock())
        existing = self.conn.execute(
            """SELECT min_version FROM consumer_watermarks
               WHERE consumer_id=? AND object_id=?""",
            (consumer_id, object_id),
        ).fetchone()
        result = {"status": "inserted", "issue": None}
        if existing is not None:
            old = existing[0]
            if int(min_version) < old:
                issue_id = self._record_issue(
                    object_id, consumer_id, "CONFLICT",
                    f"watermark moved backwards: {old} -> {min_version}",
                    recorded_at,
                )
                result = {"status": "conflict-kept-higher",
                          "issue_id": issue_id, "issue": "CONFLICT"}
                min_version = old  # 保守取高水位
            else:
                result["status"] = "updated"
        max_v = self.conn.execute(
            "SELECT MAX(version) FROM objects WHERE object_id=?",
            (object_id,),
        ).fetchone()[0]
        if max_v is not None and int(min_version) > max_v:
            issue_id = self._record_issue(
                object_id, consumer_id, "OUT_OF_RANGE",
                f"watermark {min_version} > max known version {max_v}",
                recorded_at,
            )
            result["issue"] = "OUT_OF_RANGE"
            result.setdefault("issue_id", issue_id)

        self.conn.execute(
            """INSERT INTO consumer_watermarks(consumer_id, object_id,
                   min_version, recorded_at)
               VALUES(?,?,?,?)
               ON CONFLICT(consumer_id, object_id) DO UPDATE SET
                   min_version=excluded.min_version,
                   recorded_at=excluded.recorded_at""",
            (consumer_id, object_id, int(min_version), recorded_at),
        )
        self.conn.commit()
        return result

    def _record_issue(self, object_id, consumer_id, kind, detail, at_ts):
        cur = self.conn.execute(
            """INSERT INTO watermark_issues(object_id, consumer_id, kind,
                   detail, detected_at)
               VALUES(?,?,?,?,?)""",
            (object_id, consumer_id, kind, detail, at_ts),
        )
        return cur.lastrowid

    def detect_watermark_issues(self) -> list[dict]:
        """全量扫描：有订阅无水位记 MISSING；返回全部未解决问题。"""
        at_ts = int(self._clock())
        missing = self.conn.execute(
            """SELECT DISTINCT i.consumer_id, i.object_id
               FROM consumer_interests i
               WHERE NOT EXISTS (
                   SELECT 1 FROM consumer_watermarks w
                   WHERE w.consumer_id=i.consumer_id
                     AND w.object_id=i.object_id)"""
        ).fetchall()
        for consumer_id, object_id in missing:
            exists = self.conn.execute(
                """SELECT 1 FROM watermark_issues
                   WHERE object_id=? AND consumer_id=?
                     AND kind='MISSING' AND resolved_at IS NULL""",
                (object_id, consumer_id),
            ).fetchone()
            if not exists:
                self._record_issue(
                    object_id, consumer_id, "MISSING",
                    "consumer interest has no watermark", at_ts,
                )
        self.conn.commit()
        return self.list_watermark_issues()

    def list_watermark_issues(self, only_open: bool = True) -> list[dict]:
        sql = """SELECT object_id, consumer_id, kind, detail, detected_at,
                        resolved_at
                 FROM watermark_issues
                 {where} ORDER BY issue_id""".format(
            where="WHERE resolved_at IS NULL" if only_open else ""
        )
        cols = ("object_id", "consumer_id", "kind", "detail",
                "detected_at", "resolved_at")
        return [dict(zip(cols, row)) for row in self.conn.execute(sql)]
