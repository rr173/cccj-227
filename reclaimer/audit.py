"""审计查询接口。

关键语义：查询一个对象的历史位置时，三种回答严格区分——

1. ``LIVE``      ：对象存活，附当前存储层与物理后端状态；
2. ``RECYCLED``  ：明确的已回收状态，附墓碑（计划、时间、原位置），
   绝不伪造为“不存在”；
3. ``UNKNOWN``   ：该身份/版本从未进入任何谱系快照，才是真正不存在。
"""

from __future__ import annotations

from .errors import ObjectUnknownError
from .schema import OBJ_RECYCLED


class Audit:
    def __init__(self, conn, backend=None):
        self.conn = conn
        self.backend = backend

    # ------------------------------------------------------------ 对象
    def object_status(self, object_id: str, version: int) -> dict:
        row = self.conn.execute(
            """SELECT object_id, version, obj_type, classification,
                      environment, storage_layer, created_at, lifecycle,
                      recycled_at, recycled_by_plan, payload_ref
               FROM objects WHERE object_id=? AND version=?""",
            (object_id, version),
        ).fetchone()
        if row is None:
            # 真正不存在：身份从未导入
            return {"object_id": object_id, "version": version,
                    "status": "UNKNOWN",
                    "message": "object identity never registered in any "
                               "lineage snapshot"}

        cols = ("object_id", "version", "obj_type", "classification",
                "environment", "storage_layer", "created_at", "lifecycle",
                "recycled_at", "recycled_by_plan", "payload_ref")
        result = dict(zip(cols, row))

        if result["lifecycle"] == OBJ_RECYCLED:
            tomb = self.conn.execute(
                """SELECT plan_id, recycled_at, former_layer,
                          former_payload_ref
                   FROM recycle_tombstones
                   WHERE object_id=? AND version=?""",
                (object_id, version),
            ).fetchone()
            result["status"] = "RECYCLED"
            result["message"] = (
                f"object was recycled at {tomb[1]} by plan {tomb[0]}; "
                f"former location: layer={tomb[2]} ref={tomb[3]}"
            )
            result["tombstone"] = dict(zip(
                ("plan_id", "recycled_at", "former_layer",
                 "former_payload_ref"), tomb))
            if self.backend is not None:
                result["physical_status"] = self.backend.physical_status(
                    object_id, version)
        else:
            result["status"] = "LIVE"
            result["message"] = (
                f"object is live at layer={result['storage_layer']} "
                f"ref={result['payload_ref']}")
            if self.backend is not None:
                result["physical_status"] = self.backend.physical_status(
                    object_id, version)
        return result

    def require_known(self, object_id: str, version: int) -> dict:
        """同 :meth:`object_status`，但 UNKNOWN 时抛
        :class:`ObjectUnknownError`。"""
        status = self.object_status(object_id, version)
        if status["status"] == "UNKNOWN":
            raise ObjectUnknownError(
                f"{object_id} v{version} never existed in any snapshot"
            )
        return status

    def list_recycled(self, limit: int = 100) -> list[dict]:
        cols = ("object_id", "version", "plan_id", "recycled_at",
                "former_layer", "former_payload_ref")
        return [
            dict(zip(cols, row))
            for row in self.conn.execute(
                """SELECT object_id, version, plan_id, recycled_at,
                          former_layer, former_payload_ref
                   FROM recycle_tombstones
                   ORDER BY recycled_at DESC, object_id, version LIMIT ?""",
                (limit,),
            )
        ]

    # ------------------------------------------------------------ 证据
    def evidence_for(self, object_id: str, version: int,
                     plan_id: str | None = None) -> list[dict]:
        sql = """SELECT plan_id, evidence_kind, ref_object_id, ref_version,
                        consumer_id, detail
                 FROM reference_evidence
                 WHERE object_id=? AND version=?"""
        params = [object_id, version]
        if plan_id:
            sql += " AND plan_id=?"
            params.append(plan_id)
        sql += " ORDER BY evidence_id"
        cols = ("plan_id", "evidence_kind", "ref_object_id", "ref_version",
                "consumer_id", "detail")
        return [dict(zip(cols, r)) for r in self.conn.execute(sql, params)]

    def deletion_proof(self, plan_id: str) -> list[dict]:
        """返回计划中每个 RECYCLED 条目的零引用删除证明。"""
        cols = ("object_id", "version", "stage", "refcount_at_plan",
                "current_refcount", "proof_detail")
        return [
            dict(zip(cols, row))
            for row in self.conn.execute(
                """SELECT pi.object_id, pi.version, pi.stage,
                          pi.refcount_at_plan, pi.current_refcount, re.detail
                   FROM plan_items pi
                   LEFT JOIN reference_evidence re
                     ON re.plan_id=pi.plan_id
                    AND re.object_id=pi.object_id
                    AND re.version=pi.version
                    AND re.evidence_kind='RECYCLE_PROOF'
                   WHERE pi.plan_id=? AND pi.stage='RECYCLED'
                   ORDER BY pi.object_id, pi.version""",
                (plan_id,),
            )
        ]

    # ------------------------------------------------------------ 计划
    def plan_items(self, plan_id: str, stage: str | None = None) -> list[dict]:
        sql = """SELECT object_id, version, stage, block_reason,
                        refcount_at_plan, current_refcount, batch_no,
                        attempts, last_error
                 FROM plan_items WHERE plan_id=?"""
        params = [plan_id]
        if stage:
            sql += " AND stage=?"
            params.append(stage)
        sql += " ORDER BY COALESCE(batch_no, 999999), object_id, version"
        cols = ("object_id", "version", "stage", "block_reason",
                "refcount_at_plan", "current_refcount", "batch_no",
                "attempts", "last_error")
        return [dict(zip(cols, r)) for r in self.conn.execute(sql, params)]

    def plan_diffs(self, plan_id: str) -> list[dict]:
        cols = ("diff_id", "detected_at", "kind", "detail")
        return [
            dict(zip(cols, r))
            for r in self.conn.execute(
                """SELECT diff_id, detected_at, kind, detail
                   FROM plan_diffs WHERE plan_id=? ORDER BY diff_id""",
                (plan_id,),
            )
        ]

    def execution_log(self, plan_id: str) -> list[dict]:
        cols = ("log_id", "object_id", "version", "action", "ok",
                "detail", "at_ts")
        return [
            dict(zip(cols, r))
            for r in self.conn.execute(
                """SELECT log_id, object_id, version, action, ok, detail,
                          at_ts
                   FROM execution_log WHERE plan_id=? ORDER BY log_id""",
                (plan_id,),
            )
        ]

    def audit_events(self, event_type: str | None = None,
                     limit: int = 200) -> list[dict]:
        sql = ("SELECT event_id, at_ts, event_type, plan_id, object_id, "
               "version, detail FROM audit_events")
        params = []
        if event_type:
            sql += " WHERE event_type=?"
            params.append(event_type)
        sql += " ORDER BY event_id DESC LIMIT ?"
        params.append(limit)
        cols = ("event_id", "at_ts", "event_type", "plan_id", "object_id",
                "version", "detail")
        return [dict(zip(cols, r)) for r in self.conn.execute(sql, params)]

    def watermark_issues(self) -> list[dict]:
        cols = ("object_id", "consumer_id", "kind", "detail", "detected_at",
                "resolved_at")
        return [
            dict(zip(cols, r))
            for r in self.conn.execute(
                """SELECT object_id, consumer_id, kind, detail, detected_at,
                          resolved_at
                   FROM watermark_issues
                   WHERE resolved_at IS NULL ORDER BY issue_id""")
        ]
