"""执行前重新验证计划绑定的四类输入。

绑定输入：策略版本（规则指纹）、谱系快照、有效冻结集合、消费者水位。
任一指纹变化即把计划置为 PAUSED 并写入 ``plan_diffs``，执行器拒绝继续；
差异可经审计接口逐条查看。已完成/已终止的计划不重新验证。
"""

from __future__ import annotations

import time

from .schema import (
    PLAN_ABORTED,
    PLAN_COMPLETED,
    PLAN_PAUSED,
    PLAN_READY,
)

KIND_LABELS = {
    "policy": "POLICY",
    "snapshot": "SNAPSHOT",
    "holds": "HOLDS",
    "watermarks": "WATERMARKS",
}


class Precheck:
    def __init__(self, conn, planner, clock=time.time):
        self.conn = conn
        self.planner = planner
        self._clock = clock

    def validate(self, plan_id: str, *, persist: bool = True) -> dict:
        """返回 ``{"ok": bool, "diffs": [...]}``；漂移时暂停计划。"""
        plan = self.conn.execute(
            """SELECT status, policy_version, snapshot_id,
                      holds_fingerprint, watermarks_fingerprint
               FROM plans WHERE plan_id=?""",
            (plan_id,),
        ).fetchone()
        if plan is None:
            raise ValueError(f"unknown plan: {plan_id}")
        (status, policy_version, snapshot_id,
         bound_holds_fp, bound_wm_fp) = plan
        if status in (PLAN_COMPLETED, PLAN_ABORTED):
            return {"ok": True, "diffs": [], "note": f"plan {status}"}

        stored_policy_fp = self.conn.execute(
            "SELECT fingerprint FROM policy_versions WHERE policy_version=?",
            (policy_version,),
        ).fetchone()[0]
        current = self.planner.current_fingerprints(policy_version,
                                                     snapshot_id)
        bound = {
            "policy": stored_policy_fp,
            "snapshot": self._snapshot_fingerprint(snapshot_id),
            "holds": bound_holds_fp,
            "watermarks": bound_wm_fp,
        }
        diffs = [
            {"kind": KIND_LABELS[k],
             "bound_fingerprint": bound[k],
             "current_fingerprint": current[k]}
            for k in ("policy", "snapshot", "holds", "watermarks")
            if bound[k] != current[k]
        ]
        ok = not diffs
        if not ok and persist and status != PLAN_PAUSED:
            now = int(self._clock())
            self.conn.execute(
                "UPDATE plans SET status=? WHERE plan_id=?",
                (PLAN_PAUSED, plan_id),
            )
            self.conn.executemany(
                """INSERT INTO plan_diffs(plan_id, detected_at, kind, detail)
                   VALUES(?,?,?,?)""",
                [
                    (plan_id, now, d["kind"],
                     f"bound={d['bound_fingerprint'][:12]} "
                     f"current={d['current_fingerprint'][:12]}")
                    for d in diffs
                ],
            )
            self.conn.execute(
                """INSERT INTO audit_events(at_ts, event_type, plan_id,
                       detail)
                   VALUES(?,?,?,?)""",
                (now, "PLAN_PAUSED", plan_id,
                 "input drift: " + ",".join(d["kind"] for d in diffs)),
            )
            self.conn.commit()
        elif ok and persist and status == PLAN_PAUSED:
            # 指纹重新一致（例如问题被显式修复）才允许回到 READY
            self.conn.execute(
                "UPDATE plans SET status=? WHERE plan_id=?",
                (PLAN_READY, plan_id),
            )
            self.conn.commit()
        return {"ok": ok, "diffs": diffs, "status": status}

    def _snapshot_fingerprint(self, snapshot_id: str) -> str:
        from . import hashing
        edges = [
            dict(zip(("src_id", "src_version", "dst_id", "dst_version",
                      "edge_kind"), row))
            for row in self.conn.execute(
                """SELECT src_id, src_version, dst_id, dst_version, edge_kind
                   FROM lineage_edges WHERE snapshot_id=?""",
                (snapshot_id,),
            )
        ]
        return hashing.fingerprint_snapshot(edges)

    def list_diffs(self, plan_id: str) -> list[dict]:
        cols = ("diff_id", "detected_at", "kind", "detail")
        return [
            dict(zip(cols, row))
            for row in self.conn.execute(
                """SELECT diff_id, detected_at, kind, detail
                   FROM plan_diffs WHERE plan_id=? ORDER BY diff_id""",
                (plan_id,),
            )
        ]
