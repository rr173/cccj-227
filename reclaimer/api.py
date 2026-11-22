"""对外门面：谱系导入、策略编译、计划生成、预检、执行、审计查询。

用法
====

    from reclaimer import Reclaimer

    r = Reclaimer.open("lakehouse.db", backend=MemoryBackend())
    r.import_objects([...])
    r.import_snapshot([...])
    r.import_policy([...], policy_version="v1")
    plan = r.generate_plan(policy_version="v1", snapshot_id=snap)
    r.precheck(plan["plan_id"])
    r.execute(plan["plan_id"])
    r.audit.object_status("raw_a", 1)
"""

from __future__ import annotations

import sqlite3
import time

from .audit import Audit
from .errors import PlanPausedError
from .executor import Executor, MemoryBackend, StorageBackend
from .importer import Importer
from .planner import Planner
from .precheck import Precheck
from .schema import initialize


class Reclaimer:
    def __init__(self, conn: sqlite3.Connection,
                 backend: StorageBackend | None = None,
                 clock=time.time):
        self.conn = conn
        self._clock = clock
        initialize(conn)
        self.importer = Importer(conn, clock=clock)
        self.planner = Planner(conn, clock=clock)
        self.prechecker = Precheck(conn, self.planner, clock=clock)
        self.backend = backend or MemoryBackend()
        self.executor = Executor(conn, self.backend, self.planner,
                                 clock=clock)
        self.audit = Audit(conn, backend=self.backend)

    # ------------------------------------------------------------ 工厂
    @classmethod
    def open(cls, db_path: str, backend: StorageBackend | None = None,
             clock=time.time) -> "Reclaimer":
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        return cls(conn, backend=backend, clock=clock)

    @classmethod
    def in_memory(cls, backend: StorageBackend | None = None,
                  clock=time.time) -> "Reclaimer":
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys=ON")
        return cls(conn, backend=backend, clock=clock)

    def close(self) -> None:
        self.conn.close()

    # ============================================================ 1. 导入
    def import_objects(self, objects) -> int:
        return self.importer.import_objects(objects)

    def import_snapshot(self, edges, snapshot_id=None,
                        description=None) -> str:
        return self.importer.import_snapshot(
            edges, snapshot_id=snapshot_id, description=description)

    def register_consumer(self, consumer_id, interests=(),
                          description=None) -> None:
        self.importer.register_consumer(
            consumer_id, interests=interests, description=description)

    def add_interest(self, consumer_id, object_id) -> None:
        self.importer.add_interest(consumer_id, object_id)

    def upsert_watermark(self, consumer_id, object_id, min_version,
                         recorded_at=None) -> dict:
        return self.importer.upsert_watermark(
            consumer_id, object_id, min_version, recorded_at=recorded_at)

    def detect_watermark_issues(self) -> list:
        return self.importer.detect_watermark_issues()

    # ============================================================ 2. 策略
    def import_policy(self, rules, policy_version, description=None) -> dict:
        return self.importer.import_policy(
            rules, policy_version, description=description)

    def import_hold(self, hold) -> str:
        return self.importer.import_hold(hold)

    def release_hold(self, hold_id, event_ref, release_id=None,
                     note=None) -> str:
        return self.importer.release_hold(
            hold_id, event_ref, release_id=release_id, note=note)

    # ============================================================ 3. 计划
    def generate_plan(self, *, policy_version, snapshot_id, as_of_ts=None,
                      max_batch_size=500, max_plan_items=5000,
                      plan_id=None) -> dict:
        return self.planner.generate_plan(
            policy_version=policy_version,
            snapshot_id=snapshot_id,
            as_of_ts=as_of_ts,
            max_batch_size=max_batch_size,
            max_plan_items=max_plan_items,
            plan_id=plan_id,
        )

    # ============================================================ 4. 预检
    def precheck(self, plan_id) -> dict:
        return self.prechecker.validate(plan_id)

    def plan_diffs(self, plan_id) -> list:
        return self.prechecker.list_diffs(plan_id)

    # ============================================================ 5. 执行
    def execute(self, plan_id, *, max_deletes=None,
                revalidate=True) -> dict:
        return self.executor.execute_plan(
            plan_id, max_deletes=max_deletes, revalidate=revalidate)

    def plan_summary(self, plan_id) -> dict:
        return self.planner.plan_summary(plan_id)

    # ============================================================ 6. 审计
    def object_status(self, object_id, version) -> dict:
        return self.audit.object_status(object_id, version)

    def evidence_for(self, object_id, version, plan_id=None) -> list:
        return self.audit.evidence_for(object_id, version, plan_id=plan_id)


__all__ = [
    "Reclaimer", "MemoryBackend", "StorageBackend", "PlanPausedError",
]
