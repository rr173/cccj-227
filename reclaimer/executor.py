"""分阶段执行器：叶序删除、失败重试、崩溃恢复。

安全不变量
==========
1. 只有 ``current_refcount = 0`` 且阶段为 RECYCLABLE 的条目才能删除，
   删除顺序严格沿谱系叶节点先行（同批按创建时间、身份、版本确定序）。
2. 每个对象一个事务：后端删除 + 墓碑 + 阶段推进 + 审计日志原子提交。
3. 后端接口幂等：进程在“物理删除已成功、事务未提交”之间退出，
   重开后再次调用只会得到“已不存在”，绝不产生第二次物理删除。
4. ``RECYCLED`` 是终态（schema 触发器兜底），已删对象不可能复活。
5. 删除一个对象后，递减它所依赖上游的存活引用计数；计数归零且不再
   被任何存活对象到达的 BLOCKED 条目可级联推进为 RECYCLABLE。
"""

from __future__ import annotations

import time

from . import catalog
from .errors import PlanNotExecutableError, PlanPausedError
from .planner import EV_RECYCLE_PROOF
from .precheck import Precheck
from .schema import (
    ITEM_BLOCKED,
    ITEM_FAILED,
    ITEM_RECYCLED,
    OBJ_RECYCLED,
    PLAN_COMPLETED,
    PLAN_PAUSED,
    PLAN_READY,
    PLAN_RUNNING,
)


class StorageBackend:
    """存储后端协议。生产实现可对接 S3/HDFS/表存储删除 API。"""

    def delete(self, record: dict) -> dict:
        """删除物理数据，必须幂等。

        返回 ``{"physical_delete": bool}``：真正执行了物理删除为 True，
        对象本就不存在（崩溃恢复场景）为 False。
        """
        raise NotImplementedError

    def physical_status(self, object_id: str, version: int) -> str:
        """LIVE / RECYCLED / UNKNOWN。"""
        raise NotImplementedError


class MemoryBackend(StorageBackend):
    """进程内后端，支持注入失败与“进程退出”故障用于测试。"""

    def __init__(self):
        self._store: dict[tuple, dict] = {}
        self.delete_calls: list[tuple] = []
        # (object_id, version) -> 在第几次尝试时失败（每次调用都失败直到重试耗尽次数）
        self.fail_until_attempt: dict[tuple, int] = {}
        self._attempts: dict[tuple, int] = {}
        # 崩溃钩子：在物理删除成功后、事务提交前抛出，模拟进程退出
        self.crash_after_physical_delete: dict[tuple, bool] = {}

    def add(self, object_id, version, record):
        self._store[(object_id, version)] = dict(record)

    def delete(self, record):
        key = (record["object_id"], record["version"])
        self.delete_calls.append(key)
        self._attempts[key] = self._attempts.get(key, 0) + 1
        attempt = self._attempts[key]
        if attempt <= self.fail_until_attempt.get(key, 0):
            raise RuntimeError(
                f"injected storage failure for {key} (attempt {attempt})"
            )
        existed = key in self._store
        self._store.pop(key, None)
        if self.crash_after_physical_delete.get(key):
            self.crash_after_physical_delete[key] = False
            raise SimulatedProcessExit(
                f"simulated process exit after deleting {key}"
            )
        return {"physical_delete": existed}

    def physical_status(self, object_id, version):
        key = (object_id, version)
        if key in self._store:
            return "LIVE"
        return "RECYCLED"


class SimulatedProcessExit(RuntimeError):
    """模拟进程在提交前被杀掉（os._exit 语义，不执行清理）。"""


class Executor:
    def __init__(self, conn, backend: StorageBackend, planner,
                 clock=time.time):
        self.conn = conn
        self.backend = backend
        self.planner = planner
        self.precheck = Precheck(conn, planner, clock=clock)
        self._clock = clock

    # ================================================================
    def execute_plan(self, plan_id: str, *, max_deletes: int | None = None,
                     revalidate: bool = True,
                     max_attempts_per_item: int = 1) -> dict:
        """推进计划，返回本轮执行统计。可反复调用直至完成。

        ``max_attempts_per_item`` 限制单次调用内对同一对象的尝试次数；
        失败后条目留存为 FAILED，下一次调用再重试（失败可重试且不重复删除）。
        """
        plan = self.conn.execute(
            "SELECT status FROM plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if plan is None:
            raise ValueError(f"unknown plan: {plan_id}")
        status = plan[0]
        if status == PLAN_PAUSED:
            diffs = self.precheck.list_diffs(plan_id)
            raise PlanPausedError(plan_id, diffs)
        if status == PLAN_COMPLETED:
            return self._stats(plan_id, note="already completed")
        if status not in (PLAN_READY, PLAN_RUNNING):
            raise PlanNotExecutableError(
                f"plan {plan_id} status {status} is not executable"
            )

        if revalidate:
            verdict = self.precheck.validate(plan_id)
            if not verdict["ok"]:
                raise PlanPausedError(plan_id, verdict["diffs"])

        self._set_status(plan_id, PLAN_RUNNING)
        deleted = failed = skipped = 0
        # 本次调用已尝试过的条目（失败后不在同一轮内立刻重试）
        attempted_this_call: set[tuple] = set()
        while max_deletes is None or deleted < max_deletes:
            item = self._next_leaf(plan_id, attempted_this_call)
            if item is None:
                break
            object_id, version, stage = item
            attempted_this_call.add((object_id, version))
            try:
                self._delete_one(plan_id, object_id, version, stage)
                deleted += 1
            except SimulatedProcessExit:
                raise
            except Exception as exc:  # 后端失败：记录 FAILED，留给下轮重试
                failed += 1
                self._record_failure(plan_id, object_id, version, exc)

        self._finish_or_continue(plan_id)
        stats = self._stats(plan_id)
        stats.update({"deleted_this_call": deleted,
                      "failed_this_call": failed,
                      "skipped_this_call": skipped})
        return stats

    # ------------------------------------------------------------ 叶序
    def _next_leaf(self, plan_id, excluded: set | None = None):
        """取当前可删叶：RECYCLABLE（含 FAILED 重试）且引用计数为 0。

        排序：批次号 -> 对象创建时间 -> 身份 -> 版本，保证全局确定叶序。
        ``excluded`` 中的条目本轮已尝试过，跳过以避免失败即重试。
        """
        sql = """
            SELECT pi.object_id, pi.version, pi.stage
            FROM plan_items pi
            JOIN objects o
              ON o.object_id=pi.object_id AND o.version=pi.version
            WHERE pi.plan_id=?
              AND pi.stage IN ('RECYCLABLE','FAILED')
              AND pi.current_refcount = 0
        """
        params: list = [plan_id]
        if excluded:
            placeholders = ",".join("?" * len(excluded))
            sql += (
                " AND NOT (pi.object_id || '@' || pi.version) IN ("
                + placeholders + ")"
            )
            params.extend(f"{oid}@{ver}" for oid, ver in excluded)
        sql += """
            ORDER BY pi.batch_no, o.created_at, pi.object_id, pi.version
            LIMIT 1
        """
        row = self.conn.execute(sql, params).fetchone()
        return row

    def _delete_one(self, plan_id, object_id, version, stage):
        now = int(self._clock())
        # 条目级再校验（防御并发/重入）
        guard = self.conn.execute(
            """SELECT stage, current_refcount, attempts
               FROM plan_items WHERE plan_id=? AND object_id=? AND version=?""",
            (plan_id, object_id, version),
        ).fetchone()
        if guard is None or guard[0] == ITEM_RECYCLED:
            return
        if guard[1] != 0:
            raise PlanNotExecutableError(
                f"{object_id} v{version} still has {guard[1]} live references"
            )
        if stage == ITEM_FAILED:
            # 单向状态机：FAILED -> RECYCLABLE 后再删除
            self.conn.execute(
                """UPDATE plan_items SET stage='RECYCLABLE', updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (now, plan_id, object_id, version),
            )

        record = self.conn.execute(
            """SELECT object_id, version, obj_type, storage_layer,
                      payload_ref, created_at
               FROM objects WHERE object_id=? AND version=?""",
            (object_id, version),
        ).fetchone()
        cols = ("object_id", "version", "obj_type", "storage_layer",
                "payload_ref", "created_at")
        record = dict(zip(cols, record))

        self.conn.execute(
            """INSERT INTO execution_log(plan_id, object_id, version,
                   action, ok, detail, at_ts)
               VALUES(?,?,?,'DELETE_ATTEMPT',1,?,?)""",
            (plan_id, object_id, version,
             f"attempt {guard[2] + 1}", now),
        )
        # 物理删除（幂等；崩溃恢复时返回 physical_delete=False）
        result = self.backend.delete(record)
        self._apply_recycled(plan_id, record, now, result, guard[2] + 1)

    def _apply_recycled(self, plan_id, record, now, result, attempt):
        object_id, version = record["object_id"], record["version"]
        snapshot_id = self.conn.execute(
            "SELECT snapshot_id FROM plans WHERE plan_id=?", (plan_id,)
        ).fetchone()[0]
        with self.conn:  # 条目级事务：墓碑/状态/日志/证据原子提交
            # 幂等保护：已回收则不重复推进
            lifecycle = self.conn.execute(
                "SELECT lifecycle FROM objects WHERE object_id=? AND version=?",
                (object_id, version),
            ).fetchone()[0]
            if lifecycle == OBJ_RECYCLED:
                return
            self.conn.execute(
                """UPDATE objects SET lifecycle='RECYCLED', recycled_at=?,
                       recycled_by_plan=?
                   WHERE object_id=? AND version=?""",
                (now, plan_id, object_id, version),
            )
            self.conn.execute(
                """INSERT INTO recycle_tombstones(object_id, version, plan_id,
                       recycled_at, former_layer, former_payload_ref)
                   VALUES(?,?,?,?,?,?)""",
                (object_id, version, plan_id, now,
                 record["storage_layer"], record["payload_ref"]),
            )
            self.conn.execute(
                """UPDATE plan_items SET stage='RECYCLED', attempts=?,
                       last_error=NULL, updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (attempt, now, plan_id, object_id, version),
            )
            self.conn.execute(
                """INSERT INTO execution_log(plan_id, object_id, version,
                       action, ok, detail, at_ts)
                   VALUES(?,?,?,'DELETE_OK',1,?,?)""",
                (plan_id, object_id, version,
                 f"physical_delete={result['physical_delete']}", now),
            )
            # 删除证明：删除瞬间引用计数为零、无水位/冻结阻断
            self.conn.execute(
                """INSERT INTO reference_evidence(plan_id, object_id,
                       version, evidence_kind, detail)
                   VALUES(?,?,?,?,?)""",
                (plan_id, object_id, version, EV_RECYCLE_PROOF,
                 f"deleted with zero live references under snapshot "
                 f"{snapshot_id}; physical_delete="
                 f"{result['physical_delete']}"),
            )
            self.conn.execute(
                """INSERT INTO audit_events(at_ts, event_type, plan_id,
                       object_id, version, detail)
                   VALUES(?,?,?,?,?,?)""",
                (now, "OBJECT_RECYCLED", plan_id, object_id, version,
                 f"layer={record['storage_layer']}"),
            )
            # 目录镜像变更序列：以 SQLite 提交的目录状态为准，与墓碑/状态
            # 同事务生效。后端文件删除本身不构成目录变更；幂等恢复路径
            # （上面 lifecycle 已为 RECYCLED 时提前 return）不会重复产生批次。
            catalog_row = catalog.read_catalog_row(
                self.conn, object_id, version)
            catalog.append_recycle_change(
                self.conn, catalog.next_batch_no(self.conn), now,
                catalog_row)
            self._cascade_refcounts(plan_id, object_id, version, now)

    def _cascade_refcounts(self, plan_id, object_id, version, now):
        """删除 X 后，X 所依赖的上游 Z 失去一个存活引用者，计数递减。"""
        parents = self.conn.execute(
            """SELECT e.dst_id, e.dst_version
               FROM lineage_edges e
               JOIN plans p ON p.snapshot_id = e.snapshot_id
               WHERE p.plan_id=? AND e.src_id=? AND e.src_version=?""",
            (plan_id, object_id, version),
        ).fetchall()
        for dst_id, dst_version in parents:
            row = self.conn.execute(
                """SELECT stage, current_refcount FROM plan_items
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (plan_id, dst_id, dst_version),
            ).fetchone()
            if row is None:
                continue  # 上游不在本计划（仍保留），无需推进
            stage, rc = row
            new_rc = max(0, rc - 1)
            if stage == ITEM_RECYCLED:
                continue
            self.conn.execute(
                """UPDATE plan_items SET current_refcount=?, updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (new_rc, now, plan_id, dst_id, dst_version),
            )
            # 级联解锁：计数归零且不再被任何存活对象到达
            if stage == ITEM_BLOCKED and new_rc == 0 and \
                    not self._still_reachable(dst_id, dst_version):
                self._unblock_to_recyclable(plan_id, dst_id, dst_version, now)

    def _still_reachable(self, object_id, version) -> bool:
        """当前 LIVE 对象沿 src->dst（下游到上游依赖）能否到达给定对象。"""
        row = self.conn.execute(
            """
            WITH RECURSIVE reach(oid, ver) AS (
                SELECT object_id, version FROM objects
                WHERE lifecycle='LIVE'
                  AND (object_id <> ? OR version <> ?)
                UNION
                SELECT e.dst_id, e.dst_version
                FROM reach r
                JOIN lineage_edges e
                  ON e.src_id = r.oid AND e.src_version = r.ver
                JOIN objects o
                  ON o.object_id=e.dst_id AND o.version=e.dst_version
                 AND o.lifecycle='LIVE'
            )
            SELECT 1 FROM reach WHERE oid=? AND ver=? LIMIT 1
            """,
            (object_id, version, object_id, version),
        ).fetchone()
        return row is not None

    def _unblock_to_recyclable(self, plan_id, object_id, version, now):
        next_batch = self.conn.execute(
            "SELECT COALESCE(MAX(batch_no),0)+1 FROM plan_batches "
            "WHERE plan_id=?",
            (plan_id,),
        ).fetchone()[0]
        self.conn.execute(
            "INSERT INTO plan_batches(plan_id, batch_no, item_count)"
            " VALUES(?,?,0)",
            (plan_id, next_batch),
        )
        self.conn.execute(
            """UPDATE plan_items SET stage='RECYCLABLE', block_reason=NULL,
                   batch_no=?, updated_at=?
               WHERE plan_id=? AND object_id=? AND version=?""",
            (next_batch, now, plan_id, object_id, version),
        )
        self.conn.execute(
            "UPDATE plan_batches SET item_count=item_count+1 "
            "WHERE plan_id=? AND batch_no=?",
            (plan_id, next_batch),
        )
        self.conn.execute(
            """INSERT INTO audit_events(at_ts, event_type, plan_id,
                   object_id, version, detail)
               VALUES(?,?,?,?,?,?)""",
            (now, "ITEM_UNBLOCKED", plan_id, object_id, version,
             f"cascade promoted to batch {next_batch}"),
        )

    # ------------------------------------------------------------ 失败
    def _record_failure(self, plan_id, object_id, version, exc):
        now = int(self._clock())
        with self.conn:
            self.conn.execute(
                """UPDATE plan_items SET stage='FAILED',
                       attempts=attempts+1, last_error=?, updated_at=?
                   WHERE plan_id=? AND object_id=? AND version=?""",
                (str(exc), now, plan_id, object_id, version),
            )
            self.conn.execute(
                """INSERT INTO execution_log(plan_id, object_id, version,
                       action, ok, detail, at_ts)
                   VALUES(?,?,?,'DELETE_FAILED',0,?,?)""",
                (plan_id, object_id, version, str(exc), now),
            )
            self.conn.execute(
                "UPDATE plans SET last_error=? WHERE plan_id=?",
                (f"{object_id} v{version}: {exc}", plan_id),
            )

    # ------------------------------------------------------------ 收尾
    def _finish_or_continue(self, plan_id):
        remaining = self.conn.execute(
            """SELECT COUNT(*) FROM plan_items
               WHERE plan_id=? AND stage IN ('RECYCLABLE','FAILED',
                                             'CANDIDATE')""",
            (plan_id,),
        ).fetchone()[0]
        if remaining == 0:
            self._set_status(plan_id, PLAN_COMPLETED)

    def _set_status(self, plan_id, status):
        self.conn.execute(
            "UPDATE plans SET status=? WHERE plan_id=?", (status, plan_id)
        )
        self.conn.commit()

    def _stats(self, plan_id, note=None):
        s = self.planner.plan_summary(plan_id)
        if note:
            s["note"] = note
        return s
