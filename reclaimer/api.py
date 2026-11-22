"""对外门面：谱系导入、策略编译、计划生成、预检、执行、审计查询、
质量隔离视图、存储费用结算。

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

    bill = r.settle_storage_costs(
        from_ts=..., to_ts=..., layer_amounts={"hot": 1234},
        metering=iter([...]), attributions=iter([...]))
"""

from __future__ import annotations

import sqlite3
import time

from .audit import Audit
from .catalog import CatalogMirror, CatalogProducer
from .errors import PlanPausedError
from .executor import Executor, MemoryBackend, StorageBackend
from .importer import Importer
from .planner import Planner
from .precheck import Precheck
from .quality import QualityView
from .schema import initialize
from .settlement import Settlement


class Reclaimer:
    def __init__(self, conn: sqlite3.Connection,
                 backend: StorageBackend | None = None,
                 clock=time.time, db_path: str | None = None):
        self.conn = conn
        self._clock = clock
        # 文件库路径用于打开独立只读快照连接；:memory: 库为 None，
        # 交付在同一连接的只读事务内产出。
        self.db_path = db_path
        initialize(conn)
        self.importer = Importer(conn, clock=clock)
        self.planner = Planner(conn, clock=clock)
        self.prechecker = Precheck(conn, self.planner, clock=clock)
        self.backend = backend or MemoryBackend()
        self.executor = Executor(conn, self.backend, self.planner,
                                 clock=clock)
        self.audit = Audit(conn, backend=self.backend)
        self.quality = QualityView(conn, clock=clock)
        self.settlement = Settlement(conn)
        self.catalog_producer = CatalogProducer(conn, db_path=db_path)

    # ------------------------------------------------------------ 工厂
    @classmethod
    def open(cls, db_path: str, backend: StorageBackend | None = None,
             clock=time.time) -> "Reclaimer":
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        return cls(conn, backend=backend, clock=clock, db_path=db_path)

    @classmethod
    def in_memory(cls, backend: StorageBackend | None = None,
                  clock=time.time) -> "Reclaimer":
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys=ON")
        return cls(conn, backend=backend, clock=clock, db_path=None)

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

    # ============================================================ 7. 质量隔离
    def register_quality_view(self, snapshot_id, objects, mappings=()) -> dict:
        """为已导入快照登记固定分区映射。

        ``objects``: ``{"object_id", "version", "partition_count"}`` 列表；
        ``mappings``: ``{"src_id", "src_version", "dst_id", "dst_version",
        "read": [start, end], "out": [start, end]}`` 列表。``src`` 依赖
        ``dst``（库内既有方向），坏数从 ``dst`` 传给 ``src``。
        """
        return self.quality.register(snapshot_id, objects, mappings)

    def report_quality_fault(self, snapshot_id, fault_id, object_id,
                             version, range_start, range_end) -> dict:
        """提交一条带编号的报障，返回受影响节点与本次更新量。"""
        return self.quality.report(
            snapshot_id, fault_id, object_id, version, range_start, range_end)

    def release_quality_fault(self, snapshot_id, fault_id,
                              release_ref=None) -> dict:
        """按编号解除报障；重复解除幂等。"""
        return self.quality.release(
            snapshot_id, fault_id, release_ref=release_ref)

    def quality_ranges(self, snapshot_id, object_id, version) -> dict:
        """查询供数时必须避开的分区区间（三态：AVAILABLE/RECYCLED/NO_VIEW）。"""
        return self.quality.isolated_ranges(snapshot_id, object_id, version)

    def quality_faults(self, snapshot_id, only_unreleased=True) -> list:
        return self.quality.faults(snapshot_id,
                                   only_unreleased=only_unreleased)

    # ============================================================ 8. 结算
    def settle_storage_costs(self, *, from_ts, to_ts, layer_amounts,
                             metering, attributions) -> dict:
        """把供应商按层给出的总费用拆到成本中心，返回一份账单。

        - ``from_ts`` / ``to_ts``：结算期 ``[from_ts, to_ts)``（整秒）；
        - ``layer_amounts``：每层应付的非负整数分（映射或 (层, 分) 对）；
        - ``metering``：无序计量记录 ``(storage_layer, payload_ref,
          start, end, bytes)``，允许一次性迭代器；
        - ``attributions``：``(object_id, version, cost_center)``，
          允许一次性迭代器。

        返回 ``{"items": [...], "layers": [...], ...}``：每个有计量份额
        的分摊项给出精确份额（``Fraction``）与整数分；同层各项之和恒
        等于该层应付金额。任何输入不变量被违反都抛
        :class:`reclaimer.errors.SettlementError`，不返回半张账单，
        也不改写业务表。详见 ``reclaimer.settlement`` 模块文档。
        """
        return self.settlement.settle(
            from_ts=from_ts, to_ts=to_ts, layer_amounts=layer_amounts,
            metering=metering, attributions=attributions)

    # ============================================================ 9. 目录镜像交付
    def catalog_source_id(self) -> str:
        """本主库随重开保持的来源标识。"""
        return self.catalog_producer.source_id()

    def catalog_latest_batch(self) -> int:
        """主库当前最大已提交目录批次号（无变更时为 0）。"""
        return self.catalog_producer.latest_batch_no()

    def open_full_catalog_delivery(self):
        """完整目录交付（首次交付/重基线）。

        返回上下文管理器：header 含来源标识、基准批次号、总行数；
        ``iter_chunks(chunk_size)`` 流式产出目录行并在迭代完成后给出
        摘要。基准号与全量目录取自同一个已提交快照。
        """
        return self.catalog_producer.open_full_delivery()

    def open_incremental_catalog_delivery(self, after_batch: int,
                                          max_batches: int):
        """增量交付：批次号 ``(after_batch, after_batch+max_batches]``。

        整批不拆；返回对象的 header 注明来源、起止进度与各批记录数，
        ``iter_chunks`` 可把大批切块，块末带批摘要。
        """
        return self.catalog_producer.open_incremental_delivery(
            after_batch, max_batches)

    @staticmethod
    def open_catalog_mirror(db_path: str, write_fault=None) -> CatalogMirror:
        """打开（或创建）盘点服务侧的目录镜像库。"""
        return CatalogMirror.open(db_path, write_fault=write_fault)


__all__ = [
    "Reclaimer", "MemoryBackend", "StorageBackend", "PlanPausedError",
]
