"""目录镜像：主库变更序列的生产端与盘点服务镜像的接收端。

背景
====
盘点服务需要把对象目录（``objects`` 表的目录字段，含已回收版本与回收
时间）留在自己的 SQLite 库里，离线按 ``(object_id, version)`` 查询。
本模块提供两种 Python 交付，不涉及网络服务或通用导出格式：

- **完整交付（首次交付）**：一份当前全量目录 + 来源标识 + 基准批次号，
  取自同一个已提交快照；接收端据此建立可查询镜像。
- **增量交付**：从调用方给定批次号之后开始，最多取调用方指定的批次数，
  不拆开批次；接收端无需连接主库即可推进。

主库侧
======
``schema`` 中新增两张变更表与一行来源标识：

- ``catalog_batches``：批次在写事务内按 ``MAX(batch_no)+1`` 分配，回滚
  不留痕，因此已提交批次在同一主库内按提交顺序从 1 连续编号。
- ``catalog_changes``：批次内每行保存该次写入后的**完整目录行**（不是
  diff），接收端直接 UPSERT；保留历史所需存储随写入量增长。
- ``catalog_source``：本库来源标识，建库/升级时写入，随重开保持。

只有两个目录写入口会产生批次：导入接口成功写入的新对象（IMPORT）与
回收执行器成功提交的目录状态变化（RECYCLE）。完全重复的导入、失败
回滚、没改变目录的重试都不产生批次；以 SQLite 提交的目录状态为准，
后端文件删除动作与日志不算目录变更。

镜像侧
======
- 空镜像必须先收到完整交付才能查询；
- 来源不同、缺批、批内缺行、与已应用且仍可重放的同编号批内容不符，
  一律拒绝且不推进；完全相同的已收批允许重发，交付可含已收前缀；
- 基准批次及之前不属于可重放范围；
- 一次交付的目录结果与进度在同一事务落地，写入中断/坏包后重开只能
  看到此前的完整视图；查询直接读镜像表，不重放日志。

性能
====
首次交付允许扫一次目录（keyset 分页，不把全部行放进 Python 列表）；
增量交付只按批次号做索引范围扫描，不回扫未变化的旧目录；很大的单次
导入同样按块传递、分段落盘。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

from .errors import MirrorNotInitializedError, ReclaimerError

# objects 表中属于“目录”的全部字段（不含对象文件本体；本系统不存文件）。
CATALOG_COLUMNS = (
    "object_id", "version", "obj_type", "classification", "environment",
    "storage_layer", "created_at", "lifecycle", "recycled_at",
    "recycled_by_plan", "payload_ref",
)
_CATALOG_SELECT = ", ".join(CATALOG_COLUMNS)


class CatalogDeliveryError(ReclaimerError):
    """交付内容违反镜像不变量（来源不符、缺批、缺行、摘要不符等）。

    抛出时镜像进度与目录均保持在此前已提交的视图。
    """


# ================================================================
# 规范编码与摘要
# ====================================================
def _canonical(row: tuple) -> bytes:
    """目录行的规范字节编码（列顺序固定，None/数字/文本均确定）。"""
    return json.dumps(list(row), ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")


def _digest_rows(rows) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update(_canonical(row))
    return h.hexdigest()


# ================================================================
# 主库写入口辅助（importer / executor 调用）
# ================================================================
def next_batch_no(conn: sqlite3.Connection) -> int:
    """在当前写事务内取下一个批次号（调用方稍后在同事务提交）。"""
    return conn.execute(
        "SELECT COALESCE(MAX(batch_no), 0) + 1 FROM catalog_batches"
    ).fetchone()[0]


def append_recycle_change(conn, batch_no: int, now: int,
                          catalog_row: tuple) -> None:
    """回收提交：记录单行 RECYCLE 批次（目录行取 SQLite 提交态）。"""
    conn.execute(
        "INSERT INTO catalog_batches(batch_no, change_kind, committed_at, "
        "row_count) VALUES(?, 'RECYCLE', ?, 1)",
        (batch_no, now),
    )
    conn.execute(
        "INSERT INTO catalog_changes(batch_no, seq_in_batch, "
        "object_id, version, obj_type, classification, environment, "
        "storage_layer, created_at, lifecycle, recycled_at, "
        "recycled_by_plan, payload_ref) "
        "VALUES(?,0,?,?,?,?,?,?,?,?,?,?,?)",
        (batch_no, *catalog_row),
    )


def read_catalog_row(conn, object_id: str, version: int) -> tuple:
    """读取 SQLite 中某版本当前的完整目录行（权威快照态）。"""
    row = conn.execute(
        f"SELECT {_CATALOG_SELECT} FROM objects "
        "WHERE object_id=? AND version=?",
        (object_id, version),
    ).fetchone()
    if row is None:  # pragma: no cover - 调用方保证行存在
        raise CatalogDeliveryError(
            f"catalog row vanished: {object_id} v{version}")
    return tuple(row)


# ================================================================
# 生产端：交付
# ================================================================
class _SnapshotDelivery:
    """在只读快照事务内产出交付的公共基类。

    文件库使用独立连接（不阻塞主连接写入，且快照在迭代期间保持）；
    ``:memory:`` 库的独立连接看不到数据，因此回退为复用主连接。
    """

    kind = "snapshot"

    def __init__(self, primary_conn: sqlite3.Connection,
                 db_path: str | None):
        self._owns_conn = db_path is None
        if db_path is None:
            self.conn = primary_conn
        else:
            self.conn = sqlite3.connect(db_path)
            self.conn.execute("PRAGMA foreign_keys=ON")
        # 显式开启只读事务，固定本次交付的已提交视图。快照在调用方
        # 完成全部迭代并显式 close 前保持（不能在 SELECT 进行中 COMMIT）；
        # 同连接（:memory:）场景下接收端与生产端共享连接，写操作需在
        # 交付关闭之后进行。
        self.conn.execute("BEGIN")
        self._closed = False

    def _end_snapshot(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.conn.execute("COMMIT")  # 结束只读快照
        except sqlite3.Error:  # pragma: no cover - 只读事务无写入可提交
            self.conn.execute("ROLLBACK")
        if not self._owns_conn:
            self.conn.close()

    def close(self) -> None:
        self._end_snapshot()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class FullDelivery(_SnapshotDelivery):
    """完整目录交付。

    ``header`` 含来源标识、基准批次号与总行数；:meth:`iter_chunks` 按
    keyset 分页流式产出目录行（调用方不会一次拿到全表）。完整迭代后
    :attr:`digest` 为全部行的规范摘要；源表在交付生成后继续变化不影响
    本交付（快照固定）。
    """

    kind = "full"

    def __init__(self, primary_conn, db_path, source_id: str):
        super().__init__(primary_conn, db_path)
        self.source_id = source_id
        self.baseline_batch = self.conn.execute(
            "SELECT COALESCE(MAX(batch_no), 0) FROM catalog_batches"
        ).fetchone()[0]
        self.row_count = self.conn.execute(
            "SELECT COUNT(*) FROM objects"
        ).fetchone()[0]
        self.header = {
            "kind": "full",
            "source_id": source_id,
            "baseline_batch": self.baseline_batch,
            "row_count": self.row_count,
        }
        self.digest: str | None = None

    def iter_chunks(self, chunk_size: int = 1000):
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        h = hashlib.sha256()
        last_oid: str | None = None
        last_ver: int | None = None
        emitted = 0
        while True:
            if last_oid is None:
                rows = self.conn.execute(
                    f"SELECT {_CATALOG_SELECT} FROM objects "
                    "ORDER BY object_id, version LIMIT ?",
                    (chunk_size,),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    f"SELECT {_CATALOG_SELECT} FROM objects "
                    "WHERE object_id > ? "
                    "   OR (object_id = ? AND version > ?) "
                    "ORDER BY object_id, version LIMIT ?",
                    (last_oid, last_oid, last_ver, chunk_size),
                ).fetchall()
            if not rows:
                break
            chunk = [tuple(r) for r in rows]
            for row in chunk:
                h.update(_canonical(row))
            emitted += len(chunk)
            last_oid, last_ver = chunk[-1][0], chunk[-1][1]
            yield chunk
        if emitted != self.row_count:  # 快照内目录不应变化，防御性校验
            raise CatalogDeliveryError(
                f"full delivery row drift: header={self.row_count} "
                f"emitted={emitted}")
        self.digest = h.hexdigest()


class IncrementalDelivery(_SnapshotDelivery):
    """增量交付：``(after_batch, after_batch + max_batches]`` 的完整批次。

    批次选择不拆批；单个大批内部可按 ``chunk_size`` 分块，每块标注
    ``end_of_batch`` 并在批末附带该批摘要。
    """

    kind = "incremental"

    def __init__(self, primary_conn, db_path, source_id: str,
                 after_batch: int, max_batches: int):
        super().__init__(primary_conn, db_path)
        if after_batch < 0:
            raise ValueError("after_batch must be >= 0")
        if max_batches < 1:
            raise ValueError("max_batches must be >= 1")
        self.source_id = source_id
        self.after_batch = after_batch
        batch_rows = self.conn.execute(
            "SELECT batch_no, change_kind, committed_at, row_count "
            "FROM catalog_batches WHERE batch_no > ? "
            "ORDER BY batch_no LIMIT ?",
            (after_batch, max_batches),
        ).fetchall()
        self.batches = [
            {"batch_no": bn, "change_kind": kind,
             "committed_at": committed_at, "row_count": row_count}
            for bn, kind, committed_at, row_count in batch_rows
        ]
        self.start_batch = (self.batches[0]["batch_no"]
                            if self.batches else after_batch)
        self.end_batch = (self.batches[-1]["batch_no"]
                          if self.batches else after_batch)
        self.total_rows = sum(b["row_count"] for b in self.batches)
        self.header = {
            "kind": "incremental",
            "source_id": source_id,
            "after_batch": after_batch,
            "start_batch": self.start_batch,
            "end_batch": self.end_batch,
            "total_rows": self.total_rows,
            "batches": list(self.batches),
        }

    def iter_chunks(self, chunk_size: int = 1000):
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        for meta in self.batches:
            bn = meta["batch_no"]
            h = hashlib.sha256()
            seen = 0
            while seen < meta["row_count"]:
                rows = self.conn.execute(
                    f"SELECT {_CATALOG_SELECT} FROM catalog_changes "
                    "WHERE batch_no=? AND seq_in_batch >= ? "
                    "ORDER BY seq_in_batch LIMIT ?",
                    (bn, seen, chunk_size),
                ).fetchall()
                if not rows:  # 批内行数不足即数据损坏
                    raise CatalogDeliveryError(
                        f"batch {bn} truncated in source: "
                        f"need {meta['row_count']}, got {seen}")
                chunk = [tuple(r) for r in rows]
                for row in chunk:
                    h.update(_canonical(row))
                seen += len(chunk)
                end_of_batch = seen == meta["row_count"]
                yield {
                    "batch_no": bn,
                    "rows": chunk,
                    "end_of_batch": end_of_batch,
                    "digest": h.hexdigest() if end_of_batch else None,
                }


class CatalogProducer:
    """主库侧交付工厂（由 :class:`reclaimer.api.Reclaimer` 持有）。"""

    def __init__(self, conn: sqlite3.Connection, db_path: str | None = None):
        self.conn = conn
        self.db_path = db_path

    def source_id(self) -> str:
        return self.conn.execute(
            "SELECT source_id FROM catalog_source WHERE id=1"
        ).fetchone()[0]

    def latest_batch_no(self) -> int:
        return self.conn.execute(
            "SELECT COALESCE(MAX(batch_no), 0) FROM catalog_batches"
        ).fetchone()[0]

    def open_full_delivery(self) -> FullDelivery:
        """首次/重基线交付：基准号与全量目录取自同一快照事务。"""
        return FullDelivery(self.conn, self.db_path, self.source_id())

    def open_incremental_delivery(self, after_batch: int,
                                  max_batches: int) -> IncrementalDelivery:
        """从 ``after_batch`` 之后开始的增量交付（整批不拆）。"""
        return IncrementalDelivery(
            self.conn, self.db_path, self.source_id(),
            after_batch, max_batches)


# ================================================================
# 接收端：镜像
# ================================================================
MIRROR_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

-- 可查询的当前目录：查询直接读本表，不重放任何日志。
CREATE TABLE IF NOT EXISTS mirror_catalog (
    object_id    TEXT NOT NULL,
    version      INTEGER NOT NULL,
    obj_type     TEXT NOT NULL,
    classification TEXT NOT NULL,
    environment  TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    lifecycle    TEXT NOT NULL,
    recycled_at  INTEGER,
    recycled_by_plan TEXT,
    payload_ref  TEXT,
    PRIMARY KEY (object_id, version)
);

-- 单行镜像元数据：initialized=0 表示空镜像，不得提供查询。
CREATE TABLE IF NOT EXISTS mirror_meta (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    source_id      TEXT,
    baseline_batch INTEGER NOT NULL DEFAULT 0,
    applied_batch  INTEGER NOT NULL DEFAULT 0,
    initialized    INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO mirror_meta(id) VALUES(1);

-- 已应用且仍可重放的批次（批次号 > baseline），用于重发核验。
CREATE TABLE IF NOT EXISTS mirror_batch_rows (
    batch_no     INTEGER NOT NULL,
    seq_in_batch INTEGER NOT NULL,
    object_id    TEXT NOT NULL,
    version      INTEGER NOT NULL,
    obj_type     TEXT NOT NULL,
    classification TEXT NOT NULL,
    environment  TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    lifecycle    TEXT NOT NULL,
    recycled_at  INTEGER,
    recycled_by_plan TEXT,
    payload_ref  TEXT,
    digest       TEXT NOT NULL,
    PRIMARY KEY (batch_no, seq_in_batch)
);

-- 落盘中的交付暂存：每次打开时清空，崩溃绝不污染已提交视图。
CREATE TABLE IF NOT EXISTS mirror_staging (
    staging_kind TEXT NOT NULL,      -- full / incremental
    batch_no     INTEGER NOT NULL,   -- full 固定为 0
    seq_in_batch INTEGER NOT NULL,
    object_id    TEXT NOT NULL,
    version      INTEGER NOT NULL,
    obj_type     TEXT NOT NULL,
    classification TEXT NOT NULL,
    environment  TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    lifecycle    TEXT NOT NULL,
    recycled_at  INTEGER,
    recycled_by_plan TEXT,
    payload_ref  TEXT
);
"""

_ROW_PARAMS = ", ".join("?" * len(CATALOG_COLUMNS))
_STAGING_COLS = ("staging_kind", "batch_no", "seq_in_batch",
                 *CATALOG_COLUMNS)


class CatalogMirror:
    """盘点服务侧的目录镜像（独立 SQLite 库，可跨进程重开）。"""

    def __init__(self, conn: sqlite3.Connection, write_fault=None):
        self.conn = conn
        # 测试钩子：每落一块暂存后调用，抛错即模拟写入中断
        self._write_fault = write_fault
        conn.executescript(MIRROR_SCHEMA_SQL)
        # 上次进程留下的暂存一律丢弃：已提交视图不受影响
        conn.execute("DELETE FROM mirror_staging")
        conn.commit()

    @classmethod
    def open(cls, db_path: str, write_fault=None) -> "CatalogMirror":
        return cls(sqlite3.connect(db_path), write_fault=write_fault)

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------ 状态
    def is_ready(self) -> bool:
        return self.conn.execute(
            "SELECT initialized FROM mirror_meta WHERE id=1"
        ).fetchone()[0] == 1

    def progress(self) -> dict:
        source_id, baseline, applied, initialized = self.conn.execute(
            "SELECT source_id, baseline_batch, applied_batch, initialized "
            "FROM mirror_meta WHERE id=1"
        ).fetchone()
        return {
            "initialized": bool(initialized),
            "source_id": source_id,
            "baseline_batch": baseline,
            "applied_batch": applied,
        }

    def object_status(self, object_id: str, version: int) -> dict:
        """三态查询：未登记 UNKNOWN / 存活 LIVE / 已回收 RECYCLED。

        空镜像（未收到首次完整交付）拒绝查询，避免伪造“不存在”。
        """
        if not self.is_ready():
            raise MirrorNotInitializedError(
                "mirror has no full catalog delivery yet; "
                "queries are not available")
        row = self.conn.execute(
            f"SELECT {_CATALOG_SELECT} FROM mirror_catalog "
            "WHERE object_id=? AND version=?",
            (object_id, version),
        ).fetchone()
        if row is None:
            return {"object_id": object_id, "version": version,
                    "status": "UNKNOWN",
                    "message": "object identity never registered in the "
                               "mirrored catalog"}
        result = dict(zip(CATALOG_COLUMNS, tuple(row)))
        if result["lifecycle"] == "RECYCLED":
            result["status"] = "RECYCLED"
            result["message"] = (
                f"object was recycled at {result['recycled_at']} "
                f"by plan {result['recycled_by_plan']}; former location: "
                f"layer={result['storage_layer']} ref={result['payload_ref']}")
        else:
            result["status"] = "LIVE"
            result["message"] = (
                f"object is live at layer={result['storage_layer']} "
                f"ref={result['payload_ref']}")
        return result

    # ------------------------------------------------------------ 接收完整交付
    def receive_full(self, delivery: FullDelivery,
                     chunk_size: int = 1000) -> dict:
        header = delivery.header
        if header.get("kind") != "full":
            raise CatalogDeliveryError("expected a full delivery")
        source_id = header["source_id"]
        baseline = int(header["baseline_batch"])
        meta = self.progress()
        if meta["initialized"]:
            if meta["source_id"] != source_id:
                raise CatalogDeliveryError(
                    f"source mismatch: mirror={meta['source_id']} "
                    f"delivery={source_id}")
            if baseline < meta["applied_batch"]:
                raise CatalogDeliveryError(
                    f"stale full delivery: baseline {baseline} < applied "
                    f"{meta['applied_batch']}")
        if baseline < 0:
            raise CatalogDeliveryError("baseline_batch must be >= 0")

        # 1) 流式落暂存（可分块，不要求一次性装入全部行）
        self._stage("full", delivery, chunk_size, expected_kind="full",
                    expected_count=header["row_count"])

        # 2) 单事务切换目录与进度：另一连接只能看到旧视图或新视图
        with self.conn:
            staged = self.conn.execute(
                "SELECT COUNT(*) FROM mirror_staging WHERE staging_kind='full'"
            ).fetchone()[0]
            if staged != header["row_count"]:
                raise CatalogDeliveryError(
                    f"full delivery incomplete: header="
                    f"{header['row_count']} staged={staged}")
            self.conn.execute("DELETE FROM mirror_catalog")
            self.conn.execute(
                f"INSERT INTO mirror_catalog({', '.join(CATALOG_COLUMNS)}) "
                f"SELECT {_CATALOG_SELECT} FROM mirror_staging "
                "WHERE staging_kind='full'"
            )
            # 重基线：旧重放历史全部落在新基准及之前，清空
            self.conn.execute("DELETE FROM mirror_batch_rows")
            self.conn.execute(
                "UPDATE mirror_meta SET source_id=?, baseline_batch=?, "
                "applied_batch=?, initialized=1 WHERE id=1",
                (source_id, baseline, baseline),
            )
            self.conn.execute("DELETE FROM mirror_staging")
        return {"status": "applied", "kind": "full",
                "source_id": source_id, "baseline_batch": baseline,
                "row_count": header["row_count"],
                "digest": delivery.digest}

    # ------------------------------------------------------------ 接收增量
    def receive_incremental(self, delivery: IncrementalDelivery,
                            chunk_size: int = 1000) -> dict:
        header = delivery.header
        if header.get("kind") != "incremental":
            raise CatalogDeliveryError("expected an incremental delivery")
        meta = self.progress()
        if not meta["initialized"]:
            raise MirrorNotInitializedError(
                "mirror requires a full delivery before increments")
        if meta["source_id"] != header["source_id"]:
            raise CatalogDeliveryError(
                f"source mismatch: mirror={meta['source_id']} "
                f"delivery={header['source_id']}")
        baseline = meta["baseline_batch"]
        applied = meta["applied_batch"]
        after = int(header["after_batch"])
        batches = header["batches"]

        if batches:
            # 交付窗口必须紧接 after_batch、内部连续、不缺批
            if header["start_batch"] != after + 1:
                raise CatalogDeliveryError(
                    f"delivery starts at {header['start_batch']} but "
                    f"after_batch={after} leaves a gap")
            expected_nos = list(range(header["start_batch"],
                                      header["end_batch"] + 1))
            actual_nos = [b["batch_no"] for b in batches]
            if actual_nos != expected_nos:
                raise CatalogDeliveryError(
                    f"non-consecutive batches: {actual_nos}")
            # 基准批次及之前不可重放：它们从未进入镜像，无法核验
            if header["start_batch"] <= baseline:
                raise CatalogDeliveryError(
                    f"batch {header['start_batch']} is at or before baseline "
                    f"{baseline}; historical batches are not replayable")
            if after != applied and after > applied:
                raise CatalogDeliveryError(
                    f"delivery after_batch {after} is ahead of applied "
                    f"{applied}: missing batches")
        elif after > applied:
            raise CatalogDeliveryError(
                f"empty delivery after_batch {after} exceeds applied "
                f"{applied}")

        # 1) 暂存并逐批核验行数与摘要（缺行/截断在此拒绝）
        self._stage("incremental", delivery, chunk_size,
                    expected_kind="incremental",
                    expected_count=header["total_rows"],
                    batch_metas={b["batch_no"]: b for b in batches})

        try:
            with self.conn:
                new_batches = []
                replayed = []
                for meta_b in batches:
                    bn = meta_b["batch_no"]
                    staged_rows = [
                        tuple(r) for r in self.conn.execute(
                            f"SELECT {_CATALOG_SELECT} FROM mirror_staging "
                            "WHERE staging_kind='incremental' AND batch_no=? "
                            "ORDER BY seq_in_batch",
                            (bn,))
                    ]
                    if len(staged_rows) != meta_b["row_count"]:
                        raise CatalogDeliveryError(
                            f"batch {bn} missing rows: need "
                            f"{meta_b['row_count']}, got {len(staged_rows)}")
                    if bn <= applied:
                        # 已收批重发：必须仍可重放且内容逐行一致
                        stored = [
                            tuple(r) for r in self.conn.execute(
                                f"SELECT {_CATALOG_SELECT} "
                                "FROM mirror_batch_rows WHERE batch_no=? "
                                "ORDER BY seq_in_batch",
                                (bn,))
                        ]
                        if not stored:
                            raise CatalogDeliveryError(
                                f"batch {bn} is not replayable "
                                f"(at or before baseline {baseline})")
                        if stored != staged_rows:
                            raise CatalogDeliveryError(
                                f"replayed batch {bn} content differs from "
                                "the already applied batch")
                        replayed.append(bn)
                    else:
                        if bn != applied + 1:
                            raise CatalogDeliveryError(
                                f"gap before new batch {bn}: applied={applied}")
                        new_batches.append((bn, meta_b, staged_rows))
                        applied = bn

                # 2) 新批 UPSERT 目录、登记可重放行、推进进度——同一事务
                for bn, meta_b, rows in new_batches:
                    digest = _digest_rows(rows)
                    for seq, row in enumerate(rows):
                        self.conn.execute(
                            f"INSERT INTO mirror_catalog("
                            f"{', '.join(CATALOG_COLUMNS)}) "
                            f"VALUES({_ROW_PARAMS}) "
                            "ON CONFLICT(object_id, version) DO UPDATE SET "
                            "obj_type=excluded.obj_type, "
                            "classification=excluded.classification, "
                            "environment=excluded.environment, "
                            "storage_layer=excluded.storage_layer, "
                            "created_at=excluded.created_at, "
                            "lifecycle=excluded.lifecycle, "
                            "recycled_at=excluded.recycled_at, "
                            "recycled_by_plan=excluded.recycled_by_plan, "
                            "payload_ref=excluded.payload_ref",
                            row,
                        )
                        self.conn.execute(
                            "INSERT INTO mirror_batch_rows(batch_no, "
                            "seq_in_batch, object_id, version, obj_type, "
                            "classification, environment, storage_layer, "
                            "created_at, lifecycle, recycled_at, "
                            "recycled_by_plan, payload_ref, digest) "
                            f"VALUES(?,?,{_ROW_PARAMS},?)",
                            (bn, seq, *row, digest),
                        )
                if new_batches:
                    self.conn.execute(
                        "UPDATE mirror_meta SET applied_batch=? WHERE id=1",
                        (applied,),
                    )
                self.conn.execute(
                    "DELETE FROM mirror_staging "
                    "WHERE staging_kind='incremental'")
        except Exception:
            # 任何核验失败：事务回滚，目录与进度保持旧视图，丢弃暂存
            self.conn.execute(
                "DELETE FROM mirror_staging WHERE staging_kind='incremental'")
            self.conn.commit()
            raise
        return {
            "status": "applied",
            "kind": "incremental",
            "source_id": header["source_id"],
            "start_batch": header["start_batch"],
            "end_batch": header["end_batch"],
            "replayed_batches": replayed,
            "new_batches": [bn for bn, _, _ in new_batches],
            "applied_batch": applied,
        }

    # ------------------------------------------------------------ 暂存
    def _stage(self, kind: str, delivery, chunk_size: int, *,
               expected_kind: str, expected_count: int,
               batch_metas=None) -> None:
        """把交付流式写入暂存区，并做行数/摘要完整性核验。"""
        conn = self.conn
        conn.execute("DELETE FROM mirror_staging WHERE staging_kind=?",
                     (kind,))
        conn.commit()
        try:
            if expected_kind == "full":
                got = 0
                h = hashlib.sha256()
                for chunk in delivery.iter_chunks(chunk_size):
                    conn.executemany(
                        f"INSERT INTO mirror_staging("
                        f"{', '.join(_STAGING_COLS)}) "
                        f"VALUES('full', 0, ?, {_ROW_PARAMS})",
                        [(got + i, *row) for i, row in enumerate(chunk)],
                    )
                    for row in chunk:
                        h.update(_canonical(row))
                    got += len(chunk)
                    conn.commit()
                    if self._write_fault is not None:
                        self._write_fault(kind, got)
                if got != expected_count:
                    raise CatalogDeliveryError(
                        f"full delivery truncated: header={expected_count} "
                        f"received={got}")
                if delivery.digest != h.hexdigest():
                    raise CatalogDeliveryError(
                        "full delivery digest mismatch: content changed or "
                        "corrupted")
            else:
                got_total = 0
                per_batch_count: dict[int, int] = {}
                per_batch_digest: dict[int, str] = {}
                for piece in delivery.iter_chunks(chunk_size):
                    bn = piece["batch_no"]
                    if bn not in (batch_metas or {}):
                        raise CatalogDeliveryError(
                            f"unexpected batch {bn} in delivery stream")
                    base = per_batch_count.get(bn, 0)
                    rows = piece["rows"]
                    conn.executemany(
                        f"INSERT INTO mirror_staging("
                        f"{', '.join(_STAGING_COLS)}) "
                        f"VALUES('incremental', ?, ?, {_ROW_PARAMS})",
                        [(bn, base + i, *row) for i, row in enumerate(rows)],
                    )
                    per_batch_count[bn] = base + len(rows)
                    got_total += len(rows)
                    if piece["end_of_batch"]:
                        per_batch_digest[bn] = piece["digest"]
                    conn.commit()
                    if self._write_fault is not None:
                        self._write_fault(kind, got_total)
                if got_total != expected_count:
                    raise CatalogDeliveryError(
                        f"incremental delivery truncated: header="
                        f"{expected_count} received={got_total}")
                for bn, meta_b in (batch_metas or {}).items():
                    count = per_batch_count.get(bn, 0)
                    if count != meta_b["row_count"]:
                        raise CatalogDeliveryError(
                            f"batch {bn} missing rows: need "
                            f"{meta_b['row_count']}, got {count}")
                    if bn not in per_batch_digest:
                        raise CatalogDeliveryError(
                            f"batch {bn} did not reach end-of-batch marker")
                    stored = [
                        tuple(r) for r in conn.execute(
                            f"SELECT {_CATALOG_SELECT} FROM mirror_staging "
                            "WHERE staging_kind='incremental' AND batch_no=? "
                            "ORDER BY seq_in_batch",
                            (bn,))
                    ]
                    if _digest_rows(stored) != per_batch_digest[bn]:
                        raise CatalogDeliveryError(
                            f"batch {bn} digest mismatch: content changed or "
                            "corrupted")
        except Exception:
            conn.execute("DELETE FROM mirror_staging WHERE staging_kind=?",
                         (kind,))
            conn.commit()
            raise
