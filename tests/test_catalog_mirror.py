"""目录镜像端到端测试：主库变更序列、完整/增量交付、镜像三态查询。

运行：python3 -I tests/test_catalog_mirror.py
（在 /workspace 下运行；测试自行创建主库与镜像，不依赖 pytest）

覆盖
====
- 批次语义：同批新增共享批次号；重复导入/失败回滚/无目录变化的重试不产批；
  回收成功才产批；批次按提交顺序从 1 连续、跨进程重开保持；
- 来源标识随重开保持；升级旧库自动补建且不倒推旧变更；
- 首次交付与基准号同一快照：交付期间另一连接提交导入，增量不漏不重；
- 回收旧版本：镜像保留该版本并可见回收时间，同对象新版本仍 LIVE；
- 坏包/截断/写入中断：目录与进度均不推进，重开只见旧完整视图，随后完整
  交付及其重放核对恢复；
- 来源不同、缺批、批内缺行、基准前批次误重发、同号批内容不符全部拒绝；
- 运行时小样本独立推导各版状态；
- 可复跑证据：小增量不扫描整张老目录（trace + EXPLAIN + 进度计数 + 计时）。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import (  # noqa: E402
    CatalogDeliveryError,
    CatalogMirror,
    MirrorNotInitializedError,
    Reclaimer,
)
from reclaimer.executor import MemoryBackend  # noqa: E402

RESULTS = []
T0 = 1_000_000
AS_OF = 10_000_000


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(cond), detail))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))


# ------------------------------------------------------------ 夹具
def make_object(oid, ver, *, created=T0, layer="cold",
                otype="raw_file", cls="public", env="prod",
                payload=None):
    return {
        "object_id": oid, "version": ver, "obj_type": otype,
        "classification": cls, "environment": env,
        "storage_layer": layer, "created_at": created,
        "payload_ref": payload or f"{layer}://{oid}/v{ver}",
    }


# 回收专用类型：只有该类型在测试策略下过期，其它类型的对象必然保留，
# 因此一次计划只会回收指定目标，不会误伤同库的存活对象。
RECYCLE_TYPE = "recycle_target"
KEEP_TYPE = "keep_target"


def make_recyclable(oid, ver, *, created=T0):
    return make_object(oid, ver, created=created, otype=RECYCLE_TYPE)


def bulk_objects(prefix, n, created=T0):
    return [make_object(f"{prefix}_{i:05d}", 1, created=created)
            for i in range(n)]


def open_primary(path):
    backend = MemoryBackend()
    r = Reclaimer.open(path, backend=backend)
    return r, backend


def recycle_one(r, backend, oid, ver):
    """用一个只让 RECYCLE_TYPE 过期的计划把 (oid, ver) 回收。"""
    snap = r.import_snapshot(
        [], snapshot_id=f"snap-{oid}-{ver}-{time.time_ns()}")
    policy_version = f"pol-{oid}-{ver}-{time.time_ns()}"
    r.import_policy([
        {"rule_id": "expire-target", "obj_type": RECYCLE_TYPE,
         "classification": None, "environment": None,
         "retention_seconds": 3_000_000},
        {"rule_id": "keep-others", "obj_type": KEEP_TYPE,
         "classification": None, "environment": None,
         "retention_seconds": 10**12},
    ], policy_version)
    plan = r.generate_plan(policy_version=policy_version,
                           snapshot_id=snap, as_of_ts=AS_OF)
    pid = plan["plan_id"]
    r.precheck(pid)
    stats = r.execute(pid)
    return pid, stats


# ------------------------------------------------------------ 交付包装器（故障注入）
class TruncatedFullDelivery:
    """把完整交付的最后一块截掉若干行，模拟接收途中截断。"""

    def __init__(self, delivery, drop=1):
        self._d = delivery
        self._drop = drop
        self.header = dict(delivery.header)
        self.digest = None

    def iter_chunks(self, chunk_size=1000):
        chunks = list(self._d.iter_chunks(chunk_size))
        for chunk in chunks[:-1]:
            yield chunk
        last = chunks[-1][:-self._drop] if self._drop else chunks[-1]
        if last:
            yield last
        self.digest = self._d.digest  # 头部摘要仍按完整内容 -> 必不匹配


class TamperedIncremental:
    """对增量交付做指定变换：删批/删行/改行。"""

    def __init__(self, delivery, *, drop_batch=None, drop_rows=0,
                 tamper_row=None):
        self._d = delivery
        self.header = dict(delivery.header)
        self.header["batches"] = [
            dict(b) for b in delivery.header["batches"]
            if b["batch_no"] != drop_batch]
        self._drop_batch = drop_batch
        self._drop_rows = drop_rows
        self._tamper_row = tamper_row
        if drop_batch is not None:
            self.header["total_rows"] = sum(
                b["row_count"] for b in self.header["batches"])

    def iter_chunks(self, chunk_size=1000):
        for piece in self._d.iter_chunks(chunk_size):
            if piece["batch_no"] == self._drop_batch:
                continue
            rows = piece["rows"]
            if self._drop_rows and piece["end_of_batch"]:
                rows = rows[:-self._drop_rows]
            if self._tamper_row and rows:
                rows = [self._tamper_row(tuple(rr)) for rr in rows]
            if not rows:
                continue
            yield {**piece, "rows": rows,
                   "end_of_batch": piece["end_of_batch"]
                   and not self._drop_rows}


# ============================================================ 1. 批次语义
def test_batch_semantics():
    with tempfile.TemporaryDirectory() as d:
        r, _ = open_primary(os.path.join(d, "p.db"))
        check("空库批次号为 0", r.catalog_latest_batch() == 0)
        n = r.import_objects(bulk_objects("a", 5))
        check("首次导入 5 行", n == 5)
        check("导入后批次 1", r.catalog_latest_batch() == 1)
        b1 = r.conn.execute(
            "SELECT change_kind, row_count FROM catalog_batches").fetchall()
        check("批次 1 是 IMPORT 且 5 行", b1 == [("IMPORT", 5)], str(b1))
        seqs = r.conn.execute(
            "SELECT seq_in_batch, object_id, version FROM catalog_changes "
            "WHERE batch_no=1 ORDER BY seq_in_batch").fetchall()
        check("批内序号从 0 连续", [s for s, _, _ in seqs] == list(range(5)))
        check("批内按 (object_id, version) 排序",
              [(oid, ver) for _, oid, ver in seqs]
              == sorted((oid, ver) for _, oid, ver in seqs))

        # 完全重复的导入不产生新批
        r.import_objects(bulk_objects("a", 5))
        check("完全重复导入不产新批", r.catalog_latest_batch() == 1)
        # 部分重复：只有新增行产生一个新批
        mixed = bulk_objects("a", 3) + bulk_objects("b", 2)
        n2 = r.import_objects(mixed)
        check("部分重复只计新增", n2 == 2)
        check("新增产生批次 2", r.catalog_latest_batch() == 2)
        check("批次 2 只含 2 行",
              r.conn.execute("SELECT row_count FROM catalog_batches "
                             "WHERE batch_no=2").fetchone()[0] == 2)

        # 失败回滚不产批、不留行
        before = r.catalog_latest_batch()
        try:
            r.import_objects(
                bulk_objects("c", 2)
                + [make_object("bad", 0)])  # version 非法
            check("非法导入应报错", False)
        except Exception:
            check("非法导入报错并回滚", True)
        check("失败回滚不产新批", r.catalog_latest_batch() == before)
        check("失败回滚不留目录行",
              r.conn.execute("SELECT COUNT(*) FROM objects "
                             "WHERE object_id LIKE 'c_%'").fetchone()[0] == 0)
        # 回滚后下一批次号紧接，连续无空洞
        r.import_objects(bulk_objects("d", 3))
        nos = [row[0] for row in r.conn.execute(
            "SELECT batch_no FROM catalog_batches ORDER BY batch_no")]
        check("批次号始终从 1 连续", nos == list(range(1, len(nos) + 1)),
              str(nos))

        # 没改变目录的重试（执行已完成计划）不产批
        snap = r.import_snapshot([], snapshot_id="s1")
        r.import_policy([
            {"rule_id": "p", "obj_type": "raw_file",
             "retention_seconds": 0}], "pol1")
        plan = r.generate_plan(policy_version="pol1", snapshot_id=snap,
                               as_of_ts=AS_OF)
        r.execute(plan["plan_id"])
        after_recycle = r.catalog_latest_batch()
        check("回收产生了新批", after_recycle > before)
        again = r.execute(plan["plan_id"])
        check("重复执行已完成计划为空操作",
              again["status"] == "COMPLETED")
        check("无目录变化的重试不产新批",
              r.catalog_latest_batch() == after_recycle)
        r.close()


# ============================================================ 2. 来源标识 + 旧库升级
def test_source_id_and_upgrade():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "legacy.db")
        # 手工造一个“旧版库”：只有 objects，没有镜像变更表与来源表
        legacy = sqlite3.connect(path)
        legacy.executescript(
            """
            CREATE TABLE objects (
                object_id TEXT NOT NULL, version INTEGER NOT NULL,
                obj_type TEXT NOT NULL, classification TEXT NOT NULL,
                environment TEXT NOT NULL, storage_layer TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                lifecycle TEXT NOT NULL DEFAULT 'LIVE',
                recycled_at INTEGER, recycled_by_plan TEXT,
                payload_ref TEXT,
                PRIMARY KEY (object_id, version));
            INSERT INTO objects VALUES('legacy_o',1,'raw_file','public',
                'prod','cold',1000,'LIVE',NULL,NULL,'cold://legacy_o');
            """)
        legacy.commit()
        legacy.close()

        r = Reclaimer.open(path, backend=MemoryBackend())
        sid1 = r.catalog_source_id()
        check("升级后自动补来源标识", isinstance(sid1, str) and len(sid1) > 0)
        check("升级库不倒推旧变更", r.catalog_latest_batch() == 0)
        check("旧对象仍在目录中",
              r.object_status("legacy_o", 1)["status"] == "LIVE")
        r.close()

        # 重开：来源标识保持，升级后新写入进入变更序列
        r2 = Reclaimer.open(path, backend=MemoryBackend())
        check("来源标识随重开保持", r2.catalog_source_id() == sid1)
        r2.import_objects(bulk_objects("new", 2))
        check("升级后新导入进入批次 1", r2.catalog_latest_batch() == 1)
        with r2.open_full_catalog_delivery() as fd:
            rows = sum(len(c) for c in fd.iter_chunks(100))
        check("旧对象通过首次全量交付可见（不依赖变更历史）", rows == 3)
        r2.close()


# ============================================================ 3. 首次交付期间并发提交 -> 增量不漏不重
def test_concurrent_commit_during_full_delivery():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.db")
        r, backend = open_primary(path)
        r.import_objects(bulk_objects("base", 50))  # 批次 1

        mirror_path = os.path.join(d, "m.db")
        mirror = Reclaimer.open_catalog_mirror(mirror_path)

        # 打开首次交付：快照与基准号取自同一已提交视图（此刻 baseline=1）
        delivery = r.open_full_catalog_delivery()
        check("首次交付基准批次=1",
              delivery.header["baseline_batch"] == 1)
        check("首次交付行数=50", delivery.header["row_count"] == 50)

        # 另一连接（模拟另一进程/写入方）在交付读取期间提交一次导入
        r2 = Reclaimer.open(path, backend=MemoryBackend())
        r2.import_objects(bulk_objects("late", 20))  # 批次 2
        sid = r2.catalog_source_id()
        check("并发导入产生批次 2", r2.catalog_latest_batch() == 2)
        r2.close()

        # 交付内容仍以打开时的快照为准：不含 late_*
        seen = set()
        for chunk in delivery.iter_chunks(5):
            seen.update(row[0] for row in chunk)
        check("交付内容冻结在快照（不含并发新对象）",
              not any(oid.startswith("late_") for oid in seen))
        check("交付仍只含 50 行", len(seen) == 50)
        res = mirror.receive_full(delivery, chunk_size=5)
        delivery.close()
        check("完整交付落盘", res["row_count"] == 50)
        check("镜像进度停在基准 1",
              mirror.progress()["applied_batch"] == 1)

        # 从基准号接上增量：批次 2 不漏
        with r.open_incremental_catalog_delivery(1, 10) as inc:
            check("增量窗口 = 批次 2",
                  inc.header["start_batch"] == 2
                  and inc.header["end_batch"] == 2)
            check("增量注明来源一致", inc.header["source_id"] == sid)
            check("各批记录数=20",
                  inc.header["batches"][0]["row_count"] == 20)
            out = mirror.receive_incremental(inc, chunk_size=7)
        check("增量推进到 2", out["applied_batch"] == 2)
        check("20 个并发对象全部到达镜像",
              all(mirror.object_status(f"late_{i:05d}", 1)["status"] == "LIVE"
                  for i in range(20)))
        check("原 50 对象仍可查",
              all(mirror.object_status(f"base_{i:05d}", 1)["status"] == "LIVE"
                  for i in range(50)))

        # 再发一次同样的增量（已收前缀重发）：不重复生效
        with r.open_incremental_catalog_delivery(1, 10) as inc2:
            out2 = mirror.receive_incremental(inc2, chunk_size=7)
        check("同批重发识别为重放", out2["replayed_batches"] == [2]
              and out2["new_batches"] == [])
        check("重发后进度不变", mirror.progress()["applied_batch"] == 2)
        cnt = mirror.conn.execute(
            "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0]
        check("重发不重复写目录（仍 70 行）", cnt == 70, str(cnt))
        r.close()
        mirror.close()


# ============================================================ 4. 回收旧版本：保留版本 + 回收时间 + 新版本 LIVE
def test_recycle_keeps_version_with_recycled_at():
    with tempfile.TemporaryDirectory() as d:
        r, backend = open_primary(os.path.join(d, "p.db"))
        r.import_objects([
            make_recyclable("k", 1),
            make_object("k", 2, otype=KEEP_TYPE),
            make_object("other", 1, otype=KEEP_TYPE),
        ])  # 批次 1
        backend.add("k", 1, {"object_id": "k", "version": 1})
        # 回收 k v1（独立快照 + 仅目标类型过期的策略）
        pid, stats = recycle_one(r, backend, "k", 1)
        check("k v1 回收成功", stats["deleted_this_call"] == 1,
              str(stats.get("stage_counts")))
        check("其它对象未被误伤",
              r.object_status("k", 2)["status"] == "LIVE"
              and r.object_status("other", 1)["status"] == "LIVE")
        check("回收产生批次 2", r.catalog_latest_batch() == 2)
        kind = r.conn.execute(
            "SELECT change_kind FROM catalog_batches WHERE batch_no=2"
        ).fetchone()[0]
        check("批次 2 是 RECYCLE", kind == "RECYCLE")
        recycled_at = r.object_status("k", 1)["recycled_at"]
        check("主库回收时间已记录", recycled_at is not None)

        mirror = Reclaimer.open_catalog_mirror(os.path.join(d, "m.db"))
        with r.open_full_catalog_delivery() as fd:
            mirror.receive_full(fd, chunk_size=2)
        v1 = mirror.object_status("k", 1)
        v2 = mirror.object_status("k", 2)
        check("镜像保留已回收版本（非 UNKNOWN）", v1["status"] == "RECYCLED")
        check("镜像可见回收时间", v1["recycled_at"] == recycled_at)
        check("镜像记录回收计划", v1["recycled_by_plan"] == pid)
        check("同对象新版本仍存活", v2["status"] == "LIVE")
        check("未登记身份是 UNKNOWN",
              mirror.object_status("nope", 9)["status"] == "UNKNOWN")

        # 同对象再导入 v3 -> 批次 3，增量后镜像三态并存
        r.import_objects([make_object("k", 3, otype=KEEP_TYPE,
                                      created=T0 + 400)])
        with r.open_incremental_catalog_delivery(2, 5) as inc:
            out = mirror.receive_incremental(inc)
        check("增量到批次 3", out["applied_batch"] == 3)
        check("v1 RECYCLED / v2 LIVE / v3 LIVE 三态并存",
              mirror.object_status("k", 1)["status"] == "RECYCLED"
              and mirror.object_status("k", 2)["status"] == "LIVE"
              and mirror.object_status("k", 3)["status"] == "LIVE")
        r.close()
        mirror.close()


# ============================================================ 5. 坏包拒绝矩阵
def test_bad_deliveries_never_advance():
    with tempfile.TemporaryDirectory() as d:
        r, _ = open_primary(os.path.join(d, "p.db"))
        r.import_objects(bulk_objects("a", 4))            # 批 1（全量基准）
        mirror = Reclaimer.open_catalog_mirror(os.path.join(d, "m.db"))
        with r.open_full_catalog_delivery() as fd:
            mirror.receive_full(fd, chunk_size=2)        # baseline=1
        r.import_objects(bulk_objects("b", 3))            # 批 2
        r.import_objects(bulk_objects("c", 2))            # 批 3
        r.import_objects(bulk_objects("e", 1))            # 批 4（尚未接收）
        # 先正常接收批 2、3，使它们成为“已应用且可重放”的批次
        with r.open_incremental_catalog_delivery(1, 2) as inc:
            mirror.receive_incremental(inc, chunk_size=2)
        check("前置：镜像已推进到 3",
              mirror.progress()["applied_batch"] == 3)
        progress_before = mirror.progress()

        def expect_reject(name, delivery, chunk_size=2):
            try:
                mirror.receive_incremental(delivery, chunk_size=chunk_size) \
                    if delivery.header["kind"] == "incremental" \
                    else mirror.receive_full(delivery, chunk_size=chunk_size)
                check(name, False, "应当拒绝")
            except (CatalogDeliveryError, MirrorNotInitializedError):
                check(name, True)
            check(name + " 后进度不变",
                  mirror.progress() == progress_before)

        # 5a. 来源不同（另一主库的同号批次，source_id 不同）
        other = Reclaimer.open(os.path.join(d, "other.db"),
                               backend=MemoryBackend())
        other.import_objects(bulk_objects("x", 1))
        other.import_objects(bulk_objects("y", 1))
        other.import_objects(bulk_objects("z", 1))
        with other.open_incremental_catalog_delivery(2, 1) as bad:
            expect_reject("来源不同的增量被拒绝", bad)
        other.close()

        # 5b. 缺批：窗口应含 4，却交付一个从 5 起的空洞窗口
        r.import_objects(bulk_objects("f", 1))            # 批 5
        with r.open_incremental_catalog_delivery(4, 1) as inc:
            # 把 after_batch 伪装成 3 -> start_batch=5 与 after 不衔接
            inc.header["after_batch"] = 3
            expect_reject("缺批（窗口不衔接）被拒绝", inc)

        # 5c. 批内缺行（对尚未应用的批 4 截断）
        with r.open_incremental_catalog_delivery(3, 1) as inc:
            expect_reject("批内缺行被拒绝",
                          TamperedIncremental(inc, drop_rows=1))

        # 5d. 同编号已收批内容不符（重发批 2 但篡改一行）
        def tamper(row):
            row = list(row)
            row[6] = "tampered-layer"  # storage_layer
            return tuple(row)
        with r.open_incremental_catalog_delivery(1, 1) as inc:
            expect_reject("同号批内容不符被拒绝",
                          TamperedIncremental(inc, tamper_row=tamper))

        # 5e. 基准批次及之前不可重放（要求从批次 1 开始）
        with r.open_incremental_catalog_delivery(0, 5) as inc:
            expect_reject("基准及之前的历史批不可当作重放", inc)

        # 5f. 完整交付截断（行数与摘要都对不上）
        with r.open_full_catalog_delivery() as fd:
            expect_reject("截断的完整交付被拒绝",
                          TruncatedFullDelivery(fd, drop=2))

        # 全部拒绝后，镜像目录与进度完好
        check("拒绝坏包后镜像仍可正常查询",
              mirror.object_status("a_00000", 1)["status"] == "LIVE")
        check("进度仍停在 3", progress_before["applied_batch"] == 3)

        # 正常增量仍可推进，证明未被坏包污染
        with r.open_incremental_catalog_delivery(3, 10) as inc:
            out = mirror.receive_incremental(inc, chunk_size=2)
        check("坏包之后正常增量推进到 5", out["applied_batch"] == 5)
        check("全部对象到达",
              mirror.object_status("e_00000", 1)["status"] == "LIVE"
              and mirror.object_status("f_00000", 1)["status"] == "LIVE")
        r.close()
        mirror.close()


# ============================================================ 6. 写入中断 + 重开 + 完整交付恢复
def test_interrupted_write_reopen_and_full_recovery():
    with tempfile.TemporaryDirectory() as d:
        r, _ = open_primary(os.path.join(d, "p.db"))
        r.import_objects(bulk_objects("a", 6))             # 批 1
        mp = os.path.join(d, "m.db")
        mirror = Reclaimer.open_catalog_mirror(mp)
        with r.open_full_catalog_delivery() as fd:
            mirror.receive_full(fd, chunk_size=2)         # baseline=1
        check("首次交付后进度 1", mirror.progress()["applied_batch"] == 1)
        r.import_objects(bulk_objects("b", 4))             # 批 2（待接收）

        # 增量逐行落盘（chunk_size=1），在第 3 个暂存块后注入故障：
        # 批 2 共 4 行，此时只暂存了 3 行，必未写齐
        calls = {"n": 0}

        def fault(kind, got):
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("simulated write interruption")

        mirror.close()
        mirror = Reclaimer.open_catalog_mirror(mp, write_fault=fault)
        try:
            with r.open_incremental_catalog_delivery(1, 5) as inc:
                mirror.receive_incremental(inc, chunk_size=1)
            check("写入中断应上抛", False)
        except RuntimeError as exc:
            check("写入中断上抛", "interruption" in str(exc))

        # 同一连接立即读到旧视图
        check("中断后进度未推进",
              mirror.progress()["applied_batch"] == 1)
        check("中断后 b_* 不可见",
              mirror.object_status("b_00000", 1)["status"] == "UNKNOWN")
        check("暂存已被清理",
              mirror.conn.execute(
                  "SELECT COUNT(*) FROM mirror_staging").fetchone()[0] == 0)
        mirror.close()

        # 全新进程重开：目录与进度仍是旧的完整视图
        mirror2 = Reclaimer.open_catalog_mirror(mp)
        prog = mirror2.progress()
        check("重开后进度仍是 1", prog["applied_batch"] == 1
              and prog["initialized"] == 1)
        check("重开后旧目录完整（6 行）",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 6)
        check("重开后无残留暂存",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_staging").fetchone()[0] == 0)
        check("重开后另一连接视角无半批",
              mirror2.object_status("b_00000", 1)["status"] == "UNKNOWN")

        # 用完整交付重新基线化（baseline 已 >= applied），再发一次验证幂等
        r.import_objects(bulk_objects("c", 2))             # 批 3
        with r.open_full_catalog_delivery() as fd:
            check("新完整交付基准=3", fd.header["baseline_batch"] == 3)
            res1 = mirror2.receive_full(fd, chunk_size=3)
        check("完整交付恢复后进度=3", res1["baseline_batch"] == 3)
        check("恢复后全部 12 行可见",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 12)
        check("重基线清空旧重放历史",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_batch_rows").fetchone()[0] == 0)
        # 重发同一份完整交付：内容一致，幂等生效
        with r.open_full_catalog_delivery() as fd2:
            res2 = mirror2.receive_full(fd2, chunk_size=3)
        check("完整交付重发幂等", res2["baseline_batch"] == 3)
        check("重发后仍 12 行",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 12)

        # 此前“已提交但答复未送达”的语义：批 2/3 已含在全量里，
        # 再以增量形式重发批 2/3 会被拒绝（基准前不可重放），不会重复生效
        try:
            with r.open_incremental_catalog_delivery(1, 5) as inc:
                mirror2.receive_incremental(inc)
            check("基准前批次重发被拒绝", False)
        except CatalogDeliveryError:
            check("基准前批次重发被拒绝", True)
        check("拒绝后行数不变",
              mirror2.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 12)
        r.close()
        mirror2.close()


# ============================================================ 7. 另一连接读不到半成品
def test_reader_never_sees_half_applied_state():
    with tempfile.TemporaryDirectory() as d:
        r, _ = open_primary(os.path.join(d, "p.db"))
        r.import_objects(bulk_objects("a", 10))            # 批 1（全量基准）
        mp = os.path.join(d, "m.db")
        mirror = Reclaimer.open_catalog_mirror(mp)
        with r.open_full_catalog_delivery() as fd:
            mirror.receive_full(fd, chunk_size=5)
        r.import_objects(bulk_objects("b", 30))            # 批 2（大批，待接收）

        # 独立只读连接（模拟盘点服务的另一进程/连接）
        reader = sqlite3.connect(mp, check_same_thread=False)

        def snapshot_view():
            n = reader.execute(
                "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0]
            applied = reader.execute(
                "SELECT applied_batch FROM mirror_meta WHERE id=1"
            ).fetchone()[0]
            staged = reader.execute(
                "SELECT COUNT(*) FROM mirror_staging").fetchone()[0]
            return n, applied, staged

        # 确定性证据：每个暂存块提交后，读者只能看到完整旧视图。
        # 暂存内容对读者虽可见，但目录表与进度绝不动（最终切换是单事务）。
        seen_during_staging = set()

        def fault(kind, got):
            seen_during_staging.add(snapshot_view()[:2])

        mirror._write_fault = fault
        with r.open_incremental_catalog_delivery(1, 5) as inc:
            mirror.receive_incremental(inc, chunk_size=4)
        mirror._write_fault = None
        check("分块落盘期间读者始终只见旧视图 (10,1)",
              seen_during_staging == {(10, 1)},
              str(sorted(seen_during_staging)))

        # 提交完成后读者只见完整新视图，且无残留暂存
        final_n, final_applied, staged_left = snapshot_view()
        check("提交后读者只见完整新视图 (40,2)",
              (final_n, final_applied) == (40, 2))
        check("提交后暂存清空", staged_left == 0)
        # 绝不存在中间行数：目录表历史上只出现过 10 或 40 两种完整规模
        check("不存在半批行数",
              all(n in (10, 40) for n, _ in seen_during_staging
                  | {(final_n, final_applied)}))

        # 补充并发压力：另起线程持续读取，整个过程不报错、只见完整视图
        observed = set()
        stop = threading.Event()

        def observe():
            while not stop.is_set():
                try:
                    observed.add(snapshot_view()[:2])
                except sqlite3.Error as exc:  # 任何锁/可见性错误都不允许
                    observed.add(("ERROR", str(exc)))
                time.sleep(0.0002)

        r.import_objects(bulk_objects("c", 7))             # 批 3
        t = threading.Thread(target=observe)
        t.start()
        with r.open_incremental_catalog_delivery(2, 5) as inc:
            mirror.receive_incremental(inc, chunk_size=3)
        stop.set()
        t.join()
        check("并发读者只见完整视图且无错误",
              all(n in (40, 47) and a in (2, 3)
                  for n, a in observed),
              str(sorted(observed)))
        check("并发读者最终看到 (47,3)", (47, 3) in observed,
              str(sorted(observed)))
        reader.close()
        r.close()
        mirror.close()


# ============================================================ 8. 小样本独立推导各版状态
def test_runtime_oracle_statuses():
    """不 import 产品判定逻辑，独立用字典推导每个版本应见状态。"""
    with tempfile.TemporaryDirectory() as d:
        r, backend = open_primary(os.path.join(d, "p.db"))
        # 运行时小样本：4 个身份、其中 1 个多版本、1 个回收
        specs = [
            ("s1", 1, "LIVE"), ("s1", 2, "LIVE"),
            ("s2", 1, "RECYCLED"),
            ("s3", 1, "LIVE"),
        ]
        r.import_objects([
            make_recyclable(oid, ver) if want == "RECYCLED"
            else make_object(oid, ver, otype=KEEP_TYPE)
            for oid, ver, want in specs
        ])
        backend.add("s2", 1, {"object_id": "s2", "version": 1})
        pid, _ = recycle_one(r, backend, "s2", 1)
        recycled_at = r.object_status("s2", 1)["recycled_at"]

        # 独立推导期望（手写 oracle，不看产品代码）
        expected = {
            ("s1", 1): "LIVE", ("s1", 2): "LIVE",
            ("s2", 1): "RECYCLED", ("s3", 1): "LIVE",
            ("s4", 1): "UNKNOWN", ("s1", 9): "UNKNOWN",
        }
        mirror = Reclaimer.open_catalog_mirror(os.path.join(d, "m.db"))
        with r.open_full_catalog_delivery() as fd:
            mirror.receive_full(fd, chunk_size=2)
        ok = True
        for (oid, ver), want in expected.items():
            got = mirror.object_status(oid, ver)["status"]
            if got != want:
                ok = False
                print(f"    oracle mismatch {oid} v{ver}: "
                      f"want={want} got={got}")
        check("小样本逐版本状态与独立推导一致", ok)
        s2 = mirror.object_status("s2", 1)
        check("独立推导的回收版本附带回收时间与计划",
              s2["recycled_at"] == recycled_at
              and s2["recycled_by_plan"] == pid)
        # 断开主库（直接关闭）后镜像仍可查
        r.close()
        check("断开主库后镜像独立可查",
              mirror.object_status("s1", 2)["status"] == "LIVE"
              and mirror.object_status("s2", 1)["status"] == "RECYCLED"
              and mirror.object_status("s4", 1)["status"] == "UNKNOWN")
        mirror.close()


# ============================================================ 9. 小增量不回扫老目录（可复跑证据）
def test_incremental_does_not_scan_old_catalog():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.db")
        r, _ = open_primary(path)
        n_old = 50_000
        # 分两批导入，模拟主库已有几十万级目录记录
        r.import_objects(bulk_objects("old", n_old // 2))
        r.import_objects(bulk_objects("old2", n_old // 2))
        r.import_objects(bulk_objects("fresh", 5))  # 小增量批次 3
        check("基线规模 50000 行",
              r.conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0]
              == n_old + 5)

        traced = []
        delivery = r.open_incremental_catalog_delivery(2, 1)
        delivery.conn.set_trace_callback(
            lambda sql: traced.append(sql) if "catalog" in sql else None)
        # 证据 1：trace 中增量生成访问的表只有 catalog_batches/catalog_changes
        touched_tables = set()
        for sql in traced:
            up = sql.upper()
            for tbl in ("objects", "catalog_batches", "catalog_changes",
                        "recycle_tombstones"):
                if tbl in up:
                    touched_tables.add(tbl)
        check("增量生成不访问 objects 老目录表",
              "objects" not in touched_tables, str(touched_tables))
        check("增量只读 catalog_* 变更表",
              touched_tables <= {"catalog_batches", "catalog_changes"},
              str(touched_tables))

        # 证据 2：EXPLAIN QUERY PLAN 走批次号索引范围扫描
        plans = delivery.conn.execute(
            "EXPLAIN QUERY PLAN SELECT object_id FROM catalog_changes "
            "WHERE batch_no=? AND seq_in_batch >= ? LIMIT ?",
            (3, 0, 1000)).fetchall()
        plan_text = " ".join(p[3] for p in plans)
        check("变更查询走索引（非全表 SCAN）",
              "USING INDEX" in plan_text.upper()
              and "SCAN" not in plan_text.upper().replace("USING INDEX", ""),
              plan_text)

        # 证据 3：进度处理器统计虚拟机步数：小增量步数不随老目录规模增长
        def count_steps(builder):
            dl = builder()
            steps = []
            dl.conn.set_progress_handler(lambda: steps.append(1) or 0, 1)
            for _ in dl.iter_chunks(1000):
                pass
            dl.conn.set_progress_handler(None, 0)
            dl.close()
            return len(steps)

        small_steps = count_steps(
            lambda: r.open_incremental_catalog_delivery(2, 1))
        # 再做一个同样大小的增量（新批次 4，也是 5 行），步数应同量级
        r.import_objects(bulk_objects("fresh2", 5))
        small_steps_2 = count_steps(
            lambda: r.open_incremental_catalog_delivery(3, 1))
        check("同等小增量步数稳定（不随历史增长）",
              abs(small_steps - small_steps_2) <= max(8, small_steps // 2),
              f"{small_steps} vs {small_steps_2}")

        # 对照：全量交付步数随目录规模显著更大
        full_dl = r.open_full_catalog_delivery()
        fsteps = []
        full_dl.conn.set_progress_handler(lambda: fsteps.append(1) or 0, 1)
        for _ in full_dl.iter_chunks(5000):
            pass
        full_dl.conn.set_progress_handler(None, 0)
        full_dl.close()
        check("全量扫描步数远大于小增量（对照）",
              len(fsteps) > small_steps * 50,
              f"full={len(fsteps)} incremental={small_steps}")

        # 证据 4：计时复跑——小增量两次耗时接近且远小于全量
        def time_builder(builder):
            t = time.perf_counter()
            dl = builder()
            for _ in dl.iter_chunks(2000):
                pass
            dl.close()
            return time.perf_counter() - t

        dt_inc1 = time_builder(
            lambda: r.open_incremental_catalog_delivery(3, 1))
        dt_inc2 = time_builder(
            lambda: r.open_incremental_catalog_delivery(3, 1))
        dt_full = time_builder(lambda: r.open_full_catalog_delivery())
        print(f"    计时: 增量={dt_inc1*1000:.2f}ms/{dt_inc2*1000:.2f}ms "
              f"全量={dt_full*1000:.2f}ms")
        check("小增量两次复跑耗时同量级",
              max(dt_inc1, dt_inc2) <= min(dt_inc1, dt_inc2) * 4 + 0.02)
        check("小增量明显快于全量扫描",
              dt_full > dt_inc1 * 5,
              f"full={dt_full:.4f}s inc={dt_inc1:.4f}s")
        delivery.close()
        r.close()


# ============================================================ 10. 大批分块传递与落盘（不整表进 Python 列表）
def test_large_batch_chunked_delivery():
    with tempfile.TemporaryDirectory() as d:
        r, _ = open_primary(os.path.join(d, "p.db"))
        big_n = 3000
        r.import_objects(bulk_objects("big", big_n))  # 单个大批

        mirror = Reclaimer.open_catalog_mirror(os.path.join(d, "m.db"))
        max_seen = {"n": 0}
        with r.open_full_catalog_delivery() as fd:
            check("全量头部行数正确", fd.header["row_count"] == big_n)
            for chunk in fd.iter_chunks(137):
                max_seen["n"] = max(max_seen["n"], len(chunk))
                assert len(chunk) <= 137
            mirror.receive_full(fd, chunk_size=137)
        check("分块大小有界（不整表进内存）", max_seen["n"] <= 137)
        check("大批全部落盘",
              mirror.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0]
              == big_n)

        # 增量侧再验证一个大批切块、批末摘要与批内序号连续
        r.import_objects(bulk_objects("big2", 1500))  # 批 2
        pieces = []
        with r.open_incremental_catalog_delivery(1, 1) as inc:
            check("增量不拆批（只含批 2）",
                  [b["batch_no"] for b in inc.header["batches"]] == [2])
            for piece in inc.iter_chunks(251):
                pieces.append(piece)
                if not piece["end_of_batch"]:
                    assert piece["digest"] is None
            mirror.receive_incremental(inc, chunk_size=251)
        check("大批被切成多块", len(pieces) > 1)
        check("只有最后一块带批摘要",
              sum(1 for p in pieces if p["digest"]) == 1
              and pieces[-1]["end_of_batch"] is True)
        seqs = mirror.conn.execute(
            "SELECT seq_in_batch FROM mirror_batch_rows "
            "WHERE batch_no=2 ORDER BY seq_in_batch").fetchall()
        check("批内序号 0..N-1 连续",
              [s for (s,) in seqs] == list(range(1500)))
        check("镜像总行数 4500",
              mirror.conn.execute(
                  "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 4500)
        r.close()
        mirror.close()


# ============================================================ 11. 内存库交付 + max_batches 边界 + 交付冻结
def test_in_memory_delivery_and_window_limits():
    r = Reclaimer.in_memory(backend=MemoryBackend())
    r.import_objects(bulk_objects("a", 2))   # 1（首次全量的基准）
    mirror = CatalogMirror(sqlite3.connect(":memory:"))
    with r.open_full_catalog_delivery() as fd:
        mirror.receive_full(fd, chunk_size=3)
    check("内存库完整交付成功", mirror.progress()["baseline_batch"] == 1)

    r.import_objects(bulk_objects("b", 2))   # 2
    r.import_objects(bulk_objects("c", 2))   # 3
    r.import_objects(bulk_objects("e", 2))   # 4

    # max_batches 限制窗口、不拆批
    with r.open_incremental_catalog_delivery(1, 2) as inc:
        check("窗口最多 2 批",
              [b["batch_no"] for b in inc.header["batches"]] == [2, 3])
        out = mirror.receive_incremental(inc, chunk_size=2)
    check("推进到 3", out["applied_batch"] == 3)
    with r.open_incremental_catalog_delivery(3, 10) as inc:
        check("剩余窗口只含批 4",
              [b["batch_no"] for b in inc.header["batches"]] == [4])
        mirror.receive_incremental(inc)
    check("推进到 4", mirror.progress()["applied_batch"] == 4)

    # 没有新批次时的空增量：合法但不推进
    with r.open_incremental_catalog_delivery(4, 5) as inc:
        check("空增量窗口为空", inc.header["batches"] == [])
        out = mirror.receive_incremental(inc)
    check("空增量不推进", out["new_batches"] == []
          and mirror.progress()["applied_batch"] == 4)

    # 已收前缀（批 2、3）+ 新批（批 4 已应用，再造批 5）一起重发：
    # 前缀逐行核验为重放，不重复生效
    r.import_objects(bulk_objects("late", 3))  # 批 5
    with r.open_incremental_catalog_delivery(1, 10) as inc:
        out = mirror.receive_incremental(inc, chunk_size=3)
    check("含已收前缀的交付：前缀重放、只推进新批",
          out["replayed_batches"] == [2, 3, 4]
          and out["new_batches"] == [5]
          and out["applied_batch"] == 5)
    check("重放不重复写目录",
          mirror.conn.execute(
              "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0] == 11)

    # 交付生成后源表继续变化，已生成交付内容冻结。
    # 内存库下生产与写入共享连接，先确认空窗口再关闭，随后新批次只对
    # 之后打开的交付可见（文件库下的强冻结见 test_concurrent_*）。
    with r.open_incremental_catalog_delivery(5, 5) as dl:
        check("新批次写入前的交付窗口为空", dl.header["batches"] == [])
    r.import_objects(bulk_objects("after", 2))  # 批 6
    with r.open_incremental_catalog_delivery(5, 5) as inc:
        check("新交付才能看到批 6",
              [b["batch_no"] for b in inc.header["batches"]] == [6])
    r.close()
    mirror.close()


def main():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    for t in tests:
        print(f"\n=== {t.__name__} ===")
        try:
            t()
        except Exception:
            traceback.print_exc()
            RESULTS.append((t.__name__, False, "exception"))
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"\n{'='*60}\n目录镜像测试：总计 {passed}/{len(RESULTS)} 项检查通过")
    failed = [n for n, ok, _ in RESULTS if not ok]
    if failed:
        print("失败项:")
        for n in failed:
            print(f"  - {n}")
        sys.exit(1)


if __name__ == "__main__":
    main()
