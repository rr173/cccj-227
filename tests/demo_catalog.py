"""目录镜像端到端演示：主库交付生产 + 盘点服务镜像接收。

运行：python3 -I tests/demo_catalog.py

演示流程
========
1. 主库导入对象（每个成功导入产生一个 IMPORT 批次）；
2. 完整交付建立镜像（来源标识 + 基准批次 + 全量目录，同一快照）；
3. 主库再导入新版本，增量交付推进，断开主库后镜像独立查询；
4. 回收一个旧版本：镜像保留该版本并可见回收时间，新版本仍 LIVE；
5. 坏包（来源不同 / 缺行）被拒绝，进度不动；同批重放幂等。
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import (  # noqa: E402
    CatalogDeliveryError,
    Reclaimer,
)
from reclaimer.executor import MemoryBackend  # noqa: E402

T0 = 1_000_000
AS_OF = 10_000_000


def obj(oid, ver, *, otype="raw_file", created=T0):
    return {
        "object_id": oid, "version": ver, "obj_type": otype,
        "classification": "public", "environment": "prod",
        "storage_layer": "cold", "created_at": created,
        "payload_ref": f"cold://{oid}/v{ver}",
    }


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        primary_path = os.path.join(d, "primary.db")
        mirror_path = os.path.join(d, "mirror.db")
        backend = MemoryBackend()
        r = Reclaimer.open(primary_path, backend=backend)

        # 1) 导入：批次 1
        r.import_objects([obj("orders", 1), obj("orders", 2),
                          obj("clicks", 1)])
        print(f"[主库] 来源标识: {r.catalog_source_id()}")
        print(f"[主库] 当前批次: {r.catalog_latest_batch()}")

        # 2) 完整交付 -> 建立镜像
        mirror = Reclaimer.open_catalog_mirror(mirror_path)
        with r.open_full_catalog_delivery() as full:
            print(f"[交付] 完整: 基准批次={full.header['baseline_batch']} "
                  f"行数={full.header['row_count']}")
            mirror.receive_full(full, chunk_size=2)
        print(f"[镜像] 进度: {mirror.progress()}")

        # 3) 新导入 -> 批次 2，增量交付
        r.import_objects([obj("orders", 3, created=T0 + 5_000)])
        with r.open_incremental_catalog_delivery(1, max_batches=10) as inc:
            print(f"[交付] 增量: 批次 {inc.header['start_batch']}.."
                  f"{inc.header['end_batch']} "
                  f"各批行数={[b['row_count'] for b in inc.header['batches']]}")
            result = mirror.receive_incremental(inc, chunk_size=2)
        print(f"[镜像] 应用结果: 新批={result['new_batches']} "
              f"重放={result['replayed_batches']}")

        # 断开主库：镜像独立查询三态
        r.close()
        print("[镜像] 断开主库后独立查询:")
        for oid, ver in (("orders", 1), ("orders", 3), ("ghost", 9)):
            st = mirror.object_status(oid, ver)
            print(f"    {oid} v{ver} -> {st['status']}")

        # 4) 重开主库，回收 orders v1（专用类型策略只回收目标）
        r = Reclaimer.open(primary_path, backend=backend)
        backend.add("orders", 1, {"object_id": "orders", "version": 1})
        # 直接把目标改为回收专用类型，避免误伤其它对象
        with r.conn:
            r.conn.execute(
                "UPDATE objects SET obj_type='recycle_target' "
                "WHERE object_id='orders' AND version=1")
        snap = r.import_snapshot([], snapshot_id="snap-recycle")
        r.import_policy([
            {"rule_id": "expire", "obj_type": "recycle_target",
             "retention_seconds": 3_000_000},
            {"rule_id": "keep", "obj_type": "raw_file",
             "retention_seconds": 10**12},
        ], "pol-recycle")
        plan = r.generate_plan(policy_version="pol-recycle",
                               snapshot_id=snap, as_of_ts=AS_OF)
        r.execute(plan["plan_id"])
        print(f"[主库] 回收后批次: {r.catalog_latest_batch()}")

        with r.open_incremental_catalog_delivery(2, max_batches=10) as inc:
            mirror.receive_incremental(inc)
        v1 = mirror.object_status("orders", 1)
        v3 = mirror.object_status("orders", 3)
        print(f"[镜像] orders v1 -> {v1['status']} "
              f"recycled_at={v1['recycled_at']} plan={v1['recycled_by_plan']}")
        print(f"[镜像] orders v3 -> {v3['status']}（新版本仍存活）")

        # 5a) 来源不同的交付被拒绝
        other = Reclaimer.open(os.path.join(d, "other.db"),
                               backend=MemoryBackend())
        other.import_objects([obj("x", 1)])
        other.import_objects([obj("y", 1)])
        try:
            with other.open_incremental_catalog_delivery(1, 5) as bad:
                mirror.receive_incremental(bad)
            print("[镜像] 来源校验: 失败（未拒绝）")
        except CatalogDeliveryError as exc:
            print(f"[镜像] 来源不同被拒绝: {exc}")
        other.close()

        # 5b) 同批重发：幂等，不重复生效
        rows_before = mirror.conn.execute(
            "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0]
        with r.open_incremental_catalog_delivery(2, max_batches=10) as again:
            replay = mirror.receive_incremental(again)
        rows_after = mirror.conn.execute(
            "SELECT COUNT(*) FROM mirror_catalog").fetchone()[0]
        print(f"[镜像] 重发结果: 重放={replay['replayed_batches']} "
              f"新批={replay['new_batches']} 行数 {rows_before}->{rows_after}")
        print(f"[镜像] 最终进度: {mirror.progress()}")
        r.close()
        mirror.close()
        print("演示完成。")


if __name__ == "__main__":
    main()
