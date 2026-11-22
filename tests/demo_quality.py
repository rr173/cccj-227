"""分区质量隔离视图演示：逐分区展开、报障、重复投递、解除与恢复。

运行（在 /workspace 下）：
    python3 -I tests/demo_quality.py
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tests.test_quality_view as qv
from reclaimer import Reclaimer
from reclaimer.quality import QUALITY_NO_VIEW, QUALITY_RECYCLED


def line(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def expand(result: dict) -> str:
    cells = ["."] * result["partition_count"]
    for start, end in result["intervals"]:
        for p in range(start, end):
            cells[p] = "X"
    return "".join(cells)


def table(r, snapshot_id, names, label):
    print(f"\n{label}（每个对象 8 个分区，X = 必须避开）")
    print(f"  {'object':12s} 01234567  intervals")
    for name in names:
        res = r.quality_ranges(snapshot_id, name, 1)
        print(f"  {name:12s} {expand(res)}  {res['intervals']}")


def main() -> None:
    names = qv.SMALL_NAMES
    line("0) 谱系（src 依赖 dst，坏数从 dst 传给 src）")
    for e in qv.SMALL_EDGES:
        print(f"  {e['src_id']:5s} v{e['src_version']}  <-  "
              f"{e['dst_id']:5s} v{e['dst_version']}")
    print("  映射：a 读 raw 的 [0,4)->[0,4)、[4,8)->[4,8)；"
          "b 读 raw [0,8)->[0,8)；")
    print("        join 汇合 a/b；top 只覆盖 join 的 [0,6)，故意不铺满")

    r, snap = qv.bootstrap()
    table(r, snap, names, "1) 初始：已登记且当前可读，全部为空列表")

    line("2) 报障 raw v1 [1,3)：a 的 [0,4) 命中，b 的 [0,8) 命中")
    r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 3)
    table(r, snap, names, "   报障后")

    line("3) 再报 raw v1 [5,7)：两段映射分别命中，汇合节点 join 合并")
    r.report_quality_fault(snap, "f-raw-hi", "raw", 1, 5, 7)
    table(r, snap, names, "   两条上游故障并存")

    line("4) 同节点直接报障 join v1 [6,8)，与映射贡献取并集")
    r.report_quality_fault(snap, "f-join-direct", "join", 1, 6, 8)
    table(r, snap, names, "   直接报障后")

    line("5) 重复投递 / 换载荷 / 解除未知编号")
    dup = r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 3)
    print(f"  重发 f-raw-lo -> {dup['status']}（changed="
          f"{dup['changed']}）")
    try:
        r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 4)
    except Exception as err:  # noqa: BLE001
        print(f"  同编号换区间 -> {type(err).__name__}: {err}")
    try:
        r.release_quality_fault(snap, "f-unknown")
    except Exception as err:  # noqa: BLE001
        print(f"  解除未知编号 -> {type(err).__name__}: {err}")

    line("6) 部分解封：解除 f-raw-lo，join 仍被 f-raw-hi 与直接报障支撑")
    r.release_quality_fault(snap, "f-raw-lo")
    table(r, snap, names, "   部分解封后")
    replay = r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 3)
    print(f"  重放已解除的 f-raw-lo -> {replay['status']}"
          f"（changed={replay['changed']}，不会被重新激活）")
    r.release_quality_fault(snap, "f-raw-lo")
    print("  重复解除 -> 幂等，不改变结果")

    line("7) 全部解除：逐分区参考模型核对每个版本")
    for fault_id in ("f-raw-hi", "f-join-direct"):
        r.release_quality_fault(snap, fault_id)
    table(r, snap, names, "   最终")
    ref = qv.PartitionReference(
        {(name, 1): qv.COUNT for name in names + ["solo"]}, qv.SMALL_MAPPINGS)
    reference = ref.evaluate([])
    print("\n  参考模型与产品逐分区一致:",
          all(qv.expected_intervals(reference[(name, 1)])
              == r.quality_ranges(snap, name, 1)["intervals"]
              for name in names))
    print(f"  solo（无边的已知对象）: "
          f"{r.quality_ranges(snap, 'solo', 1)['intervals']}")
    print(f"  未登记版本 raw v2: "
          f"{r.quality_ranges(snap, 'raw', 2)['status']}")

    line("8) 已回收版本明确返回 RECYCLED，且仍可作上游归因")
    r.report_quality_fault(snap, "f-recycled", "raw", 1, 0, 4)
    r.conn.execute("UPDATE objects SET lifecycle='RECYCLED', recycled_at=1,"
                   " recycled_by_plan='p0' WHERE object_id='raw'"
                   " AND version=1")
    r.conn.execute("INSERT INTO recycle_tombstones(object_id, version,"
                   " plan_id, recycled_at, former_layer, former_payload_ref)"
                   " VALUES('raw',1,'p0',1,'hot','hot://raw/v1')")
    r.conn.commit()
    recycled = r.quality_ranges(snap, "raw", 1)
    print(f"  raw v1 -> {recycled['status']} readable={recycled['readable']}"
          f" intervals={recycled['intervals']}")
    print(f"  a v1（存活下游）仍隔离: "
          f"{r.quality_ranges(snap, 'a', 1)['intervals']}")
    r.release_quality_fault(snap, "f-recycled")
    print(f"  解除上游报障后 a v1: "
          f"{r.quality_ranges(snap, 'a', 1)['intervals']}")
    r.close()

    line("9) 写到一半失败：重开文件库仍读到旧结果")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "quality-demo.db")
        r, snap = qv.bootstrap_file(db_path)
        r.report_quality_fault(snap, "f-old", "raw", 1, 1, 3)
        before = r.quality_ranges(snap, "join", 1)["intervals"]
        print(f"  已提交结果 join v1: {before}")
        original = r.quality._add_support
        state = {"n": 0}

        def crash(*args, **kwargs):
            original(*args, **kwargs)
            state["n"] += 1
            if state["n"] == 2:
                raise RuntimeError("injected process exit before commit")

        r.quality._add_support = crash
        try:
            r.report_quality_fault(snap, "f-new", "raw", 1, 5, 7)
        except RuntimeError as err:
            print(f"  本次报障失败: {err}")
        r.quality._add_support = original
        print(f"  当前连接 join v1: "
              f"{r.quality_ranges(snap, 'join', 1)['intervals']}")
        r.close()

        r2 = Reclaimer.open(db_path)
        print(f"  重新打开库 join v1: "
              f"{r2.quality_ranges(snap, 'join', 1)['intervals']}")
        print("  未提交的 f-new 不存在:",
              [f for f in r2.quality_faults(snap, only_unreleased=False)
               if f["fault_id"] == "f-new"] == [])
        r2.report_quality_fault(snap, "f-new", "raw", 1, 5, 7)
        print(f"  重试成功后 join v1: "
              f"{r2.quality_ranges(snap, 'join', 1)['intervals']}")
        r2.close()

    line("10) 运行时构造：10^9 长区间 + 无关大支路，单次更新量有界")
    for width in (10, 400):
        r = qv._build_scaling_scenario(width)
        report = r.report_quality_fault(
            "scale", "huge-fault", "huge", 1, 0, 10 ** 9)
        stats = report["stats"]
        print(f"  无关叶子数={width:4d}  nodes_changed="
              f"{stats['nodes_changed']}  mappings_examined="
              f"{stats['mappings_examined']}  intervals_written="
              f"{stats['intervals_written']}  "
              f"huge_child={r.quality_ranges('scale', 'huge_child', 1)['intervals']}")
        print(f"  大支路 noise_root 隔离: "
              f"{r.quality_ranges('scale', 'noise_root', 1)['intervals']}"
              f"（不受影响）")
        r.close()


if __name__ == "__main__":
    main()
