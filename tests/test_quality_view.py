"""分区质量隔离视图测试。

运行（在 /workspace 下）：
    python3 -I tests/test_quality_view.py

覆盖
====
1. 谱系快照登记：端点、完整性、区间边界、库外对象、快照外边；
2. 汇合分支上的报障 / 部分解封 / 半开边界 / 同节点直接报障；
3. 重复投递、旧报障重放、同编号换载荷、解除未知编号；
4. 小坐标逐分区展开的独立参考模型（不 import 产品代码）三方核对；
5. 快照之间映射与报障互不串用；
6. 已回收版本与已回收上游的质量归因语义；
7. 写到一半失败后重开文件库仍读到旧结果；
8. 运行时构造的超长区间 + 大支路更新量有界场景。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import Reclaimer
from reclaimer.errors import QualityFaultError, QualityValidationError
from reclaimer.quality import (
    QUALITY_AVAILABLE,
    QUALITY_NO_VIEW,
    QUALITY_RECYCLED,
    normalize,
)

RESULTS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(cond), detail))
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -- {detail}" if detail and not cond
                                  else ""))


# ============================================================ 夹具
COUNT = 8


def obj(object_id, version=1, kind="table", created=1_000):
    return {
        "object_id": object_id, "version": version, "obj_type": kind,
        "classification": "c", "environment": "prod",
        "storage_layer": "hot", "created_at": created,
        "payload_ref": f"hot://{object_id}/v{version}",
    }


SMALL_NAMES = ["raw", "a", "b", "join", "top"]
SMALL_OBJECTS = [obj(name) for name in SMALL_NAMES]
SMALL_SOLO = obj("solo")
SMALL_EDGES = [
    # src 依赖 dst：坏数从 dst 流向 src
    {"src_id": "a", "src_version": 1, "dst_id": "raw", "dst_version": 1},
    {"src_id": "b", "src_version": 1, "dst_id": "raw", "dst_version": 1},
    {"src_id": "join", "src_version": 1, "dst_id": "a", "dst_version": 1},
    {"src_id": "join", "src_version": 1, "dst_id": "b", "dst_version": 1},
    {"src_id": "top", "src_version": 1, "dst_id": "join", "dst_version": 1},
]


def _mapping(src, dst, read, out):
    return {"src_id": src, "src_version": 1,
            "dst_id": dst, "dst_version": 1,
            "read": list(read), "out": list(out)}


SMALL_MAPPINGS = [
    # a 读取 raw 的两段：半开边界恰好落在 4
    _mapping("a", "raw", (0, 4), (0, 4)),
    _mapping("a", "raw", (4, 8), (4, 8)),
    _mapping("b", "raw", (0, 8), (0, 8)),
    # join 由 a、b 两路上游汇合
    _mapping("join", "a", (0, 4), (0, 2)),
    _mapping("join", "a", (4, 8), (2, 4)),
    _mapping("join", "b", (0, 8), (4, 6)),
    # top 只声明 join 的前 6 个分区，故意不铺满
    _mapping("top", "join", (0, 2), (0, 1)),
    _mapping("top", "join", (2, 4), (1, 2)),
    _mapping("top", "join", (4, 6), (2, 3)),
]

SMALL_DECLARATIONS = [
    {"object_id": name, "version": 1, "partition_count": COUNT}
    for name in SMALL_NAMES
] + [{"object_id": "solo", "version": 1, "partition_count": 4}]


def bootstrap(snapshot_id="qsnap-1"):
    r = Reclaimer.in_memory()
    r.import_objects(SMALL_OBJECTS + [SMALL_SOLO])
    r.import_snapshot(SMALL_EDGES, snapshot_id=snapshot_id)
    r.register_quality_view(snapshot_id, SMALL_DECLARATIONS, SMALL_MAPPINGS)
    return r, snapshot_id


def bootstrap_file(db_path, snapshot_id="qsnap-1"):
    r = Reclaimer.open(db_path)
    r.import_objects(SMALL_OBJECTS + [SMALL_SOLO])
    r.import_snapshot(SMALL_EDGES, snapshot_id=snapshot_id)
    r.register_quality_view(snapshot_id, SMALL_DECLARATIONS, SMALL_MAPPINGS)
    return r, snapshot_id


# ============================================================ 独立参考模型
class PartitionReference:
    """逐分区展开的朴素参考实现（小坐标专用，不 import 产品代码）。"""

    def __init__(self, counts, mappings):
        self.counts = dict(counts)
        self.mappings = list(mappings)

    def all_nodes(self):
        return sorted(self.counts)

    def evaluate(self, faults):
        iso = {node: set() for node in self.counts}
        for object_id, version, start, end in faults:
            assert (object_id, version) in iso
            iso[(object_id, version)].update(range(start, end))
        changed = True
        while changed:
            changed = False
            for m in self.mappings:
                dst = (m["dst_id"], m["dst_version"])
                src = (m["src_id"], m["src_version"])
                read = set(range(*m["read"]))
                if iso[dst] & read:
                    produced = set(range(*m["out"])) - iso[src]
                    if produced:
                        iso[src].update(produced)
                        changed = True
        return iso


def expected_intervals(partition_set):
    return [[start, end] for start, end in normalize([(p, p + 1)
                                                      for p in partition_set])]


def assert_matches_reference(r, snapshot_id, ref, active_faults, label):
    iso = ref.evaluate(active_faults)
    for (object_id, version), partitions in sorted(iso.items()):
        result = r.quality_ranges(snapshot_id, object_id, version)
        want = expected_intervals(partitions)
        flat = sorted(p for s, e in result["intervals"] for p in range(s, e))
        check(f"{label}: {object_id} v{version} 区间 == 逐分区参考",
              result["status"] == QUALITY_AVAILABLE
              and result["intervals"] == want
              and flat == sorted(partitions),
              f"got {result['intervals']} want {want}")


# ============================================================ 1. 登记校验
def test_registration_validation_and_atomicity():
    r = Reclaimer.in_memory()
    r.import_objects(SMALL_OBJECTS + [SMALL_SOLO])
    r.import_snapshot(SMALL_EDGES, snapshot_id="s1")

    def expect_error(label, call, exc=QualityValidationError):
        try:
            call()
        except exc:
            check(label, True)
        except Exception as err:  # noqa: BLE001
            check(label, False, f"raised {type(err).__name__}: {err}")
        else:
            check(label, False, "did not raise")

    expect_error(
        "未导入快照登记被拒绝",
        lambda: r.register_quality_view(
            "ghost", SMALL_DECLARATIONS, SMALL_MAPPINGS))
    expect_error(
        "库里不存在的对象版本被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS
            + [{"object_id": "ghost", "version": 1, "partition_count": 4}],
            SMALL_MAPPINGS))
    expect_error(
        "重复对象声明被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS
            + [{"object_id": "raw", "version": 1, "partition_count": 8}],
            SMALL_MAPPINGS))
    expect_error(
        "分区总数为 0 被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS[:-1]
            + [{"object_id": "solo", "version": 1, "partition_count": 0}],
            SMALL_MAPPINGS))
    expect_error(
        "缺少端点声明被拒绝",
        lambda: r.register_quality_view(
            "s1",
            [d for d in SMALL_DECLARATIONS if d["object_id"] != "top"],
            SMALL_MAPPINGS))
    expect_error(
        "快照外的边被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS + [_mapping("top", "raw", (0, 1), (0, 1))]))
    expect_error(
        "某条快照边缺少映射被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:2] + SMALL_MAPPINGS[3:]))
    expect_error(
        "读取区间越界被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:-1]
            + [_mapping("top", "join", (4, 9), (2, 3))]))
    expect_error(
        "产出区间越界被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:-1]
            + [_mapping("top", "join", (4, 6), (2, 9))]))
    expect_error(
        "空区间被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:-1]
            + [_mapping("top", "join", (4, 4), (2, 3))]))
    expect_error(
        "负起点被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:-1]
            + [_mapping("top", "join", (-1, 2), (2, 3))]))
    expect_error(
        "非整数端点被拒绝",
        lambda: r.register_quality_view(
            "s1", SMALL_DECLARATIONS,
            SMALL_MAPPINGS[:-1]
            + [_mapping("top", "join", (0.5, 2), (2, 3))]))

    check("无效定义没有留下 quality_views",
          r.conn.execute("SELECT COUNT(*) FROM quality_views").fetchone()[0]
          == 0)
    check("无效定义没有留下半份映射",
          r.conn.execute("SELECT COUNT(*) FROM quality_mappings")
          .fetchone()[0] == 0)

    # 有效定义随后可以成功登记，说明失败没有占用快照
    summary = r.register_quality_view("s1", SMALL_DECLARATIONS,
                                      SMALL_MAPPINGS)
    check("校验失败后可重新登记成功",
          summary["mapping_count"] == len(SMALL_MAPPINGS))
    check("未在快照中出边但库里已知的对象版本可登记",
          r.quality_ranges("s1", "solo", 1)["status"] == QUALITY_AVAILABLE
          and r.quality_ranges("s1", "solo", 1)["intervals"] == [])
    try:
        r.register_quality_view("s1", SMALL_DECLARATIONS, SMALL_MAPPINGS)
    except QualityValidationError:
        check("同一快照重复登记质量视图被拒绝", True)
    else:
        check("同一快照重复登记质量视图被拒绝", False)
    r.close()


# ============================================================ 2. 汇合分支
def test_convergence_partial_release_and_boundaries():
    r, snap = bootstrap()
    ref = PartitionReference(
        {(name, 1): COUNT for name in SMALL_NAMES + ["solo"]},
        SMALL_MAPPINGS)
    active = []

    assert_matches_reference(r, snap, ref, active, "初始空视图")
    check("空列表只表示已登记且可读版本没有隔离",
          r.quality_ranges(snap, "raw", 1)["status"] == QUALITY_AVAILABLE
          and r.quality_ranges(snap, "raw", 1)["intervals"] == [])

    # 半开边界：raw [1,3) 只命中 a 的 [0,4) 映射
    res = r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 3)
    active.append(("raw", 1, 1, 3))
    check("报障后 raw 区间为 [1,3)", res["intervals"] == [[1, 3]])
    check("a 因读取 [1,3) 命中而输出 [0,4)",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 4]])
    check("b 的整段读取命中，输出覆盖 [0,8)",
          r.quality_ranges(snap, "b", 1)["intervals"] == [[0, 8]])
    check("汇合节点 join 同时收到 a、b 两路支持",
          r.quality_ranges(snap, "join", 1)["intervals"]
          == [[0, 2], [4, 6]])
    check("top 由 join 的两段产出得到相邻合并区间",
          r.quality_ranges(snap, "top", 1)["intervals"]
          == [[0, 1], [2, 3]])
    assert_matches_reference(r, snap, ref, active, "首条报障")

    # 半开边界：raw [3,4) 只与第一段读取 [0,4) 相交，不激活 [4,8)
    r.report_quality_fault(snap, "f-edge-just-below", "raw", 1, 3, 4)
    check("raw [3,4) 仍只命中第一段映射",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 4]])
    r.release_quality_fault(snap, "f-edge-just-below")
    # raw [4,5) 恰好从第二段起点开始，半开区间不包含 4 以外的前一段
    r.report_quality_fault(snap, "f-edge-hi", "raw", 1, 4, 5)
    active.append(("raw", 1, 4, 5))
    check("半开边界 [4,5) 激活第二段映射，a 现为两段之并",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 8]])
    r.release_quality_fault(snap, "f-edge-hi")
    active = [f for f in active if f[:2] != ("raw", 1)
              or f[2:] != (4, 5)]
    check("解除后 a 回到第一段映射",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 4]])

    # 第二条故障：raw [5,7)，与第一条分别命中两段
    r.report_quality_fault(snap, "f-raw-hi", "raw", 1, 5, 7)
    active.append(("raw", 1, 5, 7))
    check("两条故障同时存在时 a 被完整隔离",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 8]])
    check("两条故障同时存在时 join 合并为 [0,6)",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[0, 6]])
    check("join [0,6) 让 top 三段相邻合并为 [0,3)",
          r.quality_ranges(snap, "top", 1)["intervals"] == [[0, 3]])
    assert_matches_reference(r, snap, ref, active, "两条上游故障")

    # 同节点直接报障：join 自己再报 [6,8)，与映射支持取并集
    r.report_quality_fault(snap, "f-join-direct", "join", 1, 6, 8)
    active.append(("join", 1, 6, 8))
    check("join 自身报障与映射贡献并集为 [0,8)",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[0, 8]])
    check("join 新增的 [6,8) 没有 top 映射，top 不变",
          r.quality_ranges(snap, "top", 1)["intervals"] == [[0, 3]])
    assert_matches_reference(r, snap, ref, active, "同节点直接报障")

    # 直接报障落在已被映射覆盖的分区：节点并集不变，不应传播
    before_top = r.quality_ranges(snap, "top", 1)["intervals"]
    redundant = r.report_quality_fault(
        snap, "f-join-redundant", "join", 1, 1, 2)
    active.append(("join", 1, 1, 2))
    check("重合报障不改变 join 并集",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[0, 8]])
    check("节点并集未变时不向后继传播",
          redundant["stats"]["nodes_changed"] == 0
          and r.quality_ranges(snap, "top", 1)["intervals"] == before_top)
    assert_matches_reference(r, snap, ref, active, "重合报障")

    # 部分解封：解除 f-raw-lo 后 join 仍由 f-raw-hi 与直接报障支撑。
    # raw 只剩 [5,7)：a 的 [0,4) 映射失活，[4,8) 仍活跃 -> a=[4,8)；
    # join 上游 a=[4,8)、b=[0,8) -> join=[1,8)（[0,1) 被解封）；
    # top 的三段读取区间都与 join=[1,8) 相交，因此仍全部隔离
    r.release_quality_fault(snap, "f-raw-lo")
    active = [f for f in active if f[:2] != ("raw", 1)
              or f[2:] != (1, 3)]
    check("解除一条故障只移除失去支持的部分：a 从 [0,8) 缩到 [4,8)",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[4, 8]])
    check("部分解封沿链更新 join 为 [1,8)",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[1, 8]])
    check("top 的读取段仍与 join 隔离区间相交，保持 [0,3)",
          r.quality_ranges(snap, "top", 1)["intervals"] == [[0, 3]])
    assert_matches_reference(r, snap, ref, active, "部分解封")

    # 旧报障重放不得重新激活
    replay = r.report_quality_fault(snap, "f-raw-lo", "raw", 1, 1, 3)
    check("旧报障重放返回 already-released 且不激活",
          replay["status"] == "already-released"
          and replay["changed"] is False
          and r.quality_ranges(snap, "raw", 1)["intervals"] == [[5, 7]])
    check("重放后故障表仍标记为已解除",
          r.quality_faults(snap, only_unreleased=False)
          and [f for f in r.quality_faults(snap, only_unreleased=False)
               if f["fault_id"] == "f-raw-lo"][0]["released_at"] is not None)
    assert_matches_reference(r, snap, ref, active, "旧报障重放")

    # 解除剩余故障，全部清空
    for fault_id in ("f-raw-hi", "f-join-direct", "f-join-redundant"):
        result = r.release_quality_fault(snap, fault_id)
        check(f"解除 {fault_id} 成功", result["status"] == "released")
    active.clear()
    for name in SMALL_NAMES + ["solo"]:
        check(f"全部解除后 {name} 无隔离",
              r.quality_ranges(snap, name, 1)["intervals"] == [])
    assert_matches_reference(r, snap, ref, active, "全部解除")
    r.close()


# ============================================================ 3. 幂等与冲突
def test_overlapping_same_edge_mappings():
    """同一条边可有多条交叠映射；任一读取区间命中都贡献产出区间。"""
    r = Reclaimer.in_memory()
    r.import_objects([obj("u"), obj("v")])
    r.import_snapshot(
        [{"src_id": "v", "src_version": 1,
          "dst_id": "u", "dst_version": 1}], snapshot_id="ov")
    r.register_quality_view(
        "ov",
        [{"object_id": "u", "version": 1, "partition_count": 10},
         {"object_id": "v", "version": 1, "partition_count": 10}],
        [
            _mapping("v", "u", (0, 4), (0, 2)),
            _mapping("v", "u", (2, 6), (1, 5)),
            _mapping("v", "u", (5, 8), (4, 6)),
        ])
    r.report_quality_fault("ov", "f1", "u", 1, 3, 4)
    check("读取 [3,4) 同时命中前两条交叠映射",
          r.quality_ranges("ov", "v", 1)["intervals"] == [[0, 5]])
    r.report_quality_fault("ov", "f2", "u", 1, 7, 8)
    check("第三条映射独立贡献并合并相邻区间",
          r.quality_ranges("ov", "v", 1)["intervals"] == [[0, 6]])
    r.release_quality_fault("ov", "f2")
    check("解除 f2 后仍由 f1 命中的前两条映射支撑",
          r.quality_ranges("ov", "v", 1)["intervals"] == [[0, 5]])
    r.release_quality_fault("ov", "f1")
    check("全部解除后交集部分清空",
          r.quality_ranges("ov", "v", 1)["intervals"] == [])
    r.close()


def test_fault_idempotency_and_conflicts():
    r, snap = bootstrap()
    first = r.report_quality_fault(snap, "f1", "raw", 1, 2, 4)
    snapshot_before = {
        "raw": r.quality_ranges(snap, "raw", 1)["intervals"],
        "a": r.quality_ranges(snap, "a", 1)["intervals"],
    }
    duplicate = r.report_quality_fault(snap, "f1", "raw", 1, 2, 4)
    check("重发相同报障不改变结果",
          duplicate["status"] == "duplicate"
          and duplicate["changed"] is False
          and r.quality_ranges(snap, "raw", 1)["intervals"]
          == snapshot_before["raw"]
          and r.quality_ranges(snap, "a", 1)["intervals"]
          == snapshot_before["a"])

    for label, kwargs, exc in [
        ("同编号换区间报错",
         dict(object_id="raw", version=1, range_start=2, range_end=5),
         QualityFaultError),
        ("同编号换对象报错",
         dict(object_id="a", version=1, range_start=2, range_end=4),
         QualityFaultError),
    ]:
        try:
            r.report_quality_fault(snap, "f1", **kwargs)
        except exc:
            check(label, True)
        except Exception as err:  # noqa: BLE001
            check(label, False, f"raised {type(err).__name__}")
        else:
            check(label, False, "did not raise")

    released = r.release_quality_fault(snap, "f1")
    again = r.release_quality_fault(snap, "f1")
    check("重复解除幂等",
          released["status"] == "released"
          and again["status"] == "already-released"
          and again["changed"] is False)
    replay = r.report_quality_fault(snap, "f1", "raw", 1, 2, 4)
    check("解除后重放旧报障不复活",
          replay["status"] == "already-released"
          and r.quality_ranges(snap, "a", 1)["intervals"] == [])
    try:
        r.release_quality_fault(snap, "never-reported")
    except QualityFaultError:
        check("解除未知编号报错", True)
    else:
        check("解除未知编号报错", False)

    # 不同故障覆盖同一分区：解除一条仍保留另一条
    r.report_quality_fault(snap, "d1", "join", 1, 2, 5)
    r.report_quality_fault(snap, "d2", "join", 1, 3, 6)
    check("不同故障重叠分区取并集",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[2, 6]])
    r.release_quality_fault(snap, "d1")
    check("解除一条后重叠区仍由另一条支撑",
          r.quality_ranges(snap, "join", 1)["intervals"] == [[3, 6]])
    r.release_quality_fault(snap, "d2")
    check("两条都解除后为空",
          r.quality_ranges(snap, "join", 1)["intervals"] == [])
    r.close()


# ============================================================ 4. 快照隔离
def test_snapshots_are_isolated():
    r, snap1 = bootstrap("snap-1")
    r.import_snapshot(SMALL_EDGES, snapshot_id="snap-2")
    mappings2 = [
        _mapping("a", "raw", (0, 8), (0, 4)),
        _mapping("b", "raw", (0, 8), (0, 8)),
        _mapping("join", "a", (0, 4), (0, 2)),
        _mapping("join", "a", (4, 8), (2, 4)),
        _mapping("join", "b", (0, 8), (4, 6)),
        _mapping("top", "join", (0, 8), (0, 3)),
    ]
    r.register_quality_view("snap-2", SMALL_DECLARATIONS, mappings2)

    r.report_quality_fault(snap1, "same-id", "raw", 1, 1, 3)
    # 同一编号在另一快照内可独立使用，且载荷可以不同
    r.report_quality_fault("snap-2", "same-id", "a", 1, 0, 2)
    check("同一故障编号在不同快照可独立存在",
          r.quality_ranges(snap1, "a", 1)["intervals"] == [[0, 4]]
          and r.quality_ranges("snap-2", "a", 1)["intervals"] == [[0, 2]])
    # 方向核对：快照一里 raw [1,3) 同时命中 a 的两段映射之外的分区；
    # 快照二同一编号的报障落在 a 自身，结果完全独立。
    check("快照二的映射不串到快照一",
          r.quality_ranges(snap1, "a", 1)["intervals"] == [[0, 4]]
          and r.quality_ranges(snap1, "join", 1)["intervals"]
          == [[0, 2], [4, 6]]
          and r.quality_ranges("snap-2", "a", 1)["intervals"] == [[0, 2]]
          and r.quality_ranges("snap-2", "join", 1)["intervals"]
          == [[0, 2]])
    r.release_quality_fault(snap1, "same-id")
    check("解除快照一故障不影响快照二",
          r.quality_ranges(snap1, "a", 1)["intervals"] == []
          and r.quality_ranges("snap-2", "a", 1)["intervals"] == [[0, 2]])

    check("未登记质量视图的已导入快照返回 NO_QUALITY_VIEW",
          r.quality_ranges("snap-3", "raw", 1)["status"] == QUALITY_NO_VIEW
          and r.quality_ranges("snap-3", "raw", 1)["intervals"] is None)
    check("已登记快照中未声明的对象版本返回 NO_QUALITY_VIEW",
          r.quality_ranges(snap1, "raw", 2)["status"] == QUALITY_NO_VIEW)
    r.close()


# ============================================================ 5. 回收语义
def test_recycled_semantics():
    r, snap = bootstrap()
    # 已回收版本：明确返回 RECYCLED，不可当成可供数
    r.report_quality_fault(snap, "upstream-fault", "raw", 1, 0, 4)
    r.conn.execute(
        """UPDATE objects SET lifecycle='RECYCLED', recycled_at=123,
                  recycled_by_plan='p0'
           WHERE object_id='raw' AND version=1""")
    r.conn.execute(
        """INSERT INTO recycle_tombstones(object_id, version, plan_id,
                  recycled_at, former_layer, former_payload_ref)
           VALUES('raw',1,'p0',123,'hot','hot://raw/v1')""")
    r.conn.commit()

    upstream = r.quality_ranges(snap, "raw", 1)
    check("已回收版本返回 RECYCLED 且不给区间",
          upstream["status"] == QUALITY_RECYCLED
          and upstream["readable"] is False
          and upstream["intervals"] is None)
    check("已回收上游仍把影响传给存活下游",
          r.quality_ranges(snap, "a", 1)["intervals"] == [[0, 4]]
          and r.quality_ranges(snap, "join", 1)["intervals"]
          == [[0, 2], [4, 6]])
    # 回收不自动解除上游报障；解除报障才停止归因
    r.release_quality_fault(snap, "upstream-fault")
    check("解除上游报障后存活下游解除隔离",
          r.quality_ranges(snap, "a", 1)["intervals"] == []
          and r.quality_ranges(snap, "join", 1)["intervals"] == [])
    r.close()


# ============================================================ 6. 写到一半失败
def test_crash_mid_write_reopen_reads_old_result():
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "quality.db")
        r, snap = bootstrap_file(db_path)
        r.report_quality_fault(snap, "f-old", "raw", 1, 1, 3)
        old = {
            "raw": r.quality_ranges(snap, "raw", 1)["intervals"],
            "a": r.quality_ranges(snap, "a", 1)["intervals"],
            "join": r.quality_ranges(snap, "join", 1)["intervals"],
        }

        # 注入：在第二条支持写库之后抛错，模拟事务提交前进程退出
        original_add = r.quality._add_support
        calls = {"n": 0}

        def crashing_add_support(*args, **kwargs):
            original_add(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("injected failure mid-write")

        r.quality._add_support = crashing_add_support
        try:
            r.report_quality_fault(snap, "f-new", "raw", 1, 5, 7)
        except RuntimeError:
            check("写到一半的报障抛出失败", True)
        else:
            check("写到一半的报障抛出失败", False)

        r.quality._add_support = original_add
        check("失败事务未留下已接收报障",
              [f for f in r.quality_faults(snap, only_unreleased=False)
               if f["fault_id"] == "f-new"] == [])
        check("失败事务未留下新支持区间",
              r.conn.execute(
                  "SELECT COUNT(*) FROM quality_supports"
                  " WHERE source_key='f-new'").fetchone()[0] == 0)
        check("失败后的当前连接仍读到旧结果",
              r.quality_ranges(snap, "raw", 1)["intervals"] == old["raw"]
              and r.quality_ranges(snap, "a", 1)["intervals"] == old["a"]
              and r.quality_ranges(snap, "join", 1)["intervals"]
              == old["join"])
        r.close()

        r2 = Reclaimer.open(db_path)
        check("重新打开文件库仍读到旧结果",
              r2.quality_ranges(snap, "raw", 1)["intervals"] == old["raw"]
              and r2.quality_ranges(snap, "a", 1)["intervals"] == old["a"]
              and r2.quality_ranges(snap, "join", 1)["intervals"]
              == old["join"])
        check("重开后失败报障仍不存在",
              [f for f in r2.quality_faults(snap, only_unreleased=False)
               if f["fault_id"] == "f-new"] == [])
        # 重新提交同一报障可成功
        result = r2.report_quality_fault(snap, "f-new", "raw", 1, 5, 7)
        check("重开后重试同一报障成功",
              result["status"] == "reported"
              and r2.quality_ranges(snap, "raw", 1)["intervals"]
              == [[1, 3], [5, 7]])
        r2.close()


# ============================================================ 7. 运行时构造
def _build_scaling_scenario(noise_width):
    """超长区间 + 不受影响的大支路。

    huge(dst) --整段映射--> huge_child(src)
    noise_root(src) 依赖 noise_leaf_0..N-1(dst)，与 huge 支路无任何边。
    """
    r = Reclaimer.in_memory()
    objects = [obj("huge"), obj("huge_child"), obj("noise_root")]
    declarations = [
        {"object_id": "huge", "version": 1, "partition_count": 10 ** 9},
        {"object_id": "huge_child", "version": 1,
         "partition_count": 10 ** 9},
        {"object_id": "noise_root", "version": 1,
         "partition_count": noise_width},
    ]
    edges = [{"src_id": "huge_child", "src_version": 1,
              "dst_id": "huge", "dst_version": 1}]
    mappings = [_mapping("huge_child", "huge", (0, 10 ** 9), (0, 10 ** 9))]
    for i in range(noise_width):
        leaf = f"noise_leaf_{i}"
        objects.append(obj(leaf))
        declarations.append(
            {"object_id": leaf, "version": 1, "partition_count": 1})
        edges.append({"src_id": "noise_root", "src_version": 1,
                      "dst_id": leaf, "dst_version": 1})
        mappings.append(_mapping("noise_root", leaf, (0, 1), (i, i + 1)))
    r.import_objects(objects)
    r.import_snapshot(edges, snapshot_id="scale")
    r.register_quality_view("scale", declarations, mappings)
    return r


def test_long_interval_and_large_branch_bounded_update():
    observations = []
    for width in (10, 400):
        r = _build_scaling_scenario(width)
        report = r.report_quality_fault(
            "scale", "huge-fault", "huge", 1, 0, 10 ** 9)
        stats = report["stats"]
        rows = r.conn.execute(
            """SELECT range_start, range_end FROM quality_isolated_ranges
               WHERE snapshot_id='scale' AND object_id='huge'""").fetchall()
        child = r.quality_ranges("scale", "huge_child", 1)["intervals"]
        noise = r.quality_ranges("scale", "noise_root", 1)["intervals"]
        observations.append(
            (stats["nodes_changed"], stats["mappings_examined"],
             stats["intervals_written"]))
        check(f"宽度 {width}: 10^9 长区间只存一个端点区间",
              rows == [(0, 10 ** 9)] and child == [[0, 10 ** 9]])
        check(f"宽度 {width}: 单次报障只改 2 个节点并只看 1 条映射",
              stats["nodes_changed"] == 2
              and stats["mappings_examined"] == 1
              and stats["intervals_written"] == 2)
        check(f"宽度 {width}: 大支路完全不受影响", noise == [])

        release = r.release_quality_fault("scale", "huge-fault")
        check(f"宽度 {width}: 解除同样只改 2 个节点、只看 1 条映射",
              release["stats"]["nodes_changed"] == 2
              and release["stats"]["mappings_examined"] == 1
              and r.quality_ranges("scale", "huge_child", 1)["intervals"]
              == [])
        r.close()

    check("更新量不随无关支路规模增长",
          observations[0] == observations[1],
          f"{observations[0]} != {observations[1]}")

    # 小区间故障 + 长区间并存：受影响映射仍按实际相交定位
    r = _build_scaling_scenario(400)
    r.report_quality_fault("scale", "wide", "huge", 1, 0, 10 ** 9)
    small = r.report_quality_fault(
        "scale", "near-end", "huge", 1, 10 ** 9 - 2, 10 ** 9)
    check("长区间已存在时新增小报障只重算 1 个节点（无新映射激活）",
          small["stats"]["mappings_examined"] <= 1)
    check("长区间与小报障并集仍为单个端点区间",
          r.quality_ranges("scale", "huge", 1)["intervals"]
          == [[0, 10 ** 9]])
    r.release_quality_fault("scale", "wide")
    check("解除长区间后保留末端小报障",
          r.quality_ranges("scale", "huge", 1)["intervals"]
          == [[10 ** 9 - 2, 10 ** 9]]
          and r.quality_ranges("scale", "huge_child", 1)["intervals"]
          == [[0, 10 ** 9]])
    r.release_quality_fault("scale", "near-end")
    check("末端小报障解除后全部清空",
          r.quality_ranges("scale", "huge", 1)["intervals"] == []
          and r.quality_ranges("scale", "huge_child", 1)["intervals"] == [])
    r.close()


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
    print(f"\n{'=' * 60}\n总计 {passed}/{len(RESULTS)} 项检查通过")
    failed = [n for n, ok, _ in RESULTS if not ok]
    if failed:
        print("失败项:")
        for name in failed:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
