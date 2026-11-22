"""存储费用结算接口测试：手算账单 + 按秒独立核算 + 高共享度精确核对。

运行：python3 -I tests/test_settlement.py
（在 /workspace 下运行；测试包用 sys.path 引导，不依赖 pytest）

场景总览
========
1. test_hand_computed_shared_content_bill
   手算一笔跨回收时刻的共享内容账单：A v1/A v2 同属 cc-x（同时引用只算
   一个中心），B v1 从计量切换点 40 开始引用（cc-y），末段 [80,100)
   无人引用进入“未归属”，[70,80) 是计量空档不计费；余数并列时未归属
   项排在普通中心之后。回收由真实执行器在受控时钟下完成。
2. test_per_second_oracle_small_coordinates
   小坐标数据，测试内按秒独立展开核算（oracle），与产品结果逐
   （层, 中心）核对精确份额与整数分。
3. test_high_sharing_exact_and_bounded_work
   运行时生成的长时间（10^12 秒）、高共享度（350 中心）物理内容，
   4000 段计量、单段字节数 10^15 量级（字节秒总量远超 SQLite 有符号
   整数范围），核对精确份额、整数分与工作量计数。
4. test_invalid_single_mutation_no_bill
   供应商层金额保持不变，每次只变动一处输入使其无效：整次报错、不给
   可入账结果、业务表零变化，且之后仍能正常结算。
5. test_shuffled_inputs_identical_bill / test_rounding_tie_breaks /
   test_zero_amount_layers / test_attribution_required_only_when_overlapping
"""

from __future__ import annotations

import os
import random
import sys
import time
import traceback
from fractions import Fraction

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import Reclaimer
from reclaimer.errors import SettlementError
from reclaimer.executor import MemoryBackend

RESULTS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(cond), detail))
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -- {detail}" if detail and not cond
                                  else ""))


# ============================================================ 工具
def db_snapshot(conn):
    """全部业务表内容的可比较快照（TEMP 暂存表不在 sqlite_master 内）。"""
    snap = {}
    for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"):
        rows = conn.execute(f"SELECT * FROM {name}").fetchall()
        snap[name] = sorted(map(repr, rows))
    return snap


def mark_recycled(r, object_id, version, recycled_at):
    """测试夹具：把版本直接置为已回收（模拟历史回收事实）。"""
    r.conn.execute(
        "UPDATE objects SET lifecycle='RECYCLED', recycled_at=?,"
        " recycled_by_plan='fixture' WHERE object_id=? AND version=?",
        (recycled_at, object_id, version))
    r.conn.execute(
        "INSERT INTO recycle_tombstones(object_id, version, plan_id,"
        " recycled_at, former_layer, former_payload_ref)"
        " SELECT object_id, version, 'fixture', ?, storage_layer,"
        " payload_ref FROM objects WHERE object_id=? AND version=?",
        (recycled_at, object_id, version))
    r.conn.commit()


def make_obj(oid, ver, layer, created, payload, cls="c9"):
    return {"object_id": oid, "version": ver, "obj_type": "raw_file",
            "classification": cls, "environment": "prod",
            "storage_layer": layer, "created_at": created,
            "payload_ref": payload}


def item_map(bill):
    """账单 items -> {(layer, center): (share, cents)}，center None=未归属。"""
    return {(it["storage_layer"], it["cost_center"]):
            (it["share"], it["cents"]) for it in bill["items"]}


# ============================================================ 按秒独立核算
def oracle_shares(objects, metering, attributions, from_ts, to_ts):
    """独立参考：每个整秒分别判定字节数与活跃中心集合，逐秒均分。

    不 import 任何产品代码；只允许在小坐标上运行（按秒展开）。
    """
    attr = {(oid, ver): center for (oid, ver, center) in attributions}
    refs = {}
    for o in objects:
        rs = max(o["created_at"], from_ts)
        re_ = min(o["recycled_at"] if o["recycled_at"] is not None
                  else to_ts, to_ts)
        if rs < re_:
            refs.setdefault((o["storage_layer"], o["payload_ref"]),
                            []).append((rs, re_, o["object_id"],
                                        o["version"]))
    shares = {}
    for (layer, payload, s, e, b) in metering:
        for t in range(s, e):
            centers = set()
            for (rs, re_, oid, ver) in refs.get((layer, payload), []):
                if rs <= t < re_:
                    centers.add(attr[(oid, ver)])
            if centers:
                for c in centers:
                    key = (layer, c)
                    shares[key] = (shares.get(key, Fraction(0))
                                   + Fraction(b, len(centers)))
            else:
                key = (layer, None)
                shares[key] = shares.get(key, Fraction(0)) + b
    return shares


def oracle_cents(due, shares):
    """独立实现的最大余数法（规格原文：向下取整、余数大者补一分、
    并列按中心名码点序、未归属排在同余数普通中心之后）。"""
    total = sum(shares.values(), Fraction(0))
    rows = []
    floors = 0
    for center, share in shares.items():
        exact = Fraction(due) * share / total
        floor = exact.numerator // exact.denominator
        rows.append((center, floor, exact - floor))
        floors += floor
    remainder = due - floors
    rows.sort(key=lambda r: (-r[2], r[0] is None,
                             r[0] if r[0] is not None else ""))
    return {c: f + (1 if i < remainder else 0)
            for i, (c, f, _) in enumerate(rows)}


# ============================================================ 1. 手算账单
def bootstrap_recycled_backstory():
    """用真实执行器在受控时钟下制造回收历史。

    目录内容（payload_ref 相同且同层 = 同一物理内容）：

    ======== ====== ========= ======== ======= ==================
    版本      层     payload   created  回收于  归属
    ======== ====== ========= ======== ======= ==================
    A v1     hot    p-shared  0        50      cc-x
    A v2     hot    p-shared  10       70      cc-x（与 A v1 同中心）
    B v1     hot    p-shared  40       80      cc-y（从计量切换点引用）
    C v1     cold   p-shared  5        存活    （另一层=另一物理内容）
    D v1     hot    p-other   0        存活    （无计量，无需归属）
    ======== ====== ========= ======== ======= ==================
    """
    clock = [0]
    backend = MemoryBackend()
    r = Reclaimer.in_memory(backend=backend, clock=lambda: clock[0])
    objs = [
        make_obj("A", 1, "hot", 0, "p-shared", cls="c1"),
        make_obj("A", 2, "hot", 10, "p-shared", cls="c2"),
        make_obj("B", 1, "hot", 40, "p-shared", cls="c3"),
        make_obj("C", 1, "cold", 5, "p-shared"),
        make_obj("D", 1, "hot", 0, "p-other"),
    ]
    r.import_objects(objs)
    for o in objs:
        backend.add(o["object_id"], o["version"], o)
    snap = r.import_snapshot([], snapshot_id="snap-settle")

    def rule(cls):
        return {"rule_id": f"rule-{cls}", "obj_type": "raw_file",
                "classification": cls, "environment": None,
                "retention_seconds": 0}

    # 三个策略版本逐步放宽到 c1/c2/c3，控制每个版本的回收时刻
    r.import_policy([rule("c1")], "pol-1")
    clock[0] = 50
    p1 = r.generate_plan(policy_version="pol-1", snapshot_id=snap,
                         as_of_ts=100)
    r.execute(p1["plan_id"])                       # A v1 回收于 50
    r.import_policy([rule("c1"), rule("c2")], "pol-2")
    clock[0] = 70
    p2 = r.generate_plan(policy_version="pol-2", snapshot_id=snap,
                         as_of_ts=100)
    r.execute(p2["plan_id"])                       # A v2 回收于 70
    r.import_policy([rule("c1"), rule("c2"), rule("c3")], "pol-3")
    clock[0] = 80
    p3 = r.generate_plan(policy_version="pol-3", snapshot_id=snap,
                         as_of_ts=100)
    r.execute(p3["plan_id"])                       # B v1 回收于 80
    return r


MAIN_METERING = [
    ("hot", "p-shared", 0, 40, 10),
    ("hot", "p-shared", 40, 70, 20),
    ("hot", "p-shared", 80, 100, 15),
]
MAIN_ATTRIBUTIONS = [("A", 1, "cc-x"), ("A", 2, "cc-x"), ("B", 1, "cc-y")]
MAIN_AMOUNTS = {"hot": 2}


def test_hand_computed_shared_content_bill():
    r = bootstrap_recycled_backstory()
    # 目录事实确认：A v1 已回收（不是 LIVE），但历史引用必须留在账单里
    st = r.object_status("A", 1)
    check("A v1 当前状态是 RECYCLED", st["status"] == "RECYCLED")
    check("A v1 墓碑记录回收时刻 50",
          st["tombstone"]["recycled_at"] == 50)

    before = db_snapshot(r.conn)
    # 计量与归属都以一次性迭代器传入，且顺序打乱
    metering = iter([MAIN_METERING[2], MAIN_METERING[0], MAIN_METERING[1]])
    attributions = iter([MAIN_ATTRIBUTIONS[1], MAIN_ATTRIBUTIONS[2],
                         MAIN_ATTRIBUTIONS[0]])
    bill = r.settle_storage_costs(from_ts=0, to_ts=100,
                                  layer_amounts=dict(MAIN_AMOUNTS),
                                  metering=metering,
                                  attributions=attributions)

    # ---- 手算（见模块 docstring 的场景表）--------------------------
    # 引用区间：A v1 [0,50) cc-x；A v2 [10,70) cc-x；B v1 [40,80) cc-y
    # 计量段：  [0,40) 10B/s；[40,70) 20B/s；[80,100) 15B/s；[70,80) 空档
    # span 拆分（引用集合恒定）：
    #   [0,10)   {x}     10×10=100 -> x+100
    #   [10,40)  {x}     30×10=300 -> x+300   （A v1+A v2 同中心算一个）
    #   [40,50)  {x,y}   10×20=200 -> x+100, y+100
    #   [50,70)  {x,y}   20×20=400 -> x+200, y+200
    #   [70,80)  {y}     计量空档，不计费
    #   [80,100) {}      20×15=300 -> 未归属
    # 合计：x=700, y=300, 未归属=300；总份额 1300。
    # 应付 2 分：x=700/1300×2=14/13=1+1/13；y=未归属=6/13=0+6/13。
    # 向下取整后余 1 分；y 与未归属余数并列 -> 普通中心 cc-y 优先。
    # 结果：x=1, y=1, 未归属=0。
    got = item_map(bill)
    expect = {("hot", "cc-x"): (Fraction(700), 1),
              ("hot", "cc-y"): (Fraction(300), 1),
              ("hot", None): (Fraction(300), 0)}
    check("手算份额与整数分一致", got == expect,
          f"got {got}")
    check("未归属项不与普通中心合并",
          bill["items"][2]["cost_center"] is None
          and bill["items"][2]["unattributed"] is True)
    check("行序：层内普通中心码点序、未归属最后",
          [it["cost_center"] for it in bill["items"]]
          == ["cc-x", "cc-y", None])
    layer = bill["layers"][0]
    check("层汇总：应付=已分摊=2，份额总量=1300",
          layer["storage_layer"] == "hot"
          and layer["amount_due_cents"] == 2
          and layer["allocated_cents"] == 2
          and layer["share_total"] == Fraction(1300))
    check("同层各项之和等于应付金额",
          sum(it["cents"] for it in bill["items"]) == 2)
    check("总量对平：total_due == total_allocated",
          bill["total_due_cents"] == bill["total_allocated_cents"] == 2)
    check("工作量计数：1 内容 / 3 引用 / 5 计量片 / 6 次中心分摊",
          bill["stats"] == {"contents": 1, "references": 3,
                            "metered_pieces": 5,
                            "center_share_additions": 6},
          f"got {bill['stats']}")
    check("结算不改写业务表", db_snapshot(r.conn) == before)
    r.close()


# ============================================================ 2. 按秒 oracle
def test_per_second_oracle_small_coordinates():
    r = Reclaimer.in_memory(backend=MemoryBackend())
    objs = [
        make_obj("o1", 1, "hot", 0, "p1"),
        make_obj("o2", 1, "hot", 5, "p1"),
        make_obj("o3", 1, "hot", 12, "p1"),
        make_obj("o4", 1, "hot", 28, "p1"),
        make_obj("o5", 1, "cold", 0, "p2"),
        make_obj("o6", 1, "cold", 14, "p2"),
        make_obj("o7", 1, "hot", 10, "p3"),   # 引用不与计量相交 -> 无需归属
        make_obj("o9", 1, "hot", 100, "p1"),  # 结算期后创建 -> 不参与
        make_obj("o11", 1, "cold", 0, "p1"),  # 同名 payload 在另一层
    ]
    r.import_objects(objs)
    mark_recycled(r, "o1", 1, 15)
    mark_recycled(r, "o3", 1, 25)
    mark_recycled(r, "o5", 1, 6)

    metering = [
        ("hot", "p1", 0, 10, 3), ("hot", "p1", 12, 20, 5),
        ("hot", "p1", 20, 30, 2),
        ("cold", "p2", 5, 15, 100),
        ("hot", "p3", 0, 5, 7),
        ("cold", "p1", 0, 30, 1),
    ]
    attributions = [
        ("o1", 1, "cc-a"), ("o2", 1, "cc-a"), ("o3", 1, "cc-b"),
        ("o4", 1, "cc-c"), ("o5", 1, "cc-a"), ("o6", 1, "cc-b"),
        ("o11", 1, "cc-c"),
    ]
    amounts = {"hot": 125, "cold": 7}

    bill = r.settle_storage_costs(
        from_ts=0, to_ts=30, layer_amounts=amounts,
        metering=iter(metering), attributions=iter(attributions))

    # ---- 独立按秒核算 ---------------------------------------------
    objects_for_oracle = []
    for o in objs:
        d = dict(o)
        row = r.conn.execute(
            "SELECT recycled_at FROM objects WHERE object_id=? AND version=?",
            (o["object_id"], o["version"])).fetchone()
        d["recycled_at"] = row[0]
        objects_for_oracle.append(d)
    exp_shares = oracle_shares(objects_for_oracle, metering, attributions,
                               0, 30)
    got = item_map(bill)
    check("按秒 oracle：分摊项集合一致",
          set(got) == set(exp_shares),
          f"diff {set(got) ^ set(exp_shares)}")
    check("按秒 oracle：精确份额一致",
          all(got[k][0] == exp_shares[k] for k in exp_shares),
          f"got {got}")
    # 整数分：hot 应付 125 = 份额总量，每项恰好整分；cold 用独立取整核对
    exp_cents = {}
    for layer, due in amounts.items():
        layer_shares = {c: s for (l, c), s in exp_shares.items()
                        if l == layer}
        for c, cents in oracle_cents(due, layer_shares).items():
            exp_cents[(layer, c)] = cents
    check("按秒 oracle：整数分一致",
          all(got[k][1] == exp_cents[k] for k in exp_cents),
          f"got {got}")

    # ---- 手算锚点 ---------------------------------------------------
    hand = {("hot", "cc-a"): (Fraction(63), 63),
            ("hot", "cc-b"): (Fraction(25), 25),
            ("hot", "cc-c"): (Fraction(2), 2),
            ("hot", None): (Fraction(35), 35),
            ("cold", "cc-a"): (Fraction(100), 1),
            ("cold", "cc-b"): (Fraction(100), 1),
            ("cold", "cc-c"): (Fraction(30), 0),
            ("cold", None): (Fraction(800), 5)}
    check("手算锚点一致", got == hand, f"got {got}")
    for layer, due in amounts.items():
        s = sum(it["cents"] for it in bill["items"]
                if it["storage_layer"] == layer)
        check(f"{layer} 层各项之和等于应付 {due}", s == due)
    r.close()


# ============================================================ 3. 高共享度
def test_high_sharing_exact_and_bounded_work():
    """长时间 + 高共享度：工作量不随秒数或 段数×活跃中心数 增长。

    - 结算期 10^12 秒：按秒展开需要 10^12 次迭代，本测试瞬间完成，
      说明工作量与秒数无关（只与端点数量有关）。
    - 4000 段计量 × 350 个活跃中心：若“字节数每次变化都重扫活跃中心”，
      需要 4000×350 = 1.4×10^6 次中心分摊；实现把计量段先归并进 3 个
      “引用集合恒定”的 span，再按 span 整体分摊，中心分摊仅
      300+350+300 = 950 次（stats 直接给出计数）。
    - 单段字节数 ~10^15、字节秒总量 ~10^28，远超 SQLite 有符号 64 位
      整数范围：暂存用 TEXT、乘加用 Python 大整数，精确份额分毫不差。
    """
    r = Reclaimer.in_memory(backend=MemoryBackend())
    n_full, n_mid = 300, 50
    period, n_seg = 10 ** 12, 4000
    seg_len = period // n_seg                     # 250_000_000
    mid_start, mid_end = 3 * 10 ** 11, 7 * 10 ** 11
    assert mid_start % seg_len == 0 and mid_end % seg_len == 0

    objs = [make_obj(f"sh-{i:03d}", 1, "hot", 0, "p-huge")
            for i in range(n_full)]
    objs += [make_obj(f"mid-{j:03d}", 1, "hot", mid_start, "p-huge")
             for j in range(n_mid)]
    r.import_objects(objs)
    for j in range(n_mid):
        mark_recycled(r, f"mid-{j:03d}", 1, mid_end)

    attributions = ([(f"sh-{i:03d}", 1, f"cc-{i:04d}")
                     for i in range(n_full)]
                    + [(f"mid-{j:03d}", 1, f"cc-{n_full + j:04d}")
                       for j in range(n_mid)])

    def metering():
        for i in range(n_seg):
            yield ("hot", "p-huge", i * seg_len, (i + 1) * seg_len,
                   (i % 97 + 1) * 10 ** 15)

    due = 10 ** 10 + 7
    t0 = time.monotonic()
    bill = r.settle_storage_costs(
        from_ts=0, to_ts=period, layer_amounts={"hot": due},
        metering=metering(), attributions=iter(attributions))
    elapsed = time.monotonic() - t0

    # ---- 精确份额闭式核对 ------------------------------------------
    def bs(lo, hi):
        return seg_len * 10 ** 15 * sum(i % 97 + 1
                                        for i in range(lo, hi))

    b1 = bs(0, 1200)          # [0, 3e11)：300 个全程中心
    b2 = bs(1200, 2800)       # [3e11, 7e11)：300+50 个中心
    b3 = bs(2800, 4000)       # [7e11, 1e12)：300 个全程中心
    check("字节秒总量超出 SQLite 有符号整数范围",
          b1 + b2 + b3 > 2 ** 63 - 1)
    got = item_map(bill)
    full_share = (Fraction(b1, 300) + Fraction(b2, 350)
                  + Fraction(b3, 300))
    mid_share = Fraction(b2, 350)
    ok = all(got.get(("hot", f"cc-{i:04d}"), (None,))[0] == full_share
             for i in range(n_full))
    ok = ok and all(
        got.get(("hot", f"cc-{n_full + j:04d}"), (None,))[0] == mid_share
        for j in range(n_mid))
    check("350 个中心精确份额全部命中闭式解", ok)
    check("无未归属项（全程有引用）",
          all(it["cost_center"] is not None for it in bill["items"]))

    exp_cents = oracle_cents(due, {f"cc-{i:04d}": full_share
                                   for i in range(n_full)}
                             | {f"cc-{n_full + j:04d}": mid_share
                                for j in range(n_mid)})
    check("整数分与独立取整一致",
          all(got[("hot", c)][1] == cents
              for c, cents in exp_cents.items()))
    check("同层各项之和等于应付金额",
          sum(it["cents"] for it in bill["items"]) == due)

    stats = bill["stats"]
    check("工作量计数：4000 计量片 / 950 次中心分摊（≠ 4000×350）",
          stats["metered_pieces"] == 4000
          and stats["center_share_additions"] == 950
          and stats["contents"] == 1 and stats["references"] == 350,
          f"got {stats}")
    check("长时间高共享度结算在 5 秒内完成", elapsed < 5.0,
          f"{elapsed:.2f}s")
    r.close()


# ============================================================ 4. 一处无效即拒
def test_invalid_single_mutation_no_bill():
    valid = {
        "from_ts": 0, "to_ts": 100,
        "layer_amounts": dict(MAIN_AMOUNTS),
        "metering": list(MAIN_METERING),
        "attributions": list(MAIN_ATTRIBUTIONS),
    }

    def mutated(**kw):
        args = dict(valid)
        args.update(kw)
        return args

    cases = [
        ("同版本重复归属（同中心）",
         mutated(attributions=MAIN_ATTRIBUTIONS + [("A", 1, "cc-x")])),
        ("同版本重复归属（换中心）",
         mutated(attributions=MAIN_ATTRIBUTIONS + [("A", 1, "cc-z")])),
        ("归属未知版本",
         mutated(attributions=MAIN_ATTRIBUTIONS + [("ZZ", 9, "cc-x")])),
        ("同一物理内容计量重叠",
         mutated(metering=MAIN_METERING + [("hot", "p-shared", 30, 50, 1)])),
        ("计量段越出结算期",
         mutated(metering=MAIN_METERING + [("hot", "p-shared", 90, 120, 1)])),
        ("计量段为空",
         mutated(metering=MAIN_METERING + [("hot", "p-shared", 60, 60, 1)])),
        ("字节数为零",
         mutated(metering=MAIN_METERING + [("hot", "p-shared", 60, 65, 0)])),
        ("字节数为负",
         mutated(metering=MAIN_METERING + [("hot", "p-shared", 60, 65, -3)])),
        ("字节数非整数",
         mutated(metering=MAIN_METERING
                 + [("hot", "p-shared", 60, 65, 1.5)])),
        ("计量层缺少应付金额",
         mutated(metering=MAIN_METERING + [("warm", "p-x", 0, 10, 1)])),
        ("正费用层没有任何计量",
         mutated(layer_amounts={"hot": 2, "cold": 3})),
        ("引用与计量同时有效却缺失归属",
         mutated(attributions=MAIN_ATTRIBUTIONS[:2])),   # 去掉 B v1 归属
        ("无效结算期（from >= to）", mutated(from_ts=100, to_ts=100)),
        ("无效结算期（倒置）", mutated(from_ts=200, to_ts=100)),
        ("金额为负", mutated(layer_amounts={"hot": -1})),
        ("金额非整数", mutated(layer_amounts={"hot": 2.5})),
        ("中心名为空",
         mutated(attributions=[("A", 1, "cc-x"), ("A", 2, "cc-x"),
                               ("B", 1, "  ")])),
    ]

    for name, kwargs in cases:
        r = bootstrap_recycled_backstory()
        before = db_snapshot(r.conn)
        kwargs.setdefault("from_ts", 0)
        kwargs.setdefault("to_ts", 100)
        kwargs.setdefault("layer_amounts", dict(MAIN_AMOUNTS))
        kwargs.setdefault("metering", list(MAIN_METERING))
        kwargs.setdefault("attributions", list(MAIN_ATTRIBUTIONS))
        try:
            r.settle_storage_costs(**kwargs)
            check(f"无效输入被拒绝：{name}", False, "返回了可入账账单")
        except SettlementError:
            check(f"无效输入被拒绝：{name}", True)
        except Exception as exc:  # 其它异常类型也是失败：必须领域错误
            check(f"无效输入被拒绝：{name}", False, f"抛出 {exc!r}")
        check(f"失败后业务表零变化：{name}",
              db_snapshot(r.conn) == before)
        # 失败不留下半张账单：同一实例随后可正常结算
        bill = r.settle_storage_costs(
            from_ts=0, to_ts=100, layer_amounts=dict(MAIN_AMOUNTS),
            metering=iter(MAIN_METERING),
            attributions=iter(MAIN_ATTRIBUTIONS))
        check(f"失败后仍可正常结算：{name}",
              sum(it["cents"] for it in bill["items"]) == 2)
        r.close()


# ============================================================ 5. 其它性质
def test_shuffled_inputs_identical_bill():
    bills = []
    for seed in (7, 42, 2026):
        r = bootstrap_recycled_backstory()
        metering = list(MAIN_METERING)
        attributions = list(MAIN_ATTRIBUTIONS)
        random.Random(seed).shuffle(metering)
        random.Random(seed + 1).shuffle(attributions)
        bills.append(r.settle_storage_costs(
            from_ts=0, to_ts=100, layer_amounts=dict(MAIN_AMOUNTS),
            metering=iter(metering), attributions=iter(attributions)))
        r.close()
    check("打乱输入次序后结果与行序完全一致",
          bills[0] == bills[1] == bills[2])


def test_rounding_tie_breaks():
    """余数并列：普通中心按码点序，未归属项排在同余数普通中心之后。"""
    r = Reclaimer.in_memory(backend=MemoryBackend())
    r.import_objects([
        make_obj("x1", 1, "hot", 0, "pa"),
        make_obj("x2", 1, "hot", 0, "pb"),
    ])
    metering = [("hot", "pa", 0, 10, 1), ("hot", "pb", 0, 10, 1),
                ("hot", "pc", 0, 10, 1)]   # pc 无引用 -> 未归属
    attributions = [("x1", 1, "cc-b"), ("x2", 1, "cc-a")]
    # 三份份额均为 10：应付 2 -> 每项 2/3，余 2 分；
    # 并列时 cc-a 先于 cc-b，未归属最后 -> cc-a=1, cc-b=1, 未归属=0
    bill = r.settle_storage_costs(from_ts=0, to_ts=10,
                                  layer_amounts={"hot": 2},
                                  metering=iter(metering),
                                  attributions=iter(attributions))
    got = item_map(bill)
    check("并列余数：普通中心按码点序各补一分",
          got[("hot", "cc-a")][1] == 1 and got[("hot", "cc-b")][1] == 1)
    check("并列余数：未归属排在普通中心之后",
          got[("hot", None)][1] == 0)
    # 应付 1 -> 余 1 分，码点最小的 cc-a 独得
    bill1 = r.settle_storage_costs(from_ts=0, to_ts=10,
                                   layer_amounts={"hot": 1},
                                   metering=iter(metering),
                                   attributions=iter(attributions))
    got1 = item_map(bill1)
    check("余 1 分时码点最小中心独得",
          got1[("hot", "cc-a")][1] == 1
          and got1[("hot", "cc-b")][1] == 0
          and got1[("hot", None)][1] == 0)
    r.close()


def test_zero_amount_layers():
    r = Reclaimer.in_memory(backend=MemoryBackend())
    r.import_objects([make_obj("z1", 1, "hot", 0, "pz")])
    metering = [("hot", "pz", 0, 10, 4)]
    attributions = [("z1", 1, "cc-z")]
    # 零费用层有计量：分摊项照出，整数分全 0
    bill = r.settle_storage_costs(
        from_ts=0, to_ts=10,
        layer_amounts=[("hot", 0), ("warm", 0)],   # 也接受 (层,分) 对序列
        metering=iter(metering), attributions=iter(attributions))
    got = item_map(bill)
    check("零费用层仍给出精确份额、整数分为 0",
          got[("hot", "cc-z")] == (Fraction(40), 0))
    layers = {l["storage_layer"]: l for l in bill["layers"]}
    check("无计量且零费用的层无需输出分摊项",
          all(it["storage_layer"] != "warm" for it in bill["items"])
          and layers["warm"]["amount_due_cents"] == 0
          and layers["warm"]["share_total"] == 0)
    r.close()


def test_attribution_required_only_when_overlapping():
    """只有引用与计量同时有效的版本才要求归属。"""
    r = Reclaimer.in_memory(backend=MemoryBackend())
    r.import_objects([
        make_obj("k1", 1, "hot", 0, "pk"),    # [0,50) 有归属
        make_obj("k2", 1, "hot", 60, "pk"),   # [60,100) 无归属，但无计量
    ])
    # 计量只覆盖 [0,50)：k2 的引用不与计量相交 -> 结算成功
    bill = r.settle_storage_costs(from_ts=0, to_ts=100,
                                  layer_amounts={"hot": 5},
                                  metering=[("hot", "pk", 0, 50, 2)],
                                  attributions=[("k1", 1, "cc-k")])
    check("引用不跨计量的无归属版本不阻断结算",
          item_map(bill)[("hot", "cc-k")] == (Fraction(100), 5))
    # 计量延伸到 [0,70)：k2 在 [60,70) 与计量同时有效 -> 拒绝
    try:
        r.settle_storage_costs(from_ts=0, to_ts=100,
                               layer_amounts={"hot": 5},
                               metering=[("hot", "pk", 0, 70, 2)],
                               attributions=[("k1", 1, "cc-k")])
        check("引用跨入计量且缺失归属即拒绝", False)
    except SettlementError as exc:
        check("引用跨入计量且缺失归属即拒绝", "k2 v1" in str(exc),
              str(exc))
    r.close()


# ============================================================ 6. 随机差分
def _random_scenario(rng):
    """生成一个随机小坐标场景（保证归属覆盖所有可能跨计量的版本）。"""
    period = rng.randint(8, 40)
    layers = ["hot", "cold"]
    centers = ["cc-a", "cc-b", "cc-c"]
    contents = []
    for layer in layers:
        for pi in range(rng.randint(1, 2)):
            contents.append((layer, f"p{pi}"))   # 同名 payload 跨层复用
    objects, metering, attributions = [], [], []
    amounts = {}
    for (layer, payload) in contents:
        # 随机非重叠计量段（允许空档与紧贴）
        t, segs = 0, []
        while t < period and rng.random() < 0.85:
            t += rng.randint(0, 3)               # 可能留空档
            if t >= period:
                break
            e = min(period, t + rng.randint(1, 6))
            segs.append((layer, payload, t, e, rng.randint(1, 9)))
            t = e
        metering.extend(segs)
        # 随机逻辑版本：创建时刻、回收时刻（可越出结算期，裁剪后验证）
        for vi in range(rng.randint(0, 4)):
            oid = f"{layer}-{payload}-o{vi}"
            created = rng.randint(0, period - 1)
            objects.append(make_obj(oid, 1, layer, created, payload))
            if rng.random() < 0.5:
                mark = created + rng.randint(1, period + 5)
                objects[-1]["_recycled_at"] = mark
            attributions.append((oid, 1, rng.choice(centers)))
        if segs:
            amounts[layer] = rng.randint(0, 200)
    for layer in layers:
        # 无计量的层只能零费用（正费用层没有任何计量属于无效输入）
        amounts.setdefault(layer, 0)
    return period, objects, metering, attributions, amounts


def test_randomized_differential_small_coordinates():
    """随机小坐标场景：产品结果 vs 按秒独立 oracle（200 个种子）。"""
    mismatches = 0
    for seed in range(200):
        rng = random.Random(seed)
        period, objects, metering, attributions, amounts = \
            _random_scenario(rng)
        r = Reclaimer.in_memory(backend=MemoryBackend())
        r.import_objects([{k: v for k, v in o.items() if not k.startswith("_")}
                          for o in objects])
        for o in objects:
            if "_recycled_at" in o:
                mark_recycled(r, o["object_id"], 1, o["_recycled_at"])
        bill = r.settle_storage_costs(
            from_ts=0, to_ts=period, layer_amounts=amounts,
            metering=iter(metering), attributions=iter(attributions))
        oracle_objs = [
            {**o, "recycled_at": o.get("_recycled_at")} for o in objects]
        exp_shares = oracle_shares(oracle_objs, metering, attributions,
                                   0, period)
        got = item_map(bill)
        ok = set(got) == set(exp_shares) and all(
            got[k][0] == exp_shares[k] for k in exp_shares)
        exp_cents = {}
        for layer in {l for (l, _) in exp_shares} | set(amounts):
            layer_shares = {c: s for (l, c), s in exp_shares.items()
                            if l == layer}
            if layer_shares:
                for c, cents in oracle_cents(amounts.get(layer, 0),
                                             layer_shares).items():
                    exp_cents[(layer, c)] = cents
        ok = ok and all(got[k][1] == exp_cents[k] for k in exp_cents)
        ok = ok and all(
            sum(it["cents"] for it in bill["items"]
                if it["storage_layer"] == layer) == due
            for layer, due in amounts.items()
            if any(l == layer for (l, _) in exp_shares) or due == 0)
        if not ok:
            mismatches += 1
            print(f"  seed={seed} 不一致: got={got} exp={exp_shares}")
        r.close()
    check("200 个随机场景与按秒 oracle 全部一致", mismatches == 0)


def test_many_contents_streaming():
    """大量物理内容：归并两条有序流，单份内容驻留，总耗时可控。"""
    r = Reclaimer.in_memory(backend=MemoryBackend())
    n_contents, n_segs, n_refs = 2000, 10, 5
    objs, metering, attributions = [], [], []
    for ci in range(n_contents):
        payload = f"bulk-{ci:05d}"
        for vi in range(n_refs):
            oid = f"b{ci:05d}v{vi}"
            objs.append(make_obj(oid, 1, "hot", vi * 100, payload))
            attributions.append((oid, 1, f"cc-{vi % 3}"))
        for si in range(n_segs):
            metering.append(("hot", payload, si * 1000, si * 1000 + 900,
                             si + 1))
    r.import_objects(objs)
    rng = random.Random(9)
    rng.shuffle(metering)
    rng.shuffle(attributions)
    t0 = time.monotonic()
    bill = r.settle_storage_costs(
        from_ts=0, to_ts=10 ** 6, layer_amounts={"hot": 10 ** 6},
        metering=iter(metering), attributions=iter(attributions))
    elapsed = time.monotonic() - t0
    # 每份内容：首段 [0,900) 跨过 5 个引用边界 -> 5 片，其余 9 段各 1 片，
    # 共 14 片；分摊次数 = 1+2+3+3+3 = 12（span 恒定集合整体分摊）
    check("2000 内容 × 10 段 × 5 引用：工作量计数正确",
          bill["stats"]["contents"] == n_contents
          and bill["stats"]["references"] == n_contents * n_refs
          and bill["stats"]["metered_pieces"] == n_contents * 14
          and bill["stats"]["center_share_additions"] == n_contents * 12,
          f"got {bill['stats']}")
    check("同层各项之和等于应付金额",
          sum(it["cents"] for it in bill["items"]) == 10 ** 6)
    check("大批量归并在 10 秒内完成", elapsed < 10.0, f"{elapsed:.2f}s")
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
    print(f"\n{'='*60}\n总计 {passed}/{len(RESULTS)} 项检查通过")
    failed = [n for n, ok, _ in RESULTS if not ok]
    if failed:
        print("失败项:")
        for n in failed:
            print(f"  - {n}")
        sys.exit(1)


if __name__ == "__main__":
    main()
