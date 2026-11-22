"""端到端测试：产品计划器 vs 独立 oracle + 手工期望集合。

运行：python -m tests.test_reclaimer
（需要在 /workspace 下运行，测试包使用 sys.path 引导，不依赖 pytest）
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import Reclaimer
from reclaimer.errors import (
    LineageValidationError,
    ObjectUnknownError,
    PlanPausedError,
)
from reclaimer.executor import MemoryBackend, SimulatedProcessExit
from reclaimer.schema import (
    ITEM_BLOCKED,
    ITEM_HOLD_WAITING,
    ITEM_RECYCLABLE,
    ITEM_RECYCLED,
    PLAN_COMPLETED,
    PLAN_PAUSED,
    PLAN_READY,
    PLAN_RUNNING,
)

from tests import scenario as sc
from tests.oracle import ReferenceOracle

RESULTS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(cond), detail))
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -- {detail}" if detail and not cond
                                  else ""))


def stage_set(plan_items, stage):
    return {(i["object_id"], i["version"])
            for i in plan_items if i["stage"] == stage}


# ============================================================ 引导
def bootstrap(db_path: str | None = None, *, watermarks=None,
              holds=None, hold_releases=None,
              fail_targets=None, crash_targets=None):
    """用手工场景引导一个产品实例，并返回 (reclaimer, backend)。"""
    backend = MemoryBackend()
    if fail_targets:
        for key, times in fail_targets.items():
            backend.fail_until_attempt[key] = times
    if crash_targets:
        for key in crash_targets:
            backend.crash_after_physical_delete[key] = True

    if db_path:
        r = Reclaimer.open(db_path, backend=backend)
    else:
        r = Reclaimer.in_memory(backend=backend)

    r.import_objects(sc.build_objects())
    for o in sc.build_objects():
        backend.add(o["object_id"], o["version"], o)
    snap = r.import_snapshot(sc.EDGES, snapshot_id="snap-1",
                             description="hand-built lineage")
    r.import_policy(sc.policy_rules(sc.RET_V1, sc.RET_V1_EXTRA),
                    "pol-v1", description="baseline policy")
    r.import_policy(sc.policy_rules(sc.RET_V2), "pol-v2",
                    description="lengthened policy")
    for h in (holds if holds is not None else sc.HOLDS):
        r.import_hold(h)
    for rel in (hold_releases or []):
        r.release_hold(**rel)
    for c in sc.CONSUMERS:
        r.register_consumer(c["consumer_id"],
                            interests=[
                                i["object_id"] for i in sc.INTERESTS
                                if i["consumer_id"] == c["consumer_id"]],
                            description=c["description"])
    for w in (watermarks if watermarks is not None else sc.WATERMARKS_V1):
        r.upsert_watermark(w["consumer_id"], w["object_id"],
                           w["min_version"], recorded_at=sc.T0 + 9_000)
    return r, backend, snap


def oracle_result(*, watermarks=None, holds=None, hold_releases=None,
                  policy_ret=None):
    fixture = sc.build_fixture(
        policy_ret=policy_ret or sc.RET_V1,
        watermarks=watermarks, holds=holds,
        hold_releases=hold_releases,
    )
    return ReferenceOracle(fixture).evaluate(sc.AS_OF), fixture


# ============================================================ 1. oracle 自洽
def test_oracle_matches_hand_expectation():
    ora, _ = oracle_result()
    check("oracle retained == 手工保留集",
          ora.retained == sc.EXPECTED_V1_RETAINED,
          f"got {sorted(map(str, ora.retained))}")
    check("oracle expired == 手工过期集",
          ora.expired == sc.EXPECTED_V1_EXPIRED,
          f"got {sorted(map(str, ora.expired))}")
    check("oracle hold_waiting == 手工冻结等待集",
          ora.hold_waiting == sc.EXPECTED_V1_HOLD_WAITING)
    check("oracle blocked == 手工阻断集",
          ora.blocked == sc.EXPECTED_V1_BLOCKED,
          f"got {sorted(map(str, ora.blocked))}")
    check("oracle recyclable == 手工可回收集",
          ora.recyclable == sc.EXPECTED_V1_RECYCLABLE,
          f"got {sorted(map(str, ora.recyclable))}")
    check("oracle leaf_order == 手工叶序",
          ora.leaf_order == sc.EXPECTED_V1_LEAF_ORDER,
          f"got {ora.leaf_order}")


# ============================================================ 2. 产品计划 == oracle
def test_product_matches_oracle():
    r, backend, snap = bootstrap()
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF, max_batch_size=100)
    items = r.audit.plan_items(plan["plan_id"])

    check("计划初始状态 READY", plan["status"] == PLAN_READY)
    check("产品 冻结等待 == oracle",
          stage_set(items, ITEM_HOLD_WAITING) ==
          sc.EXPECTED_V1_HOLD_WAITING)
    check("产品 阻断 == oracle",
          stage_set(items, ITEM_BLOCKED) == sc.EXPECTED_V1_BLOCKED)
    check("产品 可回收 == oracle",
          stage_set(items, ITEM_RECYCLABLE) ==
          sc.EXPECTED_V1_RECYCLABLE)
    check("产品 候选总数 == oracle 候选总数",
          len(items) == len(sc.EXPECTED_V1_EXPIRED),
          f"got {len(items)}")
    # 仍保留的对象根本不进计划
    planned = {(i["object_id"], i["version"]) for i in items}
    check("保留对象不进计划",
          planned.isdisjoint(sc.EXPECTED_V1_RETAINED))
    r.close()


# ============================================================ 3. 引用证据完备
def test_evidence_completeness():
    r, backend, snap = bootstrap()
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    # 每个阻断/冻结条目都有“为什么不能删”的证据
    for item in r.audit.plan_items(pid):
        if item["stage"] in (ITEM_BLOCKED, ITEM_HOLD_WAITING):
            ev = r.evidence_for(item["object_id"], item["version"], pid)
            check(
                f"证据存在: {item['object_id']} v{item['version']} "
                f"({item['stage']})",
                len(ev) >= 1,
            )
    # m1 v1 必须有水位证据
    m1_ev = r.evidence_for("m1", 1, pid)
    check("m1 v1 含 WATERMARK 证据",
          any(e["evidence_kind"] == "WATERMARK" for e in m1_ev))
    # t1 v1 必须有存活下游证据指向 m1 v1
    t1_ev = r.evidence_for("t1", 1, pid)
    check("t1 v1 含 LIVE_DOWNSTREAM 证据(m1 v1)",
          any(e["evidence_kind"] == "LIVE_DOWNSTREAM"
              and e["ref_object_id"] == "m1" and e["ref_version"] == 1
              for e in t1_ev))
    r.close()


# ============================================================ 4. 执行：叶序 + 墓碑 + 审计三态
def test_execute_leaf_order_and_tombstone():
    r, backend, snap = bootstrap()
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    check("预检通过", r.precheck(pid)["ok"] is True)

    # 单步删除，验证叶序（本场景唯一叶：孤立对象 raw_c v1）
    s1 = r.execute(pid, max_deletes=1)
    order1 = [e for e in r.audit.execution_log(pid)
              if e["action"] == "DELETE_OK"]
    check("第一片叶是 raw_c v1",
          (order1[0]["object_id"], order1[0]["version"]) == ("raw_c", 1))
    check("raw_c v1 物理后端已删",
          backend.physical_status("raw_c", 1) == "RECYCLED")

    st = r.object_status("raw_c", 1)
    check("已回收对象返回明确 RECYCLED 状态", st["status"] == "RECYCLED")
    check("墓碑保留原位置",
          st["tombstone"]["former_layer"] == "cold"
          and st["tombstone"]["plan_id"] == pid)

    s2 = r.execute(pid)
    check("计划完成", s2["status"] == PLAN_COMPLETED)
    recycled = stage_set(r.audit.plan_items(pid), ITEM_RECYCLED)
    check("可回收对象全部回收", recycled == sc.EXPECTED_V1_RECYCLABLE)

    # 审计三态
    check("LIVE 对象状态正确",
          r.object_status("m1", 1)["status"] == "LIVE")
    check("RECYCLED 不伪造成不存在",
          r.object_status("raw_c", 1)["status"] == "RECYCLED")
    check("未知身份返回 UNKNOWN",
          r.object_status("ghost", 9)["status"] == "UNKNOWN")
    try:
        r.audit.require_known("ghost", 9)
        check("UNKNOWN 抛 ObjectUnknownError", False)
    except ObjectUnknownError:
        check("UNKNOWN 抛 ObjectUnknownError", True)

    # 删除证明：每个 RECYCLED 条目都有零引用证明
    proofs = r.audit.deletion_proof(pid)
    check("每个回收对象有 RECYCLE_PROOF",
          len(proofs) == len(sc.EXPECTED_V1_RECYCLABLE)
          and all(p["proof_detail"] for p in proofs))
    check("回收对象引用计数均为 0",
          all(p["current_refcount"] == 0 for p in proofs))
    r.close()


# ============================================================ 5. 策略漂移 -> 暂停 + 差异
def test_policy_drift_pauses_plan():
    r, backend, snap = bootstrap()
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    # 预检后修改策略规则（模拟策略版本内容被替换）
    with r.conn:
        r.conn.execute(
            "UPDATE policy_rules SET retention_seconds=? "
            "WHERE policy_version=? AND obj_type='raw_file'",
            (9_999_999, "pol-v1"))
        r.conn.execute(
            "UPDATE policy_versions SET fingerprint=? "
            "WHERE policy_version='pol-v1'",
            ("tampered",))
    verdict = r.precheck(pid)
    check("策略漂移预检失败", verdict["ok"] is False)
    check("差异类型为 POLICY",
          any(d["kind"] == "POLICY" for d in verdict["diffs"]))
    check("计划被暂停", r.plan_summary(pid)["status"] == PLAN_PAUSED)
    check("差异已落审计表", len(r.plan_diffs(pid)) >= 1)
    try:
        r.execute(pid)
        check("暂停计划拒绝执行", False)
    except PlanPausedError:
        check("暂停计划拒绝执行", True)
    # 没有任何对象被删
    check("暂停后零删除",
          r.object_status("raw_c", 1)["status"] == "LIVE")
    r.close()


# ============================================================ 6. 水位漂移 -> 暂停
def test_watermark_drift_pauses_plan():
    r, backend, snap = bootstrap()
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    # 水位变化：c-bi 对 t1 的水位 2 -> 1
    r.upsert_watermark("c-bi", "t1", 1, recorded_at=sc.AS_OF + 1)
    verdict = r.precheck(pid)
    check("水位漂移预检失败", verdict["ok"] is False)
    check("差异类型为 WATERMARKS",
          any(d["kind"] == "WATERMARKS" for d in verdict["diffs"]))
    check("计划暂停", r.plan_summary(pid)["status"] == PLAN_PAUSED)
    r.close()


# ============================================================ 7. 水位不一致 -> 阻断回收
def test_inconsistent_watermark_blocks():
    r, backend, snap = bootstrap(watermarks=sc.WATERMARKS_CONFLICT)
    issues = r.detect_watermark_issues()
    check("检测到水位冲突问题",
          any(i["kind"] == "CONFLICT" and i["object_id"] == "m1"
              for i in issues))
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    items = r.audit.plan_items(plan["plan_id"])
    m1 = [i for i in items if i["object_id"] == "m1"][0]
    check("冲突身份 m1 被阻断且原因记录",
          m1["stage"] == ITEM_BLOCKED and m1["block_reason"])
    # 缺失水位：注册新订阅但不给水位（raw_c 是本计划唯一可回收对象）
    r.register_consumer("c-audit", interests=["raw_c"])
    r.detect_watermark_issues()
    plan2 = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                            as_of_ts=sc.AS_OF)
    items2 = r.audit.plan_items(plan2["plan_id"])
    rc1 = [i for i in items2 if (i["object_id"], i["version"])
           == ("raw_c", 1)][0]
    check("水位缺失身份 raw_c 不得回收",
          rc1["stage"] == ITEM_BLOCKED)
    r.close()


# ============================================================ 8. 策略 v2：新计划集合同 oracle
def test_policy_v2_new_plan():
    r, backend, snap = bootstrap()
    ora, _ = oracle_result(policy_ret=sc.RET_V2)
    plan = r.generate_plan(policy_version="pol-v2", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    items = r.audit.plan_items(plan["plan_id"])
    check("v2 可回收集合为空（与手工期望一致）",
          stage_set(items, ITEM_RECYCLABLE) == sc.EXPECTED_V2_RECYCLABLE)
    check("v2 阻断集合匹配手工期望",
          stage_set(items, ITEM_BLOCKED) == sc.EXPECTED_V2_BLOCKED)
    check("v2 无冻结等待条目",
          stage_set(items, ITEM_HOLD_WAITING) ==
          sc.EXPECTED_V2_HOLD_WAITING)
    check("v2 产品阻断 == oracle",
          stage_set(items, ITEM_BLOCKED) == ora.blocked)
    r.close()


# ============================================================ 9. 冻结解除只能通过明确事件
def test_hold_release_only_via_event():
    rel = [{"hold_id": "hold-m2", "event_ref": "LEGAL-EVT-42",
            "note": "matter closed"}]
    r, backend, snap = bootstrap(hold_releases=rel)
    # 解除事件不可重复
    try:
        r.release_hold("hold-m2", "LEGAL-EVT-43")
        check("重复解除被拒绝", False)
    except LineageValidationError:
        check("重复解除被拒绝", True)
    ora, _ = oracle_result(hold_releases=rel)
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    items = r.audit.plan_items(plan["plan_id"])
    m2 = [i for i in items if i["object_id"] == "m2"][0]
    # m2 v1 解除后：无水位要求 m2 的消费者？c-ml 要求 m2>=1 -> 仍被水位挡住
    check("解除后 m2 v1 不再 HOLD_WAITING",
          m2["stage"] != ITEM_HOLD_WAITING)
    check("m2 v1 转为水位阻断",
          m2["stage"] == ITEM_BLOCKED
          and "earliest required version" in (m2["block_reason"] or ""))
    check("oracle 同样不再冻结等待",
          ("m2", 1) not in ora.hold_waiting)
    r.close()


# ============================================================ 10. 回收失败可重试，不重复删除
def test_failure_retry_no_double_delete():
    r, backend, snap = bootstrap()
    # raw_c v1 前两次删除尝试抛错，第三次成功（跨两次调用重试）
    backend.fail_until_attempt = {("raw_c", 1): 2}
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    # 第一轮：失败一次，条目留存 FAILED（不在同一轮内立即重试）
    s = r.execute(pid)
    check("出现 FAILED 条目",
          any(i["stage"] == "FAILED"
              for i in r.audit.plan_items(pid)))
    check("raw_c v1 尚未回收（失败期间）",
          r.object_status("raw_c", 1)["status"] == "LIVE")
    failed_calls = [c for c in backend.delete_calls if c == ("raw_c", 1)]
    check("首轮失败尝试被记录（1 次调用）", len(failed_calls) == 1)
    check("计划保持 RUNNING 未完成",
          r.plan_summary(pid)["status"] == PLAN_RUNNING)

    # 第二轮：再次失败（累计第 2 次）
    r.execute(pid)
    check("仍处 FAILED（第 2 次失败）",
          any(i["stage"] == "FAILED" and i["attempts"] == 2
              for i in r.audit.plan_items(pid)))

    # 第三轮：后端恢复后成功；物理删除只发生一次
    before = len(backend.delete_calls)
    s2 = r.execute(pid)
    after_calls = [c for c in backend.delete_calls if c == ("raw_c", 1)]
    check("重试后计划完成", s2["status"] == PLAN_COMPLETED)
    check("raw_c v1 共调用 3 次、物理删除成功 1 次",
          len(after_calls) == 3)
    check("最后一轮只新增一次调用（不重复物理删除）",
          len(backend.delete_calls) - before == 1)
    check("raw_c v1 墓碑存在",
          r.object_status("raw_c", 1)["status"] == "RECYCLED")
    r.close()


# ============================================================ 11. 进程退出后重开
def test_process_exit_recovery():
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "recovery.db")
        r1, backend1, snap = bootstrap(
            db_path, crash_targets={("raw_c", 1)})
        plan = r1.generate_plan(policy_version="pol-v1",
                                snapshot_id=snap, as_of_ts=sc.AS_OF)
        pid = plan["plan_id"]
        try:
            r1.execute(pid)
            check("崩溃异常上抛", False)
        except SimulatedProcessExit:
            check("物理删除后、提交前模拟退出", True)
        # raw_c v1 物理已删但计划事务回滚，库内仍标记 LIVE
        check("崩溃对象在库内仍 LIVE（事务回滚）",
              r1.object_status("raw_c", 1)["status"] == "LIVE")
        r1.close()

        # 重开：新连接 + 新后端实例（模拟新进程），物理层 raw_c 已不存在
        backend2 = MemoryBackend()
        for o in sc.build_objects():
            if (o["object_id"], o["version"]) != ("raw_c", 1):
                backend2.add(o["object_id"], o["version"], o)
        r2 = Reclaimer.open(db_path, backend=backend2)
        check("重开后崩溃对象不能被复活为可读",
              r2.object_status("raw_c", 1)["status"] == "LIVE"
              and backend2.physical_status("raw_c", 1) == "RECYCLED")
        rec = r2.execute(pid)
        check("重开后计划继续并完成", rec["status"] == PLAN_COMPLETED)
        check("raw_c v1 最终 RECYCLED（幂等，无二次物理删除）",
              r2.object_status("raw_c", 1)["status"] == "RECYCLED")
        log = r2.audit.execution_log(pid)
        check("日志记录 physical_delete=False 的幂等恢复",
              any(e["action"] == "DELETE_OK"
                  and "physical_delete=False" in (e["detail"] or "")
                  and e["object_id"] == "raw_c"
                  for e in log))
        # 触发器兜底：尝试手工复活必须失败
        try:
            r2.conn.execute(
                "UPDATE objects SET lifecycle='LIVE' "
                "WHERE object_id='raw_c' AND version=1")
            check("触发器禁止已回收对象复活", False)
        except sqlite3.IntegrityError:
            check("触发器禁止已回收对象复活", True)
        r2.close()


# ============================================================ 12. 大对象：有界批次 + 不全图驻留
def test_large_graph_bounded_batches():
    r = Reclaimer.in_memory()
    n = 2500
    objs = [
        {"object_id": f"bulk_{i:05d}", "version": 1, "obj_type": "raw_file",
         "classification": "public", "environment": "prod",
         "storage_layer": "cold", "created_at": sc.T0,
         "payload_ref": f"cold://bulk_{i}"}
        for i in range(n)
    ]
    r.import_objects(objs)
    # 短保留期策略：bulk 对象在 AS_OF 前全部过期（T0+3M < AS_OF）
    r.import_policy([
        {"rule_id": "bulk-raw", "obj_type": "raw_file",
         "classification": None, "environment": None,
         "retention_seconds": 3_000_000},
    ], "bulk-pol")
    snap = r.import_snapshot([], snapshot_id="bulk-snap")
    plan = r.generate_plan(policy_version="bulk-pol", snapshot_id=snap,
                           as_of_ts=sc.AS_OF,
                           max_batch_size=300, max_plan_items=1000)
    pid = plan["plan_id"]
    check("计划条目受 max_plan_items 约束",
          plan["stage_counts"].get("RECYCLABLE", 0) == 1000)
    check("批次大小有界（ceil(1000/300)=4 批）",
          plan["batch_count"] == 4)
    # 执行不需要全图：分两轮跑完
    r.execute(pid, max_deletes=400)
    mid = r.plan_summary(pid)
    check("部分执行后仍 RUNNING", mid["status"] == PLAN_RUNNING)
    r.execute(pid)
    check("有界计划全部完成",
          r.plan_summary(pid)["status"] == PLAN_COMPLETED)
    check("剩余对象留给下一计划（未入本计划）",
          r.object_status("bulk_01000", 1)["status"] == "LIVE")
    # 下一计划可以接着回收
    plan2 = r.generate_plan(policy_version="bulk-pol",
                            snapshot_id=snap, as_of_ts=sc.AS_OF,
                            max_batch_size=500, max_plan_items=5000)
    r.execute(plan2["plan_id"])
    check("第二计划回收其余对象",
          r.object_status("bulk_01000", 1)["status"] == "RECYCLED")
    r.close()


# ============================================================ 13. 谱系不变量
def test_lineage_invariants():
    r = Reclaimer.in_memory()
    r.import_objects(sc.build_objects())
    # 未知端点
    try:
        r.import_snapshot([
            {"src_id": "t1", "src_version": 1,
             "dst_id": "nope", "dst_version": 1}])
        check("未知端点边被拒绝", False)
    except LineageValidationError:
        check("未知端点边被拒绝", True)
    # 自环
    try:
        r.import_snapshot([
            {"src_id": "t1", "src_version": 1,
             "dst_id": "t1", "dst_version": 1}])
        check("自环边被拒绝", False)
    except LineageValidationError:
        check("自环边被拒绝", True)
    # 环：a->b, b->a（跨版本构造两条真实对象）
    try:
        r.import_snapshot([
            {"src_id": "t1", "src_version": 1,
             "dst_id": "t1", "dst_version": 2},
            {"src_id": "t1", "src_version": 2,
             "dst_id": "t1", "dst_version": 1},
        ], snapshot_id="cyclic")
        check("环被检测并拒绝", False)
    except LineageValidationError:
        check("环被检测并拒绝", True)
    # 已回收对象不可重新导入复活
    r.import_policy(sc.policy_rules(sc.RET_V1, sc.RET_V1_EXTRA), "p")
    snap = r.import_snapshot([], snapshot_id="empty")
    plan = r.generate_plan(policy_version="p", snapshot_id=snap,
                           as_of_ts=sc.AS_OF + 10_000_000)
    r.execute(plan["plan_id"])
    try:
        r.import_objects([
            {"object_id": "raw_c", "version": 1, "obj_type": "raw_file",
             "classification": "public", "environment": "prod",
             "storage_layer": "cold", "created_at": sc.T0}])
        check("已回收对象重新导入被拒绝", False)
    except LineageValidationError:
        check("已回收对象重新导入被拒绝", True)
    r.close()


# ============================================================ 14. 级联：存活下游阻断整条上游链 + 引用计数
def test_cascade_unblock_shared_parent():
    """链 y->x、z->y（边 src 依赖 dst）：

    场景 A：z 保留 -> 存活沿依赖链传播，y 与 x 都被阻断（无叶可删）；
    场景 B：z 也过期且无水位 -> 整条链按叶序 x -> y -> z 回收，
            引用计数随删除级联递减，共享父节点不会被重复计数。
    """
    # ---------------- 场景 A：存活下游保护整条链
    r = Reclaimer.in_memory()
    objs = [
        {"object_id": "x", "version": 1, "obj_type": "raw_file",
         "classification": "public", "environment": "prod",
         "storage_layer": "cold", "created_at": sc.T0,
         "payload_ref": "cold://x"},
        {"object_id": "y", "version": 1, "obj_type": "table",
         "classification": "public", "environment": "prod",
         "storage_layer": "hot", "created_at": sc.T0 + 10,
         "payload_ref": "hot://y"},
        {"object_id": "z", "version": 1, "obj_type": "metric",
         "classification": "public", "environment": "prod",
         "storage_layer": "hot", "created_at": sc.T0 + 20,
         "payload_ref": "hot://z"},
    ]
    r.import_objects(objs)
    edges = [
        {"src_id": "y", "src_version": 1, "dst_id": "x", "dst_version": 1},
        {"src_id": "z", "src_version": 1, "dst_id": "y", "dst_version": 1},
    ]
    snap = r.import_snapshot(edges, snapshot_id="chain")
    # raw 3M 过期；table 5M 过期；metric 20M 保留
    r.import_policy([
        {"rule_id": "r1", "obj_type": "raw_file",
         "retention_seconds": 3_000_000},
        {"rule_id": "r2", "obj_type": "table",
         "retention_seconds": 5_000_000},
        {"rule_id": "r3", "obj_type": "metric",
         "retention_seconds": 20_000_000},
    ], "chain-pol")
    plan = r.generate_plan(policy_version="chain-pol",
                           snapshot_id=snap, as_of_ts=sc.AS_OF)
    items = {(i["object_id"], i["version"]): i
             for i in r.audit.plan_items(plan["plan_id"])}
    check("A: 存活 z 使 y 阻断",
          items[("y", 1)]["stage"] == ITEM_BLOCKED)
    check("A: 存活沿链传播使 x 也阻断（无叶可删）",
          items[("x", 1)]["stage"] == ITEM_BLOCKED)
    check("A: y 引用计数为 1（被存活 z 引用）",
          items[("y", 1)]["refcount_at_plan"] == 1)
    check("A: 阻断证据指向 z",
          any(e["ref_object_id"] == "z"
              for e in r.evidence_for("y", 1, plan["plan_id"])))
    r.execute(plan["plan_id"])
    check("A: 三对象均保持 LIVE，零误删",
          all(r.object_status(k, 1)["status"] == "LIVE"
              for k in ("x", "y", "z")))
    r.close()

    # ---------------- 场景 B：全链过期 -> 叶序回收 + 引用计数级联
    r = Reclaimer.in_memory()
    r.import_objects(objs)
    snap = r.import_snapshot(edges, snapshot_id="chain2")
    r.import_policy([
        {"rule_id": "r1", "obj_type": "raw_file",
         "retention_seconds": 3_000_000},
        {"rule_id": "r2", "obj_type": "table",
         "retention_seconds": 5_000_000},
        {"rule_id": "r3", "obj_type": "metric",
         "retention_seconds": 7_000_000},   # z 也过期
    ], "chain-pol2")
    plan = r.generate_plan(policy_version="chain-pol2",
                           snapshot_id=snap, as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    items = {(i["object_id"], i["version"]): i
             for i in r.audit.plan_items(pid)}
    check("B: 无存活对象，三节点全部可回收",
          {items[k]["stage"] for k in items} == {ITEM_RECYCLABLE})
    # 全部过期时不存在存活引用者，引用计数全为 0；
    # 叶序由批次内 (created_at, object_id, version) 确定序保证
    check("B: 无存活引用者，引用计数全为 0",
          all(items[k]["current_refcount"] == 0 for k in items))
    # 逐步删除验证叶序：x(created 最小) 先于 y 先于 z
    r.execute(pid, max_deletes=1)
    order = [(e["object_id"], e["version"])
             for e in r.audit.execution_log(pid)
             if e["action"] == "DELETE_OK"]
    check("B: 第一片叶是 x", order == [("x", 1)])
    y_item = [i for i in r.audit.plan_items(pid)
              if i["object_id"] == "y"][0]
    check("B: 删除 x 后 y 仍保持零计数（无存活引用）",
          y_item["current_refcount"] == 0)
    r.execute(pid)
    check("B: 全链按叶序回收完成",
          r.plan_summary(pid)["status"] == PLAN_COMPLETED)
    order = [(e["object_id"], e["version"])
             for e in r.audit.execution_log(pid)
             if e["action"] == "DELETE_OK"]
    check("B: 叶序 x -> y -> z", order == [("x", 1), ("y", 1), ("z", 1)])
    check("B: 三对象均有墓碑",
          all(r.object_status(k, 1)["status"] == "RECYCLED"
              for k in ("x", "y", "z")))
    r.close()


# ============================================================ 15. 共享父节点：引用计数按存活引用者计数并级联
def test_shared_parent_refcount_cascade():
    """p 被两个存活子节点 c1/c2 共同依赖：

        c1 -> p <- c2     （边 src 依赖 dst）

    - p 的存活引用计数为 2，必须等两个子节点都删除后才归零；
    - 删除 c1 后 p 计数 2->1 且仍阻断；删除 c2 后 1->0，
      且 p 无其他存活路径时才级联解锁为可回收。
    """
    r = Reclaimer.in_memory()
    objs = [
        {"object_id": "p", "version": 1, "obj_type": "raw_file",
         "classification": "public", "environment": "prod",
         "storage_layer": "cold", "created_at": sc.T0,
         "payload_ref": "cold://p"},
        {"object_id": "c1", "version": 1, "obj_type": "table",
         "classification": "public", "environment": "prod",
         "storage_layer": "hot", "created_at": sc.T0 + 10,
         "payload_ref": "hot://c1"},
        {"object_id": "c2", "version": 1, "obj_type": "table",
         "classification": "public", "environment": "dev",
         "storage_layer": "hot", "created_at": sc.T0 + 20,
         "payload_ref": "hot://c2"},
    ]
    r.import_objects(objs)
    # c1 保留（prod 表长保留期），c2 过期（dev 表短保留期）但被水位保护
    snap = r.import_snapshot([
        {"src_id": "c1", "src_version": 1,
         "dst_id": "p", "dst_version": 1},
        {"src_id": "c2", "src_version": 1,
         "dst_id": "p", "dst_version": 1},
    ], snapshot_id="shared")
    r.import_policy([
        {"rule_id": "r1", "obj_type": "raw_file",
         "classification": None, "environment": None,
         "retention_seconds": 3_000_000},      # p 过期
        {"rule_id": "r2", "obj_type": "table",
         "classification": None, "environment": "prod",
         "retention_seconds": 20_000_000},     # c1 保留
        {"rule_id": "r3", "obj_type": "table",
         "classification": None, "environment": "dev",
         "retention_seconds": 5_000_000},      # c2 过期，靠水位存活
    ], "shared-pol")
    r.register_consumer("c-cons", interests=["c2"])
    r.upsert_watermark("c-cons", "c2", 1, recorded_at=sc.T0 + 9_000)
    plan = r.generate_plan(policy_version="shared-pol", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    pid = plan["plan_id"]
    items = {(i["object_id"], i["version"]): i
             for i in r.audit.plan_items(pid)}
    check("共享父 p 被阻断", items[("p", 1)]["stage"] == ITEM_BLOCKED)
    check("p 存活引用计数为 2（c1 保留 + c2 水位保护）",
          items[("p", 1)]["refcount_at_plan"] == 2)
    check("c2 被水位阻断",
          items[("c2", 1)]["stage"] == ITEM_BLOCKED)
    # c1 不在计划中（仍保留）
    check("c1 保留不进计划", ("c1", 1) not in items)

    # 水位解除（删除订阅/水位使 c2 不再受保护）：c2 成为可回收叶
    with r.conn:
        r.conn.execute(
            "DELETE FROM consumer_watermarks WHERE object_id='c2'")
        r.conn.execute(
            "DELETE FROM consumer_interests WHERE object_id='c2'")
    plan2 = r.generate_plan(policy_version="shared-pol", snapshot_id=snap,
                            as_of_ts=sc.AS_OF)
    pid2 = plan2["plan_id"]
    items2 = {(i["object_id"], i["version"]): i
              for i in r.audit.plan_items(pid2)}
    # c1 仍保留 -> p 仍有一个存活引用者，计数应为 1 且继续阻断
    check("解除水位后 p 计数降为 1（c1 仍存活）",
          items2[("p", 1)]["refcount_at_plan"] == 1)
    check("p 仍被 c1 阻断",
          items2[("p", 1)]["stage"] == ITEM_BLOCKED)
    check("c2 成为可回收叶",
          items2[("c2", 1)]["stage"] == ITEM_RECYCLABLE)
    r.execute(pid2)
    check("c2 已回收、p 仍存活",
          r.object_status("c2", 1)["status"] == "RECYCLED"
          and r.object_status("p", 1)["status"] == "LIVE")
    r.close()


# ============================================================ 16. 选择器冻结 + 重复执行幂等
def test_selector_hold_and_idempotent_rerun():
    r, backend, snap = bootstrap()
    # 追加一个按 classification 的选择器冻结，命中所有 confidential 对象
    r.import_hold({
        "hold_id": "hold-confidential",
        "selector_type": "classification",
        "selector_value": "confidential",
        "reason": "audit sweep",
    })
    plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                           as_of_ts=sc.AS_OF)
    items = r.audit.plan_items(plan["plan_id"])
    # t1 两版与 m1 v1 都是 confidential -> 冻结等待（优先于其他阻断原因）
    held = stage_set(items, ITEM_HOLD_WAITING)
    check("选择器冻结命中全部 confidential 候选",
          ("t1", 1) in held and ("t1", 2) in held and ("m1", 1) in held,
          f"held={sorted(map(str, held))}")
    # raw_c 是 public，不受选择器冻结影响，仍可回收
    check("非选择器命中对象 raw_c 仍可回收",
          stage_set(items, ITEM_RECYCLABLE) == {("raw_c", 1)})

    pid = plan["plan_id"]
    r.execute(pid)
    # 计划完成后再次执行必须是空操作，不产生任何新删除调用
    calls_before = len(backend.delete_calls)
    again = r.execute(pid)
    check("重复执行已完成计划返回 COMPLETED 且零新调用",
          again["status"] == PLAN_COMPLETED
          and len(backend.delete_calls) == calls_before)
    check("墓碑恰好一条", len(r.audit.list_recycled()) == 1)
    r.close()


# ============================================================ 17. 文件库重开：审计连续、无复活
def test_file_db_reopen_audit_continuity():
    import sqlite3 as _sqlite3
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "cont.db")
        r1, backend1, snap = bootstrap(db_path)
        plan = r1.generate_plan(policy_version="pol-v1",
                                snapshot_id=snap, as_of_ts=sc.AS_OF)
        pid = plan["plan_id"]
        r1.execute(pid)
        recycled_before = r1.audit.list_recycled()
        events_before = len(r1.audit.audit_events(limit=1000))
        r1.close()

        # 全新进程重开：审计表、墓碑、计划状态全部延续
        r2 = Reclaimer.open(db_path, backend=MemoryBackend())
        check("重开后计划仍 COMPLETED",
              r2.plan_summary(pid)["status"] == PLAN_COMPLETED)
        check("重开后墓碑连续",
              r2.audit.list_recycled() == recycled_before)
        check("重开后审计事件不丢失",
              len(r2.audit.audit_events(limit=1000)) == events_before)
        check("重开后历史位置仍明确返回 RECYCLED",
              r2.object_status("raw_c", 1)["status"] == "RECYCLED")
        # 底层连接层面也无法绕过状态机复活对象
        try:
            r2.conn.execute(
                "UPDATE objects SET lifecycle='LIVE', recycled_at=NULL"
                " WHERE object_id='raw_c'")
            check("文件库触发器同样禁止复活", False)
        except _sqlite3.IntegrityError:
            check("文件库触发器同样禁止复活", True)
        r2.close()


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
