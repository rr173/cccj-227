"""端到端演示：用手工场景跑通 导入 -> 编译 -> 计划 -> 预检 -> 执行 -> 审计。

运行（在 /workspace 下）：
    python3 -I tests/demo.py
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reclaimer import Reclaimer
from reclaimer.executor import MemoryBackend
from tests import scenario as sc
from tests.test_reclaimer import bootstrap


def line(title: str) -> None:
    print("\n" + "=" * 66)
    print(title)
    print("=" * 66)


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "demo.db")
        r, backend, snap = bootstrap(db_path)

        line("1) 谱系与策略已导入；对象清单（身份/版本/层/来源时间）")
        for row in r.conn.execute(
                "SELECT object_id, version, obj_type, storage_layer,"
                " created_at FROM objects ORDER BY object_id, version"):
            print("  ", row)
        print(f"  谱系快照: {snap}；策略版本: pol-v1 / pol-v2")

        line("2) 生成计划（绑定策略/快照/冻结/水位四指纹）")
        plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                               as_of_ts=sc.AS_OF, max_batch_size=100)
        print(f"  plan_id={plan['plan_id']}  status={plan['status']}")
        print(f"  阶段统计: {plan['stage_counts']}")
        print("  指纹绑定:")
        for k, v in plan["fingerprints"].items():
            print(f"    {k:11s}= {v[:20]}...")

        line("3) 分阶段条目（候选 -> 冻结等待/阻断/可回收）")
        for i in r.audit.plan_items(plan["plan_id"]):
            print(f"  {i['object_id']:9s} v{i['version']} -> "
                  f"{i['stage']:13s} rc={i['current_refcount']} "
                  f"{i['block_reason'] or ''}")

        line("4) 不删除证明：每个被挡住的对象都有引用证据")
        for i in r.audit.plan_items(plan["plan_id"]):
            if i["stage"] in ("BLOCKED", "HOLD_WAITING"):
                evs = r.evidence_for(i["object_id"], i["version"],
                                     plan["plan_id"])
                for e in evs:
                    print(f"  {i['object_id']} v{i['version']} <- "
                          f"{e['evidence_kind']}: {e['detail']}")

        line("5) 预检通过后执行（叶序、墓碑、幂等）")
        print("  precheck:", r.precheck(plan["plan_id"]))
        stats = r.execute(plan["plan_id"])
        print(f"  执行结果 status={stats['status']} "
              f"本轮删除={stats['deleted_this_call']}")

        line("6) 审计查询：历史位置三态严格区分")
        for oid, ver in [("raw_c", 1), ("m1", 1), ("m2", 1),
                         ("t1", 1), ("ghost", 9)]:
            st = r.object_status(oid, ver)
            print(f"  {oid} v{ver}: {st['status']:9s} {st['message']}")

        line("7) 删除证明（零引用 + 快照绑定）")
        for p in r.audit.deletion_proof(plan["plan_id"]):
            print(f"  {p['object_id']} v{p['version']} rc="
                  f"{p['current_refcount']} :: {p['proof_detail']}")

        line("8) 漂移演示：预检后改水位，计划必须暂停并出差异")
        r2, backend2, snap2 = bootstrap()
        plan2 = r2.generate_plan(policy_version="pol-v1",
                                 snapshot_id=snap2, as_of_ts=sc.AS_OF)
        r2.upsert_watermark("c-bi", "t1", 1, recorded_at=sc.AS_OF + 1)
        verdict = r2.precheck(plan2["plan_id"])
        print(f"  precheck ok={verdict['ok']} diffs="
              f"{[d['kind'] for d in verdict['diffs']]}")
        print(f"  计划状态 -> {r2.plan_summary(plan2['plan_id'])['status']}")
        print("  审计差异:", r2.plan_diffs(plan2["plan_id"]))
        r2.close()
        r.close()


if __name__ == "__main__":
    main()
