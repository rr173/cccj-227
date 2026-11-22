# 湖仓分阶段回收计划器（Python + SQLite）

对象有不可变身份与版本，派生关系跨版本存在。回收不能“看年龄直接删”，必须
沿谱系判断对象是否仍被**必须保留的结果或消费者**引用。本项目把该判断做成
可审计、可暂停、可崩溃恢复的分阶段计划，而不是一次性删除脚本。

## 1. 目录结构

```
reclaimer/                 产品代码（仅标准库，无第三方依赖）
  schema.py                表结构、索引、状态机触发器（终态不可复活）
  hashing.py               策略/谱系/冻结/水位四类输入的规范化指纹
  importer.py              谱系导入：对象、边（环检测）、策略、冻结、水位
  planner.py               策略编译 + 四阶段计划生成（递归 CTE，不全图驻留）
  precheck.py              执行前重算指纹并比对，漂移即暂停并落差异
  executor.py              叶序删除、失败重试、崩溃恢复、墓碑、引用计数级联
  audit.py                 审计查询：LIVE / RECYCLED / UNKNOWN 三态、证据
  quality.py               分区质量隔离视图（区间元数据，不删数据）
  api.py                   门面 Reclaimer（六个对外接口 + 质量视图接口）
  errors.py                领域异常
tests/
  scenario.py              手工场景与逐对象手工期望集合
  oracle.py                独立参考程序（不 import 任何产品代码）
  test_reclaimer.py        106 项断言
  test_quality_view.py     132 项质量隔离断言 + 逐分区参考模型
  demo.py                  端到端演示
  demo_quality.py          质量隔离演示（逐分区展开）
```

## 2. 阶段状态机

计划内每个过期对象单向推进，**不能直接删除**：

```
            保留期届满
CANDIDATE ───────────────┬─► HOLD_WAITING  法律冻结覆盖，只能等明确解除事件
                         ├─► BLOCKED       水位保护 / 仍被存活下游到达
                         └─► RECYCLABLE    零存活引用，按叶序进入有界批次
                                   │ 执行器删除（条目级事务 + 墓碑）
                                   ▼
                                RECYCLED（终态）
执行失败的 RECYCLABLE ─► FAILED ─（下一轮重试，物理删除幂等不重复）
```

计划本身：`DRAFT -> READY -> RUNNING -> COMPLETED`；预检发现输入漂移则
`-> PAUSED`，差异写入 `plan_diffs`，执行器拒绝继续。

## 3. “为什么不能删”的判定顺序

对每个过保留期的对象依次判断：

1. **法律冻结**：对象级（可全版本）或选择器级冻结命中 → `HOLD_WAITING`。
   冻结只能由 `hold_releases` 中的明确解除事件取消，解除事件不可撤销。
2. **消费者水位**：
   - 水位缺失（有订阅无水位）、冲突（水位倒退）、越界（高于已知最大版本）
     之一存在时，该身份**全部版本阻断，不得回收**；
   - 正常情况下 `version >= min(各消费者水位)` 的版本被保护。
3. **存活下游谱系**：存活种子 = 未过保留期对象 ∪ 水位保护对象 ∪ 问题身份
   全部版本。从种子沿边 `src -> dst`（下游到其依赖的上游）递归传播，
   能被到达的对象一律 `BLOCKED`，并为每条存活引用写 `LIVE_DOWNSTREAM` 证据。
4. 以上均不命中才是 `RECYCLABLE`，删除瞬间再校验 `current_refcount = 0`，
   并写 `RECYCLE_PROOF` 零引用证明。

## 4. 一次计划绑定四个不可变输入

`plans` 行固化四个 SHA-256 指纹：策略规则、谱系快照边集合、有效冻结集合、
消费者水位（含未解决的水位问题）。执行前 `precheck` 重算当前指纹并与绑定值
比对，任一不同即暂停：

| 漂移来源      | 差异 kind   | 后果 |
|---------------|-------------|------|
| 策略规则变化  | `POLICY`    | PAUSED + plan_diffs，拒绝执行 |
| 谱系快照变化  | `SNAPSHOT`  | 同上 |
| 冻结/解除变化 | `HOLDS`     | 同上 |
| 水位/问题变化 | `WATERMARKS`| 同上 |

## 5. 执行安全不变量

- **叶序**：只删 `current_refcount=0` 的条目，顺序按
  `批次号 -> created_at -> object_id -> version` 全局确定；删除一个对象后
  递减其上游的存活引用计数。
- **条目级事务**：物理删除调用 + 对象置 RECYCLED + 墓碑 + 阶段推进 +
  审计日志原子提交。
- **幂等不重复删除**：后端协议要求 `delete()` 幂等；崩溃恢复时对象物理上
  已不存在，后端返回 `physical_delete=False`，不会产生第二次物理删除。
- **终态不可复活**：`trg_object_no_revive` 触发器禁止 RECYCLED → LIVE；
  重新导入已回收对象直接报错；墓碑表只追加不可删。
- **崩溃恢复**：所有进度在 SQLite 中，新进程重开库即可继续；已提交的
  RECYCLED 不会回滚，未提交的物理删除在重试时走幂等路径。
- **有界内存**：图遍历全部是 SQLite 递归 CTE；`max_plan_items` 限制单计划
  条目数、`max_batch_size` 限制批次，剩余对象留给下一计划，应用层不把
  全图常驻内存。

## 6. 审计三态（严格区分，绝不伪造）

| 状态       | 含义 | 附带信息 |
|------------|------|----------|
| `LIVE`     | 对象存活 | 当前存储层、payload 引用、物理后端状态 |
| `RECYCLED` | 明确已回收 | 墓碑：回收计划、时间、**原历史位置** |
| `UNKNOWN`  | 身份从未进入任何快照 | 这才是“不存在” |

## 7. 六个对外接口（`reclaimer.api.Reclaimer`）

```python
r = Reclaimer.open("lakehouse.db", backend=MemoryBackend())

r.import_objects([...])                 # 1. 谱系导入
r.import_snapshot(edges)
r.import_policy(rules, "pol-v1")        # 2. 策略编译（具体度优先）
r.import_hold(hold); r.release_hold(...) #    冻结与明确解除事件
r.register_consumer(...); r.upsert_watermark(...)

plan = r.generate_plan(policy_version="pol-v1", snapshot_id=snap,
                       as_of_ts=..., max_batch_size=300, max_plan_items=1000)
r.precheck(plan["plan_id"])             # 4. 预检（漂移即暂停）
r.execute(plan["plan_id"])              # 5. 执行（可反复调用、可恢复）
r.object_status("raw_c", 1)             # 6. 审计查询
r.evidence_for("t1", 1, plan["plan_id"])
```

## 7b. 分区质量隔离视图（`Reclaimer.quality_*`）

下游发现坏数时，只需停用**少数分区**，而不是整张下游表。质量视图只维护
元数据：不读数据文件、不删除数据、不改变任何回收规则。

**坐标**：区间 `[start, end)` 的坐标是**对象版本内的分区序号**，不是版本号。
每个登记对象版本声明分区总数，合法区间是 `[0, partition_count)` 内的非空
整数半开区间。

**登记固定映射**（每个快照一份；快照之间完全隔离）：

```python
r.register_quality_view(
    snapshot_id="snap-1",
    objects=[                      # 参与对象版本 + 分区总数
        {"object_id": "raw",  "version": 1, "partition_count": 8},
        {"object_id": "a",    "version": 1, "partition_count": 8},
        {"object_id": "join", "version": 1, "partition_count": 8},
        {"object_id": "solo", "version": 1, "partition_count": 4},
    ],
    mappings=[                     # 每条快照边至少一条；可交叠、不必铺满
        {"src_id": "a", "src_version": 1,          # 派生对象
         "dst_id": "raw", "dst_version": 1,        # 被读取的上游
         "read": [0, 4],                           # 上游读取区间
         "out":  [0, 4]},                          # 下游产出区间
        {"src_id": "join", "src_version": 1,
         "dst_id": "a", "dst_version": 1,
         "read": [0, 4], "out": [0, 2]},
    ],
)
```

方向沿用库里既有 `lineage_edges`：`src` 依赖 `dst`，坏数从 `dst` 传给
`src`。读取区间内任何分区被隔离，对应产出区间**整体**隔离。登记时校验
端点已在库内、每条快照边至少一条映射、区间非空且不越界；无效定义不会
留下半份映射。除边的端点外，也可登记库内已知但该快照没有边的对象版本。

**报障与解除**（一次调用内原子生效，含全部下游变化）：

```python
r.report_quality_fault("snap-1", "INC-42", "raw", 1, 1, 5)
# -> {"status": "reported", "intervals": [[1, 5]], "stats": {...}}
r.release_quality_fault("snap-1", "INC-42")      # 确认修复
```

- 编号在快照内唯一；重发同一报障返回 `duplicate` 且无变化，重复解除返回
  `already-released`；**重放已解除的旧报障不会重新激活**；同编号换载荷
  或解除未知编号抛 `QualityFaultError`。
- 一个节点的隔离范围 = 自身未解除报障 ∪ 所有命中的上游映射产出区间，
  并沿派生链传到下游；多路汇合取并集，不同故障可覆盖同一分区。
- 解除只移除**失去全部支持**的部分：若另一条故障或另一条活跃映射仍覆盖
  该区间，它保持隔离。
- 已回收版本仍可作为质量归因上游：回收不自动解除它对存活下游的影响。

**查询**（供数程序按对象版本取当前必须避开的区间）：

```python
r.quality_ranges("snap-1", "join", 1)
# {"status": "AVAILABLE", "intervals": [[0, 2], [4, 6]], ...}
# 空列表 [] 只表示“已登记且当前可读”的版本没有质量隔离。
# status="RECYCLED"  -> 版本已回收，不可当成可供数（intervals=None）；
# status="NO_QUALITY_VIEW" -> 快照/版本未登记质量视图。
```

**性能组织**：隔离区间、报障、映射贡献都按端点存储；节点当前区间是各
“支持”区间的并集，查询只读物化结果。报障/解除只检查读取区间与本次
变化相交的映射；节点并集没变就不向后继传播。10^9 规模的长区间也只存
一个端点区间，不受影响的大支路不参与单次更新。

## 8. 测试方法

```bash
# 106 项断言（-I 隔离模式，不信任工作目录中的模块）
python3 -I tests/test_reclaimer.py

# 132 项质量隔离断言（含独立逐分区参考模型）
python3 -I tests/test_quality_view.py

# 端到端演示：回收计划器 / 质量隔离视图
python3 -I tests/demo.py
python3 -I tests/demo_quality.py
```

### 手工场景（tests/scenario.py）

```
raw_a v1 ─────────────► t1 v1 ──┬──► m1 v1 ──► audit_ev v1
raw_a v2 ──┐                     │      ▲
           ├────────► t1 v2 ─────┘      └── m2 v1（共享父 m1 v1）
raw_b v1 ──┘
raw_c v1（孤立对象，public 短保留期规则命中）
```

覆盖要素：原始文件→规范表→指标→审计证据链；两个消费者不同水位
（c-bi 低水位 / c-ml 高水位）；一个法律冻结（m2 v1）；两版策略
（v2 拉长 metric 保留期）；孤立对象；共享父节点；注入删除失败与
“物理删除后、提交前退出”的进程崩溃。

### 独立参考程序（tests/oracle.py）

`oracle.py` **不 import 任何产品代码**，只用标准库从同一 JSON 夹具做朴素
内存图计算（保留期匹配、冻结命中、水位保护、BFS 存活传播、引用计数、
叶序模拟），测试把它的输出与产品计划器的阶段集合、叶序逐集合比对；
同时两者都与 `scenario.py` 中手写的期望集合核对，形成三方交叉验证。

### 关键测试场景

- `test_oracle_matches_hand_expectation` / `test_product_matches_oracle`：
  手工集合、oracle、产品计划器三方一致；
- `test_policy_drift_pauses_plan` / `test_watermark_drift_pauses_plan`：
  预检后改变策略/水位，计划暂停、差异落库、零删除；
- `test_inconsistent_watermark_blocks`：水位冲突与缺失均阻断回收；
- `test_hold_release_only_via_event`：冻结只接受明确解除事件；
- `test_failure_retry_no_double_delete`：失败留存 FAILED、跨轮重试、
  物理删除恰好一次；
- `test_process_exit_recovery`：提交前退出，重开后幂等恢复，触发器禁止
  已回收对象复活；
- `test_cascade_unblock_shared_parent` /
  `test_shared_parent_refcount_cascade`：叶序、引用计数级联、共享父节点；
- `test_large_graph_bounded_batches`：2500 对象下有界批次与分计划回收。

### 质量隔离测试（tests/test_quality_view.py）

- `test_registration_validation_and_atomicity`：未知快照/对象、重复声明、
  缺端点、缺映射、快照外边、空/越界/非整数区间全部拒绝，且不留半份映射；
- `test_convergence_partial_release_and_boundaries`：汇合分支 a/b -> join，
  半开边界 `[3,4)`/`[4,5)`、同节点直接报障、重合报障不传播、部分解封；
  每一步都与 `PartitionReference`（不 import 产品代码的逐分区展开模型）
  逐对象区间核对；
- `test_fault_idempotency_and_conflicts`：重复投递、重复解除、旧报障重放、
  同编号换载荷、解除未知编号，以及不同故障覆盖同一分区时的部分解封；
- `test_snapshots_are_isolated`：同编号报障与相近映射在两个快照互不影响；
- `test_recycled_semantics`：已回收版本明确返回 `RECYCLED`；已回收上游
  仍把隔离传给存活下游，解除报障后才停止归因；
- `test_crash_mid_write_reopen_reads_old_result`：写第二条支持时抛错，
  当前连接与重新打开的文件库都读到旧提交，且失败报障可重新提交；
- `test_long_interval_and_large_branch_bounded_update`：10^9 长区间与
  400 叶大支路并存，单次报障/解除只改 2 个节点、只检查 1 条映射，
  更新量不随无关支路规模增长。

`tests/demo_quality.py` 用小坐标把每个版本逐分区展开（`X` = 必须避开），
演示报障、重复投递、同编号换载荷、解除未知编号、部分解封、全部解除后与
参考模型一致，以及写到一半失败后重开仍读旧结果。
