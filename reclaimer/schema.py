"""SQLite 物理结构：表、索引与状态机触发器。

设计要点
========
1. 对象身份 ``object_id`` 不可变，版本可变；删除不抹除历史，只写墓碑。
2. 谱系、策略、冻结、水位全部可多版本并存，由 ``snapshot_id`` /
   ``policy_version`` / 生效区间界定，计划对其做指纹绑定。
3. ``plan_items`` 上的触发器保证阶段只能沿状态机前进，
   ``objects`` 上的触发器保证 ``RECYCLED`` 是终态——任何进程都不能
   把已经删除的对象重新标成 ``LIVE``。
4. 所有图遍历都用递归 CTE 在 SQLite 内完成，应用层不全图驻留内存。
5. 分区质量隔离（``quality_*``）是独立元数据层：坐标是对象版本内的分区
   序号，只按区间端点存储与计算；不删除数据，也不改变回收规则。
"""

# 计划阶段机
PLAN_DRAFT = "DRAFT"
PLAN_READY = "READY"
PLAN_RUNNING = "RUNNING"
PLAN_PAUSED = "PAUSED"
PLAN_COMPLETED = "COMPLETED"
PLAN_ABORTED = "ABORTED"

# 对象条目阶段机（严格单向）
ITEM_CANDIDATE = "CANDIDATE"          # 已过保留期、纳入本计划评估
ITEM_HOLD_WAITING = "HOLD_WAITING"    # 被法律冻结覆盖，等待明确解除事件
ITEM_RECYCLABLE = "RECYCLABLE"        # 通过全部引用判断，等待删除
ITEM_RECYCLED = "RECYCLED"            # 已删除（墓碑终态）
ITEM_BLOCKED = "BLOCKED"              # 仍有存活下游 / 水位保护等，本计划不动
ITEM_FAILED = "FAILED"                # 后端删除失败，可重试，不重复删除

# 对象生命周期
OBJ_LIVE = "LIVE"
OBJ_RECYCLED = "RECYCLED"

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ---------------------------------------------------------------- 对象
CREATE TABLE IF NOT EXISTS objects (
    object_id    TEXT NOT NULL,
    version      INTEGER NOT NULL,
    obj_type     TEXT NOT NULL,
    classification TEXT NOT NULL,
    environment  TEXT NOT NULL,
    storage_layer TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    lifecycle    TEXT NOT NULL DEFAULT 'LIVE',
    recycled_at  INTEGER,
    recycled_by_plan TEXT,
    payload_ref  TEXT,
    PRIMARY KEY (object_id, version),
    CHECK (version >= 1),
    CHECK (lifecycle IN ('LIVE','RECYCLED')),
    CHECK (recycled_at IS NULL OR lifecycle = 'RECYCLED')
);

-- ---------------------------------------------------------------- 谱系快照
CREATE TABLE IF NOT EXISTS lineage_snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    taken_at      INTEGER NOT NULL,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    snapshot_id   TEXT NOT NULL REFERENCES lineage_snapshots(snapshot_id),
    -- src 依赖 dst：src 是派生对象，dst 是被依赖的上游
    src_id        TEXT NOT NULL,
    src_version   INTEGER NOT NULL,
    dst_id        TEXT NOT NULL,
    dst_version   INTEGER NOT NULL,
    edge_kind     TEXT NOT NULL DEFAULT 'DERIVED',
    PRIMARY KEY (snapshot_id, src_id, src_version, dst_id, dst_version)
);
CREATE INDEX IF NOT EXISTS idx_edges_dst
    ON lineage_edges(snapshot_id, dst_id, dst_version);
CREATE INDEX IF NOT EXISTS idx_edges_src
    ON lineage_edges(snapshot_id, src_id, src_version);

-- ---------------------------------------------------------------- 保留策略
CREATE TABLE IF NOT EXISTS policy_versions (
    policy_version TEXT PRIMARY KEY,
    compiled_at    INTEGER NOT NULL,
    fingerprint    TEXT NOT NULL,
    description    TEXT
);

CREATE TABLE IF NOT EXISTS policy_rules (
    policy_version TEXT NOT NULL REFERENCES policy_versions(policy_version),
    rule_id        TEXT NOT NULL,
    obj_type       TEXT,           -- NULL 表示通配
    classification TEXT,           -- NULL 表示通配
    environment    TEXT,           -- NULL 表示通配
    retention_seconds INTEGER NOT NULL CHECK (retention_seconds >= 0),
    specificity    INTEGER NOT NULL,
    PRIMARY KEY (policy_version, rule_id),
    CHECK (specificity BETWEEN 0 AND 3)
);
-- 同一策略版本内，匹配维度组合不得重复
CREATE UNIQUE INDEX IF NOT EXISTS uq_policy_match
    ON policy_rules(policy_version,
                    COALESCE(obj_type, '*'),
                    COALESCE(classification, '*'),
                    COALESCE(environment, '*'));
-- ---------------------------------------------------------------- 法律冻结
CREATE TABLE IF NOT EXISTS legal_holds (
    hold_id       TEXT PRIMARY KEY,
    object_id     TEXT,                -- NULL 表示按选择器批量冻结
    version       INTEGER,             -- NULL 表示该身份全部版本
    selector_type TEXT,                -- obj_type / classification / environment
    selector_value TEXT,
    reason        TEXT NOT NULL,
    applied_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_holds_object
    ON legal_holds(object_id, version);

-- 冻结只能由“明确解除事件”取消；解除事件不可删除、不可撤销
CREATE TABLE IF NOT EXISTS hold_releases (
    release_id    TEXT PRIMARY KEY,
    hold_id       TEXT NOT NULL REFERENCES legal_holds(hold_id),
    released_at   INTEGER NOT NULL,
    event_ref     TEXT NOT NULL,       -- 外部法律事件编号
    note          TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_release_hold ON hold_releases(hold_id);

-- ---------------------------------------------------------------- 消费者水位
CREATE TABLE IF NOT EXISTS consumers (
    consumer_id   TEXT PRIMARY KEY,
    description   TEXT
);

-- 消费者声明仍需读取的对象身份（订阅）；有订阅却无水位即“水位缺失”
CREATE TABLE IF NOT EXISTS consumer_interests (
    consumer_id   TEXT NOT NULL REFERENCES consumers(consumer_id),
    object_id     TEXT NOT NULL,
    PRIMARY KEY (consumer_id, object_id)
);

CREATE TABLE IF NOT EXISTS consumer_watermarks (
    consumer_id   TEXT NOT NULL REFERENCES consumers(consumer_id),
    object_id     TEXT NOT NULL,       -- 该消费者仍需读取的对象身份
    min_version   INTEGER NOT NULL CHECK (min_version >= 1),
    recorded_at   INTEGER NOT NULL,
    PRIMARY KEY (consumer_id, object_id)
);
CREATE INDEX IF NOT EXISTS idx_wm_object ON consumer_watermarks(object_id);

-- 水位问题（缺失 / 冲突 / 越界）；存在问题时相关对象不得回收
CREATE TABLE IF NOT EXISTS watermark_issues (
    issue_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id     TEXT NOT NULL,
    consumer_id   TEXT,
    kind          TEXT NOT NULL CHECK (kind IN ('MISSING','CONFLICT','OUT_OF_RANGE')),
    detail        TEXT NOT NULL,
    detected_at   INTEGER NOT NULL,
    resolved_at   INTEGER
);

-- ---------------------------------------------------------------- 计划
CREATE TABLE IF NOT EXISTS plans (
    plan_id          TEXT PRIMARY KEY,
    created_at       INTEGER NOT NULL,
    as_of_ts         INTEGER NOT NULL,
    status           TEXT NOT NULL,
    policy_version   TEXT NOT NULL,
    snapshot_id      TEXT NOT NULL,
    holds_fingerprint   TEXT NOT NULL,
    watermarks_fingerprint TEXT NOT NULL,
    max_batch_size   INTEGER NOT NULL,
    max_plan_items   INTEGER NOT NULL,
    last_error       TEXT
);

CREATE TABLE IF NOT EXISTS plan_diffs (
    diff_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id       TEXT NOT NULL REFERENCES plans(plan_id),
    detected_at   INTEGER NOT NULL,
    kind          TEXT NOT NULL,       -- POLICY / SNAPSHOT / HOLDS / WATERMARKS
    detail        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_batches (
    batch_no      INTEGER NOT NULL,
    plan_id       TEXT NOT NULL REFERENCES plans(plan_id),
    started_at    INTEGER,
    finished_at   INTEGER,
    item_count    INTEGER NOT NULL,
    PRIMARY KEY (plan_id, batch_no)
);

CREATE TABLE IF NOT EXISTS plan_items (
    plan_id       TEXT NOT NULL REFERENCES plans(plan_id),
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    stage         TEXT NOT NULL,
    block_reason  TEXT,
    refcount_at_plan INTEGER NOT NULL,  -- 生成时的存活下游引用计数（证据）
    current_refcount INTEGER NOT NULL,  -- 执行期动态计数
    batch_no      INTEGER,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (plan_id, object_id, version),
    CHECK (stage IN ('CANDIDATE','HOLD_WAITING','RECYCLABLE',
                     'RECYCLED','BLOCKED','FAILED')),
    CHECK (refcount_at_plan >= 0 AND current_refcount >= 0)
);
CREATE INDEX IF NOT EXISTS idx_items_stage
    ON plan_items(plan_id, stage, current_refcount);

-- 每个被删对象的不删除证明：仍存活的保留结果/消费者引用证据
CREATE TABLE IF NOT EXISTS reference_evidence (
    evidence_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id       TEXT NOT NULL,
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    evidence_kind TEXT NOT NULL,  -- LIVE_DOWNSTREAM / WATERMARK / RETAINED / HOLD
    ref_object_id TEXT,
    ref_version   INTEGER,
    consumer_id   TEXT,
    detail        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_evidence_obj
    ON reference_evidence(plan_id, object_id, version);

-- ---------------------------------------------------------------- 墓碑与审计
CREATE TABLE IF NOT EXISTS recycle_tombstones (
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    plan_id       TEXT NOT NULL,
    recycled_at   INTEGER NOT NULL,
    former_layer  TEXT NOT NULL,
    former_payload_ref TEXT,
    PRIMARY KEY (object_id, version)
);

CREATE TABLE IF NOT EXISTS execution_log (
    log_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id       TEXT NOT NULL,
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    action        TEXT NOT NULL,  -- DELETE_ATTEMPT / DELETE_OK / DELETE_FAILED / RETRY
    ok            INTEGER NOT NULL,
    detail        TEXT,
    at_ts         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_log_plan ON execution_log(plan_id, log_id);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ts         INTEGER NOT NULL,
    event_type    TEXT NOT NULL,
    plan_id       TEXT,
    object_id     TEXT,
    version       INTEGER,
    detail        TEXT NOT NULL
);

-- ================================================================
-- 分区质量隔离视图（只维护元数据，不读取/删除数据文件）
--
-- 坐标是“对象版本内的分区序号”，不是版本号。非法分区以半开区间
-- [range_start, range_end) 表示，所有区间端点都是整数，计算与存储
-- 只依赖端点，不按区间长度枚举分区。
-- ================================================================
CREATE TABLE IF NOT EXISTS quality_views (
    snapshot_id   TEXT PRIMARY KEY
                  REFERENCES lineage_snapshots(snapshot_id),
    registered_at INTEGER NOT NULL
);

-- 每个参与对象版本声明分区总数；可登记快照中没有边的已知对象版本。
CREATE TABLE IF NOT EXISTS quality_objects (
    snapshot_id     TEXT NOT NULL REFERENCES quality_views(snapshot_id),
    object_id       TEXT NOT NULL,
    version         INTEGER NOT NULL,
    partition_count INTEGER NOT NULL CHECK (partition_count >= 1),
    PRIMARY KEY (snapshot_id, object_id, version)
);

-- 一条映射绑定快照内的一条真实依赖边：
--   上游 dst 的读取区间 read_* 命中隔离 -> 下游 src 的产出区间 out_* 全隔离
-- 同一条边允许有多条交叠映射；不要求映射铺满对象。
CREATE TABLE IF NOT EXISTS quality_mappings (
    mapping_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id   TEXT NOT NULL REFERENCES quality_views(snapshot_id),
    src_id        TEXT NOT NULL,
    src_version   INTEGER NOT NULL,
    dst_id        TEXT NOT NULL,
    dst_version   INTEGER NOT NULL,
    read_start    INTEGER NOT NULL,
    read_end      INTEGER NOT NULL,
    out_start     INTEGER NOT NULL,
    out_end       INTEGER NOT NULL,
    active        INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0,1)),
    CHECK (read_start >= 0 AND read_end > read_start),
    CHECK (out_start >= 0 AND out_end > out_start),
    FOREIGN KEY (snapshot_id, src_id, src_version)
        REFERENCES quality_objects(snapshot_id, object_id, version),
    FOREIGN KEY (snapshot_id, dst_id, dst_version)
        REFERENCES quality_objects(snapshot_id, object_id, version),
    FOREIGN KEY (snapshot_id, src_id, src_version,
                 dst_id, dst_version)
        REFERENCES lineage_edges(snapshot_id, src_id, src_version,
                                 dst_id, dst_version)
);
CREATE INDEX IF NOT EXISTS idx_qmap_read
    ON quality_mappings(snapshot_id, dst_id, dst_version,
                        read_start, read_end);
CREATE INDEX IF NOT EXISTS idx_qmap_out
    ON quality_mappings(snapshot_id, src_id, src_version,
                        out_start, out_end);

-- 报障记录。编号在快照内唯一；released_at 非空表示已解除，但记录
-- 永久保留，用于重放幂等与历史追溯。
CREATE TABLE IF NOT EXISTS quality_faults (
    snapshot_id   TEXT NOT NULL REFERENCES quality_views(snapshot_id),
    fault_id      TEXT NOT NULL,
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    range_start   INTEGER NOT NULL,
    range_end     INTEGER NOT NULL,
    payload_hash  TEXT NOT NULL,
    reported_at   INTEGER NOT NULL,
    released_at   INTEGER,
    release_ref   TEXT,
    PRIMARY KEY (snapshot_id, fault_id),
    CHECK (range_start >= 0 AND range_end > range_start),
    FOREIGN KEY (snapshot_id, object_id, version)
        REFERENCES quality_objects(snapshot_id, object_id, version)
);
CREATE INDEX IF NOT EXISTS idx_qfault_node
    ON quality_faults(snapshot_id, object_id, version, released_at);

-- 区间级“支持”台账：节点当前隔离集合 = 其全部支持区间的并集。
--   FAULT   : 未解除报障直接贡献的区间（source_key = fault_id）
--   MAPPING : 上游读取区间仍命中隔离时，映射产出区间对下游的贡献
-- 解除报障 / 映射失活只删除对应支持；其它支持仍覆盖的区间不会解封。
CREATE TABLE IF NOT EXISTS quality_supports (
    snapshot_id   TEXT NOT NULL REFERENCES quality_views(snapshot_id),
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    source_kind   TEXT NOT NULL CHECK (source_kind IN ('FAULT','MAPPING')),
    source_key    TEXT NOT NULL,
    range_start   INTEGER NOT NULL,
    range_end     INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, source_kind, source_key),
    CHECK (range_start >= 0 AND range_end > range_start),
    FOREIGN KEY (snapshot_id, object_id, version)
        REFERENCES quality_objects(snapshot_id, object_id, version)
);
CREATE INDEX IF NOT EXISTS idx_qsupport_node
    ON quality_supports(snapshot_id, object_id, version);

-- 物化的当前隔离区间。查询直接读本表；报障/解除时增量维护。
-- 同一节点内区间规范化为不相交且不相邻。
CREATE TABLE IF NOT EXISTS quality_isolated_ranges (
    snapshot_id   TEXT NOT NULL REFERENCES quality_views(snapshot_id),
    object_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    range_start   INTEGER NOT NULL,
    range_end     INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, object_id, version, range_start),
    CHECK (range_start >= 0 AND range_end > range_start),
    FOREIGN KEY (snapshot_id, object_id, version)
        REFERENCES quality_objects(snapshot_id, object_id, version)
);
CREATE INDEX IF NOT EXISTS idx_qrange_lookup
    ON quality_isolated_ranges(snapshot_id, object_id, version,
                               range_start, range_end);

-- ================================================================
-- 目录镜像变更序列（供盘点服务镜像，不复制对象文件/计划/谱系/质量/账单）
--
-- 只有两个目录写入口会追加批次：importer 的成功对象导入（IMPORT）与
-- executor 成功提交的回收（RECYCLE）。批次号在写事务内按 MAX+1 分配，
-- 回滚的事务不留任何痕迹，因此已提交批次按提交顺序从 1 连续编号。
-- catalog_changes 保存该批次写入后的完整目录行（非增量 diff），接收端
-- 直接 UPSERT 即可；保留历史所需存储随写入量增长。
-- ================================================================
CREATE TABLE IF NOT EXISTS catalog_batches (
    batch_no      INTEGER PRIMARY KEY,      -- 提交顺序，从 1 连续
    change_kind   TEXT NOT NULL CHECK (change_kind IN ('IMPORT','RECYCLE')),
    committed_at  INTEGER NOT NULL,
    row_count     INTEGER NOT NULL CHECK (row_count >= 1)
);

CREATE TABLE IF NOT EXISTS catalog_changes (
    batch_no       INTEGER NOT NULL REFERENCES catalog_batches(batch_no),
    seq_in_batch   INTEGER NOT NULL,
    object_id      TEXT NOT NULL,
    version        INTEGER NOT NULL,
    obj_type       TEXT NOT NULL,
    classification TEXT NOT NULL,
    environment    TEXT NOT NULL,
    storage_layer  TEXT NOT NULL,
    created_at     INTEGER NOT NULL,
    lifecycle      TEXT NOT NULL,
    recycled_at    INTEGER,
    recycled_by_plan TEXT,
    payload_ref    TEXT,
    PRIMARY KEY (batch_no, seq_in_batch)
);
CREATE INDEX IF NOT EXISTS idx_catalog_changes_batch
    ON catalog_changes(batch_no, object_id, version);

-- 主库来源标识：单行，initialize 时补写，随库重开保持。
-- 镜像端据此拒绝来源不同的交付。
CREATE TABLE IF NOT EXISTS catalog_source (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    source_id      TEXT NOT NULL,
    initialized_at INTEGER NOT NULL
);

-- ================================================================
-- 状态机触发器
-- ================================================================

-- 对象生命周期：RECYCLED 是终态，进程重开也不能复活
CREATE TRIGGER IF NOT EXISTS trg_object_no_revive
BEFORE UPDATE OF lifecycle ON objects
WHEN OLD.lifecycle = 'RECYCLED' AND NEW.lifecycle = 'LIVE'
BEGIN
    SELECT RAISE(ABORT,
        'object already recycled cannot be marked live again');
END;

-- 已回收行不得被删除（历史必须可查）
CREATE TRIGGER IF NOT EXISTS trg_tombstone_no_delete
BEFORE DELETE ON recycle_tombstones
BEGIN
    SELECT RAISE(ABORT, 'tombstones are append-only audit evidence');
END;

-- 计划条目单向阶段机（WHEN 为纯布尔表达式；触发即 ABORT）
CREATE TRIGGER IF NOT EXISTS trg_item_forward_stage
BEFORE UPDATE OF stage ON plan_items
WHEN
       (OLD.stage = 'RECYCLED')
    OR (OLD.stage = 'RECYCLABLE'
        AND NEW.stage NOT IN ('RECYCLABLE','RECYCLED','FAILED'))
    OR (OLD.stage = 'FAILED'
        AND NEW.stage NOT IN ('FAILED','RECYCLABLE','RECYCLED'))
    OR (OLD.stage = 'HOLD_WAITING'
        AND NEW.stage NOT IN
            ('HOLD_WAITING','RECYCLABLE','RECYCLED','BLOCKED'))
    OR (OLD.stage = 'CANDIDATE'
        AND NEW.stage NOT IN
            ('CANDIDATE','HOLD_WAITING','RECYCLABLE','RECYCLED',
             'BLOCKED','FAILED'))
    OR (OLD.stage = 'BLOCKED'
        AND NEW.stage NOT IN ('BLOCKED','RECYCLABLE','RECYCLED'))
BEGIN
    SELECT RAISE(ABORT, 'illegal plan item stage transition');
END;
"""


def initialize(conn) -> None:
    """建表、建索引、建触发器（幂等）。

    对从旧版本升级的现有数据库：只补建镜像变更表，并为本库补写一份
    来源标识。升级前已存在的对象与墓碑不倒推旧变更——它们通过第一次
    完整交付进入镜像。
    """
    import time
    import uuid

    conn.executescript(SCHEMA_SQL)
    existing = conn.execute(
        "SELECT 1 FROM catalog_source WHERE id=1"
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO catalog_source(id, source_id, initialized_at)"
            " VALUES(1,?,?)",
            (f"src-{uuid.uuid4().hex}", int(time.time())),
        )
    conn.commit()
