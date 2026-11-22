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
    """建表、建索引、建触发器（幂等）。"""
    conn.executescript(SCHEMA_SQL)
    conn.commit()
