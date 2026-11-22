"""分区质量隔离视图：只维护元数据，不读取数据文件、不删除数据。

坐标
====
区间坐标是**对象版本内的分区序号**，不是对象版本号。每个登记的对象版本
声明分区总数 ``partition_count``；合法区间是 ``[0, partition_count)``
内的非空整数半开区间 ``[start, end)``。

依赖方向沿用 :mod:`reclaimer.schema` 的 ``lineage_edges``：
``src`` 派生自 ``dst``（``src`` 依赖 ``dst``），坏数从被读取的 ``dst``
传给派生出的 ``src``。一条映射声明：

- 上游读取区间 ``read``（``dst`` 的坐标）；
- 下游产出区间 ``out``（``src`` 的坐标）。

只要 ``read`` 与上游当前任意隔离区间相交，``out`` 就整体隔离。同一条边
可以登记多条可交叠的映射，不要求铺满对象。

增量维护
========
每个节点维护一组“支持区间”（``quality_supports``）：

- ``FAULT``  ：未解除报障直接贡献的区间；
- ``MAPPING``：某条映射当前激活时，其产出区间对下游的贡献。

节点的隔离区间是全部支持区间的并集，规范化后写入
``quality_isolated_ranges``，查询直接读该表。报障/解除只沿**区间变化**
定位受影响的映射；节点并集没变就不向后继传播。所有算术按端点进行，
不会按区间长度枚举分区，也不枚举血缘路径。
"""

from __future__ import annotations

import operator
import time
from collections import deque
from contextlib import contextmanager

from .errors import QualityFaultError, QualityValidationError
from .schema import OBJ_RECYCLED, initialize

# 查询状态
QUALITY_AVAILABLE = "AVAILABLE"             # 已登记且当前可读
QUALITY_RECYCLED = "RECYCLED"               # 已回收，不可当成可供数
QUALITY_NO_VIEW = "NO_QUALITY_VIEW"         # 未登记质量视图

_SOURCE_FAULT = "FAULT"
_SOURCE_MAPPING = "MAPPING"


# ---------------------------------------------------------------- 区间算术
def normalize(ranges) -> list[tuple[int, int]]:
    """排序并合并相交或相邻的半开区间。"""
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def subtract(left, right) -> list[tuple[int, int]]:
    """``left - right``；两者都应是规范化区间集合。"""
    result: list[tuple[int, int]] = []
    j = 0
    for start, end in left:
        cur = start
        while j < len(right) and right[j][1] <= cur:
            j += 1
        k = j
        while k < len(right) and right[k][0] < end:
            r_start, r_end = right[k]
            if r_start > cur:
                result.append((cur, min(r_start, end)))
            if r_end > cur:
                cur = r_end
            if cur >= end:
                break
            k += 1
        j = k
        if cur < end:
            result.append((cur, end))
    return result


def _point_int(value, field: str) -> int:
    if isinstance(value, bool):
        raise QualityValidationError(f"{field} must be an integer")
    try:
        return operator.index(value)
    except TypeError:
        raise QualityValidationError(f"{field} must be an integer") from None


def _text(value, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise QualityValidationError(f"{field} must be a non-empty string")
    return value


def _range_from(entry, prefix: str, field_prefix: str):
    """从映射字典中取区间，支持 ``"read"/"out"`` 或显式 start/end。"""
    if entry.get(prefix) is not None:
        value = entry[prefix]
        try:
            start, end = value
        except (TypeError, ValueError):
            raise QualityValidationError(
                f"{field_prefix} must be a two-item interval") from None
    else:
        try:
            start = entry[f"{prefix}_start"]
            end = entry[f"{prefix}_end"]
        except KeyError as exc:
            raise QualityValidationError(
                f"mapping missing field {exc.args[0]}") from None
    return (_point_int(start, f"{field_prefix}_start"),
            _point_int(end, f"{field_prefix}_end"))


class QualityView:
    """分区质量隔离元数据。"""

    def __init__(self, conn, clock=time.time):
        self.conn = conn
        self._clock = clock
        initialize(conn)
        self.last_update_stats: dict = self._fresh_stats()

    # ------------------------------------------------------------ 事务
    @contextmanager
    def _atomic(self):
        """把一次登记/报障/解除及其全部下游变化放在一个事务里。"""
        if self.conn.in_transaction:
            savepoint = "quality_update_sp"
            self.conn.execute(f"SAVEPOINT {savepoint}")
            try:
                yield
            except BaseException:
                self.conn.execute(f"ROLLBACK TO {savepoint}")
                self.conn.execute(f"RELEASE {savepoint}")
                raise
            else:
                self.conn.execute(f"RELEASE {savepoint}")
        else:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    # ------------------------------------------------------------ 统计
    @staticmethod
    def _fresh_stats() -> dict:
        return {
            "nodes_changed": 0,
            "intervals_written": 0,
            "mappings_examined": 0,
            "mappings_activated": 0,
            "mappings_deactivated": 0,
            "supports_added": 0,
            "supports_removed": 0,
            "faults_written": 0,
        }

    # ============================================================ 登记
    def register(self, snapshot_id, objects, mappings=()) -> dict:
        """为已导入的谱系快照登记固定映射。

        ``objects``：``{object_id, version, partition_count}`` 列表。除边的
        端点外，也允许登记库中已知、但在该快照没有边的对象版本。

        ``mappings``：每条至少包含 ``src_id/src_version/dst_id/dst_version``
        和 ``read``/``out``（或 ``read_start/read_end/out_start/out_end``）。
        每条快照边至少一条映射；不要求映射铺满对象，允许同边多映射交叠。

        全部定义先校验，再在单事务内写入；任何无效定义都不会留下半份映射。
        """
        snapshot_id = _text(snapshot_id, "snapshot_id")
        if not self.conn.execute(
            "SELECT 1 FROM lineage_snapshots WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone():
            raise QualityValidationError(
                f"snapshot {snapshot_id} has not been imported")
        if self.conn.execute(
            "SELECT 1 FROM quality_views WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone():
            raise QualityValidationError(
                f"quality view for snapshot {snapshot_id} already registered")

        # ---- 对象版本声明
        declared: dict[tuple[str, int], int] = {}
        for raw in objects:
            if not isinstance(raw, dict):
                raise QualityValidationError("object declaration must be a dict")
            object_id = _text(raw.get("object_id"), "object_id")
            version = _point_int(raw.get("version"), "version")
            count = _point_int(raw.get("partition_count"), "partition_count")
            if count < 1:
                raise QualityValidationError(
                    f"{object_id} v{version} partition_count must be >= 1")
            if (object_id, version) in declared:
                raise QualityValidationError(
                    f"duplicate object declaration: {object_id} v{version}")
            if not self.conn.execute(
                "SELECT 1 FROM objects WHERE object_id=? AND version=?",
                (object_id, version),
            ).fetchone():
                raise QualityValidationError(
                    f"object {object_id} v{version} is not known in the library")
            declared[(object_id, version)] = count

        # ---- 快照边全量
        edges = {
            (row[0], row[1], row[2], row[3])
            for row in self.conn.execute(
                """SELECT src_id, src_version, dst_id, dst_version
                   FROM lineage_edges WHERE snapshot_id=?""",
                (snapshot_id,),
            )
        }
        edge_covered: set[tuple] = set()

        # ---- 映射定义
        mapping_rows = []
        for raw in mappings:
            if not isinstance(raw, dict):
                raise QualityValidationError("mapping must be a dict")
            src_id = _text(raw.get("src_id"), "src_id")
            src_version = _point_int(raw.get("src_version"), "src_version")
            dst_id = _text(raw.get("dst_id"), "dst_id")
            dst_version = _point_int(raw.get("dst_version"), "dst_version")
            src = (src_id, src_version)
            dst = (dst_id, dst_version)
            edge = (src_id, src_version, dst_id, dst_version)
            if edge not in edges:
                raise QualityValidationError(
                    f"mapping edge {src_id} v{src_version} <- "
                    f"{dst_id} v{dst_version} is not in snapshot {snapshot_id}")
            if src not in declared or dst not in declared:
                missing = src if src not in declared else dst
                raise QualityValidationError(
                    f"mapping endpoint {missing[0]} v{missing[1]} "
                    "has no partition_count declaration")
            read_start, read_end = _range_from(raw, "read", "read")
            out_start, out_end = _range_from(raw, "out", "out")
            if not (0 <= read_start < read_end <= declared[dst]):
                raise QualityValidationError(
                    f"read interval [{read_start}, {read_end}) outside "
                    f"[0, {declared[dst]}) for {dst_id} v{dst_version}")
            if not (0 <= out_start < out_end <= declared[src]):
                raise QualityValidationError(
                    f"out interval [{out_start}, {out_end}) outside "
                    f"[0, {declared[src]}) for {src_id} v{src_version}")
            mapping_rows.append(
                (snapshot_id, src_id, src_version, dst_id, dst_version,
                 read_start, read_end, out_start, out_end))
            edge_covered.add(edge)

        # ---- 完整性：每条快照边至少一条映射；端点必须声明
        for src_id, src_version, dst_id, dst_version in edges:
            if (src_id, src_version) not in declared or \
                    (dst_id, dst_version) not in declared:
                missing = ((src_id, src_version)
                           if (src_id, src_version) not in declared
                           else (dst_id, dst_version))
                raise QualityValidationError(
                    f"edge endpoint {missing[0]} v{missing[1]} "
                    "has no partition_count declaration")
            if (src_id, src_version, dst_id, dst_version) not in edge_covered:
                raise QualityValidationError(
                    f"edge {src_id} v{src_version} <- {dst_id} v{dst_version} "
                    "has no quality mapping")

        with self._atomic():
            self.conn.execute(
                "INSERT INTO quality_views(snapshot_id, registered_at)"
                " VALUES(?,?)",
                (snapshot_id, int(self._clock())),
            )
            self.conn.executemany(
                """INSERT INTO quality_objects(snapshot_id, object_id,
                       version, partition_count) VALUES(?,?,?,?)""",
                [(snapshot_id, oid, ver, count)
                 for (oid, ver), count in declared.items()],
            )
            self.conn.executemany(
                """INSERT INTO quality_mappings(snapshot_id, src_id,
                       src_version, dst_id, dst_version, read_start, read_end,
                       out_start, out_end, active)
                   VALUES(?,?,?,?,?,?,?,?,?,0)""",
                mapping_rows,
            )
        return {
            "snapshot_id": snapshot_id,
            "object_count": len(declared),
            "mapping_count": len(mapping_rows),
        }

    # ============================================================ 报障
    def report(self, snapshot_id, fault_id, object_id, version,
               range_start, range_end) -> dict:
        """登记一条带编号的报障，并增量传播到全部下游。"""
        self.last_update_stats = self._fresh_stats()
        snapshot_id = _text(snapshot_id, "snapshot_id")
        fault_id = _text(fault_id, "fault_id")
        object_id = _text(object_id, "object_id")
        version = _point_int(version, "version")
        range_start = _point_int(range_start, "range_start")
        range_end = _point_int(range_end, "range_end")
        partition_count = self._require_object(snapshot_id, object_id, version)
        if not (0 <= range_start < range_end <= partition_count):
            raise QualityValidationError(
                f"fault interval [{range_start}, {range_end}) outside "
                f"[0, {partition_count}) for {object_id} v{version}")

        existing = self.conn.execute(
            """SELECT object_id, version, range_start, range_end, released_at
               FROM quality_faults WHERE snapshot_id=? AND fault_id=?""",
            (snapshot_id, fault_id),
        ).fetchone()
        if existing is not None:
            same_payload = (
                existing[0] == object_id and existing[1] == version
                and existing[2] == range_start and existing[3] == range_end
            )
            if not same_payload:
                raise QualityFaultError(
                    f"fault {fault_id} already exists in snapshot "
                    f"{snapshot_id} with a different payload")
            if existing[4] is not None:
                # 旧报障重放：绝不重新激活已解除故障
                return {"snapshot_id": snapshot_id, "fault_id": fault_id,
                        "status": "already-released", "changed": False,
                        "intervals": self._public_intervals(
                            snapshot_id, object_id, version)}
            return {"snapshot_id": snapshot_id, "fault_id": fault_id,
                    "status": "duplicate", "changed": False,
                    "intervals": self._public_intervals(
                        snapshot_id, object_id, version)}

        with self._atomic():
            self.conn.execute(
                """INSERT INTO quality_faults(snapshot_id, fault_id,
                       object_id, version, range_start, range_end, payload_hash,
                       reported_at) VALUES(?,?,?,?,?,?,?,?)""",
                (snapshot_id, fault_id, object_id, version,
                 range_start, range_end,
                 f"{object_id}@{version}[{range_start},{range_end})",
                 int(self._clock())),
            )
            self.last_update_stats["faults_written"] += 1
            self._add_support(snapshot_id, object_id, version,
                              _SOURCE_FAULT, fault_id,
                              range_start, range_end)
            delta = self._refresh_node(snapshot_id, object_id, version)
            if delta is not None:
                self._propagate(snapshot_id, (object_id, version), delta)

        return {"snapshot_id": snapshot_id, "fault_id": fault_id,
                "status": "reported", "changed": True,
                "intervals": self._public_intervals(
                    snapshot_id, object_id, version),
                "stats": dict(self.last_update_stats)}

    def release(self, snapshot_id, fault_id, release_ref=None) -> dict:
        """按编号解除报障；重复解除幂等，未知编号报错。"""
        self.last_update_stats = self._fresh_stats()
        snapshot_id = _text(snapshot_id, "snapshot_id")
        fault_id = _text(fault_id, "fault_id")
        row = self.conn.execute(
            """SELECT object_id, version, range_start, range_end, released_at
               FROM quality_faults WHERE snapshot_id=? AND fault_id=?""",
            (snapshot_id, fault_id),
        ).fetchone()
        if row is None:
            raise QualityFaultError(
                f"unknown fault {fault_id} in snapshot {snapshot_id}")
        if row[4] is not None:
            return {"snapshot_id": snapshot_id, "fault_id": fault_id,
                    "status": "already-released", "changed": False,
                    "intervals": self._public_intervals(
                        snapshot_id, row[0], row[1])}

        object_id, version = row[0], row[1]
        with self._atomic():
            self.conn.execute(
                """UPDATE quality_faults SET released_at=?, release_ref=?
                   WHERE snapshot_id=? AND fault_id=?""",
                (int(self._clock()), release_ref, snapshot_id, fault_id),
            )
            self._remove_support(snapshot_id, _SOURCE_FAULT, fault_id)
            delta = self._refresh_node(snapshot_id, object_id, version)
            if delta is not None:
                self._propagate(snapshot_id, (object_id, version), delta)

        return {"snapshot_id": snapshot_id, "fault_id": fault_id,
                "status": "released", "changed": True,
                "intervals": self._public_intervals(
                    snapshot_id, object_id, version),
                "stats": dict(self.last_update_stats)}

    # ============================================================ 查询
    def isolated_ranges(self, snapshot_id, object_id, version) -> dict:
        """按快照 + 对象版本返回当前必须避开的分区区间。

        三态严格区分：

        - ``AVAILABLE``        ：已登记且当前可读；``intervals`` 按起点升序，
          已合并相交及相邻项；
        - ``RECYCLED``         ：版本已回收，绝不可当成可供数；
        - ``NO_QUALITY_VIEW``  ：快照或对象版本未登记质量视图。

        查询只读已维护的物化结果，不临时重跑谱系。
        """
        version = _point_int(version, "version")
        object_id = _text(object_id, "object_id")
        snapshot_id = _text(snapshot_id, "snapshot_id")
        if not self.conn.execute(
            "SELECT 1 FROM quality_views WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone():
            return self._no_view(snapshot_id, object_id, version,
                                 "snapshot has no registered quality view")
        row = self.conn.execute(
            """SELECT partition_count FROM quality_objects
               WHERE snapshot_id=? AND object_id=? AND version=?""",
            (snapshot_id, object_id, version),
        ).fetchone()
        lifecycle = self.conn.execute(
            "SELECT lifecycle FROM objects WHERE object_id=? AND version=?",
            (object_id, version),
        ).fetchone()
        recycled = lifecycle is not None and lifecycle[0] == OBJ_RECYCLED

        if row is None:
            # 未登记：即使对象在库内已回收，也不虚构质量视图
            result = self._no_view(
                snapshot_id, object_id, version,
                "object version is not registered in the quality view")
            if recycled:
                result["object_lifecycle"] = OBJ_RECYCLED
                result["message"] += "; object version is also recycled"
            return result
        partition_count = row[0]

        if recycled:
            tomb = self.conn.execute(
                """SELECT plan_id, recycled_at FROM recycle_tombstones
                   WHERE object_id=? AND version=?""",
                (object_id, version),
            ).fetchone()
            return {
                "snapshot_id": snapshot_id,
                "object_id": object_id,
                "version": version,
                "partition_count": partition_count,
                "status": QUALITY_RECYCLED,
                "readable": False,
                "intervals": None,
                "message": "object version was recycled; it is not a "
                           "supplyable version",
                "tombstone": ({"plan_id": tomb[0], "recycled_at": tomb[1]}
                              if tomb else None),
            }

        intervals = self._public_intervals(snapshot_id, object_id, version)
        return {
            "snapshot_id": snapshot_id,
            "object_id": object_id,
            "version": version,
            "partition_count": partition_count,
            "status": QUALITY_AVAILABLE,
            "readable": True,
            "intervals": intervals,
            "message": ("no quality isolation" if not intervals
                        else "supply must avoid the listed partition "
                             "intervals"),
        }

    def faults(self, snapshot_id, only_unreleased: bool = True) -> list[dict]:
        """列出一份质量视图中的报障（用于审计与重放核对）。"""
        snapshot_id = _text(snapshot_id, "snapshot_id")
        sql = """SELECT fault_id, object_id, version, range_start, range_end,
                        reported_at, released_at, release_ref
                 FROM quality_faults WHERE snapshot_id=?"""
        if only_unreleased:
            sql += " AND released_at IS NULL"
        sql += " ORDER BY reported_at, fault_id"
        cols = ("fault_id", "object_id", "version", "range_start",
                "range_end", "reported_at", "released_at", "release_ref")
        return [dict(zip(cols, row))
                for row in self.conn.execute(sql, (snapshot_id,))]

    def _no_view(self, snapshot_id, object_id, version, message) -> dict:
        return {
            "snapshot_id": snapshot_id,
            "object_id": object_id,
            "version": version,
            "status": QUALITY_NO_VIEW,
            "readable": False,
            "intervals": None,
            "message": message,
        }

    def _public_intervals(self, snapshot_id, object_id, version):
        return [[row[0], row[1]] for row in self.conn.execute(
            """SELECT range_start, range_end
               FROM quality_isolated_ranges
               WHERE snapshot_id=? AND object_id=? AND version=?
               ORDER BY range_start, range_end""",
            (snapshot_id, object_id, version),
        )]

    # ============================================================ 内部
    def _require_object(self, snapshot_id, object_id, version) -> int:
        if not self.conn.execute(
            "SELECT 1 FROM quality_views WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone():
            raise QualityValidationError(
                f"snapshot {snapshot_id} has no registered quality view")
        row = self.conn.execute(
            """SELECT partition_count FROM quality_objects
               WHERE snapshot_id=? AND object_id=? AND version=?""",
            (snapshot_id, object_id, version),
        ).fetchone()
        if row is None:
            raise QualityValidationError(
                f"{object_id} v{version} is not registered in the quality "
                f"view of snapshot {snapshot_id}")
        return row[0]

    def _load_union(self, snapshot_id, object_id, version):
        return [(row[0], row[1]) for row in self.conn.execute(
            """SELECT range_start, range_end FROM quality_isolated_ranges
               WHERE snapshot_id=? AND object_id=? AND version=?
               ORDER BY range_start, range_end""",
            (snapshot_id, object_id, version),
        )]

    def _refresh_node(self, snapshot_id, object_id, version):
        """按支持区间重算节点并集；返回区间变化，未变则返回 None。"""
        old = self._load_union(snapshot_id, object_id, version)
        new = normalize(
            (row[0], row[1]) for row in self.conn.execute(
                """SELECT range_start, range_end FROM quality_supports
                   WHERE snapshot_id=? AND object_id=? AND version=?""",
                (snapshot_id, object_id, version),
            )
        )
        if new == old:
            return None
        self.conn.execute(
            """DELETE FROM quality_isolated_ranges
               WHERE snapshot_id=? AND object_id=? AND version=?""",
            (snapshot_id, object_id, version),
        )
        self.conn.executemany(
            """INSERT INTO quality_isolated_ranges(snapshot_id, object_id,
                   version, range_start, range_end) VALUES(?,?,?,?,?)""",
            [(snapshot_id, object_id, version, start, end)
             for start, end in new],
        )
        self.last_update_stats["nodes_changed"] += 1
        self.last_update_stats["intervals_written"] += len(new)
        return (subtract(new, old), subtract(old, new))

    def _add_support(self, snapshot_id, object_id, version, source_kind,
                     source_key, range_start, range_end):
        self.conn.execute(
            """INSERT INTO quality_supports(snapshot_id, object_id, version,
                   source_kind, source_key, range_start, range_end)
               VALUES(?,?,?,?,?,?,?)""",
            (snapshot_id, object_id, version, source_kind, str(source_key),
             range_start, range_end),
        )
        self.last_update_stats["supports_added"] += 1

    def _remove_support(self, snapshot_id, source_kind, source_key):
        cur = self.conn.execute(
            """DELETE FROM quality_supports
               WHERE snapshot_id=? AND source_kind=? AND source_key=?""",
            (snapshot_id, source_kind, str(source_key)),
        )
        self.last_update_stats["supports_removed"] += cur.rowcount

    def _activation(self, snapshot_id, node, read_start, read_end) -> bool:
        return self.conn.execute(
            """SELECT 1 FROM quality_isolated_ranges
               WHERE snapshot_id=? AND object_id=? AND version=?
                 AND range_start < ? AND range_end > ? LIMIT 1""",
            (snapshot_id, node[0], node[1], read_end, read_start),
        ).fetchone() is not None

    def _candidate_mappings(self, snapshot_id, node, changes):
        """只取读取区间与本次区间变化相交的映射。"""
        conditions = " OR ".join(
            ["(read_start < ? AND read_end > ?)"] * len(changes))
        params = [snapshot_id, node[0], node[1]]
        for start, end in changes:
            params.extend((end, start))
        rows = self.conn.execute(
            f"""SELECT mapping_id, src_id, src_version, dst_id, dst_version,
                       read_start, read_end, out_start, out_end, active
                FROM quality_mappings
                WHERE snapshot_id=? AND dst_id=? AND dst_version=?
                  AND ({conditions})
                ORDER BY mapping_id""",
            params,
        ).fetchall()
        return [
            {"mapping_id": r[0], "src": (r[1], r[2]), "dst": (r[3], r[4]),
             "read": (r[5], r[6]), "out": (r[7], r[8]), "active": bool(r[9])}
            for r in rows
        ]

    def _propagate(self, snapshot_id, start_node, delta) -> None:
        queue = deque([(start_node, delta)])
        while queue:
            node, (added, removed) = queue.popleft()
            changes = normalize(list(added) + list(removed))
            if not changes:
                continue
            mappings = self._candidate_mappings(snapshot_id, node, changes)
            self.last_update_stats["mappings_examined"] += len(mappings)

            affected_downstream = set()
            for mapping in mappings:
                active = self._activation(
                    snapshot_id, node, mapping["read"][0], mapping["read"][1])
                if active == mapping["active"]:
                    continue
                self.conn.execute(
                    """UPDATE quality_mappings SET active=?
                       WHERE snapshot_id=? AND mapping_id=?""",
                    (1 if active else 0, snapshot_id,
                     mapping["mapping_id"]),
                )
                if active:
                    self.last_update_stats["mappings_activated"] += 1
                    self._add_support(
                        snapshot_id, mapping["src"][0], mapping["src"][1],
                        _SOURCE_MAPPING, mapping["mapping_id"],
                        mapping["out"][0], mapping["out"][1])
                else:
                    self.last_update_stats["mappings_deactivated"] += 1
                    self._remove_support(
                        snapshot_id, _SOURCE_MAPPING, mapping["mapping_id"])
                affected_downstream.add(mapping["src"])

            # 同一上游的多个映射先全部改完，下游节点只重算一次
            for src in affected_downstream:
                next_delta = self._refresh_node(
                    snapshot_id, src[0], src[1])
                if next_delta is not None:
                    queue.append((src, next_delta))


__all__ = [
    "QualityView",
    "normalize",
    "subtract",
    "QUALITY_AVAILABLE",
    "QUALITY_RECYCLED",
    "QUALITY_NO_VIEW",
]
