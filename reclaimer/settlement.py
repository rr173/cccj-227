"""存储费用结算：把供应商按存储层给出的总费用拆到成本中心。

问题背景
========
供应商只按存储层结算总额；湖仓目录里多个逻辑版本可能共享同一份物理
内容（相同 ``(storage_layer, payload_ref)``），共享期间不能各算一份
存储；已回收版本虽然今天不再是 LIVE，其历史引用仍参与历史账单。

输入（两路记录都允许一次性迭代器，不要求调用方先排序）
======================================================
- 结算期 ``[from_ts, to_ts)``（整秒半开区间）；
- ``layer_amounts``：每层应付的非负整数分（映射或 ``(layer, cents)`` 对）；
- ``metering``：无序计量记录 ``(storage_layer, payload_ref, start, end,
  bytes)``，段非空、完全落在结算期内、字节数为正整数；同一
  ``(storage_layer, payload_ref)`` 的段不得重叠（可紧贴、可留空档，
  空档不计费）；同名 ``payload_ref`` 在不同层是不同物理内容；
- ``attributions``：``(object_id, version, cost_center)``，每版本恰好
  一个非空成本中心，结算期内不变。

目录事实（对象版本、回收时刻）在同一份 SQLite 读取快照内取得：全程一个
事务，不按今天的 LIVE 状态过滤——已回收版本的引用区间是
``[created_at, recycled_at)``，存活版本以 ``to_ts`` 为界。

分摊规则
========
1. 只在引用与计量同时有效的时间内要求该版本有成本中心归属；缺失即
   拒绝整次结算（不悄悄转嫁）。计量有效而完全无引用的时间进入专门的
   “未归属”项（``cost_center=None``），不与普通中心名合并。
2. 在引用集合与字节数都不变的时间段内，把 “字节数 × 秒数” 均分给当时
   所有**不同**的成本中心（同一中心两个版本同时引用只算一个）。
3. 先按层汇总各中心整个结算期的精确份额（``fractions.Fraction``，
   不用浮点），再按该层应付总额换算成分：各项向下取整，余下的分按
   小数余数从大到小补发（每项最多一分）；余数相同按中心名 Unicode
   码点顺序，未归属项排在同余数的普通中心之后。同层各项之和恒等于
   应付金额。

性能组织
========
- 计量与归属先流入 TEMP 暂存表，由 SQLite 排序；Python 侧按
  ``(layer, payload_ref)`` 归并两条有序流，同一时刻只在内存中持有
  **一份物理内容** 的计量段与引用；最终汇总（每 层×中心 一个
  Fraction）才驻留内存。
- 单份内容内不按秒展开：引用区间切出 O(引用数) 个“引用集合恒定”的
  时间跨度（span），计量段与 span 做双指针归并，先把字节秒累进
  span，再按 span 的中心集合一次性分摊。工作量是
  O(计量段数 + 引用数 + Σ_span 活跃中心数)，**不**随秒数增长，也
  **不**随 “计量段数 × 活跃中心数” 增长（段边界变化时从不重扫活跃
  中心集合）。
- 字节秒中间总量可能超过 SQLite 有符号整数范围：段字节数以 TEXT
  暂存，乘积与累加全部在 Python 大整数上进行。

失败语义
========
无效区间、金额为负、计量层缺少应付金额、正费用层没有任何计量、段为
空/越界/字节数非正、同一物理内容计量重叠、归属未知版本、同版本重复
归属、引用与计量同时有效时缺失归属——任何一种都使整次调用抛出
:class:`SettlementError`：不返回半张账单，也不改写任何业务表（暂存
表随事务回滚/丢弃）。
"""

from __future__ import annotations

import sqlite3
from fractions import Fraction
from itertools import groupby

from .errors import SettlementError

_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1

# 暂存表名（TEMP，连接私有；名字加前缀避免与计划器临时表冲突）
_T_METERING = "stl_metering"
_T_ATTR = "stl_attr"

# 目录事实查询：不按 lifecycle 过滤——已回收版本的历史引用必须保留。
# 预过滤掉与结算期不可能相交的版本（创建不早于期末 / 回收不晚于期初）。
_REFS_SQL = f"""
SELECT o.storage_layer, o.payload_ref, o.object_id, o.version,
       o.created_at, o.recycled_at, a.center
FROM objects o
JOIN (SELECT DISTINCT layer, payload FROM {_T_METERING}) m
  ON m.layer = o.storage_layer AND m.payload = o.payload_ref
LEFT JOIN {_T_ATTR} a
  ON a.object_id = o.object_id AND a.version = o.version
WHERE o.created_at < :to_ts
  AND (o.recycled_at IS NULL OR o.recycled_at > :from_ts)
ORDER BY o.storage_layer, o.payload_ref,
         o.created_at, o.object_id, o.version
"""

_METERING_SQL = f"""
SELECT layer, payload, start_ts, end_ts, bytes_txt
FROM {_T_METERING}
ORDER BY layer, payload, start_ts, end_ts
"""


def _as_tuple(rec, nfields, what):
    if isinstance(rec, (str, bytes)):
        raise SettlementError(f"{what} record must be a {nfields}-tuple: "
                              f"{rec!r}")
    try:
        rec = tuple(rec)
    except TypeError:
        raise SettlementError(f"{what} record must be a {nfields}-tuple: "
                              f"{rec!r}")
    if len(rec) != nfields:
        raise SettlementError(f"{what} record must have exactly {nfields} "
                              f"fields: {rec!r}")
    return rec


class Settlement:
    """一次存储费用结算的算法载体（无状态，可反复调用）。"""

    def __init__(self, conn):
        self.conn = conn

    # ============================================================ 入口
    def settle(self, *, from_ts, to_ts, layer_amounts, metering,
               attributions) -> dict:
        """执行一次结算，返回按存储层与成本中心拆分的账单。

        返回结构（行序确定，与输入顺序无关）::

            {
              "from_ts": int, "to_ts": int,
              "items": [{"storage_layer": str,
                         "cost_center": str | None,   # None = 未归属
                         "unattributed": bool,
                         "share": Fraction,           # 精确份额（字节·秒）
                         "cents": int}, ...],          # 按 (层, 未归属在后,
                                                      #      中心名码点) 排序
              "layers": [{"storage_layer": str,
                          "amount_due_cents": int,
                          "share_total": Fraction,
                          "allocated_cents": int}, ...],
              "total_due_cents": int,
              "total_allocated_cents": int,
              "stats": {...},                          # 工作量计数
            }
        """
        from_ts, to_ts = self._check_period(from_ts, to_ts)
        amounts = self._check_amounts(layer_amounts)

        conn = self.conn
        # 同一事务 = 同一份 SQLite 读取快照；出错整体回滚，不留任何痕迹。
        conn.execute("SAVEPOINT stl_settle")
        try:
            self._create_stage(conn)
            self._stage_metering(conn, metering, from_ts, to_ts)
            self._check_metering_nonoverlap(conn)
            self._stage_attributions(conn, attributions)
            self._check_attribution_targets(conn)
            bill = self._compute(conn, from_ts, to_ts, amounts)
        except BaseException:
            try:
                conn.execute("ROLLBACK TO SAVEPOINT stl_settle")
                conn.execute("RELEASE SAVEPOINT stl_settle")
            except sqlite3.Error:
                pass
            raise
        conn.execute("RELEASE SAVEPOINT stl_settle")
        return bill

    # ============================================================ 标量校验
    @staticmethod
    def _require_int(value, what):
        if isinstance(value, bool) or not isinstance(value, int):
            raise SettlementError(f"{what} must be an integer, got {value!r}")
        return value

    @classmethod
    def _require_ts(cls, value, what):
        cls._require_int(value, what)
        if not (_INT64_MIN <= value <= _INT64_MAX):
            raise SettlementError(f"{what} out of signed 64-bit range")
        return value

    @classmethod
    def _check_period(cls, from_ts, to_ts):
        cls._require_ts(from_ts, "from_ts")
        cls._require_ts(to_ts, "to_ts")
        if from_ts >= to_ts:
            raise SettlementError(
                f"invalid settlement period [{from_ts}, {to_ts})")
        return from_ts, to_ts

    @classmethod
    def _check_amounts(cls, layer_amounts):
        if hasattr(layer_amounts, "items"):
            pairs = list(layer_amounts.items())
        else:
            pairs = list(layer_amounts)
        amounts = {}
        for pair in pairs:
            try:
                layer, cents = pair
            except (TypeError, ValueError):
                raise SettlementError(
                    f"layer amount must be a (layer, cents) pair: {pair!r}")
            if not isinstance(layer, str) or not layer:
                raise SettlementError(f"invalid storage layer name: {layer!r}")
            cls._require_int(cents, f"amount for layer {layer!r}")
            if cents < 0:
                raise SettlementError(
                    f"amount for layer {layer!r} must be non-negative")
            if layer in amounts:
                raise SettlementError(
                    f"duplicate amount for layer {layer!r}")
            amounts[layer] = cents
        return amounts

    # ============================================================ 暂存
    @staticmethod
    def _create_stage(conn):
        conn.execute(f"DROP TABLE IF EXISTS {_T_METERING}")
        conn.execute(f"DROP TABLE IF EXISTS {_T_ATTR}")
        # bytes 以 TEXT 暂存：单段字节数可以是任意精度正整数，
        # SQLite INTEGER 只有 64 位；排序不依赖 bytes。
        conn.execute(
            f"""
            CREATE TEMP TABLE {_T_METERING}(
                layer     TEXT NOT NULL,
                payload   TEXT NOT NULL,
                start_ts  INTEGER NOT NULL,
                end_ts    INTEGER NOT NULL,
                bytes_txt TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TEMP TABLE {_T_ATTR}(
                object_id TEXT NOT NULL,
                version   INTEGER NOT NULL,
                center    TEXT NOT NULL,
                PRIMARY KEY (object_id, version)
            )
            """
        )

    def _stage_metering(self, conn, metering, from_ts, to_ts):
        # 生成器逐条校验逐条入库：全批计量不在 Python 内存中驻留
        def rows():
            for rec in metering:
                layer, payload, start, end, nbytes = \
                    self._parse_metering(rec, from_ts, to_ts)
                yield (layer, payload, start, end, str(nbytes))

        conn.executemany(
            f"INSERT INTO {_T_METERING}(layer, payload, start_ts, end_ts,"
            f" bytes_txt) VALUES(?,?,?,?,?)", rows())

    def _parse_metering(self, rec, from_ts, to_ts):
        if isinstance(rec, dict):
            try:
                rec = (rec["storage_layer"], rec["payload_ref"],
                       rec["start"], rec["end"], rec["bytes"])
            except KeyError as exc:
                raise SettlementError(
                    f"metering record missing field {exc}")
        else:
            rec = _as_tuple(rec, 5, "metering")
        layer, payload, start, end, nbytes = rec
        if not isinstance(layer, str) or not layer:
            raise SettlementError(f"metering with invalid layer: {layer!r}")
        if not isinstance(payload, str) or not payload:
            raise SettlementError(
                f"metering with invalid payload_ref: {payload!r}")
        self._require_ts(start, "metering start")
        self._require_ts(end, "metering end")
        self._require_int(nbytes, "metering bytes")
        if nbytes <= 0:
            raise SettlementError(
                f"metering bytes must be a positive integer: {nbytes!r}")
        if not (from_ts <= start < end <= to_ts):
            raise SettlementError(
                f"metering segment [{start}, {end}) is empty or not fully "
                f"inside the settlement period [{from_ts}, {to_ts})")
        return layer, payload, start, end, nbytes

    def _check_metering_nonoverlap(self, conn):
        """同一物理内容的段不得重叠；按 (内容, start) 有序后邻接检查即可。"""
        prev = None  # (layer, payload, end)
        for layer, payload, start, end in conn.execute(
                f"SELECT layer, payload, start_ts, end_ts FROM {_T_METERING}"
                f" ORDER BY layer, payload, start_ts, end_ts"):
            if prev is not None and prev[:2] == (layer, payload) \
                    and start < prev[2]:
                raise SettlementError(
                    f"overlapping metering segments for "
                    f"({layer!r}, {payload!r})")
            prev = (layer, payload, end)

    def _stage_attributions(self, conn, attributions):
        for rec in attributions:
            object_id, version, center = self._parse_attribution(rec)
            try:
                conn.execute(
                    f"INSERT INTO {_T_ATTR}(object_id, version, center)"
                    f" VALUES(?,?,?)", (object_id, version, center))
            except sqlite3.IntegrityError:
                raise SettlementError(
                    f"duplicate attribution for {object_id} v{version}")

    def _parse_attribution(self, rec):
        if isinstance(rec, dict):
            try:
                rec = (rec["object_id"], rec["version"],
                       rec["cost_center"])
            except KeyError as exc:
                raise SettlementError(
                    f"attribution record missing field {exc}")
        else:
            rec = _as_tuple(rec, 3, "attribution")
        object_id, version, center = rec
        if not isinstance(object_id, str) or not object_id:
            raise SettlementError(
                f"attribution with invalid object_id: {object_id!r}")
        self._require_int(version, "attribution version")
        if version < 1:
            raise SettlementError(f"attribution version must be >= 1: "
                                  f"{object_id} v{version}")
        if not isinstance(center, str) or not center.strip():
            raise SettlementError(
                f"attribution cost center must be a non-empty string: "
                f"{center!r}")
        return object_id, version, center

    def _check_attribution_targets(self, conn):
        row = conn.execute(
            f"""
            SELECT a.object_id, a.version FROM {_T_ATTR} a
            LEFT JOIN objects o
              ON o.object_id = a.object_id AND o.version = a.version
            WHERE o.object_id IS NULL LIMIT 1
            """
        ).fetchone()
        if row is not None:
            raise SettlementError(
                f"attribution targets unknown object version: "
                f"{row[0]} v{row[1]}")

    # ============================================================ 计算
    def _compute(self, conn, from_ts, to_ts, amounts):
        metered_layers = {
            r[0] for r in conn.execute(
                f"SELECT DISTINCT layer FROM {_T_METERING}")
        }
        for layer in sorted(metered_layers):
            if layer not in amounts:
                raise SettlementError(
                    f"layer {layer!r} has metering records but no amount "
                    f"due from the supplier bill")
        for layer, cents in sorted(amounts.items()):
            if cents > 0 and layer not in metered_layers:
                raise SettlementError(
                    f"layer {layer!r} has positive amount {cents} but no "
                    f"metering records")

        shares = {}   # (layer, center) -> Fraction 精确份额
        unattributed = {}  # layer -> Fraction
        stats = {"contents": 0, "references": 0, "metered_pieces": 0,
                 "center_share_additions": 0}

        seg_groups = self._groups(conn.execute(_METERING_SQL))
        ref_groups = self._groups(
            conn.execute(_REFS_SQL, {"from_ts": from_ts, "to_ts": to_ts}))
        ref_key, ref_rows = next(ref_groups, (None, None))
        for seg_key, seg_rows in seg_groups:
            refs = []
            # 两条流都按 (layer, payload) 有序，归并取同一物理内容
            while ref_key is not None and ref_key < seg_key:
                ref_key, ref_rows = next(ref_groups, (None, None))
            if ref_key == seg_key:
                refs = ref_rows
                ref_key, ref_rows = next(ref_groups, (None, None))
            self._settle_content(
                seg_key[0], seg_rows, refs, from_ts, to_ts,
                shares, unattributed, stats)

        return self._assemble(from_ts, to_ts, amounts, shares,
                              unattributed, stats)

    @staticmethod
    def _groups(cursor):
        """把有序游标切成 (键, 行列表) 组；同一时刻只物化一组。"""
        for key, grp in groupby(cursor, lambda r: (r[0], r[1])):
            yield key, list(grp)

    def _settle_content(self, layer, seg_rows, ref_rows, from_ts, to_ts,
                        shares, unattributed, stats):
        """分摊一份物理内容：span 归并 + 按恒定引用集合整体分摊。"""
        segments = [(s, e, int(b))
                    for (_, _, s, e, b) in seg_rows]
        # 引用区间裁剪到结算期；空区间（如回收时刻不晚于创建时刻）跳过
        refs = []
        for (_, _, oid, ver, created_at, recycled_at, center) in ref_rows:
            rstart = max(created_at, from_ts)
            rend = min(recycled_at if recycled_at is not None else to_ts,
                       to_ts)
            if rstart < rend:
                refs.append((rstart, rend, center, oid, ver))
        stats["contents"] += 1
        stats["references"] += len(refs)

        # 引用事件 -> 引用集合恒定的时间跨度（span）。
        # span 数量 <= 2*引用数 + 1，与秒数无关。
        deltas = {}
        for rstart, rend, center, _, _ in refs:
            deltas.setdefault(rstart, []).append((center, 1))
            deltas.setdefault(rend, []).append((center, -1))
        points = sorted(deltas)
        boundaries = [from_ts] + points + [to_ts]

        counts = {}          # center(或 None=未归属引用) -> 活跃引用数
        seg_i = 0            # 计量段游标：与 span 双指针归并
        nseg = len(segments)
        for i in range(len(boundaries) - 1):
            p, q = boundaries[i], boundaries[i + 1]
            if i > 0:
                for center, delta in deltas[p]:
                    new = counts.get(center, 0) + delta
                    if new:
                        counts[center] = new
                    else:
                        del counts[center]
            if p >= q:
                continue
            # 把 [p,q) 内各计量段的字节秒累进本 span（不逐段分摊）
            byte_seconds = 0
            while seg_i < nseg and segments[seg_i][1] <= p:
                seg_i += 1
            j = seg_i
            while j < nseg and segments[j][0] < q:
                s, e, b = segments[j]
                byte_seconds += b * (min(e, q) - max(s, p))
                stats["metered_pieces"] += 1
                j += 1
            if byte_seconds == 0:
                continue
            if counts.get(None):
                # 引用与计量同时有效却缺失归属：拒绝整次结算
                bad = next(
                    (f"{oid} v{ver}"
                     for (rs, re, c, oid, ver) in refs
                     if c is None and rs < q and re > p),
                    "unknown version")
                raise SettlementError(
                    f"version {bad} references metered content "
                    f"({layer!r}) during [{p}, {q}) but has no cost "
                    f"center attribution")
            centers = [c for c in counts if c is not None]
            if not centers:
                # 计量有效而没有任何逻辑引用：进入“未归属”项
                unattributed[layer] = (unattributed.get(layer, Fraction(0))
                                       + Fraction(byte_seconds))
                continue
            share = Fraction(byte_seconds, len(centers))
            for center in centers:
                key = (layer, center)
                shares[key] = shares.get(key, Fraction(0)) + share
            stats["center_share_additions"] += len(centers)

    # ============================================================ 换算成分
    def _assemble(self, from_ts, to_ts, amounts, shares, unattributed,
                  stats):
        per_layer = {}
        for (layer, center), share in shares.items():
            per_layer.setdefault(layer, []).append((center, share))
        for layer, share in unattributed.items():
            per_layer.setdefault(layer, []).append((None, share))

        items = []
        layers = []
        total_due = 0
        total_allocated = 0
        for layer in sorted(set(amounts) | set(per_layer)):
            due = amounts.get(layer, 0)
            layer_shares = per_layer.get(layer, [])
            share_total = sum((s for _, s in layer_shares), Fraction(0))
            allocated = 0
            if layer_shares:
                for center, share, cents in self._to_cents(
                        due, layer_shares):
                    items.append({
                        "storage_layer": layer,
                        "cost_center": center,
                        "unattributed": center is None,
                        "share": share,
                        "cents": cents,
                    })
                    allocated += cents
            layers.append({
                "storage_layer": layer,
                "amount_due_cents": due,
                "share_total": share_total,
                "allocated_cents": allocated,
            })
            total_due += due
            total_allocated += allocated

        # 行序确定：层码点序；层内普通中心按码点序，未归属项最后
        items.sort(key=lambda it: (it["storage_layer"], it["unattributed"],
                                   it["cost_center"] or ""))
        return {
            "from_ts": from_ts,
            "to_ts": to_ts,
            "items": items,
            "layers": layers,
            "total_due_cents": total_due,
            "total_allocated_cents": total_allocated,
            "stats": stats,
        }

    @staticmethod
    def _to_cents(due, layer_shares):
        """最大余数法把一层总额拆成整数分（精确分数运算，无浮点）。

        各项先向下取整；余下的分按小数余数从大到小补发，每项最多一分；
        余数相同按中心名 Unicode 码点顺序，未归属项排在同余数的普通
        中心之后。
        """
        total = sum((s for _, s in layer_shares), Fraction(0))
        rows = []
        floors = 0
        for center, share in layer_shares:
            exact = Fraction(due) * share / total
            floor = exact.numerator // exact.denominator
            rows.append((center, share, floor, exact - floor))
            floors += floor
        remainder = due - floors
        # 余数之和恒等于 remainder 且每个余数 < 1，故 0 <= remainder < 项数
        rows.sort(key=lambda r: (-r[3], r[0] is None,
                                 r[0] if r[0] is not None else ""))
        out = []
        for i, (center, share, floor, _frac) in enumerate(rows):
            out.append((center, share, floor + (1 if i < remainder else 0)))
        return out


__all__ = ["Settlement"]
