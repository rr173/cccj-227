"""领域异常体系。"""


class ReclaimerError(Exception):
    """所有计划器错误的基类。"""


class LineageValidationError(ReclaimerError):
    """谱系导入违反不变量（未知端点、环、重复边等）。"""


class PolicyValidationError(ReclaimerError):
    """保留策略编译失败（重叠规则、未知对象类型等）。"""


class WatermarkConflictError(ReclaimerError):
    """消费者水位缺失或不一致。

    导入阶段默认仅记录问题（``watermark_issues``）而不抛出，
    计划阶段会据此阻断回收；显式调用严格校验时抛出本异常。
    """


class PlanPausedError(ReclaimerError):
    """预检发现绑定输入漂移，计划已暂停。"""

    def __init__(self, plan_id: str, diffs):
        self.plan_id = plan_id
        self.diffs = list(diffs)
        super().__init__(
            f"plan {plan_id} paused due to input drift: "
            + ", ".join(d["kind"] for d in self.diffs)
        )


class PlanNotExecutableError(ReclaimerError):
    """计划当前状态不允许执行（非 PAUSED/READY/RUNNING）。"""


class ObjectUnknownError(ReclaimerError):
    """查询的对象身份在任何快照中都不存在（区别于“已回收”）。"""


class QualityValidationError(ReclaimerError):
    """分区质量视图登记或查询参数违反不变量。

    包括：快照未导入、快照外的边、缺少映射、端点未声明、区间越界或
    为空等。无效定义不会在库中留下任何半份映射。
    """


class QualityFaultError(ReclaimerError):
    """报障编号冲突或指向未知报障。

    - 同一快照内同一编号换载荷（对象/版本/区间不同）→ 冲突；
    - 解除一个从未登记的编号 → 未知。
    """
