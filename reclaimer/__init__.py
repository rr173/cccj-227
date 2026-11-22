"""湖仓分阶段回收计划器（Python + SQLite）。

模块划分
--------
- ``schema``     : SQLite 表结构 / 触发器 / 状态机约束
- ``hashing``    : 策略版本、谱系快照、冻结集合、消费者水位的规范化指纹
- ``importer``   : 谱系导入（对象、派生边、策略、冻结、水位、解除事件）
- ``planner``    : 策略编译与分阶段计划生成（候选 / 冻结等待 / 可回收）
- ``precheck``   : 执行前重新验证绑定输入，变化则暂停并生成差异
- ``executor``   : 分批次叶序删除、失败重试、进程退出恢复
- ``audit``      : 审计查询（对象状态、证据、计划、差异、事件）
- ``quality``    : 分区质量隔离视图（区间元数据，不删数据）
- ``api``        : 对外门面 ``Reclaimer``
"""

from .api import Reclaimer
from .quality import (
    QUALITY_AVAILABLE,
    QUALITY_NO_VIEW,
    QUALITY_RECYCLED,
    QualityView,
)

__all__ = ["Reclaimer", "QualityView", "QUALITY_AVAILABLE",
           "QUALITY_RECYCLED", "QUALITY_NO_VIEW"]
__version__ = "1.0.0"
