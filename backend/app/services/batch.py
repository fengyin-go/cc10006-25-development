"""批量转换业务规则：状态流转、字段校验、失败隔离与分区重放。

设计要点：
1. 任务按分区推进，每个分区独立记录成功/失败/未完成状态与输出路径；
2. 重放范围由输入路径、分区覆盖、依赖上游共同界定，只从最早失败分区开始，
   已成功分区的输出路径永不变化（不重复写输出）；
3. 同一任务的重放全局串行：存在运行中实例时重复提交复用该实例，不新建执行实例；
4. 失败历史只追加不覆盖，同一分区再次失败时以最早失败记录为准；
5. 失败清单驱动质量异常台账与清洗层（上游）待处理队列，跨模块同步。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "batch"
QUALITY_MODULE = "quality"
SILVER_MODULE = "silver"
REQUIRED_FIELDS = ["任务编号", "输入路径", "处理脚本"]
STATUS_ORDER = ["草稿", "待调度", "运行中", "已完成", "已失败"]
ACTION_RULES = {"提交调度": "待调度", "启动任务": "运行中", "终止任务": "已失败", "归档结果": "已完成"}
NEGATIVE_ACTIONS = []

# 分区状态
PART_SUCCESS = "成功"
PART_FAILED = "失败"
PART_PENDING = "未完成"

# 失败记录 / 重放实例状态
FAIL_OPEN = "待处理"
FAIL_RESOLVED = "已解决"
REPLAY_RUNNING = "运行中"
REPLAY_SUCCESS = "已完成"
REPLAY_FAILED = "已失败"

# 同一进程内所有批处理任务共享一把重放锁，保证并发提交时临界区完整。
_replay_lock = threading.RLock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _output_path(entry: dict[str, Any], partition_no: int) -> str:
    return f"{entry.get('输出路径') or ''}/part={partition_no:04d}"


class BatchError(ValueError):
    """非法重放请求（分区范围越界等），路由层翻译成 400。"""


class BatchService:
    # 测试钩子：每个分区处理前回调 (entry, instance, partition_no)，生产环境为 None。
    partition_hook: Any = None
    # ------------------------------------------------------------------ 列表
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        # 登记时允许同时给出输出路径、分区覆盖、依赖上游等执行约束。
        for extra in ("输出路径", "分区覆盖", "依赖上游", "重试策略", "超时终止"):
            if values.get(extra) is not None:
                entry[extra] = values.get(extra)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        try:
            self._init_partitions(entry)
        except (TypeError, ValueError) as exc:
            raise BatchError(f"分区覆盖格式非法：{exc}") from exc
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"批处理任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于批量转换可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"批处理任务已{action}"

    # ------------------------------------------------------------ 分区解析
    @staticmethod
    def parse_coverage(coverage: Any) -> list[int]:
        """把「1-16」「1,3,5-7」这类分区覆盖描述解析成有序去重的分区号列表。"""
        if isinstance(coverage, int):
            return [coverage]
        text = str(coverage or "").strip()
        if not text:
            return []
        result: set[int] = set()
        for chunk in text.split(","):
            piece = chunk.strip()
            if not piece:
                continue
            if "-" in piece:
                left, _, right = piece.partition("-")
                start, end = int(left.strip()), int(right.strip())
                if start > end:
                    raise BatchError(f"非法分区范围「{piece}」：起点不能大于终点")
                result.update(range(start, end + 1))
            else:
                result.add(int(piece))
        return sorted(result)

    def _init_partitions(self, entry: dict[str, Any]) -> None:
        """为新任务初始化分区台账；已有台账（如种子任务）保持原样。"""
        if entry.get("分区结果"):
            return
        partitions = self.parse_coverage(entry.get("分区覆盖"))
        entry["分区总数"] = len(partitions)
        entry["成功分区数"] = 0
        entry["失败分区数"] = 0
        entry["未完成分区数"] = len(partitions)
        entry["分区结果"] = {
            str(no): {
                "分区": no,
                "状态": PART_PENDING,
                "输出路径": None,
                "尝试次数": 0,
                "最近更新时间": None,
            }
            for no in partitions
        }
        entry.setdefault("失败历史", [])
        entry.setdefault("重放实例", [])
        entry.setdefault("重放中", False)

    def _partition_state(self, entry: dict[str, Any], partition_no: int) -> dict[str, Any]:
        return entry["分区结果"][str(partition_no)]

    @staticmethod
    def _open_failures(entry: dict[str, Any]) -> list[dict[str, Any]]:
        return [f for f in entry.get("失败历史", []) if f.get("状态") == FAIL_OPEN]

    def _earliest_failed_partition(self, entry: dict[str, Any]) -> int | None:
        """最早失败记录所在分区：失败分区冲突时以最早失败记录为准。"""
        open_items = self._open_failures(entry)
        if not open_items:
            return None
        return min(int(item["分区"]) for item in open_items)

    # ------------------------------------------------------------ 重放范围
    def replay_scope(self, entry_id: int) -> dict[str, Any] | None:
        """按输入路径、分区覆盖、依赖上游识别可重放范围。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        self._init_partitions(entry)
        coverage = self.parse_coverage(entry.get("分区覆盖"))
        earliest = self._earliest_failed_partition(entry)
        if earliest is None:
            replay_partitions: list[int] = []
        else:
            # 只从最早失败分区继续：之前已成功的分区不在重放范围内。
            replay_partitions = [
                no for no in coverage
                if no >= earliest and self._partition_state(entry, no)["状态"] != PART_SUCCESS
            ]
        return {
            "任务id": entry_id,
            "任务编号": entry.get("任务编号"),
            "输入路径": entry.get("输入路径"),
            "分区覆盖": entry.get("分区覆盖"),
            "依赖上游": entry.get("依赖上游"),
            "处理脚本": entry.get("处理脚本"),
            "最早失败分区": earliest,
            "可重放分区": replay_partitions,
            "已成功分区": [no for no in coverage if self._partition_state(entry, no)["状态"] == PART_SUCCESS],
            "重放中": bool(entry.get("重放中")),
        }

    # ------------------------------------------------------------ 重放执行
    def replay(
        self,
        entry_id: int,
        *,
        start_partition: int | None = None,
        end_partition: int | None = None,
        fail_partitions: list[int] | None = None,
        reason: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, int]:
        """失败隔离重放。

        返回 (实例, 消息, http状态码)：并发重复提交时复用运行中实例，返回 200。
        fail_partitions 用于模拟重放仍失败的分区（演示失败历史追加与台账联动）。
        """
        with _replay_lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"批处理任务 {entry_id} 不存在或已归档", 404
            self._init_partitions(entry)

            # 并发/重复提交：同一任务只允许一个执行实例。
            for inst in entry["重放实例"]:
                if inst["状态"] == REPLAY_RUNNING:
                    return inst, "该任务已有运行中的重放实例，重复提交已合并，未重复写输出", 200

            coverage = self.parse_coverage(entry.get("分区覆盖"))
            earliest = self._earliest_failed_partition(entry)
            if earliest is None:
                return None, "任务没有待处理的失败分区，无需重放", 400

            allowed = [
                no for no in coverage
                if no >= earliest and self._partition_state(entry, no)["状态"] != PART_SUCCESS
            ]

            # 校验请求的分区范围：非法范围一律拒绝。
            requested = self._requested_range(entry, coverage, allowed, start_partition, end_partition)
            injected_failures = self._validate_injected_failures(fail_partitions, coverage, allowed)

            instance = self._create_instance(entry, requested, reason)
            entry["重放中"] = True
            entry["status"] = "运行中"

        # 分区执行放在锁外：并发请求进入临界区即可看到运行中实例并立即合并返回，
        # 不会被整段执行过程阻塞，也不会创建第二个实例或重复写输出。
        try:
            self._run_partitions(entry, instance, requested, injected_failures)
        finally:
            with _replay_lock:
                entry["重放中"] = False
                instance["结束时间"] = _now()
                self._refresh_task_counters(entry)
                self.sync_ledgers()
        if instance["状态"] == REPLAY_SUCCESS:
            return instance, f"重放完成，处理分区 {requested[0]}-{requested[-1]}，已成功分区输出未重复写入", 200
        return instance, "重放结束但仍有分区失败，失败原因已追加到历史清单", 200

    def _requested_range(
        self,
        entry: dict[str, Any],
        coverage: list[int],
        allowed: list[int],
        start_partition: int | None,
        end_partition: int | None,
    ) -> list[int]:
        if start_partition is None and end_partition is None:
            return list(allowed)
        start = coverage[0] if start_partition is None else start_partition
        end = coverage[-1] if end_partition is None else end_partition
        if start not in coverage or end not in coverage or start > end:
            raise BatchError(
                f"非法分区范围 {start}-{end}：超出任务分区覆盖「{entry.get('分区覆盖')}」"
            )
        requested = [no for no in coverage if start <= no <= end]
        outside = [no for no in requested if no not in allowed]
        if outside:
            raise BatchError(
                f"非法分区范围 {start}-{end}：分区 {outside} 已成功或早于最早失败分区，"
                "重放不得覆盖已成功输出"
            )
        return requested

    @staticmethod
    def _validate_injected_failures(
        fail_partitions: list[int] | None,
        coverage: list[int],
        allowed: list[int],
    ) -> set[int]:
        if not fail_partitions:
            return set()
        bad = [no for no in fail_partitions if no not in coverage]
        if bad:
            raise BatchError(f"非法分区范围：模拟失败分区 {bad} 超出任务分区覆盖")
        not_allowed = [no for no in fail_partitions if no not in allowed]
        if not_allowed:
            raise BatchError(f"非法分区范围：分区 {not_allowed} 不在本次可重放范围内")
        return set(int(no) for no in fail_partitions)

    def _create_instance(
        self,
        entry: dict[str, Any],
        requested: list[int],
        reason: str | None,
    ) -> dict[str, Any]:
        seq = len(entry["重放实例"]) + 1
        instance = {
            "重放实例编号": f"{entry['任务编号']}-R{seq:03d}",
            "任务id": entry["id"],
            "任务编号": entry["任务编号"],
            "输入路径": entry.get("输入路径"),
            "处理脚本": entry.get("处理脚本"),
            "依赖上游": entry.get("依赖上游"),
            "起始分区": requested[0],
            "结束分区": requested[-1],
            "计划分区": list(requested),
            "已处理分区": [],
            "成功分区": [],
            "失败分区": [],
            "状态": REPLAY_RUNNING,
            "提交时间": _now(),
            "开始时间": _now(),
            "结束时间": None,
            "触发原因": reason or "失败分区隔离重放",
        }
        entry["重放实例"].append(instance)
        return instance

    def _run_partitions(
        self,
        entry: dict[str, Any],
        instance: dict[str, Any],
        requested: list[int],
        injected_failures: set[int],
    ) -> None:
        """逐分区推进：成功才写输出路径；再次失败则在失败处停止，后续分区保持未完成。"""
        for no in requested:
            hook = type(self).partition_hook
            if hook is not None:
                hook(entry, instance, no)
            state = self._partition_state(entry, no)
            # 幂等保护：已成功分区直接跳过，绝不重复写输出。
            if state["状态"] == PART_SUCCESS:
                instance["已处理分区"].append(no)
                instance["成功分区"].append(no)
                continue
            state["尝试次数"] = int(state.get("尝试次数") or 0) + 1
            state["最近更新时间"] = _now()
            instance["已处理分区"].append(no)
            if no in injected_failures:
                fail_reason = (
                    f"重放实例 {instance['重放实例编号']} 执行处理脚本 "
                    f"{entry.get('处理脚本')} 时分区 {no} 再次失败：依赖上游 "
                    f"{entry.get('依赖上游')} 数据未就绪"
                )
                state["状态"] = PART_FAILED
                state["输出路径"] = None
                state["失败原因"] = fail_reason
                instance["失败分区"].append(no)
                self._append_failure(entry, no, fail_reason, instance)
                instance["状态"] = REPLAY_FAILED
                # 失败隔离：停在失败分区，后续未完成分区不在本次重放内继续。
                break
            state["状态"] = PART_SUCCESS
            state["输出路径"] = _output_path(entry, no)
            state.pop("失败原因", None)
            instance["成功分区"].append(no)
            self._resolve_failure(entry, no, instance)
        else:
            instance["状态"] = REPLAY_SUCCESS
        if instance["状态"] == REPLAY_RUNNING:
            instance["状态"] = REPLAY_SUCCESS
        # 任务级状态：全部成功才完成，否则仍是已失败待重放。
        self._refresh_task_counters(entry)
        if entry["失败分区数"] == 0 and entry["未完成分区数"] == 0:
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False
        else:
            entry["status"] = "已失败"
            entry["pending"] = True
            entry["abnormal"] = True

    def _append_failure(
        self,
        entry: dict[str, Any],
        partition_no: int,
        reason: str,
        instance: dict[str, Any],
    ) -> None:
        """失败历史只追加；同分区已有待处理记录时保留最早一条，新记录标注冲突。"""
        history = entry.setdefault("失败历史", [])
        seq = len(history) + 1
        record = {
            "失败记录编号": f"{entry['任务编号']}-F{seq:03d}",
            "任务编号": entry["任务编号"],
            "任务id": entry["id"],
            "分区": partition_no,
            "失败时间": _now(),
            "处理脚本": entry.get("处理脚本"),
            "依赖上游": entry.get("依赖上游"),
            "输入路径": entry.get("输入路径"),
            "失败原因": reason,
            "状态": FAIL_OPEN,
            "重放实例编号": instance["重放实例编号"],
            "解决时间": None,
        }
        earlier_open = [
            f for f in history
            if int(f["分区"]) == partition_no and f.get("状态") == FAIL_OPEN
        ]
        if earlier_open:
            earliest = min(earlier_open, key=lambda item: str(item.get("失败时间")))
            record["冲突说明"] = (
                f"分区 {partition_no} 存在更早失败记录 {earliest['失败记录编号']}，"
                "按最早失败记录为准，本记录仅留存原因"
            )
        history.append(record)

    def _resolve_failure(self, entry: dict[str, Any], partition_no: int, instance: dict[str, Any]) -> None:
        """分区重放成功：核销该分区全部待处理失败记录，历史记录本身保留。"""
        for item in entry.get("失败历史", []):
            if int(item["分区"]) == partition_no and item.get("状态") == FAIL_OPEN:
                item["状态"] = FAIL_RESOLVED
                item["解决时间"] = _now()
                item["解决实例编号"] = instance["重放实例编号"]

    def _refresh_task_counters(self, entry: dict[str, Any]) -> None:
        states = list(entry.get("分区结果", {}).values())
        entry["分区总数"] = len(states)
        entry["成功分区数"] = sum(1 for s in states if s["状态"] == PART_SUCCESS)
        entry["失败分区数"] = sum(1 for s in states if s["状态"] == PART_FAILED)
        entry["未完成分区数"] = sum(1 for s in states if s["状态"] == PART_PENDING)

    # ------------------------------------------------------------ 失败清单
    def failure_history(
        self,
        entry_id: int,
        *,
        script: str | None = None,
        upstream: str | None = None,
        status_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """失败历史：可按处理脚本与依赖上游定位，历史每次失败的时间与分区都保留。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return []
        items = list(entry.get("失败历史", []))
        if script:
            items = [f for f in items if script in str(f.get("处理脚本", ""))]
        if upstream:
            items = [f for f in items if upstream == f.get("依赖上游")]
        if status_filter:
            items = [f for f in items if f.get("状态") == status_filter]
        return items

    def current_failures(self) -> list[dict[str, Any]]:
        """跨任务的当前失败清单（去重，每个分区仅保留最早待处理记录），驱动各台账。"""
        result: list[dict[str, Any]] = []
        for entry in store.rows(MODULE):
            earliest_by_partition: dict[int, dict[str, Any]] = {}
            for item in entry.get("失败历史", []):
                if item.get("状态") != FAIL_OPEN:
                    continue
                no = int(item["分区"])
                current = earliest_by_partition.get(no)
                if current is None or str(item.get("失败时间")) < str(current.get("失败时间")):
                    earliest_by_partition[no] = item
            result.extend(earliest_by_partition.values())
        return sorted(result, key=lambda item: (str(item.get("任务编号")), int(item["分区"])))

    def replay_instances(self, entry_id: int) -> list[dict[str, Any]]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return []
        return list(entry.get("重放实例", []))

    # ------------------------------------------------------ 跨模块质量台账
    def sync_ledgers(self) -> dict[str, int]:
        """失败清单驱动质量异常台账与清洗层待处理队列（幂等，可反复调用）。"""
        open_failures = self.current_failures()
        quality_synced = self._sync_quality_ledger(open_failures)
        silver_synced = self._sync_silver_queue(open_failures)
        return {"质量异常台账": quality_synced, "清洗待处理队列": silver_synced}

    def _sync_quality_ledger(self, open_failures: list[dict[str, Any]]) -> int:
        rows = store.rows(QUALITY_MODULE)
        # 先核销已解决的批处理质量异常。
        active_keys = {
            (str(item.get("任务编号")), int(item["分区"])) for item in open_failures
        }
        for row in rows:
            if row.get("来源模块") != MODULE:
                continue
            key = (str(row.get("来源任务")), int(row.get("来源分区")))
            if key not in active_keys:
                row["status"] = "已停用"
                row["pending"] = False
                row["abnormal"] = False
                row["异常状态"] = "已核销"
        # 再按当前失败清单逐条对齐。
        count = 0
        for item in open_failures:
            task_no = str(item["任务编号"])
            partition_no = int(item["分区"])
            existing = next(
                (
                    row for row in rows
                    if row.get("来源模块") == MODULE
                    and str(row.get("来源任务")) == task_no
                    and int(row.get("来源分区")) == partition_no
                ),
                None,
            )
            if existing is not None:
                existing["status"] = "待校验"
                existing["pending"] = True
                existing["abnormal"] = True
                existing["异常状态"] = "待处理"
                existing["关联失败记录"] = item["失败记录编号"]
            else:
                rows.append({
                    "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                    "status": "待校验",
                    "pending": True,
                    "abnormal": True,
                    "指标编号": f"QUAL-BATCH-{task_no}-{partition_no:04d}",
                    "监控对象": f"{task_no}/part={partition_no:04d}",
                    "指标类型": "批处理失败异常",
                    "阈值范围": "失败分区数=0",
                    "检测周期": "每批次",
                    "告警级别": "高",
                    "通知渠道": "数据工程值班群",
                    "恢复策略": "失败分区隔离重放",
                    "来源模块": MODULE,
                    "来源任务": task_no,
                    "来源分区": partition_no,
                    "关联失败记录": item["失败记录编号"],
                    "异常状态": "待处理",
                })
            count += 1
        return count

    def _sync_silver_queue(self, open_failures: list[dict[str, Any]]) -> int:
        """依赖上游（清洗层）按失败清单进入/退出待处理队列。"""
        rows = store.rows(SILVER_MODULE)
        # 依赖上游 -> 关联任务名 / 失败分区号，直接由失败清单聚合。
        upstream_tasks: dict[str, set[str]] = {}
        upstream_parts: dict[str, set[int]] = {}
        for item in open_failures:
            upstream = str(item.get("依赖上游"))
            upstream_tasks.setdefault(upstream, set()).add(str(item["任务编号"]))
            upstream_parts.setdefault(upstream, set()).add(int(item["分区"]))
        count = 0
        for row in rows:
            table_no = str(row.get("表编号"))
            if table_no not in upstream_tasks:
                # 之前由失败清单挂起、现在失败已全部解决的上游恢复启用。
                if row.get("关联批任务"):
                    row["status"] = "已启用"
                    row["pending"] = False
                    row["abnormal"] = False
                    row["待处理原因"] = ""
                continue
            row["status"] = "待校验"
            row["pending"] = True
            row["abnormal"] = True
            row["关联批任务"] = "、".join(sorted(upstream_tasks[table_no]))
            failed_parts = sorted(upstream_parts[table_no])
            row["待处理原因"] = (
                f"下游批处理任务分区 {failed_parts} 失败，等待清洗修复后触发隔离重放"
            )
            count += 1
        return count
