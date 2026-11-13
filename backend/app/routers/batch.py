"""批量转换接口：维护批处理任务，覆盖提交调度、启动任务、终止任务、归档结果，
以及失败隔离重放、失败清单与重放实例查询。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.batch import BatchError, BatchService

router = APIRouter(prefix="/api/batch", tags=["批量转换"])

service = BatchService()

LIST_FIELDS = ["任务编号", "输入路径", "处理脚本", "输出路径", "分区覆盖", "依赖上游", "重试策略", "超时终止"]
STATUSES = ["草稿", "待调度", "运行中", "已完成", "已失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、待调度、运行中、已完成、已失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤批量转换列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/failures/current")
def current_failures(
    script: str | None = Query(default=None, description="按处理脚本定位报错来源"),
    upstream: str | None = Query(default=None, description="按依赖上游定位不生效原因"),
) -> dict[str, Any]:
    """当前失败清单（每个失败分区仅保留最早待处理记录），驱动待处理台账。"""
    items = service.current_failures()
    if script:
        items = [row for row in items if script in str(row.get("处理脚本", ""))]
    if upstream:
        items = [row for row in items if row.get("依赖上游") == upstream]
    return {"module": "batch", "total": len(items), "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条批处理任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"批处理任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条批处理任务，缺字段时说明原因而不是静默丢弃。"""
    try:
        entry, missing = service.create_entry(payload.values)
    except BatchError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="批处理任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条批处理任务执行提交调度、启动任务、终止任务、归档结果；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}/replay-scope")
def replay_scope(entry_id: int) -> dict[str, Any]:
    """按输入路径、分区覆盖和依赖上游识别可重放范围：只从最早失败分区继续。"""
    scope = service.replay_scope(entry_id)
    if scope is None:
        raise HTTPException(status_code=404, detail=f"批处理任务 {entry_id} 不存在或已归档")
    return scope


@router.post("/{entry_id}/replay")
def replay(entry_id: int, payload: EntryPayload | None = None) -> dict[str, Any]:
    """失败隔离重放：只处理失败分区及后续未完成分区，已成功分区输出不变。

    同一任务并发重放只创建一个执行实例，重复提交合并到运行中实例，不重复写输出。
    非法分区范围返回 400 并拒绝执行。
    """
    values = payload.values if payload is not None else {}
    start_raw = values.get("start_partition")
    end_raw = values.get("end_partition")
    fail_raw = values.get("fail_partitions")
    try:
        start_partition = int(start_raw) if start_raw is not None else None
        end_partition = int(end_raw) if end_raw is not None else None
        fail_partitions: list[int] | None = None
        if fail_raw is not None:
            if isinstance(fail_raw, str):
                fail_partitions = [int(part.strip()) for part in fail_raw.split(",") if part.strip()]
            elif isinstance(fail_raw, (list, tuple)):
                fail_partitions = [int(part) for part in fail_raw]
            else:
                raise BatchError("fail_partitions 仅支持分区号数组或逗号分隔字符串")
        instance, message, status_code = service.replay(
            entry_id,
            start_partition=start_partition,
            end_partition=end_partition,
            fail_partitions=fail_partitions,
            reason=str(values.get("reason") or "") or None,
        )
    except BatchError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"非法分区范围：{exc}") from exc
    if instance is None:
        raise HTTPException(status_code=status_code, detail=message)
    return {"ok": True, "message": message, "instance": instance, "task": service.get_entry(entry_id)}


@router.get("/{entry_id}/failures")
def failure_history(
    entry_id: int,
    script: str | None = Query(default=None, description="按处理脚本定位报错"),
    upstream: str | None = Query(default=None, description="按依赖上游定位不生效"),
    status_filter: str | None = Query(default=None, alias="status", description="待处理/已解决"),
) -> dict[str, Any]:
    """失败历史清单：历史每次失败的时间与分区都保留，可按处理脚本、依赖上游筛选。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"批处理任务 {entry_id} 不存在或已归档")
    items = service.failure_history(
        entry_id, script=script, upstream=upstream, status_filter=status_filter
    )
    return {"module": "batch", "task_id": entry_id, "total": len(items), "items": items}


@router.get("/{entry_id}/replay-instances")
def replay_instances(entry_id: int) -> dict[str, Any]:
    """重放实例台账：用于核对多处重放结果、确认并发提交只生成一个实例。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"批处理任务 {entry_id} 不存在或已归档")
    items = service.replay_instances(entry_id)
    return {"module": "batch", "task_id": entry_id, "total": len(items), "items": items}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出批量转换清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "batch", "total": total, "items": items}
