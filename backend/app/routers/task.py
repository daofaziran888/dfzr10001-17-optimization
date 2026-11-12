"""采集任务接口：维护采集任务，覆盖提交执行、暂停/恢复、窗口采集与断点续做。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.checkpoint import checkpoint_store
from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.collector import DEFAULT_SAFETY_MARGIN, collector_service
from app.services.task import TaskService

router = APIRouter(prefix="/api/task", tags=["采集任务"])

service = TaskService()

LIST_FIELDS = ["任务编号", "任务名称", "关联源端", "采集频率", "增量字段", "起始水位", "批次大小", "执行窗口"]
STATUSES = ["草稿", "待执行", "执行中", "已完成"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、待执行、执行中、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤采集任务列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/workbench/resume")
def resume_workbench() -> dict[str, Any]:
    """工作台续做条目：所有窗口结束、断点挂起等待下一轮续做的任务。"""
    entries = checkpoint_store.resume_entries()
    return {"total": len(entries), "items": entries}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集任务清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "task", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条采集任务明细（含断点、游标、批次日志与调度计划）；不存在时报错。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采集任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条采集任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="采集任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条采集任务执行提交执行、暂停任务、恢复任务；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/collect", response_model=ActionResult)
def collect_round(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """跑一个执行窗口的采集：按批次取数，临近窗口结束只收尾当前批次并落断点。

    可选参数（values）：
    - ``window_seconds``：本轮执行窗口秒数，缺省用任务配置的执行窗口；
    - ``safety_margin``：窗口剩余秒数 <= 该值即停止开新批次，默认 20；
    - ``token``：恢复令牌，并发重复恢复时只有一个令牌能领到断点。
    """
    values = payload.values if payload is not None else {}
    window_seconds = values.get("window_seconds")
    safety_margin = int(values.get("safety_margin") or DEFAULT_SAFETY_MARGIN)
    token = values.get("token")
    result, message = collector_service.run_round(
        entry_id,
        window_seconds=int(window_seconds) if window_seconds else None,
        safety_margin=safety_margin,
        request_token=str(token) if token else None,
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.post("/{entry_id}/resume", response_model=ActionResult)
def resume_round(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """下一轮从断点续做：语义与窗口采集一致，区别仅在入口面向工作台续做条目。"""
    values = payload.values if payload is not None else {}
    token = values.get("token")
    result, message = collector_service.run_round(
        entry_id,
        safety_margin=int(values.get("safety_margin") or DEFAULT_SAFETY_MARGIN),
        request_token=str(token) if token else None,
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.post("/{entry_id}/sync-schedule", response_model=ActionResult)
def sync_schedule(entry_id: int) -> ActionResult:
    """任务页把采集频率/执行窗口同步到调度计划，工作台随之出现续做条目。"""
    plan, message = service.sync_schedule(entry_id)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)
