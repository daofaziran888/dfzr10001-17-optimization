"""采集任务业务规则：状态流转、字段校验、进度快照与调度计划同步。"""
from __future__ import annotations

from typing import Any

from app.checkpoint import checkpoint_store
from app.services.collector import collector_service
from app.store import store

MODULE = "task"
SCHEDULE_MODULE = "schedule"
REQUIRED_FIELDS = ["任务编号", "任务名称", "关联源端"]
STATUS_ORDER = ["草稿", "待执行", "执行中", "已完成"]
ACTION_RULES = {"提交执行": "待执行", "暂停任务": "执行中", "恢复任务": "已完成"}
NEGATIVE_ACTIONS = []


class TaskService:
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
        page_rows = rows[start:start + size]
        # 清单与详情共用同一进度口径：直接复用详情快照，避免两处各算各的。
        return [self._with_progress(row) for row in page_rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        detail = self._with_progress(entry)
        detail["schedule"] = self.linked_schedule(str(entry["任务编号"]))
        detail["logs"] = [
            dict(row)
            for row in store.rows("log")
            if row.get("关联任务") == entry["任务编号"]
        ]
        return detail

    def _with_progress(self, row: dict[str, Any]) -> dict[str, Any]:
        """清单行/详情共用：上次进度取最近批次日志，断点取断点表，游标取游标表。"""
        merged = dict(row)
        merged["progress"] = collector_service.progress_snapshot(row)
        ckpt = checkpoint_store.latest(str(row.get("任务编号")))
        merged["has_resume"] = bool(ckpt and ckpt.get("state") == "待恢复")
        return merged

    def linked_schedule(self, task_no: str) -> dict[str, Any] | None:
        for row in store.rows(SCHEDULE_MODULE):
            if str(row.get("关联任务")) == task_no:
                return dict(row)
        return None

    def sync_schedule(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """把任务页的采集参数同步到同编号调度计划，任务参数是调度口径的来源。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集任务 {entry_id} 不存在或已归档"
        task_no = str(entry["任务编号"])
        plan = self.linked_schedule(task_no)
        if plan is None:
            return None, f"任务 {task_no} 尚未关联调度计划，无法同步"
        # 找到原行再更新，保证列表里读到的是同一条记录。
        for row in store.rows(SCHEDULE_MODULE):
            if str(row.get("计划编号")) == str(plan["计划编号"]):
                row["调度表达式"] = self._frequency_to_cron(str(entry.get("采集频率", "")))
                row["超时阈值"] = str(entry.get("执行窗口", ""))
                row["是否启用"] = "是" if entry.get("status") != "草稿" else "否"
                row["status"] = "已启用" if row["是否启用"] == "是" else "草稿"
                row["pending"] = row["status"] != "已启用"
                plan = dict(row)
                break
        return plan, f"调度计划 {plan['计划编号']} 已按任务 {task_no} 同步"

    @staticmethod
    def _frequency_to_cron(frequency: str) -> str:
        """把“每N分钟”这类采集频率换算成 cron 表达式，识别不了就原样保留。"""
        table = {"每5分钟": "*/5 * * * *", "每10分钟": "*/10 * * * *", "每30分钟": "*/30 * * * *"}
        return table.get(frequency, frequency)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集任务可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集任务已{action}"
