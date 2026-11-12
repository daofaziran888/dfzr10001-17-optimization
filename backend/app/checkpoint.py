"""断点（checkpoint）存储与并发控制。

断点是「窗口结束、下一轮续做」的唯一事实来源：
- 采集引擎每个批次结束都把最新水位落到这里，游标表与日志表都从这里取数；
- ``claim`` 用状态比较并交换（CAS）保证同一个断点的恢复只生效一次，
  并发的重复恢复请求只有一个能把断点从“待恢复”改成“恢复中”；
- ``_run_locks`` 是进程内互斥锁，保证同一任务同一时刻只有一轮采集在跑，
  跨任务互不阻塞。

真实项目里这里会换成带乐观锁版本号的数据库表，接口保持不变。
"""
from __future__ import annotations

import threading
from typing import Any

# 断点生命周期：待恢复（窗口结束挂起）→ 恢复中（已被某轮领取）→ 已完成（源端追平）
PENDING = "待恢复"
RUNNING = "恢复中"
DONE = "已完成"


class CheckpointStore:
    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self._run_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---- 并发原语 -------------------------------------------------------
    def run_lock(self, task_no: str) -> threading.Lock:
        """返回某任务的专属互斥锁：同任务串行，跨任务并行。"""
        with self._locks_guard:
            lock = self._run_locks.get(task_no)
            if lock is None:
                lock = threading.Lock()
                self._run_locks[task_no] = lock
            return lock

    # ---- 断点读取 -------------------------------------------------------
    def rows(self) -> list[dict[str, Any]]:
        return self._rows

    def latest(self, task_no: str) -> dict[str, Any] | None:
        """取任务最新一条断点（按自增 id 倒序）。"""
        matched = [row for row in self._rows if row.get("task_no") == task_no]
        return matched[-1] if matched else None

    def open_one(self, task_no: str) -> dict[str, Any] | None:
        """取任务尚未完成的断点（待恢复或恢复中），已完成的不再参与续做。"""
        row = self.latest(task_no)
        if row and row.get("state") in (PENDING, RUNNING):
            return row
        return None

    # ---- 断点写入 -------------------------------------------------------
    def save(
        self,
        *,
        task_no: str,
        source: str,
        increment_field: str,
        watermark: int,
        start_watermark: int,
        batch_size: int,
        round_no: str,
        state: str,
        fetched: int,
        window_seconds: int,
        consumed_seconds: int,
    ) -> dict[str, Any]:
        """新建或覆盖当前未完成断点；任务追平时把断点标记为已完成。"""
        row = self.open_one(task_no)
        if row is None:
            row = {"id": len(self._rows) + 1}
            self._rows.append(row)
        row.update({
            "task_no": task_no,
            "source": source,
            "increment_field": increment_field,
            "watermark": int(watermark),
            "start_watermark": int(start_watermark),
            "batch_size": int(batch_size),
            "round_no": round_no,
            "state": state,
            "fetched": int(fetched),
            "window_seconds": int(window_seconds),
            "consumed_seconds": int(consumed_seconds),
            "claim_token": None,
        })
        return row

    def claim(self, task_no: str, token: str) -> dict[str, Any] | None:
        """原子领取断点（CAS）。

        只有当前状态为“待恢复”的断点能被领取，并一次性写入领取令牌；
        已经被别的轮次领走（恢复中）或已完成时返回 ``None``，
        从而保证并发重复恢复只生效一次。
        """
        row = self.open_one(task_no)
        if row is None or row.get("state") != PENDING:
            return None
        row["state"] = RUNNING
        row["claim_token"] = token
        return row

    def mark_pending(self, task_no: str) -> None:
        """窗口结束：把恢复中的断点挂起，等待下一轮领取。"""
        row = self.open_one(task_no)
        if row is not None and row.get("state") == RUNNING:
            row["state"] = PENDING
            row["claim_token"] = None

    def mark_done(self, task_no: str) -> None:
        row = self.open_one(task_no)
        if row is not None:
            row["state"] = DONE
            row["claim_token"] = None

    def resume_entries(self) -> list[dict[str, Any]]:
        """工作台续做条目：所有处于待恢复状态、跨窗口挂起的任务。"""
        return [dict(row) for row in self._rows if row.get("state") == PENDING]


checkpoint_store = CheckpointStore()
