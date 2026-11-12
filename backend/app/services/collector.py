"""采集引擎：按批次模拟取数，并把同一份进度同时落到断点、游标与日志。

设计要点（对应跨轮次续做需求）：
- 源端是确定性的模拟数据：每个源端有固定总量，记录按增量字段水位编号，
  取数即按 ``水位 > 起点`` 顺序拉取，保证每轮结果可复现；
- 一个批次要么完整跑完、要么不开始；窗口剩余时间不足以再跑一个批次时，
  只收尾当前批次并写断点，绝不在窗口结束后继续拉取；
- 每批次结束后「断点 → 游标 → 日志」三处用同一个源端、同一对起止水位回写，
  任务详情里的上次进度与日志行因此天然对应同一批次；
- 下一轮从断点水位续做，而不是回到任务的起始水位（旧水位重跑问题由此消除）；
- 断点领取走 CAS（见 :mod:`app.checkpoint`），并发重复恢复只生效一次。
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from app.checkpoint import DONE, PENDING, RUNNING, checkpoint_store
from app.store import store

TASK_MODULE = "task"
INCREMENT_MODULE = "increment"
LOG_MODULE = "log"

# 模拟参数：一个批次固定耗时 10 个窗口秒；剩余窗口 <= 安全余量（默认 20 秒）
# 时，跑完当前批次即挂起，不再开启下一批。
BATCH_COST_SECONDS = 10
# 每个批次模拟真实源端 I/O 的实际挂钟耗时（秒），让并发恢复能在锁上真实相撞；
# 设为 0 可关闭等待，窗口口径仍按 BATCH_COST_SECONDS 确定性计算。
BATCH_IO_SECONDS = 0.02
DEFAULT_SAFETY_MARGIN = 20

# 每个源端的模拟数据总量（按增量字段水位编号的记录条数上界）。
# 上界高于各任务起始水位，保证跨多轮续做仍有数据可取。
SOURCE_VOLUME = {
    "SRC-订单中心": 5000,
    "SRC-商品库": 5000,
    "SRC-会员中心": 5000,
}
DEFAULT_VOLUME = 5000


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


class CollectorService:
    # ------------------------------------------------------------------ #
    # 模拟源端
    # ------------------------------------------------------------------ #
    def source_volume(self, source: str) -> int:
        return SOURCE_VOLUME.get(source, DEFAULT_VOLUME)

    def fetch_batch(self, source: str, field: str, watermark: int, size: int) -> list[dict[str, Any]]:
        """模拟一次源端拉取：返回水位严格大于 ``watermark`` 的最多 ``size`` 条记录。"""
        ceiling = self.source_volume(source)
        records: list[dict[str, Any]] = []
        cursor = watermark
        for _ in range(size):
            if cursor >= ceiling:
                break
            cursor += 1
            records.append({field: cursor, "source": source})
        return records

    # ------------------------------------------------------------------ #
    # 任务参数
    # ------------------------------------------------------------------ #
    def _task_config(self, task: dict[str, Any]) -> dict[str, Any]:
        return {
            "task_no": str(task["任务编号"]),
            "source": str(task["关联源端"]),
            "field": str(task["增量字段"]),
            "start_watermark": _to_int(task.get("起始水位"), 0),
            "batch_size": max(_to_int(task.get("批次大小"), 100), 1),
            "window_seconds": max(_to_int(task.get("执行窗口"), 60), BATCH_COST_SECONDS),
        }

    # ------------------------------------------------------------------ #
    # 游标与日志回写（三处进度同源）
    # ------------------------------------------------------------------ #
    def _upsert_cursor(
        self,
        *,
        source: str,
        field: str,
        previous: int,
        current: int,
        fetched: int,
        finished: bool,
    ) -> dict[str, Any]:
        """按源端维护唯一游标；上次水位/当前水位与本批次起止水位一致。"""
        rows = store.rows(INCREMENT_MODULE)
        cursor = next((row for row in rows if row.get("来源标识") == source), None)
        if cursor is None:
            cursor = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            rows.append(cursor)
            cursor["游标编号"] = f"INCR-{len(rows):04d}"
            cursor["来源标识"] = source
        cursor["增量字段"] = field
        cursor["上次水位"] = str(previous)
        cursor["当前水位"] = str(current)
        cursor["增量条数"] = str(_to_int(cursor.get("增量条数"), 0) + fetched)
        cursor["采集时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor["是否完成"] = "是" if finished else "否"
        cursor["status"] = "已对齐" if finished else "采集中"
        cursor["pending"] = not finished
        cursor["abnormal"] = False
        return cursor

    def _append_log(
        self,
        *,
        task: dict[str, Any],
        source: str,
        round_no: str,
        seq: int,
        begin: datetime,
        end: datetime,
        start_watermark: int,
        end_watermark: int,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """追加一条批次日志。历史日志只追加、不改写，保留当时批次结果。"""
        rows = store.rows(LOG_MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "成功",
            "pending": False,
            "abnormal": False,
            "日志编号": f"LOG-{max((int(r.get('id', 0)) for r in rows), default=0) + 1:04d}",
            "关联任务": task["任务编号"],
            "关联源端": source,
            "轮次": round_no,
            "批次序号": seq,
            "开始时间": begin.strftime("%Y-%m-%d %H:%M:%S"),
            "结束时间": end.strftime("%Y-%m-%d %H:%M:%S"),
            "起始水位": str(start_watermark),
            "结束水位": str(end_watermark),
            "采集行数": str(len(records)),
            "成功条数": str(len(records)),
            "失败条数": "0",
            "错误信息": "",
        }
        rows.append(entry)
        return entry

    # ------------------------------------------------------------------ #
    # 一轮采集
    # ------------------------------------------------------------------ #
    def run_round(
        self,
        entry_id: int,
        *,
        window_seconds: int | None = None,
        safety_margin: int = DEFAULT_SAFETY_MARGIN,
        request_token: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """执行一轮窗口内采集，返回（轮次结果, 说明）。

        - 已有待恢复断点时，必须凭本轮令牌领取（CAS），领不到说明被并发恢复抢走；
        - 同任务互斥，防止两轮同时写同一断点；
        - 窗口剩余时间 <= 安全余量时只收尾当前批次并落断点，下一轮续做。
        """
        task = store.find(TASK_MODULE, entry_id)
        if task is None:
            return None, f"采集任务 {entry_id} 不存在或已归档"

        cfg = self._task_config(task)
        task_no = cfg["task_no"]
        window = int(window_seconds) if window_seconds and window_seconds > 0 else cfg["window_seconds"]
        token = request_token or uuid.uuid4().hex

        lock = checkpoint_store.run_lock(task_no)
        if not lock.acquire(blocking=False):
            return None, f"任务 {task_no} 已有一轮采集在执行，本轮请求被跳过"

        try:
            open_ckpt = checkpoint_store.open_one(task_no)
            if open_ckpt is not None:
                # 断点只允许被领取一次：并发的重复恢复在这里全部失败。
                claimed = checkpoint_store.claim(task_no, token)
                if claimed is None:
                    return None, f"任务 {task_no} 的断点已被另一轮恢复领取，重复恢复不生效"
                watermark = int(claimed["watermark"])
                round_index = _to_int(claimed.get("round_no", "R0").replace("R", ""), 0) + 1
            else:
                last_ckpt = checkpoint_store.latest(task_no)
                if last_ckpt is not None and last_ckpt.get("state") == DONE:
                    # 任务已追平：不允许再从旧起始水位重跑，直接返回空轮次。
                    return {
                        "task_no": task_no,
                        "round_no": last_ckpt.get("round_no", "R0"),
                        "source": cfg["source"],
                        "window_seconds": window,
                        "consumed_seconds": 0,
                        "safety_margin": safety_margin,
                        "resume_watermark": int(last_ckpt["watermark"]),
                        "round_fetched": 0,
                        "batch_count": 0,
                        "finished": True,
                        "stopped_by_window": False,
                        "batches": [],
                    }, "任务已追平，无新增数据，未重复采集"
                watermark = cfg["start_watermark"]
                round_index = 1

            round_no = f"R{round_index}"
            batch_size = cfg["batch_size"]
            source = cfg["source"]
            field = cfg["field"]
            ceiling = self.source_volume(source)

            task["status"] = "执行中"
            task["pending"] = True

            consumed = 0
            seq = 0
            round_fetched = 0
            batch_summaries: list[dict[str, Any]] = []
            clock = datetime.now()
            finished = False
            stopped_by_window = False

            while consumed < window:
                remaining = window - consumed
                # 窗口剩余时间不足以再安全跑完一个批次：只收尾，不开新批次。
                if remaining <= safety_margin:
                    stopped_by_window = True
                    break

                begin = clock
                records = self.fetch_batch(source, field, watermark, batch_size)
                if BATCH_IO_SECONDS > 0:
                    time.sleep(BATCH_IO_SECONDS)
                clock = begin + timedelta(seconds=BATCH_COST_SECONDS)
                consumed += BATCH_COST_SECONDS

                if not records:
                    finished = True
                    break

                previous = watermark
                watermark = int(records[-1][field])
                seq += 1
                round_fetched += len(records)

                # 同一批次的进度一次性落到日志、游标、断点三处，口径完全一致。
                log_row = self._append_log(
                    task=task,
                    source=source,
                    round_no=round_no,
                    seq=seq,
                    begin=begin,
                    end=clock,
                    start_watermark=previous,
                    end_watermark=watermark,
                    records=records,
                )
                self._upsert_cursor(
                    source=source,
                    field=field,
                    previous=previous,
                    current=watermark,
                    fetched=len(records),
                    finished=False,
                )
                checkpoint_store.save(
                    task_no=task_no,
                    source=source,
                    increment_field=field,
                    watermark=watermark,
                    start_watermark=cfg["start_watermark"],
                    batch_size=batch_size,
                    round_no=round_no,
                    state=RUNNING,
                    fetched=round_fetched,
                    window_seconds=window,
                    consumed_seconds=consumed,
                )
                batch_summaries.append({
                    "seq": seq,
                    "log_id": log_row["日志编号"],
                    "start_watermark": previous,
                    "end_watermark": watermark,
                    "rows": len(records),
                })

                if watermark >= ceiling:
                    finished = True
                    break

            if finished:
                checkpoint_store.mark_done(task_no)
                self._upsert_cursor(
                    source=source,
                    field=field,
                    previous=watermark,
                    current=watermark,
                    fetched=0,
                    finished=True,
                )
                task["status"] = "已完成"
                task["pending"] = False
            else:
                # 窗口结束：挂起断点，下一轮从该水位续做。
                checkpoint_store.mark_pending(task_no)
                task["status"] = "待执行"
                task["pending"] = True

            result = {
                "task_no": task_no,
                "round_no": round_no,
                "source": source,
                "window_seconds": window,
                "consumed_seconds": consumed,
                "safety_margin": safety_margin,
                "resume_watermark": watermark,
                "round_fetched": round_fetched,
                "batch_count": seq,
                "finished": finished,
                "stopped_by_window": stopped_by_window,
                "batches": batch_summaries,
            }
            return result, "采集窗口结束，断点已保存，下一轮从断点续做" if not finished else "源端已追平，任务完成"
        finally:
            lock.release()

    # ------------------------------------------------------------------ #
    # 任务详情：清单与详情共用同一份进度口径
    # ------------------------------------------------------------------ #
    def progress_snapshot(self, task: dict[str, Any]) -> dict[str, Any]:
        """汇总断点、游标、最近批次日志，供清单与详情展示同一份进度。"""
        task_no = str(task["任务编号"])
        source = str(task["关联源端"])
        ckpt = checkpoint_store.latest(task_no)
        cursor = next(
            (row for row in store.rows(INCREMENT_MODULE) if row.get("来源标识") == source),
            None,
        )
        logs = [row for row in store.rows(LOG_MODULE) if row.get("关联任务") == task_no]
        latest_log = logs[-1] if logs else None
        return {
            "task_no": task_no,
            "source": source,
            "checkpoint": dict(ckpt) if ckpt else None,
            "cursor": dict(cursor) if cursor else None,
            "latest_log": dict(latest_log) if latest_log else None,
            "log_count": len(logs),
        }


collector_service = CollectorService()
