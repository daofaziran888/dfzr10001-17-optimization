"""验收脚本：覆盖跨轮次续做、断点 CAS、游标/日志一致性与验收序列 Q。"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.checkpoint import RUNNING, checkpoint_store
from app.services.collector import collector_service
from app.services.task import TaskService
from app.store import store

task_service = TaskService()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not cond:
        failures.append(name)


def reset_state() -> None:
    """重新装载种子数据并清空断点，保证每个用例独立。"""
    from app import seed
    store._tables = {name: [dict(r) for r in rows] for name, rows in seed.SEED_ROWS.items()}
    checkpoint_store._rows.clear()


# ---------------------------------------------------------------------- #
# 验收序列 Q：TASK-0002 窗口剩余 20 秒时完成 100 条批次，下一轮断点续做
# ---------------------------------------------------------------------- #
reset_state()
task2 = next(r for r in store.rows("task") if r["任务编号"] == "TASK-0002")
# 窗口 60s、批次 10s、安全余量 20s → 应跑 4 个批次（40s），第 5 批开启前剩余 20s 被拦
r1, msg1 = collector_service.run_round(2, safety_margin=20)
check("Q-轮次1批次数量=4", r1["batch_count"] == 4, str(r1["batch_count"]))
check("Q-轮次1窗口截断", r1["stopped_by_window"] is True)
check("Q-轮次1未完成", r1["finished"] is False)
check("Q-每批100条", all(b["rows"] == 100 for b in r1["batches"]))
check("Q-轮次1断点水位=2400", r1["resume_watermark"] == 2400, str(r1["resume_watermark"]))
check("Q-轮次1采集400条", r1["round_fetched"] == 400)

ckpt = checkpoint_store.latest("TASK-0002")
check("Q-断点挂起待恢复", ckpt["state"] == "待恢复", ckpt["state"])

# 游标与断点口径一致
cursor = next(r for r in store.rows("increment") if r["来源标识"] == "SRC-商品库")
check("Q-游标当前水位=断点水位", int(cursor["当前水位"]) == ckpt["watermark"] == 2400)
check("Q-游标上次水位=2300", int(cursor["上次水位"]) == 2300)

# 最近一条日志与断点对应同一源端、同一批次
logs2 = [r for r in store.rows("log") if r["关联任务"] == "TASK-0002"]
last_log = logs2[-1]
check("Q-日志源端一致", last_log["关联源端"] == "SRC-商品库" == ckpt["source"])
check("Q-日志结束水位=断点水位", int(last_log["结束水位"]) == ckpt["watermark"])
check("Q-日志批次序号=4", last_log["批次序号"] == 4)
check("Q-日志轮次=R1", last_log["轮次"] == "R1")
check("Q-日志行数100", int(last_log["采集行数"]) == 100)

# 历史日志保持当时批次结果（种子里的 HIST-1 行未被改写）
hist = [r for r in logs2 if r["轮次"] == "HIST-1"]
check("Q-历史日志保留", len(hist) == 1 and int(hist[0]["采集行数"]) == 100)

# 并发重复恢复：第一轮持有任务锁期间，第二个恢复请求必须被拒绝；
# 另对断点 CAS 原语做直接验证，保证同一断点的领取只生效一次。
results: dict[str, object] = {}
first_claimed = threading.Event()
proceed = threading.Event()
original_claim = checkpoint_store.claim


def gated_claim(task_no: str, token: str):
    row = original_claim(task_no, token)
    if row is not None and token == "token-A":
        first_claimed.set()
        proceed.wait(timeout=5)
    return row


checkpoint_store.claim = gated_claim  # type: ignore[method-assign]


def recover(token: str) -> None:
    if token == "token-B":
        first_claimed.wait(timeout=5)  # 等第一轮完成领取、仍持锁时再发起
    res, _ = collector_service.run_round(2, request_token=token)
    results[token] = res


t_a = threading.Thread(target=recover, args=("token-A",))
t_b = threading.Thread(target=recover, args=("token-B",))
t_a.start(); t_b.start()
# 等 B 已经撞上锁，再放行 A 的批次循环
first_claimed.wait(timeout=5)
proceed.set()
t_a.join(); t_b.join()
checkpoint_store.claim = original_claim  # type: ignore[method-assign]
check("Q-并发恢复仅一次生效", results["token-A"] is not None and results["token-B"] is None,
      str({k: (v is not None) for k, v in results.items()}))

# CAS 原语：同一待恢复断点，两个令牌并发领取，仅一个成功
reset_state()
collector_service.run_round(2, safety_margin=20)
claim_a = checkpoint_store.claim("TASK-0002", "cas-A")
claim_b = checkpoint_store.claim("TASK-0002", "cas-B")
check("Q-CAS首领成功", claim_a is not None and claim_a["state"] == RUNNING and claim_a["claim_token"] == "cas-A")
check("Q-CAS重复领取失败", claim_b is None)
# 把断点重新挂起后，新一轮可再次领取（跨轮次续做不受影响）
checkpoint_store.mark_pending("TASK-0002")
claim_c = checkpoint_store.claim("TASK-0002", "cas-C")
check("Q-挂起后可再领取", claim_c is not None and claim_c["claim_token"] == "cas-C")
# 重新准备一个干净的挂起断点供后续轮次断言
checkpoint_store.mark_pending("TASK-0002")
r2, msg2 = collector_service.run_round(2)

# 第二轮：从 2400 续做，而不是回到起始水位 2000
winner = r2
check("Q-轮次2从断点续做", winner["resume_watermark"] > 2400 and winner["batches"][0]["start_watermark"] == 2400)
check("Q-轮次2编号=R2", winner["round_no"] == "R2")
check("Q-轮次2批次=4", winner["batch_count"] == 4)
check("Q-轮次2断点水位=2800", winner["resume_watermark"] == 2800)
new_logs = [r for r in store.rows("log") if r["关联任务"] == "TASK-0002" and r["轮次"] == "R2"]
check("Q-轮次2日志4条且首批起水位2400",
      len(new_logs) == 4 and int(new_logs[0]["起始水位"]) == 2400 and int(new_logs[-1]["结束水位"]) == 2800)

# 工作台出现续做条目
workbench = checkpoint_store.resume_entries()
check("Q-工作台含TASK-0002续做", any(e["task_no"] == "TASK-0002" for e in workbench))

# 清单与详情进度一致
listed, _ = task_service.list_entries()
row2 = next(r for r in listed if r["任务编号"] == "TASK-0002")
detail2 = task_service.get_entry(2)
check("Q-清单详情断点水位一致",
      row2["progress"]["checkpoint"]["watermark"] == detail2["progress"]["checkpoint"]["watermark"] == 2800)
check("Q-详情最近日志=断点批次",
      detail2["progress"]["latest_log"]["结束水位"] == str(detail2["progress"]["checkpoint"]["watermark"]))

# ---------------------------------------------------------------------- #
# TASK-0003：200 条批次 + 120s 旧执行窗口不受影响
# ---------------------------------------------------------------------- #
reset_state()
r3, _ = collector_service.run_round(3)  # 默认窗口 120s、余量 20s → 10 批
check("T3-批次大小200", all(b["rows"] == 200 for b in r3["batches"]))
check("T3-默认窗口批次=10", r3["batch_count"] == 10, str(r3["batch_count"]))
check("T3-窗口消耗100s", r3["consumed_seconds"] == 100)
check("T3-断点水位=2500", r3["resume_watermark"] == 2500)
logs3 = [r for r in store.rows("log") if r["关联任务"] == "TASK-0003" and r["轮次"] == "R1"]
check("T3-每批日志200行", all(int(r["采集行数"]) == 200 for r in logs3) and len(logs3) == 10)

# TASK-0001 走完整追平：源端 5000 条，起始 1000，窗口足够大时一轮追平
reset_state()
r1_full, _ = collector_service.run_round(1, window_seconds=100000, safety_margin=0)
check("T1-追平完成", r1_full["finished"] is True)
check("T1-最终水位5000", r1_full["resume_watermark"] == 5000)
check("T1-断点已完成", checkpoint_store.latest("TASK-0001")["state"] == "已完成")
check("T1-任务状态已完成", next(r for r in store.rows("task") if r["任务编号"] == "TASK-0001")["status"] == "已完成")
c1 = next(r for r in store.rows("increment") if r["来源标识"] == "SRC-订单中心")
check("T1-游标标记已对齐", c1["是否完成"] == "是" and c1["status"] == "已对齐")
# 完成后再触发一轮：无数据可取，直接返回完成而非重跑
r1_again, _ = collector_service.run_round(1, window_seconds=100000, safety_margin=0)
check("T1-完成后不重跑", r1_again["batch_count"] == 0 and r1_again["finished"] is True)

# ---------------------------------------------------------------------- #
# 调度计划同步
# ---------------------------------------------------------------------- #
reset_state()
plan, msg = task_service.sync_schedule(2)
check("SCH-同步cron", plan["调度表达式"] == "*/10 * * * *", plan["调度表达式"])
check("SCH-同步超时阈值=执行窗口", plan["超时阈值"] == "60")
missing = task_service.sync_schedule(999)
check("SCH-不存在任务报错", missing[0] is None)

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    raise SystemExit(1)
print("全部验收通过")
