"""HTTP 端到端验收：走真实 FastAPI 路由验证采集、续做、同步与工作台接口。"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = "http://127.0.0.1:8011"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        fails.append(name)


# 验收序列 Q（HTTP 版）
st, r = call("POST", "/api/task/2/collect", {"values": {"safety_margin": 20}})
check("HTTP-采集返回ok", st == 200 and r["ok"] is True, r.get("message", ""))
entry = r["entry"]
check("HTTP-Q批次=4", entry["batch_count"] == 4, str(entry["batch_count"]))
check("HTTP-Q断点水位2400", entry["resume_watermark"] == 2400)

st, r = call("GET", "/api/task/workbench/resume")
check("HTTP-工作台续做条目", st == 200 and r["total"] >= 1 and any(i["task_no"] == "TASK-0002" for i in r["items"]))

# 并发重复恢复：两个 token 同时打 resume，只允许一个 ok
import concurrent.futures as cf

with cf.ThreadPoolExecutor(max_workers=2) as pool:
    futs = [pool.submit(call, "POST", "/api/task/2/resume", {"values": {"token": t}}) for t in ("http-A", "http-B")]
    outcomes = [f.result() for f in futs]
oks = [body["ok"] for _, body in outcomes]
check("HTTP-并发恢复一次生效", oks.count(True) == 1, str(oks))
winner = next(body["entry"] for _, body in outcomes if body["ok"])
check("HTTP-续做从2400起", winner["batches"][0]["start_watermark"] == 2400 and winner["resume_watermark"] == 2800)
loser_msg = next(body["message"] for _, body in outcomes if not body["ok"])
check("HTTP-重复恢复有可读提示", "跳过" in loser_msg or "领取" in loser_msg, loser_msg)

# 任务详情：上次进度与日志行同一源端同一批次
st, detail = call("GET", "/api/task/2")
prog = detail["progress"]
check("HTTP-详情含进度快照", st == 200 and prog["checkpoint"] and prog["cursor"] and prog["latest_log"])
check("HTTP-三处水位一致",
      prog["checkpoint"]["watermark"] == int(prog["cursor"]["当前水位"]) == int(prog["latest_log"]["结束水位"]) == 2800)
check("HTTP-源端一致", prog["source"] == prog["latest_log"]["关联源端"] == "SRC-商品库")
check("HTTP-详情含调度计划", detail["schedule"]["计划编号"] == "SCHE-0002")
check("HTTP-详情含批次日志", len(detail["logs"]) >= 9)

# 清单与详情口径一致
st, listing = call("GET", "/api/task?keyword=TASK-0002")
listed = listing["items"][0]
check("HTTP-清单详情一致", listed["progress"]["checkpoint"]["watermark"] == prog["checkpoint"]["watermark"])
check("HTTP-清单标记可续做", listed["has_resume"] is True)

# 调度同步
st, sync = call("POST", "/api/task/2/sync-schedule")
check("HTTP-同步调度成功", st == 200 and sync["ok"] and sync["entry"]["调度表达式"] == "*/10 * * * *")

# TASK-0003 不受影响
st, r3 = call("POST", "/api/task/3/collect", {"values": {}})
check("HTTP-T3批次200", r3["entry"]["batch_count"] == 10 and all(b["rows"] == 200 for b in r3["entry"]["batches"]))

# 游标页与日志页能读到回写结果
st, incr = call("GET", "/api/increment")
srcs = {row["来源标识"]: row for row in incr["items"]}
check("HTTP-游标页水位2800", int(srcs["SRC-商品库"]["当前水位"]) == 2800 and srcs["SRC-商品库"]["status"] == "采集中")
st, logs = call("GET", "/api/log")
r2_logs = [row for row in logs["items"] if row.get("关联任务") == "TASK-0002"]
check("HTTP-日志页含R2批次", any(row["轮次"] == "R2" for row in r2_logs))
hist = [row for row in r2_logs if row["轮次"] == "HIST-1"]
check("HTTP-历史日志保留", len(hist) == 1 and hist[0]["采集行数"] == "100")

# 错误路径
st, missing = call("POST", "/api/task/999/collect", {"values": {}})
check("HTTP-不存在任务被拒", st == 200 and missing["ok"] is False)

print()
print("全部 HTTP 验收通过" if not fails else f"失败：{fails}")
sys.exit(1 if fails else 0)
