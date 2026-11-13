"""失败隔离与重放能力的端到端核验脚本（真实 HTTP 服务 + 标准库 urllib）。

运行：backend/.venv/bin/python -I scripts/verify_replay_http.py
"""
from __future__ import annotations

import json
import queue
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(BACKEND))

import uvicorn  # noqa: E402

from app.main import app  # noqa: E402
from app.services.batch import BatchService  # noqa: E402

PASS = 0
FAIL = 0
BASE = "http://127.0.0.1:8011"


def check(name: str, condition: bool, detail: object = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} -> {detail}")


def call(method: str, path: str, body: object | None = None) -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def main() -> int:
    config = uvicorn.Config(app, host="127.0.0.1", port=8011, log_level="warning")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    for _ in range(100):
        try:
            call("GET", "/api/health")
            break
        except OSError:
            time.sleep(0.1)

    print("== 1. 种子任务 batch_orders_07 卡在第 13 个分区 ==")
    _, task = call("GET", "/api/batch/7")
    check("任务编号为 batch_orders_07", task["任务编号"] == "batch_orders_07", task.get("任务编号"))
    check("依赖上游为 clean_orders", task["依赖上游"] == "clean_orders")
    check("分区总数 16", task["分区总数"] == 16)
    check("成功分区 12", task["成功分区数"] == 12)
    check("失败分区 1", task["失败分区数"] == 1)
    check("未完成分区 3", task["未完成分区数"] == 3)
    check("任务状态已失败", task["status"] == "已失败", task["status"])
    parts = task["分区结果"]
    check("分区13状态失败", parts["13"]["状态"] == "失败")
    check("分区13无输出路径", parts["13"]["输出路径"] is None)
    check("分区14未完成", parts["14"]["状态"] == "未完成")
    check("分区12有输出路径", parts["12"]["输出路径"] == f"{task['输出路径']}/part=0012")
    before_paths = {no: parts[str(no)]["输出路径"] for no in range(1, 13)}

    print("== 2. 失败历史保留时间与分区 ==")
    _, payload = call("GET", "/api/batch/7/failures")
    history = payload["items"]
    check("初始失败历史 1 条", len(history) == 1, history)
    check("失败分区=13", history[0]["分区"] == 13)
    check("保留失败时间", bool(history[0]["失败时间"]))
    check("记录处理脚本", "etl" in history[0]["处理脚本"])
    check("记录依赖上游", history[0]["依赖上游"] == "clean_orders")

    print("== 3. 重放范围只从失败分区开始 ==")
    _, scope = call("GET", "/api/batch/7/replay-scope")
    check("最早失败分区=13", scope["最早失败分区"] == 13, scope)
    check("可重放分区=13..16", scope["可重放分区"] == [13, 14, 15, 16], scope["可重放分区"])
    check("已成功分区=1..12", scope["已成功分区"] == list(range(1, 13)))

    print("== 4. 非法分区范围拒绝 ==")
    code, _ = call("POST", "/api/batch/7/replay", {"values": {"start_partition": 1, "end_partition": 13}})
    check("覆盖已成功分区 -> 400", code == 400, code)
    code, _ = call("POST", "/api/batch/7/replay", {"values": {"start_partition": 15, "end_partition": 99}})
    check("超出分区覆盖 -> 400", code == 400, code)
    code, _ = call("POST", "/api/batch/7/replay", {"values": {"start_partition": 16, "end_partition": 13}})
    check("起点大于终点 -> 400", code == 400, code)
    _, task2 = call("GET", "/api/batch/7")
    check("拒绝后未产生实例", len(task2["重放实例"]) == 0, task2["重放实例"])

    print("== 5. 两个重放请求同时到达只创建一个实例 ==")
    hook_calls: queue.Queue = queue.Queue()
    gate: queue.Queue = queue.Queue()

    def hook(_entry: dict, _instance: dict, partition_no: int) -> None:
        hook_calls.put(partition_no)
        if partition_no == 13:
            gate.get(timeout=15)

    BatchService.partition_hook = hook
    responses: list[tuple[int, object]] = []
    barrier = threading.Barrier(2)

    def fire() -> None:
        # 两个线程在屏障处同时放行，模拟请求同时到达。
        barrier.wait()
        responses.append(call("POST", "/api/batch/7/replay", {"values": {}}))

    t1 = threading.Thread(target=fire)
    t2 = threading.Thread(target=fire)
    t1.start(); t2.start()
    check("首个重放进入分区处理", hook_calls.get(timeout=15) == 13)
    # 放行前确认只有一个实例在运行（另一请求已合并返回，不会进入分区处理）。
    _, running = call("GET", "/api/batch/7/replay-instances")
    check("挂起期间仅存在一个实例", running["total"] == 1, running)
    gate.put(None)
    t1.join(); t2.join()
    BatchService.partition_hook = None
    check("两个并发请求均返回 200",
          len(responses) == 2 and all(code == 200 for code, _ in responses), responses)
    _, task3 = call("GET", "/api/batch/7")
    check("只创建一个重放实例", len(task3["重放实例"]) == 1, task3["重放实例"])
    instance_ids = {body["instance"]["重放实例编号"] for _, body in responses}
    check("两个请求拿到同一实例编号",
          instance_ids == {task3["重放实例"][0]["重放实例编号"]}, instance_ids)
    merged = [body for _, body in responses if "合并" in body["message"]]
    check("其中一个请求被合并且未重复写输出", len(merged) == 1,
          [body["message"] for _, body in responses])

    print("== 6. 重放只处理 13-16，前 12 个输出路径不变 ==")
    check("重放后任务已完成", task3["status"] == "已完成", task3["status"])
    check("成功分区 16", task3["成功分区数"] == 16)
    check("失败分区 0", task3["失败分区数"] == 0)
    check("未完成分区 0", task3["未完成分区数"] == 0)
    after_paths = {no: task3["分区结果"][str(no)]["输出路径"] for no in range(1, 13)}
    check("前12个输出路径完全不变", after_paths == before_paths, after_paths)
    check("分区13输出路径按规则生成",
          task3["分区结果"]["13"]["输出路径"] == f"{task3['输出路径']}/part=0013")
    inst = task3["重放实例"][0]
    check("实例计划分区13-16", inst["计划分区"] == [13, 14, 15, 16], inst["计划分区"])
    check("实例成功分区13-16", inst["成功分区"] == [13, 14, 15, 16], inst["成功分区"])
    check("实例状态已完成", inst["状态"] == "已完成")

    print("== 7. 失败历史保留且已核销，失败清单驱动台账 ==")
    _, payload = call("GET", "/api/batch/7/failures")
    history2 = payload["items"]
    check("失败历史仍保留 1 条", len(history2) == 1)
    check("历史记录标记已解决", history2[0]["状态"] == "已解决", history2[0]["状态"])
    check("保留解决时间", bool(history2[0]["解决时间"]))
    _, payload = call("GET", "/api/batch/failures/current")
    check("当前失败清单已清空", payload["items"] == [], payload["items"])

    _, quality = call("GET", "/api/quality?size=200")
    q_rows = [q for q in quality["items"] if q.get("来源模块") == "batch"]
    check("质量异常台账存在批处理条目", len(q_rows) == 1, q_rows)
    check("质量异常已核销", q_rows[0]["abnormal"] is False and q_rows[0]["pending"] is False, q_rows[0])

    _, silver = call("GET", "/api/silver?size=200")
    clean = next(s for s in silver["items"] if s.get("表编号") == "clean_orders")
    check("clean_orders 退出待处理队列", clean["pending"] is False and clean["abnormal"] is False, clean)

    print("== 8. 再次失败：历史追加、同分区最早记录为准、台账重新挂起 ==")
    svc = BatchService()
    entry = svc.get_entry(7)
    entry["分区结果"]["15"]["状态"] = "失败"
    entry["分区结果"]["15"]["输出路径"] = None
    entry["分区结果"]["16"]["状态"] = "未完成"
    entry["分区结果"]["16"]["输出路径"] = None
    entry["失败历史"].append({
        "失败记录编号": "batch_orders_07-F002",
        "任务编号": "batch_orders_07", "任务id": 7, "分区": 15,
        "失败时间": "2026-11-13T10:15:00", "处理脚本": entry["处理脚本"],
        "依赖上游": "clean_orders", "输入路径": entry["输入路径"],
        "失败原因": "第二轮批次分区15失败", "状态": "待处理",
        "重放实例编号": None, "解决时间": None,
    })
    entry["status"] = "已失败"; entry["pending"] = True; entry["abnormal"] = True
    svc.sync_ledgers()

    code, _ = call("POST", "/api/batch/7/replay", {"values": {"fail_partitions": [15]}})
    check("重放接口 200", code == 200, code)
    _, task5 = call("GET", "/api/batch/7")
    check("重放实例累计 2 个", len(task5["重放实例"]) == 2, len(task5["重放实例"]))
    check("第二个实例失败状态", task5["重放实例"][1]["状态"] == "已失败")
    check("实例停在分区15", task5["重放实例"][1]["失败分区"] == [15])
    _, payload = call("GET", "/api/batch/7/failures")
    hist = payload["items"]
    check("失败历史累计 3 条（每次都保留）", len(hist) == 3, len(hist))
    f003 = next(h for h in hist if h["失败记录编号"] == "batch_orders_07-F003")
    check("新记录带冲突说明并指向最早记录", "F002" in f003.get("冲突说明", ""), f003)
    _, payload = call("GET", "/api/batch/failures/current")
    current2 = payload["items"]
    check("当前清单分区15只保留最早F002",
          len(current2) == 1 and current2[0]["失败记录编号"] == "batch_orders_07-F002",
          current2)
    check("分区16保持未完成（失败隔离）", task5["分区结果"]["16"]["状态"] == "未完成",
          task5["分区结果"]["16"])
    check("已成功的13/14输出未被覆盖",
          task5["分区结果"]["13"]["输出路径"] == f"{task5['输出路径']}/part=0013"
          and task5["分区结果"]["14"]["输出路径"] == f"{task5['输出路径']}/part=0014")

    _, quality = call("GET", "/api/quality?size=200")
    q2 = [q for q in quality["items"]
          if q.get("来源模块") == "batch" and q.get("来源分区") == 15]
    check("质量台账重新挂起分区15异常", len(q2) == 1 and q2[0]["abnormal"] is True, q2)
    _, silver = call("GET", "/api/silver?size=200")
    clean2 = next(s for s in silver["items"] if s.get("表编号") == "clean_orders")
    check("clean_orders 重新进入待处理队列",
          clean2["pending"] is True and clean2["abnormal"] is True, clean2)

    print("== 9. 按处理脚本与依赖上游定位失败 ==")
    _, payload = call("GET", "/api/batch/7/failures?script=batch_orders_etl.py")
    check("按处理脚本筛选命中全部3条", len(payload["items"]) == 3, len(payload["items"]))
    _, payload = call("GET", "/api/batch/7/failures?upstream=clean_orders")
    check("按依赖上游筛选命中全部3条", len(payload["items"]) == 3, len(payload["items"]))
    _, payload = call("GET", "/api/batch/7/failures?upstream=not_exist")
    check("无关上游筛选为空", payload["items"] == [])

    print("== 10. 最后一次重放恢复，多处结果对得上 ==")
    code, _ = call("POST", "/api/batch/7/replay", {"values": {}})
    check("恢复重放 200", code == 200, code)
    _, task6 = call("GET", "/api/batch/7")
    check("任务最终已完成", task6["status"] == "已完成", task6["status"])
    check("16个分区全部成功", task6["成功分区数"] == 16)
    check("重放实例累计3个", len(task6["重放实例"]) == 3)
    _, scope2 = call("GET", "/api/batch/7/replay-scope")
    check("无可重放分区", scope2["可重放分区"] == [], scope2)
    _, payload = call("GET", "/api/batch/failures/current")
    check("当前失败清单为空", payload["items"] == [])
    _, silver = call("GET", "/api/silver?size=200")
    clean3 = next(s for s in silver["items"] if s.get("表编号") == "clean_orders")
    check("clean_orders 再次退出待处理队列", clean3["pending"] is False)
    _, quality = call("GET", "/api/quality?size=200")
    q_all = [q for q in quality["items"] if q.get("来源模块") == "batch"]
    check("全部批处理质量异常已核销", all(q["pending"] is False for q in q_all), q_all)

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    server.should_exit = True
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
