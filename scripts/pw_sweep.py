#!/usr/bin/env python3
"""pending-wake 补投扫描器（带完整留痕）。

【解决什么】
peer_dispatch 的完成通知由网关在**内存里 push 给会话**；若那一刻目标会话不常驻内存
（桌面断连 / agent 被回收），推送被**静默丢弃**：无重试、无队列，会话消息与 agent.log 都不留痕迹。
2026-10-01 实证：watcher 正常退出（exit 1），但目标会话里没有注入消息、日志里也没有投递记录。

【机制】
1. 派单终态时 wrapper 写 <dispatch>/pending-wake.json（含 origin_session；插件另写 watcher.json 带 proc id）。
2. 本扫描器（cron 每 5 分钟，no_agent）遍历未送达条目：
   a. **核实**：目标会话 state.db 里有没有"该 watcher proc 的注入记录" → 有 = 已送达（不重复打扰）
   b. 没有：向 api_server POST 一条唤醒消息（头 X-Hermes-Session-Id）把会话叫醒
   c. 记录结果；失败下轮重试。目标会话已不存在 → 记 target_missing 终态并停手。
3. 每一步都留痕（见下）。

【留痕：三处，互不覆盖】
- <dispatch>/wake-delivery.json        该派单的**投递全史**：每次尝试一行（时间/判定依据/证据/结果/HTTP码）
- <dispatch>/pending-wake.json         原始凭据（**永不改写**）
- ~/bot_dispatch/_sweeper/audit.jsonl  **全局追加**日志，一行一事件 → 便于 grep / 统计 / 事后复盘
- ~/bot_dispatch/_sweeper/sweeper.log  人类可读摘要，只在有动作时追加（平时静默）

用法：
  pw_sweep.py [--dry] [--verbose] [--max-age-hours N]
退出码：0 = 本轮无未送达或全部处理完；非 0 只在脚本自身出错（cron 会因此告警）。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(os.environ.get("PEER_DISPATCH_ROOT") or (Path.home() / "bot_dispatch"))
SWEEP_DIR = ROOT / "_sweeper"
AUDIT = SWEEP_DIR / "audit.jsonl"
LOG = SWEEP_DIR / "sweeper.log"
BUTLER_DB = Path.home() / ".hermes" / "profiles" / "发起方" / "state.db"
# 本扫描器的 scope（补投目标 profile）。生产固定 发起方；模板版由 PW_ORIGIN_PROFILE 参数化。
ORIGIN_PROFILE = os.environ.get("PW_ORIGIN_PROFILE", "hub")
# 取 key 的顺序：**先 profile 自己的**（mirror /p/<profile>/ 认的就是目标 profile 的 API_SERVER_KEY，
# 官方注释：`config_env.py:200` "the docs require the key in its .env for /p/<profile>/ auth"），
# 再退到 root（default 路由用）。
ENV_FILES = [Path.home() / ".hermes" / "profiles" / "发起方" / ".env",
             Path.home() / ".hermes" / ".env"]
# 必须带 /p/hub：POST 到根 = default 路由，会写进 **default 的库**（2026-10-01 实测踩到）。
API_BASE = os.environ.get("PW_API_BASE", "http://127.0.0.1:8642/p/hub")
API_KEY_ENV = "API_SERVER_KEY"

# ── 竞态护栏参数（2026-10-01 实证加入）────────────────────────────────────────
# 原生注入消息是在**发起方回合结束时**才提交进会话库的（实测：注入行与发起方收尾的
# assistant 行同一时刻落库）。所以扫描器若在"派单已终态、发起方回合还在跑"的窗口里
# 查库，必然看不到注入 → 把**正常投递**误判成"丢了" → 假补投。
# 2026-10-01 23:51:04 实测踩到：上一轮已落库判 already_delivered，同一 tick 的下一轮
# 因注入要到 23:51:19（回合结束）才落库而被误判补投。
GRACE_S = float(os.environ.get("PW_GRACE_S", "120"))    # 终态后最短观察窗（比提交延迟宽）
HARD_CAP_S = float(os.environ.get("PW_HARD_CAP_S", "3600"))  # 超此年龄不再拦（防永久 defer）


def _ts(t: float | None = None) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t if t else time.time()))


def audit(event: dict) -> None:
    """追加一行到全局审计日志（永不覆盖）。"""
    SWEEP_DIR.mkdir(parents=True, exist_ok=True)
    event = {"ts": time.time(), "ts_h": _ts(), **event}
    with AUDIT.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def logline(msg: str, verbose: bool = False, force: bool = False) -> None:
    """人类可读摘要：只在有动作 / verbose 时写（平时静默，避免日志噪音）。"""
    if not (verbose or force):
        return
    SWEEP_DIR.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_ts()}] {msg}\n")
    if verbose:
        print(msg)


def read_api_key() -> str:
    """从 env 文件读 API_SERVER_KEY。**只用于发请求，绝不打印。**"""
    for path in ENV_FILES:
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == API_KEY_ENV and v.strip():
                    # #31 + 审计项（独立复审）：标准 dotenv quoted 状态机——
                    #   ① 引号包裹：扫描到**未转义**的匹配闭引号为止（\" 里的引号不算闭合）；
                    #   ② 裸值：` #`（空白+井号）之后视为行内注释，剥掉。
                    val = v.strip()
                    if val[:1] in ("\"", "'"):
                        q = val[0]
                        out, i, n = [], 1, len(val)
                        while i < n:
                            c = val[i]
                            if c == "\\" and i + 1 < n and val[i + 1] in (q, "\\"):
                                out.append(val[i + 1]); i += 2; continue   # \"/\\ = 转义
                            if c == q:
                                break                                      # 真闭引号，其后全丢
                            out.append(c); i += 1
                        val = "".join(out)
                    else:
                        # 裸值：仅 " #"（前置空白）起算注释，避免误伤值里含 # 的 key
                        val = re.split(r"\s+#", val, 1)[0].strip()
                    if val:
                        return val
        except OSError:
            continue
    return ""


def sq(db: Path, query: str) -> list[list[str]]:
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            return [list(map(str, row)) for row in con.execute(query).fetchall()]
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        return [["<sqlite-error>", str(exc)]]


def session_exists(db: Path, sid: str) -> str:
    """三态：'yes' 存在 / 'no' 确证不存在 / 'unknown' 读库失败。
    #19（独立复审）：旧版 bool(sq(...)) 把 sq 的错误行 [["<sqlite-error>",...]]
    也判成 True（存在）——库锁/损坏时"存在性检查"恒真，target_missing 判定失效。
    调用方必须把 'unknown' 当"不判定"处理（defer），既不误删也不误标缺失。"""
    rows = sq(db, f"SELECT id FROM sessions WHERE id='{sid}' LIMIT 1")
    if not rows:
        return "no"
    if rows[0][0].startswith("<sqlite-error>"):
        return "unknown"
    return "yes" if rows[0][0] else "no"


def origin_idle(db: Path, sid: str) -> tuple[bool, str]:
    """发起方会话是否**已空闲**（回合已结束）。返回 (idle, 说明)。

    判据（2026-10-01 实测得到）：回合结束时最后一条 active 消息是 assistant 且无 tool_calls；
    回合进行中最后一条是 tool（工具结果）或带 tool_calls 的 assistant。
    ⚠ #19（独立复审）：读不到库时返回 **False**
    （fail-closed，按未空闲 defer，HARD_CAP_S 兜底防发起方永久卡死），**不是** True。
    """
    rows = sq(db, "SELECT id, role, tool_calls FROM messages WHERE session_id='%s' "
                  "AND active=1 ORDER BY id DESC LIMIT 1" % sid)
    if not rows or rows[0][0].startswith("<sqlite-error>") or not rows[0][0]:
        # #19（独立复审）：读库失败旧版返回 True(空闲)=fail-open → 观察窗外直接
        # deliver，库锁期间可能双投。改 fail-closed：按"未空闲"处理 → 本轮 defer，
        # HARD_CAP_S 兜底防发起方永久卡死时永不补投。
        return False, "unknown（读不到会话消息，按未空闲 defer，HARD_CAP 兜底）"
    mid, role, tc = rows[0][0], rows[0][1], rows[0][2]
    has_tc = tc not in ("", "None")   # ⚠ sq() 把每列 str() 化 → SQL NULL 变成字符串 "None"
    if role == "assistant" and not has_tc:
        return True, f"idle（last={mid} assistant/无 tool_calls）"
    return False, f"mid-turn（last={mid} role={role} tool_calls={'有' if has_tc else '无'}）"


def already_delivered(sid: str, proc_id: str, dispatch_id: str, since: float) -> str:
    """核实是否已送达。返回**证据字符串**（空 = 没送到）。三种规则分别留痕（证据里写明是哪条命中的）。

    规则顺序 = 精确优先：① 原生通知（含 watcher proc id）→ ② 本扫描器自己投过的补投消息
    → ③ 兜底时间窗启发式。③ 是"宁可漏投也别重复打扰"的保守兜底。
    ⚠ 必须包含 ②：本扫描器投出的文本不含 "Background process proc_" 特征，若无 ② 会**每轮重复投递**。
    """
    if not BUTLER_DB.exists():
        return ""
    if proc_id:
        # ⚠ 必须同时限定 role='user' + **注入形态**的文本：watcher 进程号也出现在 peer_dispatch 的
        # **工具返回 JSON** 里（`"session_id": "proc_xxx"`）。只按 proc id 搜，会把"工具结果消息"误判成
        # "已送达"——那样真丢了的唤醒**永远不会被补投**（2026-10-01 实测踩到：命中的是工具结果而非注入消息）。
        rows = sq(BUTLER_DB, "SELECT id FROM messages WHERE session_id='%s' AND role='user' "
                             "AND content LIKE '%%Background process %s%%' ORDER BY id LIMIT 1"
                 % (sid, proc_id))
        if rows and rows[0][0] not in ("<sqlite-error>", ""):
            return f"exact/native: 会话消息 id={rows[0][0]} 命中注入消息（watcher {proc_id}）"
    if dispatch_id:
        # #17（独立复审）：与 exact/native 同理必须限定 role='user'——补投文本也出现在
        # 本会话的**工具返回/上下文**里时，缺 role 会把非注入消息误判成"已送达"→ 真丢的单永不补投。
        rows = sq(BUTLER_DB, "SELECT id FROM messages WHERE session_id='%s' AND role='user' "
                             "AND content LIKE '%%%s%%' "
                             "AND content LIKE '%%[补投/系统]%%' ORDER BY id LIMIT 1" % (sid, dispatch_id))
        if rows and rows[0][0] not in ("<sqlite-error>", ""):
            return f"exact/sweeper: 会话消息 id={rows[0][0]} 命中本扫描器对 {dispatch_id} 的补投记录"
    rows = sq(BUTLER_DB, "SELECT id, timestamp FROM messages WHERE session_id='%s' AND role='user' "
                         "AND content LIKE '%%Background process proc_%%' AND timestamp > %f "
                         "ORDER BY id LIMIT 1" % (sid, since))
    if rows and rows[0][0] not in ("<sqlite-error>", ""):
        return f"heuristic: 会话消息 id={rows[0][0]}（终态后出现过后台完成注入，无法精确归属）"
    return ""


def deliver(sid: str, text: str) -> tuple[bool, str]:
    """向 api_server 投一条消息，把目标会话叫醒。返回 (成功, 细节)。"""
    key = read_api_key()
    if not key:
        return False, f"no {API_KEY_ENV} in env files"
    body = json.dumps({
        "model": os.environ.get("PW_MODEL", "local_LLM"),
        "messages": [{"role": "user", "content": text}],
        "stream": False,
    }).encode("utf-8")
    req = urllib.request.Request(
        API_BASE.rstrip("/") + "/v1/chat/completions", data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {key}",
                 "X-Hermes-Session-Id": sid})
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return True, f"HTTP {resp.status}, {len(raw)}B"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}: {exc.read()[:200].decode('utf-8', 'replace')}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def record(ddir: Path, entry: dict) -> None:
    """把一次尝试追加进该派单的投递全史（保留全部历史，不覆盖）。"""
    hist_path = ddir / "wake-delivery.json"
    try:
        hist = json.loads(hist_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        hist = {"dispatch": ddir.name, "attempts": []}
    hist["attempts"].append(entry)
    hist["last_state"] = entry.get("result", "?")
    hist["last_ts_h"] = entry.get("ts_h", _ts())
    # #18（独立复审）：tmp 名必须**每调用唯一**——固定 .json.tmp 在两个并发
    # record 下会互相 rename 走对方的 tmp（FileNotFoundError，实测出现过）。
    tmp = ddir / f"wake-delivery.{os.getpid()}.json.tmp"
    tmp.write_text(json.dumps(hist, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(hist_path)


def main() -> int:
    ap = argparse.ArgumentParser(prog="pw_sweep")
    ap.add_argument("--dry", action="store_true", help="只判定不投递（仍写审计）")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--max-age-hours", type=float, default=168.0)
    a = ap.parse_args()

    # #18（独立复审）：进程级单飞锁。cron 漏配外层去重时，两个扫描器并发
    # 会在同一 pending 上都进入 deliver（实测合成环境双投）。锁不抢、直接退：
    # 另一个扫描器会处理同一批 pending，跳过本轮无损。
    import fcntl
    SWEEP_DIR.mkdir(parents=True, exist_ok=True)
    _lock_fh = open(SWEEP_DIR / "sweep.lock", "w")
    try:
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("pw_sweep: 另一实例在跑（flock），本轮跳过")
        return 0

    cutoff = time.time() - a.max_age_hours * 3600
    pending, skipped_done, skipped_old = [], 0, 0
    for ddir in sorted(ROOT.glob("*/")):
      try:
        # #20（独立复审）：收集阶段的**每一条**都要隔离——旧版 try 只包 json.loads，
        #   JSON=[]（info.get 属性错）或 created='not-a-float'（float() 抛 ValueError）
        #   仍炸掉整轮。坏条目跳过并留痕，后面的健康条目必须照常处理。
        pw = ddir / "pending-wake.json"
        if not pw.is_file():
            continue
        info = json.loads(pw.read_text(encoding="utf-8"))
        if not isinstance(info, dict):
            raise ValueError(f"pending-wake 不是对象: {type(info).__name__}")
        # #30（独立复审）：scope 判定必须按**当前扫描器自己的 scope**，且不属于
        #   本 scope 的条目**静默跳过、不写任何终态**——写 scope_skip 终态会让日后接手
        #   的正确 scope 扫描器把它当"已处理"永久跳过（双向实测复现）。
        if str(info.get("origin_profile", "发起方")) != ORIGIN_PROFILE:
            continue
        sid = str(info.get("origin_session", ""))
        try:
            created = float(info.get("created") or 0)
        except (TypeError, ValueError):
            created = 0.0
        # 审计项（独立复审）：容错值必须**写回** info——旧版只在收集阶段局部归一，
        # 消费阶段 :344 再对原坏值 float() → 同一坏值重复 entry_error，容错做了一半。
        info["created"] = created
        if not sid:
            continue
        if created and created < cutoff:
            skipped_old += 1
            continue
        # 已完成投递且不是重试态 → 跳过（wake-delivery.json 里记着终态）
        hist = ddir / "wake-delivery.json"
        if hist.is_file():
            try:
                st = json.loads(hist.read_text(encoding="utf-8")).get("last_state", "")
                if st in ("delivered", "already_delivered", "target_missing"):
                    skipped_done += 1
                    continue
            except Exception:  # noqa: BLE001
                pass
        pending.append((ddir, info))
      except Exception as exc:  # noqa: BLE001 — 单条收集失败隔离，不得阻断整轮
        audit({"event": "collect_error", "dispatch": ddir.name, "error": repr(exc)})
        continue

    if not pending:
        if a.verbose:
            print(f"pw_sweep: 无待补投（已完结 {skipped_done}，超期 {skipped_old}）")
        return 0

    logline(f"pw_sweep: 待补投 {len(pending)} 条（超期跳过 {skipped_old}）", a.verbose, force=False)
    for ddir, info in pending:
      try:
        # #20（独立复审）：单条 pending 的任何异常只影响它自己，不得阻断整轮
        # （旧版一条坏 schema 的 pending 抛异常 → 整个 tick 崩、其余全部不处理）。
        sid = info.get("origin_session", "")
        # ⚠ R01 修复（2026-10-09 professor 审计）：pw 变量在第一轮循环后残留的是**最后一个目录**的
        # 路径；护栏取 mtime 必须按**当前单**重新绑定，否则 age 张冠李戴（该放的被拦、该拦的被放）。
        pw = ddir / "pending-wake.json"
        # ：扫描器只覆盖 发起方 来源的异步单。专家来源（多发起方架构网状派单）的 sync 单结果已
        # 被调用方前台消费，无需补投。#30（独立复审）：本分支现在**只在历史遗留**
        # 时到达——收集阶段已按扫描器 scope 静默过滤，此处兜底：静默跳过、**不写终态**
        # （写 scope_skip 会永久污染正确 scope 的接手扫描器）。
        if str(info.get("origin_profile", "发起方")) != ORIGIN_PROFILE:
            continue
        watcher = {}
        try:
            watcher = json.loads((ddir / "watcher.json").read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        proc_id = str(watcher.get("proc_id") or "")
        base = {"dispatch": ddir.name, "origin_session": sid, "status": info.get("status"),
                "watcher_proc": proc_id, "ts_h": _ts()}

        _ex = session_exists(BUTLER_DB, sid)
        if _ex == "unknown":
            # #19：读库失败 ≠ 存在也 ≠ 缺失——defer 到下一轮，绝不在库锁/损坏时做判定。
            entry = {**base, "result": "deferred", "detail": "session_exists 读库失败，本轮不判定"}
            record(ddir, entry)
            audit({"event": "deferred", **entry})
            logline(f"  {ddir.name}: 存在性检查读库失败 → 本轮 defer", a.verbose, force=True)
            continue
        if _ex == "no":
            entry = {**base, "result": "target_missing", "detail": "目标会话不存在（可能已删）"}
            record(ddir, entry)
            audit({"event": "target_missing", **entry})
            logline(f"  {ddir.name}: 目标会话 {sid} 不存在 → 停手", a.verbose, force=True)
            continue

        ev = already_delivered(sid, proc_id, ddir.name, float(info.get("created") or 0))
        if ev:
            entry = {**base, "result": "already_delivered", "evidence": ev}
            record(ddir, entry)
            audit({"event": "already_delivered", **entry})
            logline(f"  {ddir.name}: 已送达，跳过（{ev}）", a.verbose)
            continue

        # ── 竞态护栏（2026-10-01 加入；R01 修复后 pw 已按当前单绑定）──────────
        try:
            term_ts = pw.stat().st_mtime
        except OSError:
            term_ts = 0.0
        age = (time.time() - term_ts) if term_ts else 0.0
        if age < HARD_CAP_S:
            idle, why = origin_idle(BUTLER_DB, sid)
            if not idle or age < GRACE_S:
                reason = why if not idle else f"终态距今仅 {int(age)}s（<{int(GRACE_S)}s 观察窗）"
                entry = {**base, "result": "deferred",
                         "detail": f"竞态护栏拦下：{reason}"}
                # 留痕但同一轮只记一次（每 tick 都记会刷屏；有真实动作时自然追加新行）
                last = ""
                try:
                    last = json.loads((ddir / "wake-delivery.json").read_text(encoding="utf-8")
                                      ).get("last_state", "")
                except Exception:  # noqa: BLE001
                    pass
                if last != "deferred":
                    record(ddir, entry)
                    audit({"event": "deferred", **entry})
                    logline(f"  {ddir.name}: 暂不补投（{reason}）", a.verbose)
                continue

        if a.dry:
            entry = {**base, "result": "would_deliver", "detail": "dry-run 未投递"}
            audit({"event": "would_deliver", **entry})
            logline(f"  {ddir.name}: [dry] 需要补投 → {sid}", a.verbose, force=True)
            continue

        outhint = info.get("out") or ""
        text = (f"[补投/系统] 后台派单 `{ddir.name}` 已结束（status={info.get('status')}，"
                f"topic={info.get('topic')}，peer={info.get('peer')}）。\n"
                f"这条通知原本的推送丢了（会话当时不在内存），由 pw_sweep 补投。\n"
                f"结果文件：{outhint}\n"
                f"请核对后继续处理；本条为系统补投，无需向用户确认是否收到。")
        ok, detail = deliver(sid, text)
        entry = {**base, "result": "delivered" if ok else "deliver_failed", "detail": detail}
        record(ddir, entry)
        audit({"event": entry["result"], **entry})
        logline(f"  {ddir.name}: {'补投成功' if ok else '补投失败'}（{detail}）", a.verbose, force=not ok)
        if not ok:
            # 打到 stdout → cron 会把它投递出来 ⇒ 补投失败**必须被看见**，不能只躺在日志里
            print(f"pw_sweep: 补投失败 {ddir.name} → 会话 {sid}：{detail}（详见 audit.jsonl）")
      except Exception as exc:  # noqa: BLE001 — #20：单条失败隔离，不得阻断整轮
        audit({"event": "entry_error", "dispatch": ddir.name, "error": repr(exc)})
        print(f"pw_sweep: 处理 {ddir.name} 出错（已隔离，继续其余）：{exc!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
