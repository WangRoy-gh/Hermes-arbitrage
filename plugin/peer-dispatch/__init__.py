"""peer-dispatch plugin — bot-to-bot dispatch whose completion WAKES the calling session.

Registers the ``peer_dispatch`` tool. The point of this plugin (vs. hand-running
``bot_dispatch.py submit`` from the terminal tool) is that the *watch* step is a
property of the tool, not a flag the model must remember:

  1. ``submit``  — unchanged wrapper path: creates ``~/bot_dispatch/<id>/`` and launches
                   the real peer call as a ``systemd-run --user`` transient unit
                   (immune to desktop disconnect / session close).
  2. ``watch``   — spawned IN-GATEWAY via ``tools.terminal_tool.terminal_tool(
                   background=True, notify_on_complete=True, persist_on_release=True)``.
                   Because it is a process-registry job:
                     * ``persist_on_release`` keeps it alive across agent_close /
                       compression / error recovery (process_registry.py:2583);
                     * on exit the gateway's completion watcher injects a new turn into
                       THIS session, in-process — no shared-listener self-post, no
                       default-profile hop, no SOUL rule to remember.

Why not do it in the shell script: ``persist_on_release`` lives in the gateway's
process registry, which an out-of-process script cannot reach. Hence the tool.

Task text still comes from a file (``bot_dispatch.py submit --task-file``), so the
existing task-book conventions (anchoring, DISCIPLINE marker, task discipline) are
preserved verbatim.
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import random
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

_VALID_MODES = ("once", "continue", "review")
_WATCH_GRACE_S = 180  # wait budget beyond the body deadline before the watcher gives up
# sync 模式的前台等待上限。⚠ 必须 < 工具层并发超时 `_DEFAULT_CONCURRENT_TOOL_TIMEOUT_S = 420`
# （agent/tool_executor.py:132），否则**工具层先杀**，调用方连"超时了"这个信息都拿不到。
_SYNC_WAIT_CAP = 360
_NAME_MAX = 100  # = SessionDB.MAX_TITLE_LENGTH（hermes_state.py:1586）。该值只被标题生成器用作
# 自产标题的裁剪阈值，状态层/DB 无强制（title 是 TEXT）；这里仍留余量，防未来上层加限或被别的消费方裁剪。


def _script_path() -> Path:
    """The wrapper this tool thin-wraps (profile-local)."""
    return get_hermes_home() / "scripts" / "bot_dispatch.py"


def _root() -> Path:
    """Ledger root — same override as bot_dispatch.py's PEER_DISPATCH_ROOT."""
    return Path(os.environ.get("PEER_DISPATCH_ROOT") or (Path.home() / "bot_dispatch"))


def _out_file(did: str) -> Path:
    return _root() / did / "out.txt"


def _check_requirements() -> bool:
    return _script_path().is_file()


def _latest_dispatch_session(sid: str, topic: str, peer: str = "") -> str:
    """账本解析：``~/bot_dispatch/*/meta.json`` 里 (发起会话, topic[, peer]) 最近一单的会话名。

    这是 continue 的"不靠记忆续上"的机制：调用方只要重复同一个 topic 即可，
    不需要记住上一轮返回的会话名。
    ⚠ #14（独立复审）：匹配必须带 peer —— 同一 (发起会话, topic) 先后发给
    不同 peer 时，不带 peer 的解析会把会话名接到**另一个 peer** 的会话上
    （会话名落在错误 profile 的库里 → 新建或串线）。peer 为空时退回旧行为（兼容）。
    """
    best, best_t = "", -1.0
    for meta_path in _root().glob("*/meta.json"):
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if not isinstance(m, dict):
                continue     # 审计项：合法 JSON 但非对象 → 跳过（一条坏 meta 不得外抛）
            if m.get("origin_session") == sid and m.get("topic") == topic \
                    and (not peer or m.get("peer") == peer):
                try:
                    t = float(m.get("created") or 0)
                except (TypeError, ValueError):
                    t = 0.0
                if t > best_t:
                    best, best_t = str(m.get("session") or ""), t
        except Exception:  # noqa: BLE001 — 坏 meta 跳过
            continue
    return best


def _derive_session(topic: str, session_id: str | None, mode: str, fresh: bool = False,
                    peer: str = "") -> str:
    """默认会话名：锚在**发起会话 + 发出时刻**上，而不是模型记的一个字符串。

    名字 = ``bd-<发起会话id>-<topic slug>[-<HHMMSS>-<rand3>]``，于是：

      * 跨发起会话不撞名（发起会话 id 唯一）；
      * ``once`` / ``review`` 带**发出时刻 + 3hex 随机** → 同一发起会话同一 topic 发多次
        也几乎不互撞（⚠ 3hex=1/4096，同秒并发存在微小碰撞概率，不是"绝对不撞"——
        2026-10-09 professor 审计 R06 订正措辞；真撞时 wrapper 目录去重会拒单，不会串线）；
      * ``continue`` 不带时刻，走**账本解析**：接上该 (发起会话, topic) 最近一单的会话；
        解析不到才新建；``fresh=True`` 时不查账本，强制唯一新名（反悔键，R03 修复）。

    拿不到发起会话 id（cron / 无头直接调脚本）时返回 ""，退回按 mode 自动命名。
    """
    sid = (session_id or os.environ.get("HERMES_SESSION_ID") or "").strip()
    if not sid:
        # 审计项（独立复审）：本函数返回 (name, new_line) 二元组，早退分支必须同形。
        # 旧版返回裸 "" → 调用方 `session, new_line = _derive_session(...)` 解包 ValueError
        # 崩在 submit 之前（cron/无头清掉 HERMES_SESSION_ID 时必现）。返回 ("", False) =
        # 交回 wrapper 按 mode 自动命名。
        return "", False
    # 名字里的 sid 只做**可读溯源**，固定截到 28 字符（真实会话 id 是 YYYYMMDD_HHMMSS_xxxxxx ≈ 22-23
    # 字符，永不触发）。完整 origin 记在 meta.json 里，continue 的账本解析用的是完整值 —— 所以截断名字
    # 不影响任何查找。这样结构上界 = 3+28+1+24+11 = 67 < _NAME_MAX，永远不用动态裁剪。
    sid_part = sid[:28]
    slug = re.sub(r"[^a-z0-9]+", "-", (topic or "").lower()).strip("-")[:24] or "t"
    # R06 修复（2026-10-09 professor 审计）：slug 截到 24 字符 ⇒ 两个**不同** topic 若共享
    # 前 24 字符（如 "peer-dispatch-audit-round1" / "peer-dispatch-audit-round2"）会派生出
    # **同一个名字** → continue 新建时静默复用旧会话（串线）。加完整 topic 的 4hex 摘要，
    # 把"同 slug 不同 topic"的碰撞概率压到 ~1/65536（⚠ 2026-10-10 复审订正：md5 前缀**不是
    # 结构上不可能**，实测存在反例；兜底靠 wrapper 的标题碰撞检测 #15，不靠摘要本身）。
    # 账本解析用的是完整 topic（精确相等），本来不串；串线只发生在"解析不到 → 新建"这条路上。
    slug = f"{slug}-{hashlib.md5((topic or '').encode()).hexdigest()[:4]}"
    if mode == "continue" and not fresh:
        prev = _latest_dispatch_session(sid, topic, peer)
        if prev:
            # 账本命中 = **有意 resume 旧线**（同 origin+topic），不是偶然撞名 →
            # 调用方据此**不**做碰撞保护（审计项 残余 1：误把合法 resume 当碰撞拒掉）。
            return prev, False
        tail = ""
    else:
        # once / review / fresh-continue：发出时刻 + 短随机（同秒并发有 1/4096 微概率重名，
        # 真撞时 wrapper 的标题占用会重掷/拒单，不会静默串线）
        tail = (f"-{time.strftime('%H%M%S')}"
                f"-{''.join(random.choices('0123456789abcdef', k=3))}")
    name = f"bd-{sid_part}-{slug}{tail}"
    if len(name) > _NAME_MAX:  # 保险：按上面的上界结构上到不了，防将来有人改 slug/sid 的上限
        slug = slug[: max(1, len(slug) - (len(name) - _NAME_MAX))]
        name = f"bd-{sid_part}-{slug}{tail}"
    return name, True   # 新建线：once/review/fresh-continue/continue-miss 都要碰撞保护


def peer_dispatch(
    mode: str,
    peer: str,
    topic: str,
    task_file: str,
    deadline: int = 1500,
    wait: bool = True,
    session: str = "",
    fresh: bool = False,
    sync: bool = False,
    *,
    task_id: str | None = None,
    session_id: str | None = None,
) -> str:
    """Dispatch a task book to a peer bot and (by default) wake this session on completion.

    Returns a JSON string. Never raises into the agent loop.
    """
    mode = (mode or "").strip()
    if mode not in _VALID_MODES:
        return json.dumps({"ok": False,
                           "error": f"bad mode {mode!r}; use one of {_VALID_MODES}"})
    peer = (peer or "").strip()
    if not peer:
        return json.dumps({"ok": False, "error": "peer is required"})
    topic = (topic or "").strip()
    if not topic:
        return json.dumps({"ok": False, "error": "topic is required (short ASCII-able slug)"})

    # ── 默认命名：锚在「发起会话 + 发出时刻」，不靠模型记字符串 ──
    # once/review = 每次发出都是新线（名字带发出时刻+随机）；
    # continue    = 账本解析接上最近一单；fresh=True → 不查账本，强制新线（反悔键）。
    # 显式 session= 优先级最高（原样使用）；fresh 只在未显式命名时参与派生。
    origin = (session_id or os.environ.get("HERMES_SESSION_ID") or "").strip()
    derived = False       # 派生名（供 session_source 展示）
    new_line = False      # 新建线（供 wrapper 碰撞保护）：账本命中的 continue 不算
    if not session:
        session, new_line = _derive_session(topic, origin, mode, fresh=fresh, peer=peer)
        derived = bool(session)

    script = _script_path()
    if not script.is_file():
        return json.dumps({"ok": False, "error": f"wrapper not found: {script}"})

    task_path = Path(task_file).expanduser()
    if not task_path.is_file():
        return json.dumps({"ok": False, "error": f"task_file not found: {task_path}"})

    try:
        budget = max(60, int(deadline))
    except (TypeError, ValueError):
        budget = 1500

    # ── one-shot 会话强制 sync（程序化，不依赖模型记得传 flag）────────────────
    # `hermes chat -q` 起的会话会设 HERMES_SINGLE_QUERY_SESSION=1（hermes_cli/oneshot.py:514）。
    # 这类会话 turn 一结束**进程就退出**：异步 watcher 完成时推送目标已不存在，唤醒必然丢失
    # （2026-10-09 嵌套链实测 实测：peer 漏传 sync → 退化 terminal 轮询 → 被 RuntimeMaxSec 杀）。
    # 所以在这里直接改写，SOUL 写没写都不影响正确性。
    if os.environ.get("HERMES_SINGLE_QUERY_SESSION") == "1":
        sync = True

    # sync 预算钳制（必须在 submit 之前算，子单 RuntimeMaxSec 要跟着钳）：
    #   * 等待上限 < 工具层 420s（_SYNC_WAIT_CAP=360），否则工具层先杀、连超时信息都拿不到；
    #   * 给调用方留 120s 组织回答（它拿到结果后还要输出）；
    #   * 子单 deadline = 等待上限 - 60 → 常规预算下子先死、父读得到终态；
    #     ⚠ 下限例外（独立复审）：requested≤180 时 wait=60/child=60，父先超时、
    #     子由 systemd 托管跑完并落盘（非孤儿，结果稍后可读 out.txt）。
    sync_wait = 0
    if sync:
        sync_wait = min(_SYNC_WAIT_CAP, max(60, budget - 120))
        budget = max(60, sync_wait - 60)

    # ── 1) dispatch: create the run dir + launch the body as a detached systemd unit ──
    argv = [sys.executable, str(script), "submit",
            "--mode", mode, "--peer", peer, "--topic", topic,
            "--task-file", str(task_path), "--deadline", str(budget)]
    # R05 修复（2026-10-09 professor 审计）：把**唤醒意图**传给 wrapper —— 只有"会挂异步
    # watcher、结果靠唤醒回流"的单才需要 pending-wake（补投）。sync 单结果已被调用方
    # 前台消费、wait=false 是调用方主动不要唤醒，两者都不该进补投账本。
    argv += ["--wake-intent", ("no" if (sync or not wait) else "yes")]
    # 审计项 修复（2026-10-09 peer 核验）：HERMES_PROFILE 在 serve 与 one-shot 进程里都**不存在**
    # （实测 /proc/<serve>/environ 只有 HERMES_HOME）→ 旧写法恒得 "发起方"，专家来源的
    # pending 全被按 发起方 星型处理， 的 scope_skip 在生产路径形同虚设。
    # 最可靠来源 = get_hermes_home()：它优先取 context-local override（serve 内按会话路由到
    # 各 profile），与 _script_path() **完全同源** → origin_profile 必然等于"实际执行本工具的
    # profile"。one-shot 内 = …/profiles/<name>；serve 内 发起方 会话 = …/profiles/hub；
    # root/无 override = ~/.hermes（不匹配 /profiles/ → 兜底 发起方，default 不装本工具，无害）。
    try:
        _m = re.search(r"/profiles/([^/]+)$", str(get_hermes_home()).rstrip("/"))
        _prof = _m.group(1) if _m else "发起方"
    except Exception:  # noqa: BLE001 — 极端环境兜底
        _prof = os.environ.get("HERMES_PROFILE") or "发起方"
    argv += ["--origin-profile", _prof]
    if session:
        # 显式/派生会话名：同一名字第二次进入即 resume（chat --create-if-missing）。
        argv += ["--session", session]
        if new_line:
            # 新建线（once/review/fresh-continue/continue-miss）→ 让 wrapper 做标题占用
            # （审计项：查重+占用原子化）。账本命中的 continue 合法 resume，不传此 flag。
            argv += ["--session-derived"]
    if origin:
        # 记进 meta：continue 的账本解析（按 (发起会话, topic) 找最近一单）靠它。
        argv += ["--origin-session", origin]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, errors="replace", timeout=90)
    except Exception as exc:  # noqa: BLE001 — report, never raise
        logger.warning("peer_dispatch submit failed to run: %r", exc)
        return json.dumps({"ok": False, "error": f"submit could not run: {exc!r}"})

    if r.returncode != 0:
        return json.dumps({"ok": False, "error": "submit failed",
                           "stderr": (r.stderr or "").strip()[-500:]})
    # `submit` prints the dispatch id on the last stdout line.
    lines = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
    did = lines[-1] if lines else ""
    if not did:
        return json.dumps({"ok": False, "error": "submit produced no dispatch id",
                           "stdout": (r.stdout or "")[-300:]})

    out_file = _out_file(did)
    # 回传会话名：多轮调用方把它原样传回 session= 即可 resume 同一会话。
    resolved_session, pre_msgs, meta_warn = "", None, ""
    try:
        _meta = json.loads(
            (_root() / did / "meta.json").read_text(encoding="utf-8")
        )
        resolved_session = _meta.get("session", "")
        pre_msgs = _meta.get("pre_msg_count")
        meta_warn = _meta.get("warn", "")
    except Exception:  # noqa: BLE001 — 读不到就留空，不影响派单本身
        pass
    result: dict[str, Any] = {
        "ok": True, "id": did, "peer": peer, "mode": mode, "topic": topic,
        "session": resolved_session,
        "session_source": ("derived" if derived else ("explicit" if session else "auto")),
        "resumed": bool(pre_msgs),   # true = 接上了既有会话（第 N+1 轮），不是全新线
        "pre_msg_count": pre_msgs,
        "out_file": str(out_file), "watched": False,
    }
    # 审计项（独立复审）：wrapper 算出的 warn（膨胀/未知历史/未注锚）必须回传到
    #   工具返回值——否则模型主入口永远看不到，"膨胀只 WARN"的保证在真实调用路径上形同虚设。
    if meta_warn:
        result["warn"] = meta_warn
    # R03 补全（2026-10-09 peer 核验）：fresh=True 与显式 session= 同时给出时，命名以
    # session= 为准、fresh 被忽略——旧行为静默，调用方以为起了新线实则可能续旧会话。显式回告。
    if fresh and not derived and session:
        result["fresh_ignored"] = ("fresh=True 被忽略：显式给了 session= 时命名以 session= 为准"
                                   "（要强制另起新线请省略 session=）")

    # ── one-shot 会话强制 sync（程序化，不依赖模型记得传 flag）────────────────
    # `hermes chat -q` 起的会话会设 HERMES_SINGLE_QUERY_SESSION=1（hermes_cli/oneshot.py:514）。
    # 这类会话 turn 一结束**进程就退出**：异步 watcher 完成时推送目标已不存在，唤醒必然丢失
    # （2026-10-09 嵌套链实测 实测：peer 漏传 sync → 退化 terminal 轮询 → 被 RuntimeMaxSec 杀）。
    # 所以在这里直接改写，SOUL 写没写都不影响正确性。
    if os.environ.get("HERMES_SINGLE_QUERY_SESSION") == "1":
        sync = True

    # ── 2a) sync：前台阻塞等结果，正文直接进返回值 ─────────────────────────────
    # 为什么需要：one-shot CLI 会话（`hermes chat -q`）turn 一结束**进程就退出**，
    # watcher 完成时已无人可唤醒（推送目标不存在）。所以专家侧调用必须 sync=True，
    # 在本 turn 内把 peer 的结果拿回来。桌面/常驻会话用默认 wait=True 走 watcher 唤醒。
    if sync:
        # sync_wait / 子单 budget 已在 submit 前钳好（见上）；这里只负责等。
        budget_wait = sync_wait
        cmd = [sys.executable, str(script), "wait", did, "--timeout", str(budget_wait)]
        timed_out = False
        try:
            wr = subprocess.run(cmd, capture_output=True, text=True,
                                errors="replace", timeout=budget_wait + 30)
            # R02 修复（2026-10-09 professor 审计）：终态**读 status 文件**，不从 wait 的
            # stdout 末行猜 —— done 时 stdout 是 "<id>: done\n<正文>"，末行是正文最后一行。
            # R02 补全（2026-10-09 peer 核验）：wait-timeout 时 status 文件**仍是 running**
            # （非空，旧代码因此跳过 rc 兜底）→ 必须按 rc 语义覆盖"假 running"：rc=2=wait 超时。
            try:
                status = (_root() / did / "status").read_text().strip()
            except OSError:
                status = ""
            if wr.returncode == 2 or not status:
                # rc 语义优先：0=done / 1=非 done 终态 / 2=wait-timeout（文件可能滞后于 rc）
                status = {0: "done", 1: status or "unknown", 2: "timeout"}.get(wr.returncode, status or "unknown")
        except subprocess.TimeoutExpired:
            timed_out, status = True, "timeout"
        except Exception as exc:  # noqa: BLE001
            return json.dumps({**result, "ok": True, "sync": True,
                               "error": f"wait could not run: {exc!r}"})
        body = ""
        try:
            body = out_file.read_text(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            pass
        result["sync"] = True
        result["watched"] = False
        result["status"] = "timeout" if timed_out else (status or "unknown")
        result["result"] = body
        if timed_out:
            result["note"] = (f"前台等待到上限（{budget_wait}s），peer 可能仍在跑；"
                              f"稍后可用 bot_dispatch.py status {did} 复查，out_file 会更新")
        return json.dumps(result, ensure_ascii=False)

    # ── 2b) watch: spawn a PERSISTED, NOTIFYING watcher in-gateway ──
    # This is the whole reason the tool exists. Default ON; wait=false opts out.
    if wait:
        try:
            from tools.terminal_tool import terminal_tool

            cmd = (f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} "
                   f"wait {shlex.quote(did)} --timeout {budget + _WATCH_GRACE_S}")
            raw = terminal_tool(
                command=cmd,
                background=True,
                notify_on_complete=True,
                persist_on_release=True,
                task_id=task_id,
                session_id=session_id,
            )
            try:
                spawned = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            except Exception:
                spawned = {"raw": str(raw)[:300]}
            result["watcher"] = {
                k: spawned.get(k)
                for k in ("session_id", "pid", "exit_code", "error",
                          "notify_on_complete", "persist_on_release",
                          "notify_unsupported", "subagent_note")
                if spawned.get(k) is not None
            }
            result["watched"] = bool(spawned.get("notify_on_complete"))
            if spawned.get("error"):
                result["watcher_error"] = spawned.get("error")
            if not result["watched"] and spawned.get("notify_unsupported"):
                result["warning"] = spawned.get("notify_unsupported")
            # 把 watcher 进程号落进派单目录：pw_sweep 补投时用它做**精确去重**
            # （原生通知的注入文本里含 proc id；缺这个字段就只能退化成时间窗启发式）。
            try:
                (_root() / did / "watcher.json").write_text(
                    json.dumps({"proc_id": spawned.get("session_id") or "",
                                "pid": spawned.get("pid"),
                                "notify_on_complete": bool(spawned.get("notify_on_complete")),
                                "spawned": time.time()}, ensure_ascii=False), encoding="utf-8")
            except Exception:  # noqa: BLE001 — 留痕失败不影响派单
                pass
        except Exception as exc:  # noqa: BLE001 — dispatch already succeeded; degrade loudly
            logger.warning("peer_dispatch watcher spawn failed: %r", exc)
            result["watcher_error"] = repr(exc)

    return json.dumps(result, ensure_ascii=False)


def register(ctx) -> None:
    ctx.register_tool(
        name="peer_dispatch",
        toolset="peer_dispatch",
        schema={
            "name": "peer_dispatch",
            "description": (
                "Dispatch a task to a peer Hermes bot (peer / dispatcher-a / receiver-c / "
                "dispatcher-b) and — by default — have THIS session woken automatically "
                "when the peer finishes. Prefer this over hand-running the dispatch script "
                "in a terminal: the completion watch is built in, so there is no flag to "
                "forget and no background monitor to babysit. Write the task book to a file "
                "first and pass its path as task_file. Modes: once (fresh named session, "
                "for research/verification), continue (resume a named session, for relayed "
                "work), review (clean-context review with anti-contamination anchor)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": list(_VALID_MODES),
                             "description": "once | continue | review"},
                    "peer": {"type": "string",
                             "description": "Peer profile name, e.g. peer, dispatcher-a, receiver-c, dispatcher-b."},
                    "topic": {"type": "string",
                              "description": "Short topic slug for the dispatch id and session name."},
                    "task_file": {"type": "string",
                                  "description": "Absolute path to the task book written to disk."},
                    "deadline": {"type": "integer", "default": 1500,
                                 "description": "Body run budget in seconds (async path: RuntimeMaxSec = deadline + 60). "
                                                "⚠ sync path (one-shot/CLI) RE-BUDGETS it: wait=min(360,max(60,deadline-120)), "
                                                "child body=max(60,wait-60) — so a 2700 deadline under sync gives the child only "
                                                "300s. The 360s wait cap comes from the tool-layer 420s concurrency "
                                                "timeout and CANNOT be raised by increasing this deadline; if the nested "
                                                "body needs >300s, do NOT use sync (use async + wake instead)."},
                    "wait": {"type": "boolean", "default": True,
                             "description": "When true (default) a persistent watcher wakes this session "
                                            "on completion. Set false ONLY if you will poll the result yourself."},
                    "session": {"type": "string",
                                "description": "显式会话名（多轮用）。首轮省略即自动命名，返回值里的 session "
                                               "字段就是它；第 2 轮起把同一个名字传回这里 → resume 同一会话，"
                                               "多轮往返因此不会各说各话。"},
                    "fresh": {"type": "boolean", "default": False,
                              "description": "多轮反悔键，仅对 continue 有意义：continue 默认按 (发起会话,topic) "
                                             "接上最近一单会话；传 true → 不查账本、强制唯一新名（另起全新线，"
                                             "不要上一轮上下文）。once/review 本就每次唯一新名，fresh 对它们无作用；"
                                             "显式给了 session= 时 fresh 被忽略（命名以 session= 为准）。"},
                    "sync": {"type": "boolean", "default": False,
                             "description": "前台同步等结果（上限 360s），peer 正文直接进返回值 result 字段。"
                                            "one-shot/CLI 会话（chat -q 起的专家会话）**必须** true——"
                                            "你的 turn 一结束进程就退出，异步 watcher 无人可唤醒；"
                                            "常驻/桌面会话用默认 false，完成时自动唤醒本会话。"},
                },
                "required": ["mode", "peer", "topic", "task_file"],
            },
        },
        handler=lambda args, **kw: peer_dispatch(
            mode=args.get("mode", ""),
            peer=args.get("peer", ""),
            topic=args.get("topic", ""),
            task_file=args.get("task_file", ""),
            deadline=args.get("deadline", 1500),
            wait=bool(args.get("wait", True)),
            session=args.get("session", "") or "",
            fresh=bool(args.get("fresh", False)),
            sync=bool(args.get("sync", False)),
            task_id=kw.get("task_id"),
            session_id=kw.get("session_id"),
        ),
        check_fn=_check_requirements,
        description="Dispatch to a peer bot; this session is woken on completion.",
        emoji="📮",
    )
