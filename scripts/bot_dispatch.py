#!/usr/bin/env python3
"""bot_dispatch — bot 间调用统一 wrapper（v1，2026-09-30 定稿）。

设计定稿经 peer 对抗评审（~/plans/20260930_bot_dispatch_review_out.txt）：
  mode → 通道映射（程序化，调用方只声明目的）。**通道一律** chat --query-file <task>
  -c <session> --create-if-missing（2026-10-09 按实现订正；旧述"review=chat -q"已废）：
    once     单一任务/调研/核实 → 每次唯一命名会话（干净新建）
    continue 接力活            → 账本解析接上 (发起会话,topic) 最近一单；解析不到才新建
    review   单次评审          → 唯一命名会话 + 首轮自动注入防旧结论锚定句（= fresh 语义）
  保证：
    ① 执行体一律 systemd-run --user transient unit（RuntimeMaxSec=deadline+60），
       会话/desktop 断连免疫；cron 脚本内同步等待用 --inline 豁免（M5/边界条）
    ② 去重 = ~/bot_dispatch/<id>/ 目录存在即拒绝（chat -c 无 provider 端幂等)
    ③ id = r-<日期>-<ascii主题>-<HHMMSS>-<rand4>（S2：主题 ASCII 化防 unit 名转义）
    ④ 命名会话同名第二次进入即 resume；continue 膨胀只 WARN 不换会话（S3：换名=丢活体上下文）
    ⑤ 跨机 peer transport v1 不做（本机 4 peer 全回环，M1/V2）；announce 不做（V1）；
       debate 推迟（设计决策：停滞≠收敛，早期多轮辩论实验实证）
  产物：~/bot_dispatch/<id>/{task.md,out.txt,meta.json,status}
  与 delegate_task 分界：三笔账命中的批量粗活仍走 delegate，不进本 wrapper。

用法:
  bot_dispatch.py submit --mode once|continue|review --peer <p> --topic <t> \
      (--task-file F | stdin) [--deadline 1500] [--inline] [--notify TARGET]
  bot_dispatch.py wait <id> [--timeout 3600]
  bot_dispatch.py status <id>
  bot_dispatch.py gc --peer <p> --prefix <前缀> [--older-than 30d] [--dry]
"""
import argparse
import fcntl
import hashlib
import json
import os
import random
import re
import threading

import string
import subprocess
import sys
import time
import uuid

from pathlib import Path

ROOT = Path(os.environ.get("PEER_DISPATCH_ROOT") or (Path.home() / "bot_dispatch"))
ORIGIN_PROFILE = os.environ.get("PEER_DISPATCH_PROFILE", "hub")
HERMES = os.environ.get("HERMES_BIN", str(Path.home() / ".local/bin/hermes"))
PEER_PROFILE = os.environ.get("PEER_DISPATCH_PROFILE", "hub")          #  peer 注册表 per-profile，peer 操作钉死 发起方 scope
SEND_PROFILE = "default"         # multiplex 后 send 只有 default scope 持 token
MSG_WARN = 500
# 审计项（独立复审）：账本锁是全局的 → 临界区**必须有界**，否则一个卡在
# sqlite3/systemctl 里的派单会连带阻塞所有其他派单的终态提交。两道界：
#   ① 锁内所有 subprocess 调用带 timeout（_LOCK_SUBPROC_TIMEOUT）→ 持锁时间有界；
#   ② 锁获取用非阻塞轮询 + 总期限（_LOCK_ACQUIRE_TIMEOUT）→ 等待时间有界，
#      超时抛 TimeoutError（诚实报错），绝不无限排队。
_LOCK_SUBPROC_TIMEOUT = 10
_LOCK_ACQUIRE_TIMEOUT = 60
# 自查补漏（定稿自查）：launching→running 正常是亚秒级；滞留超此值 = submit
# 死在启动间隙 → read_status 判 lost（否则僵尸 launching 无任何兜底路径）。
_LAUNCHING_MAX_S = 120
ANCHOR = ("[全新独立任务 — 与任何此前话题无关，忽略你此前对任何主题的结论，"
          "只依据本任务书证据重判]")
# 2026-10-01 教训：-q 模式无人工审批，heredoc/sed 改文件被护栏 BLOCKED 后模型会反复撞墙烧预算。
DISCIPLINE = ("[执行纪律] 本任务无人工审批：改文件一律用 patch/write_file 工具，"
              "heredoc/sed 重定向会被安全护栏拒绝，不要尝试。")
DISC_MARK = "[执行纪律]"   # 去重判据与 payload 解耦（C2）：改 DISCIPLINE 措辞不漂移判据


def sh(cmd, **kw):
    # errors="replace": sqlite3 CLI 偶发非法 UTF-8 字节（blob/异常编码现场）不得炸调用方。
    return subprocess.run(cmd, capture_output=True, text=True, errors="replace", **kw)


_LOCK_FDS = threading.local()   # 每线程独立 fd：flock 是"打开文件描述"级互斥，
                                # 多线程共享同一缓存 fd 会互相视为重入而失效（2026-10-10 自测抓到）


class _ledger_lock:
    """账本级排他锁（独立复审）。

    一把锁串行化全部**状态转换临界区**：终态写入、lost 判定+转换、标题占用+meta 定稿、
    pending 重建。独立复审的教训：靠"多读一次"缩小竞态窗口不叫 CAS——要么真互斥，
    要么承认没闭合。flock 持锁者死亡由内核自动释放，无死锁残留。
    锁序纪律：**只准** 锁内做 unit 查询/文件读写，**禁止** 持锁再抢别的锁。
    实现要点：fd 存 thread-local（每线程各自 open → 同进程多线程之间 flock 真互斥；
    共享缓存 fd 会让 flock 把另一线程误当重入直接放行）。可重入 = 本线程深度计数。"""

    def __init__(self, root=None):
        self._path = (root or ROOT) / ".lock"
        self._fd = None

    def __enter__(self):
        fds = getattr(_LOCK_FDS, "fds", None)
        if fds is None:
            fds = _LOCK_FDS.fds = {}
            _LOCK_FDS.depth = {}
        fd = fds.get(self._path)
        if fd is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
            fds[self._path] = fd
        if _LOCK_FDS.depth.get(fd, 0) == 0:
            # 审计项：非阻塞轮询 + 总期限，锁获取有界（旧版无限阻塞 → 一个卡死的
            # 临界区把所有派单连带挂起，wait 预算形同虚设）。
            _dl = time.monotonic() + _LOCK_ACQUIRE_TIMEOUT
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= _dl:
                        raise TimeoutError(
                            f"账本锁获取超 {_LOCK_ACQUIRE_TIMEOUT}s（有派单卡在临界区，稍后重试）")
                    time.sleep(0.05)
        _LOCK_FDS.depth[fd] = _LOCK_FDS.depth.get(fd, 0) + 1
        self._fd = fd
        return self

    def __exit__(self, *exc):
        _LOCK_FDS.depth[self._fd] -= 1
        if _LOCK_FDS.depth[self._fd] == 0:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        return False


def write_status(d, st):
    """原子写 status：账本锁内 + 唯一名 tmp + 同目录 rename。
    ⚠ 审计项：tmp 名必须**每写者唯一**（pid+uuid）——固定名在并发下互相 rename 走对方 tmp。
    ⚠ 审计项（独立复审）：写本身入账本锁 → 与 read_status 的 lost 转换互斥。
    ⚠ 终态单调守卫（独立复审）：锁内先读当前态——已是终态而新值非终态 → 拒写。
      封死"worker 先写 done、父 submit 随后无条件覆盖 running"这类非法回退
      （锁只串行化写入，不校验转换合法性；单调性必须由写方显式保证）。"""
    _TERM = ("done", "failed", "timeout", "lost")
    with _ledger_lock():
        try:
            cur = (d / "status").read_text().strip()
        except OSError:
            cur = ""
        if cur in _TERM and st not in _TERM:
            return   # 终态不可被非终态覆盖（done 之后父进程的 running 是迟到的旧值）
        tmp = d / f"status.{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
        tmp.write_text(st, encoding="utf-8")
        tmp.replace(d / "status")   # 同目录 rename = 原子替换


def _pending_valid(obj):
    """pending-wake 记录**可补投**的判据（独立复审）：JSON 能解析 ≠ 凭据有效。
    pw_sweep 消费需要 dispatch/origin_session/origin_profile → 缺任一即视为损坏、可重建。"""
    return (isinstance(obj, dict) and bool(obj.get("dispatch"))
            and bool(obj.get("origin_session")) and bool(obj.get("origin_profile")))


def _ensure_pending(d, meta):
    """确保有**有效**的 pending-wake 记录（供 pw_sweep 兜底补投）。
    统一语义（审计项，2026-10-10 独立复审）：
      - 已存在且**凭据有效** → 绝不改写（"合法凭据永不改写"契约）；
      - 缺失/损坏/凭据无效 → 账本锁内**复检后**重建（锁内复检封死"判损坏→他人修好→
        我仍 replace"的竞态：replace 前再验一次，已被修好就让位）；
      - 只有 wake_intent=yes 且有 origin_session 才写（R05）。
    do_exec finally 与 read_status 两条路径都调本函数 → 语义统一、幂等。"""
    try:
        pw = d / "pending-wake.json"
        if pw.exists():
            try:
                if _pending_valid(json.loads(pw.read_text(encoding="utf-8"))):
                    return                  # 有效 → 不动（快路径，无锁）
            except Exception:
                pass                        # 解析失败 = 损坏 → 进锁重建
        if not (meta.get("origin_session") and meta.get("wake_intent", "yes") == "yes"):
            return
        payload = json.dumps({
            "dispatch": d.name,
            "origin_session": meta["origin_session"],
            "origin_profile": meta.get("origin_profile", ORIGIN_PROFILE),
            "peer": meta["peer"], "mode": meta["mode"], "topic": meta["topic"],
            "status": (d / "status").read_text().strip() if (d / "status").exists() else "?",
            "out": str(d / "out.txt"),
            "created": time.time(),
        }, ensure_ascii=False)
        with _ledger_lock():
            if pw.exists():
                try:
                    if _pending_valid(json.loads(pw.read_text(encoding="utf-8"))):
                        return              # 锁内复检：等锁期间被别的修复者修好了 → 让位
                except Exception:
                    pass
                tmp = d / f"pending-wake.{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
                tmp.write_text(payload, encoding="utf-8")
                tmp.replace(pw)             # 确证仍是垃圾 → 原子替换
                tmp.unlink(missing_ok=True)
            else:
                tmp = d / f"pending-wake.{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
                tmp.write_text(payload, encoding="utf-8")
                try:
                    os.link(tmp, pw)        # 缺失 → 原子创建：已存在 → 让位
                except FileExistsError:
                    pass
                finally:
                    tmp.unlink(missing_ok=True)
    except Exception:  # noqa: BLE001 — 补记录失败不得炸调用方
        pass


def ascii_slug(topic):
    s = re.sub(r"[^A-Za-z0-9_.-]+", "", topic)
    h = hashlib.sha1(topic.encode()).hexdigest()[:8]
    return (s[:40] or "t") + "-" + h


def _sq(s):
    """sqlite3 CLI 位置参数不绑定 ?，标题均为 wrapper 自生成安全串，转义后内联。"""
    return "'" + str(s).replace("'", "''") + "'"


def dump_tail(peer, session, d):
    """timeout/lost 时把会话最后几条消息落进产物目录，供接力任务书引用死因。
    绝不外抛：本函数常在 except 块内被调用，同级 except Exception 捕不到它，
    一旦抛出会跳过 notify 段（2026-10-01 peer 评审 M1）。失败写 tail.err。"""
    try:
        db = Path.home() / f".hermes/profiles/{peer}/state.db"
        if not db.exists():
            return
        r = sh(["sqlite3", str(db),
                "select role || char(9) || substr(replace(content,char(10),' '),1,300) "
                "from messages where session_id=("
                "select id from sessions where title=" + _sq(session) +
                " order by started_at desc limit 1) order by id desc limit 8;"],
               timeout=_LOCK_SUBPROC_TIMEOUT)   # 审计项：常在 read_status 锁内被调，必须有界
        if r.stdout.strip():
            lines = r.stdout.splitlines()
            lines.reverse()   # 正序（旧→新），因果读法正确（S2）
            (d / "tail.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as e:
        try:
            (d / "tail.err").write_text(f"dump_tail failed: {e!r}\n", encoding="utf-8")
        except Exception:
            pass


def _title_in_use(title, peer, exclude_did=""):
    """账本里是否存在**别的**派单（同 peer）已用这个会话标题。#15/#27 标题碰撞检测用。
    审计项（独立复审）：meta 可能是合法 JSON 但 schema 有毒（[]、null、非 dict、
    字段类型坏）→ 一切字段访问必须在 per-entry try 内，一条坏 meta 不得炸扫描方入口。"""
    for mp in ROOT.glob("*/meta.json"):
        try:
            if mp.parent.name == exclude_did:
                continue
            m = json.loads(mp.read_text(encoding="utf-8"))
            if not isinstance(m, dict):
                continue
            if m.get("peer") == peer and m.get("session") == title:
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _title_in_db(title, peer):
    """peer 的 state.db 里是否已有该标题的会话（审计项 残余 4：账本外历史）。
    返回**三态**（审计项，2026-10-10 独立复审）：
      True  = 确证存在（有账本外历史，必须重掷）；
      False = 确证不存在（库不存在，或查询成功且无此行）；
      None  = 未知（库存在但查询失败：锁/损坏）。
    ⚠ 旧版把"查询失败"并入 False（宁漏不误伤）→ 库被 BEGIN EXCLUSIVE 锁住时，
      一个已有 600 条历史的旧候选标题被判"不存在"→ 不重掷 + 注"全新"锚 + 谎报
      pre=0/resumed=false = 旧上下文污染 + 假元数据。未知≠不存在：调用方须把 None
      当"无法确证是新线"→ 保守不注锚 + WARN（与 #36 同源原则）。"""
    db = Path.home() / f".hermes/profiles/{peer}/state.db"
    if not db.exists():
        return False
    try:
        r = sh(["sqlite3", str(db),
                "SELECT 1 FROM sessions WHERE title='" + title.replace("'", "''") + "' LIMIT 1;"],
               timeout=_LOCK_SUBPROC_TIMEOUT)
    except Exception:                          # 超时/异常 = 未知
        return None
    if r.returncode != 0:                      # 查询报错（锁/损坏）= 未知
        return None
    return r.stdout.strip() == "1"


def msg_count(profile, title):
    """返回会话消息数；sqlite 失败返回 None（区别于"确实 0 条")。"""
    db = Path.home() / f".hermes/profiles/{profile}/state.db"
    if not db.exists():
        return 0
    try:
        r = sh(["sqlite3", str(db),
                "select message_count from sessions where title=" + _sq(title) +
                " order by started_at desc limit 1;"],
               timeout=_LOCK_SUBPROC_TIMEOUT)   # 审计项：submit 锁内被调，必须有界
    except Exception:                          # 超时/异常 = 未知（区别于"确实 0 条"）
        return None
    if r.returncode:
        sys.stderr.write(f"msg_count sqlite fail: {r.stderr.strip()[:200]}\n")
        return None
    try:
        return int(r.stdout.strip() or 0)
    except ValueError:
        return None


def build_session_name(mode, peer, topic, rand=""):
    """once: 每次唯一新会话（带日期+rand，根治同天同 topic 复用，V1）；
    continue: 固定命名会话（续聊，膨胀则 WARN 不 bump）。
    review: 唯一命名会话（= fresh 语义，但带 title 可被 archive 命中，侧栏不积灰）。"""
    slug = ascii_slug(topic)
    if mode == "review":
        return f"bd-rv-{slug}-{time.strftime('%H%M%S')}-{''.join(random.choices(string.hexdigits.lower(), k=2))}", ""
    if mode == "once":
        # rand 保证每次唯一 → 永不复用旧会话，符合"每次新会话"承诺；gc 用 --prefix bd-<slug> 仍命中。
        return f"bd-{slug}-{time.strftime('%Y%m%d')}-{rand}", ""
    # continue: 固定名，检查膨胀
    base = f"bd-{slug}"
    n = msg_count(peer, base)
    if n is None:
        return base, f"WARN: msg_count 查询失败(库锁/损坏?)，continue 未做膨胀检查"
    if n > MSG_WARN:
        return base, f"WARN: live session {base} has {n} msgs (> {MSG_WARN}); NOT bumped (continue keeps context). GC it: bot_dispatch.py gc --peer {peer} --prefix {base}"
    return base, ""


def cmd_argv(mode, peer, session, task_path):
    """通道映射。本机 fleet 全部 chat 通道；跨机 peer transport 未实现（V2）。
    所有 mode 同一条通道：--query-file + -c <session> --create-if-missing
    （2026-10-09 按实现订正 docstring；fresh 语义靠"会话名唯一"实现，不靠 -q）。
    任务一律 --query-file（stdin 会被 chat 当 REPL 输入，2026-09-30 实测）。"""
    argv = [HERMES, "-p", peer, "chat", "--query-file", str(task_path), "--quiet"]
    argv += ["-c", session, "--create-if-missing"]   # review 名含时间戳+随机=必然新建(fresh)
    return argv


def unit_name(did):
    return "dispatch-" + did.replace("/", "-")


def do_submit(a):
    if a.mode not in ("once", "continue", "review"):
        sys.exit(f"bad mode {a.mode}")
    task = open(a.task_file, encoding="utf-8").read() if a.task_file else sys.stdin.read()
    if not task.strip():
        sys.exit("empty task")
    rand = "".join(random.choices(string.hexdigits.lower(), k=4))
    did = f"r-{time.strftime('%Y%m%d')}-{ascii_slug(a.topic)}-{time.strftime('%H%M%S')}-{rand}"
    d = ROOT / did
    if d.exists():
        sys.exit(f"dup dispatch id {did} (dir exists)")   # 保证②
    d.mkdir(parents=True)
    # ── 定名 → 标题占用 → 历史计数，**整体在账本锁内**（独立复审）──
    # 独立复审 独立复审 1–4 的根因：查重与占用分离（TOCTOU）、保护漏 continue 新建、
    # 只查账本不查 DB、重掷后沿用旧候选的历史计数。正解 = 单一临界区：
    #   锁内：定名 → 循环查重（账本+DB）→ 写 meta 占坑 → 按**最终**标题算 _pre_msgs。
    # 保护范围 = 新建线（--session-derived：once/review/fresh-continue/continue-miss）；
    # 显式 session= 与账本命中的 continue = 有意 resume，不检查。
    # 审计项：_pre_msgs 必须在重掷**之后**算——旧写法在重掷前缓存，重掷后沿用旧候选
    # 的 600 条历史 → 新 review 不注锚、WARN 引用废弃标题。
    session, warn = build_session_name(a.mode, a.peer, a.topic, rand)
    _explicit = getattr(a, "session", None)
    _derived = bool(getattr(a, "session_derived", False))
    _pre_msgs = None
    with _ledger_lock():
        if _explicit and not _derived:
            # 模型手写的显式名 = 有意 resume：不保护，查它自己的历史
            session = _explicit
            _pre_msgs = msg_count(a.peer, session)
        else:
            # 新建线（自动名 或 plugin 派生名）。例外：continue 的**自动**固定名
            # （bd-<slug>）本就是 resume 语义，不做占用检查；plugin 的 continue-miss/
            # fresh 新建会带 --session-derived → 保护（审计项 残余 1）。
            if _explicit:
                session = _explicit
            _pre_msgs = 0
            if not (a.mode == "continue" and not _derived):
                for _try in range(6):
                    _in_db = _title_in_db(session, a.peer)
                    if _title_in_use(session, a.peer, exclude_did=did) or _in_db is True:
                        if _explicit:   # plugin 派生名：重掷尾段随机
                            session = session[: session.rfind("-")] + "-" + \
                                "".join(random.choices("0123456789abcdef", k=3))
                        else:
                            rand = "".join(random.choices(string.hexdigits.lower(), k=4))
                            session, _ = build_session_name(a.mode, a.peer, a.topic, rand)
                        continue
                    # 审计项（独立复审）：DB 查重返回 None（库锁/损坏，未知）时
                    # 不谎报 _pre_msgs=0 → 交 #36 路径（不注锚 + WARN），避免给一个
                    # 已有历史的会话注"全新独立任务"锚（旧上下文污染 + 假元数据）。
                    if _in_db is None:
                        _pre_msgs = None
                    break
                else:
                    d.rmdir()
                    sys.exit(f"title collision: {session!r} 已被占用（重掷 6 次仍撞）→ 换 topic 或稍后重试")
        if a.mode == "review" and ANCHOR[:12] not in task:
            # 只有**定稿标题确证**无历史（=第 1 轮）才注锚；第 2 轮起不注（否则叫它忘掉上一轮）。
            # ⚠ #36：None(读库失败)≠0：未知时不注入并 WARN。
            if _pre_msgs == 0:
                task = ANCHOR + "\n" + task
            elif _pre_msgs is None:
                warn = ("WARN: 目标会话历史数读取失败(库锁/损坏?)，review 保守**不注入**锚定句"
                        "（避免清掉既有上下文）；如确为首轮请手工补锚。")
        if _explicit and _pre_msgs is not None and _pre_msgs > MSG_WARN:
            # #35：显式会话膨胀只 WARN 不 bump（continue 保上下文）。
            warn = (f"WARN: live session {_explicit} has {_pre_msgs} msgs (> {MSG_WARN}); "
                    f"NOT bumped (continue keeps context). GC it: bot_dispatch.py gc "
                    f"--peer {a.peer} --prefix {_explicit}")
        if DISC_MARK not in task:
            task = task.rstrip() + "\n" + DISCIPLINE + "\n"
        (d / "task.md").write_text(task, encoding="utf-8")
        # 占坑 = meta.json 落盘（标题占用对后来者可见）**必须在锁内**：
        # 查重与占用不可分离（审计项 残余 3 的 TOCTOU 根因）。
        meta = {"id": did, "mode": a.mode, "peer": a.peer, "topic": a.topic,
                "session": session, "warn": warn, "created": time.time(),
                "deadline": a.deadline, "notify": a.notify or "",
                "session_explicit": bool(_explicit),
                "origin_session": getattr(a, "origin_session", None) or "",
                "wake_intent": getattr(a, "wake_intent", "yes") or "yes",
                "origin_profile": getattr(a, "origin_profile", "") or ORIGIN_PROFILE,
                "hermes_bin": HERMES,
                "root": str(ROOT),   # 独立复审：systemd 子体环境干净不继承 PEER_DISPATCH_ROOT
                                     # → 账本根随 meta 定稿，子体/锁/产物同一目录
                "pre_msg_count": _pre_msgs}
        (d / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1))
        # 审计项（独立复审）：占坑后先公布 launching（非 running）。running 只在
        # systemd-run 确认成功后才写。否则"写完 running、unit 尚未 exec"这段间隙里，
        # 外部 waiter 会 systemctl 判 inactive → 假 lost → 提前 rc=1 退出 + pending 固化
        # 错误终态。launching 语义 = 正在启动，read_status 见 launching 一律归 running。
        (d / "status").write_text("launching")
    inner = [sys.executable, os.path.abspath(__file__), "_exec", str(d)]
    if a.inline:
        write_status(d, "running")            # inline 无外部启动间隙，直接 running
        rc = subprocess.call(inner)          # cron worker 内同步跑，不建 transient unit
    else:
        r = sh(["systemd-run", "--user", "--collect", "--quiet",
                "--property=Type=exec",
                f"--property=RuntimeMaxSec={a.deadline + 60}",
                "--property=StandardOutput=null",
                "--property=StandardError=append:" + str(d / "unit.err"),
                "--unit=" + unit_name(did)] + inner)
        if r.returncode != 0:
            write_status(d, "failed")
            sys.exit("systemd-run failed: " + r.stderr.strip()[-200:])
        write_status(d, "running")            # 审计项：unit 确认受理后才转 running
    print(did)
    if warn:
        print("NOTE:", warn, file=sys.stderr)


def do_exec(a):
    """执行体：跑真实命令，写 out.txt + status。在 transient unit 或 cron worker 内。"""
    d = Path(a.dpath)
    meta = json.loads((d / "meta.json").read_text())
    # 审计项（独立复审）：systemd transient unit 环境干净，**不含** HERMES_BIN →
    # 执行体必须消费 submit 时定稿的 meta.hermes_bin，否则自定义 executable 只在
    # submit 父进程生效、默认子体回退到模块默认值（模板 B1 修复的穿透语义，生产同构）。
    global HERMES
    if meta.get("hermes_bin"):
        HERMES = meta["hermes_bin"]
    # 独立复审：账本根同样从 meta 恢复（submit 时定稿）。systemd transient unit 不继承
    # PEER_DISPATCH_ROOT → 不恢复则子体 ROOT 回退默认值，锁与产物分裂。
    global ROOT
    if meta.get("root"):
        ROOT = Path(meta["root"])
    task_path = d / "task.md"
    argv = cmd_argv(meta["mode"], meta["peer"], meta["session"], task_path)
    argv += ["--run-budget", str(meta["deadline"])]
    try:
        r = subprocess.run(argv, capture_output=True, text=True,
                           timeout=meta["deadline"] + 30)
        (d / "out.txt").write_text(r.stdout, encoding="utf-8")
        (d / "err.txt").write_text(r.stderr[-20000:], encoding="utf-8")
        ok = r.returncode == 0 and r.stdout.strip()
        write_status(d, "done" if ok else "failed")
    except subprocess.TimeoutExpired as e:
        write_status(d, "timeout")
        # R08 修复（2026-10-09 professor 审计）：capture_output 下 TimeoutExpired 也带
        # 已捕获的 partial stdout —— 存下来，"超时"与"超时且无任何正文"从此可区分。
        partial = getattr(e, "stdout", None) or ""
        if isinstance(partial, bytes):
            partial = partial.decode("utf-8", "replace")
        if partial.strip():
            (d / "out.txt").write_text(partial, encoding="utf-8")
        dump_tail(meta["peer"], meta["session"], d)
    except Exception as e:
        (d / "out.txt").write_text(str(e))
        write_status(d, "failed")
    finally:
        # 任何终态都归档（含 timeout/lost）。2026-10-01 实测：**归档不影响续会话** ——
        # 对已归档会话 `-c <name>` 仍 resume（同 id message_count 106→108、同名仍单条、唯一索引未冲突），
        # 故早期"归档后可能 resume 不上"的顾虑被证伪；而 continue 的续轮走账本解析，不依赖侧栏可见。
        # 不归档的后果：机器派生会话堆在专家侧栏，且 `sessions prune` **只删已结束会话** ⇒
        # 被砍在流式态（ended_at 空）的会话永远清不掉（2026-10-01 的堆积就是这么来的）。
        try:
            sh([HERMES, "-p", meta["peer"], "sessions", "archive",
                "--title", meta["session"], "--yes"], check=False)
        except Exception:
            pass
        # 落"待补投"记录：in-process 唤醒在**会话不常驻内存时会静默丢失**
        # （2026-10-01 实证：watcher 正常 exit code 1，但会话消息与 agent.log 里都没有注入）。
        # 扫描器按这条记录补投并把结果写回同一目录（见 scripts/pw_sweep.py + cron pw-sweep）。
        # ⚠ R05 修复（2026-10-09 professor 审计）：只有 wake_intent=yes 的单才写 pending ——
        #   sync 单结果已被调用方前台消费、wait=false 是主动不要唤醒，两者写 pending 只会
        #   让扫描器对"根本不需要唤醒"的单误判 target_missing / 制造漏投假象。
        # 审计项（独立复审）：与 read_status 走同一个 _ensure_pending —— 统一
        #   原子创建 + 有效即让位（不再无条件覆盖已有记录）。
        _ensure_pending(d, meta)
    if meta.get("notify") and "telegram" in str(meta.get("notify", "")):
        st = (d / "status").read_text()
        sh([HERMES, "-p", SEND_PROFILE, "send", "--to", meta["notify"], "-q",
            f"bot_dispatch {d.name}: {st}"])


def read_status(did):
    d = ROOT / did
    if not d.exists():
        return did, "unknown", None
    try:
        st = (d / "status").read_text().strip()
    except FileNotFoundError:
        # #21 边界（独立复审）：目录存在但 status 尚未写出 = submit 还在写 /
        # 崩溃在写之前。此时 unit 检查无意义（systemd-run 在 status 之后才跑），
        # 直接归 running 让 wait 下轮重读；真崩的目录无人等待，最终被 GC。
        return did, "running", d
    if st == "":
        # #21（独立复审）：status 文件为空 = 并发 truncate 窗口（旧版裸 write_text）
        # 或刚建目录未写。空串**不是终态**——归一成 running，让 wait 继续轮询，
        # 绝不让调用方把"读到空"误判成完成。
        st = "running"
    if st == "launching":
        # 审计项（独立复审）：submit 已占坑但 systemd-run 尚未确认受理。unit 可能
        # 还没 exec → 此刻 systemctl 判 inactive 是假 lost。一律归 running，让 wait 继续
        # 轮询；submit 随后会转 running（成功）或 failed（systemd-run 报错）。
        # ⚠ 自查补漏（定稿自查）：submit 若在"写 launching → systemd-run"
        # 之间被杀，僵尸 launching 会永远归 running（wait 耗到超时、ghost 不报、GC 不管）。
        # 正常转换是亚秒级 → 滞留超 _LAUNCHING_MAX_S 即判 submit 已死 → lost + 补 pending。
        try:
            _age = time.time() - (d / "status").stat().st_mtime
        except OSError:
            _age = 0
        if _age <= _LAUNCHING_MAX_S:
            return did, "running", d
        # 与 running→lost 同款协议（审计项 教训）：判定+转换整体入账本锁 + CAS 重读。
        # 否则慢 systemd-run（submit 还活着）会在我们写 lost 后覆盖回 running，
        # waiter 已拿 lost 提前 rc=1 退出，worker 却还在跑 → 假终态。
        with _ledger_lock():
            try:
                fresh = (d / "status").read_text().strip()
                fresh_age = time.time() - (d / "status").stat().st_mtime
            except OSError:
                fresh, fresh_age = "", 0
            if fresh == "launching" and fresh_age > _LAUNCHING_MAX_S:
                # 独立复审：mtime 超时只证明"父 submit 没写 running"，不证明"派单已死"
                # （慢 systemd-run 场景）。判死前必须查 unit：active = worker 已起来
                # （只是父进程还卡在 systemd-run 返回前）→ 归 running 继续等。
                try:
                    _act = sh(["systemctl", "--user", "is-active", unit_name(did)],
                              timeout=_LOCK_SUBPROC_TIMEOUT).stdout.strip()
                except Exception:
                    _act = ""
                if _act == "active":
                    return did, "running", d
                st = "lost"
                tmp = d / f"status.{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
                tmp.write_text(st, encoding="utf-8")
                tmp.replace(d / "status")
            elif fresh == "launching":
                # submit 刚刷新过 mtime（慢 systemd-run 还活着）→ 继续等。
                # ⚠ 不可落到下面 running 分支做 unit 检查：unit 可能仍未 exec，
                # systemctl 判 inactive = 审计项 的假 lost 回归。
                return did, "running", d
            elif fresh:
                st = fresh
    if st == "running":
        # 审计项（独立复审）：判定+转换**整体**入账本锁 —— 与 write_status
        # 互斥后这才是真 CAS：worker 写终态要么在我拿锁前（我读到终态，不误判），
        # 要么在我放锁后（它读到的是我写的 lost，随后被它的写覆盖 → 终态单调）。
        # 旧写法"重读一次"只是缩小窗口，不是互斥（独立复审 独立复审 3/3 复现）。
        with _ledger_lock():
            try:
                r = sh(["systemctl", "--user", "is-active", unit_name(did)],
                       timeout=_LOCK_SUBPROC_TIMEOUT)   # 审计项：锁内 subprocess 必须有界
                _active = r.stdout.strip()
            except Exception:
                # 审计项：systemctl 超时/异常 = 未知 → 保守保持 running（不判 lost），
                # 下轮 wait 重读。误判 lost 会让 waiter 提前 rc=1 退出，代价远大于多等一轮。
                return did, "running", d
            if _active == "inactive":
                try:
                    fresh = (d / "status").read_text().strip()
                except FileNotFoundError:
                    fresh = ""
                if fresh and fresh != "running":
                    st = fresh
                else:
                    st = "lost"   # unit 没了但没写终态（RuntimeMaxSec 杀/崩溃）
                    tmp = d / f"status.{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
                    tmp.write_text(st, encoding="utf-8")
                    tmp.replace(d / "status")   # 锁内直写（write_status 会重入锁，此处已在锁内）
                    try:
                        _m = json.loads((d / "meta.json").read_text())
                        dump_tail(_m["peer"], _m["session"], d)
                    except Exception:
                        pass
    if st in ("done", "failed", "timeout", "lost"):
        # #16 闭环（独立复审）：终态但缺/坏 pending-wake（进程在 write_status 后、
        # finally 前被杀）→ 任何 read_status 触点都自愈补建。_ensure_pending 幂等：
        # 有效记录存在时零动作，wake_intent=no 的单零动作。
        try:
            m = json.loads((d / "meta.json").read_text())
            _ensure_pending(d, m)
        except Exception:
            pass
    return did, st, d


def do_status(a):
    did, st, d = read_status(a.id)
    print(f"{did}: {st}")
    if d and st in ("done", "failed", "timeout", "lost") and a.verbose:
        out = d / "out.txt"
        if out.exists():
            print(out.read_text(encoding="utf-8", errors="replace"))


def do_wait(a):
    t0 = time.time()
    # #21（独立复审）：① 终态用**白名单**——"unknown"（id 打错/目录被 GC）与
    #   空串归一的 running 都不算终态，继续轮询到预算耗尽，绝不拿假终态提前退；
    # ② sleep 不越过 timeout 边界（旧版固定 20s，timeout<20 时首轮即睡过头）。
    _TERMINAL = ("done", "failed", "timeout", "lost")
    while True:
        _, st, d = read_status(a.id)
        if st in _TERMINAL:
            print(f"{a.id}: {st}")
            if st == "done" and d and (d / "out.txt").exists():
                sys.stdout.write((d / "out.txt").read_text(encoding="utf-8"))
            sys.exit(0 if st == "done" else 1)
        left = a.timeout - (time.time() - t0)
        if left <= 0:
            break
        time.sleep(min(20, left))
    print(f"{a.id}: wait-timeout", file=sys.stderr)
    sys.exit(2)


def do_gc(a):
    # ⚠ 不用 `sessions prune`：它**只删「已结束」会话**，而机器派生会话常被预算砍在流式输出中途
    # （ended_at 为空、last_activity_description="receiving stream response"）⇒ prune 永远吃不到，
    # 会话只增不减（2026-10-01 实测踩到）。`sessions delete <id>` 对 open 会话有效。
    days = float("".join(c for c in str(a.older_than) if c.isdigit() or c == ".") or 30)
    cutoff = time.time() - days * 86400
    db = Path.home() / ".hermes" / "profiles" / a.peer / "state.db"
    if not db.exists():
        db = Path.home() / ".hermes" / "state.db"
    # prefix 转义：SQL LIKE 的 ' 翻倍；% _ 是通配符，用 ESCAPE 转义（防注入 #13 + 防误匹配）。
    # ⚠ 审计项（独立复审）：ESCAPE 字符反斜杠本身必须**最先**转义——否则
    #   prefix="bd-a\_x" 里的 \_ 会被当成转义序列，LIKE 反而命中 bd-aXx（实测多删）。
    pfx = (a.prefix.replace("'", "''").replace("\\", "\\\\")
           .replace("%", "\\%").replace("_", "\\_"))
    # 账本：title → 该 peer 下全部关联派单 (created, did)，peer-aware + 按时间排序（#26）
    title2disp = {}
    for mp in ROOT.glob("*/meta.json"):
        try:
            m = json.loads(mp.read_text(encoding="utf-8"))
            if not isinstance(m, dict):
                continue     # 审计项：非对象 meta 跳过（坏 meta 不得让 GC 全停）
            _s = m.get("session") or ""
            if not (_s and m.get("peer") == a.peer):
                continue
            # 审计项（独立复审）：created 可能是 str/None/坏值（legacy 账本），
            # 直接进排序键会 TypeError → 连 active 保护都失效。统一强转 float。
            try:
                _c = float(m.get("created") or 0)
            except (TypeError, ValueError):
                _c = 0.0
            title2disp.setdefault(_s, []).append((_c, mp.parent.name))
        except Exception:  # noqa: BLE001 — 坏 meta 跳过
            continue
    def _protected(title):
        # 该 title 关联的全部执行体，任一 unit active 即保护（事实判据优先于时间启发式）。
        # 覆盖 #12（age 名单也要过保护，不只 stuck）与 #26（跨 peer/乱序 → 查全部关联单）。
        for _c, did_c in sorted(title2disp.get(title, [])):
            r = sh(["systemctl", "--user", "is-active", unit_name(did_c)])
            if r.stdout.strip() == "active":
                return True
        return False
    # age 候选（超 retention）：SELECT id,title 以便逐个过 active 保护（#12）
    q = ("SELECT id, title FROM sessions WHERE title LIKE '{}%' ESCAPE '\\' "
         "AND coalesce(last_activity_at, started_at, 0) < {}").format(pfx, cutoff)
    ids, age_n = [], 0
    for ln in sh(["sqlite3", "-readonly", str(db), q]).stdout.splitlines():
        parts = ln.split("|")
        if len(parts) != 2 or not parts[0].strip():
            continue
        age_n += 1
        if not _protected(parts[1].strip()):
            ids.append(parts[0].strip())
    # ── 「卡死」专项：bd- 机器会话若 ended_at 空且静默超 STUCK_MIN，同样过 active 保护。
    #    两层教训：① 2026-10-09 僵尸会话：旧判据绑 description LIKE '%stream%'，
    #    "注入后立刻被杀"的会话 description=空串 → 漏网永驻。
    #    ② 同日核验 V1：deadline 是自由参数（实测有 3600 反例），合法会话阻塞在单个长工具
    #    调用时 last_activity_at 不刷新 → 纯时间阈值会误杀。故 unit active 一律不删。
    STUCK_MIN = 60
    qs = ("SELECT id, title FROM sessions WHERE title LIKE '{}%' ESCAPE '\\' "
          "AND (ended_at IS NULL OR ended_at = 0) "
          "AND coalesce(last_activity_at, started_at, 0) < {}").format(
              pfx, time.time() - STUCK_MIN * 60)
    stuck = []
    for ln in sh(["sqlite3", "-readonly", str(db), qs]).stdout.splitlines():
        parts = ln.split("|")
        if len(parts) != 2 or not parts[0].strip():
            continue
        if not _protected(parts[1].strip()):
            stuck.append(parts[0].strip())
    for sid in stuck:
        if sid not in ids:
            ids.append(sid)
    if a.dry:
        print(f"gc {a.peer}: would delete {len(ids)} (age={age_n} stuck={len(stuck)})")
        for sid in ids[:50]:
            print("   ", sid)
        return
    n = 0
    for sid in ids:
        res = sh([HERMES, "-p", a.peer, "sessions", "delete", sid, "--yes"], check=False)
        if res.returncode == 0:
            n += 1
        else:
            print(f"   ! delete {sid} rc={res.returncode} {((res.stderr or '')[:120])}")
    if n:
        sh([HERMES, "-p", a.peer, "sessions", "optimize"], check=False)
    print(f"gc {a.peer}: deleted {n}/{len(ids)} (prefix={a.prefix} older_than={a.older_than} stuck={len(stuck)})")


def do_ledger(a):
    """网状审计面：扫 ~/bot_dispatch/ 全账本（多发起方架构后发起源 = 发起方 + 3 专家，星型审计失效）。

    只读。默认列最近 --days 天的派单 + 按 status/peer 汇总；--json 出机器可读行。
    """
    cutoff = time.time() - a.days * 86400 if a.days else 0
    rows = []
    for meta_path in sorted(ROOT.glob("*/meta.json")):
        # 审计项（独立复审）：整条处理路径入 try——旧版 try 只包 json.loads+float，
        # 毒字段（origin_session=42 切片 / created='inf' 溢出 / id=['x'] 排序）在
        # 循环体后段仍炸全账本。字段类型不可信，逐条兜底。
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if not isinstance(m, dict):
                continue     # 审计项：合法 JSON 但非对象（[]/null/"str"）→ 跳过
            created = float(m.get("created") or 0)
            if cutoff and created < cutoff:
                continue
            did = m.get("id") or meta_path.parent.name
            st = (meta_path.parent / "status").read_text().strip() if (meta_path.parent / "status").exists() else "?"
            try:
                outsz = (meta_path.parent / "out.txt").stat().st_size
            except OSError:
                outsz = 0
            rows.append({"id": str(did), "when": time.strftime("%m-%d %H:%M", time.localtime(created)) if created else "?",
                         "status": st, "peer": str(m.get("peer", "?")), "mode": str(m.get("mode", "?")),
                         "topic": str(m.get("topic", "")), "origin": str(m.get("origin_session") or "-")[:23],
                         "session": str(m.get("session", "")), "out_b": outsz})
        except Exception:
            continue
    rows.sort(key=lambda r: r["id"])
    # R09 修复（2026-10-09 professor 审计）：过滤必须在 JSON/文本分叉**之前**，
    # 否则 --json 与 --status/--peer 组合时过滤静默失效（两种格式必须同一行集）。
    if a.status:
        rows = [r for r in rows if r["status"] == a.status]
    if a.peer:
        rows = [r for r in rows if r["peer"] == a.peer]
    # 审计红旗：running 但 systemd 单元已消失（= 该被 read_status 标 lost 却没标到的漏网）。
    # ⚠ #5（独立复审）：这段必须在 JSON/文本分叉**之前**算——旧版放在文本分支尾部，
    #   `--json` 直接 return，ghost 红旗在机器可读路径上**从没出现过**，而 cron/脚本恰恰用 --json。
    #   inline 单同步跑完必写终态、不会留 running，故不会误报。
    for r in rows:
        r["ghost"] = (r["status"] in ("running", "launching") and
                      sh(["systemctl", "--user", "is-active", unit_name(r["id"])],
                         check=False).stdout.strip() != "active")
    if a.json:
        print(json.dumps(rows, ensure_ascii=False))
        return
    for r in rows:
        print(f"{r['when']}  {r['status']:8s} {r['peer']:14s} {r['mode']:8s} "
              f"from={r['origin']:23s} {r['id']}")
    by_st, by_peer = {}, {}
    for r in rows:
        by_st[r["status"]] = by_st.get(r["status"], 0) + 1
        by_peer[r["peer"]] = by_peer.get(r["peer"], 0) + 1
    print(f"合计 {len(rows)} 单 | status: {by_st} | peer: {by_peer}")
    ghosts = [r["id"] for r in rows if r["ghost"]]
    if ghosts:
        print(f"⚠ 幽灵 running（单元已不在）: {ghosts}")


def main():
    ap = argparse.ArgumentParser(prog="bot_dispatch")
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("submit")
    s.add_argument("--mode", required=True)
    s.add_argument("--peer", required=True)
    s.add_argument("--topic", required=True)
    s.add_argument("--task-file")
    s.add_argument("--deadline", type=int, default=1500)
    s.add_argument("--inline", action="store_true")
    s.add_argument("--notify")
    s.add_argument("--session",
                   help="显式指定会话名（覆盖自动命名）。多轮用：第 2 轮传同一名字即 resume 同一会话。")
    s.add_argument("--session-derived", action="store_true",
                   help="标记 --session 是工具层派生的唯一名（once/review 语义）→ 参与标题碰撞检测。"
                        "模型手写的显式 session（有意 resume）不带此标记。")
    s.add_argument("--origin-session",
                   help="发起方会话 id。记进 meta，供 continue 的账本解析（按 (发起会话,topic) 找最近一单）。")
    s.add_argument("--wake-intent", default="yes", choices=["yes", "no"],
                   help="本单是否需要异步唤醒回流（R05）。no = sync/wait=false，结果已被消费或不需要唤醒，"
                        "不写 pending-wake 补投记录。")
    s.add_argument("--origin-profile", default=ORIGIN_PROFILE,
                   help="发起方 profile（：pw_sweep 只补投 发起方 来源的单）。")
    s.set_defaults(fn=do_submit)
    x = sp.add_parser("_exec"); x.add_argument("dpath"); x.set_defaults(fn=do_exec)
    t = sp.add_parser("status"); t.add_argument("id"); t.add_argument("-v", dest="verbose", action="store_true"); t.set_defaults(fn=do_status)
    w = sp.add_parser("wait"); w.add_argument("id"); w.add_argument("--timeout", type=int, default=3600); w.set_defaults(fn=do_wait)
    g = sp.add_parser("gc"); g.add_argument("--peer", required=True); g.add_argument("--prefix", required=True); g.add_argument("--older-than", default="30d"); g.add_argument("--dry", action="store_true"); g.set_defaults(fn=do_gc)
    L = sp.add_parser("ledger"); L.add_argument("--days", type=float, default=7); L.add_argument("--status"); L.add_argument("--peer"); L.add_argument("--json", action="store_true"); L.set_defaults(fn=do_ledger)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
