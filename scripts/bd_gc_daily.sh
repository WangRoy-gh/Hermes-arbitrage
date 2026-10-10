#!/usr/bin/env bash
# bot_dispatch 会话 GC（**每日**）：删除 <peer> 侧 title 以 bd- 开头、且最后活动早于 5 天的会话。
#
# 为什么不用 `hermes sessions prune`：
#   prune **只删「已结束」会话**，而机器派生会话常被预算砍在流式输出中途
#   （ended_at 为空、last_activity_description="receiving stream response"）
#   ⇒ prune 永远吃不到它们，专家侧会话只增不减（2026-10-01 实测踩到，两条卡死会话滞留 5 小时+）。
#   现在走 `sessions delete <id>`（对 open 会话有效），由 bot_dispatch.py 的 gc 子命令实现。
#
# 由 cron job bd-session-gc 调用（no_agent，不进 LLM）。
# 只在真的删了东西时输出 ⇒ 平时静默（no_agent 空 stdout = 不投递，watchdog 模式）。
set -uo pipefail
# 部署时改成你的调度 profile（REF = 装了本工具的 profile 根目录）
REF=~/.hermes/profiles/${PEER_DISPATCH_PROFILE:-发起方}
BD=$REF/scripts/bot_dispatch.py
RETENTION="${1:-5d}"
# 两类 profile 集合，**必须分开**（2026-10-10 独立复审 #3：混用一个列表 → 要么漏 GC 被调方，
# 要么对未装出站工具的被调方误报孪生漂移）：
#  ① GC 集合 = 会话可能堆积的全部 profile = 装了本工具的 + **被调用方**
#     （被调方即使不装本工具，peer 派单也会在它的库里建 bd- 前缀会话，GC 必须覆盖）。
#  ② 孪生集合 = 只装了本工具（出站插件/wrapper）的 profile —— 被调方不比对，不误报。
PEER_PROFILES="${PEER_DISPATCH_GC_PROFILES:-hub dispatcher-a dispatcher-b receiver-c}"
TWIN_PROFILES="${PEER_DISPATCH_TWIN_PROFILES:-hub dispatcher-a dispatcher-b}"
for p in $PEER_PROFILES; do
  out=$(python3 "$BD" gc --peer "$p" --prefix bd- --older-than "$RETENTION" 2>&1)
  case "$out" in
    *"deleted 0/0"*) : ;;                      # 无事发生 → 静默
    *) echo "$p -> $out" ;;
  esac
done

# ── 孪生副本漂移检查（2026-10-09）──────────────────────────────────────
# peer-dispatch 插件 + bot_dispatch.py 是孪生复制进各装了本工具的 profile 的；
# 改了 REF 侧忘了同步 = 漂移 = 各 profile 行为不一致。每日 md5 比对，
# 只在漂移时输出（no_agent 空 stdout = 不投递）。部分 profile 有意不装工具，不参与比对。
drift=0
for f in plugins/peer-dispatch/__init__.py scripts/bot_dispatch.py; do
  ref=$(md5sum "$REF/$f" 2>/dev/null | cut -d' ' -f1)
  # 独立审计修复（2026-10-10）：REF 文件本身缺失时 ref 为空串，
  # 若对方也缺失则 cur==ref=="" → 静默通过。"两个都缺失"不是健康，是监控失明。
  [ -z "$ref" ] && { echo "⚠ 孪生基准缺失: $REF/$f 不存在 → 漂移检查失明，先修基准"; drift=1; continue; }
  for p in $TWIN_PROFILES; do
    cur=$(md5sum ~/.hermes/profiles/$p/$f 2>/dev/null | cut -d' ' -f1)
    [ "$cur" != "$ref" ] && { echo "⚠ 孪生漂移: $p/$f  (cur=${cur:-缺失} ref=${ref:-?}) → cp 同步"; drift=1; }
  done
done
# 独立审计 修复（2026-10-09 独立审计）：md5sum 在文件缺失时置 rc=1，循环最后一条命令的
# 退出码会泄漏成脚本退出码 → 无漂移也"失败"。no_agent cron 语义 = 空 stdout 即静默，
# 退出码必须显式归零（漂移与否由 stdout 有无表达，不由 rc 表达）。
exit 0
