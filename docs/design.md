# 设计文档：任务完成后唤醒「发起会话」

> 本文是这个模板的核心价值。代码只是骨架，真正的资产是这里记录的机制边界与踩坑——
> 每一条都付过真实代价。

## 0. 问题陈述

多 profile 的 Hermes fleet 里，调度者（dispatcher profile）向专家（peer profile）派一个
可能跑几十分钟的任务。派完之后的通知怎么回来？

朴素方案都不成立：

| 方案 | 为什么不行 |
|---|---|
| 发通知到 Telegram/邮件 | 通知的是「人」，不是「发起会话」。会话本身没被唤醒，调度者下一轮还是不知道结果 |
| 靠 SOUL/skill 提醒模型「记得挂 watcher」 | 模型会漏传 flag。实测：SOUL 写了「必须 sync=true」，模型照样漏 → 退化成前台轮询 → 被超时杀掉 |
| 外部进程写数据库表 | 持久账本只在网关**启动时**回放一次；运行期消费的是**内存队列**。外部 INSERT 一张表 ≠ 网关会看它 |
| 外部 HTTP POST 注入 | 网关没有可用的「注入」端点；route-only profile 的镜像路径鉴权返回 401（它不持有自己的 key） |
| `tail -f` 挂日志等完成 | `tail -f` 永不退出，而 notify **只在进程退出时触发一次** → 通知必然永不送达 |

## 1. 核心结论（一句话）

**「任务完成后唤醒发起会话」只能由网关进程自己发出** —— 只有它持有平台 adapter 对象
（`gateway/wake.py::deliver_wake(adapter=…)`）。外部进程够不着它。

由此两条铁律：

1. **要「默认唤醒、模型零 flag」，就把派单搬进网关（做成插件/工具）**。
   `persist_on_release` 这类字段是**网关进程注册表**的属性，网关外的脚本给自己打不上；
   只有网关内的 terminal 工具能设。
2. **可靠性必须程序化，不能靠指令**。能给程序保证的事，就不要写成给模型的提示词。

## 2. 架构（本方案）

```
调度会话 (serve 内, 常驻)
   │  调用 peer_dispatch 工具
   ├─→ ① 执行体：systemd-run --user transient unit
   │      RuntimeMaxSec = deadline + 60
   │      → 跑 `hermes -p <peer> chat --query-file <task> -c <session> --create-if-missing`
   │      → 产物落 ~/bot_dispatch/<id>/{out.txt,err.txt,status,meta.json}
   │      为什么 systemd：免疫 agent_close（会话结束/压缩/错误恢复的 SIGTERM 清扫）
   │      网关重启也带不走它，事后可取产物
   │
   └─→ ② watcher：terminal_tool(background=True, notify_on_complete=True,
             persist_on_release=True, task_id=…, session_id=…)
          跑 `bot_dispatch.py wait <id>` —— 轮询 status 文件，到终态即退出
          → 退出触发 notify → 网关进程内 push 注入发起会话（新 turn）
          persist_on_release=true 让它活过 agent_close
          不经过 default、不 self-post
```

两个组件缺一不可：执行体保证**任务活得久**，watcher 保证**通知回得来**。

## 3. 唤醒语义四级（「自动唤醒」≠「已送达」）

诚实的可靠性声明必须分级。对外只准说「默认 watched」，不准说「无丢失 / 恰好一次」：

| 级 | 含义 | 判据 / 留痕 |
|---|---|---|
| accepted | 派单被受理 | `~/bot_dispatch/<id>/` 目录 + status=running |
| watched | watcher 已挂 | 返回值 `watched=true` + watcher.json |
| delivered | 唤醒消息进了发起会话 | 原生推送（**无留痕**）或 wake-delivery.json（补投路径） |
| consumed | 发起会话真的处理了 | 发起方回合产出（需人工/下游验收） |

已知丢失窗口：`watched=false`；网关重启带走 watcher；目标会话不在内存（原生 push 静默丢，
靠补投扫描器 5 分钟粒度兜底）；竞态护栏的观察窗；超期跳过；启发式去重可能误归属
（设计取向：**宁可漏投人工补，不可重复打扰**）。

恢复路径：`ledger` 找单 → 读 out.txt 取结果 → `continue` 模式续轮。

## 4. 三种 mode 的语义（防污染设计）

通道一律 `chat --query-file <task> -c <session> --create-if-missing`，差异只在会话命名与是否注入锚定句：

| mode | 用途 | 会话 | 锚定句 |
|---|---|---|---|
| `once` | 单一任务 / 调研 / 核实 | 每次唯一命名（干净新建） | 不注入（新会话本无历史） |
| `continue` | 接力活、补齐缺口 | 账本解析接上 (发起会话, topic, peer) 最近一单 | **不注入** |
| `review` | 独立核验 / 对抗评审 | 每次新建（干净上下文） | 注入（仅首轮，见下） |

锚定句 = 「[全新独立任务 — 忽略你此前对任何主题的结论，只依据本任务书证据重判]」。
锚定句**只在 review 且目标会话确证零历史（首轮）时注入**：once 是全新会话本无旧结论可污染；
第 2 轮起不注入（注入了等于叫对方忘掉上一轮）；读库失败=未知时保守不注入并 WARN。

**为什么 continue 不注入**：注入了等于叫对方忘掉上一轮，毁掉接力上下文。
这个区分曾导致真实故障：多轮评审误用 review 模式 → 每轮新建会话 + 每轮叫专家「忘掉前论」
→ 多轮往返形同三次独立评审。

**判定轴 = 结论独立性会不会被对方会话历史污染**（不是任务类型名词）。三问任一命中即走
`review` 干净上下文：① 任务书含「核实 / 不得凭记忆 / 独立判断 / 找否决项」；
② 结论直接进决策或对外转述；③ 对方 profile 会话有同主题旧结论。

## 5. 命名 v2（防串线）

`once`/`review`：`bd-<发起会话id>-<topic slug>-<4hex topic摘要>-<HHMMSS>-<3hex>`
`continue`：不命名，走账本按 (origin_session, topic, peer) 解析最近一单。
⚠ 账本解析发生在**工具层**（插件派生会话名时）；直接调 `submit --mode continue` 不带
`--session` 时只按 topic 产生固定名 `bd-<slug>`，不做账本解析——脚本直连用户要续线请显式传 `--session`。

三个坑都修过：
- v1 只带 sid+slug → 同一会话对同一 topic 发第二单会**顶掉上一单**
- slug 截 24 字符 → 两个共享前 24 字符的**不同** topic 派生同名 → 静默复用旧会话（串线）。
  修法：把完整 topic 的 4hex 摘要塞进名字，把「同 slug 不同 topic」的碰撞概率压到 ~1/65536
  （⚠ 不是结构上不可能——md5 前缀存在实测反例；真正的兜底是 wrapper 的标题占用检测，见 §5 末）
- 3hex = 1/4096，同秒并发有微小碰撞概率，**不是「绝对不撞」**。真撞时 wrapper 在账本锁内
  做「查重+占用」（账本+DB 双查，撞了重掷随机、6 次仍撞拒单），不会静默串线；
  目录去重只防 dispatch id 撞，**不防标题撞**——两者是不同问题

## 6. 补投扫描器的竞态护栏

补投扫描器（pw_sweep）的存在前提：原生推送是**内存 push、无重试无队列**，目标会话不在内存时静默丢。
它靠「库里有没有注入消息」反推是否送达。

**但注入消息是在发起方回合结束时才提交进库的**（实测：注入行与发起方收尾的 assistant 行
同一时刻落库）。于是「派单已终态、发起方回合还在跑」这个窗口里查库必然查不到
⇒ 把**正常投递**误判成丢失 ⇒ **假补投**（一条重复通知）。

实测复现：同一台机器同一个扫描器，上一 tick 判 `already_delivered`、下一 tick 判「丢了」，
差别只在发起方回合是否还在跑。

两条护栏（放在「去重核实」之后、「实际投递」之前；任一成立即 defer）：

1. `origin_idle()` 为假（= mid-turn）：发起方会话最后一条 `active=1` 消息是 `tool`，
   **或**是带 `tool_calls` 的 `assistant`。反之最后一条是**无** `tool_calls` 的 `assistant`
   才算 idle。⚠ 别把判据写反。
2. 终态距今 `< GRACE_S`（默认 120s）。

`HARD_CAP_S`（默认 3600s）兜底：终态超 1 小时不再拦，防发起方永久卡死时永不补投。
defer 仍要留痕，但**同一轮只记一次**，否则每 5 分钟刷一行审计。

## 7. 会话 GC 的两层判据（防误杀）

派单产生的会话是机器会话（title 前缀 `bd-`），需要定期清理。「卡死」形态最难判：
`ended_at` 为空、会话再也不会推进。

- **第一版判据（错）**：绑 `last_activity_description LIKE '%stream%'`。
  漏掉「任务书注入后立刻被杀」的会话——它从没进过流式，description 是**空串** → 永驻。
- **第二版判据（仍不够）**：ended_at 空 + 静默超 60min。
  论证「合法 body 最长 46.5min < 60min 故无误杀」被实测推翻：deadline 是**自由参数**
  （账本里有 3600s 反例，RuntimeMaxSec=61min > 阈值），且合法会话**阻塞在单个长工具调用期间
  `last_activity_at` 不刷新** → 纯时间阈值存在误杀路径。
- **现行判据（两层）**：静默超阈值的候选，先按 title 反查账本找派单，命中则查
  systemd unit `is-active` —— **active 一律不删**。
  事实判据（执行体还活着）优先于时间启发式；账本查不到才退回时间阈值。

教训：**别用字符串形态当判据，也别用时间阈值单独当判据。能查事实就别猜。**

## 8. 嵌套预算链（A→B→C 三层）

sync 路径会**重钳**预算：`sync_wait = min(360, budget-120)`，子单 `deadline = sync_wait-60`，
在 submit 前钳好。子单由 systemd unit 托管到自然终态（RuntimeMaxSec=deadline+60），
父等超时**不会**杀子；结果照常落 out.txt，只是父拿不到——这不是孤儿，是"父先失去耐心"。

⚠ 三层预算要分清：
- async 路径：deadline = body 真实预算（RuntimeMaxSec = deadline+60）
- **sync 路径会重钳**：deadline=2700 与 9999 都得到 wait=360/child=300——**提高父 deadline
  不能突破 360 封顶**（封顶来自工具层 420s 并发超时，不是预算不足）。要更长的执行体就别用 sync。
- **下限例外**：requested≤180 时 wait=60、child=60——子 body 只有 60s，父 60s 后先超时退出，
  子继续跑完并落盘。短预算的 sync 调用要按"60s 内能完成"来设计，否则改 async。

**部分交付 + continue 续轮**是嵌套链的正常形态：子单预算内交不完时 out.txt 里有部分结果，
调用方读 out.txt 发现缺口 → `mode=continue` 同 topic 自动接上同一会话补齐。

## 9. 通用化要点（本模板相对原始实现的改动）

原始实现硬编码了一个 fleet 的 profile 名。本模板改为环境变量覆盖，**默认值保持原行为**：

| 变量 | 作用 | 默认 |
|---|---|---|
| `PEER_DISPATCH_PROFILE` | 调度 profile（peer 注册表 scope、origin 兜底） | `hub` |
| `HERMES_BIN` | hermes 可执行文件路径（wrapper 与执行体共用；经 meta 定稿值穿透 systemd 干净环境） | `~/.local/bin/hermes` |
| `PW_ORIGIN_PROFILE` | 补投扫描器覆盖的发起 profile | `hub` |
| `PW_API_BASE` | 补投 POST 目标 | `http://127.0.0.1:8642/p/<profile>` |
| `PEER_DISPATCH_GC_PROFILES` | 参与会话 GC 的 profile（含被调方） | 空格分隔 |
| `PEER_DISPATCH_TWIN_PROFILES` | 参与孪生漂移比对的 profile（仅装了工具的） | 空格分隔 |
| `PW_GRACE_S` / `PW_HARD_CAP_S` | 竞态护栏参数 | 120 / 3600 |
| `PW_MODEL` | 补投注入时使用的模型名 | `local_LLM` |

profile 归属检测用 `get_hermes_home()` 正则取 `/profiles/<name>$`（context-local override，
与 `_script_path()` 同源）。**不能**读 `os.environ["HERMES_PROFILE"]`——serve 与 one-shot
进程里根本没这个变量（实测 `/proc/<serve>/environ` 只有 `HERMES_HOME`），读了恒得兜底值。

## 10. 踩坑清单（每条都付过代价）

**验证方法论**
- 回归必须走**真实生产路径**。手传参数会掩盖默认路径 bug——一个 HIGH 级缺陷
  （origin_profile 恒为兜底值）就是这么从自测里漏过去的，靠独立核验才发现。
- 在**裸 python 进程**里 import 并调用依赖 registry 的判定函数会得到假结果：裸进程没加载插件，
  registry 为空 → 查不到 toolset → 结论与真实 serve 相反。至少先断言 `_registry_entry(name) is not None`。
- 合成数据回归：往 sessions 表 INSERT 必须带 `source` 列（NOT NULL），且必须检查 sqlite3 的
  stderr——「没报错」≠「插进去了」。首轮回归的「保护成功」就是这么来的假阳性。
- 护栏验证必须**双向**：既验「该拦的拦住」，也验「该放的放行」，否则等于把扫描器悄悄关掉。
- 跨时间比较性能数字前先 diff 两边命令；口径不一致的对比是假结论。

**Hermes 机制**
- 工具列表在**会话启动时定型**；Python 工具的 schema 下个会话才生效（防炸 prompt cache）。
  当次会话看不到它，别以为没装上。
- `hermes plugins enable` 的 reload 只打到 control socket 的属主进程，而它通常**不是**
  服务你那个会话的进程。桌面/dashboard 会话跑在 `hermes-serve.service` 里 →
  重启 serve 才加载。重启会瞬断桌面（会话数据在 state.db，不丢）。
- 装工具 = **两处 config 都要改**：`platform_toolsets.<platform>` 加 toolset **且**
  `plugins.enabled` 加插件。只加 toolset 不够。
- `sessions prune` 永不清理「ended_at 为空」的卡死会话 → 必须用 `sessions delete`。
- `-c <name>` **可以** resume 已归档会话（曾据此以为终态会话不该归档，实测推翻）。
- 外部进程 POST 到共享监听器的**根路径** = default 路由，会写进 default 的库。
  必须带 `/p/<profile>/`。
- 去重判据必须限定 `role='user'` + 注入文本形态。只按 proc id 全文搜，会把「工具返回的
  JSON 里也含 proc id」误判成「已送达」→ 真丢的唤醒**永远不会被补投**。
- `str()` 化 SQL 结果会把 NULL 变成字符串 `"None"`（**真值**）→ 判活逻辑恒判 mid-turn，
  把所有待补投全拦死。比较必须写 `x not in ("", "None")`。
- 孪生副本纪律：多 profile 复制同一份代码后，改一处忘同步 = 漂移 = 行为不一致。
  把 md5 比对做成每日 cron（只在漂移时输出，no_agent 空 stdout = 不投递）。

**运维**
- 长任务用 `systemd-run --user` transient unit，免疫会话清扫。
- 改生产配置前先备份（`.bak.<日期>`）；改完用同一对象重跑验收。
- 结论要引用出处（文件:行 / 表:行）。`done` = 每个验收点有工具输出背书。

## 11. 适用边界（诚实声明）

- **Linux + systemd --user**：执行体与 watcher 都依赖 systemd-run。macOS / Windows 需自行
  替换执行体抽象（这是本模板最大的未泛化点）。
- **Hermes 内部机制依赖**：`gateway/wake.py`、`persist_on_release`、`completion_queue`、
  `get_hermes_home()` 的 context override 都是**未承诺稳定的内部实现**。上游变更可能失效。
  本模板**不承诺**跨 Hermes 版本兼容。
- **需要 multiplex 网关**：`/p/<profile>/` 镜像路径与共享监听器是前提。
- 定位是**设计参考 + 可跑代码**，不是产品级发行版。

## 12. 并发正确性的已知限制（诚实声明）

本工具的状态机（status 转换、pending 自愈、标题占用）经过多轮独立对抗审计。
以下边界是**这类"文件账本 + 多进程 + 外部 systemd/sqlite"自愈代码的固有性质**，
不是尚未修完的 bug；单用户、低频派单的真实场景下触发概率极低，但对外披露如下：

- **全局账本锁是活性换正确性**：所有终态写入 / lost 判定 / 标题占用共用一把 flock。
  已加两道界（锁内 subprocess 带 timeout、锁获取带总期限轮询），但极端慢的
  systemctl/sqlite3 仍可能让某次 wait 撞锁超时而报错重试。这是"宁可报错也不无限
  阻塞"的有意取舍。
- **lost 判定依赖外部事实**（systemd unit 是否 active）。unit 启动确认前用 launching
  态隔离，但"submit 写完占坑、systemd 尚未 exec"这段窗口若被外部 status 观察者命中，
  仍依赖 launching→running 的转换时序；正常 plugin watcher 不会在此窗口抢先。
- **DB 查重是尽力而为**：state.db 被 BEGIN EXCLUSIVE 锁住时查重返回"未知"，此时
  review 保守不注锚 + WARN，而非谎报"全新会话"。即：账本外的同名历史在库不可读时
  无法 100% 拦截，退化为 WARN 提示人工确认。
- **毒账本条目逐条隔离**：单条 meta 类型损坏不会阻断 submit/GC/ledger，但被跳过的
  条目在人工修复前不参与 GC/审计视图。
- **形式正确性 ≠ 概率安全**：审计能构造"trace 精确停在竞态窗口"的反例；这些反例
  证明的是"窗口存在"，不等于"真实运行会发生"。本工具选择把工程精力放在**会导致
  数据损坏 / 会话串线 / 进程崩溃**的真 bug 上，而非穷尽所有理论竞态窗口。

### §12 补充（独立复审 R5b 后）
- **launching 判死 = mtime 阈值 + unit 检查双条件**：滞留 >120s 只触发"疑似"，判 lost 前
  必须 systemctl 确认 unit 非 active；慢 systemd-run（>120s）不会被误判。
- **终态单调守卫**：write_status 在锁内拒"终态→非终态"回退，封死"worker 先写 done、
  父 submit 迟到的 running 覆盖"时序（锁只串行化，不校验转换合法性）。
- **账本根随 meta 定稿**：submit 把 ROOT 写进 meta.root，执行体（systemd 子体环境干净、
  不继承 PEER_DISPATCH_ROOT）从 meta 恢复 → 锁与产物恒同目录。自定义账本根请设
  PEER_DISPATCH_ROOT（所有入口进程可见）；仅给 systemd 子体设环境变量是无效路径。

---

## 参考文献

这些文献支撑的是本设计服务的**经济学**（多智能体组织能买来智能）；
管道本身是零引用工程，全部教训见上文各节。

- Wang et al., *Mixture-of-Agents Enhances Large Language Model Capabilities*,
  arXiv:2406.04692 (ICLR 2025) — 开源模型分层协作击败前沿 API。
- Li et al., *More Agents Is All You Need*, arXiv:2402.05120 — 性能随 agent
  数量标度，增益 ∝ 任务难度。
- Du et al., *Improving Factuality and Reasoning in Language Models through
  Multiagent Debate*, arXiv:2305.14325 — 对抗轮次；`review` 模式的理论基础。
  （本仓库故意不内置辩论循环：停滞 ≠ 收敛。）
- Kim et al., *Towards a Science of Scaling Agent Systems*, arXiv:2512.08296 —
  协调开销与结构决定 agent 数量的天花板。
- Wu et al., *AutoGen*, arXiv:2308.08155；Qian et al., *ChatDev*,
  arXiv:2307.07924；Hong et al., *MetaGPT*, arXiv:2308.00352 — 编排层谱系；
  本项目位于它们**之下**，做传输 / 账本 / 唤醒层。
