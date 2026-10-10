# Hermes-arbitrage

> **Buy intelligence with time, not tokens.**
> 中文版本：[docs/README_zh.md](docs/README_zh.md)

The multi-agent literature has known for years that ordinary models, organized
well, can punch far above their weights — layered teams of open models beat
GPT-4o on AlpacaEval 2.0, 65.1% vs 57.5%
([MoA, arXiv:2406.04692](https://arxiv.org/abs/2406.04692)), and simply
sampling more agents lifts accuracy in proportion to task difficulty
([More Agents Is All You Need, arXiv:2402.05120](https://arxiv.org/abs/2402.05120)).
The research was never the bottleneck. The *pricing* was: nobody burns $200 of
API tokens to argue with himself.

Run your agents on a local model and the price stops being the constraint.
Tokens cost approximately zero; what you spend instead is **wall-clock**. That
is an arbitrage — trade the hours you're asleep anyway for the intelligence
you can't buy at any reasonable price.

The catch: the entire multi-agent toolchain is priced per token and shaped
accordingly — processes live as long as their tasks, calls return results,
"long-running" means thirty seconds. An overnight workflow breaks every one of
those assumptions. **Hermes-arbitrage is the plumbing that fixes exactly that** — a
dispatch layer where a task outlives the session that launched it, and the
*originating session itself* is woken when the work is done.

```
you (before bed) ──► dispatcher session ──► peer agent #1           ─┐
                                    ──► peer agent #2            ──┼──► 03:00 the answer
                                    ──► reviewer #3 (fresh ctx)  ─┘       wakes YOUR session;
you (morning) ◄── the conversation is already continued                    results land as a turn
```

## The resource gap, in numbers

Frontier capability is first a resource fact, and the vendors say so: Anthropic
reports training and serving Claude on **over one million Trainium2 chips**,
under an agreement securing up to 5 GW of capacity
([source](https://www.anthropic.com/news/anthropic-amazon-compute)). Yet the
capability gap is far smaller than the resource gap: DeepSeek-V3's full
training run cost **2.788M H800 GPU-hours** (excluding prior research and
ablations) and its results were reported as *comparable to leading
closed-source models* ([arXiv:2412.19437](https://arxiv.org/abs/2412.19437)).
Capability splits into a **resource advantage** — purchasable, monopolizable,
embargo-able — and an **organizational advantage** — published, free to cite,
impossible to embargo. This project bets on the second. The bet varies by
task; the plumbing does not.

## What problem this actually solves

Three layers, each dependent on the one below:

| Layer | Problem | Why existing answers fall short |
|---|---|---|
| Economics | Multi-agent gains are well documented but uneconomical at API prices | Not a tooling gap — a price gap. A local model removes the price. |
| Engineering | A task runs for hours. The launching session may die, the result has no receiver, stuck tasks eat concurrency slots, and a "verifier" that inherits the author's context grades its own homework. | AutoGen / ChatDev / MetaGPT are *orchestration* layers (who talks to whom). All assume the orchestrating process survives and calls return. |
| Operations | Overnight projects need: fire-and-forget, guaranteed wake-up, budget clamping, contamination isolation, an auditable ledger | No paper, no framework covers this. It is pure plumbing — and it is what this repo is. |

## How it works

Two halves, both required:

```
dispatcher session (long-lived, inside serve)
   │  calls the peer_dispatch tool
   ├─→ ① body: systemd-run --user transient unit   (immune to session cleanup)
   │      runs `hermes -p <peer> chat --query-file <task> -c <session>`
   │      artifacts → ~/bot_dispatch/<id>/{out.txt,status,meta.json}
   └─→ ② watcher: background process, persist_on_release
          polls status, exits on terminal state
          → exit triggers an in-gateway push: a new turn in the ORIGINATING session
```

The body makes the **task** outlive the session; the watcher makes the
**notification** come back. The core finding that shaped the design: waking an
originating session can only be done by the gateway process itself — it is the
only thing holding the platform adapter objects. Everything else (Telegram
notices, DB inserts, log tails) notifies a *human* — or nothing at all.

## Modes, and when to use which

Three session modes plus two execution modes. Full playbook with worked
examples: **[docs/modes.md](docs/modes.md)**. Short version:

| Mode | What it guarantees | Use it when | Best practice |
|---|---|---|---|
| `once` | Brand-new named session, zero history | Research, data pulls, opening a new work line | The task book must be self-contained — the peer knows nothing. Let the tool derive the session name; keep the handle for a later `continue`. |
| `continue` | Resumes the *same* named session via ledger lookup | Relay work: fix rounds, follow-ups, iterating on one artifact | Reuse the same `topic` (the ledger addresses by origin-session + topic + peer). Send deltas, not full context. **Never** use it to verify the peer's own work — that's `review`'s job. |
| `review` | Clean context + an anti-contamination anchor line | Any verifier role: checking conclusions, auditing plans, hunting disqualifiers | If the task says "verify / independently judge", or the conclusion feeds a decision, or the peer has prior opinions on the topic — use `review`. Hand over file paths, not summaries. |
| `sync=true` | Blocks; the result arrives in the current turn | One-shot / CLI sessions where no async wake could be received | Nested budget chains are clamped automatically (parent wait = budget − 120s, child = wait − 60s). Keep nesting depth ≤ 1. |
| `wait=false` | Fire-and-forget; you poll yourself | Headless cron only | In a persistent session, always prefer the default `watch` — completion wakes you automatically. |

Three cross-mode rules worth tattooing somewhere:

1. **A review session is never continued into relay work.** The moment you
   `continue` a reviewer, it accumulates the very context you paid for it to
   be free of.
2. **Cheap tokens ≠ free judgment.** Mechanical bulk work goes to subagents;
   work requiring taste goes to a peer whose whole job is that domain.
3. **Write the task book for a reader who is smart, new, and amnesiac** —
   context (one line), goal, constraints. It is the only interface.

## The evidence, and what it does *not* say

| Paper | Supports | Caveat |
|---|---|---|
| [Mixture-of-Agents, 2406.04692](https://arxiv.org/abs/2406.04692) (ICLR'25) | Flagship: layered open-model teams beat frontier APIs. The "weak weights + structure ≥ strong weights" proof. | Gains depend on the layered structure, not raw headcount. |
| [More Agents Is All You Need, 2402.05120](https://arxiv.org/abs/2402.05120) | Scaling agent count lifts performance ∝ task difficulty — the direct license for "tokens are cheap, spawn more". | Gains vanish on easy tasks. Don't arbitrage what doesn't need it. |
| [Multiagent Debate, 2305.14325](https://arxiv.org/abs/2305.14325) | Adversarial rounds improve factuality/reasoning — the theory behind `review`. | We deliberately ship **no** built-in debate loop: our own runs showed *stalling ≠ converging*. The primitives let you build one; the literature says budget it. |
| [Towards a Science of Scaling Agent Systems, 2512.08296](https://arxiv.org/abs/2512.08296) | The honest counterweight: coordination overhead and structure matter; naive agent-count scaling plateaus or regresses. | Read it before your fifth parallel agent. |
| [AutoGen 2308.08155](https://arxiv.org/abs/2308.08155) · [ChatDev 2307.07924](https://arxiv.org/abs/2307.07924) · [MetaGPT 2308.00352](https://arxiv.org/abs/2308.00352) | The orchestration lineage. Cited to mark position: Hermes-arbitrage sits *underneath* them, as the transport / ledger / wake layer. | None of them survives its orchestrator dying. That's the niche. |

To be scrupulous: the papers support the *economics* — multi-agent
organization buys capability. They say **nothing** about the plumbing: detached
execution, completion wakes, budget clamping, contamination isolation. That
part is zero-citation engineering, learned across six rounds of adversarial
audit, documented in [docs/design.md](docs/design.md) — scars included.

## "But every agent loses the context!"

The objection every reader raises, and it deserves a straight answer. Multi-agent
wins not by preserving information losslessly — it wins because the losses are
*engineerable*, and because the comparison is with a real single agent, not a
perfect one:

- **The single agent loses too — invisibly.** Long contexts degrade: Chroma's
  report on 18 models shows reliability falling as input grows, well below the
  advertised window ([Context Rot](https://www.trychroma.com/research/context-rot)).
  One agent loses *attention quality*: silent, unlocatable, unrecoverable.
  Split agents lose *handoff bandwidth*: visible on the page, and the next hop
  can re-run the acceptance criteria on it.
- **State lives in artifacts, not transcripts.** Files, ledgers, git — the
  world is the lossless channel; conversation history is a cache.
- **Isolation is the point.** Independent contexts mean independent error
  distributions; aggregate accuracy beats any member *because* they don't share
  a blind spot. A reviewer carrying your full context inherits your narrative —
  and his value drops to zero.
- **Handoffs are gates, not pipes.** Each hop re-verifies the criteria. The
  failure mode the literature actually documents is not "lost context" but
  *bad handoffs* ([Why Do Multi-Agent LLM Systems Fail?, 2503.13657](https://arxiv.org/abs/2503.13657))
  — which is an argument for writing the handoff down, not for giving up splitting.

Honest boundary: tightly coupled, state-heavy tasks resist splitting; there,
one agent wins. The bet is conditional — decomposable, clear interfaces,
independently checkable parts. The plumbing is the same either way.

## Quick start

```bash
# 1. copy into the profile that will dispatch
cp -r plugin/peer-dispatch ~/.hermes/profiles/<dispatcher>/plugins/
cp scripts/bot_dispatch.py ~/.hermes/profiles/<dispatcher>/scripts/

# 2. enable in BOTH places (one without the other silently does nothing)
hermes -p <dispatcher> config set plugins.enabled '[peer-dispatch]'
#   and add `peer_dispatch` to platform_toolsets.cli — see docs/SETUP.md

# 3. serve must be restarted to load it (gateway reload does NOT reach serve)
systemctl --user restart hermes-serve.service

# 4. verify (new session — tool schemas land next session)
PEER=<peer-profile> ./smoke_test.sh
```

## Configuration

Defaults preserve behavior; override with env vars:

| Variable | Purpose | Default |
|---|---|---|
| `PEER_DISPATCH_PROFILE` | dispatcher profile (peer registry scope, origin fallback) | `hub` |
| `PW_ORIGIN_PROFILE` | profile whose dispatches the sweeper re-delivers | `hub` |
| `PW_API_BASE` | sweeper POST target | `http://127.0.0.1:8642/p/<profile>` |
| `PEER_DISPATCH_GC_PROFILES` | profiles in session-GC scope (incl. receive-only) | space-separated list |
| `PEER_DISPATCH_TWIN_PROFILES` | profiles in twin-drift check scope | space-separated list |
| `HERMES_BIN` | path to the hermes executable | `hermes` |
| `PEER_DISPATCH_ROOT` | ledger root directory | `~/bot_dispatch` |
| `PW_GRACE_S` / `PW_HARD_CAP_S` | race-guard window | 120 / 3600 |

## Scope & honesty

- **Linux + `systemd --user` only.** macOS/Windows need your own
  process-supervision abstraction — the biggest un-generalised part.
- **Depends on Hermes internals** (`gateway/wake.py`, `persist_on_release`,
  `completion_queue`, `get_hermes_home()` context override). None is a stable
  API. No cross-version compatibility is promised.
- **Requires a multiplexed gateway** (`/p/<profile>/` mirrors on a shared listener).
- A **design reference with working code**, not a supported product. Not an
  official Hermes plugin; not affiliated with Nous Research.
- Reliability vocabulary is deliberate: *accepted / watched / delivered /
  consumed*. We say "watched by default", never "never loses a message".

## Why not just use Bot Mode?

Hermes Bot Mode lets profiles message each other. What Hermes-arbitrage adds — the
reason it exists — is the four things Bot Mode doesn't do: a body that survives
gateway/session teardown, completion that wakes the *originating session*, a
durable ledger with budget and GC discipline, and `review`/`continue` semantics
that keep an independent verifier from inheriting the opinions it is supposed
to challenge.

## License

MIT — see [LICENSE](LICENSE).
