# Modes: a field guide

Everything here is written against the shipped interface:

```
peer_dispatch(mode, peer, topic, task_file, deadline=1500,
              wait=True, session="", fresh=False, sync=False)
```

or the CLI underneath it (`scripts/bot_dispatch.py submit --mode … --peer …
--topic … --task-file …`). The tool and the CLI are the same machine; the tool
just fills in what a running session knows about itself (origin session id,
wake intent, derived names).

---

## The one decision that matters most: *who remembers what*

Every mode is a different answer to one question — **what does the receiving
agent remember when it starts work?** Get this wrong and the cleverest plumbing
in the world delivers your task book to the wrong kind of mind.

| Mode | Receiver's memory | Analogy |
|---|---|---|
| `once` | none — brand-new session | emailing an expert you've never met |
| `continue` | everything from your last dispatch on this topic | replying in the same thread |
| `review` | none, **plus an explicit warning not to trust you** | sending the manuscript to a stranger for peer review |

---

## `once` — open a new line of work

**What it guarantees.** A fresh named session on the peer (`-c <name>
--create-if-missing`), zero prior messages. The name is derived
(`bd-<origin>-<topic-slug>-<hash>-<time>-<rand>`) and collision-checked against
both the ledger and the peer's session DB — if a name would collide, the tool
re-rolls rather than silently resuming a stranger.

**Use it for:** research questions, data pulls, "go evaluate this repo",
opening a work line you may later continue.

**Best practices**

- The task book is the *only* interface. Structure it:
  `CONTEXT` (one line) → `GOAL` (verifiable) → `CONSTRAINT` (what not to do).
  The reader is smart, new, and amnesiac.
- Don't hand-pick a session name. Let the tool derive it; the handle in the
  result is what a later `continue` resolves against.
- Budget honestly. `deadline` is the body's wall-clock budget:
  doc-only review ≈ 1500s; anything that reads source code or runs
  experiments ≈ 2700s. A budget that's too small doesn't fail loudly — it
  returns a truncated effort that *looks* finished.
- For long tasks, require incremental persistence in the task book
  ("write findings to verdict.md as you go"). A crashed body with a partial
  file beats a perfect session that timed out holding everything in RAM.

## `continue` — relay work on one artifact

**What it guarantees.** The *same* session, resumed. Resolution is by ledger:
(origin-session, topic, peer) → most recent dispatch → its session name. That's
why you reuse the same `topic` string — it's a foreign key, not a label.

**Use it for:** fix rounds after a review, follow-up questions, multi-round
iteration where the previous reasoning *is* the working set.

**Best practices**

- Send deltas. The peer has the context; re-pasting it wastes its window and
  invites drift ("your latest message says X, my first message said Y — which?").
- Round 2, 3, N: same `topic`, no explicit `session` — the ledger does the
  addressing. Passing a hand-written session name bypasses collision checks
  *by design* (you mean it), so it's also how you accidentally resume the
  wrong thing. Prefer the ledger.
- **Never continue a peer into verifying its own work.** That's the
  contamination the next mode exists to prevent.

## `review` — a verdict you can actually trust

**What it guarantees.** A fresh session (like `once`) *plus* an anchor line
injected into the task telling the reviewer to judge only from the artifacts
you handed over, not from any prior position. Freshness is enforced, not
requested: no `--session` → the body provably starts with zero prior messages.

**Use it when any of these three questions is yes:**

1. Does the task say *verify / independently judge / find disqualifiers*?
2. Does the conclusion feed a decision, or get forwarded to a human?
3. Does the peer already have a session with opinions on this topic?

If yes to any → `review`. A verifier that inherits the author's context grades
its own homework, and it grades it *confidently*.

**Best practices**

- Hand over **paths, not summaries**. A summary is your memory wearing its
  best suit. The reviewer reads the file, the diff, the log.
- Ask for a severity-ordered list, and for the reviewer's *own commands* —
  "run X, paste output" is worth ten adjectives.
- Budget generously (2700s+ for code audits) and require
  `verdict.md`-as-you-go. Reviewers die of timeouts more than of difficulty.
- Keep the review session pure. The moment you `continue` a reviewer into
  fix-work, its next verdict is contaminated by its own fingerprints.
  New round → new `review`.

## Execution modes: `watch` (default) / `sync` / `wait=false`

These are orthogonal to session modes — they decide *how the answer comes
back*, not *who remembers what*.

| | How it returns | Use when |
|---|---|---|
| **watch** (default) | You keep working; completion injects a new turn into *your* session | Any long task in a persistent session. This is the overnight case. |
| **sync=true** | Blocks up to a clamped wait; result in this turn | One-shot / `chat -q` sessions — there is no future turn to wake, so async is a lie there. Enforced programmatically when the process is a single-query session; you don't have to remember it. |
| **wait=false** | Nothing comes back; you poll `status`/`ledger` yourself | Headless cron, or when you'll consume the artifact elsewhere. |

**Budget clamping.** Under `sync`, the tool computes:
`your wait = min(cap, your budget − 120s)`, and any nested dispatch the peer
makes gets `child budget = wait − 60s`. The chain is arithmetic, not
aspirational — a nested task can never outlive the turn that's waiting for
it. Keep nesting depth ≤ 1 anyway; a grandchild is a debugging story you
don't need.

---

## Recipes

**Overnight project (the reason this exists).**
Before bed, from your dispatcher session: one `once` per work line, default
watch, honest deadlines, task books that demand artifacts on disk. Sleep.
Each completion wakes your session as it lands; morning reads like a thread.

**Audit loop (generate → verify → fix).**
`once` to produce → `review` (fresh, paths, 2700s) to judge → if FIX:
`continue` the *producer* (same topic) with the verdict file path → new
`review` for round 2. The reviewer is never continued; the producer never
grades itself. Cap fix rounds — our own runs showed stalled ≠ converged;
a loop that repeats its last diff is done, not working.

**Fan-out research.**
N independent `once` dispatches to N peers in one turn (independent calls go
in one block). Watch wakes you once per completion; the ledger
(`bot_dispatch.py ledger --json`) is the morning roll-call.

**Chain (A's output feeds B).**
Dispatch A with watch. When the wake arrives, dispatch B with a task book
that cites A's artifact *path*. Don't build chains inside one task book —
a hop that fails silently is a hop you didn't budget for.

---

## Failure vocabulary (what the ledger will say to you)

| status | meaning | your move |
|---|---|---|
| `launching` | accepted, unit starting (sub-second normally) | nothing |
| `running` | unit alive, working | nothing |
| `done` | terminal, artifact in `~/bot_dispatch/<id>/` | read it |
| `failed` / `timeout` | terminal, partial artifact may exist | read, then re-dispatch narrower |
| `lost` | status said alive, the unit is gone (ghost) | the sweeper flags it; re-dispatch if the artifact is empty |

`launching` older than ~2 minutes with no unit behind it is *also* `lost` —
the state machine checks the unit, not the clock, before declaring death.
That distinction (clock suspects, fact decides) is the whole GC philosophy;
see [design.md](design.md) §12 for what it costs and what it can't do.
