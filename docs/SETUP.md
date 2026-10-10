# Setup & Verification

## Prerequisites

- Hermes Agent with **multiple profiles** and a **multiplexed gateway**
  (profiles reachable as `http://127.0.0.1:<port>/p/<profile>`).
- Linux with a working `systemd --user` session (`systemctl --user status` works).
- The dispatcher profile has an `API_SERVER_KEY` in its `.env` if you want the
  sweeper's re-delivery path (see `docs/design.md` §9).

## Install

### 1. Dispatcher profile

```bash
D=<dispatcher-profile>     # e.g. the profile that will dispatch
mkdir -p ~/.hermes/profiles/$D/plugins ~/.hermes/profiles/$D/scripts
cp -r plugin/peer-dispatch ~/.hermes/profiles/$D/plugins/
cp scripts/bot_dispatch.py scripts/pw_sweep.py scripts/bd_gc_daily.sh \
   ~/.hermes/profiles/$D/scripts/
```

### 2. Enable — both places, or it silently does nothing

```bash
# a) the plugin itself
hermes -p $D config set plugins.enabled '[peer-dispatch]'
```

```yaml
# b) the toolset, in the platform your sessions use (desktop sessions use `cli`)
#    edit ~/.hermes/profiles/$D/config.yaml
platform_toolsets:
  cli:
    - ...                      # your existing toolsets
    - peer_dispatch            # ← add this
```

> Adding only the toolset → the tool is "unavailable" at runtime.
> Adding only `plugins.enabled` → the tool never appears. Both are required.

### 3. Restart serve (not just the gateway)

`hermes plugins enable` reloads the process owning the control socket — usually
`hermes-gateway.service`. **Desktop/dashboard sessions run inside
`hermes-serve.service`**, which never loaded the plugin. Restart it:

```bash
systemctl --user restart hermes-serve.service
```

This briefly drops desktop connections; session data lives in `state.db` and is
not lost. Gateway, cron, and messaging platforms are unaffected.

> Tip: if you dispatch from a long-lived session, run the restart from a detached
> helper so your own turn isn't cut off:
> `systemd-run --user --collect sh -c 'sleep 6 && systemctl --user restart hermes-serve.service'`

### 4. Peer profiles that will dispatch too (optional, mesh topology)

If specialists also dispatch to each other, copy the plugin + wrapper into each
and repeat step 2. Keep the copies byte-identical — `bd_gc_daily.sh` checks this
daily. Profiles that should only *receive* (never dispatch) get nothing.

### 5. Cron jobs

Two jobs, both `no_agent` (script-only, empty stdout = no delivery):

```bash
# daily GC + twin-drift check
# schedule: 30 5 * * *   script: bd_gc_daily.sh
# completion sweeper
# schedule: every 5 min  script: pw_sweep.py
```

## Verify — in this order (saves time)

1. **Handler can be invoked** (catches spawn-layer problems):
   ```bash
   hermes -p $D chat -q "call peer_dispatch with mode=once, peer=<peer>, topic=probe, task_file=/tmp/probe.md"
   ```
   Check the returned JSON for `status` / `result`.
   ⚠ In a one-shot (`chat -q`) session the tool **forces `sync=True`** and creates
   **no watcher** — that is correct behaviour, not a bug. Don't look for
   `watcher.json` here.

2. **End-to-end wake must be tested in a desktop / interactive session.**
   The watcher and the wake-back only exist in a long-lived session. A one-shot
   session not being woken is by design (its process has already exited).

3. **Ledger sanity:**
   ```bash
   python3 ~/.hermes/profiles/$D/scripts/bot_dispatch.py ledger --days 1
   ```
   `running` rows whose systemd unit is gone are "ghost" entries — investigate.

## Troubleshooting: "installed but not active"

Work the layers in order; don't guess.

1. **Which process serves this session?** `profiles/<p>/logs/gui.log` showing
   `tui_gateway.server: session.create` = the desktop backend (serve).
2. **Who owns the control socket?** `ss -xlp | grep hermes`, then
   `tr '\0' ' ' < /proc/<pid>/cmdline` to tell `gateway run` from `serve --port`.
3. **Config layer:** is the toolset in `platform_toolsets.<platform>`, is the
   plugin in `plugins.enabled`, is the key in `cache/plugin_toolset_keys.json`?
4. **Session layer:** ask the model discriminatively — *"if you do not have the
   peer_dispatch tool, reply only 'unavailable'; do not substitute terminal or a
   script"* — then inspect `messages.tool_calls` for what it actually called.
5. **Don't be fooled by a fresh process:** `hermes … chat -q` spawns a new process
   that rediscovers plugins at startup, so it will always work. "CLI can call it"
   does **not** prove the long-lived serve/gateway can.

## Environment variables

See the table in [README.md](README.md#configuration). Set them wherever your
systemd user units read environment (e.g. a drop-in, or the profile's `.env`).
