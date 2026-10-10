#!/usr/bin/env bash
# Minimal closed-loop smoke test: submit → wait → status.
# Does NOT exercise the wake-back path (that needs a long-lived desktop session;
# see docs/SETUP.md step 2 of "Verify"). This only proves the wrapper + body +
# ledger plumbing work end to end.
#
# Usage:  PEER=<peer-profile> ./smoke_test.sh
#   PEER  = a Hermes profile that exists and can answer a trivial prompt.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BD="$HERE/scripts/bot_dispatch.py"
PEER="${PEER:-}"
[ -z "$PEER" ] && { echo "set PEER=<peer-profile> first"; exit 2; }

TASK="$(mktemp /tmp/bd-smoke-XXXX.md)"
cat > "$TASK" <<'EOF'
Reply with exactly the word: PONG
EOF

echo "▶ submit (mode=once, sync via --inline so this script can consume the result)"
OUT=$(python3 "$BD" submit --mode once --peer "$PEER" --topic smoke-$$ \
        --task-file "$TASK" --deadline 180 --inline --wake-intent no 2>&1)
echo "$OUT" | head -5
ID=$(printf '%s' "$OUT" | grep -oE 'r-[0-9a-zA-Z-]+' | head -1)
[ -z "$ID" ] && { echo "✗ no dispatch id in output"; rm -f "$TASK"; exit 1; }
echo "  dispatch id = $ID"

echo "▶ wait (poll status until terminal, ≤200s)"
python3 "$BD" wait "$ID" --timeout 200; RC=$?
echo "  wait rc=$RC  (0=done 1=non-done terminal 2=timeout)"

echo "▶ status"
python3 "$BD" status "$ID" | head -8

echo "▶ ledger"
python3 "$BD" ledger --days 1 | tail -5

rm -f "$TASK"
if [ "$RC" = 0 ]; then
  echo "✓ smoke PASS — dispatch $ID reached done"
else
  echo "✗ smoke did not reach done (rc=$RC). Check ~/bot_dispatch/$ID/{out.txt,err.txt,status}"
fi
exit $RC
